"""GraphQL-first crawl + Selenium fallback (schema 4.1).

Phase A (discover): GraphQL pages from newest; skip already processed;
  parse caption/images when present; log under [fen_gql_discover].
Phase B (enrich): Selenium permalink only when GraphQL incomplete;
  valid → download images immediately; log under [fen_gql_enrich].

Resume strategy:
  - Each batch starts catch-up from newest feed
  - Skip enriched/known post_ids
  - Skip-streak → jump to backfill_cursor (deepest progress toward 2013)
  - Checkpoint after each processed post (progress + periodic index flush)

Cookies/profile only — never password login (avoid 2FA).
"""
from __future__ import annotations

import json
import random
import re
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs, urlencode

from common.chau_ban_schema import utc_now_iso
from common.io_storage import (
    ensure_bucket,
    get_minio_client,
    list_objects_with_prefix,
    upload_binary_payload,
    upload_json_payload,
)

from final_exam_nlp_crawl_runner import (
    DEFAULT_BOTTOM_YEAR,
    DEFAULT_MATCH_SOFT_RESTART_EVERY,
    DEFAULT_PERMALINK_PAUSE_SEC,
    FB_BASE_URL,
    SCHEMA_VERSION,
    SELENIUM_RAM_HARD_STOP_MB,
    SELENIUM_RAM_SOFT_MB,
    _atomic_upload_json,
    _build_driver,
    _build_record,
    _classify_record,
    _clear_browser_caches,
    _cookies_key,
    _dedupe_image_urls,
    _drain_selenium_sessions,
    _group_root,
    _is_facebook_checkpoint,
    _is_logged_in,
    _is_content_caption,
    _is_ui_noise_label,
    _is_video_url,
    _js_heap_used_mb,
    _load_posts_index,
    _persist_index_and_logs,
    _posts_to_permalink_url,
    _progress_key,
    _ram_guard_selenium,
    _rebuild_exports,
    _run_result_key,
    _safe_quit_driver,
    _selenium_preflight,
    _settings,
    _store_post_images,
    _upsert_post,
    _year_from_posted_at,
)
from final_exam_nlp_two_phase import (
    _ensure_driver_login,
    _human_pause,
    _recheck_extract_via_permalink,
)

LOG = "[final_exam_nlp_graphql]"
LOG_DISCOVER = "[fen_gql_discover]"
LOG_ENRICH = "[fen_gql_enrich]"

# Shared CDP sniffer state (Grid se:cdp websocket) /
# State sniffer CDP dùng chung (websocket se:cdp trên Grid)
_CDP_SNIFFER: dict[str, Any] = {"thread": None, "ws": None, "events": [], "lock": None}

# Exact pagination query name — same as tests/crawl_graphql.py /
# Đúng tên query phân trang — giống tests/crawl_graphql.py
PAGINATION_QUERY = "GroupsCometFeedRegularStoriesPaginationQuery"
PAGINATION_QUERY_MARKERS = (PAGINATION_QUERY,)
DEFAULT_BATCH_TARGET = 500
DEFAULT_DISCOVER_CHUNK = 80
# Keep scrolling longer so one batch discovers dozens of posts /
# Scroll lâu hơn để một batch discover được hàng chục post
DEFAULT_SCROLL_CAPTURE_ROUNDS = 24
DEFAULT_SKIP_STREAK = 80
_ID_RE = re.compile(r"^\d+$")
_CDN_HINT = re.compile(r"(scontent|fbcdn|cdninstagram)", re.I)
_CAPTION_KEYS = (
    "text",
    "message",
    "title",
    "description",
    "plaintext",
    "story_message",
)
# Prefer real story message keys over title/description chrome /
# Ưu tiên key message bài thật hơn title/description (hay là UI)
_CAPTION_KEY_PRIORITY = {
    "story_message": 0,
    "message": 1,
    "plaintext": 2,
    "text": 3,
    "description": 8,
    "title": 9,
}
_CJK_RE = re.compile(r"[\u4e00-\u9fff\u3400-\u4dbf\u3040-\u30ff\uac00-\ud7af]")
# Skip FB static icons / tiny avatars / emoji — keep large uploads /
# Bỏ icon tĩnh / avatar nhỏ / emoji — giữ upload lớn (kể cả t39.99422)
_BAD_CDN_RE = re.compile(
    r"(static\.xx\.fbcdn\.net|rsrc\.php|"
    r"/s24x24/|/s32x32/|/s50x50/|/s60x60/|"
    r"ctp=s24x24|ctp=s32x32|ctp=s50x50|ctp=s60x60|"
    # Profile / silhouette / emoji pack (not post photos) /
    # Avatar / silhouette / emoji (không phải ảnh bài)
    r"/t39\.30808-1/|/t1\.30497-1/|/t39\.1997-)",
    re.I,
)


def _permalink_for(group_id: str, post_id: str) -> str:
    """Build group permalink URL / Tạo URL permalink của group."""
    return f"{FB_BASE_URL}/groups/{group_id}/permalink/{post_id}/"


def _pending_key(source_prefix: str, group_id: str) -> str:
    return f"{_group_root(source_prefix, group_id)}/state/pending_queue.json"


def _graphql_capture_key(source_prefix: str, group_id: str) -> str:
    return f"{_group_root(source_prefix, group_id)}/state/graphql_capture.json"


def reset_crawl_data_keep_cookies(
    *,
    bucket: str,
    source_prefix: str,
    group_id: str,
    archive: bool = True,
) -> dict[str, Any]:
    """Delete crawl progress/index/logs/exports; keep cookies + GraphQL capture.

    Xóa progress/index/logs/exports; giữ cookies.json và graphql_capture.json.
    """
    client = get_minio_client()
    ensure_bucket(client, bucket)
    root = _group_root(source_prefix, group_id).rstrip("/") + "/"
    cookies_key = _cookies_key(source_prefix, group_id)
    capture_key = _graphql_capture_key(source_prefix, group_id)
    keep_suffixes = (
        "/state/cookies.json",
        "/state/graphql_capture.json",
        "/v5/state/graphql_capture.json",
    )
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    archive_prefix = f"{_group_root(source_prefix, group_id)}/archive/{stamp}"
    deleted = 0
    archived = 0
    skipped = 0
    keys = list_objects_with_prefix(bucket, root)
    for key in keys:
        if key == cookies_key or key == capture_key or key.endswith(keep_suffixes):
            skipped += 1
            continue
        if "/meta/" in key:
            skipped += 1
            continue
        if "/archive/" in key:
            skipped += 1
            continue
        if archive:
            dest = f"{archive_prefix}/{key[len(root):]}"
            try:
                resp = client.get_object(bucket, key)
                try:
                    data = resp.read()
                finally:
                    resp.close()
                    resp.release_conn()
                upload_binary_payload(bucket, dest, data)
                archived += 1
            except Exception as exc:
                print(
                    f"{LOG} archive_skip key={key} err={type(exc).__name__}",
                    flush=True,
                )
        try:
            client.remove_object(bucket, key)
            deleted += 1
        except Exception as exc:
            print(f"{LOG} delete_fail key={key} err={type(exc).__name__}", flush=True)
    result = {
        "deleted": deleted,
        "archived": archived,
        "skipped": skipped,
        "cookies_key": cookies_key,
        "capture_key": capture_key,
        "archive_prefix": archive_prefix if archive else "",
    }
    print(f"{LOG} reset_crawl_data {result}", flush=True)
    return result


def _walk_graphql(obj: Any, found: list, feed_pi: list, any_pi: list, in_feed: bool = False) -> None:
    """Walk GraphQL JSON for post nodes + page_info / Duyệt JSON lấy post + page_info."""
    if isinstance(obj, dict):
        if "post_id" in obj and _ID_RE.match(str(obj.get("post_id", ""))):
            found.append(obj)
        pi = obj.get("page_info")
        if isinstance(pi, dict) and ("end_cursor" in pi or "has_next_page" in pi):
            (feed_pi if in_feed else any_pi).append(pi)
        for key, value in obj.items():
            _walk_graphql(value, found, feed_pi, any_pi, in_feed or key == "group_feed")
    elif isinstance(obj, list):
        for value in obj:
            _walk_graphql(value, found, feed_pi, any_pi, in_feed)


def _belongs_to_group(node: dict, group_id: str) -> bool:
    blob = json.dumps(node)[:4000]
    other = re.findall(
        r'"(?:owning_profile|to|group|target_group)"\s*:\s*\{[^{}]*"id"\s*:\s*"(\d+)"',
        blob,
    )
    return not other or group_id in other


def _caption_sort_key(item: tuple[int, str]) -> tuple:
    """Rank captions: CJK first, then key priority, then length.
    Xếp caption: CJK trước, rồi độ ưu tiên key, rồi độ dài.
    """
    key_pri, text = item
    has_cjk = 0 if _CJK_RE.search(text) else 1
    return (has_cjk, key_pri, -len(text))


def _extract_caption_from_node(node: dict) -> str:
    """Best-effort caption from GraphQL story node / Caption tốt nhất từ node GraphQL."""
    texts: list[tuple[int, str]] = []

    def walk(obj: Any, depth: int = 0) -> None:
        if depth > 12 or len(texts) >= 24:
            return
        if isinstance(obj, dict):
            for key, value in obj.items():
                lk = str(key).lower()
                if lk in _CAPTION_KEYS and isinstance(value, str):
                    cleaned = value.strip()
                    if cleaned and len(cleaned) >= 2 and not _is_ui_noise_label(cleaned):
                        texts.append((_CAPTION_KEY_PRIORITY.get(lk, 5), cleaned))
                elif lk in _CAPTION_KEYS and isinstance(value, dict):
                    walk(value, depth + 1)
                else:
                    walk(value, depth + 1)
        elif isinstance(obj, list):
            for item in obj[:40]:
                walk(item, depth + 1)

    walk(node)
    if not texts:
        return ""
    # Prefer CJK story text (calligraphy posts) over VN group chrome /
    # Ưu tiên text Hán của bài thư pháp hơn chrome nhóm tiếng Việt
    ranked = sorted(set(texts), key=_caption_sort_key)
    best = ranked[0][1].strip()
    # Reject if still not real content / Từ chối nếu vẫn không phải nội dung thật
    if not _is_content_caption(best):
        for _, cand in ranked:
            if _is_content_caption(cand):
                return cand.strip()
        return ""
    return best


def _is_usable_cdn_image(url: str) -> bool:
    """True for real post CDN photos, not icons/avatars.
    True với ảnh CDN bài thật, không phải icon/avatar.
    """
    if not url or not isinstance(url, str):
        return False
    if not _CDN_HINT.search(url):
        return False
    if _is_video_url(url) or "video" in url.lower():
        return False
    if _BAD_CDN_RE.search(url):
        return False
    return True


def _extract_image_urls_from_node(node: dict) -> list[str]:
    """Collect CDN image URLs from node / Gom URL ảnh CDN từ node."""
    urls: list[str] = []

    def walk(obj: Any, depth: int = 0) -> None:
        if depth > 14 or len(urls) >= 24:
            return
        if isinstance(obj, dict):
            for key, value in obj.items():
                lk = str(key).lower()
                if lk in {"uri", "url", "src", "image_url", "preview_image"} and isinstance(value, str):
                    if _is_usable_cdn_image(value):
                        urls.append(value)
                else:
                    walk(value, depth + 1)
        elif isinstance(obj, list):
            for item in obj[:50]:
                walk(item, depth + 1)

    walk(node)
    return _dedupe_image_urls(urls)


def _extract_posted_at_from_node(node: dict) -> str | None:
    """Unix creation_time → ISO / Đổi creation_time unix sang ISO."""
    found: list[int] = []

    def walk(obj: Any, depth: int = 0) -> None:
        if depth > 10 or found:
            return
        if isinstance(obj, dict):
            for key, value in obj.items():
                lk = str(key).lower()
                if lk in {"creation_time", "created_time", "publish_time", "timestamp"}:
                    try:
                        ts = int(value)
                        if ts > 1_000_000_000:
                            found.append(ts)
                            return
                    except (TypeError, ValueError):
                        pass
                walk(value, depth + 1)
        elif isinstance(obj, list):
            for item in obj[:30]:
                walk(item, depth + 1)

    walk(node)
    if not found:
        return None
    return datetime.fromtimestamp(found[0], tz=timezone.utc).isoformat()


def _extract_author_from_node(node: dict) -> str:
    """Best-effort author name / Tên author tốt nhất."""
    for key in ("name", "short_name", "username"):
        value = node.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    actors = node.get("actors") or node.get("actors_list")
    if isinstance(actors, list) and actors:
        name = (actors[0] or {}).get("name") if isinstance(actors[0], dict) else ""
        if isinstance(name, str) and name.strip():
            return name.strip()
    blob = json.dumps(node)
    match = re.search(r'"name"\s*:\s*"([^"]{2,80})"', blob)
    return match.group(1) if match else ""


def _node_to_candidate(node: dict, group_id: str) -> dict[str, Any] | None:
    """Normalize GraphQL node into a crawl candidate / Chuẩn hoá node thành candidate."""
    pid = str(node.get("post_id") or "").strip()
    if not _ID_RE.match(pid):
        return None
    if not _belongs_to_group(node, group_id):
        return None
    caption = _extract_caption_from_node(node)
    images = _extract_image_urls_from_node(node)
    posted_at = _extract_posted_at_from_node(node)
    author = _extract_author_from_node(node)
    permalink = _permalink_for(group_id, pid)
    complete = bool(caption.strip()) and bool(images)
    return {
        "post_id": pid,
        "permalink": permalink,
        "caption": caption,
        "image_urls": images,
        "posted_at": posted_at,
        "author": author,
        "graphql_complete": complete,
        "source": "graphql",
    }


def ingest_graphql_text(
    text: str,
    *,
    group_id: str,
    seen_page_ids: set[str],
) -> tuple[list[dict[str, Any]], dict | None, str | None]:
    """Parse GraphQL body → candidates + page_info.

    Parse body GraphQL → candidates + page_info.
    ``seen_page_ids`` only dedupes within the current page walk, not global enriched.
    """
    candidates: list[dict[str, Any]] = []
    page_info = None
    err = None
    for chunk in (text or "").split("\n"):
        chunk = chunk.strip()
        if not chunk.startswith("{"):
            continue
        try:
            payload = json.loads(chunk)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and payload.get("errors"):
            err = str(payload["errors"][0].get("message") or payload["errors"][0])[:150]
        found, feed_pi, any_pi = [], [], []
        _walk_graphql(payload, found, feed_pi, any_pi)
        for node in found:
            cand = _node_to_candidate(node, group_id)
            if not cand:
                continue
            pid = cand["post_id"]
            if pid in seen_page_ids:
                continue
            seen_page_ids.add(pid)
            candidates.append(cand)
        for pi in feed_pi or [p for p in any_pi if "end_cursor" in p]:
            page_info = pi
    return candidates, page_info, err


def _post_data_is_feed_pagination(post_data: str) -> bool:
    """True only for GroupsCometFeedRegularStoriesPaginationQuery (crawl_graphql.py).

    Chỉ True với GroupsCometFeedRegularStoriesPaginationQuery — giống test.
    """
    if not post_data:
        return False
    # Strict: same check as tests/crawl_graphql.py on_request /
    # Nghiêm: cùng check với tests/crawl_graphql.py on_request
    return PAGINATION_QUERY in post_data


def _enable_network_cdp(driver) -> None:
    """Best-effort Network.enable for response bodies / Bật Network CDP lấy body response."""
    try:
        # maxResourceBufferSize helps keep GraphQL bodies /
        # maxResourceBufferSize giúp giữ body GraphQL
        driver.execute_cdp_cmd(
            "Network.enable",
            {
                "maxTotalBufferSize": 100_000_000,
                "maxResourceBufferSize": 50_000_000,
                "maxPostDataSize": 5_000_000,
            },
        )
    except Exception:
        try:
            driver.execute_cdp_cmd("Network.enable", {})
        except Exception as exc:
            print(f"{LOG_DISCOVER} Network.enable skipped: {type(exc).__name__}", flush=True)
    try:
        driver.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
    except Exception:
        pass
    # Start Grid CDP websocket sniffer — get_log is gone on Selenium 4 W3C /
    # Bật sniffer websocket CDP Grid — get_log đã mất trên Selenium 4 W3C
    _start_cdp_graphql_sniffer(driver)


def _start_cdp_graphql_sniffer(driver) -> bool:
    """Listen GraphQL request+response via se:cdp (like Playwright on_request/on_response).

    Lắng GraphQL request+response qua se:cdp (giống Playwright on_request/on_response).
    """
    import base64
    import threading

    global _CDP_SNIFFER
    caps = getattr(driver, "capabilities", None) or {}
    ws_url = caps.get("se:cdp")
    if not isinstance(ws_url, str) or not ws_url.startswith("ws"):
        chrome = caps.get("chrome") or {}
        if isinstance(chrome, dict):
            ws_url = chrome.get("se:cdp")
    if not isinstance(ws_url, str) or not ws_url.startswith("ws"):
        print(
            f"{LOG_DISCOVER} cdp_sniffer skip — no se:cdp in caps keys={list(caps)[:20]}",
            flush=True,
        )
        return False

    try:
        from websocket import WebSocketApp
    except ImportError:
        print(f"{LOG_DISCOVER} cdp_sniffer skip — websocket-client missing", flush=True)
        return False

    _stop_cdp_graphql_sniffer()
    lock = threading.Lock()
    events: list[dict[str, Any]] = []
    bodies: list[str] = []
    pending_gql: dict[str, str] = {}
    pending_get_body: dict[int, str] = {}
    state: dict[str, Any] = {
        "ws": None,
        "ready": False,
        "id": 0,
        "attached_ids": set(),
        "session_id": None,
        "msg_count": 0,
        "methods": {},
        "rpc_ok": 0,
    }

    def _send(
        ws, method: str, params: dict | None = None, session_id: str | None = None
    ) -> int:
        state["id"] += 1
        payload: dict[str, Any] = {
            "id": state["id"],
            "method": method,
            "params": params or {},
        }
        sid = session_id or state.get("session_id")
        if sid:
            payload["sessionId"] = sid
        ws.send(json.dumps(payload))
        return int(state["id"])

    def on_open(ws) -> None:
        try:
            _send(
                ws,
                "Target.setAutoAttach",
                {
                    "autoAttach": True,
                    "flatten": True,
                    "waitForDebuggerOnStart": False,
                },
            )
            _send(ws, "Target.getTargets", {})
            _send(
                ws,
                "Network.enable",
                {
                    "maxTotalBufferSize": 100_000_000,
                    "maxResourceBufferSize": 50_000_000,
                    "maxPostDataSize": 5_000_000,
                },
            )
            state["ready"] = True
            print(f"{LOG_DISCOVER} cdp_sniffer open url={ws_url[:80]}…", flush=True)
        except Exception as exc:
            print(f"{LOG_DISCOVER} cdp_sniffer open_err={type(exc).__name__}", flush=True)

    def on_message(ws, message: str) -> None:
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return
        method = data.get("method") or ""
        params = data.get("params") or {}
        with lock:
            state["msg_count"] = int(state.get("msg_count") or 0) + 1
            if method:
                counts = state.setdefault("methods", {})
                counts[method] = int(counts.get(method) or 0) + 1
            elif "id" in data:
                state["rpc_ok"] = int(state.get("rpc_ok") or 0) + 1

        rpc_id = data.get("id")
        if rpc_id is not None and "result" in data and rpc_id in pending_get_body:
            req_id = pending_get_body.pop(rpc_id, "")
            result = data.get("result") or {}
            body = str(result.get("body") or "")
            if result.get("base64Encoded"):
                try:
                    body = base64.b64decode(body).decode("utf-8", errors="replace")
                except Exception:
                    body = ""
            if body and body.lstrip().startswith("{"):
                with lock:
                    bodies.append(body)
                    if len(bodies) > 40:
                        del bodies[:-40]
                print(
                    f"{LOG_DISCOVER} cdp_ws graphql body len={len(body)} "
                    f"req={req_id[:8]}…",
                    flush=True,
                )

        if "result" in data and isinstance(data.get("result"), dict):
            targets = (data["result"] or {}).get("targetInfos") or []
            for t in targets:
                if not isinstance(t, dict) or t.get("type") != "page":
                    continue
                tid = t.get("targetId")
                if not tid or tid in state.get("attached_ids", set()):
                    continue
                state.setdefault("attached_ids", set()).add(tid)
                state["id"] += 1
                try:
                    ws.send(
                        json.dumps(
                            {
                                "id": state["id"],
                                "method": "Target.attachToTarget",
                                "params": {"targetId": tid, "flatten": True},
                            }
                        )
                    )
                except Exception:
                    pass

        if method == "Target.attachedToTarget":
            session_id = params.get("sessionId") or ""
            if session_id:
                state["session_id"] = session_id
                _send(
                    ws,
                    "Network.enable",
                    {"maxPostDataSize": 5_000_000},
                    session_id=session_id,
                )
            return

        if method == "Network.requestWillBeSent":
            req = params.get("request") or {}
            url = str(req.get("url") or "")
            if "/api/graphql" not in url:
                return
            post_data = str(req.get("postData") or "")
            req_id = str(params.get("requestId") or "")
            if req_id:
                with lock:
                    pending_gql[req_id] = url
            if PAGINATION_QUERY not in post_data:
                return
            entry = {
                "url": url,
                "post_data": post_data,
                "headers": req.get("headers") or {},
                "request_id": req_id,
            }
            with lock:
                events.append(entry)
                if len(events) > 80:
                    del events[:-80]
            return

        if method == "Network.responseReceived":
            req_id = str(params.get("requestId") or "")
            url = str(((params.get("response") or {}).get("url")) or "")
            if req_id and "/api/graphql" in (url or pending_gql.get(req_id, "")):
                with lock:
                    pending_gql[req_id] = url or pending_gql.get(req_id, "")
            return

        if method == "Network.loadingFinished":
            req_id = str(params.get("requestId") or "")
            if not req_id:
                return
            with lock:
                if req_id not in pending_gql:
                    return
            rpc = _send(ws, "Network.getResponseBody", {"requestId": req_id})
            pending_get_body[rpc] = req_id
            return

    def on_error(ws, error) -> None:
        print(f"{LOG_DISCOVER} cdp_sniffer err={error!s}"[:180], flush=True)

    def on_close(ws, status_code, msg) -> None:
        state["ready"] = False
        print(f"{LOG_DISCOVER} cdp_sniffer closed code={status_code}", flush=True)

    ws = WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    state["ws"] = ws

    def _run() -> None:
        try:
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as exc:
            print(f"{LOG_DISCOVER} cdp_sniffer run_err={type(exc).__name__}", flush=True)

    thread = threading.Thread(target=_run, name="fen-cdp-gql", daemon=True)
    _CDP_SNIFFER = {
        "thread": thread,
        "ws": ws,
        "events": events,
        "bodies": bodies,
        "lock": lock,
        "state": state,
        "url": ws_url,
    }
    thread.start()
    for _ in range(20):
        if state.get("ready"):
            break
        time.sleep(0.1)
    print(
        f"{LOG_DISCOVER} cdp_sniffer started ready={bool(state.get('ready'))}",
        flush=True,
    )
    return True


def _stop_cdp_graphql_sniffer() -> None:
    """Stop CDP websocket sniffer / Dừng sniffer websocket CDP."""
    global _CDP_SNIFFER
    ws = (_CDP_SNIFFER or {}).get("ws")
    if ws is not None:
        try:
            ws.close()
        except Exception:
            pass
    _CDP_SNIFFER = {"thread": None, "ws": None, "events": [], "bodies": [], "lock": None}


def _drain_cdp_sniffer_capture(captured: dict[str, Any]) -> int:
    """Apply sniffer GraphQL posts into captured form; return event count.

    Áp post GraphQL từ sniffer vào form capture; trả số event.
    """
    lock = (_CDP_SNIFFER or {}).get("lock")
    events = (_CDP_SNIFFER or {}).get("events") or []
    if lock is None:
        return 0
    with lock:
        batch = list(events)
        events.clear()
    for entry in batch:
        if captured.get("form"):
            break
        url = str(entry.get("url") or "")
        post_data = str(entry.get("post_data") or "")
        if not post_data or "/api/graphql" not in url:
            continue
        if not _post_data_is_feed_pagination(post_data):
            continue
        form = {k: v[0] for k, v in parse_qs(post_data).items()}
        if not form.get("doc_id"):
            continue
        headers = {
            k: v
            for k, v in (entry.get("headers") or {}).items()
            if str(k).lower().startswith("x-") or str(k).lower() == "content-type"
        }
        captured.update(
            {"url": url.split("?")[0], "form": form, "headers": headers}
        )
        print(
            f"{LOG_DISCOVER} Captured pagination query via cdp_ws "
            f"friendly={form.get('fb_api_req_friendly_name')}",
            flush=True,
        )
    if batch:
        print(
            f"{LOG_DISCOVER} cdp_sniffer_drain events={len(batch)} "
            f"captured={bool(captured.get('form'))}",
            flush=True,
        )
    return len(batch)


def _drain_cdp_sniffer_bodies() -> list[str]:
    """Drain GraphQL response bodies (Playwright on_response equivalent).

    Lấy body response GraphQL (tương đương Playwright on_response).
    """
    lock = (_CDP_SNIFFER or {}).get("lock")
    bodies = (_CDP_SNIFFER or {}).get("bodies") or []
    if lock is None:
        return []
    with lock:
        out = list(bodies)
        bodies.clear()
    if out:
        print(f"{LOG_DISCOVER} cdp_ws bodies={len(out)}", flush=True)
    return out


def _install_fetch_hook(driver) -> None:
    """Hook fetch+XHR to capture GraphQL (when perf logs are empty).

    Hook fetch+XHR để bắt GraphQL (khi performance log trống trên Grid).
    ALWAYS re-wrap after navigation — FB overwrites window.fetch /
    LUÔN gắn lại sau navigation — FB ghi đè window.fetch.
    """
    script = r"""
    if (!window.__fenGql) {
      window.__fenGql = {entries: [], total: 0};
    }
    if (typeof window.__fenGql.total !== 'number') window.__fenGql.total = 0;
    function bodyToString(body) {
      try {
        if (body == null) return '';
        if (typeof body === 'string') return body;
        if (typeof URLSearchParams !== 'undefined' && body instanceof URLSearchParams) {
          return body.toString();
        }
        if (typeof FormData !== 'undefined' && body instanceof FormData) {
          const parts = [];
          body.forEach(function(v, k) {
            parts.push(encodeURIComponent(k) + '=' + encodeURIComponent(String(v)));
          });
          return parts.join('&');
        }
        if (typeof Blob !== 'undefined' && body instanceof Blob) return '';
        return String(body);
      } catch (e) { return ''; }
    }
    function pushEntry(url, body, text) {
      try {
        const u = String(url || '');
        if (!u || (u.indexOf('graphql') === -1 && u.indexOf('/api/graphql') === -1)) return;
        window.__fenGql.total = (window.__fenGql.total || 0) + 1;
        window.__fenGql.entries.push({
          url: u,
          body: body ? String(body).slice(0, 80000) : '',
          text: text ? String(text).slice(0, 2500000) : '',
        });
        if (window.__fenGql.entries.length > 40) {
          window.__fenGql.entries.splice(0, window.__fenGql.entries.length - 40);
        }
      } catch (e) {}
    }
    // Force re-wrap every call — FB replaces fetch after our first hook /
    // Buộc gắn lại mỗi lần — FB thay fetch sau hook đầu
    const baseFetch = window.__fenOrigFetch || window.fetch;
    window.__fenOrigFetch = baseFetch;
    window.fetch = async function(input, init) {
      let url = '';
      let reqBody = '';
      try {
        if (typeof input === 'string') {
          url = input;
          reqBody = bodyToString(init && init.body);
        } else if (input && typeof Request !== 'undefined' && input instanceof Request) {
          url = input.url || '';
          try { reqBody = await input.clone().text(); }
          catch (e) { reqBody = bodyToString(init && init.body); }
        } else if (input && input.url) {
          url = input.url;
          reqBody = bodyToString(init && init.body);
        }
      } catch (e) {}
      const res = await baseFetch.apply(this, arguments);
      try {
        const clone = res.clone();
        const text = await clone.text();
        pushEntry(url, reqBody, text);
      } catch (e) {}
      return res;
    };
    if (!window.__fenXhrHooked) {
      window.__fenXhrHooked = true;
      const OrigXHR = window.XMLHttpRequest;
      function HookedXHR() {
        const xhr = new OrigXHR();
        let url = '';
        const open = xhr.open;
        const send = xhr.send;
        xhr.open = function(m, u) {
          url = u;
          return open.apply(this, arguments);
        };
        xhr.send = function(body) {
          const bodyStr = bodyToString(body);
          this.addEventListener('load', function() {
            try { pushEntry(url, bodyStr, this.responseText || ''); } catch (e) {}
          });
          return send.apply(this, arguments);
        };
        return xhr;
      }
      HookedXHR.prototype = OrigXHR.prototype;
      window.XMLHttpRequest = HookedXHR;
    }
    window.__fenGqlHooked = true;
    return true;
    """
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument", {"source": script}
        )
    except Exception as exc:
        print(f"{LOG_DISCOVER} fetch_hook_cdp_skip={type(exc).__name__}", flush=True)
    try:
        ok = driver.execute_script(script)
        print(f"{LOG_DISCOVER} fetch_hook installed={bool(ok)} (force-rewrap)", flush=True)
    except Exception as exc:
        print(f"{LOG_DISCOVER} fetch_hook_fail={type(exc).__name__}", flush=True)


def _bootstrap_capture_from_page(driver, captured: dict[str, Any], group_id: str) -> bool:
    """Build GraphQL form from page tokens when network capture is empty.

    Dự form GraphQL từ token trong page khi network capture trống.
    """
    if captured.get("form"):
        return True
    script = r"""
    const groupId = arguments[0];
    const html = document.documentElement ? document.documentElement.innerHTML : '';
    const out = {
      doc_id: null,
      fb_dtsg: null,
      lsd: null,
      jazoest: null,
      friendly: null,
      c_user: null,
      html_len: html.length,
      feed_hits: 0,
      sample: '',
    };
    let m = html.match(/"DTSGInitialData",\[\],\{"token":"([^"]+)"/);
    if (!m) m = html.match(/name="fb_dtsg"\s+value="([^"]+)"/);
    if (!m) m = html.match(/"token":"([^"]+)"[\s\S]{0,120}"DTSGInitialData"/);
    if (!m) m = html.match(/DTSGInitialData[\s\S]{0,80}"token":"([^"]+)"/);
    if (m) out.fb_dtsg = m[1];
    m = html.match(/\["LSD",\[\],\{"token":"([^"]+)"\]/);
    if (!m) m = html.match(/name="lsd"\s+value="([^"]+)"/);
    if (!m) m = html.match(/"LSD"[^\]]*?"token":"([^"]+)"/);
    if (m) out.lsd = m[1];
    m = html.match(/jazoest=(\d+)/);
    if (!m) m = html.match(/name="jazoest"\s+value="(\d+)"/);
    if (m) out.jazoest = m[1];
    const names = [
      'GroupsCometFeedRegularStoriesPaginationQuery',
      'GroupsCometFeedPaginationQuery',
      'GroupsCometFeedStoriesPaginationQuery',
      'GroupsCometFeedRegularStoriesQuery',
      'GroupsCometFeedFocusedStoriesPaginationQuery',
    ];
    for (const name of names) {
      const patterns = [
        new RegExp(name + '[\\s\\S]{0,800}?"doc_id":"(\\d+)"'),
        new RegExp('"doc_id":"(\\d+)"[\\s\\S]{0,800}?' + name),
        new RegExp(name + '[\\s\\S]{0,800}?"id":"(\\d{15,20})"'),
        new RegExp('"id":"(\\d{15,20})"[\\s\\S]{0,800}?' + name),
        new RegExp('"' + name + '"[^\\]]{0,200}"id":"(\\d{15,20})"'),
      ];
      for (const re of patterns) {
        m = html.match(re);
        if (m) { out.doc_id = m[1]; out.friendly = name; break; }
      }
      if (out.doc_id) break;
    }
    // Scan every GroupsCometFeed* window for nearby id/doc_id /
    // Quét mọi cửa sổ GroupsCometFeed* tìm id/doc_id gần đó
    if (!out.doc_id) {
      let pos = 0;
      let hits = 0;
      while (hits < 30 && (pos = html.indexOf('GroupsCometFeed', pos)) !== -1) {
        hits += 1;
        const slice = html.slice(Math.max(0, pos - 120), pos + 900);
        if (!out.sample) out.sample = slice.slice(0, 180);
        const dm = slice.match(/"doc_id":"(\d+)"/) || slice.match(/"id":"(\d{15,20})"/);
        const nm = slice.match(/GroupsCometFeed[A-Za-z0-9]+/);
        if (dm) {
          out.doc_id = dm[1];
          out.friendly = nm ? nm[0] : 'GroupsCometFeedRegularStoriesPaginationQuery';
          break;
        }
        pos += 14;
      }
      out.feed_hits = hits;
    }
    // Relay preloader: {"id":"DOC","name":"GroupsCometFeed..."} /
    // Relay preloader: {"id":"DOC","name":"GroupsCometFeed..."}
    if (!out.doc_id) {
      const pre = html.match(
        /"id"\s*:\s*"(\d{15,20})"\s*,\s*"name"\s*:\s*"(GroupsCometFeed[A-Za-z0-9]+(?:Pagination)?Query)"/
      ) || html.match(
        /"name"\s*:\s*"(GroupsCometFeed[A-Za-z0-9]+(?:Pagination)?Query)"\s*,\s*"id"\s*:\s*"(\d{15,20})"/
      ) || html.match(
        /"name"\s*:\s*"(GroupsCometFeed[A-Za-z0-9]+(?:Pagination)?Query)"[\s\S]{0,200}?"(?:doc_)?id"\s*:\s*"(\d{15,20})"/
      );
      if (pre) {
        if (pre[1] && /^\d+$/.test(pre[1])) {
          out.doc_id = pre[1];
          out.friendly = pre[2];
        } else {
          out.friendly = pre[1];
          out.doc_id = pre[2];
        }
      }
    }
    // Last resort: any long doc_id near "PaginationQuery" in group feed context /
    // Phương án cuối: doc_id dài gần PaginationQuery trong ngữ cảnh feed
    if (!out.doc_id) {
      const all = [...html.matchAll(/"(GroupsComet[A-Za-z0-9]*PaginationQuery)"/g)];
      for (const hit of all.slice(0, 20)) {
        const i = hit.index || 0;
        const slice = html.slice(Math.max(0, i - 400), i + 400);
        const dm = slice.match(/"(?:doc_)?id"\s*:\s*"(\d{15,20})"/);
        if (dm) {
          out.doc_id = dm[1];
          out.friendly = hit[1];
          break;
        }
      }
    }
    try {
      const c = document.cookie.match(/(?:^|;\\s*)c_user=(\\d+)/);
      if (c) out.c_user = c[1];
    } catch (e) {}
    return out;
    """
    try:
        meta = driver.execute_script(script, group_id) or {}
    except Exception as exc:
        print(f"{LOG_DISCOVER} bootstrap_page_err={type(exc).__name__}", flush=True)
        return False
    doc_id = str(meta.get("doc_id") or "").strip()
    fb_dtsg = str(meta.get("fb_dtsg") or "").strip()
    friendly = str(
        meta.get("friendly") or "GroupsCometFeedRegularStoriesPaginationQuery"
    ).strip()
    # Reject false positives: group_id mistaken for doc_id /
    # Từ chối false positive: nhầm group_id thành doc_id
    if doc_id == str(group_id) or (doc_id.isdigit() and len(doc_id) < 15):
        print(
            f"{LOG_DISCOVER} bootstrap_page reject bad doc_id={doc_id!r} "
            f"(looks like group_id) friendly={friendly}",
            flush=True,
        )
        doc_id = ""
    if friendly and "Pagination" not in friendly and "FeedRegularStories" not in friendly:
        print(
            f"{LOG_DISCOVER} bootstrap_page reject non-pagination friendly={friendly}",
            flush=True,
        )
        doc_id = ""
    if not doc_id or not fb_dtsg:
        print(
            f"{LOG_DISCOVER} bootstrap_page incomplete "
            f"doc_id={bool(doc_id)} dtsg={bool(fb_dtsg)} "
            f"html_len={meta.get('html_len')} feed_hits={meta.get('feed_hits')} "
            f"sample={(meta.get('sample') or '')[:120]!r}",
            flush=True,
        )
        return False
    lsd = str(meta.get("lsd") or "").strip()
    jazoest = str(meta.get("jazoest") or "").strip() or "2"
    c_user = str(meta.get("c_user") or "").strip()
    variables = {
        "count": 5,
        "cursor": None,
        "feedLocation": "GROUP",
        "groupID": str(group_id),
        "scale": 1.5,
        "id": str(group_id),
    }
    form = {
        "fb_dtsg": fb_dtsg,
        "jazoest": jazoest,
        "fb_api_caller_class": "RelayModern",
        "fb_api_req_friendly_name": friendly,
        "variables": json.dumps(variables, separators=(",", ":")),
        "server_timestamps": "true",
        "doc_id": doc_id,
    }
    if lsd:
        form["lsd"] = lsd
    if c_user:
        form["av"] = c_user
        form["__user"] = c_user
    captured.update(
        {
            "url": "https://www.facebook.com/api/graphql/",
            "form": form,
            "headers": {
                "content-type": "application/x-www-form-urlencoded",
                "x-fb-friendly-name": friendly,
            },
        }
    )
    print(
        f"{LOG_DISCOVER} bootstrap_page capture ok friendly={friendly} "
        f"doc_id={doc_id[:10]}…",
        flush=True,
    )
    return True


def _dom_harvest_candidates(driver, group_id: str) -> list[dict[str, Any]]:
    """Bulk harvest post_id + caption + images from embedded page JSON.

    Harvest hàng loạt post_id + caption + ảnh từ JSON nhúng trong page
    (không cần performance log / GraphQL network).
    """
    script = r"""
    const groupId = arguments[0];
    const html = document.documentElement ? document.documentElement.innerHTML : '';
    const posts = {};
    const rePost = /"post_id"\s*:\s*"(\d{10,})"/g;
    let m;
    while ((m = rePost.exec(html)) !== null) {
      const pid = m[1];
      if (posts[pid]) continue;
      const start = Math.max(0, m.index - 12000);
      const end = Math.min(html.length, m.index + 28000);
      const slice = html.slice(start, end);
      // Prefer slices that mention this group / Ưu tiên đoạn có group id
      const groupHint = slice.indexOf(groupId) !== -1;
      let caption = '';
      const textRe = /"text"\s*:\s*"((?:\\.|[^"\\])*)"/g;
      const texts = [];
      let tm;
      while ((tm = textRe.exec(slice)) !== null) {
        let raw = tm[1];
        try { raw = JSON.parse('"' + raw + '"'); } catch (e) {}
        raw = String(raw || '').trim();
        if (raw.length < 12) continue;
        if (/^(Like|Comment|Share|Thích|Bình luận|Chia sẻ)$/i.test(raw)) continue;
        texts.push(raw);
      }
      texts.sort((a, b) => b.length - a.length);
      caption = texts[0] || '';
      const images = [];
      const uriRe = /https:\\\/\\\/scontent[^"\\s]+|https:\/\/scontent[^"\\s]+/g;
      let um;
      while ((um = uriRe.exec(slice)) !== null) {
        let u = um[0].replace(/\\\//g, '/').replace(/\\u003d/g, '=').replace(/&amp;/g, '&');
        if (u.indexOf('video') !== -1) continue;
        if (images.indexOf(u) === -1) images.push(u);
        if (images.length >= 8) break;
      }
      let postedAt = null;
      const ct = slice.match(/"creation_time"\s*:\s*(\d{9,12})/);
      if (ct) {
        try {
          postedAt = new Date(parseInt(ct[1], 10) * 1000).toISOString();
        } catch (e) {}
      }
      let author = '';
      const nameM = slice.match(/"name"\s*:\s*"((?:\\.|[^"\\]){2,80})"/);
      if (nameM) {
        try { author = JSON.parse('"' + nameM[1] + '"'); } catch (e) { author = nameM[1]; }
      }
      const complete = caption.length >= 12 && images.length > 0;
      posts[pid] = {
        post_id: pid,
        caption: caption,
        image_urls: images,
        posted_at: postedAt,
        author: author || '',
        graphql_complete: complete,
        group_hint: groupHint,
        source: 'html_relay',
      };
      if (Object.keys(posts).length >= 120) break;
    }
    // Also collect permalink href ids / Thêm id từ permalink href
    const hrefRe = /\/(?:permalink|posts)\/(\d{10,})/g;
    let hm;
    const hrefHtml = Array.from(document.querySelectorAll('a[href]'))
      .map(a => a.href || '')
      .join('\n');
    while ((hm = hrefRe.exec(hrefHtml)) !== null) {
      const pid = hm[1];
      if (!posts[pid]) {
        posts[pid] = {
          post_id: pid,
          caption: '',
          image_urls: [],
          posted_at: null,
          author: '',
          graphql_complete: false,
          group_hint: true,
          source: 'dom_href',
        };
      }
    }
    return {
      posts: Object.values(posts),
      articles: document.querySelectorAll('[role="article"]').length,
      html_len: html.length,
      url: location.href,
    };
    """
    try:
        payload = driver.execute_script(script, group_id) or {}
    except Exception as exc:
        print(f"{LOG_DISCOVER} rich_harvest_err={type(exc).__name__}", flush=True)
        return []
    rows = payload.get("posts") or []
    complete_n = sum(1 for r in rows if r.get("graphql_complete"))
    print(
        f"{LOG_DISCOVER} rich_harvest posts={len(rows)} complete={complete_n} "
        f"articles={payload.get('articles')} html_len={payload.get('html_len')} "
        f"url={str(payload.get('url') or '')[:100]}",
        flush=True,
    )
    out: list[dict[str, Any]] = []
    for row in rows:
        pid = str((row or {}).get("post_id") or "").strip()
        if not _ID_RE.match(pid) or len(pid) < 10:
            continue
        caption = str(row.get("caption") or "").strip()
        # Drop group pinned-rules text mistaken as caption /
        # Bỏ text luật ghim nhóm bị nhầm thành caption
        if caption and _is_ui_noise_label(caption):
            caption = ""
        images = _dedupe_image_urls(
            [u for u in (row.get("image_urls") or []) if not _is_video_url(str(u))]
        )
        posted_at = row.get("posted_at") if isinstance(row.get("posted_at"), str) else None
        complete = bool(caption) and bool(images)
        out.append(
            {
                "post_id": pid,
                "permalink": _permalink_for(group_id, pid),
                "caption": caption,
                "image_urls": images,
                "posted_at": posted_at,
                "author": str(row.get("author") or ""),
                "graphql_complete": complete,
                "source": str(row.get("source") or "html_relay"),
            }
        )
    # Prefer complete + group-hinted first / Ưu tiên đủ field trước
    out.sort(key=lambda c: (0 if c.get("graphql_complete") else 1, c["post_id"]))
    return out


def _http_graphql_replay(
    cookies: list[dict[str, Any]],
    captured: dict[str, Any],
    cursor: str | None,
) -> tuple[int, str]:
    """Paginate GraphQL over HTTP with session cookies (no Chrome heap).

    Phân trang GraphQL qua HTTP bằng cookie (không tốn heap Chrome).
    """
    import requests

    form = dict(captured.get("form") or {})
    url = str(captured.get("url") or "")
    if not form or not url:
        return 0, "missing_capture"
    try:
        variables = json.loads(form.get("variables") or "{}")
    except json.JSONDecodeError:
        return 0, "bad_variables"
    variables["cursor"] = cursor
    form["variables"] = json.dumps(variables, separators=(",", ":"))
    sess = requests.Session()
    for cookie in cookies or []:
        name = str(cookie.get("name") or "")
        value = str(cookie.get("value") or "")
        if not name:
            continue
        sess.cookies.set(
            name,
            value,
            domain=str(cookie.get("domain") or ".facebook.com"),
            path=str(cookie.get("path") or "/"),
        )
    headers = {
        "content-type": "application/x-www-form-urlencoded",
        "user-agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "accept": "*/*",
        "origin": "https://www.facebook.com",
        "referer": "https://www.facebook.com/",
    }
    for key, value in (captured.get("headers") or {}).items():
        if str(key).lower().startswith("x-") or str(key).lower() == "content-type":
            headers[str(key)] = str(value)
    try:
        resp = sess.post(url, data=form, headers=headers, timeout=60)
        return int(resp.status_code), resp.text or ""
    except Exception as exc:
        return 0, f"http_err:{type(exc).__name__}"


def _drain_fetch_hook(
    driver,
    captured: dict[str, Any],
) -> list[str]:
    """Read hooked GraphQL entries; update capture form; return bodies.

    Đọc entry GraphQL đã hook; cập nhật form capture; trả bodies.
    """
    bodies: list[str] = []
    try:
        meta = driver.execute_script(
            """
            const g = window.__fenGql || {entries: [], total: 0};
            const out = g.entries.slice();
            const total = g.total || out.length;
            g.entries = [];
            return {entries: out, total: total, hooked: !!window.__fenGqlHooked};
            """
        ) or {}
    except Exception:
        return bodies
    entries = meta.get("entries") if isinstance(meta, dict) else []
    if isinstance(meta, dict):
        print(
            f"{LOG_DISCOVER} fetch_hook_drain hooked={meta.get('hooked')} "
            f"total_calls={meta.get('total')} batch={len(entries or [])}",
            flush=True,
        )
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        url = str(entry.get("url") or "")
        post_data = str(entry.get("body") or "")
        text = str(entry.get("text") or "")
        if text:
            bodies.append(text)
        is_gql = ("graphql" in url) or ("/api/graphql" in url)
        if not is_gql or not post_data or captured.get("form"):
            continue
        # Strict like tests/crawl_graphql.py: PAGINATION_QUERY in body /
        # Nghiêm như tests/crawl_graphql.py: PAGINATION_QUERY trong body
        if not _post_data_is_feed_pagination(post_data):
            continue
        form = {k: v[0] for k, v in parse_qs(post_data).items()}
        if not form.get("doc_id"):
            continue
        headers = {}
        captured.update({"url": url.split("?")[0], "form": form, "headers": headers})
        print(f"{LOG_DISCOVER} Captured pagination query via fetch_hook", flush=True)
    return bodies


def _drain_perf_capture(driver, captured: dict[str, Any], pending_req: dict[str, str]) -> None:
    """Parse performance logs for GraphQL pagination request/response.

    Đọc performance log để bắt request/response GraphQL phân trang.
    """
    try:
        entries = driver.get_log("performance")
    except Exception as exc:
        print(f"{LOG_DISCOVER} perf_log_err={type(exc).__name__}", flush=True)
        return
    gql_seen = 0
    for entry in entries:
        try:
            msg = json.loads(entry.get("message") or "{}").get("message") or {}
        except json.JSONDecodeError:
            continue
        method = msg.get("method") or ""
        params = msg.get("params") or {}
        if method == "Network.requestWillBeSent":
            req = params.get("request") or {}
            url = str(req.get("url") or "")
            if "graphql" not in url:
                continue
            gql_seen += 1
            post_data = str(req.get("postData") or "")
            # CDP sometimes omits postData — try getRequestPostData /
            # CDP đôi khi thiếu postData — thử getRequestPostData
            req_id = str(params.get("requestId") or "")
            if not post_data and req_id:
                try:
                    extra = driver.execute_cdp_cmd(
                        "Network.getRequestPostData", {"requestId": req_id}
                    )
                    post_data = str((extra or {}).get("postData") or "")
                except Exception:
                    pass
            if req_id:
                pending_req[req_id] = url
            if captured.get("form") or not post_data:
                continue
            if not _post_data_is_feed_pagination(post_data):
                continue
            form = {k: v[0] for k, v in parse_qs(post_data).items()}
            if not form.get("doc_id"):
                continue
            headers = {
                k: v
                for k, v in (req.get("headers") or {}).items()
                if str(k).lower().startswith("x-") or str(k).lower() == "content-type"
            }
            captured.update({"url": url.split("?")[0], "form": form, "headers": headers})
            print(
                f"{LOG_DISCOVER} Captured pagination query via perf_log "
                f"friendly={form.get('fb_api_req_friendly_name')}",
                flush=True,
            )
        elif method == "Network.responseReceived":
            req_id = str(params.get("requestId") or "")
            url = str(((params.get("response") or {}).get("url")) or pending_req.get(req_id) or "")
            if "graphql" not in url or not req_id:
                continue
            pending_req[req_id] = url
            gql_seen += 1
    if entries:
        print(
            f"{LOG_DISCOVER} perf_log entries={len(entries)} graphql_events={gql_seen} "
            f"captured={bool(captured.get('form'))}",
            flush=True,
        )


def _fetch_response_bodies(driver, pending_req: dict[str, str]) -> list[str]:
    """Pull GraphQL response bodies via CDP / Lấy body GraphQL qua CDP."""
    bodies: list[str] = []
    for req_id, url in list(pending_req.items()):
        if "/api/graphql" not in url:
            continue
        try:
            payload = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": req_id})
            text = str((payload or {}).get("body") or "")
            if text:
                bodies.append(text)
        except Exception:
            continue
        pending_req.pop(req_id, None)
    return bodies


def _replay_fetch(driver, captured: dict[str, Any], cursor: str | None) -> tuple[int, str]:
    """Replay pagination from inside the page / Replay phân trang trong page."""
    form = dict(captured.get("form") or {})
    if not form or not captured.get("url"):
        return 0, "missing_capture"
    variables = json.loads(form.get("variables") or "{}")
    variables["cursor"] = cursor
    form["variables"] = json.dumps(variables, separators=(",", ":"))
    headers = dict(captured.get("headers") or {})
    headers.setdefault("content-type", "application/x-www-form-urlencoded")
    script = """
    const url = arguments[0];
    const body = arguments[1];
    const headers = arguments[2];
    const done = arguments[arguments.length - 1];
    fetch(url, {method: 'POST', body, headers, credentials: 'include'})
      .then(async (r) => {
        const text = await r.text();
        done([r.status, text]);
      })
      .catch((e) => done([0, String(e)]));
    """
    try:
        result = driver.execute_async_script(
            script, captured["url"], urlencode(form), headers
        )
    except Exception as exc:
        return 0, f"replay_err:{type(exc).__name__}"
    if not isinstance(result, (list, tuple)) or len(result) < 2:
        return 0, "bad_replay_shape"
    return int(result[0] or 0), str(result[1] or "")


def _write_progress(
    *,
    bucket: str,
    source_prefix: str,
    group_id: str,
    cursor: str | None,
    backfill_cursor: str | None,
    has_next: bool,
    known: set[str],
    enriched: set[str],
    pending: list[dict[str, Any]],
    stop_reason: str,
    should_continue: bool,
    stats: dict[str, Any],
    run_id: str,
    last_enriched_post_id: str = "",
    last_enriched_permalink: str = "",
    mode: str = "catchup",
) -> None:
    """Persist GraphQL-first checkpoint for rollover resume.

    Ghi checkpoint GraphQL-first để resume khi rollover.
    """
    payload = {
        "schema_version": SCHEMA_VERSION,
        "crawl_mode": "graphql_batch",
        "phase": "graphql_first",
        "mode": mode,
        "graphql_cursor": cursor,
        "backfill_cursor": backfill_cursor,
        "has_next": has_next,
        "last_enriched_post_id": last_enriched_post_id or None,
        "last_enriched_permalink": last_enriched_permalink or None,
        "known_post_ids": sorted(known, key=lambda x: int(x) if x.isdigit() else 0),
        "enriched_post_ids": sorted(enriched, key=lambda x: int(x) if x.isdigit() else 0),
        "pending_count": len(pending),
        "should_continue": should_continue,
        "stop_reason": stop_reason,
        "stats": stats,
        "updated_at": utc_now_iso(),
        "last_run_id": run_id,
    }
    _atomic_upload_json(bucket, _progress_key(source_prefix, group_id), payload)
    # Keep pending slim: id+permalink+flags only / Giữ pending gọn: id+permalink+flags
    slim = []
    for row in pending:
        slim.append(
            {
                "post_id": row.get("post_id"),
                "permalink": row.get("permalink"),
                "graphql_complete": bool(row.get("graphql_complete")),
                "caption": (row.get("caption") or "")[:500],
                "image_urls": list(row.get("image_urls") or [])[:12],
                "posted_at": row.get("posted_at"),
                "author": row.get("author") or "",
                "source": row.get("source") or "graphql",
            }
        )
    _atomic_upload_json(
        bucket,
        _pending_key(source_prefix, group_id),
        {"updated_at": utc_now_iso(), "pending": slim},
    )


def _candidate_needs_selenium(cand: dict[str, Any]) -> bool:
    """True when GraphQL fields are incomplete / True khi field GraphQL chưa đủ."""
    if not cand.get("graphql_complete"):
        return True
    caption = str(cand.get("caption") or "").strip()
    images = list(cand.get("image_urls") or [])
    ok, _reason = _classify_record(caption, images)
    # Still allow selenium if caption looks truncated noise /
    # Vẫn selenium nếu caption giống nhiễu/cắt cụt
    if not ok and _reason in {"missing_label", "missing_image", "missing_label_and_image"}:
        return True
    return False


def run_graphql_enrich_batch(
    *,
    group_id: str,
    batch_target: int = DEFAULT_BATCH_TARGET,
    discover_chunk: int = DEFAULT_DISCOVER_CHUNK,
    permalink_pause_sec: float = DEFAULT_PERMALINK_PAUSE_SEC,
    soft_restart_every: int = DEFAULT_MATCH_SOFT_RESTART_EVERY,
    bottom_year: int = DEFAULT_BOTTOM_YEAR,
    skip_streak_threshold: int = DEFAULT_SKIP_STREAK,
    download_images: bool = True,
    headless: bool = False,
    upload_minio: bool = True,
    reset_crawl_data: bool = False,
    run_id: str | None = None,
) -> dict[str, Any]:
    """One batch: newest-first GraphQL + skip known + Selenium fallback + download.

    Một batch: GraphQL từ mới nhất + skip đã xử lý + Selenium fallback + tải ảnh.
    """
    if not group_id.strip():
        raise ValueError("group_id is required")
    batch_target = max(1, int(batch_target))
    discover_chunk = max(10, int(discover_chunk))
    bottom_year = int(bottom_year or DEFAULT_BOTTOM_YEAR)
    skip_streak_threshold = max(10, int(skip_streak_threshold or DEFAULT_SKIP_STREAK))

    settings = _settings()
    bucket = settings["bucket_raw"]
    source_prefix = settings["source_prefix"]
    remote_url = settings["selenium_remote_url"]
    page_load_timeout = int(settings["page_load_timeout_sec"])
    image_timeout = int(settings["image_timeout_sec"])
    resolved_run_id = (run_id or "").strip() or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())

    if upload_minio:
        ensure_bucket(get_minio_client(), bucket)

    reset_info: dict[str, Any] = {}
    if reset_crawl_data and upload_minio:
        reset_info = reset_crawl_data_keep_cookies(
            bucket=bucket, source_prefix=source_prefix, group_id=group_id, archive=True
        )

    drained = _drain_selenium_sessions(remote_url)
    print(f"{LOG} pre_drain_sessions={drained}", flush=True)
    _selenium_preflight(remote_url)

    progress = {}
    if upload_minio and not reset_crawl_data:
        from final_exam_nlp_crawl_runner import _read_json_object

        progress = _read_json_object(bucket, _progress_key(source_prefix, group_id)) or {}
    posts = (
        {}
        if reset_crawl_data
        else (_load_posts_index(bucket, source_prefix, group_id) if upload_minio else {})
    )
    cookies_payload = None
    if upload_minio:
        from final_exam_nlp_crawl_runner import _read_json_object

        cookies_payload = _read_json_object(bucket, _cookies_key(source_prefix, group_id))
    cookies = list((cookies_payload or {}).get("cookies") or [])

    known: set[str] = set(str(x) for x in (progress.get("known_post_ids") or []) if x)
    known |= set(posts.keys())
    enriched: set[str] = set(str(x) for x in (progress.get("enriched_post_ids") or []) if x)
    for pid, rec in posts.items():
        enriched.add(str(pid))
        for extra in (
            rec.get("story_post_id"),
            rec.get("tile_id"),
            *(rec.get("tile_ids") or []),
        ):
            eid = str(extra or "").strip()
            if eid:
                known.add(eid)
                enriched.add(eid)

    # Pending from prior run (selenium leftovers) /
    # Pending từ run trước (còn Selenium)
    pending: list[dict[str, Any]] = []
    if upload_minio and not reset_crawl_data:
        from final_exam_nlp_crawl_runner import _read_json_object

        pq = _read_json_object(bucket, _pending_key(source_prefix, group_id)) or {}
        for row in pq.get("pending") or []:
            pid = str((row or {}).get("post_id") or "").strip()
            if not pid or pid in enriched:
                continue
            if pid in {p["post_id"] for p in pending}:
                continue
            pending.append(
                {
                    "post_id": pid,
                    "permalink": str(row.get("permalink") or _permalink_for(group_id, pid)),
                    "caption": str(row.get("caption") or ""),
                    "image_urls": list(row.get("image_urls") or []),
                    "posted_at": row.get("posted_at"),
                    "author": str(row.get("author") or ""),
                    "graphql_complete": bool(row.get("graphql_complete")),
                    "source": str(row.get("source") or "pending"),
                }
            )
            known.add(pid)

    # Each batch catch-up from newest; keep deepest backfill cursor /
    # Mỗi batch catch-up từ mới nhất; giữ cursor backfill sâu nhất
    backfill_cursor = progress.get("backfill_cursor") or progress.get("graphql_cursor")
    cursor: str | None = None
    has_next = True
    mode = "catchup"
    captured: dict[str, Any] = {"url": None, "form": None, "headers": None}
    if upload_minio:
        from final_exam_nlp_crawl_runner import _read_json_object

        saved_cap = _read_json_object(bucket, _graphql_capture_key(source_prefix, group_id)) or {}
        if saved_cap.get("form") and saved_cap.get("url"):
            captured = {
                "url": saved_cap.get("url"),
                "form": saved_cap.get("form"),
                "headers": saved_cap.get("headers") or {},
            }

    run_valid = 0
    run_invalid = 0
    run_skipped = 0
    run_gql_ok = 0
    run_selenium = 0
    discovered = 0
    processed_this_run = 0
    skip_streak = 0
    upserts: list[dict[str, Any]] = []
    stop_reason = "batch_target"
    should_continue = False
    last_enriched_post_id = str(progress.get("last_enriched_post_id") or "")
    last_enriched_permalink = str(progress.get("last_enriched_permalink") or "")
    group_feed = f"{FB_BASE_URL}/groups/{group_id}/?sorting_setting=CHRONOLOGICAL"
    group_start = f"{FB_BASE_URL}/groups/{group_id}/"
    hard_stop = False
    capture_ready = bool(captured.get("form"))
    bottom_hits = 0

    print(
        f"{LOG} start schema={SCHEMA_VERSION} target={batch_target} "
        f"known={len(known)} enriched={len(enriched)} pending={len(pending)} "
        f"backfill={'yes' if backfill_cursor else 'no'} "
        f"download={download_images} bottom_year={bottom_year} reset={bool(reset_info)}",
        flush=True,
    )

    driver = None
    session_cookies = cookies
    for boot_attempt in range(1, 3):
        _drain_selenium_sessions(remote_url)
        _selenium_preflight(remote_url)
        driver = _build_driver(
            remote_url, headless, page_load_timeout, enable_perf_logs=True
        )
        try:
            session_cookies = _ensure_driver_login(
                driver=driver,
                bucket=bucket,
                source_prefix=source_prefix,
                group_id=group_id,
                start_url=group_start,
                cookies=cookies,
                fb_username="",
                fb_password="",
                fb_totp_secret="",
                upload_minio=upload_minio,
            )
            break
        except Exception as exc:
            print(
                f"{LOG} boot_login_fail attempt={boot_attempt} err={type(exc).__name__}",
                flush=True,
            )
            _safe_quit_driver(driver)
            driver = None
            if boot_attempt >= 2:
                raise
            time.sleep(5)

    def _flush_checkpoint(reason: str, cont: bool) -> None:
        if not upload_minio:
            return
        if upserts:
            _persist_index_and_logs(
                bucket=bucket,
                source_prefix=source_prefix,
                group_id=group_id,
                run_id=resolved_run_id,
                posts=posts,
                upserts=upserts,
            )
            upserts.clear()
        _write_progress(
            bucket=bucket,
            source_prefix=source_prefix,
            group_id=group_id,
            cursor=cursor,
            backfill_cursor=backfill_cursor if isinstance(backfill_cursor, str) else None,
            has_next=has_next,
            known=known,
            enriched=enriched,
            pending=pending,
            stop_reason=reason,
            should_continue=cont,
            stats={
                "discovered": discovered,
                "run_valid": run_valid,
                "run_invalid": run_invalid,
                "run_skipped": run_skipped,
                "run_gql_ok": run_gql_ok,
                "run_selenium": run_selenium,
                "processed_this_run": processed_this_run,
                "enriched_total": len(enriched),
                "mode": mode,
            },
            run_id=resolved_run_id,
            last_enriched_post_id=last_enriched_post_id,
            last_enriched_permalink=last_enriched_permalink,
            mode=mode,
        )

    def _soft_restart_driver() -> None:
        nonlocal driver, session_cookies, capture_ready
        # Checkpoint before killing Chrome so no post is lost /
        # Checkpoint trước khi tắt Chrome để không mất post
        _flush_checkpoint("soft_restart", True)
        print(
            f"{LOG_ENRICH} soft_restart every={soft_restart_every} "
            f"processed={processed_this_run} soft_mb={SELENIUM_RAM_SOFT_MB} "
            f"hard_mb={SELENIUM_RAM_HARD_STOP_MB}",
            flush=True,
        )
        _safe_quit_driver(driver)
        _drain_selenium_sessions(remote_url)
        _selenium_preflight(remote_url)
        driver = _build_driver(
            remote_url, headless, page_load_timeout, enable_perf_logs=True
        )
        session_cookies = _ensure_driver_login(
            driver=driver,
            bucket=bucket,
            source_prefix=source_prefix,
            group_id=group_id,
            start_url=group_start,
            cookies=session_cookies,
            fb_username="",
            fb_password="",
            fb_totp_secret="",
            upload_minio=upload_minio,
        )
        _enable_network_cdp(driver)
        _install_fetch_hook(driver)
        _clear_browser_caches(driver)
        capture_ready = bool(captured.get("form"))

    def _release_page_memory() -> None:
        """Drop current page DOM to free Chrome RAM / Bỏ DOM trang hiện tại để giải phóng RAM."""
        try:
            _clear_browser_caches(driver)
        except Exception:
            pass
        try:
            driver.get("about:blank")
            time.sleep(0.6)
        except Exception:
            pass

    def _discover_fill(fill_target: int) -> None:
        """Fill pending from newest or backfill cursor / Đổ pending từ mới nhất hoặc backfill."""
        nonlocal cursor, has_next, backfill_cursor, discovered, capture_ready
        nonlocal stop_reason, should_continue, hard_stop, mode

        if len(pending) >= fill_target or not has_next:
            return

        print(
            f"{LOG_DISCOVER} fill mode={mode} pending={len(pending)} "
            f"target={fill_target} cursor={'yes' if cursor else 'newest'} "
            f"backfill={'yes' if backfill_cursor else 'no'}",
            flush=True,
        )
        # Install fetch hook BEFORE feed traffic, then hard-reload /
        # Cài fetch hook TRƯỚC traffic feed, rồi reload cứng
        _install_fetch_hook(driver)
        try:
            driver.get(group_feed)
            _human_pause(3.5, 0.6, 1.2)
        except Exception as exc:
            print(f"{LOG_DISCOVER} open_feed_err={type(exc).__name__}", flush=True)

        if _is_facebook_checkpoint(driver):
            stop_reason = "facebook_checkpoint"
            should_continue = False
            hard_stop = True
            print(
                f"{LOG_DISCOVER} facebook_checkpoint "
                f"url={(getattr(driver, 'current_url', '') or '')[:160]}",
                flush=True,
            )
            return

        if not _is_logged_in(driver):
            stop_reason = "session_lost"
            should_continue = True
            hard_stop = True
            print(f"{LOG_DISCOVER} session_lost — no 2FA login", flush=True)
            return

        _install_fetch_hook(driver)
        try:
            driver.execute_script("window.scrollTo(0, 600);")
            _human_pause(1.2, 0.3, 0.6)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            _human_pause(2.0, 0.4, 0.8)
        except Exception:
            pass

        pending_req: dict[str, str] = {}
        page_seen: set[str] = set()

        def _ingest_bodies(bodies: list[str]) -> None:
            nonlocal cursor, has_next, backfill_cursor, discovered
            for body in bodies:
                cands, pi, err = ingest_graphql_text(
                    body, group_id=group_id, seen_page_ids=page_seen
                )
                for cand in cands:
                    if cand["post_id"] in enriched:
                        continue
                    if cand["post_id"] in {p["post_id"] for p in pending}:
                        continue
                    pending.append(cand)
                    known.add(cand["post_id"])
                    discovered += 1
                    print(
                        f"{LOG_DISCOVER} +{cand['post_id']} "
                        f"complete={cand['graphql_complete']} "
                        f"src={cand.get('source')} {cand['permalink']}",
                        flush=True,
                    )
                if pi:
                    end_c = pi.get("end_cursor")
                    if end_c:
                        cursor = end_c
                        backfill_cursor = end_c
                    has_next = bool(pi.get("has_next_page", True))
                if err:
                    print(f"{LOG_DISCOVER} graphql_err={err}", flush=True)

        def _ingest_rich_dom() -> int:
            nonlocal discovered
            added = 0
            for cand in _dom_harvest_candidates(driver, group_id):
                if cand["post_id"] in enriched:
                    continue
                if cand["post_id"] in {p["post_id"] for p in pending}:
                    # Upgrade incomplete pending with richer fields /
                    # Nâng pending thiếu field bằng data giàu hơn
                    for i, old in enumerate(pending):
                        if old["post_id"] != cand["post_id"]:
                            continue
                        if cand.get("graphql_complete") and not old.get(
                            "graphql_complete"
                        ):
                            pending[i] = cand
                        break
                    continue
                pending.append(cand)
                known.add(cand["post_id"])
                discovered += 1
                added += 1
                print(
                    f"{LOG_DISCOVER} +{cand['post_id']} "
                    f"complete={cand['graphql_complete']} "
                    f"src={cand.get('source')} {cand['permalink']}",
                    flush=True,
                )
            return added

        # First rich harvest on loaded feed /
        # Harvest giàu ngay trên feed đã load
        _ingest_rich_dom()

        # Scroll-capture; ingest graphql + rich HTML every 2 rounds /
        # Scroll-capture; ingest graphql + HTML giàu mỗi 2 vòng
        if not capture_ready or len(pending) < fill_target:
            for round_idx in range(1, DEFAULT_SCROLL_CAPTURE_ROUNDS + 1):
                _drain_perf_capture(driver, captured, pending_req)
                hook_bodies = _drain_fetch_hook(driver, captured)
                cdp_bodies = _fetch_response_bodies(driver, pending_req)
                _ingest_bodies(hook_bodies + cdp_bodies)
                if round_idx % 2 == 0:
                    _ingest_rich_dom()
                if captured.get("form"):
                    capture_ready = True
                    print(
                        f"{LOG_DISCOVER} capture_ready pending={len(pending)}",
                        flush=True,
                    )
                complete_n = sum(1 for p in pending if p.get("graphql_complete"))
                # Only early-stop when we have a real work queue; never stop at 2–4 /
                # Chỉ early-stop khi đã có hàng đợi thật; không dừng ở 2–4 post
                if len(pending) >= fill_target or complete_n >= min(fill_target, 40):
                    print(
                        f"{LOG_DISCOVER} early_stop_scroll pending={len(pending)} "
                        f"complete={complete_n}",
                        flush=True,
                    )
                    break
                if (
                    len(pending) >= max(25, fill_target // 3)
                    and round_idx >= 10
                    and not captured.get("form")
                ):
                    print(
                        f"{LOG_DISCOVER} early_stop_scroll pending={len(pending)} "
                        f"(rich html, no graphql capture yet)",
                        flush=True,
                    )
                    break
                try:
                    driver.execute_script(
                        "window.scrollTo(0, document.body.scrollHeight);"
                    )
                except Exception:
                    pass
                _human_pause(1.8, 0.3, 0.8)
                if round_idx % 4 == 0:
                    print(
                        f"{LOG_DISCOVER} capture_scroll round={round_idx} "
                        f"captured={bool(captured.get('form'))} "
                        f"pending={len(pending)} hook_bodies={len(hook_bodies)}",
                        flush=True,
                    )

        # Jump to backfill cursor after catch-up skip-streak /
        # Nhảy sang backfill cursor sau skip-streak catch-up
        replay_cursor = cursor
        if mode == "backfill" and backfill_cursor:
            replay_cursor = str(backfill_cursor)

        # If network hook still empty, bootstrap form from page tokens /
        # Nếu hook mạng vẫn trống, dựng form từ token trong page
        if not captured.get("form"):
            if _bootstrap_capture_from_page(driver, captured, group_id):
                capture_ready = True

        # Prefer HTTP GraphQL replay (fast, low RAM) when capture exists /
        # Ưu tiên replay GraphQL HTTP (nhanh, ít RAM) khi đã có capture
        live_cookies = session_cookies
        try:
            live_cookies = driver.get_cookies() or session_cookies
        except Exception:
            pass

        replay_fails = 0
        while (
            has_next
            and captured.get("form")
            and len(pending) < fill_target
            and not hard_stop
        ):
            status, text = _http_graphql_replay(live_cookies, captured, replay_cursor)
            if (
                status == 0
                or not text
                or text.startswith("http_err")
                or text == "missing_capture"
            ):
                status, text = _replay_fetch(driver, captured, replay_cursor)
            if status == 429 or (text and "1357004" in text):
                wait = min(180, 30 * max(1, replay_fails + 1))
                print(f"{LOG_DISCOVER} throttled status={status} sleep={wait}s", flush=True)
                time.sleep(wait)
                replay_fails += 1
                if replay_fails >= 5:
                    stop_reason = "graphql_throttled"
                    should_continue = True
                    hard_stop = True
                    break
                continue
            cands, pi, err = ingest_graphql_text(
                text, group_id=group_id, seen_page_ids=page_seen
            )
            if pi is None and not cands:
                replay_fails += 1
                print(
                    f"{LOG_DISCOVER} replay_bad status={status} err={err} fails={replay_fails}",
                    flush=True,
                )
                if replay_fails >= 3:
                    captured.clear()
                    captured.update({"url": None, "form": None, "headers": None})
                    capture_ready = False
                    break
                time.sleep(2)
                continue
            replay_fails = 0
            new_n = 0
            for cand in cands:
                if cand["post_id"] in enriched:
                    continue
                if cand["post_id"] in {p["post_id"] for p in pending}:
                    continue
                pending.append(cand)
                known.add(cand["post_id"])
                discovered += 1
                new_n += 1
                print(
                    f"{LOG_DISCOVER} +{cand['post_id']} "
                    f"complete={cand['graphql_complete']} "
                    f"{cand['permalink']}",
                    flush=True,
                )
            end_c = None
            if pi:
                end_c = pi.get("end_cursor")
                has_next = bool(pi.get("has_next_page", bool(end_c)))
                if end_c:
                    cursor = end_c
                    replay_cursor = end_c
                    backfill_cursor = end_c
            print(
                f"{LOG_DISCOVER} http_replay +{new_n} pending={len(pending)} "
                f"has_next={has_next} mode={mode}",
                flush=True,
            )
            if pi is not None and not has_next:
                break
            time.sleep(random.uniform(0.8, 1.8))

        if upload_minio and captured.get("form"):
            upload_json_payload(
                bucket,
                _graphql_capture_key(source_prefix, group_id),
                {
                    "saved_at": utc_now_iso(),
                    "url": captured.get("url"),
                    "form": captured.get("form"),
                    "headers": captured.get("headers") or {},
                },
            )

    def _process_candidate(cand: dict[str, Any]) -> str:
        """Process one candidate; return action tag.

        Xử lý 1 candidate; trả tag hành động: processed|skipped|bottom|stop.
        """
        nonlocal processed_this_run, run_valid, run_invalid, run_skipped
        nonlocal run_gql_ok, run_selenium, skip_streak, mode
        nonlocal last_enriched_post_id, last_enriched_permalink
        nonlocal stop_reason, should_continue, hard_stop, bottom_hits
        nonlocal cursor, backfill_cursor

        post_id = str(cand.get("post_id") or "").strip()
        permalink = str(cand.get("permalink") or _permalink_for(group_id, post_id))
        if not post_id:
            return "skipped"

        # Already processed → skip; streak may jump to backfill /
        # Đã xử lý → skip; streak có thể nhảy backfill
        if post_id in enriched or post_id in posts:
            enriched.add(post_id)
            known.add(post_id)
            run_skipped += 1
            skip_streak += 1
            print(
                f"{LOG_DISCOVER} skip {post_id} streak={skip_streak}/{skip_streak_threshold}",
                flush=True,
            )
            if (
                mode == "catchup"
                and skip_streak >= skip_streak_threshold
                and backfill_cursor
            ):
                mode = "backfill"
                cursor = str(backfill_cursor)
                skip_streak = 0
                print(
                    f"{LOG_DISCOVER} skip_streak → backfill_cursor engaged",
                    flush=True,
                )
            return "skipped"

        skip_streak = 0
        caption = str(cand.get("caption") or "").strip()
        image_urls = _dedupe_image_urls(
            [u for u in (cand.get("image_urls") or []) if not _is_video_url(u)]
        )
        author = str(cand.get("author") or "")
        posted_at = cand.get("posted_at") if isinstance(cand.get("posted_at"), str) else None
        phase = "graphql_first"
        used_selenium = False

        if _candidate_needs_selenium(cand):
            # Selenium fallback for incomplete GraphQL /
            # Selenium fallback khi GraphQL thiếu field
            try:
                _ram_guard_selenium(
                    driver,
                    soft_limit_mb=SELENIUM_RAM_SOFT_MB,
                    log_prefix=LOG_ENRICH,
                )
            except Exception:
                pass
            heap_mb = _js_heap_used_mb(driver)
            if heap_mb >= SELENIUM_RAM_HARD_STOP_MB:
                # End batch but keep pending — rollover continues all posts /
                # Kết batch nhưng giữ pending — rollover lấy đủ post
                stop_reason = "selenium_ram_limit"
                should_continue = True
                hard_stop = True
                pending.insert(0, cand)
                _flush_checkpoint(stop_reason, True)
                print(
                    f"{LOG_ENRICH} ram_stop heap≈{heap_mb:.0f}MB "
                    f">={SELENIUM_RAM_HARD_STOP_MB:.0f}MB — rollover keeps pending",
                    flush=True,
                )
                return "stop"

            if (
                soft_restart_every > 0
                and processed_this_run > 0
                and processed_this_run % soft_restart_every == 0
            ):
                _soft_restart_driver()

            if _is_facebook_checkpoint(driver):
                stop_reason = "facebook_checkpoint"
                should_continue = False
                hard_stop = True
                pending.insert(0, cand)
                return "stop"

            # about:blank after RAM release hides FB cookies — re-open FB first /
            # about:blank sau giải phóng RAM ẩn cookie FB — mở lại FB trước
            try:
                cur = (getattr(driver, "current_url", "") or "").lower()
            except Exception:
                cur = ""
            if "facebook.com" not in cur:
                try:
                    driver.get("https://www.facebook.com/")
                    _human_pause(1.5, 0.3, 0.6)
                except Exception as exc:
                    print(
                        f"{LOG_ENRICH} reopen_fb_err={type(exc).__name__}",
                        flush=True,
                    )

            if not _is_logged_in(driver):
                stop_reason = "session_lost"
                should_continue = True
                hard_stop = True
                pending.insert(0, cand)
                print(f"{LOG_ENRICH} session_lost — stop without 2FA", flush=True)
                return "stop"

            extracted = _recheck_extract_via_permalink(
                driver,
                group_id=group_id,
                story_url=permalink,
                permalink_pause_sec=permalink_pause_sec,
                existing_images=image_urls,
            )
            caption = (extracted.get("caption") or caption or "").strip()
            image_urls = _dedupe_image_urls(
                [
                    u
                    for u in (
                        list(extracted.get("image_urls") or []) + list(image_urls)
                    )
                    if not _is_video_url(u)
                ]
            )
            author = (extracted.get("author") or author or "").strip()
            if extracted.get("posted_at"):
                posted_at = extracted.get("posted_at")
            permalink = (
                _posts_to_permalink_url(
                    extracted.get("story_url") or permalink, group_id
                )
                or permalink
            )
            phase = "graphql_selenium_fallback"
            used_selenium = True
            run_selenium += 1
            _human_pause(permalink_pause_sec, 0.3, 1.0)
            # Release permalink DOM before next post /
            # Giải phóng DOM permalink trước post tiếp
            _release_page_memory()
        else:
            run_gql_ok += 1

        year = _year_from_posted_at(posted_at if isinstance(posted_at, str) else None)
        if year is not None and year < bottom_year:
            bottom_hits += 1
            print(
                f"{LOG_ENRICH} bottom_year candidate id={post_id} year={year} "
                f"hits={bottom_hits}",
                flush=True,
            )
            if bottom_hits >= 3:
                stop_reason = "bottom_year_reached"
                should_continue = False
                hard_stop = True
                return "bottom"

        is_valid, invalid_reason = _classify_record(caption, image_urls)
        if caption and _is_ui_noise_label(caption):
            is_valid = False
            invalid_reason = invalid_reason or "ui_noise_caption"

        image_local_keys: list[str] = []
        images_downloaded = False
        images_download_skipped = not download_images
        if is_valid and download_images and upload_minio:
            image_local_keys, dl_errs = _store_post_images(
                bucket=bucket,
                source_prefix=source_prefix,
                group_id=group_id,
                post_id=post_id,
                image_urls=image_urls,
                timeout=image_timeout,
            )
            images_downloaded = bool(image_local_keys)
            images_download_skipped = False
            if dl_errs:
                print(
                    f"{LOG_ENRICH} download_warn id={post_id} errs={dl_errs[:2]}",
                    flush=True,
                )

        record = _build_record(
            post_id=post_id,
            post_url=permalink,
            author=author,
            label=caption,
            image_urls=image_urls,
            posted_at=posted_at,
            group_id=group_id,
            run_id=resolved_run_id,
            phase=phase,
            image_local_keys=image_local_keys,
            images_downloaded=images_downloaded,
            images_download_skipped=images_download_skipped,
            is_valid=is_valid,
            invalid_reason=None if is_valid else (invalid_reason or "extract_failed"),
            tile_id=post_id,
            story_post_id=post_id,
            sub_caption="",
        )
        key, merged = _upsert_post(posts, record)
        enriched.add(str(key or post_id))
        known.add(str(key or post_id))
        known.add(post_id)
        last_enriched_post_id = str(key or post_id)
        last_enriched_permalink = str(merged.get("permalink") or permalink)
        upserts.append(merged)
        processed_this_run += 1
        if is_valid:
            run_valid += 1
        else:
            run_invalid += 1

        print(
            f"{LOG_ENRICH} {processed_this_run}/{batch_target} id={key or post_id} "
            f"valid={is_valid} gql_ok={not used_selenium} "
            f"images={len(image_urls)} downloaded={images_downloaded} "
            f"caption={caption[:48]!r}",
            flush=True,
        )
        # Checkpoint after every processed post /
        # Checkpoint sau mỗi post đã xử lý
        _flush_checkpoint("in_progress", True)
        return "processed"

    try:
        _enable_network_cdp(driver)
        _clear_browser_caches(driver)
        _install_fetch_hook(driver)

        if _is_facebook_checkpoint(driver):
            stop_reason = "facebook_checkpoint"
            should_continue = False
            hard_stop = True
            print(
                f"{LOG} facebook_checkpoint url={(getattr(driver, 'current_url', '') or '')[:160]} "
                "— resolve manually, then re-trigger with RESET_CRAWL_DATA=false",
                flush=True,
            )

        while (not hard_stop) and processed_this_run < batch_target:
            remain = batch_target - processed_this_run
            fill_target = min(discover_chunk, max(remain, 1))

            # Only discover when queue empty — avoid re-scroll between each enrich /
            # Chỉ discover khi queue trống — tránh scroll lại giữa mỗi enrich
            if not pending:
                _discover_fill(fill_target)
                if hard_stop:
                    break
                # After discover, drop feed DOM before permalink enrich /
                # Sau discover, bỏ DOM feed trước khi enrich permalink
                if pending:
                    _release_page_memory()

            if not pending:
                if _is_facebook_checkpoint(driver):
                    stop_reason = "facebook_checkpoint"
                    should_continue = False
                elif not has_next and capture_ready:
                    stop_reason = "feed_exhausted"
                    should_continue = False
                else:
                    # Wave empty but feed may be deeper — rollover to keep collecting /
                    # Wave trống nhưng feed có thể còn sâu — rollover để lấy đủ
                    stop_reason = "discover_wave_empty"
                    should_continue = True
                    has_next = True
                print(
                    f"{LOG_DISCOVER} no_pending stop={stop_reason} "
                    f"continue={should_continue} capture={capture_ready} "
                    f"has_next={has_next}",
                    flush=True,
                )
                break

            cand = pending.pop(0)
            # Prefer GraphQL/HTML-complete posts first (download without Selenium) /
            # Ưu tiên post đủ field từ GraphQL/HTML trước (download không cần Selenium)
            if not cand.get("graphql_complete"):
                for i, other in enumerate(pending):
                    if other.get("graphql_complete"):
                        pending[i] = cand
                        cand = other
                        break
            action = _process_candidate(cand)
            if action in {"stop", "bottom"}:
                break

        if processed_this_run >= batch_target:
            stop_reason = "batch_target"
            should_continue = bool(has_next or pending)
        elif stop_reason == "batch_target":
            if hard_stop:
                pass
            elif not pending and not has_next:
                stop_reason = "feed_exhausted"
                should_continue = False
            elif pending or has_next:
                stop_reason = "partial_batch"
                should_continue = True

        if upload_minio and _is_logged_in(driver):
            try:
                session_cookies = driver.get_cookies() or session_cookies
                upload_json_payload(
                    bucket,
                    _cookies_key(source_prefix, group_id),
                    {"saved_at": utc_now_iso(), "cookies": session_cookies},
                )
            except Exception as exc:
                print(f"{LOG} cookie_save_skip={type(exc).__name__}", flush=True)

    finally:
        _safe_quit_driver(driver)
        try:
            _drain_selenium_sessions(remote_url)
        except Exception:
            pass

    result = {
        "schema_version": SCHEMA_VERSION,
        "crawl_mode": "graphql_batch",
        "group_id": group_id,
        "run_id": resolved_run_id,
        "stop_reason": stop_reason,
        "should_continue": should_continue,
        "batch_target": batch_target,
        "discovered": discovered,
        "valid_count": run_valid,
        "invalid_count": run_invalid,
        "skipped_count": run_skipped,
        "graphql_complete_count": run_gql_ok,
        "selenium_fallback_count": run_selenium,
        "processed_this_run": processed_this_run,
        "enriched_total": len(enriched),
        "pending_left": len(pending),
        "has_next": has_next,
        "mode": mode,
        "graphql_cursor": bool(cursor),
        "backfill_cursor": bool(backfill_cursor),
        "last_enriched_post_id": last_enriched_post_id,
        "last_enriched_permalink": last_enriched_permalink,
        "download_images": download_images,
        "bottom_year": bottom_year,
        "reset_crawl_data": reset_info,
    }

    if upload_minio:
        _flush_checkpoint(stop_reason, should_continue)
        _rebuild_exports(
            bucket=bucket, source_prefix=source_prefix, group_id=group_id, posts=posts
        )
        upload_json_payload(
            bucket, _run_result_key(source_prefix, group_id, resolved_run_id), result
        )

    print(f"{LOG} done {result}", flush=True)
    return result
