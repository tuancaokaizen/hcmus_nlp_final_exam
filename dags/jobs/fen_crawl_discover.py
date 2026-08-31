"""Job1 Discover — practice from tests/crawl_graphql.py (no invented capture).

Strategy (same as tests/crawl_graphql.py):
1. Open group chronological, scroll until FB fires pagination GraphQL.
2. Capture ONLY when body contains GroupsCometFeedRegularStoriesPaginationQuery.
3. Replay via in-page fetch() (same cookies/fingerprint) — NOT external HTTP.
4. One batch ≤200 then exit; DAG loop continues.

Job1 Discover — practice từ tests/crawl_graphql.py (không bịa capture).
"""
from __future__ import annotations

import json
import os
import random
import time
from typing import Any

from final_exam_nlp_crawl_runner import (
    DEFAULT_BOTTOM_YEAR,
    _atomic_upload_json,
    _build_driver,
    _cookies_key,
    _drain_selenium_sessions,
    _read_json_object,
    _safe_quit_driver,
    _selenium_preflight,
    _settings,
    _year_from_posted_at,
)
from final_exam_nlp_graphql_batch import (
    PAGINATION_QUERY,
    _drain_cdp_sniffer_bodies,
    _drain_cdp_sniffer_capture,
    _drain_fetch_hook,
    _drain_perf_capture,
    _enable_network_cdp,
    _fetch_response_bodies,
    _install_fetch_hook,
    _replay_fetch,
    _stop_cdp_graphql_sniffer,
    ingest_graphql_text,
    _graphql_capture_key as _legacy_gql_capture_key,
)
from final_exam_nlp_two_phase import _ensure_driver_login, _human_pause
from fen_crawl_common import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_SKIP_STREAK,
    LOG_PIPELINE,
    SCHEMA_CRAWL,
    build_discover_row,
    classify_then_maybe_download,
    discover_batch_key,
    ensure_raw_bucket,
    graphql_capture_key,
    load_checkpoint,
    load_seen_ids,
    crawl_root,
    merge_seen_from_download_log,
    permalink_for,
    save_checkpoint,
    save_seen_ids,
    set_active_batch,
    upsert_task_exports_with_shared,
    write_jsonl,
)

LOG = "[fen_crawl_discover]"
# Persist seen often so Airflow timeout does not lose dedupe /
# Ghi seen thường xuyên để timeout Airflow không mất lọc trùng
FLUSH_SEEN_EVERY = 10

# Mirrors tests/crawl_graphql.py timing /
# Giống timing tests/crawl_graphql.py
SCROLL_PAUSE = (2.0, 4.5)
REPLAY_PAUSE = (1.5, 3.5)
SCROLL_BARREN_RELOAD = 25
MAX_SCROLL_ROUNDS = 40
# In-session recapture budget (crawl_graphql.py reload) /
# Số lần recapture trong một job (reload crawl_graphql.py)
MAX_REPLAY_RELOADS = 2
DEFAULT_DUMP_PREFIX = "facebook_seed2"

# Strip offscreen media without deleting React nodes (RAM) /
# Gỡ media ngoài viewport, không xóa node React (RAM)
_STRIP_MEDIA_JS = """
let n = 0;
document.querySelectorAll('video').forEach(v => {
  try {
    v.pause(); v.removeAttribute('src');
    v.querySelectorAll('source').forEach(s => s.remove());
    v.load(); n++;
  } catch (e) {}
});
document.querySelectorAll('img').forEach(i => {
  try {
    if (i.src && !String(i.src).startsWith('data:')
        && i.getBoundingClientRect().bottom < -2000) {
      i.removeAttribute('srcset');
      i.src = 'data:image/gif;base64,R0lGODlhAQABAAAAACw=';
      n++;
    }
  } catch (e) {}
});
return n;
"""


def _load_cookies(bucket: str, source_prefix: str, group_id: str) -> list[dict[str, Any]]:
    raw = _read_json_object(bucket, _cookies_key(source_prefix, group_id)) or {}
    cookies = raw.get("cookies") if isinstance(raw, dict) else None
    return list(cookies or []) if isinstance(cookies, list) else []


def _capture_is_valid(captured: dict[str, Any], group_id: str) -> bool:
    """Accept only real PaginationQuery capture (crawl_graphql.py rule).

    Chỉ nhận capture PaginationQuery thật (rule crawl_graphql.py).
    """
    form = captured.get("form") if isinstance(captured, dict) else None
    if not isinstance(form, dict) or not captured.get("url"):
        return False
    doc_id = str(form.get("doc_id") or "")
    friendly = str(form.get("fb_api_req_friendly_name") or "")
    # Never accept group_id mistaken for doc_id /
    # Không bao giờ nhận group_id nhầm thành doc_id
    if not doc_id or doc_id == str(group_id):
        return False
    # Exact friendly name from tests/crawl_graphql.py /
    # Đúng friendly name như tests/crawl_graphql.py
    return friendly == PAGINATION_QUERY


def _load_capture(bucket: str, source_prefix: str, group_id: str) -> dict[str, Any]:
    for key in (
        graphql_capture_key(source_prefix, group_id),
        _legacy_gql_capture_key(source_prefix, group_id),
    ):
        raw = _read_json_object(bucket, key) or {}
        if not (raw.get("form") and raw.get("url")):
            continue
        captured = {
            "url": raw["url"],
            "form": dict(raw["form"]),
            "headers": dict(raw.get("headers") or {}),
        }
        if not _capture_is_valid(captured, group_id):
            print(
                f"{LOG} ignore bad stored capture key={key} "
                f"friendly={(captured['form'] or {}).get('fb_api_req_friendly_name')!r} "
                f"doc_id={(captured['form'] or {}).get('doc_id')!r}",
                flush=True,
            )
            continue
        return captured
    return {}


def _save_capture(
    bucket: str, source_prefix: str, group_id: str, captured: dict[str, Any]
) -> None:
    if not _capture_is_valid(captured, group_id):
        return
    payload = {
        "url": captured.get("url"),
        "form": captured.get("form"),
        "headers": captured.get("headers") or {},
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    _atomic_upload_json(bucket, graphql_capture_key(source_prefix, group_id), payload)
    _atomic_upload_json(bucket, _legacy_gql_capture_key(source_prefix, group_id), payload)


def _clear_stored_capture(bucket: str, source_prefix: str, group_id: str) -> None:
    """Drop stale GraphQL form so the next scroll recaptures tokens.
    Xóa form GraphQL cũ để scroll lần sau bắt token mới.
    """
    from common.io_storage import delete_object

    for key in (
        graphql_capture_key(source_prefix, group_id),
        _legacy_gql_capture_key(source_prefix, group_id),
    ):
        try:
            delete_object(bucket, key)
        except Exception as exc:
            print(f"{LOG} clear_capture {key} err={type(exc).__name__}", flush=True)


def _strip_offscreen_media(driver) -> int:
    """Blank offscreen img/video src; do not remove React nodes.
    Xóa src ảnh/video ngoài màn; không gỡ node React.
    """
    try:
        n = driver.execute_script(_STRIP_MEDIA_JS)
        return int(n or 0)
    except Exception:
        return 0


def _load_dump_skip_ids(bucket: str, group_id: str) -> set[str]:
    """Skip decoded-final + seed2 seen so GraphQL A does not re-enrich dump posts.
    Bỏ decoded-final + seen B để GraphQL A không enrich lại bài dump.
    """
    skip: set[str] = set()
    dump_prefix = (
        os.environ.get("FEN_DUMP_PREFIX", DEFAULT_DUMP_PREFIX).strip() or DEFAULT_DUMP_PREFIX
    )
    skip |= load_seen_ids(bucket, dump_prefix, group_id)
    seed_key = f"{crawl_root(dump_prefix, group_id)}/seed/decoded-final.jsonl"
    try:
        from common.io_storage import get_minio_client

        client = get_minio_client()
        resp = client.get_object(bucket, seed_key)
        try:
            body = resp.read().decode("utf-8", errors="replace")
        finally:
            resp.close()
            resp.release_conn()
        for line in body.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            pid = str(row.get("post_id") or "").strip()
            if pid.isdigit():
                skip.add(pid)
    except Exception as exc:
        print(f"{LOG} dump_skip seed_load err={type(exc).__name__}", flush=True)
    print(f"{LOG} dump_skip n={len(skip)} prefix={dump_prefix}", flush=True)
    return skip


def _is_throttle_replay(status: int, text: str, err: str | None) -> bool:
    blob = f"{text or ''} {err or ''}".lower()
    return status == 429 or "1357004" in blob or "rate limit" in blob


def _append_candidates(
    *,
    cands: list[dict[str, Any]],
    group_id: str,
    seen: set[str],
    new_rows: list[dict[str, Any]],
    batch_size: int,
    bottom_year: int,
    skip_run: int,
    skip_streak: int,
    mode: str,
    backfill_cursor: str | None,
    bucket: str = "",
    source_prefix: str = "",
    extra_skip: set[str] | None = None,
) -> tuple[int, int, int, str, str | None, bool]:
    """Apply ingest candidates → new_rows; return skip_run, page_new, page_skip, mode, cursor_switch, reached.

    Áp candidates ingest → new_rows.
    """
    page_new = 0
    page_skip = 0
    reached_2013 = False
    cursor_switch: str | None = None
    for cand in cands:
        pid = str(cand.get("post_id") or "")
        if not pid:
            continue
        if extra_skip and pid in extra_skip:
            page_skip += 1
            skip_run += 1
            print(f"{LOG} skip={pid} reason=dump_or_seed2", flush=True)
            if mode == "catchup" and backfill_cursor and skip_run >= skip_streak:
                mode = "backfill"
                cursor_switch = str(backfill_cursor)
                skip_run = 0
                print(f"{LOG} switch mode=backfill cursor={cursor_switch[:24]}…", flush=True)
            continue
        if pid in seen:
            page_skip += 1
            skip_run += 1
            print(f"{LOG} skip={pid} reason=duplicate", flush=True)
            if mode == "catchup" and backfill_cursor and skip_run >= skip_streak:
                mode = "backfill"
                cursor_switch = str(backfill_cursor)
                skip_run = 0
                print(f"{LOG} switch mode=backfill cursor={cursor_switch[:24]}…", flush=True)
            continue
        skip_run = 0
        images = list(cand.get("image_urls") or [])
        posted_at = cand.get("posted_at")
        year = _year_from_posted_at(str(posted_at or ""))
        # Schema 5.1: keep GraphQL caption/CDN + valid flag /
        # Schema 5.1: giữ caption/CDN GraphQL + cờ valid
        row = build_discover_row(
            group_id=group_id,
            post_id=pid,
            permalink=str(cand.get("permalink") or permalink_for(group_id, pid)),
            caption=str(cand.get("caption") or ""),
            image_urls=images,
            posted_at=posted_at,
            author=str(cand.get("author") or ""),
            source=str(cand.get("source") or "graphql"),
            batch_seq=0,
        )
        # CDN expires fast — classify calligraphy then download if keep /
        # CDN hết hạn nhanh — classify thư pháp rồi tải nếu giữ
        if row.get("valid") and bucket and source_prefix:
            row = classify_then_maybe_download(
                row,
                bucket=bucket,
                source_prefix=source_prefix,
                group_id=group_id,
                log_prefix=LOG,
            )
        new_rows.append(row)
        seen.add(pid)
        page_new += 1
        # Flush seen so a mid-batch timeout still dedupes on retry /
        # Ghi seen giữa batch để timeout giữa chừng vẫn lọc trùng khi retry
        if bucket and source_prefix and page_new % FLUSH_SEEN_EVERY == 0:
            save_seen_ids(bucket, source_prefix, group_id, seen)
            print(f"{LOG} flushed_seen count={len(seen)}", flush=True)
        print(
            f"{LOG} +{pid} valid={row['valid']} images={row['image_count']} "
            f"calligraphy={row.get('is_calligraphy')} kind={row.get('calligraphy_kind')} "
            f"downloaded={row.get('images_downloaded')} "
            f"cap={(row['caption'][:28] + '…') if len(row['caption']) > 28 else row['caption']!r} "
            f"reason={row.get('invalid_reason') or '-'} permalink={row['permalink']}",
            flush=True,
        )
        if year is not None and year <= bottom_year:
            reached_2013 = True
            print(f"{LOG} reached bottom year={year} id={pid}", flush=True)
            break
        if len(new_rows) >= batch_size:
            break
    return skip_run, page_new, page_skip, mode, cursor_switch, reached_2013


def run_discover_batch(
    *,
    group_id: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    bottom_year: int = DEFAULT_BOTTOM_YEAR,
    skip_streak: int = DEFAULT_SKIP_STREAK,
    headless: bool = False,
    run_id: str | None = None,
    reset_crawl_data: bool = False,
) -> dict[str, Any]:
    """One discover shot via scroll-capture + in-page replay (crawl_graphql.py).

    Một lần discover: scroll-capture + replay trong page (crawl_graphql.py).
    """
    if not group_id.strip():
        raise ValueError("group_id is required")
    batch_size = max(1, min(5000, int(batch_size)))
    bottom_year = int(bottom_year or DEFAULT_BOTTOM_YEAR)
    skip_streak = max(10, int(skip_streak))
    resolved_run_id = (run_id or "").strip() or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())

    bucket, source_prefix = ensure_raw_bucket()
    # Optional reset: wipe crawl artifacts, keep cookies + GraphQL capture /
    # Reset tùy chọn: xóa artifact crawl, giữ cookies + GraphQL capture
    if reset_crawl_data:
        from final_exam_nlp_graphql_batch import reset_crawl_data_keep_cookies

        reset_info = reset_crawl_data_keep_cookies(
            bucket=bucket,
            source_prefix=source_prefix,
            group_id=group_id,
            archive=True,
        )
        print(f"{LOG} reset_crawl_data {reset_info}", flush=True)

    checkpoint = load_checkpoint(bucket, source_prefix, group_id)
    checkpoint["bottom_year"] = bottom_year
    seen = load_seen_ids(bucket, source_prefix, group_id)
    recovered = merge_seen_from_download_log(bucket, source_prefix, group_id, seen)
    if recovered:
        print(
            f"{LOG} recovered_seen_from_download_log +{recovered} known={len(seen)}",
            flush=True,
        )
    cookies = _load_cookies(bucket, source_prefix, group_id)
    stored = _load_capture(bucket, source_prefix, group_id)
    dump_skip = _load_dump_skip_ids(bucket, group_id)
    last_stop = str(checkpoint.get("stop_reason") or "")
    # Stale form after graphql_replay_fail must be recaptured /
    # Form cũ sau graphql_replay_fail phải bắt lại
    force_recapture = last_stop in {"graphql_replay_fail", "missing_graphql_capture"}

    print(
        f"{LOG} start batch_seq={checkpoint['batch_seq'] + 1} "
        f"cursor={'yes' if checkpoint.get('cursor') else 'newest'} "
        f"known={len(seen)} target={batch_size} bottom_year={bottom_year} "
        f"practice=crawl_graphql.py query={PAGINATION_QUERY}",
        flush=True,
    )

    if not cookies:
        checkpoint.update(
            {
                "should_continue": False,
                "stop_reason": "missing_cookies",
                "last_run_id": resolved_run_id,
            }
        )
        save_checkpoint(bucket, source_prefix, group_id, checkpoint)
        print(f"{LOG} missing_cookies — HITL login required", flush=True)
        return {"ok": False, "stop_reason": "missing_cookies", "new_count": 0}

    settings = _settings()
    remote = settings["selenium_remote_url"]
    timeout = int(settings["page_load_timeout_sec"])
    group_feed = (
        f"https://www.facebook.com/groups/{group_id}/?sorting_setting=CHRONOLOGICAL"
    )

    _drain_selenium_sessions(remote)
    _selenium_preflight(remote)
    driver = _build_driver(remote, headless, timeout, enable_perf_logs=True)

    captured: dict[str, Any] = dict(stored) if stored else {}
    pending_req: dict[str, str] = {}
    new_rows: list[dict[str, Any]] = []
    page_seen: set[str] = set()
    # Restore crawl mode/cursor so recapture does not restart from newest /
    # Khôi phục mode/cursor để recapture không chạy lại từ bài mới nhất
    mode = str(checkpoint.get("mode") or "catchup")
    replay_cursor = checkpoint.get("cursor") if mode == "backfill" else None
    if last_stop == "graphql_replay_fail":
        replay_cursor = checkpoint.get("cursor") or checkpoint.get("backfill_cursor")
        mode = "backfill" if replay_cursor else mode
    backfill_cursor = checkpoint.get("backfill_cursor") or checkpoint.get("cursor")
    skip_run = 0
    reached_2013 = bool(checkpoint.get("reached_bottom_year"))
    has_next = bool(checkpoint.get("has_next", True))
    # Resume replay whenever a cursor exists and feed is not exhausted /
    # Tiếp replay khi còn cursor và chưa hết feed (has_next=false cũ hay kẹt)
    if (
        (replay_cursor or backfill_cursor)
        and not reached_2013
        and last_stop != "feed_exhausted"
    ):
        has_next = True
        if last_stop in {"graphql_replay_fail", "batch_full", "empty_replay"}:
            print(
                f"{LOG} resume_replay last_stop={last_stop} has_next=True",
                flush=True,
            )
    stop_reason = "batch_full"
    cursor_from_scroll: str | None = None

    try:
        # Login first, then CDP — attaching CDP before login kills the Grid session /
        # Login trước, rồi mới CDP — gắn CDP trước login sẽ giết session Grid
        try:
            driver.get("about:blank")
        except Exception:
            pass
        need_capture = (not _capture_is_valid(captured, group_id)) or force_recapture
        if force_recapture:
            print(f"{LOG} force_recapture last_stop={last_stop}", flush=True)
            captured = {}
            _clear_stored_capture(bucket, source_prefix, group_id)

        cookies = _ensure_driver_login(
            driver=driver,
            bucket=bucket,
            source_prefix=source_prefix,
            group_id=group_id,
            start_url=group_feed,
            cookies=cookies,
            fb_username="",
            fb_password="",
            fb_totp_secret="",
            upload_minio=True,
        )
        # Only start CDP sniffer after login, and only when we still need the form /
        # Chỉ bật CDP sniffer sau login, và chỉ khi còn cần bắt form
        if need_capture:
            _enable_network_cdp(driver)
        _install_fetch_hook(driver)
        driver.get(group_feed)
        # tests/crawl_graphql.py waits 6–9s after open /
        # tests/crawl_graphql.py chờ 6–9s sau khi mở
        _human_pause(7.5, 1.5, 1.5)
        _install_fetch_hook(driver)

        # If capture already on MinIO — skip SCROLL, go REPLAY (practice) /
        # Nếu đã có capture trên MinIO — bỏ SCROLL, vào REPLAY
        if not need_capture:
            print(
                f"{LOG} using stored capture — skip SCROLL, enter REPLAY "
                f"friendly={(captured.get('form') or {}).get('fb_api_req_friendly_name')}",
                flush=True,
            )

        # ---- SCROLL mode: look for pagination request (crawl_graphql.py) ----
        # ---- SCROLL: tìm request phân trang (crawl_graphql.py) ----
        if need_capture:
            captured = {}
            print(
                f"{LOG} SCROLL mode — looking for {PAGINATION_QUERY}",
                flush=True,
            )
            barren = 0
            prev_total = len(new_rows)
            for round_idx in range(1, MAX_SCROLL_ROUNDS + 1):
                # Practice: form + cursor → replay; or form alone after enough scrolls /
                # Practice: form + cursor → replay; hoặc chỉ form sau đủ scroll
                if _capture_is_valid(captured, group_id) and (
                    cursor_from_scroll or replay_cursor
                ):
                    print(
                        f"{LOG} Captured pagination query + cursor — enter REPLAY "
                        f"saved_cursor={bool(replay_cursor)}",
                        flush=True,
                    )
                    break
                if _capture_is_valid(captured, group_id) and round_idx >= 3:
                    # Have form — bootstrap cursor via one in-page replay (null cursor) /
                    # Đã có form — lấy cursor bằng 1 lần replay trong page (cursor null)
                    print(
                        f"{LOG} Captured form — bootstrap cursor via in-page replay",
                        flush=True,
                    )
                    status, text = _replay_fetch(driver, captured, None)
                    if status == 200 and text and text.lstrip().startswith("{"):
                        cands, pi, err = ingest_graphql_text(
                            text, group_id=group_id, seen_page_ids=page_seen
                        )
                        if err:
                            print(f"{LOG} bootstrap_gql_err={err[:100]}", flush=True)
                        (
                            skip_run,
                            page_new,
                            page_skip,
                            mode,
                            cursor_switch,
                            hit_bottom,
                        ) = _append_candidates(
                            cands=cands,
                            group_id=group_id,
                            seen=seen,
                            new_rows=new_rows,
                            batch_size=batch_size,
                            bottom_year=bottom_year,
                            skip_run=skip_run,
                            skip_streak=skip_streak,
                            mode=mode,
                            backfill_cursor=backfill_cursor,
                            bucket=bucket,
                            source_prefix=source_prefix,
                            extra_skip=dump_skip,
                        )
                        if isinstance(pi, dict) and pi.get("end_cursor"):
                            cursor_from_scroll = str(pi.get("end_cursor"))
                            backfill_cursor = cursor_from_scroll
                            # Do not clobber a restored backfill cursor /
                            # Không ghi đè cursor backfill đã khôi phục
                            if not replay_cursor:
                                replay_cursor = cursor_from_scroll
                            has_next = bool(pi.get("has_next_page", True))
                        print(
                            f"{LOG} bootstrap_replay status={status} "
                            f"+{page_new} skip={page_skip} "
                            f"cursor_ok={bool(cursor_from_scroll)}",
                            flush=True,
                        )
                        if hit_bottom:
                            reached_2013 = True
                            stop_reason = f"reached_bottom_year_{bottom_year}"
                            has_next = False
                        break
                    print(
                        f"{LOG} bootstrap_replay fail status={status} "
                        f"len={len(text or '')} — keep scrolling",
                        flush=True,
                    )
                if barren >= SCROLL_BARREN_RELOAD:
                    print(f"{LOG} {barren} barren scrolls — stop scroll", flush=True)
                    break

                if round_idx % 3 == 0:
                    stripped = _strip_offscreen_media(driver)
                    if stripped:
                        print(f"{LOG} strip_media n={stripped} round={round_idx}", flush=True)
                if round_idx % 3 == 1:
                    _install_fetch_hook(driver)
                try:
                    driver.execute_script(
                        "window.scrollTo(0, document.body.scrollHeight);"
                    )
                except Exception:
                    pass
                time.sleep(random.uniform(*SCROLL_PAUSE))

                _drain_perf_capture(driver, captured, pending_req)
                _drain_cdp_sniffer_capture(captured)
                bodies = _drain_fetch_hook(driver, captured)
                bodies.extend(_drain_cdp_sniffer_bodies())
                if pending_req:
                    bodies.extend(_fetch_response_bodies(driver, pending_req))

                # Ingest scroll GraphQL responses (on_response in crawl_graphql.py) /
                # Ingest response GraphQL khi scroll (on_response trong test)
                for text in bodies:
                    if not text or not text.strip().startswith("{"):
                        continue
                    cands, pi, err = ingest_graphql_text(
                        text, group_id=group_id, seen_page_ids=page_seen
                    )
                    if err:
                        print(f"{LOG} scroll_gql_err={err[:100]}", flush=True)
                    (
                        skip_run,
                        page_new,
                        page_skip,
                        mode,
                        cursor_switch,
                        hit_bottom,
                    ) = _append_candidates(
                        cands=cands,
                        group_id=group_id,
                        seen=seen,
                        new_rows=new_rows,
                        batch_size=batch_size,
                        bottom_year=bottom_year,
                        skip_run=skip_run,
                        skip_streak=skip_streak,
                        mode=mode,
                        backfill_cursor=backfill_cursor,
                        bucket=bucket,
                        source_prefix=source_prefix,
                        extra_skip=dump_skip,
                    )
                    if cursor_switch:
                        replay_cursor = cursor_switch
                    if isinstance(pi, dict) and pi.get("end_cursor"):
                        cursor_from_scroll = str(pi.get("end_cursor"))
                        backfill_cursor = cursor_from_scroll
                        has_next = bool(pi.get("has_next_page", True))
                    if page_new or page_skip:
                        print(
                            f"{LOG} scroll_page +{page_new} skip={page_skip} "
                            f"cursor_ok={bool(cursor_from_scroll)}",
                            flush=True,
                        )
                    if hit_bottom:
                        reached_2013 = True
                        stop_reason = f"reached_bottom_year_{bottom_year}"
                        has_next = False
                        break
                    if len(new_rows) >= batch_size:
                        break

                if reached_2013 or len(new_rows) >= batch_size:
                    break

                if len(new_rows) > prev_total:
                    barren = 0
                    prev_total = len(new_rows)
                else:
                    barren += 1
                    if barren in (10, 20):
                        print(f"{LOG} {barren} barren scrolls", flush=True)

                if round_idx % 5 == 0:
                    print(
                        f"{LOG} scroll round={round_idx} "
                        f"captured={_capture_is_valid(captured, group_id)} "
                        f"cursor={bool(cursor_from_scroll)} new={len(new_rows)}",
                        flush=True,
                    )

            # NEVER invent capture from HTML (that produced group_id as doc_id) /
            # KHÔNG BAO GIỜ bịa capture từ HTML (đã sinh group_id làm doc_id)
            if not _capture_is_valid(captured, group_id):
                print(
                    f"{LOG} missing_graphql_capture — need real "
                    f"{PAGINATION_QUERY} network request (no HTML invent)",
                    flush=True,
                )
                stop_reason = "missing_graphql_capture"
                has_next = False
            else:
                _save_capture(bucket, source_prefix, group_id, captured)
                print(
                    f"{LOG} Captured pagination query "
                    f"friendly={(captured.get('form') or {}).get('fb_api_req_friendly_name')}",
                    flush=True,
                )

        # ---- REPLAY mode: in-page fetch (crawl_graphql.py replay_fetch) ----
        # ---- REPLAY: fetch trong page (crawl_graphql.py) ----
        if (
            _capture_is_valid(captured, group_id)
            and not reached_2013
            and len(new_rows) < batch_size
        ):
            print(f"{LOG} REPLAY mode — in-page fetch, DOM frozen", flush=True)
            # Prefer saved cursor after recapture; only fall back to scroll cursor /
            # Ưu tiên cursor đã lưu sau recapture; chỉ fallback cursor lúc scroll
            if replay_cursor is None:
                replay_cursor = cursor_from_scroll or checkpoint.get("cursor")
            print(
                f"{LOG} replay_cursor={'yes' if replay_cursor else 'newest'} "
                f"mode={mode} has_next={has_next}",
                flush=True,
            )
            if replay_cursor and not reached_2013:
                has_next = True

            fail_pages = 0
            replay_reloads = 0

            def _recapture_form() -> bool:
                """Reload feed and scroll briefly for a fresh pagination form.
                Reload feed và scroll ngắn để bắt form phân trang mới.
                """
                _clear_stored_capture(bucket, source_prefix, group_id)
                captured.clear()
                _strip_offscreen_media(driver)
                try:
                    driver.get(group_feed)
                except Exception:
                    pass
                _human_pause(7.5, 1.5, 1.5)
                _enable_network_cdp(driver)
                _install_fetch_hook(driver)
                for round_idx in range(1, 16):
                    if round_idx % 3 == 0:
                        _strip_offscreen_media(driver)
                    try:
                        driver.execute_script(
                            "window.scrollTo(0, document.body.scrollHeight);"
                        )
                    except Exception:
                        pass
                    time.sleep(random.uniform(*SCROLL_PAUSE))
                    _drain_perf_capture(driver, captured, pending_req)
                    _drain_cdp_sniffer_capture(captured)
                    _drain_fetch_hook(driver, captured)
                    if _capture_is_valid(captured, group_id):
                        _save_capture(bucket, source_prefix, group_id, captured)
                        print(f"{LOG} recaptured pagination form", flush=True)
                        return True
                return False

            def _handle_dead_replay() -> bool:
                """True = stop replay loop (cursor kept for next DAG run).
                True = dừng replay (giữ cursor cho DAG run sau).
                """
                nonlocal fail_pages, replay_reloads, stop_reason, has_next
                fail_pages += 1
                if fail_pages < 3:
                    time.sleep(5 * fail_pages)
                    return False
                if replay_reloads >= MAX_REPLAY_RELOADS:
                    stop_reason = "graphql_replay_fail"
                    has_next = True
                    print(
                        f"{LOG} graphql_replay_fail after {replay_reloads} recaptures "
                        "— keep cursor, should_continue next run",
                        flush=True,
                    )
                    return True
                replay_reloads += 1
                fail_pages = 0
                print(
                    f"{LOG} replay broken — recapture #{replay_reloads} keep cursor",
                    flush=True,
                )
                if not _recapture_form():
                    stop_reason = "graphql_replay_fail"
                    has_next = True
                    return True
                return False

            while len(new_rows) < batch_size and has_next and not reached_2013:
                _strip_offscreen_media(driver)
                status, text = _replay_fetch(driver, captured, replay_cursor)
                if _is_throttle_replay(status, text or "", None):
                    wait_s = min(180, 45)
                    print(
                        f"{LOG} replay_throttled status={status} sleep={wait_s}s",
                        flush=True,
                    )
                    time.sleep(wait_s)
                    continue
                if status != 200 or not text or text.startswith("replay_err"):
                    print(
                        f"{LOG} replay_fail status={status} "
                        f"text={(text or '')[:80]} fails={fail_pages + 1}",
                        flush=True,
                    )
                    if _handle_dead_replay():
                        break
                    continue

                cands, page_info, err = ingest_graphql_text(
                    text, group_id=group_id, seen_page_ids=page_seen
                )
                if err:
                    print(f"{LOG} gql_err={err[:120]}", flush=True)

                # crawl_graphql.py: no page_info → bad response /
                # crawl_graphql.py: không page_info → response hỏng
                if page_info is None:
                    print(
                        f"{LOG} replay bad response (no page_info) "
                        f"status={status} len={len(text)} fails={fail_pages + 1}",
                        flush=True,
                    )
                    if _handle_dead_replay():
                        break
                    continue
                fail_pages = 0

                (
                    skip_run,
                    page_new,
                    page_skip,
                    mode,
                    cursor_switch,
                    hit_bottom,
                ) = _append_candidates(
                    cands=cands,
                    group_id=group_id,
                    seen=seen,
                    new_rows=new_rows,
                    batch_size=batch_size,
                    bottom_year=bottom_year,
                    skip_run=skip_run,
                    skip_streak=skip_streak,
                    mode=mode,
                    backfill_cursor=backfill_cursor,
                    bucket=bucket,
                    source_prefix=source_prefix,
                    extra_skip=dump_skip,
                )
                if cursor_switch:
                    replay_cursor = cursor_switch
                if hit_bottom:
                    reached_2013 = True
                    stop_reason = f"reached_bottom_year_{bottom_year}"
                    has_next = False

                end_cursor = page_info.get("end_cursor") if isinstance(page_info, dict) else None
                page_has_next = (
                    bool(page_info.get("has_next_page"))
                    if isinstance(page_info, dict)
                    else False
                )
                print(
                    f"{LOG} page cursor_ok={bool(end_cursor)} new={page_new} "
                    f"skip={page_skip} has_next={page_has_next} mode={mode}",
                    flush=True,
                )

                if end_cursor:
                    backfill_cursor = str(end_cursor)
                    replay_cursor = str(end_cursor)
                has_next = page_has_next and bool(end_cursor)
                if not has_next and stop_reason == "batch_full":
                    stop_reason = "feed_exhausted"

                if bucket and source_prefix and seen:
                    save_seen_ids(bucket, source_prefix, group_id, seen)

                time.sleep(random.uniform(*REPLAY_PAUSE))

    finally:
        # Best-effort persist if Airflow sends SIGTERM on timeout /
        # Ghi seen nếu Airflow gửi SIGTERM khi timeout
        try:
            if seen:
                save_seen_ids(bucket, source_prefix, group_id, seen)
        except Exception as exc:
            print(f"{LOG} finally_save_seen err={type(exc).__name__}", flush=True)
        _stop_cdp_graphql_sniffer()
        _safe_quit_driver(driver)
        _drain_selenium_sessions(remote)

    batch_seq = int(checkpoint.get("batch_seq") or 0) + 1
    # Stamp batch_seq on every discover row / Gắn batch_seq lên mọi dòng discover
    for row in new_rows:
        row["batch_seq"] = batch_seq
        row["schema_version"] = SCHEMA_CRAWL
    discover_key = discover_batch_key(source_prefix, group_id, batch_seq)
    write_jsonl(bucket, discover_key, new_rows)
    # Task.xlsx B1 export upsert (discover may already download valid) /
    # Upsert file nộp B1 (discover có thể đã tải valid)
    upsert_task_exports_with_shared(
        bucket=bucket,
        source_prefix=source_prefix,
        group_id=group_id,
        records=new_rows,
    )
    set_active_batch(
        bucket,
        source_prefix,
        group_id,
        batch_seq=batch_seq,
        discover_key=discover_key,
        new_count=len(new_rows),
    )
    save_seen_ids(bucket, source_prefix, group_id, seen)

    n_valid = sum(1 for r in new_rows if r.get("valid"))
    n_invalid = len(new_rows) - n_valid
    stats = dict(checkpoint.get("stats") or {})
    stats["seen"] = len(seen)
    stats["last_discover_new"] = len(new_rows)
    stats["last_discover_batch_seq"] = batch_seq
    stats["last_discover_valid"] = n_valid
    stats["last_discover_invalid"] = n_invalid

    should_continue = (not reached_2013) and stop_reason not in {
        "missing_cookies",
        "missing_graphql_capture",
        "feed_exhausted",
    }
    if stop_reason == "graphql_replay_fail":
        # Keep going next DAG run after recapture / Chạy DAG sau sau khi recapture
        should_continue = not reached_2013
        has_next = True
    if len(new_rows) == 0 and not reached_2013 and stop_reason != "missing_graphql_capture":
        should_continue = True
        has_next = True
        if stop_reason == "batch_full":
            stop_reason = "empty_replay"

    if stop_reason == "missing_graphql_capture":
        should_continue = False

    checkpoint.update(
        {
            "mode": mode,
            "cursor": replay_cursor,
            "backfill_cursor": backfill_cursor,
            "has_next": has_next,
            "batch_seq": batch_seq,
            "reached_bottom_year": reached_2013,
            "should_continue": should_continue,
            "stop_reason": stop_reason,
            "stats": stats,
            "last_run_id": resolved_run_id,
        }
    )
    save_checkpoint(bucket, source_prefix, group_id, checkpoint)

    print(
        f"{LOG} end batch_seq={batch_seq} new={len(new_rows)} "
        f"valid={n_valid} invalid={n_invalid} schema={SCHEMA_CRAWL} "
        f"reached_2013={reached_2013} should_continue={should_continue} "
        f"reason={stop_reason}",
        flush=True,
    )
    print(
        f"{LOG_PIPELINE} discover_done batch_seq={batch_seq} new={len(new_rows)} "
        f"valid={n_valid} invalid={n_invalid}",
        flush=True,
    )
    return {
        "ok": stop_reason != "missing_graphql_capture",
        "batch_seq": batch_seq,
        "new_count": len(new_rows),
        "valid": n_valid,
        "invalid": n_invalid,
        "discover_key": discover_key,
        "should_continue": should_continue,
        "stop_reason": stop_reason,
        "reached_bottom_year": reached_2013,
        "schema_version": SCHEMA_CRAWL,
    }
