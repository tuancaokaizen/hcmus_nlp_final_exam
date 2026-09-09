"""JSONL seed ingest → discover batch (CDN live vs Selenium enrich).

Ingest file JSONL seed → batch discover (CDN sống vs Selenium enrich).
"""
from __future__ import annotations

import base64
import json
import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

from fen_crawl_common import (
    SCHEMA_CRAWL,
    build_discover_row,
    discover_batch_key,
    ensure_raw_bucket,
    load_checkpoint,
    load_seen_ids,
    permalink_for,
    save_checkpoint,
    save_seen_ids,
    set_active_batch,
    upsert_task_exports_with_shared,
    write_jsonl,
)

LOG = "[fen_jsonl_ingest]"
DEFAULT_JSONL_PATH = "/opt/fen-exam/seeds/input.jsonl"
_VK_RE = re.compile(r"VK:(\d+)", re.IGNORECASE)


def decode_post_id(raw: str, *, encoding: str = "auto") -> str:
    """Decode plain digits or base64 ``…VK:{id}`` post_id.
    Giải mã post_id dạng số thuần hoặc base64 ``…VK:{id}``.
    """
    text = str(raw or "").strip()
    if not text:
        raise ValueError("empty post_id")

    enc = (encoding or "auto").strip().lower()
    if enc == "plain" or (enc == "auto" and text.isdigit()):
        return text

    # Try base64 (pad if needed) / Thử base64 (pad nếu thiếu)
    candidates = [text]
    pad = "=" * ((4 - len(text) % 4) % 4)
    if pad:
        candidates.append(text + pad)
    for cand in candidates:
        try:
            decoded = base64.b64decode(cand).decode("utf-8", errors="replace")
        except Exception:
            continue
        m = _VK_RE.search(decoded)
        if m:
            return m.group(1)
        digits = "".join(ch for ch in decoded if ch.isdigit())
        if digits and len(digits) >= 8:
            return digits

    if text.isdigit():
        return text
    raise ValueError(f"cannot decode post_id={text[:48]}")


def probe_cdn_url(url: str, *, timeout: float = 8.0) -> bool:
    """Return True when CDN URL still serves an image.
    True khi URL CDN vẫn phục vụ được ảnh.
    """
    if not url or not str(url).startswith(("http://", "https://")):
        return False
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    }
    try:
        # Prefer HEAD; some FB CDNs reject it → fall back to ranged GET /
        # Ưu tiên HEAD; một số CDN FB từ chối → fallback GET có Range
        resp = requests.head(url, timeout=timeout, allow_redirects=True, headers=headers)
        if resp.status_code == 200:
            ctype = (resp.headers.get("Content-Type") or "").lower()
            if "image" in ctype or not ctype:
                return True
        if resp.status_code in {403, 405, 400}:
            resp = requests.get(
                url,
                timeout=timeout,
                allow_redirects=True,
                headers={**headers, "Range": "bytes=0-1023"},
                stream=True,
            )
            try:
                if resp.status_code in {200, 206}:
                    chunk = next(resp.iter_content(1024), b"")
                    return bool(chunk)
            finally:
                resp.close()
        return False
    except Exception:
        return False


def _resolve_jsonl_path(explicit: str | None = None) -> Path:
    """Resolve seed JSONL path (env / default / repo-relative).
    Resolve đường dẫn JSONL seed (env / mặc định / relative repo).
    """
    raw = (
        (explicit or "").strip()
        or os.environ.get("FEN_JSONL_PATH", "").strip()
        or DEFAULT_JSONL_PATH
    )
    path = Path(raw)
    if path.is_file():
        return path

    # Host bind may differ from container default / Bind host có thể khác default container
    repo_candidates = [
        Path(__file__).resolve().parents[2] / "seeds" / "input.jsonl",
        Path("/opt/fen-exam/seeds/input.jsonl"),
    ]
    for cand in repo_candidates:
        if cand.is_file():
            return cand
    return path


def _load_seed_rows(path: Path) -> list[dict[str, Any]]:
    """Parse JSONL seed file into dict rows / Parse file JSONL seed thành list dict."""
    if not path.is_file():
        raise FileNotFoundError(
            f"Seed JSONL not found: {path}. "
            "Copy seeds/input.example.jsonl → seeds/input.jsonl and fill posts."
        )
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            text = line.strip()
            if not text or text.startswith("#"):
                continue
            try:
                obj = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_no}: invalid JSON: {exc}") from exc
            if not isinstance(obj, dict):
                raise ValueError(f"{path}:{line_no}: each line must be a JSON object")
            rows.append(obj)
    return rows


def _row_images(raw: dict[str, Any]) -> list[str]:
    """Collect image URLs from seed aliases / Lấy URL ảnh từ các alias seed."""
    images = raw.get("images") or raw.get("image_urls") or raw.get("cdn_urls") or []
    if isinstance(images, str):
        images = [images]
    out: list[str] = []
    seen: set[str] = set()
    for item in images:
        url = str(item or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def _row_caption(raw: dict[str, Any]) -> str:
    return str(raw.get("label") or raw.get("caption") or "").strip()


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def run_jsonl_ingest(
    *,
    group_id: str,
    jsonl_path: str | None = None,
    limit: int = 0,
    encoding: str = "auto",
    skip_seen: bool = True,
    run_id: str | None = None,
    cdn_timeout: float = 8.0,
) -> dict[str, Any]:
    """Ingest seed JSONL into a discover batch for enrich/download.
    Ingest JSONL seed thành batch discover cho enrich/download.
    """
    gid = str(group_id or "").strip()
    if not gid:
        raise ValueError("group_id is required")

    path = _resolve_jsonl_path(jsonl_path)
    seed_rows = _load_seed_rows(path)
    print(f"{LOG} path={path} seed_lines={len(seed_rows)} group_id={gid}", flush=True)

    bucket, source_prefix = ensure_raw_bucket()
    checkpoint = load_checkpoint(bucket, source_prefix, gid)
    seen = load_seen_ids(bucket, source_prefix, gid)

    max_n = int(limit or 0)
    if max_n <= 0:
        max_n = int(os.environ.get("FEN_BATCH_TARGET", "0").strip() or "0")
    if max_n <= 0:
        max_n = len(seed_rows)

    batch_seq = int(checkpoint.get("batch_seq") or 0) + 1
    discover_rows: list[dict[str, Any]] = []
    stats = {
        "seed_total": len(seed_rows),
        "selected": 0,
        "cdn_live": 0,
        "cdn_expired": 0,
        "skipped_seen": 0,
        "skipped_bad": 0,
        "decode_errors": 0,
    }

    for raw in seed_rows:
        if len(discover_rows) >= max_n:
            break
        try:
            post_id = decode_post_id(
                str(raw.get("post_id") or ""),
                encoding=str(raw.get("post_id_encoding") or encoding),
            )
        except ValueError as exc:
            stats["decode_errors"] += 1
            stats["skipped_bad"] += 1
            print(f"{LOG} skip decode_err={exc}", flush=True)
            continue

        if skip_seen and post_id in seen:
            stats["skipped_seen"] += 1
            continue

        caption = _row_caption(raw)
        seed_urls = _row_images(raw)
        live_urls = [u for u in seed_urls if probe_cdn_url(u, timeout=cdn_timeout)]
        expired_urls = [u for u in seed_urls if u not in live_urls]

        permalink = str(raw.get("permalink") or "").strip() or permalink_for(gid, post_id)
        author = str(raw.get("author") or raw.get("from_id") or "").strip()
        posted_at = raw.get("posted_at") or raw.get("created_time")

        if live_urls:
            # CDN still live → discover-complete row; enrich will gate+download /
            # CDN còn sống → dòng discover đủ; enrich sẽ gate+download
            row = build_discover_row(
                group_id=gid,
                post_id=post_id,
                permalink=permalink,
                caption=caption,
                image_urls=live_urls,
                posted_at=posted_at,
                author=author,
                source="jsonl_cdn",
                batch_seq=batch_seq,
            )
            row["cdn_probe"] = "live"
            row["cdn_urls_expired"] = expired_urls[:20]
            stats["cdn_live"] += 1
        else:
            # All CDN expired → leave incomplete so Selenium enrich refetches /
            # CDN hết hạn → incomplete để Selenium enrich lấy lại
            row = build_discover_row(
                group_id=gid,
                post_id=post_id,
                permalink=permalink,
                caption=caption,
                image_urls=[],
                posted_at=posted_at,
                author=author,
                source="jsonl_expired",
                batch_seq=batch_seq,
            )
            row["valid"] = False
            row["graphql_complete"] = False
            row["invalid_reason"] = "cdn_expired" if seed_urls else (row.get("invalid_reason") or "missing_image")
            row["cdn_probe"] = "expired" if seed_urls else "missing"
            row["cdn_urls_expired"] = seed_urls[:20]
            row["needs_enrich"] = True
            stats["cdn_expired"] += 1

        row["schema_version"] = SCHEMA_CRAWL
        row["seed_source_path"] = str(path)
        if run_id:
            row["run_id"] = run_id
        # Keep host path of CDN for debug / Giữ host CDN để debug
        if live_urls:
            row["cdn_host"] = urlparse(live_urls[0]).netloc

        discover_rows.append(row)
        seen.add(post_id)
        stats["selected"] += 1
        print(
            f"{LOG} +post={post_id} probe={row.get('cdn_probe')} "
            f"images={row.get('image_count')} valid={row.get('valid')}",
            flush=True,
        )

    if not discover_rows:
        raise ValueError(
            f"No posts ingested from {path} "
            f"(seen_skip={stats['skipped_seen']} bad={stats['skipped_bad']} "
            f"decode_err={stats['decode_errors']}). "
            "Add lines to seeds/input.jsonl or set skip_seen=false."
        )

    discover_key = discover_batch_key(source_prefix, gid, batch_seq)
    write_jsonl(bucket, discover_key, discover_rows)
    # Audit copy of selected seed rows / Bản audit các dòng seed đã chọn
    seed_audit_key = f"{source_prefix}/{gid}/crawl/seed/batches/batch_{batch_seq:06d}.jsonl"
    write_jsonl(bucket, seed_audit_key, discover_rows)

    upsert_task_exports_with_shared(
        bucket=bucket,
        source_prefix=source_prefix,
        group_id=gid,
        records=discover_rows,
    )
    set_active_batch(
        bucket,
        source_prefix,
        gid,
        batch_seq=batch_seq,
        discover_key=discover_key,
        new_count=len(discover_rows),
    )
    save_seen_ids(bucket, source_prefix, gid, seen)

    cp_stats = dict(checkpoint.get("stats") or {})
    cp_stats["last_jsonl_ingest"] = {
        **stats,
        "batch_seq": batch_seq,
        "path": str(path),
    }
    checkpoint.update(
        {
            "batch_seq": batch_seq,
            # Seed ingest is one-shot — do not rollover crawl /
            # Ingest seed one-shot — không rollover crawl
            "should_continue": False,
            "stop_reason": "jsonl_ingest_done",
            "stats": cp_stats,
            "last_run_id": run_id,
        }
    )
    save_checkpoint(bucket, source_prefix, gid, checkpoint)

    result = {
        "batch_seq": batch_seq,
        "discover_key": discover_key,
        "seed_audit_key": seed_audit_key,
        "new_count": len(discover_rows),
        "cdn_live": stats["cdn_live"],
        "cdn_expired": stats["cdn_expired"],
        "skipped_seen": stats["skipped_seen"],
        "path": str(path),
    }
    print(f"{LOG} done {result}", flush=True)
    return result


if __name__ == "__main__":
    run_jsonl_ingest(
        group_id=os.environ.get("FEN_GROUP_ID", "").strip(),
        jsonl_path=os.environ.get("FEN_JSONL_PATH"),
        limit=int(os.environ.get("FEN_JSONL_LIMIT", "0").strip() or "0"),
        encoding=os.environ.get("FEN_JSONL_ENCODING", "auto").strip() or "auto",
        skip_seen=_as_bool(os.environ.get("FEN_JSONL_SKIP_SEEN"), default=True),
        run_id=os.environ.get("FEN_RUN_ID") or None,
    )
