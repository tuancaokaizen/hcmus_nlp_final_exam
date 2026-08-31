"""Job3 Download — HTTP download images for valid posts in active batch.

Job3 Download — tải ảnh HTTP cho post valid trong batch đang active.
"""
from __future__ import annotations

import json
import os
import time
from typing import Any

from common.chau_ban_schema import utc_now_iso
from common.io_storage import upload_text_payload

from final_exam_nlp_crawl_runner import (
    _settings,
    _store_post_images,
)

from fen_crawl_common import (
    LOG_PIPELINE,
    download_log_key,
    enrich_batch_key,
    ensure_raw_bucket,
    get_active_batch,
    load_checkpoint,
    load_seen_ids,
    read_jsonl,
    save_checkpoint,
    save_seen_ids,
    ocr_shared_batch_seq,
    result_source_prefix,
    upsert_task_exports_with_shared,
    write_jsonl,
    write_ocr_queue,
)

LOG = "[fen_crawl_download]"


def run_download_batch(
    *,
    group_id: str,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Download CDN images for valid enrich rows of the active batch.

    Tải ảnh CDN cho các dòng enrich valid của batch đang active.
    """
    if not group_id.strip():
        raise ValueError("group_id is required")
    resolved_run_id = (run_id or "").strip() or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())

    bucket, source_prefix = ensure_raw_bucket()
    # Images + shared Task.xlsx + OCR queue go to A prefix when set /
    # Ảnh + file nộp chung + hàng đợi OCR ghi vào prefix A khi đã set
    result_prefix = result_source_prefix(source_prefix)
    settings = _settings()
    image_timeout = int(settings["image_timeout_sec"])
    active = get_active_batch(bucket, source_prefix, group_id)
    batch_seq = int(active.get("batch_seq") or 0)
    shared_ocr_seq = ocr_shared_batch_seq(batch_seq)
    enrich_key = enrich_batch_key(source_prefix, group_id, batch_seq)
    rows = read_jsonl(bucket, enrich_key)
    valid_rows = [
        r
        for r in rows
        if r.get("valid")
        and (r.get("cdn_urls") or r.get("image_urls") or r.get("photo_urls"))
        and not r.get("images_downloaded")
    ]

    print(
        f"{LOG} start batch_seq={batch_seq} valid_queue={len(valid_rows)} "
        f"(skip already downloaded)",
        flush=True,
    )

    ok_n = 0
    fail_n = 0
    log_lines: list[str] = []
    stored_by_post: dict[str, list[str]] = {}

    for row in valid_rows:
        pid = str(row.get("post_id") or "")
        # Prefer CDN for bytes; fall back to Job1 image_urls /
        # Ưu tiên CDN; fallback image_urls từ Job1
        urls = list(row.get("cdn_urls") or row.get("image_urls") or [])
        if not urls:
            print(f"{LOG} id={pid} ok=False reason=no_cdn_urls files=0", flush=True)
            fail_n += 1
            log_lines.append(
                json.dumps(
                    {
                        "post_id": pid,
                        "ok": False,
                        "reason": "no_cdn_urls",
                        "files": 0,
                        "batch_seq": batch_seq,
                        "at": utc_now_iso(),
                    },
                    ensure_ascii=False,
                )
            )
            continue
        stored, errors = _store_post_images(
            bucket=bucket,
            source_prefix=result_prefix,
            group_id=group_id,
            post_id=pid,
            image_urls=urls,
            timeout=image_timeout,
        )
        ok = bool(stored) and not errors
        if stored and errors:
            ok = True  # partial success / thành công một phần
        if ok:
            ok_n += 1
            stored_by_post[pid] = stored
        else:
            fail_n += 1
        reason = ",".join(errors[:3]) if errors else ""
        print(
            f"{LOG} id={pid} ok={ok} files={len(stored)}"
            + (f" reason={reason}" if reason else ""),
            flush=True,
        )
        log_lines.append(
            json.dumps(
                {
                    "post_id": pid,
                    "caption": str(row.get("caption") or row.get("label") or ""),
                    "ok": ok,
                    "files": len(stored),
                    "stored": stored,
                    "errors": errors[:5],
                    "batch_seq": batch_seq,
                    "run_id": resolved_run_id,
                    "at": utc_now_iso(),
                },
                ensure_ascii=False,
            )
        )

    # Append download log / Append log download
    if log_lines:
        existing = ""
        try:
            from common.io_storage import get_minio_client

            client = get_minio_client()
            resp = client.get_object(bucket, download_log_key(source_prefix, group_id))
            try:
                existing = resp.read().decode("utf-8", errors="replace")
            finally:
                resp.close()
                resp.release_conn()
        except Exception:
            existing = ""
        text = existing + ("\n".join(log_lines) + "\n")
        upload_text_payload(
            bucket, download_log_key(source_prefix, group_id), text, suffix=".jsonl"
        )

    print(f"{LOG} refresh session_cleared", flush=True)

    # Merge downloaded keys into enrich batch, then write OCR queue /
    # Gộp key ảnh vào enrich batch, rồi ghi hàng đợi OCR
    merged: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        pid = str(item.get("post_id") or "")
        if pid in stored_by_post:
            item["image_local_keys"] = stored_by_post[pid]
            item["images_downloaded"] = True
            item["downloaded_at"] = utc_now_iso()
        merged.append(item)
    write_jsonl(bucket, enrich_key, merged)
    upsert_task_exports_with_shared(
        bucket=bucket,
        source_prefix=source_prefix,
        group_id=group_id,
        records=merged,
    )
    queue_n = write_ocr_queue(
        bucket=bucket,
        source_prefix=result_prefix,
        group_id=group_id,
        batch_seq=shared_ocr_seq,
        records=merged,
    )

    checkpoint = load_checkpoint(bucket, source_prefix, group_id)
    stats = dict(checkpoint.get("stats") or {})
    stats["downloaded"] = int(stats.get("downloaded") or 0) + ok_n
    stats["download_fail"] = int(stats.get("download_fail") or 0) + fail_n
    checkpoint["stats"] = stats
    checkpoint["last_run_id"] = resolved_run_id
    # should_continue already set by discover; enrich may have cleared it /
    # should_continue đã set bởi discover; enrich có thể đã clear
    save_checkpoint(bucket, source_prefix, group_id, checkpoint)

    union_prefix = os.environ.get("FEN_UNION_SEEN_PREFIX", "").strip()
    if union_prefix and stored_by_post:
        # After success, A GraphQL skip these ids / Sau khi tải xong, A GraphQL bỏ qua các id này
        a_seen = load_seen_ids(bucket, union_prefix, group_id)
        before = len(a_seen)
        a_seen.update(stored_by_post.keys())
        save_seen_ids(bucket, union_prefix, group_id, a_seen)
        print(
            f"{LOG} union_seen prefix={union_prefix} added={len(a_seen) - before} "
            f"a_seen={len(a_seen)}",
            flush=True,
        )

    print(
        f"{LOG} end batch_seq={batch_seq} ok={ok_n} fail={fail_n}",
        flush=True,
    )
    print(
        f"{LOG_PIPELINE} download_done batch_seq={batch_seq} ok={ok_n} fail={fail_n}",
        flush=True,
    )
    return {
        "ok": True,
        "batch_seq": batch_seq,
        "downloaded": ok_n,
        "failed": fail_n,
        "ocr_queue": queue_n,
    }
