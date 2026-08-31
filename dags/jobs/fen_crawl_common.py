"""Shared MinIO paths + checkpoint for Final Exam NLP V5 pipeline.

Đường dẫn MinIO + checkpoint dùng chung pipeline V5 (3 job, loop ở DAG).
"""
from __future__ import annotations

import json
import os
from typing import Any

from common.chau_ban_schema import utc_now_iso
from common.io_storage import ensure_bucket, get_minio_client, upload_text_payload

from final_exam_nlp_crawl_runner import (
    FB_BASE_URL,
    _atomic_upload_json,
    _group_root,
    _progress_key,
    _read_json_object,
    _settings,
)

SCHEMA_CRAWL = "5.1"
LOG_PIPELINE = "[fen_pipeline]"
DEFAULT_BATCH_SIZE = 200
DEFAULT_SKIP_STREAK = 80
DEFAULT_SOFT_RESTART_EVERY = 10
DEFAULT_BOTTOM_YEAR = 2013
DEFAULT_ROLLOVER_COOLDOWN_SEC = 180


def permalink_for(group_id: str, post_id: str) -> str:
    """Group permalink URL / URL permalink group."""
    return f"{FB_BASE_URL}/groups/{group_id}/permalink/{post_id}/"


def incomplete_reason(caption: str, image_urls: list[str]) -> str | None:
    """Map missing fields to invalid_reason / Map field thiếu → invalid_reason."""
    has_cap = bool((caption or "").strip())
    has_img = bool(image_urls)
    if has_cap and has_img:
        return None
    if not has_cap and not has_img:
        return "missing_caption_and_image"
    if not has_cap:
        return "missing_caption"
    return "missing_image"


def build_discover_row(
    *,
    group_id: str,
    post_id: str,
    permalink: str = "",
    caption: str = "",
    image_urls: list[str] | None = None,
    posted_at: Any = None,
    author: str = "",
    video_count: int = 0,
    source: str = "graphql",
    batch_seq: int = 0,
) -> dict[str, Any]:
    """Build Job1 schema 5.1 row with valid flag.
    Dự dòng Job1 schema 5.1 kèm cờ valid.
    """
    from final_exam_nlp_crawl_runner import _classify_record, _dedupe_image_urls, _is_video_url

    images = _dedupe_image_urls(
        [u for u in (image_urls or []) if u and not _is_video_url(str(u))]
    )
    caption = (caption or "").strip()
    author = (author or "").strip()
    is_valid, reason = _classify_record(caption, images)
    if not is_valid:
        # Align discover reason names with schema proposal /
        # Đồng bộ tên reason discover với schema đã chốt
        mapping = {
            "missing_label": "missing_caption",
            "missing_label_and_image": "missing_caption_and_image",
        }
        reason = mapping.get(reason or "", reason) or incomplete_reason(caption, images)
    graphql_complete = bool(caption) and bool(images)
    return {
        "schema_version": SCHEMA_CRAWL,
        "post_id": post_id,
        "permalink": permalink or permalink_for(group_id, post_id),
        "posted_at": posted_at,
        "author": author,
        "caption": caption,
        "image_urls": images,
        "image_count": len(images),
        "video_count": int(video_count or 0),
        "valid": bool(is_valid),
        "invalid_reason": None if is_valid else (reason or incomplete_reason(caption, images)),
        "graphql_complete": graphql_complete,
        "source": source,
        "discovered_at": utc_now_iso(),
        "batch_seq": int(batch_seq or 0),
    }


def crawl_root(source_prefix: str, group_id: str) -> str:
    return f"{_group_root(source_prefix, group_id)}/crawl"


def checkpoint_key(source_prefix: str, group_id: str) -> str:
    return f"{crawl_root(source_prefix, group_id)}/checkpoint.json"


def seen_ids_key(source_prefix: str, group_id: str) -> str:
    return f"{crawl_root(source_prefix, group_id)}/discover/seen_post_ids.json"


def discover_batch_key(source_prefix: str, group_id: str, batch_seq: int) -> str:
    return f"{crawl_root(source_prefix, group_id)}/discover/batches/batch_{batch_seq:06d}.jsonl"


def enrich_batch_key(source_prefix: str, group_id: str, batch_seq: int) -> str:
    return f"{crawl_root(source_prefix, group_id)}/enrich/batches/batch_{batch_seq:06d}.jsonl"


def enrich_valid_key(source_prefix: str, group_id: str, post_id: str) -> str:
    return f"{crawl_root(source_prefix, group_id)}/enrich/valid/{post_id}.json"


def enrich_invalid_key(source_prefix: str, group_id: str, post_id: str) -> str:
    return f"{crawl_root(source_prefix, group_id)}/enrich/invalid/{post_id}.json"


def ocr_queue_key(source_prefix: str, group_id: str, batch_seq: int) -> str:
    """Per-batch OCR input (caption + downloaded images).
    Input OCR theo batch (caption + ảnh đã tải).
    """
    return f"{_group_root(source_prefix, group_id)}/ocr/queue/batch_{batch_seq:06d}.jsonl"


def ocr_batch_result_key(source_prefix: str, group_id: str, batch_seq: int) -> str:
    """Per-batch Task B2 OCR output / Output OCR Task B2 theo batch."""
    return f"{_group_root(source_prefix, group_id)}/ocr/batches/batch_{batch_seq:06d}.jsonl"


def download_log_key(source_prefix: str, group_id: str) -> str:
    return f"{crawl_root(source_prefix, group_id)}/download/log.jsonl"


def result_source_prefix(operational_prefix: str) -> str:
    """Shared crawl/OCR prefix when FEN_RESULT_PREFIX is set.
    Prefix kết quả crawl/OCR chung khi có FEN_RESULT_PREFIX.
    """
    shared = os.environ.get("FEN_RESULT_PREFIX", "").strip()
    return shared or operational_prefix


def ocr_shared_batch_seq(operational_batch_seq: int) -> int:
    """Offset OCR batch ids so seed2 does not overwrite A's queue files.
    Offset batch OCR để seed2 không ghi đè file queue của A.
    """
    try:
        offset = int(os.environ.get("FEN_OCR_BATCH_OFFSET", "0") or 0)
    except (TypeError, ValueError):
        offset = 0
    return offset + int(operational_batch_seq)


def upsert_task_exports_with_shared(
    *,
    bucket: str,
    source_prefix: str,
    group_id: str,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    """Upsert operational prefix, then shared facebook export if configured.
    Upsert prefix vận hành, rồi file nộp chung facebook nếu đã cấu hình.
    """
    stats = upsert_task_exports(
        bucket=bucket,
        source_prefix=source_prefix,
        group_id=group_id,
        records=records,
    )
    shared = os.environ.get("FEN_RESULT_PREFIX", "").strip()
    if shared and shared != source_prefix:
        shared_stats = upsert_task_exports(
            bucket=bucket,
            source_prefix=shared,
            group_id=group_id,
            records=records,
        )
        print(
            f"{LOG_PIPELINE} shared_export prefix={shared} "
            f"valid={shared_stats.get('valid_count')} "
            f"invalid={shared_stats.get('invalid_count')}",
            flush=True,
        )
    return stats


def write_ocr_queue(
    *,
    bucket: str,
    source_prefix: str,
    group_id: str,
    batch_seq: int,
    records: list[dict[str, Any]],
) -> int:
    """Write OCR queue rows that have caption + local images.

    Ghi hàng đợi OCR cho post đủ caption + ảnh local.
    """
    rows: list[dict[str, Any]] = []
    for record in records:
        row = record_to_task_export_row(record, group_id=group_id)
        if not row.get("is_valid"):
            continue
        images = [str(x) for x in (row.get("images") or []) if str(x).strip()]
        if not images:
            continue
        row["batch_seq"] = int(batch_seq)
        row["images"] = images
        rows.append(row)
    write_jsonl(bucket, ocr_queue_key(source_prefix, group_id, batch_seq), rows)
    print(
        f"{LOG_PIPELINE} ocr_queue batch_seq={batch_seq} posts={len(rows)}",
        flush=True,
    )
    return len(rows)


def task_export_valid_key(source_prefix: str, group_id: str) -> str:
    """Task.xlsx B1 deliverable — valid_post.jsonl (group export/).
    File nộp Task.xlsx B1 — valid_post.jsonl.
    """
    return f"{_group_root(source_prefix, group_id)}/export/valid_post.jsonl"


def task_export_invalid_key(source_prefix: str, group_id: str) -> str:
    """Task.xlsx B1 deliverable — invalid_post.jsonl.
    File nộp Task.xlsx B1 — invalid_post.jsonl.
    """
    return f"{_group_root(source_prefix, group_id)}/export/invalid_post.jsonl"


def allow_empty_label() -> bool:
    """Accept a handwritten image with no caption as a valid deliverable row.

    Chấp nhận ảnh viết tay không có caption vẫn là dòng deliverable hợp lệ.
    """
    return (os.environ.get("FEN_ALLOW_EMPTY_LABEL", "").strip().lower()) in {
        "1",
        "true",
        "yes",
        "on",
    }


def record_to_task_export_row(record: dict[str, Any], *, group_id: str) -> dict[str, Any]:
    """Map schema 5.1 → Task.xlsx B1 row ({label, images} + upsert keys).

    Map schema 5.1 → dòng Task.xlsx B1 ({label, images} + khóa upsert).
    """
    from final_exam_nlp_crawl_runner import _is_content_caption, _is_ui_noise_label

    label = str(record.get("caption") or record.get("label") or "").strip()
    images = list(record.get("image_local_keys") or record.get("images") or [])
    images = [str(x) for x in images if str(x).strip()]
    pid = str(record.get("post_id") or "").strip()
    label_ok = bool(label) and not _is_ui_noise_label(label) and _is_content_caption(label)
    # Caption-less rows only pass when the caller opted in (cdn-refetch pod) /
    # Dòng không caption chỉ pass khi caller bật cờ (pod cdn-refetch)
    if not label and allow_empty_label():
        label_ok = True
    # Task B1 valid = đủ label + image file / B1 hợp lệ = đủ label + ảnh đã lưu
    is_deliverable_valid = bool(record.get("valid")) and label_ok and bool(images)
    row = {
        "label": label if label_ok else ("" if _is_ui_noise_label(label) else label),
        "images": images if is_deliverable_valid else images,
        "post_id": pid,
        "post_link": str(record.get("permalink") or record.get("post_link") or ""),
        "group_id": group_id,
        "schema_version": SCHEMA_CRAWL,
        "is_valid": is_deliverable_valid,
        "updated_at": record.get("updated_at") or record.get("downloaded_at") or utc_now_iso(),
    }
    if not is_deliverable_valid:
        reason = record.get("invalid_reason") or record.get("reason")
        if label and _is_ui_noise_label(label):
            reason = "ui_noise_caption"
            row["label"] = ""
        elif bool(record.get("valid")) and label_ok and not images:
            reason = reason or "missing_downloaded_images"
        row["invalid_reason"] = reason
    return row


def _load_task_export_map(bucket: str, object_key: str) -> dict[str, dict[str, Any]]:
    """Load JSONL export into post_id → row map / Đọc JSONL export → map post_id."""
    out: dict[str, dict[str, Any]] = {}
    for row in read_jsonl(bucket, object_key):
        pid = str(row.get("post_id") or "").strip()
        if pid:
            out[pid] = row
    return out


def upsert_task_exports(
    *,
    bucket: str,
    source_prefix: str,
    group_id: str,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    """Upsert Task.xlsx B1 valid/invalid JSONL by post_id (separate from v5/).

    Upsert valid/invalid JSONL theo Task.xlsx B1 theo post_id (tách khỏi v5/).
    """
    if not records:
        return {"valid_count": 0, "invalid_count": 0, "upserted": 0}

    valid_key = task_export_valid_key(source_prefix, group_id)
    invalid_key = task_export_invalid_key(source_prefix, group_id)
    valid_map = _load_task_export_map(bucket, valid_key)
    invalid_map = _load_task_export_map(bucket, invalid_key)

    upserted = 0
    for record in records:
        pid = str(record.get("post_id") or "").strip()
        if not pid:
            continue
        row = record_to_task_export_row(record, group_id=group_id)
        # Move between sides on status change / Chuyển file khi đổi valid/invalid
        valid_map.pop(pid, None)
        invalid_map.pop(pid, None)
        if row.get("is_valid"):
            valid_map[pid] = row
        else:
            invalid_map[pid] = row
        upserted += 1

    def _sorted_rows(m: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        return [m[k] for k in sorted(m.keys(), key=lambda x: int(x) if x.isdigit() else 0)]

    write_jsonl(bucket, valid_key, _sorted_rows(valid_map))
    write_jsonl(bucket, invalid_key, _sorted_rows(invalid_map))
    print(
        f"{LOG_PIPELINE} task_export upserted={upserted} "
        f"valid={len(valid_map)} invalid={len(invalid_map)} "
        f"keys={valid_key} | {invalid_key}",
        flush=True,
    )
    return {
        "valid_count": len(valid_map),
        "invalid_count": len(invalid_map),
        "upserted": upserted,
        "valid_key": valid_key,
        "invalid_key": invalid_key,
    }


def download_cdn_immediately(
    *,
    bucket: str,
    source_prefix: str,
    group_id: str,
    post_id: str,
    image_urls: list[str],
    batch_seq: int = 0,
    caption: str = "",
    log_prefix: str = LOG_PIPELINE,
) -> dict[str, Any]:
    """Download CDN bytes ASAP when post becomes valid (URLs expire fast).

    Tải byte CDN ngay khi post valid (URL scontent hết hạn nhanh).
    """
    from final_exam_nlp_crawl_runner import _store_post_images

    pid = str(post_id or "").strip()
    urls = [u for u in (image_urls or []) if isinstance(u, str) and u.startswith("http")]
    if not pid or not urls:
        return {
            "ok": False,
            "files": 0,
            "stored": [],
            "errors": ["no_cdn_urls"],
            "images_downloaded": False,
        }

    timeout = int(_settings().get("image_timeout_sec") or 30)
    stored, errors = _store_post_images(
        bucket=bucket,
        source_prefix=result_source_prefix(source_prefix),
        group_id=group_id,
        post_id=pid,
        image_urls=urls,
        timeout=timeout,
    )
    ok = bool(stored)
    reason = ",".join(errors[:3]) if errors else ""
    print(
        f"{log_prefix} download_now id={pid} ok={ok} files={len(stored)}"
        + (f" reason={reason}" if reason else ""),
        flush=True,
    )
    # Append one-line download log including caption for OCR label /
    # Ghi 1 dòng log download kèm caption để OCR lấy label
    try:
        line = json.dumps(
            {
                "post_id": pid,
                "caption": str(caption or "").strip(),
                "ok": ok,
                "files": len(stored),
                "stored": stored,
                "errors": errors[:5],
                "batch_seq": batch_seq,
                "immediate": True,
                "at": utc_now_iso(),
            },
            ensure_ascii=False,
        )
        existing = ""
        client = get_minio_client()
        try:
            resp = client.get_object(bucket, download_log_key(source_prefix, group_id))
            try:
                existing = resp.read().decode("utf-8", errors="replace")
            finally:
                resp.close()
                resp.release_conn()
        except Exception:
            existing = ""
        upload_text_payload(
            bucket,
            download_log_key(source_prefix, group_id),
            existing + line + "\n",
            suffix=".jsonl",
        )
    except Exception as exc:
        print(
            f"{log_prefix} download_log_fail id={pid} err={type(exc).__name__}",
            flush=True,
        )
    return {
        "ok": ok,
        "files": len(stored),
        "stored": stored,
        "errors": errors,
        "images_downloaded": ok,
        "image_local_keys": stored,
    }


def classify_then_maybe_download(
    record: dict[str, Any],
    *,
    bucket: str,
    source_prefix: str,
    group_id: str,
    log_prefix: str = LOG_PIPELINE,
) -> dict[str, Any]:
    """Per-image calligraphy classify → download only kept URLs.
    Classify từng ảnh → chỉ tải URL được giữ.
    """
    from final_exam_nlp_calligraphy_classify import (
        apply_calligraphy_gate,
        calligraphy_classify_enabled,
        classify_cdn_urls,
    )

    out = dict(record)
    if not out.get("valid"):
        return out

    urls = list(out.get("cdn_urls") or out.get("image_urls") or [])
    if not calligraphy_classify_enabled():
        print(f"{log_prefix} calligraphy_classify skipped (disabled)", flush=True)
        dl = download_cdn_immediately(
            bucket=bucket,
            source_prefix=source_prefix,
            group_id=group_id,
            post_id=str(out.get("post_id") or ""),
            image_urls=urls,
            batch_seq=int(out.get("batch_seq") or 0),
            caption=str(out.get("caption") or out.get("label") or ""),
            log_prefix=log_prefix,
        )
        out["images_downloaded"] = bool(dl.get("images_downloaded"))
        out["image_local_keys"] = list(dl.get("image_local_keys") or [])
        out["download_errors"] = list(dl.get("errors") or [])[:5]
        out["downloaded_at"] = utc_now_iso()
        return out

    timeout = int(_settings().get("image_timeout_sec") or 25)
    classify = classify_cdn_urls(urls, timeout=timeout)
    print(
        f"{log_prefix} calligraphy id={out.get('post_id')} "
        f"keep={classify.get('keep_as_valid')} kept={classify.get('kept_image_count')}/"
        f"{classify.get('classified_image_count')} kind={classify.get('calligraphy_kind')} "
        f"media={classify.get('media_class')} conf={classify.get('calligraphy_confidence')} "
        f"ok={classify.get('classify_ok')} err={classify.get('classify_error') or '-'}",
        flush=True,
    )
    out = apply_calligraphy_gate(out, classify)
    if not out.get("valid"):
        return out

    # Download only kept calligraphy URLs / Chỉ tải URL thư pháp được giữ
    keep_urls = list(out.get("cdn_urls") or [])
    dl = download_cdn_immediately(
        bucket=bucket,
        source_prefix=source_prefix,
        group_id=group_id,
        post_id=str(out.get("post_id") or ""),
        image_urls=keep_urls,
        batch_seq=int(out.get("batch_seq") or 0),
        caption=str(out.get("caption") or out.get("label") or ""),
        log_prefix=log_prefix,
    )
    out["images_downloaded"] = bool(dl.get("images_downloaded"))
    out["image_local_keys"] = list(dl.get("image_local_keys") or [])
    out["download_errors"] = list(dl.get("errors") or [])[:5]
    out["downloaded_at"] = utc_now_iso()
    return out


def active_batch_key(source_prefix: str, group_id: str) -> str:
    """Pointer to the batch this DAG run should process / Con trỏ batch của DAG run hiện tại."""
    return f"{crawl_root(source_prefix, group_id)}/active_batch.json"


def graphql_capture_key(source_prefix: str, group_id: str) -> str:
    return f"{_group_root(source_prefix, group_id)}/state/graphql_capture.json"


def load_checkpoint(bucket: str, source_prefix: str, group_id: str) -> dict[str, Any]:
    """Load V5 checkpoint or empty defaults / Đọc checkpoint V5 hoặc mặc định rỗng."""
    raw = _read_json_object(bucket, checkpoint_key(source_prefix, group_id)) or {}
    return {
        "schema_version": SCHEMA_CRAWL,
        "mode": str(raw.get("mode") or "catchup"),
        "cursor": raw.get("cursor"),
        "backfill_cursor": raw.get("backfill_cursor"),
        "has_next": bool(raw.get("has_next", True)),
        "batch_seq": int(raw.get("batch_seq") or 0),
        "bottom_year": int(raw.get("bottom_year") or DEFAULT_BOTTOM_YEAR),
        "reached_bottom_year": bool(raw.get("reached_bottom_year")),
        "should_continue": bool(raw.get("should_continue", True)),
        "stop_reason": str(raw.get("stop_reason") or ""),
        "stats": dict(raw.get("stats") or {}),
        "updated_at": raw.get("updated_at"),
        "last_run_id": raw.get("last_run_id"),
    }


def save_checkpoint(
    bucket: str,
    source_prefix: str,
    group_id: str,
    checkpoint: dict[str, Any],
) -> None:
    """Persist V5 checkpoint + mirror should_continue for Airflow ShortCircuit.

    Ghi checkpoint V5 + mirror should_continue cho ShortCircuit Airflow.
    """
    checkpoint = dict(checkpoint)
    checkpoint["schema_version"] = SCHEMA_CRAWL
    checkpoint["updated_at"] = utc_now_iso()
    _atomic_upload_json(bucket, checkpoint_key(source_prefix, group_id), checkpoint)
    # Keep legacy progress.json in sync for should_continue helper /
    # Đồng bộ progress.json cũ cho helper should_continue
    legacy = {
        "schema_version": SCHEMA_CRAWL,
        "crawl_mode": "split",
        "should_continue": bool(checkpoint.get("should_continue")),
        "stop_reason": checkpoint.get("stop_reason") or "",
        "graphql_cursor": checkpoint.get("cursor"),
        "backfill_cursor": checkpoint.get("backfill_cursor"),
        "mode": checkpoint.get("mode"),
        "batch_seq": checkpoint.get("batch_seq"),
        "reached_bottom_year": checkpoint.get("reached_bottom_year"),
        "stats": checkpoint.get("stats") or {},
        "updated_at": checkpoint["updated_at"],
        "last_run_id": checkpoint.get("last_run_id"),
    }
    _atomic_upload_json(bucket, _progress_key(source_prefix, group_id), legacy)


def load_seen_ids(bucket: str, source_prefix: str, group_id: str) -> set[str]:
    """Load discovered post_id set / Đọc tập post_id đã discover."""
    raw = _read_json_object(bucket, seen_ids_key(source_prefix, group_id)) or {}
    ids = raw.get("post_ids") or []
    return {str(x) for x in ids if str(x).isdigit()}


def save_seen_ids(bucket: str, source_prefix: str, group_id: str, ids: set[str]) -> None:
    payload = {
        "updated_at": utc_now_iso(),
        "count": len(ids),
        "post_ids": sorted(ids, key=lambda x: int(x) if x.isdigit() else 0),
    }
    _atomic_upload_json(bucket, seen_ids_key(source_prefix, group_id), payload)


def merge_seen_from_download_log(
    bucket: str,
    source_prefix: str,
    group_id: str,
    seen: set[str],
) -> int:
    """Add post_ids already downloaded so timeout/retry does not recrawl.
    Thêm post_id đã tải để timeout/retry không crawl lại.
    """
    before = len(seen)
    for row in read_jsonl(bucket, download_log_key(source_prefix, group_id)):
        pid = str(row.get("post_id") or "").strip()
        if pid.isdigit():
            seen.add(pid)
    added = len(seen) - before
    if added:
        save_seen_ids(bucket, source_prefix, group_id, seen)
    return added


def write_jsonl(bucket: str, object_key: str, rows: list[dict[str, Any]]) -> None:
    """Upload JSONL batch file / Upload file batch JSONL."""
    text = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
    if text:
        text += "\n"
    upload_text_payload(bucket, object_key, text)


def read_jsonl(bucket: str, object_key: str) -> list[dict[str, Any]]:
    """Download and parse JSONL / Tải và parse JSONL."""
    client = get_minio_client()
    try:
        resp = client.get_object(bucket, object_key)
        try:
            raw = resp.read().decode("utf-8", errors="replace")
        finally:
            resp.close()
            resp.release_conn()
    except Exception:
        return []
    rows: list[dict[str, Any]] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def set_active_batch(
    bucket: str,
    source_prefix: str,
    group_id: str,
    *,
    batch_seq: int,
    discover_key: str,
    new_count: int,
) -> None:
    _atomic_upload_json(
        bucket,
        active_batch_key(source_prefix, group_id),
        {
            "batch_seq": batch_seq,
            "discover_key": discover_key,
            "new_count": new_count,
            "updated_at": utc_now_iso(),
        },
    )


def get_active_batch(bucket: str, source_prefix: str, group_id: str) -> dict[str, Any]:
    return _read_json_object(bucket, active_batch_key(source_prefix, group_id)) or {}


def ensure_raw_bucket() -> tuple[str, str]:
    """Return (bucket, source_prefix) and ensure bucket exists.

    Trả (bucket, source_prefix) và đảm bảo bucket tồn tại.
    """
    settings = _settings()
    bucket = settings["bucket_raw"]
    source_prefix = settings["source_prefix"]
    ensure_bucket(get_minio_client(), bucket)
    return bucket, source_prefix
