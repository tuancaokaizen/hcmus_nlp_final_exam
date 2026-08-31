"""Retry empty OCR text with Flash-low → Qwen → Gemini 3.6-high.

Retry ảnh ground_truth rỗng: Flash-low → Qwen → Gemini 3.6-high.
Writes MinIO JSONL only (exam stack has no Qdrant). Skip+success when none.
"""
from __future__ import annotations

import json
import os
import time
from typing import Any

from common.chau_ban_schema import extract_json_object, utc_now_iso
from common.config import get_value, load_config
from common.io_storage import (
    list_objects_with_prefix,
    object_exists,
    upload_json_payload,
    upload_text_payload,
)

from final_exam_nlp_crawl_runner import _group_root, _settings
from final_exam_nlp_ocr import (
    GEMINI_PROMPT,
    SCHEMA_OCR,
    _ground_truth,
    _normalize_boxes,
    _object_key_from_image_path,
    _ocr_detail_key,
    _ocr_output_key,
    _read_object_bytes,
    _split_boxes,
    _vision_ocr,
    is_text_only_vision_model,
    preprocess_light,
)
from fen_crawl_common import ocr_batch_result_key, read_jsonl

LOG = "[fen_ocr_retry]"
DEFAULT_MODELS = (
    "gemini-3.5-flash-low",
    "gemini-3.6-flash-high",
)
HIGH_MODEL = "gemini-3.6-flash-high"


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def resolve_retry_window(
    *,
    current_batch: int,
    env_min: int,
    env_max: int,
    backfill_done: bool,
) -> tuple[int, int, bool]:
    """Compute inclusive batch window / Tính cửa sổ batch inclusive.

    First run (no marker): 1..current. Later: current only.
    Lần đầu (chưa cờ): 1..batch hiện tại. Sau đó: chỉ batch hiện tại.
    env_min/env_max override when > 0 / env > 0 thì ghi đè.
    """
    max_b = env_max if env_max > 0 else (current_batch if current_batch > 0 else 0)
    if env_min > 0:
        min_b = env_min
    elif current_batch > 0 and backfill_done:
        min_b = current_batch
    elif current_batch > 0:
        min_b = 1
    else:
        min_b = 0
    if max_b > 0 and min_b > max_b:
        min_b = max_b
    is_backfill = bool(max_b > 0 and min_b > 0 and min_b < max_b)
    return min_b, max_b, is_backfill


def in_batch_window(seq: int, min_b: int, max_b: int, *, is_backfill: bool) -> bool:
    """Whether a row's batch_seq is in the retry window / Dòng có batch_seq nằm trong cửa sổ retry."""
    if seq > 0:
        if min_b > 0 and seq < min_b:
            return False
        if max_b > 0 and seq > max_b:
            return False
        return True
    # Unknown seq: only during full backfill / Không có batch_seq: chỉ khi backfill
    return is_backfill or (min_b <= 0 and max_b <= 0)


def _retry_backfill_key(source_prefix: str, group_id: str) -> str:
    return f"{_group_root(source_prefix, group_id)}/ocr/retry_backfill_done.json"


def _retry_models() -> list[str]:
    """Model ladder from env / Bậc model từ biến môi trường."""
    raw = os.environ.get("FEN_OCR_RETRY_MODELS", "").strip()
    if raw:
        return [p.strip() for p in raw.split(",") if p.strip()]
    qwen = os.environ.get("FEN_OCR_QWEN_MODEL", "").strip() or get_value(
        load_config(), "final_exam_nlp", "qwen_ocr_model", fallback=""
    )
    flash = get_value(load_config(), "gemini_opencv", "model", fallback="gemini-3.5-flash-low")
    high = os.environ.get("FEN_OCR_RETRY_HIGH_MODEL", HIGH_MODEL).strip() or HIGH_MODEL
    models = [flash]
    if qwen and not is_text_only_vision_model(qwen):
        models.append(qwen)
    if high and high not in models:
        models.append(high)
    return [m for m in models if m] or list(DEFAULT_MODELS)


def _ocr_text_empty(record: dict[str, Any]) -> bool:
    """True when B2 OCR text is blank / True khi chữ OCR B2 trống."""
    gt = str(record.get("ground_truth") or "").strip()
    joined = "\n".join(
        str(item.get("text") or "").strip()
        for item in (record.get("gemini") or [])
        if isinstance(item, dict) and str(item.get("text") or "").strip()
    )
    return not gt and not joined


def _already_exhausted(record: dict[str, Any], high_model: str) -> bool:
    """Skip if high model already failed / Bỏ nếu đã thử model cao và vẫn trống."""
    if _bool_env("FEN_OCR_RETRY_FORCE", False):
        return False
    last = str(record.get("retry_model") or "").strip()
    return last == high_model and _ocr_text_empty(record)


def _load_ocr_by_image(bucket: str, source_prefix: str, group_id: str) -> dict[str, dict[str, Any]]:
    """Merge ocr_result + per-batch JSONL by image path / Gộp JSONL theo đường dẫn ảnh."""
    by_image: dict[str, dict[str, Any]] = {}
    keys = [_ocr_output_key(source_prefix, group_id)]
    batch_prefix = f"{_group_root(source_prefix, group_id)}/ocr/batches/"
    keys.extend(list_objects_with_prefix(bucket, batch_prefix, suffix=".jsonl"))
    seen_keys: set[str] = set()
    for key in keys:
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        for row in read_jsonl(bucket, key):
            image = str(row.get("image") or "").strip()
            if image:
                by_image[image] = row
    return by_image


def _ocr_with_models(image_bytes: bytes, models: list[str]) -> dict[str, Any]:
    """Try vision models until boxes exist / Thử lần lượt model đến khi có bbox."""
    processed = preprocess_light(image_bytes)
    last_err: str | None = None
    last_raw = ""
    engine = models[0] if models else "gemini"
    for model_name in models:
        if is_text_only_vision_model(model_name):
            print(f"{LOG} skip text-only model={model_name}", flush=True)
            continue
        print(f"{LOG} vision model={model_name}", flush=True)
        raw, err = _vision_ocr(processed, model=model_name, prompt=GEMINI_PROMPT)
        last_err = err
        last_raw = raw or ""
        parsed = extract_json_object(raw) if raw else None
        boxes = _normalize_boxes((parsed or {}).get("gemini"))
        main, side = _split_boxes(boxes)
        engine = model_name
        if main or side:
            return {
                "engine": engine,
                "retry_model": model_name,
                "gemini": boxes,
                "main_boxes": main,
                "side_boxes": side,
                "ocr_ok": True,
                "ocr_error": None,
            }
        print(f"{LOG} model={model_name} still empty err={err or '-'}", flush=True)
    return {
        "engine": engine,
        "retry_model": engine,
        "gemini": [],
        "main_boxes": [],
        "side_boxes": [],
        "ocr_ok": False,
        "ocr_error": last_err or "empty_ocr",
        "raw_preview": last_raw[:500],
    }


def _merge_jsonl(bucket: str, key: str, updates: dict[str, dict[str, Any]]) -> None:
    existing = {str(r.get("image") or ""): r for r in read_jsonl(bucket, key)}
    existing.update(updates)
    lines = [
        json.dumps(existing[k], ensure_ascii=False)
        for k in sorted(existing)
        if k
    ]
    upload_text_payload(
        bucket, key, "\n".join(lines) + ("\n" if lines else ""), suffix=".jsonl"
    )


def run_ocr_retry(
    *,
    group_id: str,
    limit: int = 80,
    batch_seq: int = 0,
) -> dict[str, Any]:
    """Retry blank OCR rows; always return ok / Retry dòng OCR trống; luôn ok."""
    if not group_id:
        raise ValueError("FEN_GROUP_ID is required")
    settings = _settings()
    bucket = settings["bucket_raw"]
    source_prefix = settings["source_prefix"]
    models = _retry_models()
    high_model = models[-1] if models else HIGH_MODEL
    by_image = _load_ocr_by_image(bucket, source_prefix, group_id)
    marker_key = _retry_backfill_key(source_prefix, group_id)
    backfill_done = object_exists(bucket, marker_key)
    min_b, max_b, is_backfill = resolve_retry_window(
        current_batch=batch_seq,
        env_min=_int_env("FEN_OCR_RETRY_MIN_BATCH", 0),
        env_max=_int_env("FEN_OCR_RETRY_MAX_BATCH", 0),
        backfill_done=backfill_done,
    )
    print(
        f"{LOG} window min_batch={min_b} max_batch={max_b} "
        f"is_backfill={is_backfill} marker={backfill_done} current={batch_seq}",
        flush=True,
    )

    empty_rows = [
        rec
        for rec in by_image.values()
        if _ocr_text_empty(rec)
        and not _already_exhausted(rec, high_model)
        and in_batch_window(
            int(rec.get("batch_seq") or 0), min_b, max_b, is_backfill=is_backfill
        )
    ]
    empty_found = sum(1 for rec in by_image.values() if _ocr_text_empty(rec))
    skipped_exhausted = empty_found - len(empty_rows)
    print(
        f"{LOG} empty_ocr_text_found={empty_found} "
        f"eligible={len(empty_rows)} skipped_already_retried={skipped_exhausted}",
        flush=True,
    )
    if not empty_rows:
        marked = False
        if is_backfill:
            # Window 1..N already clean / Cửa sổ 1..N đã hết empty
            upload_json_payload(
                bucket,
                marker_key,
                {"through_batch": max_b, "marked_at": utc_now_iso()},
            )
            marked = True
            print(f"{LOG} backfill_done through_batch={max_b} (no empty)", flush=True)
        print(
            f"{LOG} processed=0 recovered=0 still_empty=0 "
            f"(skip no empty ocr_text)",
            flush=True,
        )
        return {
            "ok": True,
            "skipped": True,
            "empty_ocr_text_found": empty_found,
            "processed": 0,
            "recovered": 0,
            "still_empty": 0,
            "min_batch": min_b,
            "max_batch": max_b,
            "is_backfill": is_backfill,
            "backfill_marked": marked,
        }

    if limit > 0:
        empty_rows = empty_rows[:limit]

    recovered: list[dict[str, Any]] = []
    processed_map: dict[str, dict[str, Any]] = {}
    still_empty = 0
    missing_img = 0
    processed = 0
    delay = float(get_value(load_config(), "gemini_opencv", "page_delay_sec", fallback="3") or "3")

    for rec in empty_rows:
        image_path = str(rec.get("image") or "").strip()
        post_id = str(rec.get("post_id") or "").strip()
        label = str(rec.get("label") or "").strip()
        object_key = _object_key_from_image_path(source_prefix, group_id, image_path)
        if not object_exists(bucket, object_key):
            missing_img += 1
            print(f"{LOG} missing image {object_key}", flush=True)
            continue
        raw_bytes = _read_object_bytes(bucket, object_key)
        ocr = _ocr_with_models(raw_bytes, models)
        gt, side_matter, gt_err = _ground_truth(
            ocr["main_boxes"], ocr["side_boxes"], label
        )
        processed += 1
        out = dict(rec)
        out.update(
            {
                "ground_truth": gt,
                "side_matter": side_matter,
                "gemini": ocr["gemini"],
                "ocr_engine": ocr["engine"],
                "ocr_ok": ocr["ocr_ok"],
                "ocr_error": ocr["ocr_error"],
                "gt_error": gt_err,
                "schema_version": SCHEMA_OCR,
                "retry_model": ocr.get("retry_model"),
                "retry_count": int(rec.get("retry_count") or 0) + 1,
                "retried_at": utc_now_iso(),
                "ocr_at": utc_now_iso(),
            }
        )
        detail_key = _ocr_detail_key(
            source_prefix, group_id, post_id or "unknown", image_path
        )
        upload_text_payload(
            bucket, detail_key, json.dumps(out, ensure_ascii=False, indent=2), suffix=".json"
        )
        by_image[image_path] = out
        processed_map[image_path] = out
        if _ocr_text_empty(out):
            still_empty += 1
            print(
                f"{LOG} still_empty post={post_id} image={image_path} "
                f"model={ocr.get('retry_model')}",
                flush=True,
            )
        else:
            recovered.append(out)
            print(
                f"{LOG} recovered post={post_id} image={image_path} "
                f"model={ocr.get('retry_model')} gt_len={len(gt)}",
                flush=True,
            )
        time.sleep(max(0.0, delay))

    if processed_map:
        out_key = _ocr_output_key(source_prefix, group_id)
        _merge_jsonl(bucket, out_key, processed_map)

        by_batch: dict[int, dict[str, dict[str, Any]]] = {}
        for rec in processed_map.values():
            seq = int(rec.get("batch_seq") or 0)
            if seq <= 0:
                continue
            by_batch.setdefault(seq, {})[str(rec.get("image") or "")] = rec
        for seq, mapping in by_batch.items():
            _merge_jsonl(
                bucket, ocr_batch_result_key(source_prefix, group_id, seq), mapping
            )

    if recovered:
        print(f"{LOG} recovered_records={len(recovered)} (minio only)", flush=True)

    remaining_eligible = [
        rec
        for rec in by_image.values()
        if _ocr_text_empty(rec)
        and not _already_exhausted(rec, high_model)
        and in_batch_window(
            int(rec.get("batch_seq") or 0), min_b, max_b, is_backfill=is_backfill
        )
    ]
    marked = False
    # Mark backfill done only when window 1..N is drained / Chỉ ghi cờ khi đã hết empty trong cửa sổ
    if is_backfill and not remaining_eligible:
        upload_json_payload(
            bucket,
            marker_key,
            {
                "through_batch": max_b,
                "marked_at": utc_now_iso(),
            },
        )
        marked = True
        print(f"{LOG} backfill_done through_batch={max_b} key={marker_key}", flush=True)

    print(
        f"{LOG} processed={processed} recovered={len(recovered)} "
        f"still_empty={still_empty} missing_img={missing_img} "
        f"empty_ocr_text_found={empty_found} remaining_eligible={len(remaining_eligible)}",
        flush=True,
    )
    return {
        "ok": True,
        "skipped": False,
        "empty_ocr_text_found": empty_found,
        "processed": processed,
        "recovered": len(recovered),
        "still_empty": still_empty,
        "missing_images": missing_img,
        "models": models,
        "min_batch": min_b,
        "max_batch": max_b,
        "is_backfill": is_backfill,
        "backfill_marked": marked,
        "remaining_eligible": len(remaining_eligible),
    }


def main() -> None:
    group_id = os.environ.get("FEN_GROUP_ID", "").strip() or "322453387859386"
    result = run_ocr_retry(
        group_id=group_id,
        limit=_int_env("FEN_OCR_RETRY_LIMIT", 80),
        batch_seq=_int_env("FEN_OCR_BATCH_SEQ", 0),
    )
    print(f"{LOG} done {result}", flush=True)


if __name__ == "__main__":
    main()
