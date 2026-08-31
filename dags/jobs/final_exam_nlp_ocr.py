"""Final Exam NLP OCR: OpenCV light → Gemini bbox → Qwen fallback → DeepSeek GT.

OCR đồ án NLP: OpenCV nhẹ → Gemini bbox → Qwen fallback → DeepSeek ground_truth.
Task.xlsx B2: {image, label, ground_truth, side_matter, gemini[{text, bounding_box, kind}]}.
"""
from __future__ import annotations

import base64
import json
import os
import time
from io import BytesIO
from typing import Any

from common.fen_stage_config import stage_api_key, stage_base_url, stage_model
from common.chau_ban_schema import extract_json_object, utc_now_iso
from common.config import get_value, load_config
from common.io_storage import (
    get_minio_client,
    list_objects_with_prefix,
    object_exists,
    upload_text_payload,
)
from common.ocr_helpers import resize_png_bytes

from final_exam_nlp_crawl_runner import (
    IMAGE_DIR_NAME,
    _group_root,
    _logs_posts_key,
    _posts_index_key,
    _read_json_object,
    _settings,
)
from fen_crawl_common import (
    download_log_key,
    enrich_invalid_key,
    enrich_valid_key,
    ocr_batch_result_key,
    ocr_queue_key,
    read_jsonl,
    task_export_invalid_key,
    task_export_valid_key,
    v5_root,
    write_jsonl,
)

LOG = "[fen_ocr]"
SCHEMA_OCR = "ocr-1.1"
DEFAULT_LIMIT = 0  # 0 = all posts in source / 0 = mọi post trong nguồn
# Flush shared jsonl often so SIGKILL does not drop a whole batch /
# Ghi jsonl chung thường xuyên để SIGKILL không mất cả batch
OCR_JSONL_FLUSH_EVERY = 5


def _ocr_flush_every() -> int:
    """Images per shared-jsonl flush, overridable per pipeline.

    Số ảnh mỗi lần ghi jsonl chung, mỗi pipeline có thể đặt riêng.
    """
    try:
        value = int(os.environ.get("FEN_OCR_FLUSH_EVERY", "") or OCR_JSONL_FLUSH_EVERY)
    except (TypeError, ValueError):
        return OCR_JSONL_FLUSH_EVERY
    return max(1, value)
MAIN_KINDS = frozenset({"ink_text", "main"})
SIDE_KINDS = frozenset({"margin", "seal", "printed", "other"})
KIND_ALIASES = {
    "ink": "ink_text",
    "text": "ink_text",
    "calligraphy": "ink_text",
    "body": "ink_text",
    "colophon": "margin",
    "inscription": "margin",
    "side": "margin",
    "stamp": "seal",
    "chop": "seal",
    "grid": "printed",
    "label": "printed",
    "page": "printed",
}

GEMINI_PROMPT = """You OCR ONE Chinese calligraphy photo.

Return ONLY compact JSON (no markdown):
{
  "gemini": [
    {
      "text": "characters in this region",
      "bounding_box": [ymin, xmin, ymax, xmax],
      "kind": "ink_text"
    }
  ]
}

kind (required):
- ink_text: main handwritten body (large columns or grid cells)
- margin: handwritten text around the body (colophon, 年/月/日, smaller side column)
- seal: any stamp/chop, any color (red, blue, black, white); seal script inside stamps
- printed: printed red grid, page marks, cell labels such as 12格, folio numbers
- other: UI chrome, watermarks, non-text

Rules:
- bounding_box is 0-1000 normalized [ymin, xmin, ymax, xmax]
- Read traditional Chinese as written; do not translate
- One object per column, line, cell, seal, or printed label
- Array order: within a column top-to-bottom; columns left-to-right; grids left-to-right then top-to-bottom
- For seal: set kind=seal; if unreadable put text="" or a short note like "[seal]"
- Do not invent characters from landscape, trees, or mountains; only real writing
- If no readable ink, return {"gemini": []}
"""
GT_PROMPT = """You are a traditional Chinese calligraphy expert.

Split OCR fragments into TWO strings. Do NOT copy the Facebook caption.

- ground_truth: ONLY kind=ink_text (main body). Traditional Chinese. Reading order: top-to-bottom in each column, then columns left-to-right (or grid left-to-right, top-to-bottom). No seals, grids, page numbers, printed labels.
- side_matter: kind=margin + seal + printed + other, same reading order. Join with newlines. Unreadable seals as [seal]. Empty string if none.

Return ONLY JSON:
{{"ground_truth": "...", "side_matter": "..."}}

Main fragments (JSON):
{main_json}

Side fragments (JSON):
{side_json}

Caption hint (may be empty/wrong, do not copy blindly):
{caption}
"""


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


def _ocr_output_key(source_prefix: str, group_id: str) -> str:
    return f"{_group_root(source_prefix, group_id)}/ocr/ocr_result.jsonl"


def _ocr_detail_key(source_prefix: str, group_id: str, post_id: str, image_rel: str) -> str:
    safe = image_rel.replace("/", "_").lstrip("_")
    return f"{_group_root(source_prefix, group_id)}/ocr/details/{post_id}/{safe}.json"


def _newer_ocr_record(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    """Keep the record with later ocr_at / Giữ bản có ocr_at mới hơn."""
    if str(right.get("ocr_at") or "") >= str(left.get("ocr_at") or ""):
        return right
    return left


def _merge_ocr_by_image(*maps: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Merge OCR maps by image path / Gộp map OCR theo đường dẫn ảnh."""
    out: dict[str, dict[str, Any]] = {}
    for mapping in maps:
        for key, rec in mapping.items():
            image = str(key or rec.get("image") or "").strip()
            if not image or not isinstance(rec, dict):
                continue
            prev = out.get(image)
            out[image] = rec if prev is None else _newer_ocr_record(prev, rec)
    return out


def _records_from_jsonl(bucket: str, object_key: str) -> dict[str, dict[str, Any]]:
    """Load jsonl into image→record map / Đọc jsonl thành map ảnh→record."""
    rows = {str(r.get("image") or ""): r for r in read_jsonl(bucket, object_key)}
    return {k: v for k, v in rows.items() if k}


def _hydrate_existing_from_detail(
    bucket: str,
    source_prefix: str,
    group_id: str,
    post_id: str,
    image_path: str,
    existing: dict[str, dict[str, Any]],
) -> bool:
    """Skip re-OCR if a detail sidecar already exists / Bỏ OCR lại nếu đã có file detail."""
    detail_key = _ocr_detail_key(source_prefix, group_id, post_id or "unknown", image_path)
    if not object_exists(bucket, detail_key):
        return False
    raw = _read_json_object(bucket, detail_key)
    if not isinstance(raw, dict):
        return False
    image = str(raw.get("image") or image_path).strip()
    if not image:
        return False
    prev = existing.get(image)
    existing[image] = raw if prev is None else _newer_ocr_record(prev, raw)
    return True


def _persist_ocr_jsonl(
    bucket: str,
    out_key: str,
    local: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Re-read MinIO jsonl, merge, upload / Đọc lại jsonl MinIO, gộp, upload."""
    # Other workers may have flushed meanwhile / Worker khác có thể vừa ghi
    remote = _records_from_jsonl(bucket, out_key)
    merged = _merge_ocr_by_image(remote, local)
    lines = [
        json.dumps(merged[k], ensure_ascii=False)
        for k in sorted(merged)
        if k
    ]
    upload_text_payload(bucket, out_key, "\n".join(lines) + ("\n" if lines else ""), suffix=".jsonl")
    return merged


def _object_key_from_image_path(source_prefix: str, group_id: str, image_path: str) -> str:
    """Map Task B1 /images/... to MinIO object key / Map /images/... → key MinIO."""
    path = str(image_path or "").strip()
    if not path:
        return ""
    if path.startswith("facebook/") or path.startswith(f"{source_prefix}/"):
        return path.lstrip("/")
    rel = path.lstrip("/")
    if rel.startswith(f"{IMAGE_DIR_NAME}/"):
        return f"{_group_root(source_prefix, group_id)}/{rel}"
    return f"{_group_root(source_prefix, group_id)}/{IMAGE_DIR_NAME}/{rel}"


def _read_object_bytes(bucket: str, object_key: str) -> bytes:
    client = get_minio_client()
    resp = client.get_object(bucket, object_key)
    try:
        return resp.read()
    finally:
        resp.close()
        resp.release_conn()


def preprocess_light(image_bytes: bytes) -> bytes:
    """Deskew if |angle|>2° and mild CLAHE; never binarize.

    Xoay nếu |góc|>2° và CLAHE nhẹ; không nhị phân hóa.
    """
    try:
        import cv2
        import numpy as np
    except ModuleNotFoundError:
        return image_bytes

    arr = np.frombuffer(image_bytes, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if img is None:
        return image_bytes

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    edges = cv2.Canny(gray, 50, 150, apertureSize=3)
    lines = cv2.HoughLinesP(
        edges, 1, np.pi / 180, threshold=80, minLineLength=40, maxLineGap=12
    )
    angle = 0.0
    if lines is not None and len(lines) >= 4:
        angles: list[float] = []
        for line in lines[:80]:
            x1, y1, x2, y2 = line[0]
            if abs(x2 - x1) < 2:
                continue
            deg = float(np.degrees(np.arctan2(y2 - y1, x2 - x1)))
            # Near-horizontal strokes only / Chỉ nét gần ngang
            if abs(deg) < 20:
                angles.append(deg)
        if angles:
            angle = float(np.median(np.array(angles)))

    if abs(angle) > 2.0 and abs(angle) < 15.0:
        h, w = img.shape[:2]
        matrix = cv2.getRotationMatrix2D((w / 2.0, h / 2.0), angle, 1.0)
        img = cv2.warpAffine(
            img,
            matrix,
            (w, h),
            flags=cv2.INTER_CUBIC,
            borderMode=cv2.BORDER_REPLICATE,
        )

    lab = cv2.cvtColor(img, cv2.COLOR_BGR2LAB)
    channel_l, channel_a, channel_b = cv2.split(lab)
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    channel_l = clahe.apply(channel_l)
    img = cv2.cvtColor(cv2.merge([channel_l, channel_a, channel_b]), cv2.COLOR_LAB2BGR)
    ok, buf = cv2.imencode(".png", img)
    if not ok:
        return image_bytes
    return buf.tobytes()


def _to_png_bytes(image_bytes: bytes, max_side: int) -> bytes:
    try:
        return resize_png_bytes(image_bytes, max_side=max_side)
    except Exception:
        from PIL import Image

        img = Image.open(BytesIO(image_bytes)).convert("RGB")
        w, h = img.size
        longest = max(w, h)
        if longest > max_side:
            scale = max_side / float(longest)
            img = img.resize(
                (max(1, int(w * scale)), max(1, int(h * scale))),
                Image.Resampling.LANCZOS,
            )
        buf = BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()


def _vision_ocr(image_bytes: bytes, *, model: str, prompt: str) -> tuple[str, str | None]:
    """Call OpenAI-compatible vision; return (raw_text, error).

    Gọi vision tương thích OpenAI; trả (text thô, lỗi).
    """
    try:
        from openai import OpenAI
    except ModuleNotFoundError as exc:
        return "", f"missing_openai:{exc}"

    cfg = load_config()
    fallback_key = stage_api_key("fen_ocr") or get_value(cfg, "gemini_opencv", "api_key", fallback="").strip()
    base_url = stage_base_url("fen_ocr")
    max_retries = int(get_value(cfg, "gemini_opencv", "max_retries", fallback="4"))
    max_side = int(get_value(cfg, "gemini_opencv", "max_image_side", fallback="1536"))
    max_tokens = int(get_value(cfg, "gemini_opencv", "max_tokens", fallback="4096"))

    png = _to_png_bytes(image_bytes, max_side)
    image_b64 = base64.standard_b64encode(png).decode("ascii")

    last_error: str | None = None
    for attempt in range(max(1, max_retries)):
        try:
            from common.api_keys import next_api_key

            try:
                use_key = next_api_key()
            except Exception:
                use_key = fallback_key
            if not use_key:
                return "", "missing_gemini_api_key"
            client = OpenAI(api_key=use_key, base_url=base_url or None)
            print(f"{LOG} vision model={model} attempt={attempt}", flush=True)
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            {
                                "type": "image_url",
                                "image_url": {"url": f"data:image/png;base64,{image_b64}"},
                            },
                        ],
                    }
                ],
                max_tokens=max_tokens,
                temperature=0.1,
            )
            text = (response.choices[0].message.content or "").strip()
            if text:
                return text, None
            last_error = "empty_reply"
        except Exception as exc:
            last_error = str(exc)
            message = last_error.lower()
            # Qwen 3.8 on Ramclouds is text-only / Qwen 3.8 trên Ramclouds chỉ nhận text
            if "text-only" in message or "must be a text part" in message:
                break
            if "429" in message and attempt < max_retries - 1:
                time.sleep(min(15.0 * (attempt + 1), 60.0))
                continue
            break
    return "", last_error


def _chat_json(prompt: str, *, model: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        from openai import OpenAI
    except ModuleNotFoundError as exc:
        return None, f"missing_openai:{exc}"

    cfg = load_config()
    api_key = (
        get_value(cfg, "align", "api_key", fallback="").strip()
        or get_value(cfg, "gemini_opencv", "api_key", fallback="").strip()
    )
    if not api_key:
        return None, "missing_align_api_key"
    base_url = get_value(cfg, "align", "base_url", fallback="https://ramclouds.me/v1").strip()
    client = OpenAI(api_key=api_key, base_url=base_url or None)
    try:
        print(f"{LOG} chat model={model}", flush=True)
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2048,
            temperature=0.1,
        )
        raw = (response.choices[0].message.content or "").strip()
    except Exception as exc:
        return None, str(exc)
    parsed = extract_json_object(raw)
    return parsed, None if parsed else "gt_parse_failed"


def _normalize_kind(value: Any) -> str:
    """Map model kind aliases / Chuẩn hóa kind từ model."""
    text = str(value or "").strip().lower().replace("-", "_")
    text = KIND_ALIASES.get(text, text)
    if text in MAIN_KINDS or text in SIDE_KINDS:
        return "ink_text" if text == "main" else text
    return "ink_text"


def _normalize_boxes(raw: Any) -> list[dict[str, Any]]:
    items = raw if isinstance(raw, list) else []
    out: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        kind = _normalize_kind(item.get("kind"))
        text = str(item.get("text") or "").strip()
        # Seals may have empty text / Ấn có thể không đọc được chữ
        if not text and kind == "seal":
            text = "[seal]"
        box = item.get("bounding_box") or item.get("bbox")
        if not isinstance(box, list) or len(box) != 4:
            continue
        try:
            nums = [int(round(float(x))) for x in box]
        except (TypeError, ValueError):
            continue
        nums = [max(0, min(1000, n)) for n in nums]
        if not text:
            continue
        out.append({"text": text, "bounding_box": nums, "kind": kind})
    # Columns left-to-right, then top-to-bottom / Cột trái→phải, trong cột trên→dưới
    out.sort(key=lambda r: (r["bounding_box"][1], r["bounding_box"][0]))
    return out


def _split_boxes(
    boxes: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Main ink vs seals/margins/print / Tách chính văn vs ấn/mép/in."""
    main: list[dict[str, Any]] = []
    side: list[dict[str, Any]] = []
    for box in boxes:
        kind = str(box.get("kind") or "ink_text")
        if kind in MAIN_KINDS:
            main.append(box)
        else:
            side.append(box)
    return main, side


def _join_box_text(boxes: list[dict[str, Any]]) -> str:
    return "\n".join(str(b.get("text") or "").strip() for b in boxes if str(b.get("text") or "").strip())


def is_text_only_vision_model(model: str) -> bool:
    """True if Ramclouds model rejects image parts / True nếu model từ chối ảnh."""
    name = str(model or "").strip().lower()
    if not name:
        return True
    if "vl" in name or "vision" in name or "gemini" in name:
        return False
    # qwen-3.8-max open checkpoint is text-only / checkpoint qwen-3.8-max chỉ text
    if "qwen" in name and "3.8" in name:
        return True
    return False


def ocr_one_image(image_bytes: bytes) -> dict[str, Any]:
    """Run preprocess + Gemini (+ vision fallback) on one image.

    Preprocess + Gemini (+ fallback vision) 1 ảnh.
    """
    cfg = load_config()
    gemini_model = get_value(cfg, "gemini_opencv", "model", fallback="gemini-3.5-flash-low")
    qwen_model = os.environ.get("FEN_OCR_QWEN_MODEL", "").strip() or get_value(
        cfg, "final_exam_nlp", "qwen_ocr_model", fallback=""
    )
    processed = preprocess_light(image_bytes)
    raw, err = _vision_ocr(processed, model=gemini_model, prompt=GEMINI_PROMPT)
    engine = "gemini"
    if (err or not raw) and qwen_model and not is_text_only_vision_model(qwen_model):
        raw2, err2 = _vision_ocr(processed, model=qwen_model, prompt=GEMINI_PROMPT)
        if raw2:
            raw, err = raw2, None
            engine = "qwen"
        else:
            err = err or err2
    parsed = extract_json_object(raw) if raw else None
    boxes = _normalize_boxes((parsed or {}).get("gemini"))
    main, side = _split_boxes(boxes)
    return {
        "engine": engine,
        "gemini": boxes,
        "main_boxes": main,
        "side_boxes": side,
        "ocr_ok": bool(main or side),
        "ocr_error": None if (main or side) else (err or "empty_ocr"),
        "raw_preview": (raw or "")[:500],
    }


def _ground_truth(
    main: list[dict[str, Any]],
    side: list[dict[str, Any]],
    caption: str,
) -> tuple[str, str, str | None]:
    """Return ground_truth, side_matter, error / Trả chính văn, phụ lục, lỗi."""
    main_join = _join_box_text(main)
    side_join = _join_box_text(side)
    if not main_join and not side_join:
        return "", "", "no_ocr_text"
    if not main_join:
        return "", side_join, "no_main_text"
    cfg = load_config()
    model = get_value(cfg, "align", "model", fallback="deepseek-v4-flash")
    prompt = GT_PROMPT.format(
        main_json=json.dumps(main, ensure_ascii=False),
        side_json=json.dumps(side, ensure_ascii=False),
        caption=(caption or "")[:400],
    )
    parsed, err = _chat_json(prompt, model=model)
    if parsed:
        gt = str(parsed.get("ground_truth") or "").strip()
        sm = str(parsed.get("side_matter") or "").strip() or side_join
        if gt:
            return gt, sm, None
    return main_join, side_join, err or "gt_fallback_concat"


def _put_caption(out: dict[str, str], row: dict[str, Any]) -> None:
    pid = str(row.get("post_id") or "").strip()
    cap = str(row.get("caption") or row.get("label") or "").strip()
    if pid and cap:
        out[pid] = cap


def _caption_map(bucket: str, source_prefix: str, group_id: str) -> dict[str, str]:
    """Join captions from export + crawl logs + download log.

    Ghép caption từ export + log crawl + log download.
    """
    out: dict[str, str] = {}
    for key in (
        task_export_valid_key(source_prefix, group_id),
        task_export_invalid_key(source_prefix, group_id),
        _posts_index_key(source_prefix, group_id),
        _logs_posts_key(source_prefix, group_id),
        download_log_key(source_prefix, group_id),
    ):
        for row in read_jsonl(bucket, key):
            _put_caption(out, row)
    # Mid-crawl batches also carry caption / Batch đang crawl cũng có caption
    for sub in ("discover/batches/", "enrich/batches/"):
        prefix = f"{v5_root(source_prefix, group_id)}/{sub}"
        for key in list_objects_with_prefix(bucket, prefix, suffix=".jsonl"):
            for row in read_jsonl(bucket, key):
                _put_caption(out, row)
    print(f"{LOG} caption_map size={len(out)}", flush=True)
    return out


def _caption_for_post(
    *,
    bucket: str,
    source_prefix: str,
    group_id: str,
    post_id: str,
    cached: dict[str, str],
) -> str:
    """Resolve caption for one post, including enrich JSON sidecar.

    Lấy caption 1 post, kể cả file enrich JSON.
    """
    if post_id in cached:
        return cached[post_id]
    for key_fn in (enrich_valid_key, enrich_invalid_key):
        raw = _read_json_object(bucket, key_fn(source_prefix, group_id, post_id))
        if isinstance(raw, dict):
            cap = str(raw.get("caption") or raw.get("label") or "").strip()
            if cap:
                cached[post_id] = cap
                return cap
    return ""


def _posts_from_stored_images(
    *,
    bucket: str,
    source_prefix: str,
    group_id: str,
) -> list[dict[str, Any]]:
    """Rebuild posts from leftover MinIO images when export JSONL is empty.

    Dự lại post từ ảnh MinIO khi export JSONL trống (sau reset crawl).
    """
    prefix = f"{_group_root(source_prefix, group_id)}/{IMAGE_DIR_NAME}/"
    keys = list_objects_with_prefix(bucket, prefix)
    by_post: dict[str, list[str]] = {}
    for key in keys:
        if not key.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
            continue
        rel = key[len(prefix) :].lstrip("/")
        parts = rel.split("/", 1)
        if len(parts) != 2:
            continue
        post_id, fname = parts
        local = f"/{IMAGE_DIR_NAME}/{post_id}/{fname}"
        by_post.setdefault(post_id, []).append(local)
    captions = _caption_map(bucket, source_prefix, group_id)
    rows: list[dict[str, Any]] = []
    for post_id, images in sorted(by_post.items(), reverse=True):
        images = sorted(set(images))
        label = _caption_for_post(
            bucket=bucket,
            source_prefix=source_prefix,
            group_id=group_id,
            post_id=post_id,
            cached=captions,
        )
        rows.append(
            {
                "post_id": post_id,
                "label": label,
                "images": images,
            }
        )
    filled = sum(1 for r in rows if r.get("label"))
    print(
        f"{LOG} image_scan posts={len(rows)} objects={len(keys)} with_caption={filled}",
        flush=True,
    )
    return rows


def _load_ocr_posts(
    bucket: str,
    source_prefix: str,
    group_id: str,
    *,
    batch_seq: int = 0,
) -> list[dict[str, Any]]:
    if batch_seq > 0:
        queue_key = ocr_queue_key(source_prefix, group_id, batch_seq)
        rows = [r for r in read_jsonl(bucket, queue_key) if r.get("images")]
        print(f"{LOG} source=ocr/queue batch_seq={batch_seq} n={len(rows)}", flush=True)
        return rows
    valid_key = task_export_valid_key(source_prefix, group_id)
    rows = [r for r in read_jsonl(bucket, valid_key) if r.get("images")]
    if rows:
        print(f"{LOG} source=export/valid_post.jsonl n={len(rows)}", flush=True)
        return rows
    print(f"{LOG} export empty — fallback to images/", flush=True)
    return _posts_from_stored_images(
        bucket=bucket, source_prefix=source_prefix, group_id=group_id
    )


def rebuild_ocr_jsonl(*, group_id: str) -> dict[str, Any]:
    """Rebuild ocr_result.jsonl from ocr/details sidecars.

    Dựng lại ocr_result.jsonl từ các file ocr/details (cứu batch bị kill).
    """
    if not group_id:
        raise ValueError("FEN_GROUP_ID is required")
    settings = _settings()
    bucket = settings["bucket_raw"]
    source_prefix = settings["source_prefix"]
    out_key = _ocr_output_key(source_prefix, group_id)
    before = _records_from_jsonl(bucket, out_key)

    details_prefix = f"{_group_root(source_prefix, group_id)}/ocr/details/"
    keys = list_objects_with_prefix(bucket, details_prefix, suffix=".json")
    print(f"{LOG} rebuild details={len(keys)} jsonl_before={len(before)}", flush=True)

    from_details: dict[str, dict[str, Any]] = {}
    bad = 0
    for idx, key in enumerate(keys, start=1):
        raw = _read_json_object(bucket, key)
        if not isinstance(raw, dict):
            bad += 1
            continue
        image = str(raw.get("image") or "").strip()
        if not image:
            bad += 1
            continue
        prev = from_details.get(image)
        from_details[image] = raw if prev is None else _newer_ocr_record(prev, raw)
        if idx % 500 == 0:
            print(f"{LOG} rebuild scanned={idx}/{len(keys)}", flush=True)

    merged = _persist_ocr_jsonl(bucket, out_key, _merge_ocr_by_image(before, from_details))
    summary = {
        "group_id": group_id,
        "detail_files": len(keys),
        "detail_records": len(from_details),
        "unreadable_details": bad,
        "jsonl_before": len(before),
        "jsonl_after": len(merged),
        "recovered": max(0, len(merged) - len(before)),
        "output": f"{bucket}/{out_key}",
    }
    print(f"{LOG} rebuild done {summary}", flush=True)
    return summary


def run_ocr(
    *,
    group_id: str,
    batch_seq: int = 0,
    limit: int = DEFAULT_LIMIT,
    force: bool = False,
) -> dict[str, Any]:
    """OCR queue/export posts into Task B2 JSONL / OCR hàng đợi/export → JSONL Task B2."""
    if not group_id:
        raise ValueError("FEN_GROUP_ID is required")
    settings = _settings()
    bucket = settings["bucket_raw"]
    source_prefix = settings["source_prefix"]
    rows = _load_ocr_posts(bucket, source_prefix, group_id, batch_seq=batch_seq)
    if limit > 0:
        rows = rows[:limit]
    captions = _caption_map(bucket, source_prefix, group_id)
    for post in rows:
        pid = str(post.get("post_id") or "").strip()
        label = str(post.get("label") or post.get("caption") or "").strip()
        if pid and not label:
            post["label"] = _caption_for_post(
                bucket=bucket,
                source_prefix=source_prefix,
                group_id=group_id,
                post_id=pid,
                cached=captions,
            )
    out_key = _ocr_output_key(source_prefix, group_id)
    existing = _records_from_jsonl(bucket, out_key)

    written: list[dict[str, Any]] = []
    skipped = 0
    missing_img = 0
    label_backfill = 0
    resumed_detail = 0
    flushes = 0
    pending_flush = 0
    flush_every = _ocr_flush_every()
    for post in rows:
        label = str(post.get("label") or post.get("caption") or "").strip()
        post_id = str(post.get("post_id") or "").strip()
        images = [str(x) for x in (post.get("images") or []) if str(x).strip()]
        for image_path in images:
            if image_path not in existing and not force:
                # Resume from per-image sidecar when a batch was killed /
                # Tiếp tục từ file từng ảnh khi batch bị kill giữa đường
                if _hydrate_existing_from_detail(
                    bucket, source_prefix, group_id, post_id, image_path, existing
                ):
                    resumed_detail += 1
            if image_path in existing and not force:
                rec = dict(existing[image_path])
                if label and not str(rec.get("label") or "").strip():
                    rec["label"] = label
                    rec["label_backfill"] = True
                    label_backfill += 1
                    existing[image_path] = rec
                    print(f"{LOG} backfill_label post={post_id} image={image_path}", flush=True)
                skipped += 1
                written.append(rec)
                continue
            object_key = _object_key_from_image_path(source_prefix, group_id, image_path)
            if not object_exists(bucket, object_key):
                missing_img += 1
                print(f"{LOG} missing image {object_key}", flush=True)
                continue
            raw_bytes = _read_object_bytes(bucket, object_key)
            ocr = ocr_one_image(raw_bytes)
            gt, side_matter, gt_err = _ground_truth(
                ocr["main_boxes"], ocr["side_boxes"], label
            )
            record = {
                "image": image_path if image_path.startswith("/") else f"/{image_path.lstrip('/')}",
                "label": label,
                "ground_truth": gt,
                "side_matter": side_matter,
                "gemini": ocr["gemini"],
                "post_id": post_id,
                "ocr_engine": ocr["engine"],
                "ocr_ok": ocr["ocr_ok"],
                "ocr_error": ocr["ocr_error"],
                "gt_error": gt_err,
                "schema_version": SCHEMA_OCR,
                "batch_seq": int(post.get("batch_seq") or batch_seq or 0),
                "group_id": group_id,
                "post_link": str(post.get("post_link") or post.get("permalink") or ""),
                "ocr_at": utc_now_iso(),
            }
            detail_key = _ocr_detail_key(source_prefix, group_id, post_id or "unknown", image_path)
            upload_text_payload(
                bucket, detail_key, json.dumps(record, ensure_ascii=False, indent=2), suffix=".json"
            )
            existing[record["image"]] = record
            written.append(record)
            print(
                f"{LOG} post={post_id} image={record['image']} boxes={len(ocr['gemini'])} "
                f"main={len(ocr['main_boxes'])} side={len(ocr['side_boxes'])} "
                f"engine={ocr['engine']} gt_len={len(gt)} sm_len={len(side_matter)}",
                flush=True,
            )
            # Incremental flush so a SIGKILL loses at most N images /
            # Flush dần để SIGKILL mất tối đa N ảnh
            pending_flush += 1
            if pending_flush >= flush_every:
                existing = _persist_ocr_jsonl(bucket, out_key, existing)
                flushes += 1
                pending_flush = 0
                print(f"{LOG} flush rows={len(existing)} flushes={flushes}", flush=True)
            delay = float(get_value(load_config(), "gemini_opencv", "page_delay_sec", fallback="3") or "3")
            time.sleep(max(0.0, delay))

    # Final merge with remote so parallel workers do not clobber /
    # Gộp lần cuối với MinIO để worker song song không ghi đè nhau
    by_image = _merge_ocr_by_image(
        existing,
        {str(rec.get("image") or ""): rec for rec in written},
    )
    by_image = _persist_ocr_jsonl(bucket, out_key, by_image)
    batch_key = ""
    if batch_seq > 0:
        batch_key = ocr_batch_result_key(source_prefix, group_id, batch_seq)
        write_jsonl(bucket, batch_key, written)
    summary = {
        "group_id": group_id,
        "batch_seq": batch_seq,
        "valid_posts_considered": len(rows),
        "records": len(written),
        "skipped_existing": skipped,
        "resumed_from_detail": resumed_detail,
        "jsonl_flush_every": flush_every,
        "jsonl_flushes": flushes,
        "jsonl_rows": len(by_image),
        "label_backfill": label_backfill,
        "missing_images": missing_img,
        "with_label": sum(1 for r in written if str(r.get("label") or "").strip()),
        "output": f"{bucket}/{out_key}",
        "batch_output": f"{bucket}/{batch_key}" if batch_key else "",
    }
    print(f"{LOG} done {summary}", flush=True)
    return summary


def main() -> None:
    group_id = os.environ.get("FEN_GROUP_ID", "").strip() or "322453387859386"
    run_ocr(
        group_id=group_id,
        batch_seq=_int_env("FEN_OCR_BATCH_SEQ", 0),
        limit=_int_env("FEN_OCR_LIMIT", DEFAULT_LIMIT),
        force=_bool_env("FEN_OCR_FORCE", False),
    )


if __name__ == "__main__":
    main()
