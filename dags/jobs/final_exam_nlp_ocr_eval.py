"""Qwen reading-order eval for FEN OCR (text-only, batch + rollover).

Đánh giá thứ tự đọc OCR FEN bằng Qwen (chỉ text, batch + rollover).
Does not overwrite ocr_result.jsonl / Không ghi đè ocr_result.jsonl.
"""
from __future__ import annotations

import json
import os
import re
import time
from collections import Counter
from difflib import SequenceMatcher
from typing import Any

from common.chau_ban_schema import extract_json_object, utc_now_iso
from common.config import get_value, load_config
from common.io_storage import upload_json_payload, upload_text_payload
from common.llm_chat import call_chat_completion

from final_exam_nlp_crawl_runner import _group_root, _settings
from final_exam_nlp_ocr import _normalize_boxes, _ocr_output_key, _split_boxes
from fen_crawl_common import read_jsonl, write_jsonl

LOG = "[fen_ocr_eval]"
SCHEMA_EVAL = "eval-1.0"
# Persist jsonl every N judged images, or after this many seconds /
# Ghi jsonl mỗi N ảnh đã chấm, hoặc sau số giây này
EVAL_FLUSH_EVERY = 20
EVAL_FLUSH_SEC = 120.0
CJK_RE = re.compile(r"[\u3400-\u4DBF\u4E00-\u9FFF\uF900-\uFAFF]")
READING_ORDERS = frozenset({"rtl_columns", "ltr_columns", "grid", "mixed", "unknown"})
CAPTION_ALIGNS = frozenset({"quote", "paraphrase", "unrelated", "empty"})
FLAG_CODES = frozenset(
    {
        "order_splice",
        "semantic_break",
        "ocr_suspect",
        "ocr_garbage",
        "seal_leaked_into_gt",
        "printed_in_gt",
        "caption_conflict",
        "needs_hitl",
        "col_order_reversed",
        "gt_copied_ocr",
        "invented_chars_rejected",
        "empty_main",
        "caption_mismatch",
    }
)
REOCR_FLAGS = frozenset({"ocr_suspect", "ocr_garbage"})
HITL_FLAGS = frozenset({"needs_hitl", "caption_conflict"})
REFINE_FLAGS = frozenset(
    {"order_splice", "semantic_break", "seal_leaked_into_gt", "printed_in_gt", "col_order_reversed"}
)

EVAL_PROMPT = """You are a traditional Chinese calligraphy reading-order judge.
You receive OCR boxes (already recognized). You do NOT see the image.
Do NOT invent characters. You may only reorder, split kinds, and lightly
normalize traditional/simplified forms that already appear in the boxes.
Do NOT copy the Facebook caption into the body.

Return ONLY one JSON object (no markdown):
{{
  "reading_order": "rtl_columns|ltr_columns|grid|mixed|unknown",
  "order_ok": true,
  "ocr_text_refined": "...",
  "gt_refined": "...",
  "gt_ok": true,
  "side_matter_refined": "...",
  "seals_refined": [{{"text": "...", "note": "...", "bounding_box": [ymin,xmin,ymax,xmax]}}],
  "printed_refined": "...",
  "dropped": ["..."],
  "caption_align": "quote|paraphrase|unrelated|empty",
  "flags": [{{"code": "order_splice", "severity": "high", "action": "refine_text", "span": "...", "note": "..."}}],
  "sentence_spans": [{{"text": "...", "ok": true, "flag": null}}],
  "status": "ok",
  "issues": ["col_order_reversed"],
  "confidence": 0.0
}}

Rules:
- ocr_text_refined and gt_refined: ONLY kind=ink_text (main body).
  Default vertical order: top-to-bottom in a column, then columns RIGHT-to-LEFT.
  Use grid left-to-right, top-to-bottom only if boxes are a cell grid.
- side_matter_refined: kind=margin + printed + other, same page order. Join with newlines.
- seals_refined: one item per kind=seal. Unreadable → text="[seal]". Never guess seal-script.
- If current ground_truth mixed seals/printed into the body → flags+=seal_leaked_into_gt
  or printed_in_gt, and move them out of gt_refined.
- If CPU baseline_best_order is rtl_columns and current GT matches LTR concat →
  flags+=col_order_reversed / order_splice, order_ok=false.
- Concatenating two columns into one meaningless clause → order_splice; split sentence_spans.
- Character likely misread (does not fit couplet/caption quote) → ocr_suspect, action=re_ocr.
  Do NOT invent a replacement character; keep the box text and flag it.
- Unsure → needs_hitl and confidence<=0.5.
- caption_align=quote only if caption CJK substantially appears in gt_refined.
- confidence 0..1.
- Keep JSON compact: sentence_spans at most 12 items. No markdown. No <think>.

CPU baseline: {baseline_json}
Current ground_truth: {source_gt}
Current side_matter: {source_side}
Caption (hint, may be wrong): {caption}

Main boxes (JSON):
{main_json}

Side boxes (JSON):
{side_json}
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


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def should_flush(
    *,
    pending: int,
    last_flush_at: float,
    every: int = EVAL_FLUSH_EVERY,
    interval_sec: float = EVAL_FLUSH_SEC,
    now: float | None = None,
) -> bool:
    """Flush when N records piled up or the time interval elapsed.

    Flush khi đủ N record hoặc đã hết khoảng thời gian.
    """
    if pending <= 0:
        return False
    if pending >= max(1, int(every)):
        return True
    elapsed = (now if now is not None else time.time()) - last_flush_at
    return interval_sec > 0 and elapsed >= interval_sec


def eval_root(source_prefix: str, group_id: str) -> str:
    """Eval prefix under the OCR folder / Prefix eval trong thư mục OCR."""
    return f"{_group_root(source_prefix, group_id)}/ocr/eval"


def _safe_image_name(image: str) -> str:
    rel = str(image or "").split("/images/")[-1].lstrip("/")
    return rel.replace("/", "_") or "image"


def compact_cjk(text: str) -> str:
    """Keep CJK chars only / Chỉ giữ ký tự CJK."""
    return "".join(CJK_RE.findall(str(text) or ""))


def lcs_len(left: str, right: str, *, left_cap: int = 80, right_cap: int = 200) -> int:
    """Longest common substring length / Độ dài substring chung dài nhất."""
    a = compact_cjk(left)[:left_cap]
    b = compact_cjk(right)[:right_cap]
    if not a or not b:
        return 0
    block = SequenceMatcher(None, a, b, autojunk=False).find_longest_match(0, len(a), 0, len(b))
    return int(block.size)


def _join_boxes(boxes: list[dict[str, Any]], key_fn: Any) -> str:
    ordered = sorted(boxes, key=key_fn)
    return "\n".join(str(item.get("text") or "").strip() for item in ordered if str(item.get("text") or "").strip())


def reconstruct_orders(main_boxes: list[dict[str, Any]]) -> dict[str, str]:
    """Build LTR / RTL / grid concatenations from ink boxes.

    Dựng chuỗi LTR / RTL / lưới từ box chính văn.
    """
    def xmin_ymin(item: dict[str, Any]) -> tuple[int, int]:
        box = item.get("bounding_box") or [0, 0, 0, 0]
        return int(box[1]), int(box[0])

    def rtl_key(item: dict[str, Any]) -> tuple[int, int]:
        box = item.get("bounding_box") or [0, 0, 0, 0]
        xmax = int(box[3]) if len(box) > 3 else 0
        ymin = int(box[0])
        return (-xmax, ymin)

    def grid_key(item: dict[str, Any]) -> tuple[int, int]:
        box = item.get("bounding_box") or [0, 0, 0, 0]
        return int(box[0]), int(box[1])

    return {
        "ltr_text": _join_boxes(main_boxes, xmin_ymin),
        "rtl_text": _join_boxes(main_boxes, rtl_key),
        "grid_text": _join_boxes(main_boxes, grid_key),
    }


def pick_best_order(orders: dict[str, str], ground_truth: str) -> dict[str, Any]:
    """Score concatenations against current GT / Chấm các cách ghép so với GT hiện tại."""
    gt = str(ground_truth or "")
    ltr_lcs = lcs_len(orders["ltr_text"], gt)
    rtl_lcs = lcs_len(orders["rtl_text"], gt)
    grid_lcs = lcs_len(orders["grid_text"], gt)
    scores = {"ltr_columns": ltr_lcs, "rtl_columns": rtl_lcs, "grid": grid_lcs}
    best = max(scores, key=lambda name: scores[name])
    ranked = sorted(scores.values(), reverse=True)
    if len(ranked) > 1 and ranked[0] == ranked[1]:
        best = "tie"
    return {
        "ltr_lcs": ltr_lcs,
        "rtl_lcs": rtl_lcs,
        "grid_lcs": grid_lcs,
        "best_order": best,
    }


def normalize_flag(raw: Any) -> dict[str, Any] | None:
    """Clamp one flag object / Chuẩn hóa một object cờ."""
    if not isinstance(raw, dict):
        code = str(raw or "").strip()
        raw = {"code": code} if code else None
    if not raw:
        return None
    code = str(raw.get("code") or "").strip()
    if code not in FLAG_CODES:
        return None
    action = str(raw.get("action") or "").strip()
    if code in REOCR_FLAGS:
        action = "re_ocr"
    elif code in HITL_FLAGS:
        action = action or "hitl"
    elif code in REFINE_FLAGS:
        action = action or "refine_text"
    else:
        action = action or "none"
    severity = str(raw.get("severity") or "medium").strip() or "medium"
    return {
        "code": code,
        "severity": severity,
        "action": action,
        "span": str(raw.get("span") or "")[:200],
        "note": str(raw.get("note") or "")[:300],
    }


def derive_status(flags: list[dict[str, Any]], *, confidence: float) -> str:
    """Map flags to eval status / Map cờ sang status eval."""
    codes = {str(item.get("code")) for item in flags}
    if codes & REOCR_FLAGS:
        return "needs_re_ocr"
    if "needs_hitl" in codes or (confidence <= 0.5 and "semantic_break" in codes):
        return "needs_hitl"
    if "caption_conflict" in codes and confidence < 0.6:
        return "needs_hitl"
    if codes & REFINE_FLAGS:
        return "refined"
    return "ok"


def baseline_for_record(record: dict[str, Any]) -> dict[str, Any]:
    """CPU LTR/RTL/grid vs GT / CPU LTR/RTL/lưới so GT."""
    boxes = _normalize_boxes(record.get("gemini") or [])
    main, _side = _split_boxes(boxes)
    orders = reconstruct_orders(main)
    scores = pick_best_order(orders, str(record.get("ground_truth") or ""))
    return {
        "schema_version": SCHEMA_EVAL,
        "image": str(record.get("image") or ""),
        "post_id": str(record.get("post_id") or ""),
        **orders,
        "gt_compact": compact_cjk(str(record.get("ground_truth") or "")),
        **scores,
        "baseline_at": utc_now_iso(),
    }


def _image_key(record: dict[str, Any]) -> str:
    return str(record.get("image") or "").strip()


def _queue_action(status: str) -> str | None:
    if status == "needs_re_ocr":
        return "re_ocr"
    if status == "needs_hitl":
        return "hitl"
    if status == "refined":
        return "refine"
    return None


def _parse_eval_response(parsed: dict[str, Any], *, baseline: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
    flags = [normalize_flag(item) for item in (parsed.get("flags") or [])]
    extra_issues = [str(x).strip() for x in (parsed.get("issues") or []) if str(x).strip()]
    for issue in extra_issues:
        if issue in FLAG_CODES and not any(flag and flag["code"] == issue for flag in flags):
            flags.append(normalize_flag({"code": issue}))
    flags = [flag for flag in flags if flag]
    if baseline.get("best_order") == "rtl_columns" and not any(f["code"] == "col_order_reversed" for f in flags):
        ltr = compact_cjk(str(baseline.get("ltr_text") or ""))
        gt = compact_cjk(str(record.get("ground_truth") or ""))
        if ltr and gt and ltr == gt:
            flags.append(normalize_flag({"code": "col_order_reversed", "note": "GT equals LTR concat"}))
    try:
        confidence = float(parsed.get("confidence") if parsed.get("confidence") is not None else 0.0)
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    status = str(parsed.get("status") or "").strip()
    derived = derive_status(flags, confidence=confidence)
    if status not in {"ok", "refined", "needs_re_ocr", "re_ocr_done", "needs_hitl", "merged"}:
        status = derived
    elif derived == "needs_re_ocr":
        status = "needs_re_ocr"
    elif derived == "needs_hitl" and status == "ok":
        status = "needs_hitl"
    reading = str(parsed.get("reading_order") or "unknown").strip()
    if reading not in READING_ORDERS:
        reading = "unknown"
    caption_align = str(parsed.get("caption_align") or "empty").strip()
    if caption_align not in CAPTION_ALIGNS:
        caption_align = "unrelated" if str(record.get("label") or "").strip() else "empty"
    seals = parsed.get("seals_refined") if isinstance(parsed.get("seals_refined"), list) else []
    spans = parsed.get("sentence_spans") if isinstance(parsed.get("sentence_spans"), list) else []
    dropped = parsed.get("dropped") if isinstance(parsed.get("dropped"), list) else []
    return {
        "schema_version": SCHEMA_EVAL,
        "image": _image_key(record),
        "post_id": str(record.get("post_id") or ""),
        "label": str(record.get("label") or ""),
        "source_gt": str(record.get("ground_truth") or ""),
        "source_side_matter": str(record.get("side_matter") or ""),
        "reading_order": reading,
        "order_ok": bool(parsed.get("order_ok")),
        "ocr_text_refined": str(parsed.get("ocr_text_refined") or "").strip(),
        "gt_refined": str(parsed.get("gt_refined") or "").strip(),
        "gt_ok": bool(parsed.get("gt_ok")),
        "side_matter_refined": str(parsed.get("side_matter_refined") or "").strip(),
        "seals_refined": seals,
        "printed_refined": str(parsed.get("printed_refined") or "").strip(),
        "dropped": [str(x) for x in dropped],
        "caption_align": caption_align,
        "flags": flags,
        "sentence_spans": spans,
        "issues": extra_issues,
        "confidence": round(confidence, 4),
        "status": status,
        "eval_round": 1,
        "baseline_best_order": baseline.get("best_order"),
        "eval_at": utc_now_iso(),
    }


def _append_queue(bucket: str, key: str, row: dict[str, Any]) -> None:
    rows = read_jsonl(bucket, key)
    image = str(row.get("image") or "")
    rows = [item for item in rows if str(item.get("image") or "") != image]
    rows.append(row)
    write_jsonl(bucket, key, rows)


def _merge_jsonl(bucket: str, key: str, incoming: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    existing = {_image_key(row): row for row in read_jsonl(bucket, key) if _image_key(row)}
    for row in incoming:
        image = _image_key(row)
        if image:
            existing[image] = row
    write_jsonl(bucket, key, list(existing.values()))
    return existing


def _parse_eval_json(raw: str) -> dict[str, Any] | None:
    """Parse Qwen JSON; strip think tags / Parse JSON Qwen; bỏ thẻ think."""
    text = str(raw or "")
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"</?think>", "", text, flags=re.IGNORECASE)
    parsed = extract_json_object(text)
    if parsed:
        return parsed
    # Drop trailing commas that break json.loads / Bỏ dấu phẩy thừa làm json.loads fail
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        return None
    blob = re.sub(r",\s*([}\]])", r"\1", text[start : end + 1])
    try:
        data = json.loads(blob)
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def _judge_one(
    record: dict[str, Any],
    baseline: dict[str, Any],
    *,
    model: str,
) -> dict[str, Any] | None:
    boxes = _normalize_boxes(record.get("gemini") or [])
    main, side = _split_boxes(boxes)
    prompt = EVAL_PROMPT.format(
        baseline_json=json.dumps(
            {
                "best_order": baseline.get("best_order"),
                "ltr_lcs": baseline.get("ltr_lcs"),
                "rtl_lcs": baseline.get("rtl_lcs"),
                "grid_lcs": baseline.get("grid_lcs"),
            },
            ensure_ascii=False,
        ),
        source_gt=str(record.get("ground_truth") or "")[:2000],
        source_side=str(record.get("side_matter") or "")[:1000],
        caption=str(record.get("label") or "")[:400],
        main_json=json.dumps(main, ensure_ascii=False)[:8000],
        side_json=json.dumps(side, ensure_ascii=False)[:4000],
    )
    used = model
    raw = ""
    try:
        raw, used = call_chat_completion(prompt, model=model, temperature=0.1, max_tokens=8192)
        parsed = _parse_eval_json(raw)
        # One retry if the first reply was not JSON / Retry một lần nếu lần 1 không ra JSON
        if not parsed:
            print(
                f"{LOG} judge_parse_retry image={_image_key(record)} model={used} "
                f"raw={raw[:400]!r}",
                flush=True,
            )
            raw, used = call_chat_completion(
                prompt + "\n\nYour previous reply was not valid JSON. Return ONLY the JSON object.",
                model=model,
                temperature=0.0,
                max_tokens=8192,
            )
            parsed = _parse_eval_json(raw)
    except Exception as exc:
        print(f"{LOG} judge_fail image={_image_key(record)} err={exc}", flush=True)
        return None
    if not parsed:
        print(
            f"{LOG} judge_parse_fail image={_image_key(record)} model={used} raw={raw[:400]!r}",
            flush=True,
        )
        return None
    out = _parse_eval_response(parsed, baseline=baseline, record=record)
    out["eval_model"] = used
    return out


def _maybe_patched(row: dict[str, Any]) -> dict[str, Any] | None:
    if row.get("status") not in {"refined", "re_ocr_done"}:
        return None
    if row.get("status") == "needs_hitl":
        return None
    gt = str(row.get("gt_refined") or "").strip()
    if not gt:
        return None
    return {
        "schema_version": SCHEMA_EVAL,
        "image": row.get("image"),
        "post_id": row.get("post_id"),
        "ground_truth": gt,
        "side_matter": str(row.get("side_matter_refined") or ""),
        "seals": row.get("seals_refined") or [],
        "patch_from": SCHEMA_EVAL,
        "replace_gt": False,
        "eval_at": row.get("eval_at"),
    }


def _reocr_one(record: dict[str, Any], eval_row: dict[str, Any]) -> dict[str, Any] | None:
    """Vision re-OCR for flagged images / Re-OCR vision cho ảnh bị cờ."""
    from final_exam_nlp_ocr import (
        GEMINI_PROMPT,
        _ground_truth,
        _object_key_from_image_path,
        _read_object_bytes,
        _vision_ocr,
        preprocess_light,
    )
    from common.io_storage import object_exists

    settings = _settings()
    bucket = settings["bucket_raw"]
    source_prefix = settings["source_prefix"]
    group_id = str(record.get("group_id") or os.environ.get("FEN_GROUP_ID") or "")
    image_path = _image_key(record)
    object_key = _object_key_from_image_path(source_prefix, group_id, image_path)
    if not object_exists(bucket, object_key):
        print(f"{LOG} reocr_missing {object_key}", flush=True)
        return None
    raw_bytes = _read_object_bytes(bucket, object_key)
    processed = preprocess_light(raw_bytes)
    model = get_value(load_config(), "final_exam_nlp", "eval_reocr_model", fallback="gemini-3.6-flash-high")
    # Same vision prompt as live OCR / Cùng prompt vision với OCR live
    raw, err = _vision_ocr(processed, model=model, prompt=GEMINI_PROMPT)
    if err or not raw:
        print(f"{LOG} reocr_fail image={image_path} err={err}", flush=True)
        return None
    parsed = extract_json_object(raw) or {}
    boxes = _normalize_boxes(parsed.get("gemini") or [])
    main, side = _split_boxes(boxes)
    gt, side_matter, gt_err = _ground_truth(main, side, str(record.get("label") or ""))
    new_record = dict(record)
    new_record["gemini"] = boxes
    new_record["ground_truth"] = gt
    new_record["side_matter"] = side_matter
    new_record["gt_error"] = gt_err
    new_record["ocr_engine"] = model
    eval_row = dict(eval_row)
    eval_row["eval_round"] = int(eval_row.get("eval_round") or 1) + 1
    eval_row["re_ocr_of"] = eval_row.get("eval_at")
    eval_row["source_gt"] = gt
    eval_row["source_side_matter"] = side_matter
    eval_row["status"] = "re_ocr_done"
    eval_row["eval_at"] = utc_now_iso()
    return {"ocr": new_record, "eval": eval_row}


def run_ocr_eval(
    *,
    group_id: str,
    batch_size: int = 200,
    force: bool = False,
    run_reocr: bool = True,
    reocr_limit: int = 15,
) -> dict[str, Any]:
    """Eval one batch of OCR images and queue rollover.

    Đánh giá một batch ảnh OCR và xếp hàng rollover.
    """
    settings = _settings()
    bucket = settings["bucket_raw"]
    source_prefix = settings["source_prefix"]
    gid = group_id or "322453387859386"
    cfg = load_config()
    model = (
        os.environ.get("FEN_EVAL_MODEL", "").strip()
        or get_value(cfg, "final_exam_nlp", "eval_qwen_model", fallback="gpt-5.6-luna")
    )
    delay = _float_env(
        "FEN_EVAL_DELAY_SEC",
        float(get_value(cfg, "final_exam_nlp", "eval_page_delay_sec", fallback="2.5") or "2.5"),
    )
    flush_every = max(
        1,
        _int_env(
            "FEN_EVAL_FLUSH_EVERY",
            int(get_value(cfg, "final_exam_nlp", "eval_flush_every", fallback=str(EVAL_FLUSH_EVERY)) or EVAL_FLUSH_EVERY),
        ),
    )
    flush_sec = _float_env(
        "FEN_EVAL_FLUSH_SEC",
        float(get_value(cfg, "final_exam_nlp", "eval_flush_sec", fallback=str(EVAL_FLUSH_SEC)) or EVAL_FLUSH_SEC),
    )
    root = eval_root(source_prefix, gid)
    ocr_key = _ocr_output_key(source_prefix, gid)
    qwen_key = f"{root}/qwen_order.jsonl"
    baseline_key = f"{root}/baseline_order.jsonl"
    ck_key = f"{root}/checkpoint.json"
    summary_key = f"{root}/summary.json"
    patched_key = f"{root}/patched.jsonl"
    reocr_key = f"{root}/queue/re_ocr.jsonl"
    refine_key = f"{root}/queue/refine.jsonl"
    hitl_key = f"{root}/queue/hitl.jsonl"

    ocr_rows = read_jsonl(bucket, ocr_key)
    ocr_by_image = {_image_key(row): row for row in ocr_rows if _image_key(row)}
    print(f"{LOG} ocr_rows={len(ocr_by_image)} key={ocr_key} model={model}", flush=True)

    baselines = {row["image"]: row for row in read_jsonl(bucket, baseline_key) if row.get("image")}
    need_baseline = force or len(baselines) + 50 < len(ocr_by_image)
    if need_baseline:
        print(f"{LOG} rebuild baseline n={len(ocr_by_image)}", flush=True)
        baselines = {}
        for rec in ocr_by_image.values():
            base = baseline_for_record(rec)
            if base.get("image"):
                baselines[base["image"]] = base
        write_jsonl(bucket, baseline_key, list(baselines.values()))

    done = set()
    if not force:
        done = {_image_key(row) for row in read_jsonl(bucket, qwen_key) if _image_key(row)}
    pending: list[dict[str, Any]] = []
    for image, rec in ocr_by_image.items():
        if image in done:
            continue
        base = baselines.get(image) or baseline_for_record(rec)
        rtl = int(base.get("rtl_lcs") or 0)
        ltr = int(base.get("ltr_lcs") or 0)
        rank = 0 if rtl > ltr + 1 else 1
        pending.append((rank, -rtl, image, rec, base))
    pending.sort(key=lambda item: (item[0], item[1], item[2]))
    batch = pending[: max(1, batch_size)]
    print(f"{LOG} pending={len(pending)} take={len(batch)} done={len(done)} force={force}", flush=True)

    written: list[dict[str, Any]] = []
    counts = Counter()
    pending_flush = 0
    flushes = 0
    last_flush_at = time.time()
    print(
        f"{LOG} flush_every={flush_every} flush_sec={flush_sec}",
        flush=True,
    )
    for index, (_rank, _rtl, image, rec, base) in enumerate(batch):
        judged = _judge_one(rec, base, model=model)
        if not judged:
            counts["judge_fail"] += 1
            continue
        judged["eval_batch"] = _int_env("FEN_EVAL_BATCH", 0) or (len(done) // max(batch_size, 1)) + 1
        post_id = str(judged.get("post_id") or "unknown")
        detail_key = f"{root}/details/{post_id}/{_safe_image_name(image)}.json"
        upload_text_payload(bucket, detail_key, json.dumps(judged, ensure_ascii=False, indent=2), suffix=".json")
        written.append(judged)
        counts[str(judged.get("status"))] += 1
        counts[f"order:{judged.get('reading_order')}"] += 1
        queue_name = _queue_action(str(judged.get("status")))
        if queue_name == "re_ocr":
            _append_queue(
                bucket,
                reocr_key,
                {
                    "schema_version": SCHEMA_EVAL,
                    "image": image,
                    "post_id": post_id,
                    "reason_flags": [f["code"] for f in judged.get("flags") or []],
                    "preferred_model": get_value(
                        cfg, "final_exam_nlp", "eval_reocr_model", fallback="gemini-3.6-flash-high"
                    ),
                    "eval_batch": judged.get("eval_batch"),
                    "queued_at": utc_now_iso(),
                },
            )
        elif queue_name == "hitl":
            _append_queue(bucket, hitl_key, {"image": image, "post_id": post_id, "status": "needs_hitl", "queued_at": utc_now_iso()})
        elif queue_name == "refine":
            _append_queue(bucket, refine_key, {"image": image, "post_id": post_id, "status": "refined", "queued_at": utc_now_iso()})
        patched = _maybe_patched(judged)
        if patched:
            _append_queue(bucket, patched_key, patched)
        print(
            f"{LOG} {index+1}/{len(batch)} image={image[-40:]} status={judged.get('status')} "
            f"order={judged.get('reading_order')} conf={judged.get('confidence')}",
            flush=True,
        )
        pending_flush += 1
        if should_flush(
            pending=pending_flush,
            last_flush_at=last_flush_at,
            every=flush_every,
            interval_sec=flush_sec,
        ):
            _merge_jsonl(bucket, qwen_key, written)
            flushes += 1
            elapsed = time.time() - last_flush_at
            print(
                f"{LOG} flush n={pending_flush} total={len(written)} flushes={flushes} "
                f"elapsed_sec={elapsed:.1f}",
                flush=True,
            )
            pending_flush = 0
            last_flush_at = time.time()
        if delay > 0 and index + 1 < len(batch):
            time.sleep(delay)

    # Final leftover flush / Flush phần còn lại cuối batch
    if pending_flush > 0:
        flushes += 1
        elapsed = time.time() - last_flush_at
        print(
            f"{LOG} flush_final n={pending_flush} total={len(written)} flushes={flushes} "
            f"elapsed_sec={elapsed:.1f}",
            flush=True,
        )
    merged = _merge_jsonl(bucket, qwen_key, written)
    remaining = max(0, len(ocr_by_image) - len(merged))
    reocr_done = 0
    if run_reocr and reocr_limit > 0:
        queue = read_jsonl(bucket, reocr_key)
        take = queue[:reocr_limit]
        keep = queue[reocr_limit:]
        updates: list[dict[str, Any]] = []
        for item in take:
            image = str(item.get("image") or "")
            rec = ocr_by_image.get(image)
            ev = merged.get(image)
            if not rec or not ev:
                keep.append(item)
                continue
            try:
                result = _reocr_one(rec, ev)
            except Exception as exc:
                print(f"{LOG} reocr_exc image={image} err={exc}", flush=True)
                keep.append(item)
                continue
            if not result:
                keep.append(item)
                continue
            version_key = f"{root}/versions/{ev.get('post_id') or 'unknown'}/{_safe_image_name(image)}.jsonl"
            prev = read_jsonl(bucket, version_key)
            prev.append({"round": ev.get("eval_round"), "eval": ev, "at": utc_now_iso()})
            write_jsonl(bucket, version_key, prev)
            updates.append(result["eval"])
            reocr_done += 1
        write_jsonl(bucket, reocr_key, keep)
        if updates:
            merged = _merge_jsonl(bucket, qwen_key, updates)

    # Stop rollover if Qwen judged nothing this run / Dừng rollover nếu Qwen không chấm được ảnh nào
    stop_reason = None
    should_continue = remaining > 0
    if should_continue and batch and not written:
        should_continue = False
        stop_reason = "judge_fail_empty_batch"
    elif not should_continue:
        stop_reason = "caught_up"
    checkpoint = {
        "schema_version": SCHEMA_EVAL,
        "group_id": gid,
        "source_ocr_key": ocr_key,
        "batch_size": batch_size,
        "eval_batch": _int_env("FEN_EVAL_BATCH", 0) or (len(merged) // max(batch_size, 1)),
        "done_images": len(merged),
        "remaining_hint": remaining,
        "last_image": written[-1]["image"] if written else "",
        "last_run_at": utc_now_iso(),
        "status": "rollover" if should_continue else (stop_reason or "caught_up"),
        "should_continue": should_continue,
        "stop_reason": stop_reason,
        "judged_this_run": len(written),
        "reocr_this_run": reocr_done,
        "jsonl_flush_every": flush_every,
        "jsonl_flush_sec": flush_sec,
        "jsonl_flushes": flushes,
    }
    upload_json_payload(bucket, ck_key, checkpoint)
    order_counts = Counter(str(row.get("reading_order")) for row in written)
    cap_counts = Counter(str(row.get("caption_align")) for row in written)
    flag_counts: Counter[str] = Counter()
    for row in written:
        for flag in row.get("flags") or []:
            flag_counts[str(flag.get("code"))] += 1
    summary = {
        "schema_version": SCHEMA_EVAL,
        "eval_batch": checkpoint["eval_batch"],
        "n": len(written),
        "reading_order": dict(order_counts),
        "caption_align": dict(cap_counts),
        "status_counts": dict(counts),
        "issue_counts": dict(flag_counts),
        "mean_confidence": round(
            sum(float(row.get("confidence") or 0) for row in written) / max(len(written), 1), 4
        ),
        "remaining_hint": remaining,
        "should_continue": should_continue,
        "jsonl_flush_every": flush_every,
        "jsonl_flush_sec": flush_sec,
        "jsonl_flushes": flushes,
        "updated_at": utc_now_iso(),
    }
    upload_json_payload(bucket, summary_key, summary)
    print(f"{LOG} done {summary}", flush=True)
    return {**summary, "checkpoint": checkpoint}


def main() -> None:
    group_id = os.environ.get("FEN_GROUP_ID", "").strip() or "322453387859386"
    run_ocr_eval(
        group_id=group_id,
        batch_size=_int_env("FEN_EVAL_BATCH_SIZE", 200),
        force=_bool_env("FEN_EVAL_FORCE", False),
        run_reocr=_bool_env("FEN_EVAL_REOCR", True),
        reocr_limit=_int_env("FEN_EVAL_REOCR_LIMIT", 15),
    )


if __name__ == "__main__":
    main()
