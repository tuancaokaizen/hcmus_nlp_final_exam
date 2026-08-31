from __future__ import annotations

import re
from typing import Any

# Vietnamese + Latin letters / Chữ Latin và tiếng Việt có dấu
_LATIN_CHAR = re.compile(
    r"[A-Za-z"
    r"\u00C0-\u024F"
    r"\u1E00-\u1EFF"
    r"\u0300-\u036F"
    r"]"
)
_CJK_CHAR = re.compile(r"[\u3400-\u4DBF\u4E00-\u9FFF\uF900-\uFAFF]")


def _box_bounds(box: Any) -> tuple[float, float, float, float] | None:
    # Convert polygon box to axis-aligned bounds / Chuyển box đa giác sang khung chữ nhật
    if not box:
        return None
    points: list[tuple[float, float]] = []
    for point in box:
        if isinstance(point, (list, tuple)) and len(point) >= 2:
            points.append((float(point[0]), float(point[1])))
    if not points:
        return None
    xs = [point[0] for point in points]
    ys = [point[1] for point in points]
    return min(xs), min(ys), max(xs), max(ys)


def _box_iou(box_a: Any, box_b: Any) -> float:
    # Intersection-over-union for two OCR boxes / IoU giữa hai box OCR
    bounds_a = _box_bounds(box_a)
    bounds_b = _box_bounds(box_b)
    if bounds_a is None or bounds_b is None:
        return 0.0
    ax0, ay0, ax1, ay1 = bounds_a
    bx0, by0, bx1, by1 = bounds_b
    inter_x0 = max(ax0, bx0)
    inter_y0 = max(ay0, by0)
    inter_x1 = min(ax1, bx1)
    inter_y1 = min(ay1, by1)
    if inter_x1 <= inter_x0 or inter_y1 <= inter_y0:
        return 0.0
    inter_area = (inter_x1 - inter_x0) * (inter_y1 - inter_y0)
    area_a = max((ax1 - ax0) * (ay1 - ay0), 1.0)
    area_b = max((bx1 - bx0) * (by1 - by0), 1.0)
    return inter_area / (area_a + area_b - inter_area)


def _script_ratios(text: str) -> tuple[float, float]:
    # Return (cjk_ratio, latin_ratio) over non-space chars / Tỷ lệ CJK và Latin trên ký tự không space
    compact = "".join(ch for ch in text if not ch.isspace())
    if not compact:
        return 0.0, 0.0
    cjk_count = len(_CJK_CHAR.findall(compact))
    latin_count = len(_LATIN_CHAR.findall(compact))
    total = len(compact)
    return cjk_count / total, latin_count / total


def _pick_block(ch_block: dict[str, Any], latin_block: dict[str, Any] | None) -> dict[str, Any]:
    # Choose Chinese or Latin recognition for one spatial cluster / Chọn nhận dạng ch hoặc latin cho một cụm
    if latin_block is None:
        chosen = dict(ch_block)
        chosen["source"] = "ch"
        return chosen

    ch_text = str(ch_block.get("text", ""))
    latin_text = str(latin_block.get("text", ""))
    ch_conf = float(ch_block.get("confidence", 0.0))
    latin_conf = float(latin_block.get("confidence", 0.0))
    ch_cjk, ch_latin = _script_ratios(ch_text)
    lat_cjk, lat_latin = _script_ratios(latin_text)

    if ch_cjk >= 0.35 and ch_cjk >= ch_latin and ch_cjk >= lat_cjk:
        chosen = dict(ch_block)
        chosen["source"] = "ch"
        return chosen
    if lat_latin >= 0.35 or (ch_latin >= 0.35 and lat_latin >= ch_latin):
        if latin_conf >= ch_conf * 0.85:
            chosen = dict(latin_block)
            chosen["source"] = "latin"
            return chosen
    if latin_conf > ch_conf * 1.1 and lat_latin >= ch_latin:
        chosen = dict(latin_block)
        chosen["source"] = "latin"
        return chosen

    chosen = dict(ch_block if ch_conf >= latin_conf else latin_block)
    chosen["source"] = "ch" if ch_conf >= latin_conf else "latin"
    return chosen


def merge_dual_pass_blocks(
    ch_blocks: list[dict[str, Any]],
    latin_blocks: list[dict[str, Any]],
    *,
    iou_threshold: float = 0.25,
) -> list[dict[str, Any]]:
    """Merge Chinese and Latin OCR blocks by bbox overlap.

    Gộp block OCR tiếng Trung và Latin theo độ chồng bbox.
    """
    used_latin: set[int] = set()
    merged: list[dict[str, Any]] = []

    for ch_block in ch_blocks:
        best_idx: int | None = None
        best_iou = 0.0
        for idx, latin_block in enumerate(latin_blocks):
            if idx in used_latin:
                continue
            iou = _box_iou(ch_block.get("box"), latin_block.get("box"))
            if iou > best_iou:
                best_iou = iou
                best_idx = idx
        latin_match = latin_blocks[best_idx] if best_idx is not None and best_iou >= iou_threshold else None
        if best_idx is not None and latin_match is not None:
            used_latin.add(best_idx)
        merged.append(_pick_block(ch_block, latin_match))

    for idx, latin_block in enumerate(latin_blocks):
        if idx in used_latin:
            continue
        if not any(_box_iou(latin_block.get("box"), block.get("box")) >= iou_threshold for block in ch_blocks):
            extra = dict(latin_block)
            extra["source"] = "latin"
            merged.append(extra)

    merged.sort(
        key=lambda block: (
            float((_box_bounds(block.get("box")) or (0, 0, 0, 0))[1]),
            float((_box_bounds(block.get("box")) or (0, 0, 0, 0))[0]),
        )
    )
    return merged
