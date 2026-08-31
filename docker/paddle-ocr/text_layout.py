from __future__ import annotations

import re
from typing import Any

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


def _block_item(block: dict[str, Any]) -> dict[str, float | str] | None:
    # Normalize one OCR block into layout item / Chuẩn hóa một block OCR cho layout
    text = str(block.get("text", "")).strip()
    bounds = _box_bounds(block.get("box"))
    if not text or bounds is None:
        return None
    x0, y0, x1, y1 = bounds
    width = max(x1 - x0, 1.0)
    height = max(y1 - y0, 1.0)
    compact = "".join(ch for ch in text if not ch.isspace())
    cjk_ratio = len(_CJK_CHAR.findall(compact)) / max(len(compact), 1)
    return {
        "text": text,
        "x0": x0,
        "y0": y0,
        "x1": x1,
        "y1": y1,
        "w": width,
        "h": height,
        "cx": (x0 + x1) / 2.0,
        "cy": (y0 + y1) / 2.0,
        "cjk_ratio": cjk_ratio,
    }


def _is_vertical_cjk(item: dict[str, float | str]) -> bool:
    # Tall CJK boxes are vertical columns / Box CJK cao là cột dọc
    height = float(item["h"])
    width = float(item["w"])
    cjk_ratio = float(item["cjk_ratio"])
    return height > width * 1.4 and cjk_ratio >= 0.4


def _render_horizontal_items(
    items: list[dict[str, float | str]],
    *,
    line_y_ratio: float = 0.6,
    space_gap_ratio: float = 0.35,
) -> list[tuple[float, str]]:
    # Group wide blocks into horizontal lines / Gom block ngang thành từng dòng
    if not items:
        return []

    items.sort(key=lambda item: (float(item["cy"]), float(item["x0"])))
    median_height = sorted(float(item["h"]) for item in items)[len(items) // 2]
    line_threshold = max(median_height * line_y_ratio, 4.0)

    lines: list[list[dict[str, float | str]]] = []
    current_line: list[dict[str, float | str]] = []
    current_center_y: float | None = None

    for item in items:
        center_y = float(item["cy"])
        if current_center_y is None or abs(center_y - current_center_y) <= line_threshold:
            current_line.append(item)
            centers = [float(entry["cy"]) for entry in current_line]
            current_center_y = sum(centers) / len(centers)
            continue
        lines.append(current_line)
        current_line = [item]
        current_center_y = center_y

    if current_line:
        lines.append(current_line)

    rendered: list[tuple[float, str]] = []
    for line in lines:
        line.sort(key=lambda item: float(item["x0"]))
        parts: list[str] = []
        previous_x1: float | None = None
        previous_height = float(line[0]["h"])
        for item in line:
            if previous_x1 is not None:
                gap = float(item["x0"]) - previous_x1
                if gap > previous_height * space_gap_ratio:
                    parts.append(" ")
            parts.append(str(item["text"]))
            previous_x1 = float(item["x1"])
            previous_height = float(item["h"])
        center_y = sum(float(item["cy"]) for item in line) / len(line)
        rendered.append((center_y, "".join(parts)))
    return rendered


def _render_vertical_items(items: list[dict[str, float | str]]) -> list[tuple[float, str]]:
    # Cluster vertical CJK blocks into columns top-to-bottom / Gom cột CJK dọc từ trên xuống
    if not items:
        return []

    items.sort(key=lambda item: (float(item["cx"]), float(item["y0"])))
    median_width = sorted(float(item["w"]) for item in items)[len(items) // 2]
    column_threshold = max(median_width * 0.75, 24.0)

    columns: list[list[dict[str, float | str]]] = []
    current_column: list[dict[str, float | str]] = []
    current_center_x: float | None = None

    for item in items:
        center_x = float(item["cx"])
        if current_center_x is None or abs(center_x - current_center_x) <= column_threshold:
            current_column.append(item)
            centers = [float(entry["cx"]) for entry in current_column]
            current_center_x = sum(centers) / len(centers)
            continue
        columns.append(current_column)
        current_column = [item]
        current_center_x = center_x

    if current_column:
        columns.append(current_column)

    rendered: list[tuple[float, str]] = []
    for column in columns:
        column.sort(key=lambda item: float(item["y0"]))
        for item in column:
            rendered.append((float(item["cy"]), str(item["text"])))
    return rendered


def reconstruct_text_from_blocks(
    blocks: list[dict[str, Any]],
    *,
    line_y_ratio: float = 0.6,
    space_gap_ratio: float = 0.35,
) -> str:
    """Rebuild page text using bbox layout instead of naive join.

    Ghép lại văn bản trang theo tọa độ box; tách cột Hán dọc khỏi dòng Latin ngang.
    """
    items: list[dict[str, float | str]] = []
    for block in blocks:
        item = _block_item(block)
        if item is not None:
            items.append(item)

    if not items:
        return ""

    vertical_items = [item for item in items if _is_vertical_cjk(item)]
    horizontal_items = [item for item in items if item not in vertical_items]

    rendered_lines = _render_horizontal_items(
        horizontal_items,
        line_y_ratio=line_y_ratio,
        space_gap_ratio=space_gap_ratio,
    )
    rendered_lines.extend(_render_vertical_items(vertical_items))
    rendered_lines.sort(key=lambda pair: pair[0])
    return "\n".join(text for _, text in rendered_lines if text.strip())
