"""Unit tests for sort_ink_by_bbox (RTL / Y-cut / orphan).

Kiểm thử sort_ink_by_bbox (RTL / Y-cut / orphan).
"""
from __future__ import annotations

import sys
from pathlib import Path

_JOBS = Path(__file__).resolve().parents[1]
if str(_JOBS) not in sys.path:
    sys.path.insert(0, str(_JOBS))

import final_exam_nlp_ocr_label_dual as ld  # noqa: E402


def _box(text: str, bb: list[int], kind: str = "ink_text") -> dict:
    return {"text": text, "bounding_box": bb, "kind": kind, "confidence": 0.9}


def test_rtl_columns_right_to_left() -> None:
    # Three columns LTR in input list → output must be right column first /
    # Ba cột đưa vào LTR → output phải cột phải trước
    boxes = [
        _box("左柱", [10, 100, 200, 160]),
        _box("中柱", [10, 400, 200, 460]),
        _box("右柱", [10, 700, 200, 760]),
    ]
    out = ld.sort_ink_by_bbox(boxes)
    assert [b["text"] for b in out] == ["右柱", "中柱", "左柱"]


def test_orphan_merges_into_previous_column_box() -> None:
    # Same X band: long line + orphan 夜深 → one box (orphan trailing) /
    # Cùng dải X: câu dài + orphan 夜深 → một box (orphan ở đuôi)
    boxes = [
        _box("飽秉燭相尋語", [50, 100, 800, 160]),
        _box("夜深", [50, 100, 200, 150]),
    ]
    out = ld.sort_ink_by_bbox(boxes)
    assert len(out) == 1
    assert out[0]["text"] == "飽秉燭相尋語夜深"


def test_orphan_column_merges_into_longer_rtl_neighbor() -> None:
    # Orphan shifted left (own X) still joins the longer column to its right /
    # Orphan lệch trái (X riêng) vẫn nhập cột dài bên phải
    boxes = [
        _box("眾教會信心越發堅固人數天天加", [100, 200, 900, 280]),
        _box("增", [100, 80, 200, 140]),
    ]
    out = ld.sort_ink_by_bbox(boxes)
    assert len(out) == 1
    assert out[0]["text"].endswith("增") or "增" in out[0]["text"]


def test_short_sole_column_kept() -> None:
    # Real 2-char couplet column must not vanish / Cột đối 2 chữ thật không bị xoá
    boxes = [
        _box("神茶", [50, 700, 200, 780]),
        _box("蘭壘", [50, 200, 200, 280]),
    ]
    out = ld.sort_ink_by_bbox(boxes)
    assert [b["text"] for b in out] == ["神茶", "蘭壘"]


def test_y_cut_upper_tier_before_lower() -> None:
    # Upper band (cy~200) then lower (cy~800); within band RTL /
    # Dải trên (cy~200) rồi dưới (cy~800); trong dải RTL
    boxes = [
        _box("下右", [700, 700, 900, 780]),
        _box("下左", [700, 200, 900, 280]),
        _box("上左", [100, 200, 300, 280]),
        _box("上右", [100, 700, 300, 780]),
    ]
    out = ld.sort_ink_by_bbox(boxes, y_gap=80)
    assert [b["text"] for b in out] == ["上右", "上左", "下右", "下左"]


def test_reassemble_keeps_margin_after_ink() -> None:
    ink = [_box("正文", [10, 500, 200, 560])]
    page = ink + [_box("落款", [400, 100, 700, 140], kind="margin")]
    ordered = ld.sort_ink_by_bbox(ink)
    out = ld.reassemble_page_boxes(page, ordered)
    assert out[0]["kind"] == "ink_text"
    assert out[-1]["kind"] == "margin"
