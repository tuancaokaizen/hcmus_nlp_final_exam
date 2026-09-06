"""Unit tests for fen_gt_clean (Han punct kept by default).

Kiểm thử làm sạch GT — mặc định giữ dấu câu Hán.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_JOBS = Path(__file__).resolve().parents[1]
if str(_JOBS) not in sys.path:
    sys.path.insert(0, str(_JOBS))

from common.fen_gt_clean import (  # noqa: E402
    clean_b2_texts,
    clean_ground_truth,
    clean_side_matter,
)


def test_keeps_han_punct_and_cjk() -> None:
    raw = "竹雨松風，琴韻。\n茶煙梧月書聲！"
    out, log = clean_ground_truth(raw, keep_han_punct=True)
    assert out == "竹雨松風，琴韻。\n茶煙梧月書聲！"
    assert log == "unchanged"


def test_strips_emoji_latin_western_punct() -> None:
    raw = "竹雨松風琴韻❤★\nHello茶煙梧月？OK!"
    out, log = clean_ground_truth(raw, keep_han_punct=True)
    assert "❤" not in out and "★" not in out
    assert "Hello" not in out and "OK" not in out
    assert "!" not in out  # ASCII bang dropped
    assert "？" in out  # fullwidth / Han question kept if present — raw used ？
    # Raw used ASCII ! after OK and fullwidth？ — check：
    # "Hello茶煙梧月？OK!" → Latin gone, ？ kept, ! gone
    assert "茶煙梧月？" in out
    assert "icons_emoji_or_latin" in log or "punct" in log


def test_strips_device_watermark() -> None:
    raw = "臨江仙\nREDMI NOTE 12 2024/01/02 12:30:00"
    out, log = clean_ground_truth(raw, keep_han_punct=True)
    assert "REDMI" not in out
    assert "臨江仙" in out
    assert "device_wm_or_timestamp" in log


def test_preserves_year_and_verse_digits() -> None:
    raw = "詩篇五十九：8\n作於2024年"
    out, _ = clean_ground_truth(raw, keep_han_punct=True)
    assert "：8" in out or "8" in out
    assert "2024" in out


def test_hard_strip_removes_han_punct() -> None:
    raw = "竹雨，松風。"
    out, log = clean_ground_truth(raw, keep_han_punct=False)
    assert "，" not in out and "。" not in out
    assert "竹雨" in out and "松風" in out
    assert "punct_strip_all_modern" in log


def test_hallucination_repeat_collapsed() -> None:
    raw = "龍" * 8
    out, log = clean_ground_truth(raw, keep_han_punct=True)
    assert out == "龍"
    assert "hallucination_repeat_ge6" in log


def test_triple_repeat_poetry_kept() -> None:
    raw = "莫莫莫"
    out, log = clean_ground_truth(raw, keep_han_punct=True)
    assert out == "莫莫莫"
    assert log == "unchanged"


def test_clean_side_matter_drops_seal_tag_and_emoji() -> None:
    raw = "[seal]\n壬寅秋\n👍"
    out = clean_side_matter(raw)
    assert "[seal]" not in out
    assert "👍" not in out
    assert "壬寅秋" in out


def test_clean_b2_texts_disabled() -> None:
    meta = clean_b2_texts("竹★", "[seal]款", enabled=False)
    assert meta["ground_truth"] == "竹★"
    assert meta["side_matter"] == "[seal]款"
    assert meta["changed"] is False


def test_clean_b2_texts_enabled_changes() -> None:
    meta = clean_b2_texts("竹★雨", "款♥\n[seal]", enabled=True, keep_han_punct=True)
    assert meta["ground_truth"] == "竹雨"
    assert "[seal]" not in meta["side_matter"]
    assert "♥" not in meta["side_matter"]
    assert meta["changed"] is True


def test_to_task_b2_row_applies_clean(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FEN_LABEL_CLEAN_GT", "true")
    monkeypatch.setenv("FEN_LABEL_CLEAN_GT_KEEP_HAN_PUNCT", "true")
    from final_exam_nlp_ocr_label_dual import to_task_b2_row

    row = to_task_b2_row(
        image="/images/x/1.jpg",
        ground_truth="人生說難也不難❤\n說容易也不容易！",
        side_matter="[seal]\n甲辰",
        gemini=[{"text": "人", "bounding_box": [0, 0, 1, 1], "kind": "ink_text"}],
        label="caption",
    )
    assert "❤" not in row["ground_truth"]
    assert "！" in row["ground_truth"]  # Han bang kept
    assert row["side_matter"] == "甲辰"
    assert row["label"] == "caption"
