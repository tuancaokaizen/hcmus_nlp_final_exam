"""Clean calligraphy ground_truth / side_matter for Task B2 submit.

Làm sạch ground_truth / side_matter thư pháp trước khi nộp Task B2.

Ported from test/clean_ground_truth_and_side_matter.py with optional Han
punctuation retention (exam default) /
Port từ script test; mặc định exam giữ dấu câu Hán.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Any

# CJK + Ext-A + compat ideographs / CJK + Ext-A + ideograph tương thích
_CJK_CP = (
    (0x3400, 0x4DBF),
    (0x4E00, 0x9FFF),
    (0xF900, 0xFAFF),
    (0x20000, 0x2FA1F),
)

# Keep when keep_han_punct=True / Giữ khi keep_han_punct=True
HAN_PUNCT_KEEP = frozenset(
    "，。、；：？！「」『』（）《》〈〉【】—–…·‥〃『』「」"
)

# Device watermark lines / Dòng watermark máy ảnh
_DEVICE_WM_RE = re.compile(
    r"(?i)(?:REDMI|IPHONE|SAMSUNG|HUAWEI|VIVO|OPPO|XIAOMI)[^\n]*"
)
_TIMESTAMP_RE = re.compile(
    r"\d{4}[./-]\d{2}[./-]\d{2}(?:\s+\d{2}:\d{2}(?::\d{2})?)?"
)
_SEAL_SIZE_RE = re.compile(
    r"(?:尺寸\s*[:：]\s*)?\d+(?:\.\d+)?\s*(?:[xX×*]\s*\d+(?:\.\d+)?)+(?:\s*(?:cm|mm))?"
)
_CM_MM_RE = re.compile(r"\d+(?:\.\d+)?\s*(?:cm|mm)")
_GRID_RE = re.compile(r"\d+\s*格")
_SEAL_TAG_RE = re.compile(
    r"\[seal\]|\[SEAL\]|\[unreadable\]|\[other\]|inktext|margin",
    re.IGNORECASE,
)
_VERSE_PROTECT_RE = re.compile(
    r"(?:[\u4e00-\u9fff]+[:：]\d+)|(?:\d+[-–—~]\d+)|(?:\d+[:：.]\d+)"
)
# Strip Western / ASCII punct only (Han punct kept separately) /
# Chỉ bỏ dấu Tây / ASCII (dấu Hán giữ riêng)
_WESTERN_PUNCT_RE = re.compile(r"[?!~～;,\.\"'`\(\)\[\]\{\}\*\^#@%&_+=|\\/<>]")
# Legacy hard strip (all modern punct incl. Han) / Strip cứng (mọi dấu hiện đại kể cả Hán)
_ALL_MODERN_PUNCT_RE = re.compile(
    r"[?？!！~～:：;；,，。、“”‘’（）()《》〈〉【】\[\]\.\*\^#@%&_+=|\\/`]"
)
_LATIN_RE = re.compile(r"[a-zA-Z]+")
_HALLUC_REP_RE = re.compile(r"([^\s])(?:\s*\1){5,}")
_ICON_EXTRA = frozenset("♥👍☉■□💜🔷◆^★☆▲▼●○◇")


def _is_cjk(ch: str) -> bool:
    """True if codepoint is CJK ideograph / True nếu là chữ Hán."""
    cp = ord(ch)
    return any(lo <= cp <= hi for lo, hi in _CJK_CP)


def _strip_icons_and_symbols(text: str) -> str:
    """Drop emoji / So-Sk symbols that are not CJK or Han punct.

    Bỏ emoji / symbol So-Sk không phải CJK hoặc dấu Hán.
    """
    out: list[str] = []
    for ch in text:
        if ch in _ICON_EXTRA:
            continue
        cp = ord(ch)
        # Emoji blocks / Khối emoji
        if 0x1F300 <= cp <= 0x1FAFF or 0x2600 <= cp <= 0x27BF:
            continue
        cat = unicodedata.category(ch)
        if cat in ("So", "Sk") and not (0x3000 <= cp <= 0x303F):
            if not _is_cjk(ch) and ch not in HAN_PUNCT_KEEP:
                continue
        out.append(ch)
    return "".join(out)


def _protect_verse(m: re.Match[str]) -> str:
    """Tokenize verse number connectors during punct strip / Token hoá nối số câu khi strip dấu."""
    return (
        m.group(0)
        .replace("：", "TOKENCOLON")
        .replace(":", "TOKENCOLON")
        .replace("-", "TOKENDASH")
        .replace("–", "TOKENDASH")
        .replace("—", "TOKENDASH")
        .replace(".", "TOKENDOT")
    )


def _restore_verse(text: str) -> str:
    return (
        text.replace("TOKENCOLON", "：")
        .replace("TOKENDASH", "-")
        .replace("TOKENDOT", ".")
    )


def _strip_shared_noise(text: str) -> tuple[str, list[str]]:
    """Watermark / size / seal-tag cleanup shared by GT and SM.

    Khử watermark / kích thước / tag seal dùng chung GT và SM.
    """
    mods: list[str] = []
    t = text
    before = t
    t = _DEVICE_WM_RE.sub("", t)
    t = _TIMESTAMP_RE.sub("", t)
    if t != before:
        mods.append("device_wm_or_timestamp")
    before = t
    t = _SEAL_SIZE_RE.sub("", t)
    t = _CM_MM_RE.sub("", t)
    t = _GRID_RE.sub("", t)
    if t != before:
        mods.append("seal_size_or_grid")
    before = t
    t = _SEAL_TAG_RE.sub("", t)
    if t != before:
        mods.append("seal_or_tech_tag")
    return t, mods


def clean_ground_truth(
    raw_gt: str,
    *,
    keep_han_punct: bool = True,
) -> tuple[str, str]:
    """Clean main calligraphy ground_truth; return (text, mod_log).

    Làm sạch ground_truth chính; trả (text, mod_log).
    keep_han_punct=True keeps ，。！？「」… (exam default) /
    keep_han_punct=True giữ dấu câu Hán (mặc định exam).
    """
    if not isinstance(raw_gt, str) or not raw_gt.strip():
        return "", "empty"

    cleaned = raw_gt.strip()
    modifications: list[str] = []

    cleaned, noise_mods = _strip_shared_noise(cleaned)
    modifications.extend(noise_mods)

    # Protect verse refs, then strip punct /
    # Bảo vệ số câu/chương, rồi bỏ dấu
    before = cleaned
    cleaned = _VERSE_PROTECT_RE.sub(_protect_verse, cleaned)
    if keep_han_punct:
        cleaned = _WESTERN_PUNCT_RE.sub("", cleaned)
        # Drop leftover ASCII quotes etc. already covered; keep Han punct chars /
        # Đã bỏ ASCII; giữ ký tự dấu Hán
    else:
        cleaned = _ALL_MODERN_PUNCT_RE.sub("", cleaned)
    cleaned = _restore_verse(cleaned)
    if cleaned != before:
        modifications.append(
            "punct_strip_keep_han" if keep_han_punct else "punct_strip_all_modern"
        )

    before = cleaned
    cleaned = _strip_icons_and_symbols(cleaned)
    cleaned = _LATIN_RE.sub("", cleaned)
    if cleaned != before:
        modifications.append("icons_emoji_or_latin")

    cleaned, n_rep = _HALLUC_REP_RE.subn(r"\1", cleaned)
    if n_rep > 0:
        modifications.append("hallucination_repeat_ge6")

    cleaned = "\n".join(line.strip() for line in cleaned.split("\n") if line.strip())
    mod_log = "; ".join(modifications) if modifications else "unchanged"
    return cleaned, mod_log


def clean_side_matter(raw_sm: str) -> str:
    """Clean side_matter (colophon / seals noise); keep author text & years.

    Làm sạch side_matter (lạc khoản / tạp ấn); giữ chữ ký & năm.
    """
    if not isinstance(raw_sm, str) or not raw_sm.strip():
        return ""
    t, _ = _strip_shared_noise(raw_sm.strip())
    t = _strip_icons_and_symbols(t)
    lines = [line.strip() for line in t.split("\n") if line.strip()]
    return "\n".join(lines)


def clean_b2_texts(
    ground_truth: str,
    side_matter: str,
    *,
    enabled: bool = True,
    keep_han_punct: bool = True,
) -> dict[str, Any]:
    """Clean GT + SM for B2; returns raw/cleaned/meta.

    Làm sạch GT + SM cho B2; trả raw/cleaned/meta.
    """
    gt_raw = str(ground_truth or "")
    sm_raw = str(side_matter or "")
    if not enabled:
        return {
            "ground_truth": gt_raw,
            "side_matter": sm_raw,
            "ground_truth_raw": gt_raw,
            "side_matter_raw": sm_raw,
            "gt_clean_log": "disabled",
            "changed": False,
        }
    gt, log = clean_ground_truth(gt_raw, keep_han_punct=keep_han_punct)
    sm = clean_side_matter(sm_raw)
    return {
        "ground_truth": gt,
        "side_matter": sm,
        "ground_truth_raw": gt_raw,
        "side_matter_raw": sm_raw,
        "gt_clean_log": log,
        "changed": gt != gt_raw or sm != sm_raw,
    }
