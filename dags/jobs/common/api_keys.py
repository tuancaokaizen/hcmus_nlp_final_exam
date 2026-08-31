"""Round-robin Ramclouds keys so one pod can use the 12-key pool.

Xoay vòng key Ramclouds để một pod dùng được pool 12 key.
"""
from __future__ import annotations

import os
import threading

from common.config import get_value, load_config

_LOCK = threading.Lock()
_INDEX = 0
_POOL: list[str] | None = None


def reset_api_key_pool() -> None:
    """Clear cached pool (tests) / Xóa cache pool (test)."""
    global _INDEX, _POOL
    with _LOCK:
        _INDEX = 0
        _POOL = None


def collect_api_keys() -> list[str]:
    """Ordered unique keys from pool env, stage env, then config sections.

    Key duy nhất: pool env → stage env → section config.
    """
    global _POOL
    if _POOL is not None:
        return _POOL
    keys: list[str] = []
    seen: set[str] = set()

    def _add(raw: str) -> None:
        raw = (raw or "").strip()
        if raw and raw not in seen:
            seen.add(raw)
            keys.append(raw)

    for i in range(1, 13):
        _add(os.environ.get(f"FEN_API_KEY_{i:02d}", ""))
    # Exam stage keys from .env / Key theo stage exam từ .env
    for env_name in (
        "FEN_ALIGN_API_KEY",
        "FEN_GEMINI_OPENCV_API_KEY",
        "FEN_LABEL_GEMINI_API_KEY",
        "FEN_LABEL_GPT_API_KEY",
        "FEN_LABEL_GLM_API_KEY",
        "FEN_LABEL_DEEPSEEK_API_KEY",
        "FEN_OCR_API_KEY",
        "FEN_CALLIGRAPHY_API_KEY",
    ):
        _add(os.environ.get(env_name, ""))
    cfg = None
    try:
        cfg = load_config()
    except Exception:
        cfg = None
    if cfg:
        for section in (
            "align",
            "gemini_opencv",
            "fen_label_gemini",
            "fen_label_gpt",
            "fen_label_glm",
            "fen_label_deepseek",
            "fen_ocr",
            "fen_calligraphy",
        ):
            _add(get_value(cfg, section, "api_key", fallback=""))
    _POOL = keys
    return keys


def next_api_key() -> str:
    """Next key in the pool; raises if none configured.

    Key kế trong pool; không có thì raise.
    """
    global _INDEX
    pool = collect_api_keys()
    if not pool:
        raise ValueError("Missing align/gemini api_key for chat completion")
    with _LOCK:
        key = pool[_INDEX % len(pool)]
        _INDEX += 1
        return key
