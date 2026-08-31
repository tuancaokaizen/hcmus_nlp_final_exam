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
    """Ordered unique keys from FEN_API_KEY_01..12 then config env.

    Key duy nhất theo thứ tự FEN_API_KEY_01..12 rồi env config.
    """
    global _POOL
    if _POOL is not None:
        return _POOL
    keys: list[str] = []
    seen: set[str] = set()
    for i in range(1, 13):
        raw = os.environ.get(f"FEN_API_KEY_{i:02d}", "").strip()
        if raw and raw not in seen:
            seen.add(raw)
            keys.append(raw)
    cfg = None
    for env_name, section, option in (
        ("FEN_ALIGN_API_KEY", "align", "api_key"),
        ("FEN_GEMINI_OPENCV_API_KEY", "gemini_opencv", "api_key"),
    ):
        raw = os.environ.get(env_name, "").strip()
        if not raw:
            if cfg is None:
                try:
                    cfg = load_config()
                except Exception:
                    cfg = False
            if cfg:
                raw = get_value(cfg, section, option, fallback="").strip()
        if raw and raw not in seen:
            seen.add(raw)
            keys.append(raw)
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
