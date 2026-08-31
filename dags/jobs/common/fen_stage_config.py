"""Per-stage API key helpers for exam Docker (separate Ramcloud keys).
Helper API key theo stage — key Ramcloud tách riêng cho exam Docker.
"""
from __future__ import annotations

from common.config import get_value, load_config


def stage_api_key(section: str, *, fallback_section: str = "gemini_opencv") -> str:
    """Read api_key from [section], else fallback / Đọc api_key section, fallback nếu rỗng."""
    cfg = load_config()
    key = get_value(cfg, section, "api_key", fallback="").strip()
    if key:
        return key
    return get_value(cfg, fallback_section, "api_key", fallback="").strip()


def stage_base_url(section: str, *, fallback_section: str = "gemini_opencv") -> str:
    cfg = load_config()
    url = get_value(cfg, section, "base_url", fallback="").strip()
    if url:
        return url
    return get_value(
        cfg, fallback_section, "base_url", fallback="https://ramclouds.me/v1"
    ).strip()


def stage_model(section: str, *, fallback: str) -> str:
    cfg = load_config()
    return get_value(cfg, section, "model", fallback=fallback).strip() or fallback
