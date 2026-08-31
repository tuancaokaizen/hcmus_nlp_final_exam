from __future__ import annotations

import configparser
import os
from pathlib import Path
from typing import Any


def _default_config_path() -> Path:
    # Resolve config path from env or dags folder / Xác định config từ env hoặc thư mục dags
    env_path = os.environ.get("FEN_CONFIG_PATH")
    if env_path:
        return Path(env_path)
    return Path(__file__).resolve().parents[2] / "config.ini"


def _get_env_key(section: str, option: str) -> str:
    # Build predictable env var names / Tạo tên biến môi trường nhất quán
    return f"FEN_{section}_{option}".upper()


def get_output_dir() -> Path:
    # Resolve writable output directory for local artifacts / Xác định thư mục output ghi được
    if os.environ.get("FEN_SKIP_LOCAL_OUTPUT", "").strip().lower() in {"1", "true", "yes", "on"}:
        return Path("/tmp/fen-output")

    cfg = load_config()
    raw = get_value(cfg, "paths", "output_dir", fallback="data/output")
    path = Path(raw)
    if path.is_absolute():
        resolved = path
    elif os.environ.get("FEN_CONFIG_PATH", "").startswith("/workspace"):
        # Job container may load config synced from MinIO / Container job có thể đọc config sync từ MinIO
        resolved = Path("/tmp/fen-output")
    elif os.environ.get("FEN_JOB"):
        resolved = Path("/tmp/fen-output")
    else:
        dags_root = Path(__file__).resolve().parents[2]
        resolved = (dags_root / raw).resolve()
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def load_config(config_path: str | None = None) -> configparser.ConfigParser:
    # Resolve config path with fallback / Xác định đường dẫn config kèm fallback
    resolved = Path(config_path) if config_path else _default_config_path()
    if not resolved.exists():
        raise FileNotFoundError(
            f"Missing config file: {resolved}. Copy dags/config.ini.example to dags/config.ini."
        )

    parser = configparser.ConfigParser()
    parser.read(resolved)
    return parser


def get_value(
    cfg: configparser.ConfigParser, section: str, option: str, fallback: Any | None = None
) -> str:
    # Environment overrides file config / Biến môi trường sẽ ghi đè file config
    env_key = _get_env_key(section, option)
    if env_key in os.environ:
        return os.environ[env_key]
    return cfg.get(section, option, fallback=fallback)
