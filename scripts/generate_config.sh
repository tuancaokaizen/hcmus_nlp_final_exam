#!/usr/bin/env bash
# Generate dags/config.ini from .env (separate API keys per stage)
# Sinh config.ini từ .env — mỗi stage một API key
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${ROOT}/.env"
TEMPLATE="${ROOT}/dags/config.ini.example"
OUT="${ROOT}/dags/config.ini"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Copy .env.example to .env first"
  exit 1
fi
if [[ ! -f "$TEMPLATE" ]]; then
  echo "Missing $TEMPLATE"
  exit 1
fi

# shellcheck disable=SC1091
set -a
source "$ENV_FILE"
set +a

cp "$TEMPLATE" "$OUT"
export OUT

python3 <<'PY'
import os
import re
from pathlib import Path

out = Path(os.environ["OUT"])
text = out.read_text(encoding="utf-8")

mapping = {
    "MINIO_ENDPOINT": os.environ.get("FEN_MINIO_ENDPOINT", "http://minio:9000"),
    "MINIO_ACCESS_KEY": os.environ.get("MINIO_ROOT_USER", "admin"),
    "MINIO_SECRET_KEY": os.environ.get("MINIO_ROOT_PASSWORD", "admin"),
    "BUCKET_RAW": os.environ.get("FEN_BUCKET_RAW", "final-exam-nlp-raw"),
    "BUCKET_AIRFLOW": os.environ.get("FEN_BUCKET_AIRFLOW", "airflow"),
    "DAGS_PREFIX": os.environ.get("FEN_DAGS_PREFIX", "fen-exam"),
    "SELENIUM_URL": os.environ.get("SELENIUM_REMOTE_URL", "http://selenium-chrome:4444/wd/hub"),
    "PADDLE_URL": os.environ.get("PADDLE_SERVICE_URL", "http://paddle-ocr:8080"),
    "RAMCLOUD_BASE": os.environ.get("RAMCLOUDS_BASE_URL", "https://ramclouds.me/v1"),
    "FEN_CALLIGRAPHY_KEY": os.environ.get("FEN_CALLIGRAPHY_API_KEY", ""),
    "FEN_OCR_KEY": os.environ.get("FEN_OCR_API_KEY", ""),
    "FEN_LABEL_GEMINI_KEY": os.environ.get("FEN_LABEL_GEMINI_API_KEY", ""),
    "FEN_LABEL_GPT_KEY": os.environ.get("FEN_LABEL_GPT_API_KEY", ""),
    "FEN_LABEL_DEEPSEEK_KEY": os.environ.get("FEN_LABEL_DEEPSEEK_API_KEY", ""),
    "FEN_LABEL_GLM_KEY": os.environ.get("FEN_LABEL_GLM_API_KEY", ""),
    "FEN_CALLIGRAPHY_MODEL": os.environ.get("FEN_CALLIGRAPHY_MODEL", "gemini-3.5-flash-low"),
    "FEN_OCR_MODEL": os.environ.get("FEN_OCR_MODEL", "gemini-3.5-flash-low"),
    "HOST_PROJECT_DIR": os.environ.get("FEN_HOST_PROJECT_DIR", ""),
}

replacements = [
    (r"^endpoint = .*", f"endpoint = {mapping['MINIO_ENDPOINT']}"),
    (r"^access_key = .*", f"access_key = {mapping['MINIO_ACCESS_KEY']}"),
    (r"^secret_key = .*", f"secret_key = {mapping['MINIO_SECRET_KEY']}"),
    (r"^bucket_raw = .*", f"bucket_raw = {mapping['BUCKET_RAW']}"),
    (r"^bucket_airflow = .*", f"bucket_airflow = {mapping['BUCKET_AIRFLOW']}"),
    (r"^airflow_dags_prefix = .*", f"airflow_dags_prefix = {mapping['DAGS_PREFIX']}"),
    (r"^selenium_remote_url = .*", f"selenium_remote_url = {mapping['SELENIUM_URL']}"),
    (r"^service_url = http://paddle.*", f"service_url = {mapping['PADDLE_URL']}"),
    (r"^compose_project = .*", f"compose_project = {os.environ.get('FEN_COMPOSE_PROJECT', 'fen-exam')}"),
    (r"^project_dir = .*", f"project_dir = {mapping['HOST_PROJECT_DIR'] or '/opt/fen-exam'}"),
]

# Section-specific keys
sections = {
    "[fen_calligraphy]": [
        ("base_url", mapping["RAMCLOUD_BASE"]),
        ("api_key", mapping["FEN_CALLIGRAPHY_KEY"]),
        ("model", mapping["FEN_CALLIGRAPHY_MODEL"]),
    ],
    "[fen_ocr]": [
        ("base_url", mapping["RAMCLOUD_BASE"]),
        ("api_key", mapping["FEN_OCR_KEY"]),
        ("model", mapping["FEN_OCR_MODEL"]),
    ],
    "[fen_label_gemini]": [
        ("base_url", mapping["RAMCLOUD_BASE"]),
        ("api_key", mapping["FEN_LABEL_GEMINI_KEY"]),
        ("model", os.environ.get("FEN_LABEL_GEMINI_MODEL", "gemini-3.6-flash-high")),
    ],
    "[fen_label_gpt]": [
        ("base_url", mapping["RAMCLOUD_BASE"]),
        ("api_key", mapping["FEN_LABEL_GPT_KEY"]),
        ("model", os.environ.get("FEN_LABEL_GPT_MODEL", "gpt-5.6-luna")),
    ],
    "[fen_label_deepseek]": [
        ("base_url", mapping["RAMCLOUD_BASE"]),
        ("api_key", mapping["FEN_LABEL_DEEPSEEK_KEY"]),
        ("model", os.environ.get("FEN_LABEL_DEEPSEEK_MODEL", "deepseek-v4-flash")),
    ],
    "[fen_label_glm]": [
        ("base_url", mapping["RAMCLOUD_BASE"]),
        ("api_key", mapping["FEN_LABEL_GLM_KEY"]),
        ("model", os.environ.get("FEN_LABEL_GLM_MODEL", "glm-5.3-flash")),
    ],
}

lines = text.splitlines()
out_lines = []
current = None
for line in lines:
    if line.startswith("[") and line.endswith("]"):
        current = line
        out_lines.append(line)
        continue
    if "=" in line and not line.strip().startswith("#"):
        key = line.split("=", 1)[0].strip()
        if current in sections:
            for sk, sv in sections[current]:
                if key == sk:
                    line = f"{key} = {sv}"
                    break
        else:
            for pat, repl in replacements:
                if re.match(pat, line):
                    line = repl
                    break
    out_lines.append(line)

out.write_text("\n".join(out_lines) + "\n", encoding="utf-8")
print(f"Wrote {out}")
PY
