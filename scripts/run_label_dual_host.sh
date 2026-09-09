#!/usr/bin/env bash
# Run fen_label_dual on the Mac host (uses host network → ramclouds works).
# Chạy fen_label_dual trên Mac (mạng host → gọi được ramclouds).
# Requires: MinIO+Paddle still up via compose (ports published).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
set -a
# shellcheck disable=SC1091
source .env
set +a

LIMIT="${1:-10}"
export PYTHONPATH="${ROOT}/dags/jobs${PYTHONPATH:+:$PYTHONPATH}"
export FEN_CONFIG_PATH="${ROOT}/dags/config.ini"
export FEN_JOB=fen_label_dual
export FEN_GROUP_ID="${FEN_GROUP_ID:-322453387859386}"
export FEN_LABEL_LIMIT="${LIMIT}"
export FEN_LABEL_FLUSH_POSTS="${FEN_LABEL_FLUSH_POSTS:-5}"
export FEN_LABEL_FORCE="${FEN_LABEL_FORCE:-true}"
export FEN_LABEL_GLM="${FEN_LABEL_GLM:-true}"
export FEN_LABEL_PREPARE_QUEUES="${FEN_LABEL_PREPARE_QUEUES:-true}"
export FEN_LABEL_BATCH_SEQ="${FEN_LABEL_BATCH_SEQ:-0}"
export FEN_LABEL_WORKERS="${FEN_LABEL_WORKERS:-2}"
# Host reaches published ports / Host gọi cổng publish
export FEN_MINIO_ENDPOINT="${FEN_MINIO_ENDPOINT_HOST:-http://127.0.0.1:9000}"
export FEN_PADDLE_OCR_URL="${FEN_PADDLE_OCR_URL_HOST:-http://127.0.0.1:8088/ocr}"
export PADDLE_SERVICE_URL="${PADDLE_SERVICE_URL_HOST:-http://127.0.0.1:8088}"
export FEN_SKIP_LOCAL_OUTPUT="${FEN_SKIP_LOCAL_OUTPUT:-false}"
export FEN_PATHS_OUTPUT_DIR="${ROOT}/output"
export FEN_RUNTIME=host

echo "Host label_dual limit=${LIMIT} minio=${FEN_MINIO_ENDPOINT} paddle=${FEN_PADDLE_OCR_URL}"
# Smoke ramclouds before job / Smoke ramclouds trước khi chạy job
curl -sS -m 20 -o /dev/null -w "ramclouds_http=%{http_code}\n" \
  -H "Authorization: Bearer ${FEN_LABEL_GEMINI_API_KEY}" \
  -H "Content-Type: application/json" \
  -d "{\"model\":\"${FEN_LABEL_GEMINI_MODEL:-gemini-3.6-flash-high}\",\"messages\":[{\"role\":\"user\",\"content\":\"PONG\"}],\"max_tokens\":8}" \
  https://ramclouds.me/v1/chat/completions

python3 "${ROOT}/dags/jobs/run_job.py"
