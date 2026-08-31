#!/usr/bin/env bash
# Mirror DAGs + config to MinIO airflow bucket (prod pattern)
# Upload DAG + config lên bucket airflow giống prod
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ ! -f .env ]]; then
  echo "Missing .env — run: cp .env.example .env && make configure"
  exit 1
fi

# shellcheck disable=SC1091
source .env

ENDPOINT="${FEN_MINIO_ENDPOINT:-http://minio:9000}"
# Host-side deploy uses localhost / Deploy từ host dùng localhost
if [[ "$ENDPOINT" == http://minio:* ]]; then
  ENDPOINT="http://127.0.0.1:${ENDPOINT##*:}"
fi

USER="${MINIO_ROOT_USER:-admin}"
PASS="${MINIO_ROOT_PASSWORD:-admin}"
BUCKET="${FEN_BUCKET_AIRFLOW:-airflow}"
PREFIX="${FEN_DAGS_PREFIX:-fen-exam}"
TARGET="local/${BUCKET}/dags/${PREFIX}"

echo "==> deploy Airflow DAGs to ${TARGET}"
mkdir -p "$ROOT/.mc"
export MC_CONFIG_DIR="$ROOT/.mc"

mc alias set local "$ENDPOINT" "$USER" "$PASS" --api s3v4
mc mb "local/${BUCKET}" 2>/dev/null || true

if [[ ! -f dags/config.ini ]]; then
  echo "Missing dags/config.ini — run: make configure"
  exit 1
fi

mc mirror --overwrite --remove \
  --exclude "*.sh" \
  --exclude "__pycache__/*" \
  --exclude "*.pyc" \
  --exclude "config.ini.example" \
  --exclude "pipelines/__init__.py" \
  "$ROOT/dags" "$TARGET"

mc cp "$ROOT/dags/config.ini" "${TARGET}/config.ini"
echo "==> deploy done: ${ENDPOINT}/${BUCKET}/dags/${PREFIX}/"
