#!/bin/sh
# Mirror Airflow DAGs from MinIO bucket into shared volume (prod-like).
# Mirror DAG Airflow từ bucket MinIO vào volume dùng chung (giống prod).
set -eu

ENDPOINT="${FEN_MINIO_ENDPOINT:-http://minio:9000}"
USER="${MINIO_ROOT_USER:-admin}"
PASS="${MINIO_ROOT_PASSWORD:-admin}"
BUCKET="${FEN_BUCKET_AIRFLOW:-airflow}"
PREFIX="${FEN_DAGS_PREFIX:-fen-exam}"
INTERVAL="${AIRFLOW_DAG_SYNC_INTERVAL_SEC:-30}"
TARGET="local/${BUCKET}/dags/${PREFIX}"

echo "==> airflow-dag-sync endpoint=${ENDPOINT} source=${TARGET} -> /dags interval=${INTERVAL}s"

mc alias set local "$ENDPOINT" "$USER" "$PASS" --api s3v4

_sync_once() {
  # Best-effort mirror; bucket may be empty before first make deploy
  # Mirror best-effort; bucket có thể rỗng trước lần deploy đầu
  if mc mirror --overwrite --remove "$TARGET" /dags/; then
    echo "==> sync ok $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  else
    echo "==> sync skipped or empty source $(date -u +%Y-%m-%dT%H:%M:%SZ)" >&2
  fi
}

_sync_once
while true; do
  sleep "$INTERVAL"
  _sync_once
done
