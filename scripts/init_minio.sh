#!/usr/bin/env bash
# Create MinIO buckets + seed layout prefixes for crawl/OCR artifacts
# Tạo bucket MinIO + prefix layout cho crawl/OCR
set -euo pipefail

ENDPOINT="${FEN_MINIO_ENDPOINT:-http://minio:9000}"
USER="${MINIO_ROOT_USER:-admin}"
PASS="${MINIO_ROOT_PASSWORD:-admin}"
BUCKET_RAW="${FEN_BUCKET_RAW:-final-exam-nlp-raw}"
BUCKET_AIRFLOW="${FEN_BUCKET_AIRFLOW:-airflow}"
SOURCE_PREFIX="${FEN_SOURCE_PREFIX:-facebook}"
GROUP_ID="${FEN_GROUP_ID:-322453387859386}"

echo "==> MinIO init endpoint=$ENDPOINT"
mc alias set local "$ENDPOINT" "$USER" "$PASS" --api s3v4

for bucket in "$BUCKET_AIRFLOW" "$BUCKET_RAW"; do
  if mc ls "local/${bucket}" >/dev/null 2>&1; then
    echo "  bucket exists: ${bucket}"
  else
    mc mb "local/${bucket}"
    echo "  created bucket: ${bucket}"
  fi
done

# S3/MinIO has no real folders — seed .keep markers so console shows expected tree
# MinIO không có thư mục thật — ghi .keep để UI hiện cây path chuẩn
seed_prefix() {
  local bucket="$1" key="$2"
  if mc stat "local/${bucket}/${key}" >/dev/null 2>&1; then
    return 0
  fi
  printf '' | mc pipe "local/${bucket}/${key}" >/dev/null
  echo "  seeded: ${bucket}/${key}"
}

BASE="${SOURCE_PREFIX}/${GROUP_ID}"
echo "==> seed raw layout under ${BASE}/ (group_id=${GROUP_ID})"
RAW_PREFIXES=(
  "${BASE}/crawl/discover/batches/.keep"
  "${BASE}/crawl/enrich/batches/.keep"
  "${BASE}/crawl/enrich/valid/.keep"
  "${BASE}/crawl/enrich/invalid/.keep"
  "${BASE}/crawl/download/.keep"
  "${BASE}/crawl/state/.keep"
  "${BASE}/export/.keep"
  "${BASE}/images/.keep"
  "${BASE}/ocr/queue/.keep"
  "${BASE}/ocr/batches/.keep"
  "${BASE}/ocr/details/.keep"
)
for key in "${RAW_PREFIXES[@]}"; do
  seed_prefix "$BUCKET_RAW" "$key" || true
done

echo "==> MinIO init done"
