#!/usr/bin/env bash
# Verify E2E pipeline artifacts on MinIO / Kiểm tra artifact pipeline E2E trên MinIO
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ ! -f .env ]]; then
  echo "FAIL: missing .env"
  exit 1
fi
# shellcheck disable=SC1091
source .env

ENDPOINT="${FEN_MINIO_ENDPOINT:-http://minio:9000}"
if [[ "$ENDPOINT" == http://minio:* ]]; then
  ENDPOINT="http://127.0.0.1:${ENDPOINT##*:}"
fi
USER="${MINIO_ROOT_USER:-admin}"
PASS="${MINIO_ROOT_PASSWORD:-admin}"
BUCKET="${FEN_BUCKET_RAW:-final-exam-nlp-raw}"
GID="${FEN_GROUP_ID:-322453387859386}"
PREFIX="facebook/${GID}"
LD="${PREFIX}/ocr/label_dual_pilot"

export MC_CONFIG_DIR="${ROOT}/.mc"
mc alias set local "$ENDPOINT" "$USER" "$PASS" --api s3v4

fail=0
check() {
  local path="$1" msg="$2"
  if mc stat "local/${BUCKET}/${path}" >/dev/null 2>&1; then
    echo "OK  $msg"
  else
    echo "FAIL $msg ($path)"
    fail=1
  fi
}

echo "==> crawl (B1)"
check "${PREFIX}/crawl/checkpoint.json" "checkpoint"
check "${PREFIX}/crawl/discover/seen_post_ids.json" "seen_post_ids"
check "${PREFIX}/export/valid_post.jsonl" "valid_post.jsonl"

echo "==> label dual (B2) — optional until OCR runs"
ld_fail=0
for path in \
  "${LD}/task_b2.jsonl" \
  "${LD}/summary.json" \
  "${LD}/glm/recommend.jsonl"
do
  if mc stat "local/${BUCKET}/${path}" >/dev/null 2>&1; then
    echo "OK  $(basename "$path")"
  else
    echo "SKIP $(basename "$path") (label dual not run yet)"
    ld_fail=1
  fi
done
if [[ "$ld_fail" -eq 0 ]]; then
  if mc stat "local/${BUCKET}/${LD}/task_b2.xlsx" >/dev/null 2>&1; then
    echo "OK  task_b2.xlsx"
  else
    echo "WARN task_b2.xlsx missing (run may still be in progress)"
  fi
fi

if [[ "$fail" -eq 0 ]]; then
  echo "verify: PASS (crawl); see label dual lines above"
else
  echo "verify: FAIL"
  exit 1
fi
