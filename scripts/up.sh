#!/usr/bin/env bash
# One-shot up: prepare workspace → build (if needed) → compose → buckets → deploy DAGs → Airflow
# Khởi động một lần: chuẩn bị workspace → build → compose → bucket → deploy DAG → Airflow
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

bash scripts/prepare_workspace.sh

# shellcheck disable=SC1091
source .env

COMPOSE=(docker compose -f "$ROOT/docker-compose.yml")
if [[ "${FEN_DAG_SOURCE:-minio}" != "local" ]]; then
  COMPOSE+=(-f "$ROOT/docker-compose.minio-dags.yml")
fi

DAG_SOURCE="${FEN_DAG_SOURCE:-minio}"
UP_BUILD="${FEN_UP_BUILD:-}"

_images_missing() {
  if ! docker image inspect fen-exam-fen-job >/dev/null 2>&1; then
    return 0
  fi
  local img
  img="$("${COMPOSE[@]}" images -q airflow-webserver 2>/dev/null | head -1 || true)"
  if [[ -z "$img" ]] || ! docker image inspect "$img" >/dev/null 2>&1; then
    return 0
  fi
  return 1
}

if [[ "$UP_BUILD" == "always" ]] || _images_missing; then
  echo "==> docker compose build (images missing or FEN_UP_BUILD=always)"
  "${COMPOSE[@]}" build
  "${COMPOSE[@]}" --profile job build fen-job
else
  echo "==> docker images present — skip build (set FEN_UP_BUILD=always to rebuild)"
fi

echo "==> docker compose up -d"
"${COMPOSE[@]}" up -d

echo "==> minio-init (buckets)"
sleep 3
"${COMPOSE[@]}" run --rm minio-init

if [[ "$DAG_SOURCE" != "local" ]]; then
  if command -v mc >/dev/null 2>&1; then
    echo "==> deploy DAGs + config to MinIO"
    bash scripts/deploy_airflow.sh
  else
    echo "WARN: 'mc' not on PATH — install MinIO client or use FEN_DAG_SOURCE=local"
    echo "      Skipping deploy; Airflow may have no DAGs until: make deploy"
  fi
  echo "==> airflow-dag-sync (FEN_DAG_SOURCE=minio)"
  "${COMPOSE[@]}" up -d airflow-dag-sync
fi

echo "==> airflow-init + webserver + scheduler"
"${COMPOSE[@]}" up -d airflow-init
"${COMPOSE[@]}" up -d airflow-webserver airflow-scheduler

echo ""
echo "Stack is up:"
echo "  Airflow UI: http://localhost:8080 (admin / admin)"
echo "  MinIO UI:   http://localhost:9001 (admin / admin)"
echo "  DAG source: ${DAG_SOURCE}"
if [[ "$DAG_SOURCE" != "local" ]]; then
  echo "  DAG edits: make deploy  (or re-run make up; sync ~${AIRFLOW_DAG_SYNC_INTERVAL_SEC:-30}s)"
else
  echo "  DAGs: bind mount ./dags"
fi
echo "  Job code edits: FEN_UP_BUILD=always make up  (rebuild fen-job image)"
echo "  First FB crawl: make fb-login"
