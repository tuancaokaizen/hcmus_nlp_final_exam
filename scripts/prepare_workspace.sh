#!/usr/bin/env bash
# Ensure dirs, config, and required DAG/job artifacts exist before compose up
# Đảm bảo thư mục, config và file DAG/job cần thiết trước khi compose up
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

mkdir -p \
  dags/pipelines \
  dags/jobs/common \
  docker/paddle-ocr \
  .mc \
  scripts

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from .env.example — run: make configure (API keys, FB creds)"
fi

# shellcheck disable=SC1091
source .env

if [[ -z "${FEN_HOST_PROJECT_DIR:-}" ]]; then
  if grep -q '^FEN_HOST_PROJECT_DIR=' .env; then
    sed -i.bak "s|^FEN_HOST_PROJECT_DIR=.*|FEN_HOST_PROJECT_DIR=$ROOT|" .env && rm -f .env.bak
  else
    echo "FEN_HOST_PROJECT_DIR=$ROOT" >> .env
  fi
  # shellcheck disable=SC1091
  source .env
fi

echo "==> generate dags/config.ini from .env"
bash scripts/generate_config.sh

REQUIRED=(
  dags/jobs/run_job.py
  dags/jobs/fen_crawl_common.py
  dags/jobs/fen_crawl_discover.py
  dags/jobs/fen_crawl_enrich.py
  dags/jobs/fen_crawl_download.py
  dags/jobs/common/docker_executor.py
  dags/pipelines/fen_crawl_pipeline.py
  dags/pipelines/fen_label_dual_pipeline.py
  dags/pipelines/fen_e2e_pipeline.py
  dags/jobs/final_exam_nlp_ocr_label_dual.py
)

missing=0
for f in "${REQUIRED[@]}"; do
  if [[ ! -f "$f" ]]; then
    echo "  missing: $f"
    missing=1
  fi
done

if [[ "$missing" -eq 1 ]]; then
  echo "==> required job/DAG files missing — trying sync_from_upstream"
  if bash scripts/sync_from_upstream.sh; then
    echo "==> sync_from_upstream OK"
  else
    echo "ERROR: clone repo with committed dags/ or set UPSTREAM_ROOT and run: make sync"
    exit 1
  fi
  for f in "${REQUIRED[@]}"; do
    if [[ ! -f "$f" ]]; then
      echo "ERROR: still missing after sync: $f"
      exit 1
    fi
  done
fi

echo "==> workspace ready (dags, jobs, pipelines, config.ini)"
