#!/usr/bin/env bash
# Print docker compose -f args based on FEN_DAG_SOURCE / In args compose theo nguồn DAG
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE="${COMPOSE_FILE:-docker-compose.yml}"
OVERLAY="${COMPOSE_OVERLAY:-docker-compose.minio-dags.yml}"

if [[ -f "${ROOT}/.env" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT}/.env"
fi

SOURCE="${FEN_DAG_SOURCE:-minio}"
echo "-f ${ROOT}/${BASE}"
if [[ "$SOURCE" != "local" ]]; then
  echo "-f ${ROOT}/${OVERLAY}"
fi
