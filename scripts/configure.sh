#!/usr/bin/env bash
# Interactive .env setup / Wizard cấu hình .env
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${ROOT}/.env"
EXAMPLE="${ROOT}/.env.example"

if [[ ! -f "$ENV_FILE" ]]; then
  cp "$EXAMPLE" "$ENV_FILE"
  echo "Created $ENV_FILE from example"
fi

read -r -p "Facebook email/phone [skip]: " fb_user || true
read -r -s -p "Facebook password [skip]: " fb_pass || true
echo
read -r -p "Ramcloud base URL [https://ramclouds.me/v1]: " base_url || true
base_url="${base_url:-https://ramclouds.me/v1}"

read -r -s -p "FEN_CALLIGRAPHY_API_KEY (crawl gate): " k_call || true
echo
read -r -s -p "FEN_OCR_API_KEY (OCR text): " k_ocr || true
echo

upsert() {
  local key="$1" val="$2"
  if grep -q "^${key}=" "$ENV_FILE"; then
    # macOS sed
    sed -i.bak "s|^${key}=.*|${key}=${val}|" "$ENV_FILE" && rm -f "${ENV_FILE}.bak"
  else
    echo "${key}=${val}" >> "$ENV_FILE"
  fi
}

[[ -n "${fb_user:-}" ]] && upsert FB_USERNAME "$fb_user"
[[ -n "${fb_pass:-}" ]] && upsert FB_PASSWORD "$fb_pass"
upsert RAMCLOUDS_BASE_URL "$base_url"
[[ -n "${k_call:-}" ]] && upsert FEN_CALLIGRAPHY_API_KEY "$k_call"
[[ -n "${k_ocr:-}" ]] && upsert FEN_OCR_API_KEY "$k_ocr"
upsert FEN_HOST_PROJECT_DIR "$ROOT"

bash "${ROOT}/scripts/generate_config.sh"
echo "Configure done. Next: make up && make deploy"
