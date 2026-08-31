#!/usr/bin/env bash
# Full bootstrap = prepare workspace + force rebuild + up (alias for first-time setup)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export FEN_UP_BUILD=always
bash scripts/up.sh

echo ""
echo "Bootstrap complete — same as: FEN_UP_BUILD=always make up"
