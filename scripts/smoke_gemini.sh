#!/usr/bin/env bash
# Smoke-test Gemini via ramclouds (text + vision).
# Chạy thử Gemini qua ramclouds (text + vision).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
set -a
# shellcheck disable=SC1091
source .env
set +a

KEY="${FEN_LABEL_GEMINI_API_KEY:?missing FEN_LABEL_GEMINI_API_KEY}"
MODEL="${FEN_LABEL_GEMINI_MODEL:-gemini-3.6-flash-high}"
BASE="https://ramclouds.me/v1"
OUT_DIR="${TMPDIR:-/tmp}/fen_gemini_smoke"
mkdir -p "$OUT_DIR"

echo "== DNS =="
ping -c1 -W2000 ramclouds.me 2>&1 | head -3 || true
echo

echo "== TEXT model=${MODEL} =="
curl -sS -m 90 -w "\nHTTP=%{http_code}\n" \
  -H "Authorization: Bearer ${KEY}" \
  -H "Content-Type: application/json" \
  -d "{\"model\":\"${MODEL}\",\"messages\":[{\"role\":\"user\",\"content\":\"Reply with exactly one word: PONG\"}],\"max_tokens\":16}" \
  "${BASE}/chat/completions" | tee "${OUT_DIR}/text.json"
echo

echo "== TEXT model=gemini-3.5-flash-low =="
curl -sS -m 90 -w "\nHTTP=%{http_code}\n" \
  -H "Authorization: Bearer ${KEY}" \
  -H "Content-Type: application/json" \
  -d '{"model":"gemini-3.5-flash-low","messages":[{"role":"user","content":"Reply with exactly one word: PONG"}],"max_tokens":16}' \
  "${BASE}/chat/completions" | tee "${OUT_DIR}/text_low.json"
echo

IMG="${ROOT}/report/examples/ex4_jin_se.jpg"
PNG="${OUT_DIR}/ex4.png"
sips -s format png -Z 768 "$IMG" --out "$PNG" >/dev/null
B64_FILE="${OUT_DIR}/ex4.b64"
base64 < "$PNG" | tr -d '\n' > "$B64_FILE"

python3 - "$OUT_DIR" "$MODEL" "$B64_FILE" <<'PY'
import json, sys
from pathlib import Path
out_dir, model, b64_path = sys.argv[1], sys.argv[2], sys.argv[3]
b64 = Path(b64_path).read_text()
payload = {
    "model": model,
    "messages": [{
        "role": "user",
        "content": [
            {
                "type": "text",
                "text": (
                    "OCR Chinese calligraphy. JSON only: "
                    '{"boxes":[{"text":"...","kind":"ink_text","bounding_box":[0,0,1,1]}],'
                    '"confidence":0.9}'
                ),
            },
            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}},
        ],
    }],
    "max_tokens": 1024,
    "temperature": 0.1,
}
Path(out_dir, "vision_req.json").write_text(json.dumps(payload))
print("vision payload ready, png_b64_chars=", len(b64))
PY

echo "== VISION model=${MODEL} image=ex4_jin_se =="
curl -sS -m 120 -w "\nHTTP=%{http_code}\n" \
  -H "Authorization: Bearer ${KEY}" \
  -H "Content-Type: application/json" \
  -d @"${OUT_DIR}/vision_req.json" \
  "${BASE}/chat/completions" | tee "${OUT_DIR}/vision.json"
echo
echo "Saved under ${OUT_DIR}"
