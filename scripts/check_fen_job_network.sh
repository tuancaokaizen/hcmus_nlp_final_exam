#!/usr/bin/env bash
# Check fen-job / compose network reachability to ramclouds.
# Kiểm tra mạng container tới ramclouds.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
set -a
# shellcheck disable=SC1091
source .env
set +a

KEY="${FEN_LABEL_GEMINI_API_KEY:?missing FEN_LABEL_GEMINI_API_KEY}"
MODEL="${FEN_LABEL_GEMINI_MODEL:-gemini-3.6-flash-high}"
COMPOSE=(docker compose -f docker-compose.yml)
if [[ -f docker-compose.minio-dags.yml ]]; then
  COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.minio-dags.yml)
fi

echo "== host DNS =="
ping -c1 -W2000 ramclouds.me 2>&1 | head -3 || true
echo

echo "== docker ps (fen) =="
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Networks}}' | grep -E 'NAME|fen|airflow|paddle|minio|selenium' || docker ps --format 'table {{.Names}}\t{{.Status}}'
echo

pick_container() {
  docker ps --format '{{.Names}}' | grep -E "$1" | head -1 || true
}

AIRFLOW="$(pick_container 'airflow-scheduler|airflow-worker|airflow-webserver')"
PADDLE="$(pick_container 'paddle')"
echo "airflow_like=${AIRFLOW:-none} paddle=${PADDLE:-none}"

probe_in() {
  local name="$1"
  echo "== probe inside: ${name} =="
  docker exec "$name" sh -c 'command -v getent >/dev/null && getent hosts ramclouds.me || nslookup ramclouds.me 2>/dev/null || ping -c1 -W2 ramclouds.me' 2>&1 | head -10 || true
  if docker exec "$name" sh -c 'command -v curl >/dev/null' 2>/dev/null; then
    docker exec -e KEY="$KEY" -e MODEL="$MODEL" "$name" sh -c \
      'curl -sS -m 45 -w "\nHTTP=%{http_code}\n" -H "Authorization: Bearer $KEY" -H "Content-Type: application/json" -d "{\"model\":\"$MODEL\",\"messages\":[{\"role\":\"user\",\"content\":\"Reply with exactly one word: PONG\"}],\"max_tokens\":16}" https://ramclouds.me/v1/chat/completions' \
      2>&1 | tail -c 900
  else
    echo "(no curl in image — skip HTTP)"
  fi
  echo
}

if [[ -n "${AIRFLOW}" ]]; then
  probe_in "$AIRFLOW"
fi
if [[ -n "${PADDLE}" ]]; then
  probe_in "$PADDLE"
fi

echo "== fen-job one-shot (compose network) =="
# Prefer image with curl; fall back to python urllib in fen-job
"${COMPOSE[@]}" --profile job run --rm --no-deps --entrypoint "" fen-job \
  python - <<'PY' 2>&1 | tee /tmp/fen_job_ramclouds_probe.txt | tail -c 2000
import json, os, socket, urllib.request
host = "ramclouds.me"
print("cwd", os.getcwd())
try:
    print("DNS", host, "->", socket.getaddrinfo(host, 443)[0][4][0])
except Exception as e:
    print("DNS FAIL", type(e).__name__, e)
key = os.environ.get("FEN_LABEL_GEMINI_API_KEY") or os.environ.get("FEN_OCR_API_KEY") or ""
model = os.environ.get("FEN_LABEL_GEMINI_MODEL") or "gemini-3.6-flash-high"
print("key_len", len(key), "model", model)
payload = json.dumps({
    "model": model,
    "messages": [{"role": "user", "content": "Reply with exactly one word: PONG"}],
    "max_tokens": 16,
}).encode()
req = urllib.request.Request(
    "https://ramclouds.me/v1/chat/completions",
    data=payload,
    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
    method="POST",
)
try:
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = resp.read().decode()
        print("HTTP", resp.status)
        print(body[:500])
except Exception as e:
    print("HTTP FAIL", type(e).__name__, e)
PY

echo
echo "== fen-job one-shot (host network) =="
"${COMPOSE[@]}" --profile job run --rm --no-deps --network host --entrypoint "" fen-job \
  python - <<'PY' 2>&1 | tee /tmp/fen_job_ramclouds_hostnet.txt | tail -c 2000
import json, os, socket, urllib.request
host = "ramclouds.me"
try:
    print("DNS", host, "->", socket.getaddrinfo(host, 443)[0][4][0])
except Exception as e:
    print("DNS FAIL", type(e).__name__, e)
key = os.environ.get("FEN_LABEL_GEMINI_API_KEY") or ""
model = os.environ.get("FEN_LABEL_GEMINI_MODEL") or "gemini-3.6-flash-high"
payload = json.dumps({
    "model": model,
    "messages": [{"role": "user", "content": "Reply with exactly one word: PONG"}],
    "max_tokens": 16,
}).encode()
req = urllib.request.Request(
    "https://ramclouds.me/v1/chat/completions",
    data=payload,
    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
    method="POST",
)
try:
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = resp.read().decode()
        print("HTTP", resp.status)
        print(body[:500])
except Exception as e:
    print("HTTP FAIL", type(e).__name__, e)
PY

echo
echo "Done. Raw logs: /tmp/fen_job_ramclouds_probe.txt /tmp/fen_job_ramclouds_hostnet.txt"
