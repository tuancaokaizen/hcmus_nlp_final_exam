#!/bin/bash
set -euo pipefail

# Persistent dirs on named volume (survive container restarts) / Thư mục trên named volume
DATA_ROOT="${FEN_PADDLE_DATA_ROOT:-/var/lib/fen-paddle}"
VENV_DIR="${DATA_ROOT}/venv"
MARKER="${VENV_DIR}/.install-complete"

mkdir -p "${VENV_DIR}" "${DATA_ROOT}/paddlex"
# PaddleX defaults to ~/.paddlex; link to volume subdir / PaddleX mặc định ~/.paddlex; symlink sang volume
mkdir -p /root
ln -sfn "${DATA_ROOT}/paddlex" /root/.paddlex
export PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK="${PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK:-True}"

# Reuse venv on volume to avoid multi-GB pip install every restart / Tái dùng venv trên volume
if [ ! -f "${MARKER}" ]; then
  echo "First-time setup: installing system + Python packages into volume venv..."
  apt-get update
  apt-get install -y --no-install-recommends libgl1 libglib2.0-0 libgomp1 python3-venv
  rm -rf /var/lib/apt/lists/*

  python3 -m venv "${VENV_DIR}"
  # shellcheck disable=SC1091
  source "${VENV_DIR}/bin/activate"

  echo "Installing PaddlePaddle (${PADDLE_DEVICE:-cpu})..."
  if [ "${PADDLE_DEVICE:-cpu}" = "gpu" ]; then
    PADDLE_GPU_INDEX="${PADDLE_GPU_INDEX:-https://www.paddlepaddle.org.cn/packages/stable/cu126/}"
    pip install --no-cache-dir "paddlepaddle-gpu==3.2.2" -i "${PADDLE_GPU_INDEX}"
  else
    pip install --no-cache-dir "paddlepaddle==3.2.2"
  fi

  echo "Installing Python packages (may take several minutes)..."
  pip install --no-cache-dir -r /app/requirements.txt
  touch "${MARKER}"
  echo "Volume venv install complete."
else
  echo "Reusing volume venv (skip pip install)."
  # shellcheck disable=SC1091
  source "${VENV_DIR}/bin/activate"
  apt-get update
  apt-get install -y --no-install-recommends libgl1 libglib2.0-0 libgomp1
  rm -rf /var/lib/apt/lists/*
fi

# GPU memory cap for dual-pass on 6GB cards / Giới hạn VRAM khi chạy dual-pass trên GPU 6GB
if [ "${PADDLE_DEVICE:-cpu}" = "gpu" ]; then
  export FLAGS_fraction_of_gpu_memory_to_use="${FLAGS_fraction_of_gpu_memory_to_use:-0.88}"
  export FLAGS_allocator_strategy="${FLAGS_allocator_strategy:-auto_growth}"
fi

echo "Starting PaddleOCR service on :${PADDLE_OCR_PORT:-8080}"
cd /app
exec uvicorn app:app --host 0.0.0.0 --port "${PADDLE_OCR_PORT:-8080}"
