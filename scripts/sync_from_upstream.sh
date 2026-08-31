#!/usr/bin/env bash
# Sync job code from upstream NLP processing repo and apply exam renames (crawl/, fen_crawl_*)
# Đồng bộ job từ repo upstream và đổi tên cho exam (crawl/, fen_crawl_*)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
UPSTREAM="${UPSTREAM_ROOT:-$(cd "$ROOT/../implement_ocr_pipeline/hvb-processing" 2>/dev/null && pwd || true)}"

if [[ -z "$UPSTREAM" || ! -d "$UPSTREAM/dags/jobs" ]]; then
  UPSTREAM="$(cd "$ROOT/../../implement_ocr_pipeline/hvb-processing" 2>/dev/null && pwd || true)"
fi
if [[ ! -d "${UPSTREAM:-}/dags/jobs" ]]; then
  echo "Upstream repo not found. Set UPSTREAM_ROOT=/path/to/processing-repo"
  exit 1
fi

echo "==> sync from $UPSTREAM"

JOBS="$ROOT/dags/jobs"
COMMON="$JOBS/common"
PIPE="$ROOT/dags/pipelines"
mkdir -p "$COMMON" "$PIPE" "$ROOT/docker/paddle-ocr"

apply_fen_prefix() {
  local f="$1"
  sed -i.bak \
    -e 's/HVB_/FEN_/g' \
    -e 's/hvb_base/fen_base/g' \
    -e 's/hvb-upload-/fen-upload-/g' \
    -e 's/hvb-output/fen-output/g' \
    -e 's/hvb-preprocessed/fen-preprocessed/g' \
    -e 's/hvb-ocr/fen-ocr/g' \
    -e 's/hvb-aligned/fen-aligned/g' \
    -e 's/hvb-paddle/fen-paddle/g' \
    -e 's|/workspace/hvb-processing|/opt/fen-exam|g' \
    -e 's/HVB PaddleOCR/FEN PaddleOCR/g' \
    -e 's/as HVB OCR/as FEN OCR/g' \
    -e 's/như OCR HVB/như OCR FEN/g' \
    "$f" && rm -f "${f}.bak"
}

# Common modules
for f in config.py io_storage.py chau_ban_schema.py ocr_helpers.py api_keys.py llm_chat.py fen_dag_rollover.py; do
  cp "$UPSTREAM/dags/jobs/common/$f" "$COMMON/"
  apply_fen_prefix "$COMMON/$f"
done

# Crawl support
for f in \
  final_exam_nlp_crawl_runner.py \
  final_exam_nlp_graphql_batch.py \
  final_exam_nlp_two_phase.py \
  final_exam_nlp_calligraphy_classify.py \
  final_exam_nlp_ocr.py \
  final_exam_nlp_ocr_retry.py \
  final_exam_nlp_ocr_eval.py \
  final_exam_nlp_ocr_label_dual.py
do
  cp "$UPSTREAM/dags/jobs/$f" "$JOBS/"
  apply_fen_prefix "$JOBS/$f"
done
sed -i.bak 's/final_exam_nlp_v5_common/fen_crawl_common/g' "$JOBS/final_exam_nlp_ocr_retry.py" 2>/dev/null || true
sed -i.bak 's/final_exam_nlp_v5_common/fen_crawl_common/g' "$JOBS/final_exam_nlp_ocr_eval.py" 2>/dev/null || true
sed -i.bak 's/final_exam_nlp_v5_common/fen_crawl_common/g' "$JOBS/final_exam_nlp_ocr_label_dual.py" 2>/dev/null || true
rm -f "$JOBS/final_exam_nlp_ocr_retry.py.bak" \
      "$JOBS/final_exam_nlp_ocr_eval.py.bak" \
      "$JOBS/final_exam_nlp_ocr_label_dual.py.bak"

# Transform v5 → crawl
transform_crawl() {
  local src="$1" dst="$2"
  sed -e 's/final_exam_nlp_v5_common/fen_crawl_common/g' \
      -e 's/v5_root/crawl_root/g' \
      -e 's|/v5/|/crawl/|g' \
      -e 's|/v5"|/crawl"|g' \
      -e 's|/v5$|/crawl|g' \
      -e 's/SCHEMA_V5/SCHEMA_CRAWL/g' \
      -e 's/v5_split/split/g' \
      -e 's/\[fen_discover\]/[fen_crawl_discover]/g' \
      -e 's/\[fen_enrich\]/[fen_crawl_enrich]/g' \
      -e 's/\[fen_download\]/[fen_crawl_download]/g' \
      -e 's/V5 split/Crawl split/g' \
      "$src" > "$dst"
  apply_fen_prefix "$dst"
}

transform_crawl "$UPSTREAM/dags/jobs/final_exam_nlp_v5_common.py" "$JOBS/fen_crawl_common.py"
transform_crawl "$UPSTREAM/dags/jobs/final_exam_nlp_v5_discover.py" "$JOBS/fen_crawl_discover.py"
transform_crawl "$UPSTREAM/dags/jobs/final_exam_nlp_v5_enrich.py" "$JOBS/fen_crawl_enrich.py"
transform_crawl "$UPSTREAM/dags/jobs/final_exam_nlp_v5_download.py" "$JOBS/fen_crawl_download.py"

# run_job from run_k8s_job + aliases
cp "$UPSTREAM/dags/jobs/run_k8s_job.py" "$JOBS/run_job.py"
sed -i.bak \
  -e 's/final_exam_nlp_v5_common/fen_crawl_common/g' \
  -e 's/final_exam_nlp_v5_discover/fen_crawl_discover/g' \
  -e 's/final_exam_nlp_v5_enrich/fen_crawl_enrich/g' \
  -e 's/final_exam_nlp_v5_download/fen_crawl_download/g' \
  -e 's/run_k8s_job.py/run_job.py/g' \
  "$JOBS/run_job.py" && rm -f "$JOBS/run_job.py.bak"
apply_fen_prefix "$JOBS/run_job.py"

export JOBS="$JOBS"

# Job aliases for exam naming / Alias tên job exam
python3 <<'PY'
import os
from pathlib import Path
p = Path(os.environ["JOBS"]) / "run_job.py"
text = p.read_text(encoding="utf-8")
text = text.replace("run_k8s_job.py", "run_job.py")
aliases = '''
    if job == "fen_bootstrap_login":
        _run_final_exam_nlp_bootstrap_login()
        return
    if job == "fen_label_dual":
        _run_final_exam_nlp_ocr_label_dual()
        return
    if job == "fen_ocr":
        _run_final_exam_nlp_ocr()
        return
    if job == "fen_ocr_retry":
        _run_final_exam_nlp_ocr_retry()
        return
'''
needle = '    if job == "fen_crawl_discover":'
if needle in text and 'if job == "fen_bootstrap_login":' not in text:
    text = text.replace(needle, aliases + needle)
# Fix defaults for Docker exam / Sửa default cho Docker exam
text = text.replace(
    'os.environ.setdefault("FEN_CONFIG_PATH", "/opt/fen-exam/config.ini")',
    'os.environ.setdefault("FEN_CONFIG_PATH", "/opt/fen-exam/dags/config.ini")',
)
text = text.replace(
    '"""CLI entrypoint for FEN v2 jobs running inside KubernetesPodOperator pods.',
    '"""CLI entrypoint for FEN exam jobs running inside Docker containers.',
)
text = text.replace(
    'Điểm vào CLI cho job FEN v2 chạy trong pod KubernetesPodOperator.',
    'Điểm vào CLI cho job FEN exam chạy trong container Docker.',
)
p.write_text(text, encoding="utf-8")
print("patched run_job.py aliases")
PY

# fen_stage_config helper (exam-only, not overwritten)
cat > "$COMMON/fen_stage_config.py" <<'PYEOF'
"""Per-stage API key helpers for exam Docker (separate Ramcloud keys).
Helper API key theo stage — key Ramcloud tách riêng cho exam Docker.
"""
from __future__ import annotations

from common.config import get_value, load_config


def stage_api_key(section: str, *, fallback_section: str = "gemini_opencv") -> str:
    """Read api_key from [section], else fallback / Đọc api_key section, fallback nếu rỗng."""
    cfg = load_config()
    key = get_value(cfg, section, "api_key", fallback="").strip()
    if key:
        return key
    return get_value(cfg, fallback_section, "api_key", fallback="").strip()


def stage_base_url(section: str, *, fallback_section: str = "gemini_opencv") -> str:
    cfg = load_config()
    url = get_value(cfg, section, "base_url", fallback="").strip()
    if url:
        return url
    return get_value(
        cfg, fallback_section, "base_url", fallback="https://ramclouds.me/v1"
    ).strip()


def stage_model(section: str, *, fallback: str) -> str:
    cfg = load_config()
    return get_value(cfg, section, "model", fallback=fallback).strip() or fallback
PYEOF

# Patch calligraphy
export JOBS="$JOBS"
python3 <<'PY'
from pathlib import Path
import os
p = Path(os.environ["JOBS"]) / "final_exam_nlp_calligraphy_classify.py"
t = p.read_text(encoding="utf-8")
old = '    api_key = get_value(cfg, "gemini_opencv", "api_key", fallback="")'
new = '''    from common.fen_stage_config import stage_api_key, stage_base_url, stage_model
    api_key = stage_api_key("fen_calligraphy")
    if not api_key:
        api_key = get_value(cfg, "gemini_opencv", "api_key", fallback="")'''
if old in t and "fen_stage_config" not in t:
    t = t.replace(old, new, 1)
    t = t.replace(
        '    base_url = get_value(\n        cfg, "gemini_opencv", "base_url", fallback="https://ramclouds.me/v1"\n    ).strip()',
        '    base_url = stage_base_url("fen_calligraphy")',
        1,
    )
    t = t.replace(
        '    primary = get_value(\n        cfg, "gemini_opencv", "model", fallback="gemini-3.5-flash-low"\n    ).strip()',
        '    primary = stage_model("fen_calligraphy", fallback="gemini-3.5-flash-low")',
        1,
    )
    p.write_text(t, encoding="utf-8")
    print("patched calligraphy_classify")
PY

# Patch OCR imports
export JOBS="$JOBS"
python3 <<'PY'
from pathlib import Path
import os
p = Path(os.environ["JOBS"]) / "final_exam_nlp_ocr.py"
t = p.read_text(encoding="utf-8")
if "fen_stage_config" not in t:
    t = t.replace(
        "from final_exam_nlp_v5_common import",
        "from fen_crawl_common import",
    )
    insert = "from common.fen_stage_config import stage_api_key, stage_base_url, stage_model\n"
    if insert not in t:
        t = t.replace("from common.chau_ban_schema import", insert + "from common.chau_ban_schema import", 1)
    t = t.replace(
        '    fallback_key = get_value(cfg, "gemini_opencv", "api_key", fallback="").strip()',
        '    fallback_key = stage_api_key("fen_ocr") or get_value(cfg, "gemini_opencv", "api_key", fallback="").strip()',
    )
    t = t.replace(
        '    base_url = get_value(cfg, "gemini_opencv", "base_url", fallback="https://ramclouds.me/v1").strip()',
        '    base_url = stage_base_url("fen_ocr")',
    )
    p.write_text(t, encoding="utf-8")
    print("patched fen_ocr")
PY

# Exam stack: no Qdrant in ocr_retry / Exam không dùng Qdrant
export JOBS="$JOBS"
python3 <<'PY'
from pathlib import Path
import os
import re

p = Path(os.environ["JOBS"]) / "final_exam_nlp_ocr_retry.py"
t = p.read_text(encoding="utf-8")
t = re.sub(r"from index_fen_qdrant import upsert_fen_records\n", "", t)
t = t.replace(
    "Upserts MinIO JSONL + Qdrant. Skip+success when none / Upsert MinIO+Qdrant; không có thì success.",
    "Writes MinIO JSONL only (exam stack has no Qdrant). Skip+success when none.",
)
t = re.sub(
    r"\n    qdrant = \{\"upserted\": 0\}\n    if recovered:\n        qdrant = upsert_fen_records\(group_id=group_id, records=recovered\)\n",
    "\n    if recovered:\n        print(f\"{LOG} recovered_records={len(recovered)} (minio only)\", flush=True)\n",
    t,
    count=1,
)
t = t.replace('        "qdrant_upserted": qdrant.get("upserted"),\n', "")
p.write_text(t, encoding="utf-8")
print("patched ocr_retry (no qdrant)")

p2 = Path(os.environ["JOBS"]) / "final_exam_nlp_ocr_label_dual.py"
t2 = p2.read_text(encoding="utf-8")
old = """    if target <= 0 and not priority_only and not force:
        # Safe default chunk for rollover (skip-done) / Chunk mặc định an toàn khi rollover
        target = DEFAULT_TARGET_CHUNK"""
new = """    if (
        target <= 0
        and not priority_only
        and not force
        and not _bool_env("FEN_LABEL_UNLIMITED", False)
    ):
        # Safe default chunk for rollover (skip-done) / Chunk mặc định an toàn khi rollover
        target = DEFAULT_TARGET_CHUNK"""
if old in t2 and "FEN_LABEL_UNLIMITED" not in t2:
    t2 = t2.replace(old, new, 1)
    p2.write_text(t2, encoding="utf-8")
    print("patched label_dual FEN_LABEL_UNLIMITED")
PY

# Paddle OCR service files
cp "$UPSTREAM/services/paddle_ocr/"* "$ROOT/docker/paddle-ocr/" 2>/dev/null || true
apply_fen_prefix "$ROOT/docker/paddle-ocr/start.sh" 2>/dev/null || true
apply_fen_prefix "$ROOT/docker/paddle-ocr/app.py" 2>/dev/null || true

# requirements
cp "$UPSTREAM/dags/requirements.txt" "$ROOT/dags/requirements.txt"

chmod +x "$ROOT"/scripts/*.sh

echo "==> sync done. Run: make config && make deploy"
