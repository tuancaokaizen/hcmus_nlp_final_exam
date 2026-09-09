# Reviewer test pack

Top-50 calligraphy images + OCR diary. Re-run label dual locally and compare to diary GT.

## Contents

| Path | Purpose |
|------|---------|
| `dataset/top50.jsonl` | 50 samples + OCR diary |
| `dataset/images/` | 50 jpg files |
| `run_label_dual_review.py` | Re-run OCR fuse + compare vs diary |
| `.env.example` | Shared Ramcloud key (Gemini / GPT / GLM) |
| `requirements.txt` | Python deps |
| `output/run_*/` | Generated results (local only) |

## Setup

```bash
cd reviewer_test
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Optional: start Paddle from repo root so `http://127.0.0.1:8088/ocr` is up. If Paddle is down, Gemini still runs.

Needs the repo checkout (imports `dags/jobs/final_exam_nlp_ocr_label_dual.py`).

## Run

```bash
# Smoke
python3 run_label_dual_review.py --limit 2

# Full top-50
python3 run_label_dual_review.py

# Skip GLM
python3 run_label_dual_review.py --no-glm
```

Stack: Gemini + Paddle + GPT refine + optional GLM (no DeepSeek).

## Outputs

Under `output/run_<UTC>/`:

| File | Content |
|------|---------|
| `compare.jsonl` | CER / bag / `agree_diary_gt` vs diary |
| `task_b2.jsonl` | New B2-style rows |
| `pages/*.json` | Per-image fuse dumps |
| `summary.json` | Agree rate, mean CER |
