# Crawl state & checkpoints

Exam repo stores crawl checkpoints under `facebook/{group_id}/crawl/`.

## Files on MinIO (`final-exam-nlp-raw`)

| Path | Purpose |
|------|---------|
| `crawl/checkpoint.json` | Cursor, batch seq, `should_continue`, `mode` (`catchup`/`backfill`) |
| `crawl/discover/seen_post_ids.json` | Dedup post IDs (shared by crawl + JSONL ingest) |
| `crawl/discover/batches/` | Discover batch JSONL |
| `crawl/seed/batches/` | Audit copy of JSONL-ingest batches (`source=jsonl`) |
| `crawl/enrich/` | Enriched posts + calligraphy gate results |
| `crawl/download/log.jsonl` | Download audit log |
| `crawl/state/cookies.json` | FB session (after `make fb-login`) |
| `images/{post_id}/` | Downloaded image bytes (group root) |
| `export/valid_post.jsonl` | Posts passing calligraphy gate (group level) |
| `export/invalid_post.jsonl` | Rejected / incomplete posts |
| `ocr/queue/` | Post queues after download (input to label dual prepare) |
| `ocr/label_dual_pilot/` | **Main B2 path** — see [LABEL_DUAL_OUTPUT.md](LABEL_DUAL_OUTPUT.md) |

## JSONL seed ingest (`source=jsonl`)

Host path: **`seeds/input.jsonl`** — rename your seed to this name under `seeds/` (no path param). Mounted as `/opt/fen-exam/seeds`.

Job `fen_jsonl_ingest`:

1. Decode `post_id` (plain digits or base64 `…VK:{id}`).
2. If `jsonl_skip_seen=true` (DAG default) and `post_id` ∈ `seen` → skip (`skipped_seen`; does **not** count toward `batch_target`).
3. Probe each CDN URL.
4. **Live** → discover row with images (`source=jsonl_cdn`) → enrich calligraphy → download if keep.
5. **Expired** → incomplete (`cdn_expired`) → Selenium enrich via permalink → new CDN → same gate → download if keep.

Sets `checkpoint.should_continue=false` (no catch-bottom rollover for seed runs).

## Calligraphy gate vs label dual OCR

- **Gate (during enrich):** `final_exam_nlp_calligraphy_classify.py` — Gemini vision on CDN preview bytes; uses `[fen_calligraphy]` / `FEN_CALLIGRAPHY_API_KEY`. Decides **valid** vs **invalid** (keep for download?).
- **OCR (after download):** **`fen_label_dual_pipeline`** / `final_exam_nlp_ocr_label_dual.py` — Gemini ∥ Paddle → fuse → GLM. Uses `FEN_LABEL_*` keys. Output under `ocr/label_dual_pilot/` (`task_b2.jsonl`).

Legacy single-model OCR (`fen_ocr_pipeline` / `final_exam_nlp_ocr.py` / `[fen_ocr]`) is **optional only** — not used for new B2 submissions.

## Catch-bottom vs one batch

When Airflow DAG param **`catch_bottom=true`** (default on **`fen_crawl_pipeline` only**), rollover runs while `checkpoint.json` has `should_continue=true` (until `bottom_year`, typically 2013, or feed exhausted). Set **`catch_bottom=false`** for a single `batch_target` batch.

**`fen_e2e_pipeline`** has **no** `catch_bottom` — always one batch then label dual.

Legacy: `demo_mode=true` is the same as `catch_bottom=false` (deprecated name).

## MinIO init (`make up`)

`minio-init` creates buckets `airflow` + `final-exam-nlp-raw` and seeds `.keep` markers under `facebook/{FEN_GROUP_ID}/` for crawl, export, images, ocr. Jobs still create real files on first run.

Reset crawl data: trigger with `reset_crawl_data=true` or delete `crawl/` prefix for the group in MinIO (cookies under `state/` can be kept).

## Queue, `ocr_limit`, and skip (label dual)

After download, valid posts with images are written to `ocr/queue/batch_{batch_seq:06d}.jsonl`. **`fen_crawl_pipeline`** and **`fen_e2e_pipeline`** auto-trigger **`fen_label_dual_pipeline`** (not the legacy OCR DAG).

| Concept | Detail |
|---------|--------|
| **`batch_target`** | Crawl / JSONL — target **new** posts per batch (duplicates do not count) |
| **`ocr_limit` / `label_limit`** | Label dual — max pending **images**; `0` = no cap (full queue) |
| **Skip OCR** | Per **image** if a page sidecar already exists under `ocr/label_dual_pilot/pages/`; `force=true` re-OCRs |
| **Skip crawl** | `post_id` already in `seen_post_ids.json` |
| **Skip JSONL** | Same seen set when `jsonl_skip_seen=true` (default) |

Rollover (`catch_bottom=true` on crawl DAG): label-dual trigger and next crawl batch can run in parallel — OCR does not block rollover.
