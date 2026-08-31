# Crawl state & checkpoints

Exam repo reuses the production crawl state pattern under `facebook/{group_id}/crawl/` (not `v5/`).

## Files on MinIO (`final-exam-nlp-raw`)

| Path | Purpose |
|------|---------|
| `crawl/checkpoint.json` | Cursor, batch seq, `should_continue` |
| `crawl/discover/seen_post_ids.json` | Dedup post IDs |
| `crawl/discover/batches/` | Discover batch JSONL |
| `crawl/enrich/` | Enriched posts + calligraphy gate results |
| `crawl/download/log.jsonl` | Download audit log |
| `crawl/state/cookies.json` | FB session (after `make fb-login`) |
| `images/{post_id}/` | Downloaded image bytes (group root) |
| `export/valid_post.jsonl` | Posts passing gate (group level) |
| `ocr/` | OCR JSONL output (group level) |

## Calligraphy gate vs OCR

- **Gate (during enrich):** `final_exam_nlp_calligraphy_classify.py` — Gemini vision on CDN preview bytes; uses `[fen_calligraphy]` API key.
- **OCR (after download):** `final_exam_nlp_ocr.py` — full text OCR on saved images; uses `[fen_ocr]` API key.

## Demo mode

When Airflow DAG param **`catch_bottom=true`** (default), rollover runs while `checkpoint.json` has `should_continue=true` (until `bottom_year`, typically 2013, or feed exhausted). Set **`catch_bottom=false`** for a single `batch_target` batch only.

Legacy: `demo_mode=true` is the same as `catch_bottom=false` (deprecated name).

## MinIO init (`make up`)

`minio-init` creates buckets `airflow` + `final-exam-nlp-raw` and seeds `.keep` markers under `facebook/{FEN_GROUP_ID}/` for crawl, export, images, ocr. Jobs still create real files on first run; empty prefixes are optional in S3 but help the MinIO console.

Reset crawl data: trigger with `reset_crawl_data=true` or delete `crawl/` prefix for the group in MinIO.

## OCR queue & skip

After download, valid posts with images are written to `ocr/queue/batch_{batch_seq:06d}.jsonl`. `fen_crawl_pipeline` auto-triggers `fen_ocr_pipeline` for that `batch_seq` with default `ocr_limit=0` (entire queue).

| Concept | Detail |
|---------|--------|
| **`batch_target`** | Crawl stage — target posts per batch |
| **`ocr_limit`** | OCR stage — max **posts** from queue per run (`0` = no cap) |
| **Default** | All DAGs: `ocr_limit=0` → OCR full queue after crawl |
| **Optional cap** | Set `ocr_limit: N` to OCR only first N queued posts |
| **Skip** | Per **image** in `ocr_result.jsonl` (`skipped_existing`); `force=true` re-OCRs |
| **Resume** | Partial runs hydrate from `ocr/details/*.json` |

Rollover (`catch_bottom=true`): OCR trigger and next crawl batch run in parallel — OCR does not block rollover.
