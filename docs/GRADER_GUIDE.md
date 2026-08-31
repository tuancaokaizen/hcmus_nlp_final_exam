# Grader guide

Checklist chấm bài pipeline exam (Docker Compose).

Chi tiết artifact label dual: **[LABEL_DUAL_OUTPUT.md](LABEL_DUAL_OUTPUT.md)**.

---

## 1. Infrastructure

1. `docker compose ps` — `minio`, `postgres`, `airflow-webserver`, `airflow-scheduler`, `paddle-ocr` running.
2. MinIO http://localhost:9001 (`admin` / `admin1234`) — buckets `airflow`, `final-exam-nlp-raw`.
3. Airflow http://localhost:8080 (`admin` / `admin`) — DAGs:
   - `fen_e2e_pipeline`
   - `fen_crawl_pipeline`
   - **`fen_label_dual_pipeline`**
   - `fen_ocr_pipeline` (legacy, optional)

---

## 2. Config

- `dags/config.ini` exists after `make configure`.
- Keys **tách stage** (không dùng chung một key):
  - `[fen_calligraphy]` — enrich
  - `[fen_label_gemini]`, `[fen_label_gpt]`, `[fen_label_glm]` — label dual
  - `[fen_ocr]` — chỉ legacy OCR

---

## 3. Artifacts sau E2E

Prefix: `final-exam-nlp-raw/facebook/{group_id}/`

### Crawl (B1)

| Path | Required |
|------|----------|
| `crawl/checkpoint.json` | Yes |
| `crawl/discover/seen_post_ids.json` | Yes |
| `export/valid_post.jsonl` | Yes |

### Label dual (B2) — luồng chính

| Path | Required |
|------|----------|
| `ocr/label_dual_pilot/task_b2.jsonl` | Yes (nếu OCR đã chạy) |
| `ocr/label_dual_pilot/task_b2.xlsx` | Yes (cuối run) |
| `ocr/label_dual_pilot/summary.json` | Yes |
| `ocr/label_dual_pilot/glm/recommend.jsonl` | Yes nếu `glm=true` — có field **`fuse_gt`** |

Legacy only (không thay B2):

- `ocr/ocr_result.jsonl` — từ `fen_ocr_pipeline`

---

## 4. Quick verify

```bash
make verify
```

Checks crawl + optional label dual paths.

---

## 5. Expected scope

| Setting | Default | Note |
|---------|---------|------|
| `batch_target` | 10 posts | Crawl batch |
| `ocr_limit` | 0 | Full label-dual queue (images) |
| `flush_posts` | 5 | Upsert every 5 images |
| `catch_bottom` | true | Rollover until `bottom_year` (2013) |

- No Qdrant — MinIO only.
- Jobs run in `fen-exam-fen-job` container (DockerOperator).

---

## 6. Stack lifecycle

| Command | Effect |
|---------|--------|
| `make down` | Stop; **keep** data |
| `make down-v` | Wipe volumes |

Fresh grading run (clone **`main`**):

```bash
git clone https://github.com/tuancaokaizen/hcmus_nlp_final_exam.git
cd hcmus_nlp_final_exam
git checkout main
make down-v && make up && make fb-login
```

---

## 7. Optional: without live Facebook

Primary path: live crawl + grader API keys. Sample MinIO data may be provided separately (not in repo by default).
