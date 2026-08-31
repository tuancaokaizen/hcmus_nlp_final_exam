# FEN NLP Exam Pipeline (Docker)

Reproducible **crawl → calligraphy classify → download → label dual OCR** demo for the HCMUS NLP final exam, **Docker Compose only**.

## Requirements

- Docker + Docker Compose v2
- [`mc`](https://min.io/docs/minio/linux/reference/minio-mc.html) (MinIO client) on the host — used by `make up` / `make deploy` when `FEN_DAG_SOURCE=minio`
- Ramcloud API keys: **separate** per stage — calligraphy (`FEN_CALLIGRAPHY_API_KEY`), label dual (`FEN_LABEL_*`), legacy OCR (`FEN_OCR_API_KEY`)
- Facebook credentials for live crawl (optional until you run crawl)

---

## End-to-end flow (how to run)

### 1. First-time setup

> **Branch:** use **`main`** only. Do not checkout feature branches for the exam pipeline.

```bash
git clone https://github.com/tuancaokaizen/hcmus_nlp_final_exam.git
cd hcmus_nlp_final_exam
git checkout main
git pull origin main
cp .env.example .env
make configure          # wizard: FB creds + API keys; sets FEN_HOST_PROJECT_DIR
make up                 # workspace + images + stack + buckets + deploy DAGs + Airflow
make fb-login           # save FB cookies to MinIO (needed before crawl)
```

**Update an existing clone:**

```bash
git checkout main
git pull origin main
make deploy             # after DAG/job changes
```

| URL | Login |
|-----|-------|
| Airflow | http://localhost:8080 — `admin` / `admin` |
| MinIO console | http://localhost:9001 — `admin` / `admin` |

Force rebuild all images (slow, first clone or after Dockerfile changes):

```bash
make bootstrap          # same as FEN_UP_BUILD=always make up
```

### 2. Run the pipeline (Airflow)

1. Open Airflow UI → **unpause** the DAG (DAGs are paused at creation).
2. **Trigger DAG** → optional **Configuration JSON** (examples below).
3. Watch task logs in the UI.

| DAG | Use when |
|-----|----------|
| **`fen_e2e_pipeline`** | One linear run: crawl batch → **label dual** full queue (`ocr_limit` default **`0`**) |
| **`fen_crawl_pipeline`** | Full crawl: discover → enrich → download → auto-trigger **label dual**; optional rollover |
| **`fen_label_dual_pipeline`** | Label dual only (Gemini ∥ Paddle → fuse → GLM → `fuse_gt` / `task_b2.jsonl`) |
| **`fen_ocr_pipeline`** | Legacy single-model OCR (optional manual re-run) |

**Trigger examples** (Configuration JSON):

`fen_e2e_pipeline`:

```json
{
  "group_id": "322453387859386",
  "batch_target": 10,
  "reset_crawl_data": true
}
```

Optional: `"ocr_limit": 5` to cap images; `"flush_posts": 10` to change upsert interval (default **`5`** images).

`fen_crawl_pipeline` (label dual full queue by default):

```json
{
  "group_id": "322453387859386",
  "batch_target": 10,
  "reset_crawl_data": true,
  "catch_bottom": false
}
```

- **`catch_bottom`** (default **`true`**): `true` = bắt đáy — rollover liên tục đến `bottom_year` (mặc định 2013) hoặc hết feed; `false` = chỉ **một** batch `batch_target` post rồi dừng
- `demo_mode`: deprecated — `true` tương đương `catch_bottom=false`

`fen_label_dual_pipeline`:

```json
{
  "group_id": "322453387859386",
  "batch_seq": 1,
  "label_limit": 0,
  "prepare_queues": true,
  "glm": true,
  "force": false
}
```

Output: MinIO `facebook/{group_id}/ocr/label_dual_pilot/task_b2.jsonl` and `glm/recommend.jsonl` (field **`fuse_gt`**).

`fen_ocr_pipeline` (legacy):

```json
{
  "group_id": "322453387859386",
  "batch_seq": 1,
  "ocr_limit": 0,
  "force": false
}
```

### 3. Verify results

```bash
make verify
```

Checks MinIO for crawl artifacts + optional `ocr/label_dual_pilot/task_b2.jsonl` (see [Label dual output](docs/LABEL_DUAL_OUTPUT.md)).

### 4. After code changes

| You changed | Command |
|-------------|---------|
| DAG / `config.ini` | `make up` or `make deploy` (wait ~30s for DAG sync if `FEN_DAG_SOURCE=minio`) |
| Job Python (`dags/jobs/`) | `FEN_UP_BUILD=always make up` or `make deploy` (rebuilds `fen-job`) |
| `.env` API keys | `make config` then `make deploy` |

### 5. Stop / restart / reset

| Command | Containers | Data (volumes) | When to use |
|---------|------------|----------------|-------------|
| **`make down`** | Stopped & removed | **Kept** — MinIO artifacts, Postgres, FB profile, DAG cache | Pause work; resume with `make up` |
| **`make down-v`** | Stopped & removed | **Deleted** — all named volumes wiped | Clean slate; then `make up` + `make fb-login` again |

**`make down` does not delete:** repo files (`.env`, `dags/`, `config.ini`), Docker images.

**`make down-v` deletes:** crawl checkpoints, images on MinIO, OCR output, Airflow DB, Selenium profile, synced DAG cache. You must re-seed buckets and log in to Facebook again.

Typical reset:

```bash
make down-v
make up
make fb-login
# trigger DAG with reset_crawl_data: true
```

---

## What `make up` does

Single command to prepare and start the full environment:

1. **prepare_workspace** — local dirs, `dags/config.ini`, verify job/DAG files (optional `make sync` if missing)
2. **build** — Docker images if not present (`fen-job`, Airflow, paddle-ocr)
3. **compose up** — MinIO, Postgres, Airflow, Paddle, Selenium, `airflow-dag-sync`
4. **minio-init** — buckets `airflow` + `final-exam-nlp-raw`; seed layout `facebook/{group_id}/…`
5. **deploy** — mirror `dags/` → MinIO (when `FEN_DAG_SOURCE=minio` and `mc` is installed)
6. **airflow-init** + webserver + scheduler

DAG source:

| Mode | Setting | Airflow reads DAGs from |
|------|---------|-------------------------|
| Default | `FEN_DAG_SOURCE=minio` | MinIO → sidecar → volume |
| Dev fast | `FEN_DAG_SOURCE=local` or `make up-dev` | Bind mount `./dags` |

---

## Architecture

| Stage | Job | API key (`config.ini`) |
|-------|-----|------------------------|
| Discover | `fen_crawl_discover` | — |
| Enrich + calligraphy gate | `fen_crawl_enrich` | `[fen_calligraphy]` |
| Download images | `fen_crawl_download` | — |
| Label dual (OCR) | `fen_label_dual` | `[fen_label_gemini]`, `[fen_label_gpt]`, `[fen_label_glm]` + Paddle |
| Legacy OCR | `fen_ocr` | `[fen_ocr]` |

MinIO layout (`final-exam-nlp-raw`):

```
facebook/{group_id}/
  crawl/checkpoint.json, discover/, enrich/, download/, state/cookies.json
  export/valid_post.jsonl
  images/{post_id}/
  ocr/label_dual_pilot/task_b2.jsonl, glm/recommend.jsonl (fuse_gt), pages/
  ocr/queue/ … (legacy fen_ocr only)
```

**Label dual notes:** `ocr_limit` / `label_limit` caps **pending images** (`0` = full queue). **`flush_posts`** (default **5**) = upsert jsonl every N images. Done pages skipped unless `force=true`. Artifacts: MinIO **`task_b2.jsonl`** (có `side_matter`); local submit **`output/{group_id}/task_b2.jsonl` + `.xlsx`** — thuần 5 cột, **không** `side_matter`.

### `batch_target` vs `ocr_limit` vs `flush_posts`

| Param | Stage | Meaning | DAG defaults |
|-------|-------|---------|--------------|
| **`batch_target`** | Crawl (discover) | Target posts per crawl batch | `10` |
| **`ocr_limit`** | Label dual | Max pending **images**; `0` = full queue | **`0`** |
| **`flush_posts`** | Label dual | Upsert jsonl every N **images** | **`5`** |

**Optional cap:** set `"ocr_limit": N` at trigger to label only the first N pending images (save API cost during smoke tests).

**`fen_crawl_pipeline`:** after download, auto-triggers **`fen_label_dual_pipeline`** with `label_limit=0` (entire pending queue). Re-runs skip pages already done under `ocr/label_dual_pilot/`.

**Rollover / bắt đáy:** `catch_bottom` default is **`true`** — multi-batch crawl while `should_continue` until `bottom_year`. Set `"catch_bottom": false` for one `batch_target` batch only. Label dual and rollover run **in parallel** after download.

---

## Make targets (cheat sheet)

| Target | Description |
|--------|-------------|
| `make configure` | Interactive `.env` + `config.ini` |
| `make up` | Full environment (see above) |
| `make down` | Stop stack; **keep** volumes / pipeline data |
| `make down-v` | Stop stack; **wipe** volumes (fresh MinIO + DB) |
| `make bootstrap` | `FEN_UP_BUILD=always make up` |
| `make deploy` | Buckets + mirror DAGs to MinIO + rebuild `fen-job` |
| `make fb-login` | Save FB cookies to MinIO |
| `make verify` | Check E2E pipeline artifacts on MinIO |
| `make e2e` | `deploy` + `fb-login`, then trigger `fen_e2e_pipeline` in UI |
| `make build` | Rebuild all images |
| `make sync` | Đồng bộ/transform job code (optional) |
| `make up-dev` | Compose with `./dags` bind mount only (no MinIO DAG sync) |

---

## Env defaults (`.env`)

- `FEN_BATCH_TARGET=10` — crawl batch size target (posts)
- `FEN_OCR_LIMIT=0` — cap when running `fen_ocr` via **env only**; DAG `params` override this at trigger time
- `FEN_GROUP_ID` — Facebook group
- `FEN_CATCH_BOTTOM=true` — hint in `.env`; use DAG param `catch_bottom` at trigger on `fen_crawl_pipeline`
- `FEN_DEMO_MODE` — deprecated alias for `catch_bottom=false`

**DAG param defaults (at trigger):** see table in [User setup — batch_target vs ocr_limit](docs/USER_SETUP.md#batch_target-vs-ocr_limit-read-this-first).

---

## Docs

- **[Label dual — output, upsert, flush](docs/LABEL_DUAL_OUTPUT.md)** ⭐
- [User setup & trigger config](docs/USER_SETUP.md)
- [Pipeline build, deploy & E2E](docs/PIPELINE_BUILD_DEPLOY_RUN.md)
- [Crawl state & checkpoints](docs/CRAWL_STATE.md)
- [Grader guide](docs/GRADER_GUIDE.md)
- [Proposal (Docker)](docs/PROPOSAL_DOCKER_EXAM_PIPELINE.md)
