# FEN NLP Exam Pipeline (Docker)

HCMUS NLP exam pipeline / demo: **crawl Facebook → calligraphy filter → download images → dual OCR** (Gemini ∥ Paddle → fuse → GLM). **Docker Compose only**.

---

## New users: what first?

1. Install Docker + Compose; recommended: also install [`mc`](https://min.io/docs/minio/linux/reference/minio-client/minio-mc.html) (MinIO CLI).
2. Clone **`main`**, create `.env`, start the stack, log in to Facebook.
3. In Airflow: unpause **`fen_e2e_pipeline`** → Trigger (one small batch, no catch-bottom).

Details: **[docs/USER_SETUP.md](docs/USER_SETUP.md)** · Troubleshooting / noVNC: **[docs/LOCAL_SETUP_LOG.md](docs/LOCAL_SETUP_LOG.md)**.

### Quick start

```bash
git clone https://github.com/tuancaokaizen/hcmus_nlp_final_exam.git
cd hcmus_nlp_final_exam
git checkout main && git pull
cp .env.example .env
make configure          # FB (optional) + calligraphy/OCR key
# Open .env: fill FEN_LABEL_GEMINI/GPT/GLM(_DEEPSEEK)_API_KEY (same Ramcloud key is fine)
make up                 # first build may take 10–20 minutes
make fb-login-manual    # http://localhost:7900 (pass: secret); .env user/pass auto-fill → you only do 2FA
```

Airflow http://localhost:8080 (`admin` / `admin`) → unpause **`fen_e2e_pipeline`** → Trigger:

```json
{
  "group_id": "322453387859386",
  "batch_target": 10,
  "reset_crawl_data": true,
  "ocr_limit": 3
}
```

`ocr_limit: 3` = smoke (few images, cheap API). OCR the full queue: omit `ocr_limit` or set `"ocr_limit": 0`.

| URL | Login |
|-----|-------|
| Airflow | http://localhost:8080 — `admin` / `admin` |
| MinIO | http://localhost:9001 — `admin` / `admin1234` |
| noVNC (FB) | http://localhost:7900 — `secret` |

---

## What does the pipeline do? (1 minute)

```
discover → enrich (calligraphy gate) → download → label dual (OCR B2)
   new posts      valid / invalid           MinIO images     task_b2.jsonl
```

| Step | Meaning |
|------|---------|
| **Discover** | Scroll FB group, take ~`batch_target` **unseen** posts |
| **Enrich** | Enough caption + image? Handwritten calligraphy? → `valid` / `invalid` |
| **Download** | Upload **valid** post images to MinIO |
| **Label dual** | OCR each image: Gemini ∥ Paddle → fuse → GLM → B2 submit files |

**Auto-skip:** already-`seen` posts are not crawled again; images that already have an OCR page are not re-OCR’d (unless `force: true`).

---

## Which DAG? (important)

`fen_e2e_pipeline` = **one-shot end-to-end** (crawl + OCR in the same DAG).  
It does **not** catch-bottom / rollover — treat it as **one manual batch**.

| DAG | When to use | One batch? | Catch-bottom? |
|-----|-------------|------------|---------------|
| **`fen_e2e_pipeline`** | **New users / grading smoke** — full path in one go | Yes (`batch_target`) | **No** (no `catch_bottom`) |
| **`fen_crawl_pipeline`** | Long crawl / many batches | Each run = 1 batch | **Yes** — default `catch_bottom: true` (rollover) |
| **`fen_label_dual_pipeline`** | Images already on MinIO; OCR only | — | — |
| `fen_ocr_pipeline` | Legacy — **do not** use for new B2 | — | — |

**Tip:** always start with `fen_e2e_pipeline`. Open `fen_crawl_pipeline` only for deep crawls; first time set `"catch_bottom": false`.

### Sample triggers

**E2E — one batch (recommended):**

```json
{
  "group_id": "322453387859386",
  "batch_target": 10,
  "reset_crawl_data": true,
  "ocr_limit": 0,
  "flush_posts": 5
}
```

**Crawl — one batch then stop** (no catch-bottom):

```json
{
  "group_id": "322453387859386",
  "batch_target": 10,
  "reset_crawl_data": true,
  "catch_bottom": false
}
```

**Crawl — catch-bottom** (default if unset; can take a very long time):

```json
{
  "group_id": "322453387859386",
  "batch_target": 10,
  "catch_bottom": true
}
```

**Label dual only:**

```json
{
  "group_id": "322453387859386",
  "batch_seq": 0,
  "label_limit": 0,
  "prepare_queues": true,
  "glm": true,
  "force": false
}
```

`batch_seq: 0` = all image queues (recommended). `force: true` = re-OCR images already done.

---

## Easy-to-confuse parameters

| Param | Unit | Meaning | Default |
|-------|------|---------|---------|
| **`batch_target`** | **posts** / crawl batch | Max new posts discover takes | `10` |
| **`ocr_limit`** / `label_limit` | pending **images** | OCR cap; `0` = full queue | `0` |
| **`flush_posts`** | **images** | Upsert `task_b2` every N images | `5` |
| **`catch_bottom`** | bool | Only on **`fen_crawl_pipeline`** | `true` |
| **`reset_crawl_data`** | bool | Clear crawl state (keep cookies) before run | `false` |

There is no `batch_size` param — use **`batch_target`**.

---

## Setup / operations

### Requirements

- Docker + Compose v2  
- [`mc`](https://min.io/docs/minio/linux/reference/minio-client/minio-mc.html) when `FEN_DAG_SOURCE=minio` (default)  
- Ramcloud keys in `.env` (do not commit)  
- Facebook account (manual login via noVNC if 2FA)

### Update an existing clone

```bash
git checkout main && git pull origin main
make deploy
```

Rebuild images on first run / Dockerfile change: `make bootstrap`.

### After you change X, run Y

| Change | Command |
|--------|---------|
| DAG / `config.ini` | `make deploy` (wait ~30s for DAG sync in MinIO mode) |
| Job Python under `dags/jobs/` | `FEN_UP_BUILD=always make up` or `make deploy` |
| `.env` API keys | `make config` then `make deploy` |

### Stop / reset

| Command | Containers | Data volumes |
|---------|------------|--------------|
| `make down` | Stop | **Keep** (MinIO, cookies, DB) |
| `make down-v` | Stop | **Wipe all** — FB login again |

```bash
make down-v && make up && make fb-login-manual
```

### What `make up` does

Prepare workspace → build images (if missing) → MinIO/Postgres/Airflow/Paddle/Selenium → buckets + deploy DAGs → Airflow ready.

| Mode | Setting | Where DAGs come from |
|------|---------|----------------------|
| Default | `FEN_DAG_SOURCE=minio` | MinIO → sidecar |
| Fast dev | `make up-dev` | Bind mount `./dags` |

### Make cheat sheet

| Target | Action |
|--------|--------|
| `make configure` | Wizard for `.env` + `config.ini` |
| `make up` / `make down` / `make down-v` | Start / stop / wipe |
| `make deploy` | Buckets + sync DAGs + rebuild `fen-job` |
| `make fb-login-manual` | Manual FB login (noVNC) |
| `make verify` | Check artifacts on MinIO |
| `make e2e` | deploy + fb-login; trigger `fen_e2e_pipeline` in the UI |

---

## Output & short architecture

**MinIO** bucket `final-exam-nlp-raw`:

```
facebook/{group_id}/
  crawl/…          # checkpoint, seen, cookies
  export/          # valid_post.jsonl / invalid_post.jsonl  (B1)
  images/…         # downloaded images
  ocr/label_dual_pilot/   # task_b2.jsonl, glm/recommend.jsonl (fuse_gt), pages/
```

**Local submit (pure B2, 5 columns):** `output/{group_id}/task_b2.jsonl` + `.xlsx`.

| Stage | Job | Key (`config.ini`) |
|-------|-----|---------------------|
| Discover | `fen_crawl_discover` | — |
| Enrich + calligraphy | `fen_crawl_enrich` | `[fen_calligraphy]` |
| Download | `fen_crawl_download` | — |
| Label dual | `fen_label_dual` | `[fen_label_gemini]`, `[fen_label_gpt]`, `[fen_label_glm]` + Paddle |

B2 file details: **[docs/LABEL_DUAL_OUTPUT.md](docs/LABEL_DUAL_OUTPUT.md)**.

---

## Docs

| Doc | Contents |
|-----|----------|
| [USER_SETUP.md](docs/USER_SETUP.md) | Setup + trigger |
| [LOCAL_SETUP_LOG.md](docs/LOCAL_SETUP_LOG.md) | `.env`, noVNC, common errors |
| [LABEL_DUAL_OUTPUT.md](docs/LABEL_DUAL_OUTPUT.md) | Role of each OCR model (§0) + output, flush, skip |
| [CRAWL_STATE.md](docs/CRAWL_STATE.md) | Checkpoint / seen |
| [GRADER_GUIDE.md](docs/GRADER_GUIDE.md) | Grading checklist |
| [PIPELINE_BUILD_DEPLOY_RUN.md](docs/PIPELINE_BUILD_DEPLOY_RUN.md) | Deep build / deploy |
