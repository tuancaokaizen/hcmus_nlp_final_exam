# User setup

Install and run the exam pipeline: **crawl → calligraphy gate → download → label dual OCR**.

- Troubleshooting / noVNC: **[LOCAL_SETUP_LOG.md](LOCAL_SETUP_LOG.md)**
- B2 output files + **role of each OCR model**: **[LABEL_DUAL_OUTPUT.md](LABEL_DUAL_OUTPUT.md)** (§0)
- Short README: **[../README.md](../README.md)**

---

## Quick read: what each OCR model does

| Model | Role |
|-------|------|
| **Gemini** | Branch A — main vision OCR → `gemini` column |
| **Paddle** | Branch B — local OCR, parallel with Gemini |
| **GPT** | Refine each branch before fuse |
| **DeepSeek** | Optional — column order / layout |
| **Fuse** | Merge A∥B → submitted `ground_truth` |
| **GLM** | QC judge → silver / needs HITL (`fuse_gt`) |
| **Calligraphy (Gemini)** | Crawl gate only (is it calligraphy?) — **not** B2 OCR |

Details + diagram: **[LABEL_DUAL_OUTPUT.md §0](LABEL_DUAL_OUTPUT.md#0-role-of-each-model-ocr--label-dual)**.

---

## Quick read: what is a DAG?

You do **not** need to rename DAGs. Remember this table:

| DAG | Plain meaning | Catch-bottom? |
|-----|---------------|---------------|
| **`fen_e2e_pipeline`** | **One batch** crawl + OCR full path — **start here** | **No** (no `catch_bottom` param) |
| **`fen_crawl_pipeline`** | Crawl (many batches possible) + auto OCR | **Yes** — default `true` (rollover to `bottom_year`) |
| **`fen_label_dual_pipeline`** | OCR only when images already on MinIO | — |
| `fen_ocr_pipeline` | Legacy — skip for new B2 | — |

`e2e` = end-to-end (same DAG does crawl→OCR); it does **not** mean “production catch-bottom”.  
For one batch on `fen_crawl_pipeline` → set `"catch_bottom": false`.

Flow inside each crawl run:

```
discover → enrich → download → (label dual)
  posts     valid?     images      OCR B2
```

- Already-**seen** posts → skip re-crawl.  
- Already-OCR’d images (page exists) → skip (unless `force: true`).

---

## First time (checklist)

1. Install **Docker** + Compose v2 + (recommended) [`mc`](https://min.io/docs/minio/linux/reference/minio-client/minio-mc.html)
2. Clone **`main`** → `cp .env.example .env` → `make configure`
3. Open `.env`, fill **`FEN_LABEL_GEMINI_API_KEY`**, **`FEN_LABEL_GPT_API_KEY`**, **`FEN_LABEL_GLM_API_KEY`** (wizard only asks calligraphy + OCR — copying the same Ramcloud key is fine)
4. `make config` → `make up` (first build may take 10–20 minutes)
5. `make fb-login-manual` → terminal prints **http://localhost:7900** (pass `secret`). With `FB_USERNAME`/`FB_PASSWORD` in `.env`, fields auto-fill; you only do **2FA** on noVNC.
6. Airflow http://localhost:8080 (`admin`/`admin`) → **unpause** `fen_e2e_pipeline` → **Trigger**

Smoke (cheap API):

```json
{
  "group_id": "322453387859386",
  "batch_target": 10,
  "reset_crawl_data": true,
  "ocr_limit": 3
}
```

| URL | Login |
|-----|-------|
| Airflow | `admin` / `admin` |
| MinIO `:9001` | `admin` / `admin1234` |
| noVNC `:7900` | `secret` |

---

## 1. Clone & configure

> Grading / submit branch: **`main`**. Do not use `features/*` when grading.

```bash
git clone https://github.com/tuancaokaizen/hcmus_nlp_final_exam.git
cd hcmus_nlp_final_exam
git checkout main && git pull origin main
cp .env.example .env
make configure
```

**Wizard asks:** Facebook (Enter = skip) + `FEN_CALLIGRAPHY_API_KEY` + `FEN_OCR_API_KEY`.  
**Fill by hand:** all `FEN_LABEL_*_API_KEY`, then `make config`.

| `.env` key | Used for |
|------------|----------|
| `FEN_CALLIGRAPHY_API_KEY` | Enrich — calligraphy gate |
| `FEN_LABEL_GEMINI_API_KEY` | Label dual — vision |
| `FEN_LABEL_GPT_API_KEY` | Label dual — GPT |
| `FEN_LABEL_GLM_API_KEY` | Label dual — GLM / `fuse_gt` |
| `FEN_LABEL_DEEPSEEK_API_KEY` | Label dual (optional) |
| `FEN_OCR_API_KEY` | Legacy `fen_ocr_pipeline` only |

`configure` sets `FEN_HOST_PROJECT_DIR` = absolute repo path (keep quotes if the path has spaces).

Already cloned — update only:

```bash
git checkout main && git pull origin main
make config && make deploy
```

---

## 2. Start stack + FB login

```bash
make up
make fb-login-manual    # recommended if 2FA
# or: make fb-login   # if FB_PASSWORD (+ TOTP) is set
```

Force image rebuild: `make bootstrap`.

| Mode | When |
|------|------|
| Default (`FEN_DAG_SOURCE=minio`) | Grading / new machine (needs `mc`) |
| `make up-dev` | Editing DAGs often |

**If Airflow shows `No module named 'common'`:** with default MinIO mode, Airflow parses DAGs from the sync volume (`airflow-dags-cache`), not your host `./dags`. That volume must contain `jobs/common/` after deploy + sidecar sync.

1. Install [`mc`](https://min.io/docs/minio/linux/reference/minio-client/minio-mc.html) on the host (required for `make deploy` / `make up` in MinIO mode).
2. Re-run `make deploy`, wait ~30s for `airflow-dag-sync`, then refresh the DAG list.
3. Or skip the sidecar: `make up-dev` / `FEN_DAG_SOURCE=local make up` (bind-mount `./dags`).

If `make up` printed `WARN: 'mc' not on PATH — … Skipping deploy`, you will hit this until `mc` is installed and deploy runs.

---

## 3. Daily loop

```bash
make up
make deploy      # after DAG/job edits
# Airflow → unpause → Trigger
make verify
```

---

## 4. Trigger config (Configuration JSON)

### Easy-to-confuse parameters

| Param | Stage | Unit | Default |
|-------|-------|------|---------|
| **`batch_target`** | Discover | **posts** / batch | `10` |
| **`ocr_limit`** | Label dual (e2e / crawl) | pending **images**; `0` = full | `0` |
| **`label_limit`** | Label dual DAG | same as `ocr_limit` | `0` |
| **`flush_posts`** | Label dual | upsert every N **images** | `5` |
| **`catch_bottom`** | **`fen_crawl_pipeline` only** | bool | `true` |
| **`reset_crawl_data`** | Crawl | clear crawl state, keep cookies | `false` |
| **`force`** | Label dual | re-OCR finished images | `false` |

No `batch_size` — use **`batch_target`**.

### `fen_e2e_pipeline` — one batch, no catch-bottom

```json
{
  "group_id": "322453387859386",
  "batch_target": 10,
  "reset_crawl_data": true,
  "ocr_limit": 0,
  "flush_posts": 5,
  "run_fb_login": false
}
```

- Always exactly **one** discover batch of `batch_target`, then enrich → download → label dual.
- No `catch_bottom` / rollover.
- OCR uses the full internal quote queue (`batch_seq=0`); no need to pass `batch_seq`.
- `prepare_queues` compares images to `valid_post`: **new images → auto-rebuild** `quote_01..12` (no skip for stale queues). Already-OCR’d images still skip via page sidecars.

### `fen_e2e_pipeline` — JSONL seed (Phase C)

```bash
cp your_posts.jsonl seeds/input.jsonl
```

```json
{
  "group_id": "322453387859386",
  "source": "jsonl",
  "batch_target": 10,
  "ocr_limit": 3,
  "flush_posts": 5,
  "jsonl_skip_seen": false
}
```

- No GraphQL discover — reads `seeds/input.jsonl`.
- Live CDN → calligraphy gate → download; expired → Selenium enrich → same gate.
- See [`seeds/README.md`](../seeds/README.md).

### `fen_crawl_pipeline` — crawl + (optional) catch-bottom

```json
{
  "group_id": "322453387859386",
  "batch_target": 10,
  "reset_crawl_data": true,
  "catch_bottom": false,
  "ocr_limit": 0,
  "flush_posts": 5
}
```

- **`catch_bottom: false`** — same spirit as e2e for crawl count: **one** batch then stop.
- **`catch_bottom: true`** (default) — after each batch, cooldown then **re-trigger** crawl until `bottom_year` (2013) or feed end → **long**.
- **`source: jsonl`** — one-shot seed ingest; rollover is forced off.
- After download → auto-triggers **`fen_label_dual_pipeline`**.
- `demo_mode: true` (legacy) = `catch_bottom: false`.

### `fen_label_dual_pipeline` — OCR only

```json
{
  "group_id": "322453387859386",
  "batch_seq": 0,
  "label_limit": 0,
  "flush_posts": 5,
  "prepare_queues": true,
  "glm": true,
  "force": false
}
```

- `batch_seq: 0` = all queues 1–12 (recommended). `1`–`12` = one shard (may be empty with few images).
- Output: `ocr/label_dual_pilot/task_b2.jsonl` (+ `.xlsx`), `glm/recommend.jsonl` (`fuse_gt`).
- Local submit: `output/{group_id}/task_b2.jsonl` + `.xlsx`.

---

## 5. Valid / invalid (summary)

A post is **valid** when:

1. It has a real caption (not Facebook UI) + ≥ 1 **image** URL (video does not count → `missing_image`).
2. Calligraphy gate: ≥ 1 image **handwritten / mixed** (printed-only / spam rejected).
3. B1 export: plus images downloaded to MinIO.

Reason details: enrich logs / `invalid_post.jsonl`.

---

## 6. Facebook login

```bash
make fb-login-manual   # noVNC :7900
make fb-login          # auto if password/TOTP in .env
```

---

## 7. After code changes

```bash
make deploy
```

Many Python job changes: `FEN_UP_BUILD=always make up`

---

## 8. Stop / reset

| Command | Data |
|---------|------|
| `make down` | Keep MinIO, Postgres, cookies |
| `make down-v` | Wipe volumes — FB login again |

```bash
make down-v && make up && make fb-login-manual
```
