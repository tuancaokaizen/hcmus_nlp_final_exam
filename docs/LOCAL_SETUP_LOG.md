# Local setup log — build → cookies

Local setup steps with Docker Compose. Keep default values; **only fill in** Facebook credentials and API keys (do not commit `.env`).

**New users:** read the short checklist in [USER_SETUP.md](USER_SETUP.md) first; this file adds detail + common errors.

## 0. Create `.env`

`.env` is **not in git** — each machine creates it from `.env.example`.

### Option 1 — recommended (`make configure`)

```bash
# From repo root (directory with docker-compose.yml, Makefile)
cp .env.example .env    # only if .env is missing; configure also copies if needed
make configure
```

Wizard prompts in order:

| Prompt | Written to |
|--------|------------|
| Facebook email/phone | `FB_USERNAME` (Enter = skip) |
| Facebook password | `FB_PASSWORD` (Enter = skip; use `make fb-login-manual` later) |
| Ramcloud base URL | `RAMCLOUDS_BASE_URL` (default `https://ramclouds.me/v1`) |
| `FEN_CALLIGRAPHY_API_KEY` | crawl gate key |
| `FEN_OCR_API_KEY` | OCR key |

After the wizard, the script also:

- Sets `FEN_HOST_PROJECT_DIR` = absolute repo path (auto-**quotes** if the path has spaces)
- Runs `make config` → generates `dags/config.ini`

Remaining keys (`FEN_LABEL_*_API_KEY`, …) if the wizard did not ask: open `.env` and fill by hand (same Ramcloud value is fine if you share one key).

### Option 2 — copy then edit by hand

```bash
cp .env.example .env
# Open .env, fill FB + API keys
# Set repo path (quotes required if spaces):
#   FEN_HOST_PROJECT_DIR="/absolute/path/to/repo"
make config             # generate dags/config.ini from .env
```

### Checklist after `.env` exists

| Variable | Local default (in `.env.example`) | Your action |
|----------|-----------------------------------|-------------|
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | `admin` / `admin1234` | Keep |
| `AIRFLOW_USERNAME` / `AIRFLOW_PASSWORD` | `admin` / `admin` | Keep |
| `FEN_GROUP_ID` | `322453387859386` | Change if crawling another group |
| `FEN_HOST_PROJECT_DIR` | empty → `make configure` fills it | Check quotes if path has spaces |
| `FB_USERNAME` / `FB_PASSWORD` | empty | Fill, or leave empty + `make fb-login-manual` |
| `FEN_*_API_KEY` | empty | Fill your Ramcloud keys |

**Do not commit** `.env` (listed in `.gitignore`). Share only `.env.example`.

## 1. Script / image fixes (already in repo if merged)

| Issue | Fix |
|-------|-----|
| `scripts/up.sh` fails on `mapfile` (old bash) | Pass `-f` compose paths quoted directly |
| Paths with spaces break compose `-f` | Use `"$ROOT/docker-compose.yml"` |
| Docker Hub timeout / `gcr.io` mirror | Restart Docker Desktop; disable proxy/registry mirror if needed |
| `minio/mc:RELEASE.2024-12-18...` **not found** | Use `minio/mc:latest` |
| Pinned Selenium tag missing on some hosts | Use `selenium/standalone-chrome:latest` |
| Paddle missing `block_merge.py` / `text_layout.py` | Dockerfile copies all 3 files + rebuild |
| Paddle crash `paddlepaddle is not installed` | Dockerfile installs `paddlepaddle==3.2.2` then `requirements.txt` |

## 2. `make up`

```bash
# From repo root
make up
```

Stack: MinIO, Postgres, Airflow (web + scheduler), dag-sync, Selenium, Paddle OCR.

| Service | URL | Login (local default) |
|---------|-----|------------------------|
| Airflow | http://localhost:8080 | `admin` / `admin` |
| MinIO console | http://localhost:9001 | `admin` / `admin1234` |
| Paddle OCR | http://localhost:8088 | — |
| Selenium | http://localhost:4444 | — |
| noVNC (manual FB) | http://localhost:7900 | password `secret` |

After `make up`: MinIO buckets + DAGs under `airflow/dags/fen-exam/`.

## 3. Sync `config.ini`

Whenever you change MinIO / API keys in `.env`:

```bash
make config
# equivalent: bash scripts/generate_config.sh
```

`dags/config.ini` → `[minio] secret_key` must match `MINIO_ROOT_PASSWORD` (mismatch → `SignatureDoesNotMatch`).

> `config.ini` may contain API keys after generate — **do not commit**.

## 4. Facebook login (manual + save cookies)

`FB_TOTP_SECRET` is not required for manual login. Flow:

1. Compose exposes noVNC `7900`, `SE_VNC_PASSWORD=secret`.
2. Job with `FEN_MANUAL_LOGIN=true` opens FB login, waits for the user (including 2FA), then saves cookies.

```bash
# Chrome profile permissions (seluser uid 1200)
docker exec -u root fen-exam-selenium-chrome-1 \
  sh -c 'chown -R 1200:1201 /data/chrome-profile'

make fb-login-manual
```

**User steps**

1. Open http://localhost:7900 — password `secret`.
2. Log in to Facebook with **your account** in Chrome on noVNC (manual 2FA if needed).
3. Job detects `logged_in=True` → checks group → uploads cookies.

**Success log (example)**

```
saved N cookies to MinIO key=facebook/<FEN_GROUP_ID>/state/cookies.json slot=a
```

Default group: `FEN_GROUP_ID=322453387859386` (changeable in `.env`).

If login OK but upload fails with `SignatureDoesNotMatch`: `make config` then re-run `make fb-login-manual` (profile already logged in → only save cookies).

## 5. Related files (in repo)

- `.env` / `.env.example` — quote `FEN_HOST_PROJECT_DIR`; MinIO `admin` / `admin1234`
- `scripts/up.sh` — old-bash + paths with spaces
- `docker-compose.yml` — `minio/mc:latest`, selenium `latest`, port `7900`, VNC env
- `docker-compose.minio-dags.yml` — `minio/mc:latest`
- `docker/paddle-ocr/Dockerfile` — copies `app.py`, `block_merge.py`, `text_layout.py`
- `dags/jobs/run_job.py` — `FEN_MANUAL_LOGIN` / noVNC wait
- `Makefile` — `fb-login-manual` target

## 6. Next steps

1. Airflow UI → **unpause** **`fen_e2e_pipeline`** (recommended for new users: **one batch**, no catch-bottom).
2. Trigger with Configuration JSON — see [USER_SETUP.md](USER_SETUP.md) / README.
3. Watch task logs in Airflow.

For multi-batch / catch-bottom crawls → use `fen_crawl_pipeline` and read `catch_bottom` carefully in USER_SETUP.

## Short commands

```bash
cp .env.example .env
make configure           # wizard FB + API key + FEN_HOST_PROJECT_DIR + config.ini
# (or edit .env by hand then: make config)
make up
make fb-login-manual     # manual FB login on :7900 → cookies to MinIO
# → Airflow :8080 unpause + trigger DAG
```
