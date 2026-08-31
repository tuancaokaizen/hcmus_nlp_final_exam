# Local setup log — build → cookies

Ghi lại các bước setup trên máy Apple Silicon (Docker Desktop). Giá trị mặc định local giữ nguyên; **chỉ điền riêng** tài khoản Facebook và API key (không commit `.env`).

**Người mới:** đọc checklist ngắn ở [USER_SETUP.md](USER_SETUP.md) trước; file này bổ sung chi tiết + lỗi thường gặp.

## 0. Tạo file `.env`

`.env` **không có sẵn trong git** — mỗi máy tự tạo từ template `.env.example`.

### Cách 1 — khuyến nghị (`make configure`)

```bash
# Từ root repo (thư mục có docker-compose.yml, Makefile)
cp .env.example .env    # chỉ cần nếu chưa có .env; configure cũng tự copy nếu thiếu
make configure
```

Wizard hỏi lần lượt:

| Prompt | Ghi vào |
|--------|---------|
| Facebook email/phone | `FB_USERNAME` (Enter = bỏ qua) |
| Facebook password | `FB_PASSWORD` (Enter = bỏ qua; dùng `make fb-login-manual` sau) |
| Ramcloud base URL | `RAMCLOUDS_BASE_URL` (mặc định `https://ramclouds.me/v1`) |
| `FEN_CALLIGRAPHY_API_KEY` | key crawl gate |
| `FEN_OCR_API_KEY` | key OCR |

Sau wizard, script còn:

- Set `FEN_HOST_PROJECT_DIR` = absolute path repo (tự **quote** nếu path có khoảng trắng)
- Chạy `make config` → sinh `dags/config.ini`

Các key còn lại (`FEN_LABEL_*_API_KEY`, …) nếu wizard không hỏi: mở `.env` điền tay (có thể copy cùng giá trị Ramcloud nếu dùng chung key).

### Cách 2 — copy rồi sửa tay

```bash
cp .env.example .env
# Mở .env, điền FB + API keys
# Set path repo (bắt buộc quote nếu có space):
#   FEN_HOST_PROJECT_DIR="/absolute/path/to/repo"
make config             # sinh dags/config.ini từ .env
```

### Checklist sau khi có `.env`

| Biến | Mặc định local (trong `.env.example`) | Việc của bạn |
|------|----------------------------------------|--------------|
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | `admin` / `admin1234` | Giữ nguyên |
| `AIRFLOW_USERNAME` / `AIRFLOW_PASSWORD` | `admin` / `admin` | Giữ nguyên |
| `FEN_GROUP_ID` | `322453387859386` | Đổi nếu crawl group khác |
| `FEN_HOST_PROJECT_DIR` | trống → `make configure` tự điền | Kiểm tra có quote nếu path có space |
| `FB_USERNAME` / `FB_PASSWORD` | trống | Điền, hoặc để trống + `make fb-login-manual` |
| `FEN_*_API_KEY` | trống | Điền key Ramcloud của bạn |

**Không commit** `.env` (đã có trong `.gitignore`). Chỉ share `.env.example`.

## 1. Sửa script / image (đã có trong repo nếu đã merge)

| Vấn đề | Cách xử lý |
|--------|------------|
| `scripts/up.sh` lỗi `mapfile` (bash 3.2 macOS) | Ghép `-f` compose trực tiếp với path quoted |
| Path có space làm vỡ `-f` compose | Dùng `"$ROOT/docker-compose.yml"` |
| Docker Hub timeout / mirror `gcr.io` | Restart Docker Desktop; tắt proxy/registry mirror nếu cần |
| `minio/mc:RELEASE.2024-12-18...` **not found** | Dùng `minio/mc:latest` |
| `selenium/standalone-chrome:4.27.0` không có arm64 | Dùng `selenium/standalone-chrome:latest` |
| Paddle thiếu `block_merge.py` / `text_layout.py` | Dockerfile copy đủ 3 file + rebuild |
| Paddle crash `paddlepaddle is not installed` | Dockerfile cài `paddlepaddle==3.2.2` rồi `requirements.txt` |

## 2. `make up`

```bash
# Từ root repo
make up
```

Stack: MinIO, Postgres, Airflow (web + scheduler), dag-sync, Selenium, Paddle OCR.

| Service | URL | Login (mặc định local) |
|---------|-----|------------------------|
| Airflow | http://localhost:8080 | `admin` / `admin` |
| MinIO console | http://localhost:9001 | `admin` / `admin1234` |
| Paddle OCR | http://localhost:8088 | — |
| Selenium | http://localhost:4444 | — |
| noVNC (manual FB) | http://localhost:7900 | password `secret` |

Sau `make up`: buckets MinIO + DAG trên `airflow/dags/fen-exam/`.

## 3. Đồng bộ `config.ini`

Mỗi lần đổi MinIO / API keys trong `.env`:

```bash
make config
# tương đương: bash scripts/generate_config.sh
```

`dags/config.ini` → `[minio] secret_key` phải khớp `MINIO_ROOT_PASSWORD` (lệch → `SignatureDoesNotMatch`).

> `config.ini` có thể chứa API key sau generate — **không commit**.

## 4. Facebook login (thủ công + lưu cookies)

Không bắt buộc `FB_TOTP_SECRET` nếu login tay. Flow:

1. Compose expose noVNC `7900`, `SE_VNC_PASSWORD=secret`.
2. Job `FEN_MANUAL_LOGIN=true` mở FB login, chờ người đăng nhập (kể cả 2FA), rồi lưu cookies.

```bash
# Quyền profile Chrome (seluser uid 1200)
docker exec -u root fen-exam-selenium-chrome-1 \
  sh -c 'chown -R 1200:1201 /data/chrome-profile'

make fb-login-manual
```

**Thao tác người dùng**

1. Mở http://localhost:7900 — password `secret`.
2. Đăng nhập Facebook bằng **acc của bạn** trong Chrome trên noVNC (2FA thủ công nếu có).
3. Job detect `logged_in=True` → kiểm tra group → upload cookies.

**Log thành công (ví dụ)**

```
saved N cookies to MinIO key=facebook/<FEN_GROUP_ID>/state/cookies.json slot=a
```

Mặc định group: `FEN_GROUP_ID=322453387859386` (có thể đổi trong `.env`).

Nếu login OK nhưng upload fail `SignatureDoesNotMatch`: `make config` rồi chạy lại `make fb-login-manual` (profile đã login → chỉ lưu cookies).

## 5. File liên quan (trong repo)

- `.env` / `.env.example` — quote `FEN_HOST_PROJECT_DIR`; MinIO `admin` / `admin1234`
- `scripts/up.sh` — bash 3.2 + path có space
- `docker-compose.yml` — `minio/mc:latest`, selenium `latest`, port `7900`, VNC env
- `docker-compose.minio-dags.yml` — `minio/mc:latest`
- `docker/paddle-ocr/Dockerfile` — copy `app.py`, `block_merge.py`, `text_layout.py`
- `dags/jobs/run_job.py` — `FEN_MANUAL_LOGIN` / chờ noVNC
- `Makefile` — target `fb-login-manual`

## 6. Bước tiếp theo

1. Airflow UI → **unpause** DAG (`fen_e2e_pipeline` hoặc `fen_crawl_pipeline`).
2. Trigger DAG với Configuration JSON (xem README).
3. Theo dõi task logs trên Airflow.

## Lệnh gọn

```bash
cp .env.example .env
make configure           # wizard FB + API key + FEN_HOST_PROJECT_DIR + config.ini
# (hoặc sửa .env tay rồi: make config)
make up
make fb-login-manual     # login FB tay trên :7900 → cookies MinIO
# → Airflow :8080 unpause + trigger DAG
```
