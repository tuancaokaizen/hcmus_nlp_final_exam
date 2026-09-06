# FEN NLP Exam Pipeline (Docker)

Pipeline chấm / demo đề NLP HCMUS: **crawl Facebook → lọc thư pháp → tải ảnh → OCR dual** (Gemini ∥ Paddle → fuse → GLM). Chỉ dùng **Docker Compose**.

---

## Người mới: làm gì trước?

1. Cài Docker + Compose; khuyến nghị cài thêm [`mc`](https://min.io/docs/minio/linux/reference/minio-client/minio-mc.html) (MinIO CLI).
2. Clone **`main`**, tạo `.env`, bật stack, login Facebook.
3. Trong Airflow: unpause **`fen_e2e_pipeline`** → Trigger (một batch nhỏ, không bắt đáy).

Chi tiết tiếng Việt: **[docs/USER_SETUP.md](docs/USER_SETUP.md)** · Lỗi / noVNC: **[docs/LOCAL_SETUP_LOG.md](docs/LOCAL_SETUP_LOG.md)**.

### Quick start

```bash
git clone https://github.com/tuancaokaizen/hcmus_nlp_final_exam.git
cd hcmus_nlp_final_exam
git checkout main && git pull
cp .env.example .env
make configure          # FB (optional) + calligraphy/OCR key
# Mở .env: điền FEN_LABEL_GEMINI/GPT/GLM(_DEEPSEEK)_API_KEY (có thể copy cùng key Ramcloud)
make up                 # lần đầu build có thể 10–20 phút
make fb-login-manual    # http://localhost:7900 (pass: secret); .env user/pass auto-fill → bạn chỉ 2FA
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

`ocr_limit: 3` = smoke (ít ảnh, rẻ API). Chạy hết queue OCR: bỏ `ocr_limit` hoặc `"ocr_limit": 0`.

| URL | Login |
|-----|-------|
| Airflow | http://localhost:8080 — `admin` / `admin` |
| MinIO | http://localhost:9001 — `admin` / `admin1234` |
| noVNC (FB) | http://localhost:7900 — `secret` |

---

## Pipeline làm gì? (1 phút)

```
discover → enrich (gate thư pháp) → download → label dual (OCR B2)
   posts mới      valid / invalid           ảnh MinIO     task_b2.jsonl
```

| Bước | Ý nghĩa |
|------|---------|
| **Discover** | Scroll group FB, lấy ~`batch_target` post **chưa seen** |
| **Enrich** | Đủ caption + ảnh? Có phải thư pháp viết tay? → `valid` / `invalid` |
| **Download** | Tải ảnh post **valid** lên MinIO |
| **Label dual** | OCR từng ảnh: Gemini ∥ Paddle → fuse → GLM → file nộp B2 |

**Skip tự động:** post đã `seen` không crawl lại; ảnh đã có page OCR không OCR lại (trừ `force: true`).

---

## Chọn DAG nào? (quan trọng)

Tên `fen_e2e_pipeline` = **end-to-end một lần** (crawl + OCR trong cùng DAG).  
Nó **không** có bắt đáy / rollover — hiểu đúng là **một batch thủ công**.

| DAG | Khi nào dùng | Một batch? | Bắt đáy? |
|-----|--------------|------------|----------|
| **`fen_e2e_pipeline`** | **Người mới / chấm smoke** — full path một phát | Có (`batch_target`) | **Không** (không có `catch_bottom`) |
| **`fen_crawl_pipeline`** | Crawl lâu / nhiều batch | Mỗi run = 1 batch | **Có** — mặc định `catch_bottom: true` (rollover) |
| **`fen_label_dual_pipeline`** | Đã có ảnh trên MinIO, chỉ OCR lại | — | — |
| `fen_ocr_pipeline` | Legacy — **không** dùng cho B2 mới | — | — |

**Gợi ý:** luôn bắt đầu bằng `fen_e2e_pipeline`. Chỉ mở `fen_crawl_pipeline` khi cần crawl sâu; lần đầu nên `"catch_bottom": false`.

### Trigger mẫu

**E2E — một batch (khuyến nghị):**

```json
{
  "group_id": "322453387859386",
  "batch_target": 10,
  "reset_crawl_data": true,
  "ocr_limit": 0,
  "flush_posts": 5
}
```

**Crawl — một batch rồi dừng** (không bắt đáy):

```json
{
  "group_id": "322453387859386",
  "batch_target": 10,
  "reset_crawl_data": true,
  "catch_bottom": false
}
```

**Crawl — bắt đáy** (mặc định nếu không set; có thể rất lâu):

```json
{
  "group_id": "322453387859386",
  "batch_target": 10,
  "catch_bottom": true
}
```

**Chỉ label dual:**

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

`batch_seq: 0` = mọi queue ảnh (khuyến nghị). `force: true` = OCR lại ảnh đã xong.

---

## Tham số dễ nhầm

| Param | Đơn vị | Nghĩa | Default |
|-------|--------|-------|---------|
| **`batch_target`** | **post** / batch crawl | Discover lấy tối đa bao nhiêu post mới | `10` |
| **`ocr_limit`** / `label_limit` | **ảnh** pending | Cap OCR; `0` = hết queue | `0` |
| **`flush_posts`** | **ảnh** | Upsert `task_b2` mỗi N ảnh | `5` |
| **`catch_bottom`** | bool | Chỉ trên **`fen_crawl_pipeline`** | `true` |
| **`reset_crawl_data`** | bool | Xóa state crawl (giữ cookies) trước run | `false` |

Không có param tên `batch_size` — dùng **`batch_target`**.

---

## Setup / vận hành

### Requirements

- Docker + Compose v2  
- [`mc`](https://min.io/docs/minio/linux/reference/minio-client/minio-mc.html) khi `FEN_DAG_SOURCE=minio` (mặc định)  
- Key Ramcloud trong `.env` (không commit)  
- Tài khoản Facebook (login tay qua noVNC nếu 2FA)

### Cập nhật code đã clone

```bash
git checkout main && git pull origin main
make deploy
```

Rebuild image lần đầu / đổi Dockerfile: `make bootstrap`.

### Sau khi sửa gì thì chạy gì

| Đổi | Lệnh |
|-----|------|
| DAG / `config.ini` | `make deploy` (chờ ~30s sync DAG nếu mode MinIO) |
| Job Python `dags/jobs/` | `FEN_UP_BUILD=always make up` hoặc `make deploy` |
| `.env` API keys | `make config` rồi `make deploy` |

### Stop / reset

| Lệnh | Containers | Data volumes |
|------|------------|--------------|
| `make down` | Tắt | **Giữ** (MinIO, cookies, DB) |
| `make down-v` | Tắt | **Xóa hết** — login FB lại |

```bash
make down-v && make up && make fb-login-manual
```

### `make up` làm gì

Chuẩn bị workspace → build image (nếu thiếu) → MinIO/Postgres/Airflow/Paddle/Selenium → bucket + deploy DAG → Airflow sẵn sàng.

| Mode | Setting | DAG lấy từ đâu |
|------|---------|----------------|
| Mặc định | `FEN_DAG_SOURCE=minio` | MinIO → sidecar |
| Dev nhanh | `make up-dev` | Bind mount `./dags` |

### Make cheat sheet

| Target | Việc |
|--------|------|
| `make configure` | Wizard `.env` + `config.ini` |
| `make up` / `make down` / `make down-v` | Bật / tắt / wipe |
| `make deploy` | Bucket + sync DAG + rebuild `fen-job` |
| `make fb-login-manual` | Login FB tay (noVNC) |
| `make verify` | Kiểm tra artifact trên MinIO |
| `make e2e` | deploy + fb-login; trigger `fen_e2e_pipeline` trên UI |

---

## Output & kiến trúc ngắn

**MinIO** bucket `final-exam-nlp-raw`:

```
facebook/{group_id}/
  crawl/…          # checkpoint, seen, cookies
  export/          # valid_post.jsonl / invalid_post.jsonl  (B1)
  images/…         # ảnh đã tải
  ocr/label_dual_pilot/   # task_b2.jsonl, glm/recommend.jsonl (fuse_gt), pages/
```

**Nộp local (B2 thuần 5 cột):** `output/{group_id}/task_b2.jsonl` + `.xlsx`.

| Stage | Job | Key (`config.ini`) |
|-------|-----|---------------------|
| Discover | `fen_crawl_discover` | — |
| Enrich + calligraphy | `fen_crawl_enrich` | `[fen_calligraphy]` |
| Download | `fen_crawl_download` | — |
| Label dual | `fen_label_dual` | `[fen_label_gemini]`, `[fen_label_gpt]`, `[fen_label_glm]` + Paddle |

Chi tiết file B2: **[docs/LABEL_DUAL_OUTPUT.md](docs/LABEL_DUAL_OUTPUT.md)**.

---

## Docs

| Doc | Nội dung |
|-----|----------|
| [USER_SETUP.md](docs/USER_SETUP.md) | Setup + trigger (tiếng Việt) |
| [LOCAL_SETUP_LOG.md](docs/LOCAL_SETUP_LOG.md) | `.env`, noVNC, lỗi thường gặp |
| [LABEL_DUAL_OUTPUT.md](docs/LABEL_DUAL_OUTPUT.md) | Vai trò từng model OCR (§0) + output, flush, skip |
| [CRAWL_STATE.md](docs/CRAWL_STATE.md) | Checkpoint / seen |
| [GRADER_GUIDE.md](docs/GRADER_GUIDE.md) | Checklist chấm |
| [PIPELINE_BUILD_DEPLOY_RUN.md](docs/PIPELINE_BUILD_DEPLOY_RUN.md) | Build / deploy sâu |
