# User setup

Hướng dẫn cài đặt và chạy pipeline exam: **crawl → calligraphy gate → download → label dual OCR** (Gemini ∥ Paddle → fuse → GLM).

Chi tiết file output / upsert / flush: **[LABEL_DUAL_OUTPUT.md](LABEL_DUAL_OUTPUT.md)**.

---

## 1. Clone & configure

> **Nhánh dùng cho thi / chấm:** **`main`** — không checkout nhánh `features/*`.

```bash
git clone https://github.com/tuancaokaizen/hcmus_nlp_final_exam.git
cd hcmus_nlp_final_exam
git checkout main
git pull origin main
cp .env.example .env
make configure
```

**Đã clone rồi — cập nhật code:**

```bash
git checkout main
git pull origin main
make config && make deploy
```

Wizard hỏi:

| Input | Dùng cho |
|-------|----------|
| Facebook (`FB_USERNAME`, `FB_PASSWORD`) | Crawl (sau `make fb-login`) |
| `FEN_CALLIGRAPHY_API_KEY` | Enrich — gate thư pháp |
| `FEN_LABEL_GEMINI_API_KEY` | Label dual — vision |
| `FEN_LABEL_GPT_API_KEY` | Label dual — GPT track |
| `FEN_LABEL_GLM_API_KEY` | Label dual — GLM / `fuse_gt` |
| `FEN_LABEL_DEEPSEEK_API_KEY` | Label dual (optional) |
| `FEN_OCR_API_KEY` | Chỉ **legacy** `fen_ocr_pipeline` |

`configure` set `FEN_HOST_PROJECT_DIR` = đường dẫn tuyệt đối repo (bắt buộc cho DockerOperator).

---

## 2. Bootstrap stack

```bash
make configure   # lần đầu
make up          # đủ: dirs + config.ini + compose + buckets + deploy DAGs + Airflow
make fb-login    # cookie Facebook → MinIO
```

| URL | Login |
|-----|-------|
| Airflow | http://localhost:8080 — `admin` / `admin` |
| MinIO | http://localhost:9001 — `admin` / `admin` |

Force rebuild images:

```bash
make bootstrap   # = FEN_UP_BUILD=always make up
```

| Mode | Setting | DAG source |
|------|---------|------------|
| Prod-like | `FEN_DAG_SOURCE=minio` | MinIO → sidecar sync |
| Dev | `make up-dev` hoặc `FEN_DAG_SOURCE=local` | Bind mount `./dags` |

---

## 3. Vòng test hàng ngày

```bash
make up
make deploy      # sau khi sửa DAG/job
# Airflow UI → unpause → Trigger DAG
make verify
```

---

## 4. Chọn DAG nào?

| DAG | Mô tả ngắn |
|-----|-------------|
| **`fen_e2e_pipeline`** | Một lần: crawl batch → label dual (không rollover) |
| **`fen_crawl_pipeline`** | Crawl đầy đủ + auto label dual + rollover (`catch_bottom`) |
| **`fen_label_dual_pipeline`** | Chỉ label dual (đã crawl xong) |
| `fen_ocr_pipeline` | Legacy OCR — không dùng cho B2 mới |

---

## 5. Trigger config (Configuration JSON)

### `batch_target` vs `ocr_limit` vs `flush_posts`

| Param | Giai đoạn | Đơn vị | Default |
|-------|-----------|--------|---------|
| **`batch_target`** | Crawl discover | **post** / batch | `10` |
| **`ocr_limit`** | Label dual | **ảnh** pending tối đa; `0` = full queue | `0` |
| **`flush_posts`** | Label dual | Upsert jsonl mỗi N **ảnh** | `5` |

- **`ocr_limit=0`**: xử lý hết queue pending (không cap 300 ảnh/chunk).
- **`flush_posts=5`**: mỗi 5 ảnh ghi upsert `task_b2.jsonl`, `flags.jsonl`, `glm/recommend.jsonl`.
- Ảnh đã label xong được **skip** (trừ `force: true`).

### `fen_e2e_pipeline`

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

- `batch_seq`: không cần — auto sau crawl
- `reset_crawl_data`: xóa state crawl (giữ cookies)
- Smoke test: `"ocr_limit": 3`, `"flush_posts": 2`

### `fen_crawl_pipeline`

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

- **`catch_bottom`**: default `true` = bắt đáy đến `bottom_year`; `false` = một batch rồi dừng
- Sau download → tự trigger **`fen_label_dual_pipeline`**
- Label dual và rollover chạy **song song** (crawl batch tiếp không chờ OCR)

### `fen_label_dual_pipeline`

```json
{
  "group_id": "322453387859386",
  "batch_seq": 1,
  "label_limit": 0,
  "flush_posts": 5,
  "prepare_queues": true,
  "glm": true,
  "force": false
}
```

**Output:** `ocr/label_dual_pilot/task_b2.jsonl`, `task_b2.xlsx`, `glm/recommend.jsonl` (`fuse_gt`).

### `fen_ocr_pipeline` (legacy)

```json
{
  "group_id": "322453387859386",
  "batch_seq": 1,
  "ocr_limit": 0,
  "force": false
}
```

---

## 6. Facebook login

```bash
make fb-login
```

Headed nếu cần:

```bash
FEN_HEADLESS=false docker compose --profile job run --rm -e FEN_JOB=fen_bootstrap_login fen-job
```

---

## 7. Sau khi sửa code

```bash
make deploy
```

Job Python: `FEN_UP_BUILD=always make up`

Upstream sync:

```bash
make sync && make config && make deploy
```

---

## 8. Stop / reset

| Command | Data |
|---------|------|
| `make down` | Giữ MinIO, Postgres, cookies |
| `make down-v` | Xóa hết volumes — cần `make fb-login` lại |

```bash
make down-v && make up && make fb-login
```
