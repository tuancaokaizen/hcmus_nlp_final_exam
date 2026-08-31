# User setup

Hướng dẫn cài đặt và chạy pipeline exam: **crawl → calligraphy gate → download → label dual OCR** (Gemini ∥ Paddle → fuse → GLM).

- Setup lần đầu: **[LOCAL_SETUP_LOG.md](LOCAL_SETUP_LOG.md)**
- Chi tiết file output: **[LABEL_DUAL_OUTPUT.md](LABEL_DUAL_OUTPUT.md)**

---

## Lần đầu (checklist)

1. Cài **Docker** + Docker Compose v2 + (khuyến nghị) MinIO client [`mc`](https://min.io/docs/minio/linux/reference/minio-mc.html) trên host
2. Clone repo → `cp .env.example .env` → `make configure`
3. Mở `.env`, điền **`FEN_LABEL_GEMINI_API_KEY`**, **`FEN_LABEL_GPT_API_KEY`**, **`FEN_LABEL_GLM_API_KEY`** (wizard chỉ hỏi calligraphy + OCR — có thể copy cùng key Ramcloud)
4. `make config` → `make up` (lần đầu build có thể 10–20 phút)
5. `make fb-login-manual` → mở http://localhost:7900 (pass `secret`) → login Facebook tay
6. Airflow http://localhost:8080 (`admin`/`admin`) → unpause **`fen_e2e_pipeline`** → Trigger

Smoke JSON (rẻ API):

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
| Airflow | http://localhost:8080 — `admin` / `admin` |
| MinIO | http://localhost:9001 — `admin` / `admin1234` |
| noVNC | http://localhost:7900 — `secret` |

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

**Wizard hỏi gì?** Facebook (có thể Enter bỏ qua) + `FEN_CALLIGRAPHY_API_KEY` + `FEN_OCR_API_KEY`.  
**Bạn vẫn phải điền tay** các `FEN_LABEL_*_API_KEY` trong `.env`, rồi `make config`.

| Key trong `.env` | Dùng cho |
|------------------|----------|
| `FEN_CALLIGRAPHY_API_KEY` | Enrich — gate thư pháp |
| `FEN_LABEL_GEMINI_API_KEY` | Label dual — vision |
| `FEN_LABEL_GPT_API_KEY` | Label dual — GPT track |
| `FEN_LABEL_GLM_API_KEY` | Label dual — GLM / `fuse_gt` |
| `FEN_LABEL_DEEPSEEK_API_KEY` | Label dual (optional) |
| `FEN_OCR_API_KEY` | Chỉ **legacy** `fen_ocr_pipeline` |

`configure` set `FEN_HOST_PROJECT_DIR` = đường dẫn tuyệt đối repo (bắt buộc; path có space → giữ quote).

**Đã clone rồi — cập nhật code:**

```bash
git checkout main
git pull origin main
make config && make deploy
```

---

## 2. Bootstrap stack

```bash
make up                 # dirs + config.ini + compose + buckets + deploy DAGs + Airflow
make fb-login-manual    # khuyến nghị nếu có 2FA
# hoặc: make fb-login   # nếu đã điền FB_PASSWORD (+ TOTP)
```

Force rebuild images:

```bash
make bootstrap   # = FEN_UP_BUILD=always make up
```

| Mode | Setting | Khi nào |
|------|---------|---------|
| Default | `FEN_DAG_SOURCE=minio` | Chấm / máy mới (cần `mc`) |
| Dev | `make up-dev` | Sửa DAG thường xuyên, không cần sync MinIO |

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
| **`fen_e2e_pipeline`** | Một lần: crawl batch → label dual (không rollover) — **bắt đầu ở đây** |
| **`fen_crawl_pipeline`** | Crawl nhiều batch + auto label dual; `catch_bottom` mặc định `true` (lâu) |
| **`fen_label_dual_pipeline`** | Chỉ label dual (đã crawl xong) |
| `fen_ocr_pipeline` | Legacy OCR — không dùng cho B2 mới |

Với `fen_crawl_pipeline` lần đầu: set `"catch_bottom": false` để chỉ một batch.

---

## 5. Trigger config (Configuration JSON)

### `batch_target` vs `ocr_limit` vs `flush_posts`

| Param | Giai đoạn | Đơn vị | Default |
|-------|-----------|--------|---------|
| **`batch_target`** | Crawl discover | **post** / batch | `10` |
| **`ocr_limit`** | Label dual | **ảnh** pending tối đa; `0` = full queue | `0` |
| **`flush_posts`** | Label dual | Upsert jsonl mỗi N **ảnh** | `5` |

- **`ocr_limit=0`**: xử lý hết queue pending.
- **`ocr_limit=3`**: smoke test — chỉ vài ảnh đầu.
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

- `batch_seq`: không cần — e2e OCR hết quote queue (`batch_seq=0` nội bộ)
- `reset_crawl_data`: xóa state crawl (giữ cookies)

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

- **`catch_bottom`**: default `true` = bắt đáy đến `bottom_year` (có thể rất lâu); `false` = một batch rồi dừng
- Sau download → tự trigger **`fen_label_dual_pipeline`**

### `fen_label_dual_pipeline`

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

`batch_seq`: **0** = mọi quote queue 1–12 (khuyến nghị). `1`–`12` = chỉ một shard (dễ chạy nhầm queue rỗng nếu ít ảnh).

**Output:** `ocr/label_dual_pilot/task_b2.jsonl`, `task_b2.xlsx`, `glm/recommend.jsonl` (`fuse_gt`).

---

## 6. Facebook login

```bash
make fb-login-manual   # noVNC :7900 — khuyến nghị khi có 2FA
make fb-login          # auto (cần password / TOTP trong .env)
```

---

## 7. Sau khi sửa code

```bash
make deploy
```

Job Python đổi nhiều: `FEN_UP_BUILD=always make up`

---

## 8. Stop / reset

| Command | Data |
|---------|------|
| `make down` | Giữ MinIO, Postgres, cookies |
| `make down-v` | Xóa hết volumes — cần login FB lại |

```bash
make down-v && make up && make fb-login-manual
```
