# User setup

Hướng dẫn cài và chạy pipeline exam (tiếng Việt): **crawl → gate thư pháp → download → label dual OCR**.

- Lỗi / noVNC chi tiết: **[LOCAL_SETUP_LOG.md](LOCAL_SETUP_LOG.md)**
- File output B2 + **vai trò từng model OCR**: **[LABEL_DUAL_OUTPUT.md](LABEL_DUAL_OUTPUT.md)** (§0)
- README ngắn (EN/VI mix): **[../README.md](../README.md)**

---

## Đọc nhanh: từng model OCR làm gì?

| Model | Việc |
|-------|------|
| **Gemini** | Nhánh A — OCR vision chính → cột `gemini` |
| **Paddle** | Nhánh B — OCR local, song song Gemini |
| **GPT** | Refine từng nhánh trước khi fuse |
| **DeepSeek** | Optional — thứ tự cột / layout |
| **Fuse** | Ghép A∥B → `ground_truth` nộp |
| **GLM** | Judge QC → silver / cần HITL (`fuse_gt`) |
| **Calligraphy (Gemini)** | Chỉ gate crawl (thư pháp?) — **không** phải OCR B2 |

Chi tiết + sơ đồ: **[LABEL_DUAL_OUTPUT.md §0](LABEL_DUAL_OUTPUT.md#0-công-dụng-từng-mô-hình-ocr--label-dual)**.

---

## Đọc nhanh: DAG là gì?

Bạn **không** cần đổi tên DAG. Nhớ bảng này:

| DAG | Nghĩa dễ hiểu | Bắt đáy? |
|-----|----------------|----------|
| **`fen_e2e_pipeline`** | **Một batch** crawl + OCR full path — **bắt đầu ở đây** | **Không** (không có param `catch_bottom`) |
| **`fen_crawl_pipeline`** | Crawl (có thể nhiều batch) + tự gọi OCR | **Có** — mặc định `true` (rollover tới `bottom_year`) |
| **`fen_label_dual_pipeline`** | Chỉ OCR khi đã có ảnh trên MinIO | — |
| `fen_ocr_pipeline` | Legacy — bỏ qua nếu làm B2 mới | — |

`e2e` = end-to-end (cùng DAG làm hết crawl→OCR), **không** có nghĩa “chạy production bắt đáy”.  
Muốn một batch trên `fen_crawl_pipeline` → set `"catch_bottom": false`.

Luồng bên trong mỗi lần crawl:

```
discover → enrich → download → (label dual)
  posts     valid?     ảnh        OCR B2
```

- Post đã **seen** → skip crawl lại.  
- Ảnh đã OCR (có page) → skip (trừ `force: true`).

---

## Lần đầu (checklist)

1. Cài **Docker** + Compose v2 + (khuyến nghị) [`mc`](https://min.io/docs/minio/linux/reference/minio-client/minio-mc.html)
2. Clone **`main`** → `cp .env.example .env` → `make configure`
3. Mở `.env`, điền **`FEN_LABEL_GEMINI_API_KEY`**, **`FEN_LABEL_GPT_API_KEY`**, **`FEN_LABEL_GLM_API_KEY`** (wizard chỉ hỏi calligraphy + OCR — copy cùng key Ramcloud được)
4. `make config` → `make up` (lần đầu build có thể 10–20 phút)
5. `make fb-login-manual` → terminal in URL **http://localhost:7900** (pass `secret`). Có `FB_USERNAME`/`FB_PASSWORD` trong `.env` thì tự điền; bạn chỉ làm **2FA** trên noVNC.
6. Airflow http://localhost:8080 (`admin`/`admin`) → **unpause** `fen_e2e_pipeline` → **Trigger**

Smoke (rẻ API):

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

> Nhánh chấm / nộp: **`main`**. Không dùng `features/*` khi chấm.

```bash
git clone https://github.com/tuancaokaizen/hcmus_nlp_final_exam.git
cd hcmus_nlp_final_exam
git checkout main && git pull origin main
cp .env.example .env
make configure
```

**Wizard hỏi:** Facebook (Enter = bỏ qua) + `FEN_CALLIGRAPHY_API_KEY` + `FEN_OCR_API_KEY`.  
**Phải điền tay:** các `FEN_LABEL_*_API_KEY`, rồi `make config`.

| Key `.env` | Dùng cho |
|------------|----------|
| `FEN_CALLIGRAPHY_API_KEY` | Enrich — gate thư pháp |
| `FEN_LABEL_GEMINI_API_KEY` | Label dual — vision |
| `FEN_LABEL_GPT_API_KEY` | Label dual — GPT |
| `FEN_LABEL_GLM_API_KEY` | Label dual — GLM / `fuse_gt` |
| `FEN_LABEL_DEEPSEEK_API_KEY` | Label dual (optional) |
| `FEN_OCR_API_KEY` | Chỉ legacy `fen_ocr_pipeline` |

`configure` set `FEN_HOST_PROJECT_DIR` = path tuyệt đối repo (path có space → giữ quote).

Đã clone — chỉ cập nhật:

```bash
git checkout main && git pull origin main
make config && make deploy
```

---

## 2. Bật stack + login FB

```bash
make up
make fb-login-manual    # khuyến nghị nếu 2FA
# hoặc: make fb-login   # nếu đã có FB_PASSWORD (+ TOTP)
```

Force rebuild image: `make bootstrap`.

| Mode | Khi nào |
|------|---------|
| Default (`FEN_DAG_SOURCE=minio`) | Chấm / máy mới (cần `mc`) |
| `make up-dev` | Sửa DAG thường xuyên |

---

## 3. Vòng làm việc hàng ngày

```bash
make up
make deploy      # sau khi sửa DAG/job
# Airflow → unpause → Trigger
make verify
```

---

## 4. Trigger config (Configuration JSON)

### Tham số dễ nhầm

| Param | Giai đoạn | Đơn vị | Default |
|-------|-----------|--------|---------|
| **`batch_target`** | Discover | **post** / batch | `10` |
| **`ocr_limit`** | Label dual (e2e / crawl) | **ảnh** pending; `0` = full | `0` |
| **`label_limit`** | Label dual DAG | giống `ocr_limit` | `0` |
| **`flush_posts`** | Label dual | upsert mỗi N **ảnh** | `5` |
| **`catch_bottom`** | **Chỉ** `fen_crawl_pipeline` | bool | `true` |
| **`reset_crawl_data`** | Crawl | xóa state crawl, giữ cookies | `false` |
| **`force`** | Label dual | OCR lại ảnh đã xong | `false` |

Không có `batch_size` — dùng **`batch_target`**.

### `fen_e2e_pipeline` — một batch, không bắt đáy

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

- Luôn chỉ **một** batch discover theo `batch_target`, rồi enrich → download → label dual.
- Không có `catch_bottom` / rollover.
- OCR dùng hết quote queue nội bộ (`batch_seq=0`); không cần truyền `batch_seq`.
- `prepare_queues` so sánh tập ảnh với `valid_post`: **có ảnh mới → tự rebuild** `quote_01..12` (không còn skip vì queue stale). Ảnh đã OCR vẫn skip theo page sidecar.

### `fen_crawl_pipeline` — crawl + (tuỳ chọn) bắt đáy

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

- **`catch_bottom: false`** — giống tinh thần e2e về số batch crawl: **một** batch rồi dừng.
- **`catch_bottom: true`** (mặc định) — sau mỗi batch, cooldown rồi **trigger lại** crawl tới `bottom_year` (2013) hoặc hết feed → **lâu**.
- Sau download → tự trigger **`fen_label_dual_pipeline`**.
- `demo_mode: true` (cũ) = `catch_bottom: false`.

### `fen_label_dual_pipeline` — chỉ OCR

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

- `batch_seq: 0` = mọi queue 1–12 (khuyến nghị). `1`–`12` = một shard (dễ rỗng nếu ít ảnh).
- Output: `ocr/label_dual_pilot/task_b2.jsonl` (+ `.xlsx`), `glm/recommend.jsonl` (`fuse_gt`).
- Nộp local: `output/{group_id}/task_b2.jsonl` + `.xlsx`.

---

## 5. Valid / invalid (tóm tắt)

Post **valid** khi:

1. Có caption thật (không phải UI Facebook) + ≥ 1 URL **ảnh** (video không tính → `missing_image`).
2. Gate thư pháp: ≥ 1 ảnh **handwritten / mixed** (không nhận printed-only / spam).
3. Xuất B1: thêm ảnh đã tải về MinIO.

Chi tiết reason: xem log enrich / `invalid_post.jsonl`.

---

## 6. Facebook login

```bash
make fb-login-manual   # noVNC :7900
make fb-login          # auto nếu có password/TOTP trong .env
```

---

## 7. Sau khi sửa code

```bash
make deploy
```

Đổi nhiều job Python: `FEN_UP_BUILD=always make up`

---

## 8. Stop / reset

| Command | Data |
|---------|------|
| `make down` | Giữ MinIO, Postgres, cookies |
| `make down-v` | Xóa volumes — login FB lại |

```bash
make down-v && make up && make fb-login-manual
```
