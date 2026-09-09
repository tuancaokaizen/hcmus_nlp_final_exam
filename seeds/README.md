# Seed JSONL input / Thư mục input JSONL

Drop a seed file here — rename to **`input.jsonl`** (no path param needed).

Đặt file seed vào đây — đổi tên thành **`input.jsonl`** (không cần truyền path).

## Quick start

```bash
cp seeds/input.example.jsonl seeds/input.jsonl
# edit seeds/input.jsonl — one JSON object per line
```

Trigger:

```json
{
  "group_id": "322453387859386",
  "source": "jsonl",
  "batch_target": 10,
  "ocr_limit": 3,
  "jsonl_skip_seen": false
}
```

Container path: `/opt/fen-exam/seeds/input.jsonl` (compose mounts `./seeds`).

DAG default **`jsonl_skip_seen=true`** — the example sets `false` so re-testing the same seed re-selects rows. Seen IDs are shared with crawl (`crawl/discover/seen_post_ids.json`).

## Files

| File | Tracked? | Purpose |
|------|----------|---------|
| `input.example.jsonl` | Yes | Schema sample |
| **`input.jsonl`** | **No** (gitignored) | Real input |
| `README.md` | Yes | This guide |

## Schema (one line = one post)

```json
{
  "post_id": "27433081289703229",
  "label": "caption / ground text",
  "images": ["https://scontent..../....jpg"]
}
```

Aliases: `label`|`caption`, `images`|`image_urls`|`cdn_urls`.  
`post_id`: plain digits **or** base64 Facebook `…VK:{id}`.

## Flow

```text
seeds/input.jsonl
  → fen_jsonl_ingest (decode + probe CDN)
      → live     → discover row with images → enrich calligraphy
                  (fetch bytes → Gemini → keep? → MinIO download)
      → expired  → incomplete row → Selenium enrich (permalink → new CDN)
                  → same calligraphy gate → download if keep
  → crawl_download (remaining)
  → label dual
```

No GraphQL discover. No catch-bottom rollover (`should_continue=false`).
