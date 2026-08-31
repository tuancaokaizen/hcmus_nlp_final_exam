"""Crawl Facebook group posts into MinIO (logs + capped images).

Crawl bài viết Facebook group lên MinIO (log đầy đủ + ảnh có trần).

Layout:
    facebook/{group_id}/meta/group.json
    facebook/{group_id}/state/progress.json|posts_index.jsonl|cookies.json
    facebook/{group_id}/logs/posts.jsonl
    facebook/{group_id}/logs/by_run/{run_id}/upserts.jsonl|result.json
    facebook/{group_id}/images/{post_id}/{n}.jpg
    facebook/{group_id}/export/valid_post.jsonl|invalid_post.jsonl
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import random
import re
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Any

from common.chau_ban_schema import utc_now_iso
from common.config import get_value, load_config
from common.io_storage import (
    ensure_bucket,
    get_minio_client,
    list_objects_with_prefix,
    object_exists,
    read_object_text,
    upload_binary_payload,
    upload_json_payload,
    upload_text_payload,
)

SCHEMA_VERSION = "4.4"
SOURCE_NAME = "facebook_group"
# Light defaults to reduce Meta rate-limit risk / Mặc định nhẹ để giảm nguy cơ bị Meta chặn
DEFAULT_MEDIA_BATCH_SIZE = 50
DEFAULT_SCROLL_PAUSE_SEC = 4.5
DEFAULT_PERMALINK_PAUSE_SEC = 3.5
DEFAULT_MATCH_SOFT_RESTART_EVERY = 10
DEFAULT_MEDIA_SOFT_RESTART_EVERY = 5
POST_SELECTORS = (
    "div[role='feed'] > div:not([data-fen-pruned='1']), "
    "div[role='article']:not([data-fen-pruned='1'])"
)
FB_BASE_URL = "https://www.facebook.com"
FB_LOGIN_URL = f"{FB_BASE_URL}/login"
IMAGE_DIR_NAME = "images"
PASSWORD_WAIT_SEC = 15
LOGIN_WAIT_SEC = 45
PRUNE_KEEP_LAST = 15
BROWSER_RESTART_EVERY_BATCHES = 2
SESSION_RECOVERY_LIMIT = 3
DEFAULT_TARGET_VALID_DELTA = 30
DEFAULT_MAX_IMAGE_VALID = 20_000
DEFAULT_BOTTOM_YEAR = 2013
# Cooldown between Airflow rollover runs (seconds) / Nghỉ giữa các lần rollover Airflow
DEFAULT_ROLLOVER_COOLDOWN_SEC = 180  # 3 phút / 3 minutes
DEFAULT_GALLERY_WALK_COUNT = 500
DEFAULT_INVALID_RECHECK_LIMIT = 50
DEFAULT_INVALID_RECHECK_MAX_ATTEMPTS = 3
# Max skipped (viewer-broken) tiles to recheck per run / Số tile skip (viewer gãy) recheck mỗi run
DEFAULT_SKIPPED_RECHECK_LIMIT = 20
DEFAULT_SKIPPED_RECHECK_MAX_ATTEMPTS = 3
# JS heap soft limit before cache clear (pod hard-capped at 4Gi) /
# Ngưỡng mềm JS heap trước khi clear cache (pod trần cứng 4Gi)
SELENIUM_RAM_SOFT_MB = 2000.0
# Hard stop scroll/harvest near pod RAM ceiling / Dừng scroll/harvest gần trần RAM pod
SELENIUM_RAM_HARD_STOP_MB = 2500.0
# Adaptive Media harvest rounds across rollovers /
# Số round harvest Media thích ứng qua các lần rollover
DEFAULT_MEDIA_HARVEST_ROUNDS = 120
MEDIA_HARVEST_ROUNDS_STEP = 40
MEDIA_HARVEST_ROUNDS_MAX = 250
# Slack px to treat Media as scrolled to bottom / Nới px coi như đã cuộn tới đáy Media
MEDIA_BOTTOM_SLACK_PX = 900
# Permanent invalid after max recheck tries / Invalid vĩnh viễn sau đủ số lần recheck
INVALID_RECHECK_EXHAUSTED_REASON = "recheck_exhausted"
# Invalid reasons eligible for photo-first recheck / Lý do invalid được recheck photo-first
RECHECKABLE_INVALID_REASONS = frozenset(
    {
        "missing_image",
        "missing_caption",
        "missing_label",
        "missing_label_and_image",
        "redirect_homepage",
        "gallery_extract_failed",
    }
)
DEFAULT_GALLERY_BASELINE_FBID = "122202424946469042"
DEFAULT_GALLERY_STEP_PAUSE_SEC = 4.5
DEFAULT_GALLERY_DIRECTION = "right"

EMAIL_INPUT_SELECTORS = (
    "input[name='email']",
    "input[type='email']",
    "input[id='email']",
)
PASSWORD_INPUT_SELECTORS = (
    "input[name='pass']",
    "input[type='password']",
    "input[id='pass']",
)
TOTP_CODE_SELECTORS = (
    "input[name='approvals_code']",
    "input[autocomplete='one-time-code']",
    "input[inputmode='numeric']",
    "input[name='code']",
)
SUBMIT_BUTTON_XPATHS = (
    "//button[@id='checkpointSubmitButton']",
    "//div[@role='button'][.//span[normalize-space()='Continue']]",
    "//div[@role='button'][.//span[normalize-space()='Tiếp tục']]",
    "//button[normalize-space()='Continue']",
    "//button[normalize-space()='Tiếp tục']",
)
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
TINY_IMAGE_MARKERS = ("/p24x24/", "/p32x32/", "/p50x50/", "/s32x32/", "/s60x60/", "/p64x64/")
# Facebook video / reel URL markers — never treat as photos /
# Dấu hiệu URL video / reel Facebook — không coi là ảnh
VIDEO_URL_MARKERS = (
    "/videos/",
    "/video/",
    "/watch/",
    "/reel/",
    "/reels/",
    "video_id=",
    "video.php",
    "/live/",
    "/watchparty/",
)
CAPTION_SELECTORS = (
    "div[data-ad-rendering-role='story_message']",
    "div[data-ad-comet-preview='message']",
    "div[data-ad-preview='message']",
    "div[data-testid='post_message']",
)
FEED_GATE_MARKERS = (
    "join group",
    "tham gia nhóm",
    "this content isn't available",
    "nội dung này hiện không có",
    "you must log in",
    "bạn phải đăng nhập",
    "visible to members",
    "chỉ thành viên",
)
UI_NOISE_TEXT = {
    "like", "comment", "share", "see more", "see less", "all reactions",
    "thích", "bình luận", "chia sẻ", "xem thêm", "xem bớt", "tất cả cảm xúc",
    "xem bản dịch", "see translation", "viết bản dịch", "write a translation",
    "người đóng góp nổi bật", "top contributor", "most relevant", "liên quan nhất",
    "follow", "theo dõi", "message", "nhắn tin", "join", "tham gia",
    "invite", "mời", "featured", "đáng chú ý", "photos", "ảnh",
    "xem bài viết", "view post", "see post",
    "hãy là người đầu tiên bình luận", "be the first to comment",
    "write a comment", "viết bình luận", "write comment",
    "bình luận đầu tiên", "first comment", "no comments yet",
    "chưa có bình luận", "react", "phản ứng",
    # Group privacy badge (not post body) / Badge quyền riêng tư nhóm (không phải nội dung bài)
    "nhóm công khai", "public group", "nhóm riêng tư", "private group",
    "nhóm kín", "closed group", "nhóm bí mật", "secret group",
}
# Regex patterns for Facebook engagement chrome / Pattern regex cho UI tương tác Facebook
UI_NOISE_REGEX = (
    r"^\d+[\s,.]*(lượt thích|likes?|reactions?)\b",
    r"^\d+[\s,.]*(bình luận|comments?|shares?|chia sẻ)\b",
    r"^(hãy là|be the)\s+(người|first)\s",
    r"\b(lượt thích|người thích|people reacted|reacted to this)\b",
    r"\b(đã bình luận|commented on this|view insights)\b",
    r"^(photo|ảnh|video)\s*(\d+\s*of|\/)",
    # Relative timestamps / Thời gian tương đối (vd. "4 giờ", "2 hours")
    r"^\d+\s*(giờ|phút|ngày|tuần|tháng|năm|hours?|minutes?|days?|weeks?|months?|years?)\b",
    # Group privacy / visibility chrome mistaken as caption /
    # UI quyền riêng tư nhóm bị nhầm thành caption
    r"bất kỳ ai cũng có thể",
    r"nhìn thấy mọi người trong nhóm",
    r"những gì họ đăng",
    r"anyone can see (everyone|who.s|all members)",
    r"what.?s posted in (the )?group",
    r"visible to (all )?group members",
    r"chỉ thành viên (nhóm )?mới (xem|thấy)",
    r"^nhóm\s+(công khai|riêng tư|kín|bí mật)\b",
    r"^(public|private|closed|secret)\s+group\b",
)


def _is_recheckable_invalid(reason: str | None) -> bool:
    """True when a failed extract may succeed on recheck / True khi recheck có thể thành công."""
    text = (reason or "").strip().lower()
    if not text:
        return False
    if text == INVALID_RECHECK_EXHAUSTED_REASON:
        return False
    if text in RECHECKABLE_INVALID_REASONS:
        return True
    return text.startswith("missing_")


def _recheck_attempt_count(record: dict[str, Any]) -> int:
    """How many invalid_recheck runs already tried this post / Số lần recheck đã thử."""
    try:
        return max(0, int(record.get("recheck_attempts") or 0))
    except (TypeError, ValueError):
        return 0


def _is_recheck_eligible(record: dict[str, Any], *, max_attempts: int) -> bool:
    """Skip exhausted or over-limit posts / Bỏ qua post đã hết lượt recheck."""
    if record.get("recheck_exhausted"):
        return False
    if _recheck_attempt_count(record) >= max(1, max_attempts):
        return False
    return _is_recheckable_invalid(str(record.get("invalid_reason") or ""))


def _now_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def _settings() -> dict[str, str]:
    cfg = load_config()
    # Per-worker overrides (seed2 / Selenium B) / Ghi đè theo worker (seed2 / Selenium B)
    src = os.environ.get("FEN_SOURCE_PREFIX", "").strip()
    if src:
        os.environ["FEN_FINAL_EXAM_NLP_SOURCE_PREFIX"] = src
    remote = os.environ.get("FEN_SELENIUM_REMOTE_URL", "").strip()
    if remote:
        os.environ["FEN_FINAL_EXAM_NLP_SELENIUM_REMOTE_URL"] = remote
    return {
        "bucket_raw": get_value(cfg, "final_exam_nlp", "bucket_raw", fallback="final-exam-nlp-raw"),
        "source_prefix": get_value(cfg, "final_exam_nlp", "source_prefix", fallback="facebook"),
        "selenium_remote_url": get_value(
            cfg,
            "final_exam_nlp",
            "selenium_remote_url",
            fallback="http://selenium-chrome:4444/wd/hub",
        ),
        "page_load_timeout_sec": get_value(
            cfg, "final_exam_nlp", "page_load_timeout_sec", fallback="90"
        ),
        "image_timeout_sec": get_value(cfg, "final_exam_nlp", "image_timeout_sec", fallback="25"),
        "min_image_width": get_value(cfg, "final_exam_nlp", "min_image_width", fallback="200"),
        "target_valid_delta": get_value(
            cfg, "final_exam_nlp", "target_valid_delta", fallback=str(DEFAULT_TARGET_VALID_DELTA)
        ),
        "max_image_valid_count": get_value(
            cfg, "final_exam_nlp", "max_image_valid_count", fallback=str(DEFAULT_MAX_IMAGE_VALID)
        ),
        "bottom_year": get_value(
            cfg, "final_exam_nlp", "bottom_year", fallback=str(DEFAULT_BOTTOM_YEAR)
        ),
        "gallery_walk_count": get_value(
            cfg,
            "final_exam_nlp",
            "gallery_walk_count",
            fallback=str(DEFAULT_GALLERY_WALK_COUNT),
        ),
        "invalid_recheck_limit": get_value(
            cfg,
            "final_exam_nlp",
            "invalid_recheck_limit",
            fallback=str(DEFAULT_INVALID_RECHECK_LIMIT),
        ),
        "invalid_recheck_max_attempts": get_value(
            cfg,
            "final_exam_nlp",
            "invalid_recheck_max_attempts",
            fallback=str(DEFAULT_INVALID_RECHECK_MAX_ATTEMPTS),
        ),
        "skipped_recheck_limit": get_value(
            cfg,
            "final_exam_nlp",
            "skipped_recheck_limit",
            fallback=str(DEFAULT_SKIPPED_RECHECK_LIMIT),
        ),
        "skipped_recheck_max_attempts": get_value(
            cfg,
            "final_exam_nlp",
            "skipped_recheck_max_attempts",
            fallback=str(DEFAULT_SKIPPED_RECHECK_MAX_ATTEMPTS),
        ),
    }


def _group_root(source_prefix: str, group_id: str) -> str:
    return f"{source_prefix}/{group_id}"


def _meta_key(source_prefix: str, group_id: str) -> str:
    return f"{_group_root(source_prefix, group_id)}/meta/group.json"


def _progress_key(source_prefix: str, group_id: str) -> str:
    return f"{_group_root(source_prefix, group_id)}/state/progress.json"


def _posts_index_key(source_prefix: str, group_id: str) -> str:
    return f"{_group_root(source_prefix, group_id)}/state/posts_index.jsonl"


def _cookies_key(source_prefix: str, group_id: str, slot: str = "") -> str:
    """MinIO cookie object; slot B uses cookies_b.json.
    Object cookie MinIO; slot B dùng cookies_b.json.
    """
    # Optional cookie prefix so seed2 still reads A's facebook/.../cookies_b.json /
    # Prefix cookie tùy chọn để seed2 vẫn đọc facebook/.../cookies_b.json của A
    cookie_prefix = (os.environ.get("FEN_COOKIES_PREFIX") or source_prefix).strip() or source_prefix
    resolved_slot = (slot or os.environ.get("FEN_COOKIES_SLOT") or "").strip()
    # Session B keeps a separate cookie file so A is never overwritten /
    # Session B dùng file cookie riêng để không ghi đè A
    name = "cookies_b.json" if resolved_slot.lower() in {"b", "2", "beta"} else "cookies.json"
    return f"{_group_root(cookie_prefix, group_id)}/state/{name}"


def _logs_posts_key(source_prefix: str, group_id: str) -> str:
    return f"{_group_root(source_prefix, group_id)}/logs/posts.jsonl"


def _run_upserts_key(source_prefix: str, group_id: str, run_id: str) -> str:
    return f"{_group_root(source_prefix, group_id)}/logs/by_run/{run_id}/upserts.jsonl"


def _run_result_key(source_prefix: str, group_id: str, run_id: str) -> str:
    return f"{_group_root(source_prefix, group_id)}/logs/by_run/{run_id}/result.json"


def _export_key(source_prefix: str, group_id: str, file_name: str) -> str:
    return f"{_group_root(source_prefix, group_id)}/export/{file_name}"


def _batch_dir(source_prefix: str, group_id: str, batch_id: str) -> str:
    return f"{_group_root(source_prefix, group_id)}/batches/{batch_id}"


def _batch_pending_key(source_prefix: str, group_id: str, batch_id: str) -> str:
    return f"{_batch_dir(source_prefix, group_id, batch_id)}/pending.jsonl"


def _batch_meta_key(source_prefix: str, group_id: str, batch_id: str) -> str:
    return f"{_batch_dir(source_prefix, group_id, batch_id)}/batch_meta.json"


def _image_object_key(
    source_prefix: str, group_id: str, post_id: str, index: int, extension: str
) -> str:
    safe = _safe_post_id(post_id)
    return f"{_group_root(source_prefix, group_id)}/{IMAGE_DIR_NAME}/{safe}/{index}{extension}"


def _read_json_object(bucket: str, object_key: str) -> dict[str, Any] | None:
    if not object_exists(bucket, object_key):
        return None
    client = get_minio_client()
    response = client.get_object(bucket, object_key)
    try:
        data = json.loads(response.read())
    finally:
        response.close()
        response.release_conn()
    return data if isinstance(data, dict) else None


def _to_jsonl(records: list[dict[str, Any]]) -> str:
    if not records:
        return ""
    return "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n"


def _atomic_upload_text(bucket: str, object_key: str, text: str, suffix: str = ".jsonl") -> None:
    """Write via .tmp then overwrite final key / Ghi qua .tmp rồi ghi đè key cuối."""
    tmp_key = f"{object_key}.tmp"
    upload_text_payload(bucket, tmp_key, text, suffix=suffix)
    upload_text_payload(bucket, object_key, text, suffix=suffix)
    try:
        client = get_minio_client()
        client.remove_object(bucket, tmp_key)
    except Exception:
        pass


def _atomic_upload_json(bucket: str, object_key: str, payload: dict[str, Any]) -> None:
    tmp_key = f"{object_key}.tmp"
    upload_json_payload(bucket, tmp_key, payload)
    upload_json_payload(bucket, object_key, payload)
    try:
        get_minio_client().remove_object(bucket, tmp_key)
    except Exception:
        pass


def _cdn_url_base(url: str) -> str:
    """Strip query params for image URL dedup / Bỏ query để dedup URL ảnh."""
    return (url or "").split("?", 1)[0].strip()


def _dedupe_image_urls(urls: list[str] | None) -> list[str]:
    """Keep unique CDN images (order-preserving) / Giữ URL ảnh CDN duy nhất (giữ thứ tự)."""
    out: list[str] = []
    seen: set[str] = set()
    for raw in urls or []:
        if not isinstance(raw, str) or not raw.strip():
            continue
        if _is_video_url(raw):
            continue
        base = _cdn_url_base(raw)
        if not base or base in seen:
            continue
        seen.add(base)
        out.append(raw.strip())
    return out


def _canonical_index_key(record: dict[str, Any]) -> str:
    """Prefer story_post_id, else tile_id/post_id / Ưu tiên story_post_id, không thì tile/post."""
    for field in ("story_post_id", "post_id", "tile_id"):
        value = str(record.get(field) or "").strip()
        if value:
            return value
    return ""


def _merge_post_records(
    existing: dict[str, Any] | None, incoming: dict[str, Any]
) -> dict[str, Any]:
    """Upsert merge: union images, prefer richer matched fields.
    Gộp upsert: hợp ảnh, ưu tiên field đã match đầy đủ hơn.
    """
    if not existing:
        merged = dict(incoming)
    else:
        merged = dict(existing)
        for key, value in incoming.items():
            if key in {"image_urls", "image_local_keys", "tile_ids", "sub_captions"}:
                continue
            if value in (None, "", [], {}):
                continue
            # Do not downgrade valid → pending/invalid unless explicit /
            # Không hạ valid → pending/invalid trừ khi tường minh
            if key == "match_status":
                rank = {"pending": 0, "invalid": 1, "valid": 2}
                old_r = rank.get(str(merged.get("match_status") or ""), -1)
                new_r = rank.get(str(value), -1)
                if new_r >= old_r:
                    merged[key] = value
                continue
            if key == "is_valid" and merged.get("is_valid") and not value:
                continue
            if key == "invalid_reason" and merged.get("is_valid"):
                continue
            if key == "first_seen_at" and merged.get("first_seen_at"):
                continue
            merged[key] = value

    images = _dedupe_image_urls(
        list(existing.get("image_urls") or []) if existing else []
    ) + _dedupe_image_urls(list(incoming.get("image_urls") or []))
    merged["image_urls"] = _dedupe_image_urls(images)
    merged["image_count"] = len(merged["image_urls"])

    # Union per-image captions into sub_captions / Gộp caption từng ảnh vào sub_captions
    sub_captions: list[str] = []
    for src in (existing, incoming):
        if not src:
            continue
        for text in src.get("sub_captions") or []:
            t = str(text or "").strip()
            if t and t not in sub_captions:
                sub_captions.append(t)
        one = str(src.get("sub_caption") or "").strip()
        if one and one not in sub_captions:
            sub_captions.append(one)
    if sub_captions:
        merged["sub_captions"] = sub_captions
        if not merged.get("sub_caption"):
            merged["sub_caption"] = sub_captions[-1]

    tile_ids: list[str] = []
    story = str(
        (incoming.get("story_post_id") or (existing or {}).get("story_post_id") or "")
    ).strip()
    for src in (existing, incoming):
        if not src:
            continue
        for tid in src.get("tile_ids") or []:
            tid_s = str(tid).strip()
            # Exclude story id from tile list / Không đưa story id vào danh sách tile
            if tid_s and tid_s not in tile_ids and tid_s != story:
                tile_ids.append(tid_s)
        tid = str(src.get("tile_id") or "").strip()
        if tid and tid not in tile_ids and tid != story:
            tile_ids.append(tid)
    if tile_ids:
        merged["tile_ids"] = tile_ids
        if not merged.get("tile_id"):
            merged["tile_id"] = tile_ids[0]

    if story:
        merged["post_id"] = story
        merged["story_post_id"] = story
    elif not merged.get("post_id") and merged.get("tile_id"):
        merged["post_id"] = merged["tile_id"]

    if not merged.get("first_seen_at"):
        merged["first_seen_at"] = (
            (existing or {}).get("first_seen_at")
            or incoming.get("first_seen_at")
            or incoming.get("crawled_at")
            or utc_now_iso()
        )
    # Recheck metadata — never drop attempt count / Metadata recheck — không mất số lần thử
    merged["recheck_attempts"] = max(
        _recheck_attempt_count(existing or {}),
        _recheck_attempt_count(incoming),
    )
    merged["recheck_exhausted"] = bool(
        (existing or {}).get("recheck_exhausted") or incoming.get("recheck_exhausted")
    )
    merged["updated_at"] = utc_now_iso()
    merged["schema_version"] = SCHEMA_VERSION
    return merged


def _upsert_post(
    posts: dict[str, dict[str, Any]], record: dict[str, Any]
) -> tuple[str, dict[str, Any]]:
    """Insert/merge record; rekey tile→story when story_post_id appears.
    Chèn/gộp bản ghi; đổi key tile→story khi đã có story_post_id.
    """
    story = str(record.get("story_post_id") or "").strip()
    tile = str(record.get("tile_id") or record.get("post_id") or "").strip()
    key = story or tile
    if not key:
        return "", record

    existing = posts.get(key)
    # Merge prior tile-keyed pending into story key / Gộp pending theo tile vào key story
    if story and tile and tile != story and tile in posts:
        existing = _merge_post_records(posts.pop(tile), existing or {})
    merged = _merge_post_records(existing, record)
    posts[key] = merged
    return key, merged


def _load_posts_index(bucket: str, source_prefix: str, group_id: str) -> dict[str, dict[str, Any]]:
    """Load canonical_id -> record map from index JSONL / Nạp map id chuẩn -> bản ghi."""
    key = _posts_index_key(source_prefix, group_id)
    if not object_exists(bucket, key):
        return {}
    posts: dict[str, dict[str, Any]] = {}
    for line in read_object_text(bucket, key).splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, dict):
            continue
        # Normalize legacy rows / Chuẩn hóa bản ghi cũ
        record["image_urls"] = _dedupe_image_urls(record.get("image_urls") or [])
        record["image_count"] = int(
            record.get("image_count")
            if record.get("image_count") is not None
            else len(record["image_urls"])
        )
        _upsert_post(posts, record)
    return posts


def _count_valid_with_images(posts: dict[str, dict[str, Any]]) -> int:
    return sum(
        1
        for record in posts.values()
        if record.get("is_valid") and record.get("images_downloaded")
    )


def _rebuild_exports(
    *, bucket: str, source_prefix: str, group_id: str, posts: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    """Rebuild deliverable valid/invalid JSONL from the index.

    Dựng file valid/invalid nộp bài từ index.
    """
    valid_records: list[dict[str, Any]] = []
    invalid_records: list[dict[str, Any]] = []
    for record in posts.values():
        export_row = {
            "label": record.get("label") or "",
            "sub_caption": record.get("sub_caption") or "",
            "sub_captions": record.get("sub_captions") or [],
            "images": record.get("image_local_keys") or [],
            "post_id": record.get("post_id"),
            "story_post_id": record.get("story_post_id") or record.get("post_id"),
            "tile_id": record.get("tile_id"),
            "tile_ids": record.get("tile_ids") or [],
            "post_link": record.get("permalink") or record.get("post_link") or "",
            "author": record.get("author") or "",
            "image_urls": record.get("image_urls") or [],
            "image_count": int(
                record.get("image_count")
                if record.get("image_count") is not None
                else len(record.get("image_urls") or [])
            ),
            "posted_at": record.get("posted_at"),
            "images_downloaded": bool(record.get("images_downloaded")),
            "images_download_skipped": bool(record.get("images_download_skipped")),
            "is_valid": bool(record.get("is_valid")),
            "invalid_reason": record.get("invalid_reason"),
            "match_status": record.get("match_status"),
            "phase": record.get("phase"),
            "group_id": group_id,
            "schema_version": SCHEMA_VERSION,
            "source": SOURCE_NAME,
            "updated_at": record.get("updated_at"),
        }
        # Skip unresolved pending — only export matched valid/invalid /
        # Bỏ pending chưa match — chỉ export valid/invalid đã kết luận
        if record.get("match_status") == "pending":
            continue
        if record.get("is_valid"):
            valid_records.append(export_row)
        else:
            invalid_records.append(export_row)

    valid_key = _export_key(source_prefix, group_id, "valid_post.jsonl")
    invalid_key = _export_key(source_prefix, group_id, "invalid_post.jsonl")
    _atomic_upload_text(bucket, valid_key, _to_jsonl(valid_records))
    _atomic_upload_text(bucket, invalid_key, _to_jsonl(invalid_records))
    return {
        "valid_count": len(valid_records),
        "invalid_count": len(invalid_records),
        "valid_key": valid_key,
        "invalid_key": invalid_key,
    }


def _persist_index_and_logs(
    *,
    bucket: str,
    source_prefix: str,
    group_id: str,
    run_id: str,
    posts: dict[str, dict[str, Any]],
    upserts: list[dict[str, Any]],
) -> None:
    """Flush index + append-only run upsert events / Ghi index + append upsert theo run."""
    ordered = list(posts.values())
    _atomic_upload_text(
        bucket, _posts_index_key(source_prefix, group_id), _to_jsonl(ordered)
    )
    _atomic_upload_text(bucket, _logs_posts_key(source_prefix, group_id), _to_jsonl(ordered))
    if not upserts:
        return
    # Append-only upsert log (deduped index lives in posts_index) /
    # Log upsert append-only (index đã dedup nằm ở posts_index)
    upsert_key = _run_upserts_key(source_prefix, group_id, run_id)
    prev = ""
    if object_exists(bucket, upsert_key):
        prev = read_object_text(bucket, upsert_key)
        if prev and not prev.endswith("\n"):
            prev += "\n"
    upload_text_payload(
        bucket,
        upsert_key,
        prev + _to_jsonl(upserts),
        suffix=".jsonl",
    )


def read_should_continue(group_id: str | None = None) -> bool:
    """Airflow ShortCircuit helper: continue rollover? / Helper Airflow: còn rollover không?"""
    settings = _settings()
    bucket = settings["bucket_raw"]
    source_prefix = settings["source_prefix"]
    gid = (group_id or "").strip() or get_value(
        load_config(), "final_exam_nlp", "default_group_id", fallback="322453387859386"
    )
    progress = _read_json_object(bucket, _progress_key(source_prefix, gid)) or {}
    return bool(progress.get("should_continue"))


def _clear_chrome_profile_locks(profile_dir: str) -> None:
    """Remove stale singleton locks after pod restart / Xóa lock singleton cũ sau restart pod."""
    import os
    from pathlib import Path

    lock_names = ("SingletonLock", "SingletonCookie", "SingletonSocket", "DevToolsActivePort")
    roots = [Path(profile_dir)]
    default_profile = Path(profile_dir) / "Default"
    if default_profile.is_dir():
        roots.append(default_profile)
    for root in roots:
        if not root.is_dir():
            continue
        for name in lock_names:
            try:
                (root / name).unlink(missing_ok=True)
            except OSError:
                pass


def _build_driver(
    remote_url: str,
    headless: bool,
    page_load_timeout: int,
    *,
    enable_perf_logs: bool = False,
):
    """Attach to a remote Selenium Grid / standalone Chrome node.

    Kết nối tới Selenium Grid / node Chrome standalone chạy từ xa.
    """
    import os

    from selenium import webdriver

    options = webdriver.ChromeOptions()
    # Headless=new is easy for Facebook to drop. Prefer Xvfb headed Chrome. /
    # Headless=new dễ bị Facebook bỏ session. Ưu tiên Chrome headed qua Xvfb.
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    # Cap disk/media caches — selenium pod hard-limited to 4Gi /
    # Giới hạn cache disk/media — pod selenium trần cứng 4Gi
    options.add_argument("--disk-cache-size=1")
    options.add_argument("--media-cache-size=1")
    options.add_argument("--blink-settings=imagesEnabled=true")
    options.add_argument("--js-flags=--max-old-space-size=512")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    # Enable Chrome performance logs for GraphQL capture /
    # Bật performance log Chrome để bắt GraphQL
    if enable_perf_logs:
        options.set_capability("goog:loggingPrefs", {"performance": "ALL", "browser": "ALL"})
        # experimental_option is what Grid Chrome often honors /
        # experimental_option thường được Grid Chrome tôn trọng
        try:
            options.add_experimental_option(
                "perfLoggingPrefs",
                {"enableNetwork": True, "enablePage": False},
            )
        except Exception:
            pass
        options.set_capability(
            "goog:perfLoggingPrefs",
            {"enableNetwork": True, "enablePage": False},
        )
    # Persist cookies inside Chrome's own profile on the selenium volume /
    # Giữ cookie trong profile Chrome trên volume của selenium
    profile_dir = (os.environ.get("SELENIUM_CHROME_PROFILE_DIR") or "/data/chrome-profile").strip()
    if profile_dir:
        # Stale locks after selenium rollout block new sessions / Lock cũ sau rollout chặn session mới
        _clear_chrome_profile_locks(profile_dir)
        options.add_argument(f"--user-data-dir={profile_dir}")
        options.add_argument("--profile-directory=Default")
        print(f"[final_exam_nlp_crawl] chrome profile={profile_dir}", flush=True)

    driver = webdriver.Remote(command_executor=remote_url, options=options)
    driver.set_page_load_timeout(page_load_timeout)
    driver.set_script_timeout(60)
    return driver


def _safe_quit_driver(driver) -> None:
    # Ignore quit errors when the session is already dead / Bỏ qua lỗi quit khi session đã chết
    if driver is None:
        return
    try:
        driver.quit()
    except Exception:
        pass


def _clear_browser_caches(driver) -> None:
    """Drop Chrome image/network caches via CDP without logging out.

    Xoá cache ảnh/mạng của Chrome qua CDP mà không đăng xuất.
    """
    # CDP can hang on busy Chrome — keep each step best-effort /
    # CDP có thể treo khi Chrome bận — mỗi bước best-effort
    try:
        driver.set_script_timeout(8)
    except Exception:
        pass
    try:
        driver.execute_cdp_cmd("Network.clearBrowserCache", {})
    except Exception:
        pass
    try:
        # Drop decoded images kept by the renderer / Bỏ ảnh đã decode mà renderer đang giữ
        driver.execute_script(
            """
            try { if (window.gc) window.gc(); } catch (e) {}
            document.querySelectorAll('img').forEach(img => {
              if (img.complete && img.naturalWidth) {
                const src = img.currentSrc || img.src;
                if (src && src.includes('scontent')) {
                  img.removeAttribute('srcset');
                  img.src = '';
                }
              }
            });
            """
        )
    except Exception:
        pass


def _js_heap_used_mb(driver) -> float:
    """Approximate JS heap MB via CDP Performance metrics.
    Ước lượng JS heap (MB) qua CDP Performance metrics.
    """
    try:
        payload = driver.execute_cdp_cmd("Performance.getMetrics", {}) or {}
        for metric in payload.get("metrics") or []:
            if str(metric.get("name") or "") == "JSHeapUsedSize":
                return float(metric.get("value") or 0.0) / (1024.0 * 1024.0)
    except Exception:
        pass
    return 0.0


def _ram_guard_selenium(
    driver,
    *,
    soft_limit_mb: float = SELENIUM_RAM_SOFT_MB,
    log_prefix: str = "[final_exam_nlp_crawl]",
) -> bool:
    """Clear caches when JS heap nears soft limit (pod hard-capped at 4Gi).

    Clear cache khi JS heap gần ngưỡng mềm (pod trần cứng 4Gi).
    Returns True if a clear was performed / True nếu đã clear.
    """
    used = _js_heap_used_mb(driver)
    if used <= 0.0 or used < float(soft_limit_mb):
        return False
    print(
        f"{log_prefix} ram_guard heap≈{used:.0f}MB >= {soft_limit_mb:.0f}MB — clear cache",
        flush=True,
    )
    _clear_browser_caches(driver)
    try:
        driver.get("about:blank")
        time.sleep(1.0 + random.uniform(0.1, 0.4))
    except Exception:
        pass
    return True


def _normalize_fbid_list(raw: Any, *, limit: int = 500) -> list[str]:
    """Deduped fbid list preserved order / Danh sách fbid không trùng, giữ thứ tự."""
    out: list[str] = []
    seen: set[str] = set()
    for item in list(raw or []):
        fid = str(item or "").strip()
        if not fid or fid in seen:
            continue
        seen.add(fid)
        out.append(fid)
        if len(out) >= limit:
            break
    return out


def _open_group_media_photos(
    driver,
    *,
    group_id: str,
    scroll_pause_sec: float = 2.0,
) -> bool:
    """Open group Media/Photos grid. / Mở lưới Media/Photos của group."""
    gid = (group_id or "").strip()
    if not gid:
        return False
    media_url = f"{FB_BASE_URL}/groups/{gid}/media/photos"
    media_fallback = f"{FB_BASE_URL}/groups/{gid}/media"
    # Timeout on get is OK if URL already landed on /media /
    # Timeout khi get vẫn OK nếu URL đã vào /media
    try:
        driver.get(media_url)
    except Exception:
        pass
    time.sleep(max(1.2, min(float(scroll_pause_sec), 3.0)))
    try:
        landed = (driver.current_url or "").lower()
    except Exception:
        landed = ""
    if "/media" not in landed:
        try:
            driver.get(media_fallback)
        except Exception:
            pass
        time.sleep(max(1.2, min(float(scroll_pause_sec), 3.0)))
        try:
            landed = (driver.current_url or "").lower()
        except Exception:
            landed = ""
    return "/media" in landed


def _ingest_media_fbids(driver, ordered: list[str], seen: set[str]) -> None:
    """Append newly visible media tile fbids in grid order.
    Thêm fbid ô media mới hiện theo thứ tự lưới.
    """
    for href in _collect_media_hrefs(driver):
        fid = _post_id_from_url(href)
        if not fid or fid in seen:
            continue
        seen.add(fid)
        ordered.append(fid)


def _seek_to_media_fbid(
    driver,
    *,
    group_id: str,
    checkpoint_fbid: str,
    max_scroll_rounds: int = 40,
    scroll_pause_sec: float = 2.0,
    reopen_media: bool = True,
    prefer_scroll_y: int = 0,
    log_prefix: str = "[final_exam_nlp_crawl]",
) -> tuple[bool, int, list[str]]:
    """Scroll Media grid until ``checkpoint_fbid`` is visible; return (found, scrollY, ordered).

    Cuộn lưới Media đến khi thấy ``checkpoint_fbid``; trả (found, scrollY, ordered).
    """
    target = (checkpoint_fbid or "").strip()
    if not target or not (group_id or "").strip():
        return False, 0, []

    if reopen_media:
        if not _open_group_media_photos(
            driver, group_id=group_id, scroll_pause_sec=scroll_pause_sec
        ):
            return False, 0, []

    # Jump near prior depth before fine-seeking fbid /
    # Nhảy gần độ sâu cũ trước khi tinh chỉnh theo fbid
    if int(prefer_scroll_y or 0) > 200:
        print(
            f"{log_prefix} media_seek_to restore_scroll_y≈{prefer_scroll_y}",
            flush=True,
        )
        _restore_media_scroll(driver, int(prefer_scroll_y), float(scroll_pause_sec))

    ordered: list[str] = []
    seen: set[str] = set()
    scroll_y = int(prefer_scroll_y or 0)
    pause = max(1.0, min(float(scroll_pause_sec), 2.5))

    for round_idx in range(1, max(1, int(max_scroll_rounds)) + 1):
        _ingest_media_fbids(driver, ordered, seen)
        if round_idx == 1 or round_idx % 5 == 0:
            print(
                f"{log_prefix} media_seek_to round={round_idx}/{max_scroll_rounds} "
                f"tiles={len(ordered)} target={target}",
                flush=True,
            )
        if target in seen:
            # Stabilize: same fbid still present after short wait /
            # Ổn định: cùng fbid còn sau chờ ngắn
            time.sleep(0.35 + random.uniform(0.05, 0.2))
            _ingest_media_fbids(driver, ordered, seen)
            if target in seen:
                metrics = _scroll_metrics(driver)
                scroll_y = int(metrics.get("scrollY") or 0)
                print(
                    f"{log_prefix} media_seek_to ok fbid={target} round={round_idx} "
                    f"tiles={len(ordered)} scrollY≈{scroll_y}",
                    flush=True,
                )
                return True, scroll_y, list(ordered)
        if round_idx % 5 == 0:
            _ram_guard_selenium(driver, log_prefix=log_prefix)
        grew, y = _advance_media_scroll(driver, pause, steps=3)
        scroll_y = max(scroll_y, int(y or 0))
        if not grew and round_idx > 3:
            _ingest_media_fbids(driver, ordered, seen)
            break

    print(
        f"{log_prefix} media_seek_to miss fbid={target} tiles_seen={len(ordered)}",
        flush=True,
    )
    return False, scroll_y, list(ordered)


def _harvest_media_fbids_after(
    driver,
    *,
    group_id: str,
    after_fbid: str,
    want: int = 12,
    max_scroll_rounds: int = 20,
    scroll_pause_sec: float = 2.0,
    reopen_media: bool = False,
    log_prefix: str = "[final_exam_nlp_crawl]",
) -> tuple[list[str], int]:
    """Return up to ``want`` media fbids strictly after ``after_fbid`` (older direction).

    Trả tối đa ``want`` fbid media đứng sau ``after_fbid`` (hướng cũ hơn).
    """
    after = (after_fbid or "").strip()
    want_n = max(1, int(want))
    if reopen_media:
        if not _open_group_media_photos(
            driver, group_id=group_id, scroll_pause_sec=scroll_pause_sec
        ):
            return [], 0

    ordered: list[str] = []
    seen: set[str] = set()
    scroll_y = 0

    if after:
        found, scroll_y, ordered = _seek_to_media_fbid(
            driver,
            group_id=group_id,
            checkpoint_fbid=after,
            max_scroll_rounds=max_scroll_rounds,
            scroll_pause_sec=scroll_pause_sec,
            reopen_media=False,
            prefer_scroll_y=0,
            log_prefix=log_prefix,
        )
        seen = set(ordered)
        if not found:
            # Checkpoint missing — keep scrolling for any new tiles /
            # Không thấy checkpoint — tiếp tục cuộn lấy tile mới
            ordered = list(ordered)
            seen = set(ordered)

    for round_idx in range(1, max(1, int(max_scroll_rounds)) + 1):
        _ingest_media_fbids(driver, ordered, seen)
        if after and after in seen:
            idx = ordered.index(after)
            nxt = [f for f in ordered[idx + 1 :] if f and f != after]
        else:
            nxt = [f for f in ordered if f and f != after]
        if len(nxt) >= want_n:
            return nxt[:want_n], scroll_y
        if round_idx % 5 == 0:
            _ram_guard_selenium(driver, log_prefix=log_prefix)
        grew, y = _advance_media_scroll(driver, float(scroll_pause_sec), steps=3)
        scroll_y = max(scroll_y, int(y or 0))
        if not grew and round_idx > 2:
            _ingest_media_fbids(driver, ordered, seen)
            break

    if after and after in seen:
        idx = ordered.index(after)
        nxt = [f for f in ordered[idx + 1 :] if f and f != after]
    else:
        nxt = [f for f in ordered if f and f != after]
    return nxt[:want_n], scroll_y


def _media_scroll_at_bottom(metrics: dict[str, Any], *, slack_px: int = MEDIA_BOTTOM_SLACK_PX) -> bool:
    """True when Media scroller is near the bottom. / True khi scroller Media gần đáy."""
    try:
        scroll_y = int(metrics.get("scrollY") or 0)
        max_y = int(metrics.get("maxScrollY") or 0)
    except Exception:
        return False
    if max_y <= 0:
        return False
    return scroll_y >= max(0, max_y - max(200, int(slack_px)))


def _harvest_unseen_media_fbids(
    driver,
    *,
    group_id: str,
    known_fbids: set[str],
    want: int = 12,
    max_scroll_rounds: int = DEFAULT_MEDIA_HARVEST_ROUNDS,
    scroll_pause_sec: float = 2.0,
    reopen_media: bool = True,
    start_scroll_y: int = 0,
    log_prefix: str = "[final_exam_nlp_crawl]",
) -> tuple[list[str], int, bool, bool]:
    """Scroll Media for unseen fbids; report bottom + RAM hard-stop.

    Cuộn Media lấy fbid unseen; báo đáy + dừng cứng khi gần tràn RAM.
    Returns ``(unseen, scroll_y, at_bottom, ram_stop)``.
    """
    want_n = max(1, int(want))
    known = {str(x).strip() for x in (known_fbids or set()) if str(x).strip()}
    rounds = max(1, min(int(max_scroll_rounds), int(MEDIA_HARVEST_ROUNDS_MAX)))
    if reopen_media:
        if not _open_group_media_photos(
            driver, group_id=group_id, scroll_pause_sec=scroll_pause_sec
        ):
            return [], int(start_scroll_y or 0), False, False
        if int(start_scroll_y or 0) > 0:
            print(
                f"{log_prefix} media_unseen restore_scrollY≈{int(start_scroll_y)} "
                f"rounds={rounds}",
                flush=True,
            )
            _restore_media_scroll(driver, int(start_scroll_y), max(1.0, float(scroll_pause_sec)))

    ordered: list[str] = []
    seen: set[str] = set()
    unseen: list[str] = []
    scroll_y = int(start_scroll_y or 0)
    pause = max(1.0, min(float(scroll_pause_sec), 2.5))
    stagnant = 0
    at_bottom = False
    ram_stop = False

    for round_idx in range(1, rounds + 1):
        # Hard-stop before Chrome OOMKills the selenium pod /
        # Dừng cứng trước khi Chrome OOMKill pod selenium
        heap_mb = _js_heap_used_mb(driver)
        if heap_mb >= float(SELENIUM_RAM_HARD_STOP_MB):
            ram_stop = True
            metrics = _scroll_metrics(driver)
            scroll_y = max(scroll_y, int(metrics.get("scrollY") or 0))
            at_bottom = _media_scroll_at_bottom(metrics)
            print(
                f"{log_prefix} media_unseen ram_stop heap≈{heap_mb:.0f}MB "
                f">={SELENIUM_RAM_HARD_STOP_MB:.0f}MB scrollY≈{scroll_y} "
                f"at_bottom={at_bottom} unseen={len(unseen)}",
                flush=True,
            )
            break

        _ingest_media_fbids(driver, ordered, seen)
        for fid in ordered:
            if not fid or fid in known or fid in unseen:
                continue
            unseen.append(fid)
            if len(unseen) >= want_n:
                metrics = _scroll_metrics(driver)
                scroll_y = int(metrics.get("scrollY") or scroll_y)
                at_bottom = _media_scroll_at_bottom(metrics)
                print(
                    f"{log_prefix} media_unseen ok n={len(unseen)} "
                    f"round={round_idx}/{rounds} tiles={len(ordered)} "
                    f"scrollY≈{scroll_y} at_bottom={at_bottom}",
                    flush=True,
                )
                return unseen[:want_n], scroll_y, at_bottom, False
        if round_idx == 1 or round_idx % 10 == 0:
            metrics = _scroll_metrics(driver)
            scroll_y = max(scroll_y, int(metrics.get("scrollY") or 0))
            print(
                f"{log_prefix} media_unseen round={round_idx}/{rounds} "
                f"tiles={len(ordered)} unseen={len(unseen)} known={len(known)} "
                f"scrollY≈{scroll_y} heap≈{heap_mb:.0f}MB",
                flush=True,
            )
        if round_idx % 5 == 0 and heap_mb >= float(SELENIUM_RAM_SOFT_MB):
            # Soft clear only — hard stop handled above /
            # Chỉ soft clear — hard stop xử lý phía trên
            _ram_guard_selenium(driver, log_prefix=log_prefix)

        prev_tiles = len(ordered)
        grew, y = _advance_media_scroll(driver, pause, steps=3)
        scroll_y = max(scroll_y, int(y or 0))
        metrics = _scroll_metrics(driver)
        at_bottom = _media_scroll_at_bottom(metrics)
        if not grew and len(ordered) <= prev_tiles:
            stagnant += 1
        else:
            stagnant = 0
        if at_bottom and stagnant >= 6:
            _ingest_media_fbids(driver, ordered, seen)
            for fid in ordered:
                if fid and fid not in known and fid not in unseen:
                    unseen.append(fid)
            print(
                f"{log_prefix} media_unseen bottom_hit round={round_idx} "
                f"unseen={len(unseen)} scrollY≈{scroll_y}",
                flush=True,
            )
            break
        if stagnant >= 12 and round_idx > 20:
            _ingest_media_fbids(driver, ordered, seen)
            for fid in ordered:
                if fid and fid not in known and fid not in unseen:
                    unseen.append(fid)
            metrics = _scroll_metrics(driver)
            at_bottom = _media_scroll_at_bottom(metrics)
            break

    if not at_bottom:
        metrics = _scroll_metrics(driver)
        scroll_y = max(scroll_y, int(metrics.get("scrollY") or 0))
        at_bottom = _media_scroll_at_bottom(metrics)

    print(
        f"{log_prefix} media_unseen done n={len(unseen)} tiles={len(ordered)} "
        f"known={len(known)} scrollY≈{scroll_y} at_bottom={at_bottom} "
        f"ram_stop={ram_stop} rounds={rounds}",
        flush=True,
    )
    return unseen[:want_n], scroll_y, at_bottom, ram_stop


def _seek_next_media_fbid(
    driver,
    *,
    group_id: str,
    stuck_fbid: str,
    max_scroll_rounds: int = 30,
    scroll_pause_sec: float = 2.0,
    log_prefix: str = "[final_exam_nlp_crawl]",
) -> str:
    """Scroll group Media grid past ``stuck_fbid`` and return the next tile fbid.

    Cuộn lưới Media của group qua ``stuck_fbid`` và trả về fbid ô kế tiếp.
    """
    stuck = (stuck_fbid or "").strip()
    if not stuck or not (group_id or "").strip():
        return ""

    nxt_list, _y = _harvest_media_fbids_after(
        driver,
        group_id=group_id,
        after_fbid=stuck,
        want=1,
        max_scroll_rounds=max_scroll_rounds,
        scroll_pause_sec=scroll_pause_sec,
        reopen_media=True,
        log_prefix=log_prefix,
    )
    if nxt_list:
        nxt = nxt_list[0]
        print(
            f"{log_prefix} media_seek_next stuck={stuck} next={nxt}",
            flush=True,
        )
        return nxt
    print(
        f"{log_prefix} media_seek_miss stuck={stuck}",
        flush=True,
    )
    return ""


def _probe_profile_session(driver, pause_sec: float = 2.0) -> bool:
    """Check whether the persisted Chrome profile is already logged into Facebook.

    Kiểm tra profile Chrome đã đăng nhập Facebook hay chưa.
    """
    try:
        driver.get(FB_BASE_URL + "/")
    except Exception as exc:
        print(
            f"[final_exam_nlp_crawl] profile_probe nav_err={type(exc).__name__}",
            flush=True,
        )
        return False
    time.sleep(max(1.0, float(pause_sec)))
    try:
        logged = _is_logged_in(driver)
        cur = (getattr(driver, "current_url", "") or "")[:90]
    except Exception as exc:
        print(
            f"[final_exam_nlp_crawl] profile_probe session_dead={type(exc).__name__}",
            flush=True,
        )
        return False
    print(
        f"[final_exam_nlp_crawl] profile_probe logged_in={logged} url={cur}",
        flush=True,
    )
    return logged


def _restore_session_profile_first(
    driver,
    cookies: list[dict[str, Any]] | None,
    *,
    log_prefix: str = "[final_exam_nlp_crawl]",
) -> tuple[bool, str]:
    """Prefer Chrome profile volume; fall back to MinIO cookie inject only when needed.

    Ưu tiên session trên profile Chrome; chỉ inject cookie MinIO khi profile chưa login.

    Returns / Trả về:
        (logged_in, source) where source is profile|minio|none
    """
    if _probe_profile_session(driver):
        print(f"{log_prefix} using persisted chrome profile — skip MinIO cookie inject", flush=True)
        return True, "profile"

    cookie_list = list(cookies or [])
    if not cookie_list:
        _warmup_facebook_session(driver)
        return _is_logged_in(driver), "none"

    restored = _restore_cookies(driver, cookie_list)
    if restored:
        print(f"{log_prefix} profile empty — restored MinIO cookies", flush=True)
    warmed = _warmup_facebook_session(driver)
    print(f"{log_prefix} MinIO cookie warmup logged_in={warmed}", flush=True)
    return _is_logged_in(driver), "minio"


def _open_group_feed(driver, group_url: str, cookies: list[dict[str, Any]] | None) -> None:
    """Open group feed using Chrome profile first, MinIO cookies as fallback.

    Mở feed group: ưu tiên profile Chrome, cookie MinIO chỉ là fallback.
    """
    _restore_session_profile_first(driver, cookies)
    driver.get(group_url)
    time.sleep(3)


def _soft_restart_browser(
    *,
    driver,
    remote_url: str,
    headless: bool,
    page_load_timeout: int,
    group_url: str,
    cookies: list[dict[str, Any]] | None,
    reason: str,
    fb_username: str = "",
    fb_password: str = "",
    fb_totp_secret: str = "",
):
    """Quit Chrome and open a fresh session so heap memory is released.

    Đóng Chrome và mở session mới để giải phóng heap.
    If cookies fail, do not password-login (avoids 2FA) /
    Nếu cookie fail, không login password (tránh kích 2FA).
    """
    print(f"[final_exam_nlp_crawl] soft-restart browser ({reason})", flush=True)
    _safe_quit_driver(driver)
    time.sleep(2)
    new_driver = _build_driver(
        remote_url=remote_url,
        headless=headless,
        page_load_timeout=page_load_timeout,
    )
    _open_group_feed(new_driver, group_url, cookies)
    try:
        _require_logged_in_session(
            new_driver,
            had_cookies=bool(cookies),
            fb_username=fb_username,
            fb_password=fb_password,
            fb_totp_secret=fb_totp_secret,
        )
    except Exception:
        _safe_quit_driver(new_driver)
        raise
    print(f"[final_exam_nlp_crawl] soft-restart ready: {new_driver.current_url}", flush=True)
    return new_driver


def _is_logged_in(driver) -> bool:
    """Detect an authenticated session via the c_user cookie.

    Nhận biết phiên đã đăng nhập qua cookie c_user.

    The group URL stays unchanged when logged out — Facebook renders a public
    teaser page — so the URL is not a usable signal /
    URL group không đổi khi chưa đăng nhập vì Facebook trả trang teaser public,
    nên không thể dựa vào URL.

    about:blank / non-facebook pages hide domain cookies from get_cookie() —
    use CDP Network.getAllCookies as fallback /
    about:blank / trang ngoài FB ẩn cookie domain với get_cookie() —
    fallback CDP Network.getAllCookies.
    """
    try:
        cookie = driver.get_cookie("c_user") or {}
        if cookie.get("value"):
            return True
    except Exception:
        pass
    # Fallback: cookies exist but current page is about:blank after RAM release /
    # Fallback: cookie vẫn còn nhưng trang đang about:blank sau khi giải phóng RAM
    try:
        payload = driver.execute_cdp_cmd("Network.getAllCookies", {}) or {}
        for item in payload.get("cookies") or []:
            if item.get("name") == "c_user" and item.get("value"):
                return True
    except Exception:
        pass
    try:
        for item in driver.get_cookies() or []:
            if item.get("name") == "c_user" and item.get("value"):
                return True
    except Exception:
        pass
    return False


def _is_facebook_checkpoint(driver) -> bool:
    """True when Facebook shows a security/checkpoint interstitial.

    True khi Facebook hiện màn checkpoint/bảo mật (cần người xử lý).
    """
    try:
        url = (getattr(driver, "current_url", "") or "").lower()
    except Exception:
        url = ""
    return "/checkpoint/" in url or "checkpoint/" in url


def _cookie_payload(cookie: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize a stored cookie for Selenium / CDP.
    Chuẩn hoá cookie đã lưu cho Selenium / CDP.
    """
    payload = {
        k: v
        for k, v in cookie.items()
        if k in {"name", "value", "domain", "path", "secure", "expiry", "httpOnly", "sameSite"}
    }
    name = str(payload.get("name") or "")
    if not name or payload.get("value") is None:
        return None
    payload.setdefault("path", "/")
    payload.setdefault("secure", True)
    if "facebook.com" in str(payload.get("domain") or "facebook.com"):
        payload["domain"] = ".facebook.com"
    if "expiry" in payload:
        try:
            payload["expiry"] = int(payload["expiry"])
        except Exception:
            payload.pop("expiry", None)
    same = payload.get("sameSite") or cookie.get("same_site")
    if same:
        raw = str(same).lower()
        payload["sameSite"] = {"none": "None", "lax": "Lax", "strict": "Strict"}.get(raw, "None")
    elif name in {"c_user", "xs", "datr", "sb", "fr"}:
        payload["sameSite"] = "None"
    return payload


def _add_cookie_robust(driver, payload: dict[str, Any]) -> bool:
    """Try Selenium add_cookie, then CDP Network.setCookie.
    Thử Selenium add_cookie, rồi CDP Network.setCookie.
    """
    try:
        driver.add_cookie(payload)
        return True
    except Exception:
        pass
    try:
        driver.execute_cdp_cmd("Network.enable", {})
        cdp = {
            "name": payload["name"],
            "value": str(payload["value"]),
            "domain": payload.get("domain") or ".facebook.com",
            "path": payload.get("path") or "/",
            "secure": bool(payload.get("secure", True)),
        }
        if payload.get("httpOnly") is not None:
            cdp["httpOnly"] = bool(payload["httpOnly"])
        if payload.get("sameSite"):
            cdp["sameSite"] = payload["sameSite"]
        if payload.get("expiry"):
            cdp["expires"] = int(payload["expiry"])
        driver.execute_cdp_cmd("Network.setCookie", cdp)
        return True
    except Exception:
        return False


def _restore_cookies(driver, cookies: list[dict[str, Any]]) -> bool:
    # Replay saved cookies, then refresh so Facebook hydrates the session /
    # Nạp cookie đã lưu, rồi refresh để Facebook hydrate session
    if not cookies:
        return False
    driver.get(FB_BASE_URL)
    restored = 0
    restored_names: list[str] = []
    skipped: list[str] = []
    for cookie in cookies:
        payload = _cookie_payload(cookie)
        if not payload:
            continue
        name = str(payload["name"])
        if _add_cookie_robust(driver, payload):
            restored += 1
            restored_names.append(name)
        else:
            skipped.append(name)
    try:
        driver.refresh()
        time.sleep(2)
    except Exception:
        pass
    print(
        f"[final_exam_nlp_crawl] cookie_restore count={restored}/{len(cookies)} "
        f"has_c_user={'c_user' in restored_names} logged_in={_is_logged_in(driver)} "
        f"skipped={skipped}",
        flush=True,
    )
    return restored > 0


def _warmup_facebook_session(driver, pause_sec: float = 2.5) -> bool:
    """Stay on facebook.com until c_user sticks, before opening the group.
    Ở facebook.com đến khi c_user bám, rồi mới mở group.
    """
    try:
        driver.get(FB_BASE_URL + "/")
    except Exception:
        pass
    time.sleep(max(1.5, float(pause_sec)))
    if _is_logged_in(driver):
        return True
    try:
        driver.refresh()
    except Exception:
        pass
    time.sleep(2)
    return _is_logged_in(driver)


def _require_logged_in_session(
    driver,
    *,
    had_cookies: bool,
    fb_username: str = "",
    fb_password: str = "",
    fb_totp_secret: str = "",
    log_prefix: str = "[final_exam_nlp_crawl]",
) -> None:
    """Prefer cookies; if they fail, auto-login only when TOTP is configured.
    Ưu tiên cookie; cookie fail thì auto-login chỉ khi đã có TOTP.
    """
    if _is_logged_in(driver):
        return
    totp = str(fb_totp_secret or "").strip()
    # Auto-login with password+TOTP whenever the authenticator key exists /
    # Auto-login bằng password+TOTP khi đã có khóa authenticator
    if fb_username and fb_password and totp:
        print(
            f"{log_prefix} session missing after cookies={had_cookies}; "
            "auto-login with credentials+TOTP",
            flush=True,
        )
        _login_with_credentials(driver, fb_username, fb_password, totp)
        if _is_logged_in(driver):
            return
        raise RuntimeError(
            f"Facebook auto-login failed at {getattr(driver, 'current_url', '')}"
        )
    if had_cookies:
        cur = (getattr(driver, "current_url", "") or "")[:160]
        raise RuntimeError(
            "Facebook cookies were restored but c_user is missing after navigation. "
            f"url={cur}. "
            "Add FB_TOTP_SECRET to secret 'facebook-crawler-login' for auto-login, "
            "or re-seed cookies.json. Password login is skipped to avoid 2FA."
        )
    raise RuntimeError(
        "Facebook is logged out. Add FB_TOTP_SECRET to secret 'facebook-crawler-login' "
        "(authenticator setup key, not the 6-digit code) or seed cookies.json."
    )


def _click_first(driver, xpaths: tuple[str, ...]) -> bool:
    # Click the first matching visible control / Bấm control khớp đầu tiên đang hiển thị
    from selenium.webdriver.common.by import By

    for xpath in xpaths:
        for element in driver.find_elements(By.XPATH, xpath):
            try:
                if element.is_displayed():
                    element.click()
                    return True
            except Exception:
                continue
    return False


def _dump_inputs(driver, label: str) -> None:
    # Print form controls so an unknown Facebook screen can be identified from logs /
    # In các control của form để nhận diện màn hình lạ của Facebook từ log
    from selenium.webdriver.common.by import By

    try:
        print(f"[final_exam_nlp_crawl][debug] {label} url={driver.current_url[:120]}")
        for element in driver.find_elements(By.TAG_NAME, "input")[:10]:
            print(
                f"[final_exam_nlp_crawl][debug]   input type={element.get_attribute('type')!r} "
                f"name={element.get_attribute('name')!r} "
                f"autocomplete={element.get_attribute('autocomplete')!r} "
                f"displayed={element.is_displayed()}"
            )
    except Exception as exc:
        print(f"[final_exam_nlp_crawl][debug] {label} dump failed: {exc}")


def _wait_logged_in(driver, timeout: float) -> bool:
    # Poll for the session cookie instead of a fixed sleep / Chờ cookie phiên thay vì sleep cứng
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _is_logged_in(driver):
            return True
        time.sleep(2)
    return False


def _submit_totp(driver, totp_secret: str) -> None:
    """Answer the two-factor prompt with a generated TOTP code.

    Trả lời bước xác thực 2 lớp bằng mã TOTP sinh tự động.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys

    try:
        import pyotp
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Two-factor authentication required but 'pyotp' is not installed. "
            "Add pyotp to dags/requirements.txt."
        ) from exc

    # Facebook prints the setup key in groups of four, strip them out / Facebook hiển thị khoá theo nhóm 4 ký tự, cần bỏ khoảng trắng
    code = pyotp.TOTP(totp_secret.replace(" ", "").upper()).now()
    code_box = None
    for selector in TOTP_CODE_SELECTORS:
        elements = [e for e in driver.find_elements(By.CSS_SELECTOR, selector) if e.is_displayed()]
        if elements:
            code_box = elements[0]
            break
    if code_box is None:
        _dump_inputs(driver, "two_factor_screen")
        raise RuntimeError("Could not locate the two-factor code input on the Facebook page")

    code_box.clear()
    code_box.send_keys(code)
    if not _click_first(driver, SUBMIT_BUTTON_XPATHS):
        code_box.send_keys(Keys.ENTER)

    if _wait_logged_in(driver, LOGIN_WAIT_SEC):
        return
    # "Save your login info" / "Trust this device" interstitial blocks the session /
    # Màn hình "Lưu thông tin đăng nhập" / "Tin cậy thiết bị" chặn phiên đăng nhập
    _click_first(driver, SUBMIT_BUTTON_XPATHS)
    _wait_logged_in(driver, LOGIN_WAIT_SEC)


def _find_visible(driver, selectors: tuple[str, ...], timeout: float):
    # Poll a selector list until one yields a visible control / Chờ danh sách selector đến khi có control hiển thị
    from selenium.webdriver.common.by import By

    deadline = time.time() + timeout
    while time.time() < deadline:
        for selector in selectors:
            for element in driver.find_elements(By.CSS_SELECTOR, selector):
                try:
                    if element.is_displayed():
                        return element
                except Exception:
                    continue
        time.sleep(1)
    return None


def _login_with_credentials(driver, username: str, password: str, totp_secret: str = "") -> None:
    """Fill and submit the Facebook login form, then wait for the session.

    Điền và gửi form đăng nhập Facebook, sau đó chờ phiên được thiết lập.
    """
    from selenium.webdriver.common.keys import Keys

    # Expired cookies make /login render the "log in as <name>" screen, which has
    # no email field, so start from an empty cookie jar /
    # Cookie hết hạn làm /login hiện màn hình "đăng nhập với <tên>" không có ô
    # email, nên phải bắt đầu với cookie rỗng
    try:
        driver.delete_all_cookies()
    except Exception:
        pass

    driver.get(FB_LOGIN_URL)
    # Facebook randomises element ids on every render, `name` and `type` stay stable /
    # Facebook random hoá id mỗi lần render, `name` và `type` mới ổn định
    email_box = _find_visible(driver, EMAIL_INPUT_SELECTORS, 30)
    pass_box = _find_visible(driver, PASSWORD_INPUT_SELECTORS, 10)
    if email_box is None or pass_box is None:
        _dump_inputs(driver, "login_screen")
        raise RuntimeError(
            "Facebook login form not found — the page is not the standard login screen "
            f"(url={driver.current_url[:120]})"
        )
    email_box.clear()
    email_box.send_keys(username)
    pass_box.clear()
    pass_box.send_keys(password)
    pass_box.send_keys(Keys.ENTER)

    if _wait_logged_in(driver, PASSWORD_WAIT_SEC):
        return

    if "two_step_verification" in driver.current_url:
        if not totp_secret:
            raise RuntimeError(
                "Two-factor authentication required but FB_TOTP_SECRET is empty. "
                "Add the authenticator setup key to secret 'facebook-crawler-login'."
            )
        print("[final_exam_nlp_crawl] two-factor prompt detected, submitting TOTP code")
        _submit_totp(driver, totp_secret)


def _close_dialogs(driver, debug: bool = False) -> None:
    """Close modals with ESC so page scrolling is released.

    Đóng modal bằng ESC để trang được cuộn lại.

    Overriding styles on body / html / div[role='feed'] breaks Facebook's
    virtualised feed — posts stop hydrating and pagination dies — so the DOM is
    left untouched apart from dismissing modals /
    Ghi đè style của body / html / div[role='feed'] làm vỡ virtualized feed của
    Facebook — post ngừng hydrate và mất phân trang — nên không can thiệp DOM,
    chỉ đóng modal.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys

    try:
        dialogs = driver.find_elements(By.CSS_SELECTOR, "div[role='dialog']")
        if not dialogs:
            return
        if debug:
            print(f"[final_exam_nlp_crawl][debug] closing {len(dialogs)} dialog(s) with ESC")
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
        time.sleep(0.5)
    except Exception:
        pass


SCROLL_METRICS_SCRIPT = """
const feed = document.querySelector("div[role='feed']");
// Measure the element that actually scrolls, body.scrollHeight overstates it /
// Đo phần tử thật sự cuộn, body.scrollHeight cho số lớn hơn thực tế
const scroller = document.scrollingElement || document.documentElement;
return {
    scrollY: Math.round(window.scrollY),
    innerHeight: window.innerHeight,
    scrollHeight: scroller.scrollHeight,
    maxScrollY: Math.max(0, scroller.scrollHeight - scroller.clientHeight),
    feedChildren: feed ? feed.children.length : -1,
    feedLabel: feed ? (feed.getAttribute('aria-label') || '') : '',
    articles: document.querySelectorAll("div[role='article']").length,
    dialogs: document.querySelectorAll("div[role='dialog']").length,
};
"""
# Nudge distance that produces a real scroll delta event / Khoảng kéo lên để sinh event cuộn thật
SCROLL_NUDGE_PX = 1200


def _scroll_metrics(driver) -> dict[str, Any]:
    # Snapshot of scroll position, feed size and scroll-lock state / Ảnh chụp vị trí cuộn, kích thước feed và trạng thái khoá cuộn
    empty: dict[str, Any] = {
        "scrollY": 0,
        "innerHeight": 0,
        "scrollHeight": 0,
        "maxScrollY": 0,
        "feedChildren": -1,
        "feedLabel": "",
        "articles": 0,
        "dialogs": 0,
    }
    try:
        raw = driver.execute_script(SCROLL_METRICS_SCRIPT) or {}
    except Exception:
        return empty
    return {**empty, **raw}


def _scroll_feed(driver, pause: float, debug: bool = False) -> bool:
    """Drive the infinite feed one step and report whether new posts rendered.

    Cuộn feed vô hạn một bước và cho biết có post mới được render không.

    Facebook's loader listens to scroll delta events, so a single jump to the
    bottom stops producing new pages once the viewport is already there — the
    nudge up and back down is what keeps the loader firing /
    Bộ nạp của Facebook nghe event delta cuộn, nên nhảy một phát xuống đáy sẽ
    hết tác dụng khi viewport đã ở đó — phải kéo lên rồi xuống lại để nó tiếp tục nạp.
    """
    before = _scroll_metrics(driver)

    def _grew() -> bool:
        after = _scroll_metrics(driver)
        if debug:
            print(
                f"[final_exam_nlp_crawl][debug] scroll y={after['scrollY']}/{after['maxScrollY']} "
                f"height={after['scrollHeight']} feed_children={after['feedChildren']} "
                f"articles={after['articles']} dialogs={after['dialogs']} "
                f"feed_label={after['feedLabel']!r}"
            )
        return (
            int(after["scrollHeight"]) > int(before["scrollHeight"])
            or int(after["feedChildren"]) > int(before["feedChildren"])
        )

    try:
        driver.execute_script("window.scrollTo(0, arguments[0]);", int(before["scrollHeight"]))
    except Exception:
        pass
    time.sleep(pause + random.uniform(0, 1.2))
    if _grew():
        return True

    # Re-trigger the loader with an explicit up-then-down delta / Kích lại bộ nạp bằng delta lên rồi xuống
    try:
        driver.execute_script("window.scrollBy(0, arguments[0]);", -SCROLL_NUDGE_PX)
        time.sleep(0.8)
        driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
    except Exception:
        pass
    time.sleep(pause)
    if _grew():
        return True

    # Some layouts scroll an inner container, not the document /
    # Một số layout cuộn container bên trong chứ không cuộn document
    inner_scroll = """
    const feed = document.querySelector("div[role='feed']");
    let node = feed ? feed.parentElement : null;
    while (node && node !== document.body) {
        if (node.scrollHeight > node.clientHeight + 50) {
            node.scrollTop = node.scrollHeight;
            return true;
        }
        node = node.parentElement;
    }
    return false;
    """
    try:
        driver.execute_script(inner_scroll)
    except Exception:
        pass
    time.sleep(pause)
    return _grew()


def _restore_media_scroll(driver, target_y: int, pause: float) -> None:
    """Re-scroll the photos grid to a saved depth after remounting the page.
    Cuộn lại lưới ảnh về độ sâu đã lưu sau khi reload trang.
    """
    if target_y <= 0:
        return
    # Step down so Facebook virtualization hydrates tiles along the way /
    # Cuộn từng bước để virtualization hydrate ô dọc đường
    step = 900
    y = 0
    while y < target_y:
        y = min(y + step, int(target_y))
        try:
            driver.execute_script("window.scrollTo(0, arguments[0]);", y)
        except Exception:
            break
        time.sleep(max(0.35, pause * 0.3))
    try:
        driver.execute_script(
            """
            const target = arguments[0];
            for (const node of document.querySelectorAll('div')) {
              if (node.scrollHeight <= node.clientHeight + 80) continue;
              node.scrollTop = Math.min(target, node.scrollHeight);
            }
            """,
            int(target_y),
        )
    except Exception:
        pass
    time.sleep(pause * 0.4)


def _advance_media_scroll(driver, pause: float, steps: int = 4) -> tuple[bool, int]:
    """Scroll the media photos grid several steps; return (grew, scrollY).
    Cuộn lưới ảnh media nhiều bước; trả về (có tiến triển, scrollY).
    """
    before = _scroll_metrics(driver)
    try:
        before_hrefs = set(_collect_media_hrefs(driver))
    except Exception:
        before_hrefs = set()

    for index in range(max(1, steps)):
        try:
            driver.execute_script(
                "window.scrollBy(0, Math.max(Math.floor(window.innerHeight * 0.85), 700));"
            )
        except Exception:
            pass
        time.sleep(pause * 0.45 + random.uniform(0, 0.35))
        # Last step: up/down nudge to re-fire FB infinite loader /
        # Bước cuối: kéo lên/xuống để kích loader vô hạn của FB
        if index == steps - 1:
            try:
                driver.execute_script("window.scrollBy(0, arguments[0]);", -SCROLL_NUDGE_PX // 2)
                time.sleep(0.45)
                driver.execute_script("window.scrollBy(0, arguments[0]);", SCROLL_NUDGE_PX)
            except Exception:
                pass
            time.sleep(pause * 0.6)

    # Fallback: scroll the tallest inner container / Fallback: cuộn container cao nhất
    try:
        driver.execute_script(
            """
            let best = null;
            for (const node of document.querySelectorAll('div')) {
              if (node.scrollHeight <= node.clientHeight + 100) continue;
              if (!best || node.scrollHeight > best.scrollHeight) best = node;
            }
            if (best) {
              best.scrollTop = Math.min(best.scrollTop + best.clientHeight, best.scrollHeight);
            }
            """
        )
    except Exception:
        pass
    time.sleep(pause * 0.4)

    after = _scroll_metrics(driver)
    try:
        after_hrefs = set(_collect_media_hrefs(driver))
    except Exception:
        after_hrefs = set()
    # Prefer new tile hrefs over noisy scrollHeight alone /
    # Ưu tiên tile href mới hơn chỉ nhìn scrollHeight nhiễu
    grew = (
        len(after_hrefs - before_hrefs) > 0
        or int(after["scrollY"]) > int(before["scrollY"]) + 150
        or int(after["scrollHeight"]) > int(before["scrollHeight"]) + 80
    )
    return grew, int(after["scrollY"] or 0)


PRUNE_SCRIPT = """
const keepLast = arguments[0];
const feed = document.querySelector("div[role='feed']");
if (!feed) return 0;
const kids = Array.from(feed.children);
const cutoff = kids.length - keepLast;
let pruned = 0;
for (let i = 0; i < cutoff; i++) {
    const el = kids[i];
    if (el.dataset.fenPruned === '1') continue;
    // Freeze the height first so emptying the node cannot shift the scroll position /
    // Chốt chiều cao trước để việc xoá nội dung không làm nhảy vị trí cuộn
    const h = el.offsetHeight;
    if (h > 0) {
        el.style.height = h + 'px';
        el.style.minHeight = h + 'px';
    }
    el.innerHTML = '';
    el.dataset.fenPruned = '1';
    pruned++;
}
return pruned;
"""


def _prune_processed_posts(driver, keep_last: int = PRUNE_KEEP_LAST) -> int:
    """Empty already-scanned feed posts so Chrome can release their memory.

    Rỗng hoá các post đã quét để Chrome giải phóng bộ nhớ.

    An endless feed keeps every scrolled post in the DOM, so Chrome grows until
    the container is OOM-killed and each round re-scans more elements. Emptying
    the node while pinning its height frees the images and text without moving
    the scroll position or removing the child the loader counts on /
    Feed vô hạn giữ lại mọi post đã cuộn trong DOM, nên Chrome phình lên tới khi
    container bị OOM-kill và mỗi vòng phải quét lại nhiều element hơn. Rỗng hoá
    node nhưng ghim chiều cao sẽ giải phóng ảnh và text mà không làm dịch vị trí
    cuộn hay xoá đi child mà bộ nạp của Facebook đang dựa vào.
    """
    try:
        return int(driver.execute_script(PRUNE_SCRIPT, keep_last) or 0)
    except Exception:
        return 0



def _expand_see_more(driver, post) -> None:
    """Expand a truncated caption without opening the photo lightbox.

    Mở caption bị cắt mà không bật lightbox ảnh.

    A "See more" inside an anchor navigates to the permalink and opens a modal,
    which locks page scrolling and freezes the feed, so anchors are skipped /
    Chữ "Xem thêm" nằm trong thẻ <a> sẽ mở permalink dạng modal, khoá cuộn trang
    và làm feed đứng, nên phải bỏ qua các anchor.
    """
    script = """
    const post = arguments[0];
    const labels = ["Xem thêm", "See more"];
    post.querySelectorAll("div[role='button'], span[role='button'], span").forEach(el => {
        if (el.children.length !== 0 || !el.innerText) return;
        if (!labels.includes(el.innerText.trim())) return;
        // Anchors navigate instead of expanding / Thẻ anchor sẽ điều hướng thay vì mở rộng
        if (el.closest("a")) return;
        el.click();
    });
    """
    try:
        driver.execute_script(script, post)
    except Exception:
        pass


def _extract_caption(post) -> str:
    from selenium.webdriver.common.by import By

    try:
        # Message containers Facebook uses for the post body / Container Facebook dùng cho nội dung bài viết
        for selector in CAPTION_SELECTORS:
            for element in post.find_elements(By.CSS_SELECTOR, selector):
                text = element.text.strip()
                # Any non-empty body text counts / Mọi text thân bài không rỗng đều tính
                if text and not _is_ui_noise_label(text):
                    return text
        # Fallback: longest auto-direction text that is not UI chrome / Dự phòng: đoạn text dir=auto dài nhất, bỏ phần UI
        texts = []
        for selector in ("div[dir='auto']", "span[dir='auto']"):
            for element in post.find_elements(By.CSS_SELECTOR, selector):
                text = element.text.strip()
                if text and not _is_ui_noise_label(text):
                    texts.append(text)
        if texts:
            return max(texts, key=len)
    except Exception:
        pass
    return ""


def _debug_dump_feed(driver) -> None:
    """Report why the feed yields no posts: gated page vs wrong selectors.

    Báo vì sao feed không ra post: trang bị chặn hay selector sai.
    """
    from selenium.webdriver.common.by import By

    try:
        feeds = driver.find_elements(By.CSS_SELECTOR, "div[role='feed']")
        articles = driver.find_elements(By.CSS_SELECTOR, "div[role='article']")
        body = driver.find_element(By.TAG_NAME, "body").text
        gates = [marker for marker in FEED_GATE_MARKERS if marker in body.lower()]
        print(
            f"[final_exam_nlp_crawl][debug] title={driver.title[:80]!r} "
            f"feed_containers={len(feeds)} articles={len(articles)} "
            f"body_len={len(body)} gate_markers={gates}"
        )
        print(f"[final_exam_nlp_crawl][debug] body_head={body[:600]!r}")
    except Exception as exc:
        print(f"[final_exam_nlp_crawl][debug] feed dump failed: {exc}")


def _debug_dump_post(index: int, post) -> None:
    # Print selector hit counts to tune extraction against the live DOM / In số lượng khớp selector để tinh chỉnh theo DOM thật
    from selenium.webdriver.common.by import By

    try:
        print(f"[final_exam_nlp_crawl][debug] post {index} text={post.text[:200]!r}")
        for selector in (*CAPTION_SELECTORS, "div[dir='auto']", "span[dir='auto']"):
            elements = post.find_elements(By.CSS_SELECTOR, selector)
            texts = [element.text.strip() for element in elements if element.text.strip()]
            longest = max(texts, key=len)[:120] if texts else ""
            print(
                f"[final_exam_nlp_crawl][debug]   {selector} n={len(elements)} "
                f"nonempty={len(texts)} longest={longest!r}"
            )
    except Exception as exc:
        print(f"[final_exam_nlp_crawl][debug] post {index} dump failed: {exc}")


def _extract_images(post, min_width: int) -> list[str]:
    from selenium.webdriver.common.by import By

    images: list[str] = []
    seen: set[str] = set()
    try:
        for img in post.find_elements(By.CSS_SELECTOR, "img"):
            src = img.get_attribute("src") or ""
            # Keep CDN photos only, drop avatars and icons / Chỉ giữ ảnh CDN, bỏ avatar và icon
            if not src.startswith("http") or "scontent" not in src:
                continue
            if any(marker in src for marker in TINY_IMAGE_MARKERS):
                continue
            # Drop thumbnails rendered too small to hold calligraphy / Bỏ ảnh render quá nhỏ, không chứa thư pháp
            try:
                if int(img.size.get("width") or 0) < min_width:
                    continue
            except Exception:
                pass
            clean = src.split("?")[0]
            if clean not in seen:
                seen.add(clean)
                images.append(src)
    except Exception:
        pass
    return images


def _extract_author(post) -> str:
    from selenium.webdriver.common.by import By

    try:
        for selector in ("h2 a", "h3 a", "h4 a", "strong a"):
            for element in post.find_elements(By.CSS_SELECTOR, selector):
                name = element.text.strip()
                if name and len(name) > 1 and "http" not in name.lower():
                    return name
    except Exception:
        pass
    return ""


def _extract_post_link(post) -> str:
    from selenium.webdriver.common.by import By

    try:
        for selector in ("a[href*='/posts/']", "a[href*='/permalink/']", "a[href*='fbid=']"):
            for element in post.find_elements(By.CSS_SELECTOR, selector):
                href = element.get_attribute("href") or ""
                if href and "facebook.com" in href:
                    return href.split("&__cft__")[0].split("?__cft__")[0]
    except Exception:
        pass
    return ""


def _post_id_from_url(post_url: str) -> str:
    # Prefer stable Facebook identifiers from the permalink / Ưu tiên ID ổn định lấy từ permalink
    if not post_url:
        return ""
    for pattern in (
        r"/permalink/(\d+)",
        r"(pfbid[0-9A-Za-z]+)",
        r"/posts/(\d+)",
        r"[?&]story_fbid=(\d+)",
        r"[?&]fbid=(\d+)",
        r"[?&]set=gm\.(\d+)",
    ):
        match = re.search(pattern, post_url)
        if match:
            return match.group(1)
    return ""


def _gallery_photo_url(group_id: str, fbid: str) -> str:
    """Direct group-album photo URL for gallery-walk resume.
    URL ảnh album group để resume gallery-walk.
    """
    tid = (fbid or "").strip()
    gid = (group_id or "").strip()
    if not tid or not gid:
        return ""
    return f"{FB_BASE_URL}/photo/?fbid={tid}&set=g.{gid}"


def _gallery_arrow_step(driver, direction: str = DEFAULT_GALLERY_DIRECTION) -> bool:
    """Advance photo viewer with keyboard arrow (right = older posts).
    Chuyển ảnh bằng phím mũi tên (phải = bài cũ hơn).
    """
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys

    before = _post_id_from_url(driver.current_url or "")
    go_right = (direction or "right").lower() == "right"
    key = Keys.ARROW_RIGHT if go_right else Keys.ARROW_LEFT

    # Focus the viewer so arrow keys actually change photos /
    # Focus photo viewer để mũi tên thật sự đổi ảnh
    try:
        driver.execute_script(
            """
            const dlg = document.querySelector('[role="dialog"]') || document.body;
            try { dlg.focus(); } catch (e) {}
            const img = dlg.querySelector('img[src*="scontent"]');
            if (img) { try { img.click(); } catch (e2) {} }
            """
        )
    except Exception:
        pass
    time.sleep(0.25)

    def _fbid_changed() -> bool:
        after = _post_id_from_url(driver.current_url or "")
        return bool(after) and after != before

    for send in (
        lambda: driver.find_element(By.TAG_NAME, "body").send_keys(key),
        lambda: ActionChains(driver).send_keys(key).perform(),
    ):
        try:
            send()
        except Exception:
            continue
        time.sleep(max(1.0, 1.2 + random.uniform(0.2, 0.5)))
        if _fbid_changed():
            return True

    # Click Next/Prev control if keyboard did not move /
    # Bấm nút Tiếp/Trước nếu phím không đổi ảnh
    labels = (
        ["Next", "Next photo", "Tiếp", "Ảnh tiếp theo", "Right"]
        if go_right
        else ["Previous", "Previous photo", "Trước", "Ảnh trước", "Left"]
    )
    try:
        driver.execute_script(
            """
            const labels = arguments[0].map((s) => s.toLowerCase());
            const nodes = document.querySelectorAll('[aria-label], [role="button"]');
            for (const el of nodes) {
              const lab = (el.getAttribute('aria-label') || '').toLowerCase();
              if (!lab) continue;
              if (labels.some((l) => lab === l || lab.includes(l))) {
                el.click();
                return lab;
              }
            }
            return '';
            """,
            labels,
        )
        time.sleep(max(1.0, 1.2 + random.uniform(0.2, 0.5)))
        if _fbid_changed():
            return True
    except Exception:
        pass
    return False


def _gallery_advance_with_refresh(
    driver,
    *,
    group_id: str,
    fbid: str,
    direction: str = DEFAULT_GALLERY_DIRECTION,
    permalink_pause_sec: float = 3.5,
    max_refresh: int = 10,
    load_wait_sec: float = 10.0,
    log_prefix: str = "[final_exam_nlp_crawl]",
) -> bool:
    """Advance gallery arrow; on stuck, refresh the photo URL up to ``max_refresh`` times.

    Tiến gallery bằng mũi tên; nếu kẹt thì refresh URL ảnh tối đa ``max_refresh`` lần.
    """
    prev = (fbid or "").strip()
    if not prev:
        return _gallery_arrow_step(driver, direction)

    # Wait after reopen/refresh so the photo viewer finishes loading /
    # Chờ sau reopen/refresh để photo viewer load xong
    wait_sec = max(float(load_wait_sec), float(permalink_pause_sec), 1.0)

    for attempt in range(1, max(1, int(max_refresh)) + 1):
        # First try: arrow without refresh / Lần đầu: mũi tên không refresh
        if attempt == 1 and _gallery_arrow_step(driver, direction):
            after = _post_id_from_url(driver.current_url or "")
            if after and after != prev:
                return True

        reopen = _gallery_photo_url(group_id, prev)
        print(
            f"{log_prefix} gallery_stuck_refresh attempt={attempt}/{max_refresh} "
            f"fbid={prev} wait={wait_sec:.1f}s",
            flush=True,
        )
        if reopen:
            try:
                driver.get(reopen)
            except Exception as exc:
                print(
                    f"{log_prefix} gallery_refresh_nav_err "
                    f"{type(exc).__name__}: {str(exc)[:120]}",
                    flush=True,
                )
            time.sleep(wait_sec + random.uniform(0.2, 1.0))

        # From 2nd attempt also hard-refresh the page / Từ lần 2 thêm refresh cứng
        if attempt >= 2:
            try:
                driver.refresh()
                time.sleep(wait_sec + random.uniform(0.2, 1.0))
            except Exception as exc:
                print(
                    f"{log_prefix} gallery_refresh_err "
                    f"{type(exc).__name__}: {str(exc)[:80]}",
                    flush=True,
                )

        if _gallery_arrow_step(driver, direction):
            after = _post_id_from_url(driver.current_url or "")
            if after and after != prev:
                print(
                    f"{log_prefix} gallery_unstuck after_refresh attempt={attempt} "
                    f"from={prev} to={after}",
                    flush=True,
                )
                return True

    return False


def _peek_view_post_href(driver) -> str:
    """Read 'Xem bài viết' href without clicking (stay in photo viewer).
    Đọc href 'Xem bài viết' mà không bấm (ở lại photo viewer).
    """
    script = """
    const texts = ['xem bài viết', 'view post', 'see post', 'xem bài đăng'];
    const nodes = Array.from(document.querySelectorAll(
      'a[href], [role="link"], [role="button"]'
    ));
    for (const el of nodes) {
      const raw = (el.innerText || el.textContent || '').replace(/\\s+/g, ' ').trim();
      if (!raw || raw.length > 80) continue;
      const t = raw.toLowerCase();
      if (!texts.some((w) => t === w || t.includes(w))) continue;
      let href = el.href || el.getAttribute('href') || '';
      if (!href) {
        const a = el.closest && el.closest('a[href]');
        if (a) href = a.href || '';
      }
      if (href) return href;
    }
    return '';
    """
    try:
        return str(driver.execute_script(script) or "").strip()
    except Exception:
        return ""


def _resolve_gallery_start_fbid(
    *,
    progress: dict[str, Any],
    baseline_fbid: str,
    seek_fbid: str,
    force: bool,
) -> tuple[str, str]:
    """Pick open target: seek > cursor > baseline (unless force → baseline).
    Chọn ảnh mở: seek > cursor > baseline (force thì về baseline).
    """
    baseline = (
        (baseline_fbid or "").strip()
        or str(progress.get("gallery_baseline_fbid") or "").strip()
        or DEFAULT_GALLERY_BASELINE_FBID
    )
    cursor = str(progress.get("gallery_cursor_fbid") or "").strip()
    seek = (seek_fbid or "").strip()
    if force:
        return baseline, "force_baseline"
    if seek:
        return seek, "seek"
    if cursor:
        return cursor, "cursor"
    return baseline, "baseline"


def _canonicalize_story_url(url: str, group_id: str = "") -> str:
    """Strip comment/tracking query; keep clean group permalink or posts URL.
    Bỏ query comment/tracking; giữ URL permalink hoặc posts sạch của group.
    """
    raw = (url or "").strip()
    if not raw:
        return ""
    lower = raw.lower()
    # Reject login/media/checkpoint paths / Từ chối path login/media/checkpoint
    if any(bad in lower for bad in ("/login", "/checkpoint", "/recover", "/media/")):
        return ""
    no_hash = raw.split("#", 1)[0]
    path_only = no_hash.split("?", 1)[0]
    match = re.search(r"/groups/(\d+)/permalink/(\d+)", path_only)
    if match:
        gid, pid = match.group(1), match.group(2)
        if group_id and gid != group_id:
            return ""
        return f"{FB_BASE_URL}/groups/{gid}/permalink/{pid}/"
    match = re.search(r"/groups/(\d+)/posts/(pfbid[0-9A-Za-z]+|\d+)", path_only)
    if match:
        gid, pid = match.group(1), match.group(2)
        if group_id and gid != group_id:
            return ""
        return f"{FB_BASE_URL}/groups/{gid}/posts/{pid}/"
    # story_fbid marks a story, not a photo fbid / story_fbid là bài, không phải fbid ảnh
    if "story_fbid=" in lower and "/photo" not in path_only.lower():
        story = re.search(r"[?&]story_fbid=(\d+)", no_hash, flags=re.I)
        if story:
            gid = group_id
            if not gid:
                owned = re.search(r"[?&]id=(\d+)", no_hash)
                gid = owned.group(1) if owned else ""
            if gid:
                return f"{FB_BASE_URL}/groups/{gid}/permalink/{story.group(1)}/"
    return ""


def _has_story_junk_query(url: str) -> bool:
    """True if URL deep-links a comment or notification instead of the post.
    True nếu URL nhảy tới comment hoặc notification thay vì bài viết.
    """
    lower = (url or "").lower()
    return any(
        token in lower
        for token in ("comment_id=", "reply_comment_id=", "notif_id=", "notif_t=")
    )


def _is_story_permalink_url(url: str, group_id: str = "") -> bool:
    """True only for real group post permalinks (not feed/notif/media).
    True chỉ với permalink bài group thật (không phải feed/notif/media).
    """
    return bool(_canonicalize_story_url(url, group_id))


def _click_view_post(driver, pause_sec: float = 2.5) -> tuple[bool, str]:
    """Click 'Xem bài viết' in photo-viewer sidebar; return (ok, href).
    Bấm 'Xem bài viết' trên sidebar photo viewer; trả (ok, href).
    """
    from selenium.webdriver.common.by import By

    texts = (
        "Xem bài viết",
        "View post",
        "See post",
        "Xem bài đăng",
        "See original post",
        "View the post",
    )
    banner_hints = (
        "Ảnh này nằm trong một bài viết",
        "This photo is in a post",
        "This photo is part of a post",
        "nằm trong một bài viết",
        "is in a post",
        "part of a post",
    )

    # Prefer exact match on the blue "Xem bài viết" control in the banner /
    # Ưu tiên khớp đúng nút xanh "Xem bài viết" trong banner
    script = """
    const texts = arguments[0];
    const banners = arguments[1];
    const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim().toLowerCase();
    const wanted = texts.map(norm);
    const bannerNeed = banners.map(norm);

    const isWanted = (t) => wanted.some(w => t === w || t.includes(w));
    const nearBanner = (el) => {
      let n = el;
      for (let i = 0; i < 8 && n; i++) {
        const t = norm(n.innerText || n.textContent || '');
        if (bannerNeed.some(b => t.includes(b))) return true;
        n = n.parentElement;
      }
      return false;
    };

    const nodes = Array.from(document.querySelectorAll(
      'a[href], [role="link"], [role="button"], span, div'
    ));
    const scored = [];
    for (const el of nodes) {
      const raw = (el.innerText || el.textContent || '').replace(/\\s+/g, ' ').trim();
      if (!raw || raw.length > 80) continue;
      const t = norm(raw);
      if (!isWanted(t)) continue;
      let href = el.href || el.getAttribute('href') || '';
      if (!href) {
        const a = el.closest && el.closest('a[href]');
        if (a) href = a.href || '';
      }
      let score = 10;
      if (t === 'xem bài viết' || t === 'view post') score += 5;
      if (nearBanner(el)) score += 8;
      if (href.includes('/permalink/') || href.includes('/posts/') || href.includes('story_fbid=')) score += 12;
      scored.push({ el, href, score, t });
    }
    scored.sort((a, b) => b.score - a.score);
    if (!scored.length) return { ok: false, href: '', debug: 'no_match' };
    const best = scored[0];
    try { best.el.scrollIntoView({block:'center'}); } catch (e) {}
    const target = (best.el.closest && best.el.closest('a[href], [role="link"], [role="button"]')) || best.el;
    try { target.click(); } catch (e) {
      try { target.dispatchEvent(new MouseEvent('click', {bubbles:true, cancelable:true, view:window})); } catch (e2) {}
    }
    return { ok: true, href: best.href || '', debug: best.t };
    """
    # Retry while photo sidebar hydrates / Thử lại trong lúc sidebar ảnh hydrate
    for attempt in range(4):
        time.sleep(1.0 if attempt == 0 else 0.9)
        try:
            result = driver.execute_script(script, list(texts), list(banner_hints)) or {}
            if result.get("ok"):
                href = str(result.get("href") or "").strip()
                time.sleep(max(1.2, float(pause_sec)))
                return True, href
        except Exception as exc:
            print(
                f"[final_exam_nlp_crawl] view_post js err attempt={attempt+1}: "
                f"{type(exc).__name__}",
                flush=True,
            )

    # XPath fallback including banner context / XPath dự phòng kèm context banner
    xpaths = [
        "//*[contains(text(),'Ảnh này nằm trong một bài viết')]/following::*[contains(text(),'Xem bài viết')][1]",
        "//*[contains(text(),'This photo is in a post')]/following::*[contains(text(),'View post')][1]",
        "//*[contains(text(),'Xem bài viết')]",
        "//*[contains(text(),'View post')]",
        "//a[contains(., 'Xem bài viết')]",
        "//*[@role='link' and contains(., 'Xem bài viết')]",
        "//*[@role='button' and contains(., 'Xem bài viết')]",
        "//a[contains(., 'View post')]",
        "//*[@role='link' and contains(., 'View post')]",
    ]
    for xpath in xpaths:
        try:
            for element in driver.find_elements(By.XPATH, xpath):
                try:
                    text = (element.text or "").strip()
                    if not text:
                        continue
                    # Skip the long banner sentence; click the short CTA /
                    # Bỏ câu banner dài; click CTA ngắn
                    if len(text) > 40 and "Xem bài viết" not in text and "View post" not in text:
                        continue
                    clickable = element
                    for ancestor_xpath in (
                        "./ancestor::a[1]",
                        "./ancestor::*[@role='link'][1]",
                        "./ancestor::*[@role='button'][1]",
                    ):
                        try:
                            found = element.find_elements(By.XPATH, ancestor_xpath)
                            if found:
                                clickable = found[0]
                                break
                        except Exception:
                            pass
                    href = (clickable.get_attribute("href") or element.get_attribute("href") or "").strip()
                    driver.execute_script(
                        "arguments[0].scrollIntoView({block:'center'});", clickable
                    )
                    time.sleep(0.35)
                    try:
                        clickable.click()
                    except Exception:
                        driver.execute_script("arguments[0].click();", clickable)
                    time.sleep(max(1.2, float(pause_sec)))
                    return True, href
                except Exception:
                    continue
        except Exception:
            continue
    return False, ""


def _discover_story_permalink(driver, group_id: str) -> str:
    """Find group story permalink on photo viewer / permalink page.
    Tìm permalink bài viết trên trang xem ảnh hoặc permalink.
    """
    gid = json.dumps(group_id or "")
    script = f"""
    const gid = {gid};
    const scored = [];
    const roots = [];
    const dlg = document.querySelector('div[role="dialog"]');
    if (dlg) roots.push(dlg);
    roots.push(document.body);
    for (const root of roots) {{
      for (const a of root.querySelectorAll('a[href]')) {{
        const raw = a.href || '';
        if (!raw.includes('facebook.com')) continue;
        if (/\\/media\\/|\\/login|\\/checkpoint/i.test(raw)) continue;
        const path = raw.split('#')[0].split('?')[0];
        let score = 0;
        let clean = '';
        const perm = path.match(/\\/groups\\/(\\d+)\\/permalink\\/(\\d+)/);
        const posts = path.match(/\\/groups\\/(\\d+)\\/posts\\/(pfbid[0-9A-Za-z]+|\\d+)/);
        if (perm) {{
          score += 12;
          clean = 'https://www.facebook.com/groups/' + perm[1] + '/permalink/' + perm[2] + '/';
        }} else if (posts) {{
          score += 10;
          const pid = posts[2];
          if (/^\\d+$/.test(pid)) {{
            clean = 'https://www.facebook.com/groups/' + posts[1] + '/permalink/' + pid + '/';
            score += 2;
          }} else {{
            clean = 'https://www.facebook.com/groups/' + posts[1] + '/posts/' + pid + '/';
          }}
        }} else {{
          continue;
        }}
        if (gid && clean.includes(gid)) score += 3;
        // Prefer the post itself over a comment deep-link /
        // Ưu tiên bài viết, không lấy deep-link comment
        if (/comment_id=|reply_comment_id=/i.test(raw)) score -= 6;
        if (/notif_id=|notif_t=/i.test(raw)) score -= 8;
        if (raw.includes('/photo/') || /[?&]fbid=/.test(raw)) score -= 6;
        if (score <= 0) continue;
        scored.push({{ h: clean, score }});
      }}
    }}
    scored.sort((a, b) => b.score - a.score);
    const seen = new Set();
    for (const row of scored) {{
      if (seen.has(row.h)) continue;
      seen.add(row.h);
      return row.h;
    }}
    return '';
    """
    try:
        url = driver.execute_script(script) or ""
    except Exception:
        url = ""
    url = str(url).strip()
    # Always persist/open the clean post URL / Luôn lưu/mở URL bài sạch
    canon = _canonicalize_story_url(url, group_id)
    if canon:
        return canon
    # Fallback: scan HTML for permalink ids embedded in FB markup /
    # Dự phòng: quét HTML tìm id permalink trong markup FB
    try:
        html = driver.page_source or ""
    except Exception:
        html = ""
    found = _discover_story_permalink_from_html(html, group_id)
    return found if _is_story_permalink_url(found, group_id) else ""


def _discover_story_permalink_from_html(html: str, group_id: str) -> str:
    """Regex fallback when DOM links are hidden in React markup.
    Regex dự phòng khi link DOM bị ẩn trong markup React.
    """
    if not html or not group_id:
        return ""
    gid = re.escape(group_id.strip())
    for pattern in (
        rf"/groups/{gid}/permalink/(\d+)",
        rf"/groups/{gid}/posts/(pfbid[0-9A-Za-z]+|\d+)",
        rf"story_fbid=(\d+)[^\"'&]*[&\"']id={gid}",
    ):
        match = re.search(pattern, html)
        if not match:
            continue
        pid = match.group(1)
        if "permalink" in pattern:
            return f"{FB_BASE_URL}/groups/{group_id}/permalink/{pid}/"
        if "posts" in pattern:
            if pid.isdigit():
                return f"{FB_BASE_URL}/groups/{group_id}/permalink/{pid}/"
            return f"{FB_BASE_URL}/groups/{group_id}/posts/{pid}/"
        return f"{FB_BASE_URL}/permalink.php?story_fbid={pid}&id={group_id}"
    return ""


def _extract_scoped_cdn_urls(driver, *, max_images: int = 8) -> list[str]:
    """CDN urls from photo dialog / largest visible images only.
    URL CDN từ dialog ảnh / chỉ ảnh lớn đang hiển thị.
    """
    script = """
    const maxN = arguments[0];
    const add = (arr, u, w) => {
      if (!u || typeof u !== 'string') return;
      if (!u.startsWith('http') || !u.includes('scontent')) return;
      if (/p24x24|p32x32|p50x50|s32x32|s60x60|p64x64|stp=/.test(u)) return;
      arr.push({ u, w: w || 0 });
    };
    const items = [];
    const root = document.querySelector('div[role="dialog"]')
      || document.querySelector('[aria-label*="Photo" i]')
      || document.querySelector('[data-pagelet="MediaViewerPhoto"]')
      || document.body;
    for (const img of root.querySelectorAll('img')) {
      add(items, img.currentSrc || img.src, img.naturalWidth || img.width || 0);
    }
    items.sort((a, b) => b.w - a.w);
    const seen = new Set();
    const out = [];
    for (const row of items) {
      const base = row.u.split('?')[0];
      if (seen.has(base)) continue;
      seen.add(base);
      out.push(row.u);
      if (out.length >= maxN) break;
    }
    return out;
    """
    try:
        urls = driver.execute_script(script, max_images) or []
    except Exception:
        urls = []
    return _dedupe_image_urls(
        [u for u in urls if isinstance(u, str) and not _is_video_url(u)]
    )


def _extract_meta_caption(driver) -> str:
    """Fallback caption from og:description on permalink pages.
    Caption dự phòng từ og:description trên trang permalink.
    """
    script = """
    for (const sel of ['meta[property="og:description"]', 'meta[name="description"]']) {
      const el = document.querySelector(sel);
      const text = (el && el.content ? el.content : '').trim();
      if (text && text.length > 2) return text;
    }
    return '';
    """
    try:
        text = driver.execute_script(script) or ""
    except Exception:
        text = ""
    text = str(text).strip()
    return "" if _is_ui_noise_label(text) else text


def _fallback_id(caption: str, images: list[str], author: str) -> str:
    # Hash content when no permalink id is available / Băm nội dung khi không có ID permalink
    raw = f"{author[:40]}|{caption[:180]}|{images[0].split('?')[0] if images else ''}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _normalize_label(label: str) -> str:
    return re.sub(r"\s+", " ", label.strip().lower())


def _safe_post_id(post_id: str) -> str:
    # Keep image file names filesystem safe / Giữ tên file ảnh an toàn trên filesystem
    return re.sub(r"[^0-9A-Za-z_-]", "_", post_id)[:64] or "unknown"


def _image_extension(url: str, content_type: str) -> str:
    # Trust content-type first, fall back to URL suffix / Ưu tiên content-type, sau đó mới đến đuôi URL
    mapping = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp", "image/gif": ".gif"}
    if content_type in mapping:
        return mapping[content_type]
    match = re.search(r"\.(jpe?g|png|webp|gif)(?:$|\?)", url.lower())
    if match:
        return ".jpg" if match.group(1).startswith("jp") else f".{match.group(1)}"
    return ".jpg"


def _fetch_image(url: str, timeout: int) -> tuple[bytes, str]:
    # Download one CDN photo with a browser-like UA / Tải một ảnh CDN với User-Agent giống trình duyệt
    request = urllib.request.Request(url, headers={"User-Agent": BROWSER_UA})
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
        return response.read(), (response.headers.get("Content-Type") or "").split(";")[0].strip()



def _is_ui_noise_label(label: str) -> bool:
    """Reject captions that are only Facebook chrome / Loại caption chỉ là UI Facebook."""
    normalized = _normalize_label(label)
    if not normalized:
        return True
    if normalized in UI_NOISE_TEXT:
        return True
    # Short chrome phrases / Cụm UI ngắn
    if len(normalized) < 8 and normalized in UI_NOISE_TEXT:
        return True
    for noise in UI_NOISE_TEXT:
        if normalized == noise or normalized.startswith(noise + " "):
            return True
        if len(normalized) <= 64 and noise in normalized:
            return True
    for pattern in UI_NOISE_REGEX:
        if re.search(pattern, normalized, flags=re.I):
            return True
    # Profile/likes chrome from photo-viewer sidebar / Chrome tên+like trên sidebar photo viewer
    if re.search(
        r"(lượt thích|people talking|đang nói về|người đang nói|"
        r"\blikes?\b|followers?|người theo dõi|lượt theo dõi|"
        r"người đầu tiên bình luận|first to comment|"
        r"這是書法社團|其他都不准貼文|用政治相關寫書法)",
        normalized,
        flags=re.I,
    ):
        return True
    return False


def _is_content_caption(label: str) -> bool:
    """Keep real post text (CJK, etc.); drop Facebook-only notes.
    Giữ caption bài thật (Hán, v.v.); bỏ ghi chú UI Facebook.
    """
    text = (label or "").strip()
    if not text or _is_ui_noise_label(text):
        return False
    # CJK / Hán-Nôm / Kana — typical exam content / Nội dung đề thi thường gặp
    if re.search(r"[\u4e00-\u9fff\u3400-\u4dbf\u3040-\u30ff\uac00-\ud7af]", text):
        return True
    # Vietnamese body (diacritics), after noise filter /
    # Nội dung tiếng Việt (dấu), sau khi đã lọc chrome
    if len(text) >= 20 and re.search(
        r"[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]",
        text,
        flags=re.I,
    ):
        return True
    # Latin fallback: need real Latin words, not VN chrome with 1 ascii fragment /
    # Latin dự phòng: cần từ Latin thật, không nhận chrome VI vì 1 cụm ascii (vd. "khai")
    latin_words = re.findall(r"[A-Za-z]{4,}", text)
    if len(text) >= 16 and len(latin_words) >= 2:
        return True
    # Pure ASCII post (rare) / Bài thuần ASCII (hiếm)
    if len(text) >= 20 and re.fullmatch(r"[A-Za-z0-9\s\.,!?'\"\-:#@]+", text):
        return True
    return False


def _extract_posted_at(post) -> str | None:
    """Best-effort ISO timestamp from post DOM / Lấy timestamp ISO từ DOM (best-effort)."""
    from selenium.webdriver.common.by import By

    try:
        for element in post.find_elements(By.CSS_SELECTOR, "abbr[data-utime], span[data-utime]"):
            utime = element.get_attribute("data-utime") or ""
            if utime.isdigit():
                return datetime.fromtimestamp(int(utime), tz=timezone.utc).isoformat()
        for element in post.find_elements(By.CSS_SELECTOR, "a[href*='/posts/'] abbr, a[href*='/permalink/'] abbr, abbr"):
            title = (element.get_attribute("title") or element.get_attribute("aria-label") or "").strip()
            if not title:
                continue
            parsed = _parse_absolute_date(title)
            if parsed:
                return parsed
        for element in post.find_elements(By.CSS_SELECTOR, "a[aria-label], span[aria-label]"):
            label = (element.get_attribute("aria-label") or "").strip()
            parsed = _parse_absolute_date(label)
            if parsed:
                return parsed
    except Exception:
        return None
    return None


def _parse_absolute_date(text: str) -> str | None:
    # Accept common absolute date strings only / Chỉ nhận chuỗi ngày tuyệt đối phổ biến
    patterns = (
        r"(?P<y>20\d{2})[-/](?P<m>\d{1,2})[-/](?P<d>\d{1,2})",
        r"(?P<m>[A-Za-z]{3,9})\s+(?P<d>\d{1,2}),?\s+(?P<y>20\d{2})",
        r"(?P<d>\d{1,2})\s+(?P<m>[A-Za-z]{3,9})\s+(?P<y>20\d{2})",
    )
    months = {
        "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
        "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
        "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
    }
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        groups = match.groupdict()
        year = int(groups["y"])
        if "m" in groups and groups["m"].isdigit():
            month = int(groups["m"])
            day = int(groups["d"])
        else:
            month = months.get(groups["m"].lower()[:3] if len(groups["m"]) > 3 else groups["m"].lower())
            if month is None:
                month = months.get(groups["m"].lower())
            if month is None:
                continue
            day = int(groups["d"])
        try:
            return datetime(year, month, day, tzinfo=timezone.utc).isoformat()
        except ValueError:
            continue
    return None


def _year_from_posted_at(posted_at: str | None) -> int | None:
    if not posted_at:
        return None
    match = re.match(r"(20\d{2})", posted_at)
    return int(match.group(1)) if match else None


def _store_post_images(
    *,
    bucket: str,
    source_prefix: str,
    group_id: str,
    post_id: str,
    image_urls: list[str],
    timeout: int,
) -> tuple[list[str], list[str]]:
    """Download photos into images/{post_id}/N.ext / Tải ảnh vào images/{post_id}/N.ext."""
    stored: list[str] = []
    errors: list[str] = []
    for index, url in enumerate(image_urls, start=1):
        try:
            payload, content_type = _fetch_image(url, timeout)
        except Exception as exc:
            errors.append(f"{type(exc).__name__}: {exc}")
            continue
        if len(payload) < 4096:
            errors.append(f"too_small:{len(payload)}")
            continue
        extension = _image_extension(url, content_type)
        object_key = _image_object_key(source_prefix, group_id, post_id, index, extension)
        try:
            if not object_exists(bucket, object_key):
                upload_binary_payload(bucket, object_key, payload)
        except Exception as exc:
            errors.append(f"upload_failed: {exc}")
            continue
        safe = _safe_post_id(post_id)
        stored.append(f"/{IMAGE_DIR_NAME}/{safe}/{index}{extension}")
    return stored, errors


def _classify_record(label: str, image_urls: list[str]) -> tuple[bool, str | None]:
    """Valid iff meaningful caption and at least one image (OCR later).
    Valid khi caption có nội dung thật và có ít nhất một ảnh (OCR sau).
    """
    missing: list[str] = []
    if not label.strip():
        missing.append("label")
    elif _is_ui_noise_label(label):
        return False, "ui_noise_caption"
    elif not _is_content_caption(label):
        return False, "not_content_caption"
    if not image_urls:
        missing.append("image")
    if missing:
        return False, f"missing_{'_and_'.join(missing)}"
    return True, None


def _build_record(
    *,
    post_id: str,
    post_url: str,
    author: str,
    label: str,
    image_urls: list[str],
    posted_at: str | None,
    group_id: str,
    run_id: str,
    phase: str,
    image_local_keys: list[str],
    images_downloaded: bool,
    images_download_skipped: bool,
    is_valid: bool,
    invalid_reason: str | None,
    tile_id: str = "",
    tile_href: str = "",
    story_post_id: str = "",
    sub_caption: str = "",
) -> dict[str, Any]:
    deduped = _dedupe_image_urls(image_urls)
    now = utc_now_iso()
    tile = (tile_id or "").strip() or (post_id or "").strip()
    story = (story_post_id or "").strip()
    canonical = story or tile or (post_id or "").strip()
    sub = (sub_caption or "").strip()
    return {
        "post_id": canonical,
        "story_post_id": story or None,
        "tile_id": tile or None,
        "tile_ids": [tile] if tile else [],
        "tile_href": (tile_href or "").strip() or None,
        "permalink": post_url,
        "post_link": post_url,
        "author": author,
        "label": label,
        "sub_caption": sub or None,
        "sub_captions": [sub] if sub else [],
        "image_urls": deduped,
        "image_count": len(deduped),
        "image_local_keys": image_local_keys,
        "images_downloaded": images_downloaded,
        "images_download_skipped": images_download_skipped,
        "posted_at": posted_at,
        "is_valid": is_valid,
        "invalid_reason": invalid_reason,
        "phase": phase,
        "group_id": group_id,
        "run_id": run_id,
        "source": SOURCE_NAME,
        "schema_version": SCHEMA_VERSION,
        "first_seen_at": now,
        "updated_at": now,
        "crawled_at": now,
    }


def _is_video_url(url: str) -> bool:
    """True when the URL points at a Facebook video/reel / True nếu URL là video/reel Facebook."""
    lower = (url or "").lower()
    return any(marker in lower for marker in VIDEO_URL_MARKERS)


def _is_homepage_redirect(url: str) -> bool:
    """True when Facebook bounced us to bare homepage / True khi bị đẩy về homepage trần."""
    u = (url or "").strip().rstrip("/").lower()
    if not u:
        return True
    return u in {
        "https://www.facebook.com",
        "https://facebook.com",
        "https://m.facebook.com",
    }


def _posts_to_permalink_url(url: str, group_id: str = "") -> str:
    """Convert /posts/{numeric_id}/ to /permalink/{id}/ when possible.
    Đổi /posts/{numeric_id}/ sang /permalink/{id}/ khi có thể.
    """
    canon = _canonicalize_story_url(url, group_id)
    if not canon:
        return ""
    match = re.search(r"/groups/(\d+)/posts/(\d+)/?", canon)
    if match:
        gid, pid = match.group(1), match.group(2)
        return f"{FB_BASE_URL}/groups/{gid}/permalink/{pid}/"
    return canon


def _story_nav_urls(story_url: str, group_id: str = "") -> list[str]:
    """Navigation candidates: permalink, posts, multi_permalinks.
    URL thử mở bài: permalink, posts, multi_permalinks.
    """
    canon = _canonicalize_story_url(story_url, group_id) or (story_url or "").strip()
    if not canon:
        return []
    perm = _posts_to_permalink_url(canon, group_id)
    post_id = _post_id_from_url(canon) or _post_id_from_url(story_url) or ""
    gid = (group_id or "").strip()
    ordered: list[str] = []
    if perm:
        ordered.append(perm)
    if post_id and gid:
        ordered.append(f"{FB_BASE_URL}/groups/{gid}/posts/{post_id}/")
        ordered.append(f"{FB_BASE_URL}/groups/{gid}/permalink/{post_id}/")
        # Feed deep-link often survives when bare permalink bounces /
        # Deep-link feed thường sống khi permalink trần bị đẩy về home
        ordered.append(f"{FB_BASE_URL}/groups/{gid}/?multi_permalinks={post_id}")
    if canon:
        ordered.append(canon)
    return list(dict.fromkeys(u for u in ordered if u))


def _warm_group_feed(driver, group_id: str, pause_sec: float) -> None:
    """Open group feed so deep links inherit a warm session.
    Mở feed group để deep-link kế thừa session ấm.
    """
    gid = (group_id or "").strip()
    if not gid:
        return
    feed = f"{FB_BASE_URL}/groups/{gid}/"
    try:
        cur = (driver.current_url or "").lower()
        need = (
            not cur
            or _is_homepage_redirect(cur)
            or _is_junk_facebook_url(cur)
            or gid not in cur
        )
        if need:
            driver.get(feed)
            time.sleep(max(1.0, float(pause_sec) + random.uniform(0.5, 1.6)))
            _close_dialogs(driver)
    except Exception:
        pass


def _goto_story_url(driver, url: str, *, post_id: str, pause_sec: float, via_js: bool) -> str:
    """Navigate to story URL and wait for post_id to appear in location.
    Điều hướng tới URL bài và chờ post_id xuất hiện trên location.
    """
    if via_js:
        # JS assign avoids some Selenium get() homepage bounces /
        # assign JS tránh một số bounce homepage của driver.get()
        driver.execute_script("window.location.assign(arguments[0]);", url)
    else:
        driver.get(url)
    deadline = time.time() + max(5.0, float(pause_sec) * 2.8)
    landed = ""
    while time.time() < deadline:
        time.sleep(0.35)
        try:
            landed = driver.current_url or ""
        except Exception:
            landed = ""
        if post_id and post_id in landed and not _is_homepage_redirect(landed):
            break
        if _is_homepage_redirect(landed) or _is_junk_facebook_url(landed):
            # brief settle window before giving up this attempt /
            # cửa sổ ổn định ngắn trước khi bỏ attempt này
            if time.time() + 1.2 >= deadline:
                break
    time.sleep(max(0.5, float(pause_sec) * 0.55 + random.uniform(0.25, 1.0)))
    _close_dialogs(driver)
    try:
        return driver.current_url or landed
    except Exception:
        return landed


def _navigate_story_permalink(
    driver,
    story_url: str,
    group_id: str,
    pause_sec: float,
) -> tuple[str, bool]:
    """Open story URL; warm feed + retry variants if Facebook redirects home.
    Mở URL bài; warm feed + thử biến thể nếu Facebook đẩy về homepage.
    """
    last_landed = ""
    post_id = _post_id_from_url(story_url) or ""
    _warm_group_feed(driver, group_id, pause_sec)
    for attempt_url in _story_nav_urls(story_url, group_id):
        for via_js in (False, True):
            try:
                last_landed = _goto_story_url(
                    driver,
                    attempt_url,
                    post_id=post_id,
                    pause_sec=pause_sec,
                    via_js=via_js,
                )
            except Exception:
                last_landed = ""
                continue
            if _is_homepage_redirect(last_landed) or _is_junk_facebook_url(last_landed):
                _warm_group_feed(driver, group_id, pause_sec)
                continue
            if _is_story_permalink_url(last_landed, group_id):
                return last_landed, True
            # multi_permalinks may land on group with story overlay /
            # multi_permalinks có thể land group kèm overlay bài
            if post_id and post_id in last_landed and group_id in (last_landed or ""):
                return last_landed, True
            if _is_story_permalink_url(attempt_url, group_id) and group_id in (last_landed or ""):
                return last_landed, True
    return last_landed, False


def _is_junk_facebook_url(url: str) -> bool:
    """True for login/checkpoint/blank URLs unsuitable as post links.
    True với URL login/checkpoint/trống — không dùng làm link bài.
    """
    lower = (url or "").strip().lower()
    if not lower or _is_homepage_redirect(lower):
        return True
    return any(
        token in lower
        for token in ("/login", "/checkpoint", "/recover", "facebook.com/?", "facebook.com?")
    )


def _photo_url_variants(group_id: str, tile_id: str, photo_url: str) -> list[str]:
    """Alternate photo URLs to try when opening a media tile.
    Các URL ảnh thay thế khi mở ô media.
    """
    urls: list[str] = []
    if photo_url:
        urls.append(photo_url)
    tid = (tile_id or "").strip()
    gid = (group_id or "").strip()
    if tid and gid:
        urls.append(f"{FB_BASE_URL}/photo/?fbid={tid}&set=g.{gid}")
        urls.append(f"{FB_BASE_URL}/photo.php?fbid={tid}&set=g.{gid}")
        urls.append(f"https://m.facebook.com/photo.php?fbid={tid}&set=g.{gid}")
    return list(dict.fromkeys(u for u in urls if u))


def _navigate_photo_tile(
    driver,
    *,
    group_id: str,
    tile_id: str,
    photo_url: str,
    pause_sec: float,
    log_prefix: str = "[final_exam_nlp_crawl]",
) -> tuple[str, str]:
    """Open media tile via click or in-group navigation (avoid homepage redirect).
    Mở ô media bằng click hoặc điều hướng trong group (tránh redirect homepage).
    """
    from selenium.webdriver.common.by import By

    group_url = f"{FB_BASE_URL}/groups/{group_id}/"
    media_url = f"{FB_BASE_URL}/groups/{group_id}/media/photos"

    def pause() -> None:
        time.sleep(max(0.8, float(pause_sec) + random.uniform(0.3, 1.2)))

    def landed_ok(url: str) -> bool:
        if _is_homepage_redirect(url):
            return False
        lower = (url or "").lower()
        tid = (tile_id or "").lower()
        return (
            (tid and tid in lower)
            or "/photo" in lower
            or "fbid=" in lower
            or "/permalink/" in lower
            or f"/groups/{group_id}" in lower
        )

    def try_click_on_page() -> str:
        for sel in (f"a[href*='fbid={tile_id}']", f"a[href*='{tile_id}']"):
            for element in driver.find_elements(By.CSS_SELECTOR, sel):
                try:
                    driver.execute_script(
                        "arguments[0].scrollIntoView({block:'center'});", element
                    )
                    time.sleep(0.4)
                    element.click()
                    pause()
                    url = driver.current_url or ""
                    if landed_ok(url):
                        return url
                except Exception:
                    continue
        return ""

    strategies: list[tuple[str, Any]] = []

    strategies.append(("click", try_click_on_page))

    def media_click() -> str:
        current = driver.current_url or ""
        if group_id not in current or "/media" not in current:
            driver.get(media_url)
            pause()
            _close_dialogs(driver)
        return try_click_on_page()

    strategies.append(("media_click", media_click))

    for variant in _photo_url_variants(group_id, tile_id, photo_url):

        def js_nav(u: str = variant) -> str:
            if group_id not in (driver.current_url or ""):
                driver.get(group_url)
                pause()
            driver.execute_script("window.location.assign(arguments[0]);", u)
            pause()
            return driver.current_url or ""

        strategies.append((f"js:{variant[:48]}", js_nav))

    for variant in _photo_url_variants(group_id, tile_id, photo_url):

        def warm_get(u: str = variant) -> str:
            driver.get(group_url)
            time.sleep(1.0)
            driver.get(u)
            pause()
            return driver.current_url or ""

        strategies.append((f"get:{variant[:48]}", warm_get))

    last_url = driver.current_url or ""
    for name, fn in strategies:
        try:
            url = fn()
            last_url = url or last_url
            if landed_ok(url):
                print(
                    f"{log_prefix} nav_ok strategy={name} tile={tile_id} "
                    f"landed={url[:90]}",
                    flush=True,
                )
                return url, name
            if url:
                print(
                    f"{log_prefix} nav_fail strategy={name} tile={tile_id} "
                    f"landed={url[:90]}",
                    flush=True,
                )
        except Exception as exc:
            print(
                f"{log_prefix} nav_err strategy={name} tile={tile_id} "
                f"err={type(exc).__name__}",
                flush=True,
            )
    return last_url, "none"


def _collect_media_hrefs(driver) -> list[str]:
    """Collect photo tiles only, sorted top→bottom then left→right.

    Chỉ lấy ô ảnh (bỏ video), sắp xếp trên→dưới rồi trái→phải.
    """
    # One DOM pass: skip video tiles, sort by visual grid position /
    # Một lần quét DOM: bỏ ô video, sort theo vị trí lưới
    script = """
    const videoRe = /\\/(videos?|watch|reels?|live|watchparty)\\b|video\\.php|video_id=/i;
    const anchors = Array.from(document.querySelectorAll('a[href]'));
    const items = [];
    const seen = new Set();
    for (const a of anchors) {
      let href = a.href || '';
      if (!href.includes('facebook.com')) continue;
      href = href.split('&__cft__')[0].split('?__cft__')[0];
      if (videoRe.test(href)) continue;
      // Photos only: photo.php /photo /photos or set=a. / Chỉ ảnh: photo hoặc album set
      const isPhoto = /\\/photo\\b|photo\\.php|\\/photos\\b|[?&]set=a\\.|[?&]fbid=/.test(href);
      if (!isPhoto) continue;
      // Skip if ancestor looks like a video tile / Bỏ nếu cha là ô video
      const tile = a.closest('[role="listitem"], [role="article"], div') || a;
      const label = ((tile.getAttribute('aria-label') || '') + ' ' + (a.getAttribute('aria-label') || '')).toLowerCase();
      if (label.includes('video') || label.includes('phim') || label.includes('reel')) continue;
      if (tile.querySelector('video, [aria-label*="Video" i], [aria-label*="Phim" i]')) continue;
      if (seen.has(href)) continue;
      seen.add(href);
      const rect = a.getBoundingClientRect();
      if (rect.width < 8 || rect.height < 8) continue;
      items.push({ href, top: Math.round(rect.top + window.scrollY), left: Math.round(rect.left) });
    }
    items.sort((x, y) => (x.top - y.top) || (x.left - y.left));
    return items.map(i => i.href);
    """
    try:
        hrefs = driver.execute_script(script) or []
    except Exception:
        hrefs = []
    # Defensive second filter in Python / Lọc phòng thủ lần hai ở Python
    return [href for href in hrefs if isinstance(href, str) and not _is_video_url(href)]


def _extract_from_dialog_or_page(driver, min_width: int) -> dict[str, Any]:
    """Extract fields from open photo dialog or permalink page.

    Trích field từ dialog ảnh đang mở hoặc trang permalink.
    """
    from selenium.webdriver.common.by import By

    root = None
    try:
        dialogs = driver.find_elements(By.CSS_SELECTOR, "div[role='dialog']")
        if dialogs:
            root = dialogs[-1]
        else:
            for selector in ("div[role='article']", "div[role='main']", "body"):
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    root = elements[0]
                    break
    except Exception:
        return {"caption": "", "images": [], "author": "", "post_url": "", "posted_at": None}

    _expand_see_more(driver, root)
    caption = _extract_caption(root)
    if not caption:
        caption = _extract_meta_caption(driver)
    images = _extract_images(root, min_width)
    author = _extract_author(root)
    post_url = _extract_post_link(root) or driver.current_url
    posted_at = _extract_posted_at(root)
    return {
        "caption": caption,
        "images": images,
        "author": author,
        "post_url": post_url,
        "posted_at": posted_at,
    }


def _drain_selenium_sessions(remote_url: str) -> int:
    """Delete stuck Selenium sessions so the grid becomes ready again.
    Xoá session Selenium kẹt để grid sẵn sàng lại.
    """
    base = remote_url.replace("/wd/hub", "").rstrip("/")
    status_url = f"{base}/status"
    deleted = 0
    try:
        with urllib.request.urlopen(status_url, timeout=10) as response:  # noqa: S310
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        print(f"[final_exam_nlp_crawl] drain status failed: {type(exc).__name__}", flush=True)
        return 0

    for node in ((payload.get("value") or {}).get("nodes") or []):
        for slot in node.get("slots") or []:
            session = slot.get("session") or {}
            sid = session.get("sessionId") or ""
            if not sid:
                continue
            for path in (f"{base}/session/{sid}", f"{base}/wd/hub/session/{sid}"):
                req = urllib.request.Request(path, method="DELETE")
                try:
                    with urllib.request.urlopen(req, timeout=8) as resp:  # noqa: S310
                        _ = resp.read()
                    deleted += 1
                    print(f"[final_exam_nlp_crawl] drained session {sid[:12]}...", flush=True)
                    break
                except Exception:
                    continue
    return deleted


def _selenium_preflight(remote_url: str, *, retries: int = 8, wait_sec: float = 5.0) -> None:
    """Fail fast when the Selenium grid is not ready / Fail sớm khi Selenium chưa sẵn sàng."""
    status_url = remote_url.replace("/wd/hub", "").rstrip("/") + "/status"
    last_exc: Exception | None = None
    drained = False
    for attempt in range(1, max(1, retries) + 1):
        try:
            with urllib.request.urlopen(status_url, timeout=10) as response:  # noqa: S310
                payload = json.loads(response.read().decode("utf-8"))
            ready = bool(((payload.get("value") or {}).get("ready")))
            if ready:
                print(
                    f"[final_exam_nlp_crawl] selenium preflight ok ({status_url}) "
                    f"attempt={attempt}",
                    flush=True,
                )
                return
            last_exc = RuntimeError(f"Selenium grid not ready: {payload}")
            print(
                f"[final_exam_nlp_crawl] selenium not ready attempt={attempt}/{retries}",
                flush=True,
            )
            # After first miss, drain orphan sessions then retry /
            # Sau lần miss đầu, drain session orphan rồi retry
            if not drained:
                n = _drain_selenium_sessions(remote_url)
                drained = True
                print(f"[final_exam_nlp_crawl] drained_sessions={n}", flush=True)
        except Exception as exc:
            last_exc = exc
            print(
                f"[final_exam_nlp_crawl] selenium preflight err attempt={attempt}/{retries}: "
                f"{type(exc).__name__}",
                flush=True,
            )
        if attempt < retries:
            time.sleep(wait_sec)
    raise RuntimeError(f"Selenium preflight failed ({status_url}): {last_exc}") from last_exc


def crawl_group_batches(
    *,
    group_id: str,
    batch_size: int = 25,
    max_batches: int = 20,
    target_valid_delta: int = DEFAULT_TARGET_VALID_DELTA,
    max_image_valid_count: int = DEFAULT_MAX_IMAGE_VALID,
    bottom_year: int = DEFAULT_BOTTOM_YEAR,
    scroll_pause_sec: float = 3.0,
    cooldown_sec: float = 35.0,
    stall_limit: int = 12,
    headless: bool = True,
    upload_minio: bool = True,
    force: bool = False,
    download_images: bool = True,
    merge_dataset: bool = True,
    debug_dom: bool = False,
    start_phase: str = "media",
    fb_username: str = "",
    fb_password: str = "",
    fb_totp_secret: str = "",
    run_id: str | None = None,
    # Back-compat aliases / Alias tương thích ngược
    target_valid_count: int | None = None,
) -> dict[str, Any]:
    """Crawl Media then Feed; stop after +N valid this run or bottom year.

    Crawl Media rồi Feed; dừng sau +N valid trong lần chạy hoặc tới năm đáy.

    Images download until ``max_image_valid_count`` unique valid posts have
    local files; afterwards only logs + CDN links are stored /
    Chỉ tải ảnh đến khi đủ ``max_image_valid_count`` post valid có file local;
    sau đó chỉ lưu log + link CDN.
    """
    if not group_id.strip():
        raise ValueError("group_id is required")
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    if max_batches < 1:
        raise ValueError("max_batches must be >= 1")
    if target_valid_delta < 1:
        raise ValueError("target_valid_delta must be >= 1")
    # Legacy absolute target ignored — use target_valid_delta per run /
    # Bỏ target tuyệt đối cũ — dùng target_valid_delta mỗi lần chạy
    _ = target_valid_count

    settings = _settings()
    bucket = settings["bucket_raw"]
    source_prefix = settings["source_prefix"]
    image_timeout = int(settings["image_timeout_sec"])
    min_image_width = int(settings["min_image_width"])
    resolved_run_id = run_id or _now_stamp()
    remote_url = settings["selenium_remote_url"]
    page_load_timeout = int(settings["page_load_timeout_sec"])

    if upload_minio:
        ensure_bucket(get_minio_client(), bucket)
        _selenium_preflight(remote_url)

    posts: dict[str, dict[str, Any]] = {} if force else (
        _load_posts_index(bucket, source_prefix, group_id) if upload_minio else {}
    )
    progress = {} if force else (
        (_read_json_object(bucket, _progress_key(source_prefix, group_id)) or {})
        if upload_minio
        else {}
    )
    # Resume saved phase on rollover; force/start_phase only when empty or forced /
    # Tiếp phase đã lưu khi rollover; chỉ dùng start_phase khi trống hoặc force
    if force:
        phase = (start_phase or "media").strip().lower()
    else:
        phase = (progress.get("phase") or start_phase or "media").strip().lower()
    if phase not in {"media", "feed", "done"}:
        phase = "media"

    valid_with_images = _count_valid_with_images(posts)
    seen_ids: set[str] = set(posts.keys()) | set(progress.get("seen_ids") or [])
    complete_ids: set[str] = {
        pid for pid, rec in posts.items() if rec.get("is_valid") is not None
    } | set(progress.get("complete_ids") or [])

    run_valid = 0
    run_invalid = 0
    run_skipped = 0
    image_count = 0
    upserts: list[dict[str, Any]] = []
    batches: list[dict[str, Any]] = []
    valid_buffer: list[dict[str, Any]] = []
    invalid_buffer: list[dict[str, Any]] = []
    batch_index = 0
    stall_rounds = 0
    scroll_round = 0
    pruned_total = 0
    debug_dumped = 0
    session_recoveries = 0
    oldest_posted_at: str | None = progress.get("oldest_posted_at")
    bottom_hits = 0
    stop_reason = "max_batches"
    # Persist scroll depth across media remounts / Giữ độ sâu cuộn khi remount media
    media_scroll_y = int(progress.get("media_scroll_y") or 0)
    media_url = f"{FB_BASE_URL}/groups/{group_id}/media/photos"
    # Fallback if /media/photos redirects away / Dự phòng nếu /media/photos bị redirect
    media_fallback_url = f"{FB_BASE_URL}/groups/{group_id}/media"
    feed_url = f"{FB_BASE_URL}/groups/{group_id}/?sorting_setting=CHRONOLOGICAL"
    active_url = media_url if phase == "media" else feed_url

    print(
        f"[final_exam_nlp_crawl] group={group_id} run_id={resolved_run_id} phase={phase} "
        f"indexed={len(posts)} valid_with_images={valid_with_images}/{max_image_valid_count} "
        f"target_valid_delta={target_valid_delta} bottom_year={bottom_year}",
        flush=True,
    )

    if phase == "done":
        result = {
            "schema_version": SCHEMA_VERSION,
            "group_id": group_id,
            "run_id": resolved_run_id,
            "stop_reason": "already_done",
            "should_continue": False,
            "phase": phase,
            "valid_count": 0,
            "invalid_count": 0,
            "image_count": 0,
            "batch_count": 0,
            "total_valid_after_run": sum(1 for r in posts.values() if r.get("is_valid")),
            "target_valid_count": target_valid_delta,
            "valid_with_images_count": valid_with_images,
            "finished_at": utc_now_iso(),
        }
        if upload_minio:
            upload_json_payload(bucket, _run_result_key(source_prefix, group_id, resolved_run_id), result)
        return result

    cookies = (_read_json_object(bucket, _cookies_key(source_prefix, group_id)) or {}).get("cookies")
    session_cookies: list[dict[str, Any]] = list(cookies or [])
    driver = _build_driver(
        remote_url=remote_url, headless=headless, page_load_timeout=page_load_timeout
    )

    def _images_quota_left() -> int:
        return max(0, max_image_valid_count - valid_with_images)

    def _delta_reached() -> bool:
        return run_valid >= target_valid_delta

    def _note_oldest(posted_at: str | None) -> None:
        nonlocal oldest_posted_at, bottom_hits
        if not posted_at:
            return
        if oldest_posted_at is None or posted_at < oldest_posted_at:
            oldest_posted_at = posted_at
        year = _year_from_posted_at(posted_at)
        if year is not None and year < bottom_year:
            bottom_hits += 1

    def _persist_progress(extra: dict[str, Any] | None = None) -> None:
        if not upload_minio:
            return
        payload = {
            "updated_at": utc_now_iso(),
            "group_id": group_id,
            "last_run_id": resolved_run_id,
            "phase": phase,
            "seen_ids": sorted(seen_ids)[-50000:],
            "complete_ids": sorted(complete_ids)[-50000:],
            "valid_with_images_count": valid_with_images,
            "max_image_valid_count": max_image_valid_count,
            "oldest_posted_at": oldest_posted_at,
            "media_scroll_y": media_scroll_y,
            "bottom_year": bottom_year,
            "should_continue": stop_reason not in {
                "bottom_year_reached", "already_done", "phase_done_no_continue"
            } and phase != "done",
            "stop_reason": stop_reason,
            "run_valid": run_valid,
            "run_invalid": run_invalid,
        }
        if extra:
            payload.update(extra)
        _atomic_upload_json(bucket, _progress_key(source_prefix, group_id), payload)

    def _flush_buffers(partial: bool = False) -> None:
        nonlocal batch_index, valid_buffer, invalid_buffer, upserts
        if not (valid_buffer or invalid_buffer):
            return
        batch_index += 1
        for record in valid_buffer + invalid_buffer:
            posts[str(record["post_id"])] = record
            upserts.append(record)
            complete_ids.add(str(record["post_id"]))
            seen_ids.add(str(record["post_id"]))
        if upload_minio:
            _persist_index_and_logs(
                bucket=bucket,
                source_prefix=source_prefix,
                group_id=group_id,
                run_id=resolved_run_id,
                posts=posts,
                upserts=upserts,
            )
            if merge_dataset:
                _rebuild_exports(
                    bucket=bucket, source_prefix=source_prefix, group_id=group_id, posts=posts
                )
            _persist_progress({"last_batch_index": batch_index, "partial": partial})
        batches.append(
            {
                "batch_index": batch_index,
                "valid_count": len(valid_buffer),
                "invalid_count": len(invalid_buffer),
                "partial": partial,
            }
        )
        print(
            f"[final_exam_nlp_crawl] flush batch {batch_index} "
            f"valid={len(valid_buffer)} invalid={len(invalid_buffer)} "
            f"run_valid={run_valid}/{target_valid_delta} "
            f"images_quota_left={_images_quota_left()}",
            flush=True,
        )
        valid_buffer = []
        invalid_buffer = []

    def _upsert_extracted(
        *,
        post_id: str,
        post_url: str,
        author: str,
        label: str,
        image_urls: list[str],
        posted_at: str | None,
        current_phase: str,
    ) -> str:
        """Insert or skip a post; return action tag / Thêm hoặc bỏ qua post; trả về action."""
        nonlocal run_valid, run_invalid, run_skipped, image_count, valid_with_images
        if not post_id:
            return "no_id"
        # Skip anything already processed this run or in index /
        # Bỏ qua mọi post đã xử lý trong run này hoặc đã có trong index
        if post_id in complete_ids or post_id in seen_ids:
            run_skipped += 1
            return "skip"

        seen_ids.add(post_id)
        _note_oldest(posted_at)
        # Video-only media is never valid for this exam crawl /
        # Media chỉ có video không bao giờ hợp lệ cho crawl đồ án này
        if _is_video_url(post_url) and not image_urls:
            is_valid, invalid_reason = False, "video_only"
        else:
            is_valid, invalid_reason = _classify_record(label, image_urls)
        image_local_keys: list[str] = []
        images_downloaded = False
        images_download_skipped = False

        if is_valid and download_images and upload_minio and _images_quota_left() > 0:
            image_local_keys, image_errors = _store_post_images(
                bucket=bucket,
                source_prefix=source_prefix,
                group_id=group_id,
                post_id=post_id,
                image_urls=image_urls,
                timeout=image_timeout,
            )
            if image_local_keys:
                images_downloaded = True
                valid_with_images += 1
                image_count += len(image_local_keys)
            else:
                is_valid = False
                invalid_reason = "image_download_failed"
                image_local_keys = []
                # Keep errors only on the record / Chỉ giữ lỗi trên bản ghi
                _ = image_errors
        elif is_valid and (not download_images or _images_quota_left() <= 0):
            # Still valid: log + CDN links only / Vẫn valid: chỉ log + link CDN
            images_download_skipped = _images_quota_left() <= 0
            image_local_keys = []

        record = _build_record(
            post_id=post_id,
            post_url=post_url,
            author=author,
            label=label.strip(),
            image_urls=image_urls,
            posted_at=posted_at,
            group_id=group_id,
            run_id=resolved_run_id,
            phase=current_phase,
            image_local_keys=image_local_keys,
            images_downloaded=images_downloaded,
            images_download_skipped=images_download_skipped,
            is_valid=is_valid,
            invalid_reason=invalid_reason,
        )
        if is_valid:
            valid_buffer.append(record)
            run_valid += 1
            print(
                f"[final_exam_nlp_crawl] +valid {run_valid}/{target_valid_delta} "
                f"post={post_id} downloaded={images_downloaded} "
                f"skipped_dl={images_download_skipped} label={label[:50]!r}",
                flush=True,
            )
        else:
            invalid_buffer.append(record)
            run_invalid += 1

        # Mark complete immediately so later rounds do not re-open the same tile /
        # Đánh dấu xong ngay để round sau không mở lại cùng ô
        posts[post_id] = record
        complete_ids.add(post_id)

        if len(valid_buffer) >= batch_size:
            _flush_buffers()
            time.sleep(cooldown_sec)
        return "valid" if is_valid else "invalid"

    try:
        from selenium.common.exceptions import (
            InvalidSessionIdException,
            StaleElementReferenceException,
        )
        from selenium.webdriver.common.by import By

        logged_in, session_source = _restore_session_profile_first(driver, cookies or [])
        if not logged_in and cookies:
            _restore_cookies(driver, cookies or [])
            _warmup_facebook_session(driver)
            logged_in = _is_logged_in(driver)
        driver.get(active_url)
        time.sleep(3)

        had_cookies = bool(cookies) or session_source == "profile"
        if not _is_logged_in(driver):
            _wait_logged_in(driver, 8)
        _require_logged_in_session(
            driver,
            had_cookies=had_cookies,
            fb_username=fb_username,
            fb_password=fb_password,
            fb_totp_secret=fb_totp_secret,
        )
        if upload_minio and _is_logged_in(driver):
            session_cookies = driver.get_cookies() or session_cookies
            upload_json_payload(
                bucket,
                _cookies_key(source_prefix, group_id),
                {"saved_at": utc_now_iso(), "cookies": session_cookies},
            )

        try:
            session_cookies = driver.get_cookies() or session_cookies
        except Exception:
            pass
        print(f"[final_exam_nlp_crawl] logged in, opened {driver.current_url}", flush=True)
        if upload_minio:
            upload_json_payload(
                bucket,
                _meta_key(source_prefix, group_id),
                {
                    "group_id": group_id,
                    "bottom_year": bottom_year,
                    "max_image_valid_count": max_image_valid_count,
                    "updated_at": utc_now_iso(),
                },
            )

        while batch_index < max_batches and not _delta_reached() and phase != "done":
            try:
                _close_dialogs(driver, debug=debug_dom)
                if phase == "feed" and group_id not in driver.current_url:
                    driver.get(feed_url)
                    time.sleep(3)

                new_in_round = 0
                had_unseen = False
                if phase == "media":
                    # Remount resets scroll — restore depth then advance further /
                    # Remount mất scroll — khôi phục độ sâu rồi cuộn tiếp
                    if "media" not in driver.current_url or "photo" not in driver.current_url.lower():
                        driver.get(media_url)
                        time.sleep(3)
                        if "photo" not in driver.current_url.lower():
                            try:
                                driver.get(media_fallback_url)
                                time.sleep(2)
                            except Exception:
                                pass
                        if media_scroll_y > 200:
                            _restore_media_scroll(driver, media_scroll_y, scroll_pause_sec)
                    elif media_scroll_y > 200:
                        _restore_media_scroll(driver, media_scroll_y, scroll_pause_sec)

                    grew, scrolled_y = _advance_media_scroll(
                        driver, scroll_pause_sec, steps=4
                    )
                    # Aim deeper next remount / Nhắm sâu hơn lần remount sau
                    media_scroll_y = max(media_scroll_y, scrolled_y) + 1200

                    hrefs = _collect_media_hrefs(driver)
                    # Only open tiles not yet processed (avoids skip storms) /
                    # Chỉ mở ô chưa xử lý (tránh bão skip)
                    unseen_hrefs: list[str] = []
                    for href in hrefs:
                        if _is_video_url(href):
                            continue
                        post_id = _post_id_from_url(href)
                        if post_id and (post_id in complete_ids or post_id in seen_ids):
                            continue
                        unseen_hrefs.append(href)
                    had_unseen = bool(unseen_hrefs)
                    print(
                        f"[final_exam_nlp_crawl] media photo tiles={len(hrefs)} "
                        f"unseen={len(unseen_hrefs)} scrollY≈{media_scroll_y} "
                        f"(images only, L→R / T→B)",
                        flush=True,
                    )
                    for href in unseen_hrefs:
                        if _delta_reached() or batch_index >= max_batches:
                            break
                        post_id = _post_id_from_url(href)
                        try:
                            driver.get(href)
                            time.sleep(2)
                            # Bail if Facebook routed us into a video player /
                            # Thoát nếu Facebook đưa vào trình phát video
                            if _is_video_url(driver.current_url):
                                run_skipped += 1
                                if post_id:
                                    seen_ids.add(post_id)
                                    complete_ids.add(post_id)
                                continue
                            extracted = _extract_from_dialog_or_page(driver, min_image_width)
                            post_url = extracted["post_url"] or href
                            if _is_video_url(post_url) and not extracted["images"]:
                                run_skipped += 1
                                continue
                            post_id = _post_id_from_url(post_url) or post_id or _fallback_id(
                                extracted["caption"], extracted["images"], extracted["author"]
                            )
                            action = _upsert_extracted(
                                post_id=post_id,
                                post_url=post_url,
                                author=extracted["author"],
                                label=extracted["caption"],
                                image_urls=extracted["images"],
                                posted_at=extracted["posted_at"],
                                current_phase="media",
                            )
                            if action in {"valid", "invalid"}:
                                new_in_round += 1
                            _close_dialogs(driver)
                        except InvalidSessionIdException:
                            raise
                        except Exception:
                            continue
                    # Return to media grid and restore depth / Quay lưới media và khôi phục độ sâu
                    try:
                        driver.get(media_url)
                        time.sleep(2)
                        if media_scroll_y > 200:
                            _restore_media_scroll(driver, media_scroll_y, scroll_pause_sec)
                    except Exception:
                        pass
                else:
                    grew = _scroll_feed(driver, scroll_pause_sec, debug=debug_dom)
                    post_elements = driver.find_elements(By.CSS_SELECTOR, POST_SELECTORS)
                    for post_index, post in enumerate(post_elements, start=1):
                        if post_index % 10 == 0:
                            print(
                                f"[final_exam_nlp_crawl] scanning {post_index}/{len(post_elements)} "
                                f"run_valid={run_valid}/{target_valid_delta} "
                                f"skipped={run_skipped}",
                                flush=True,
                            )
                        try:
                            post_url = _extract_post_link(post)
                        except InvalidSessionIdException:
                            raise
                        except (StaleElementReferenceException, Exception):
                            continue
                        linked_id = _post_id_from_url(post_url)
                        if linked_id and linked_id in complete_ids:
                            run_skipped += 1
                            continue
                        try:
                            _expand_see_more(driver, post)
                            if debug_dom and debug_dumped < 3:
                                _debug_dump_post(post_index, post)
                                debug_dumped += 1
                            caption = _extract_caption(post)
                            image_urls = _extract_images(post, min_image_width)
                            author = _extract_author(post)
                            posted_at = _extract_posted_at(post)
                        except InvalidSessionIdException:
                            raise
                        except (StaleElementReferenceException, Exception):
                            continue
                        post_id = linked_id or _fallback_id(caption, image_urls, author)
                        action = _upsert_extracted(
                            post_id=post_id,
                            post_url=post_url,
                            author=author,
                            label=caption,
                            image_urls=image_urls,
                            posted_at=posted_at,
                            current_phase="feed",
                        )
                        if action in {"valid", "invalid"}:
                            new_in_round += 1
                        if _delta_reached():
                            break

                pruned = _prune_processed_posts(driver) if phase == "feed" else 0
                pruned_total += pruned
                scroll_round += 1
                print(
                    f"[final_exam_nlp_crawl] round {scroll_round} phase={phase} "
                    f"new={new_in_round} run_valid={run_valid}/{target_valid_delta} "
                    f"invalid={run_invalid} skipped={run_skipped} "
                    f"stall={stall_rounds}/{stall_limit} "
                    f"oldest={oldest_posted_at} bottom_hits={bottom_hits}",
                    flush=True,
                )

                # Bottom-year stop needs repeated dated hits / Dừng năm đáy cần nhiều hit có ngày
                if bottom_hits >= max(3, stall_limit // 2):
                    stop_reason = "bottom_year_reached"
                    phase = "done"
                    break

                stall_rounds = (
                    0
                    if (
                        new_in_round
                        or (phase == "media" and had_unseen)
                        or (phase == "feed" and grew)
                    )
                    else stall_rounds + 1
                )
                if stall_rounds >= stall_limit:
                    if phase == "media":
                        print(
                            "[final_exam_nlp_crawl] media stalled — switching to feed backfill",
                            flush=True,
                        )
                        phase = "feed"
                        stall_rounds = 0
                        driver.get(feed_url)
                        time.sleep(3)
                        continue
                    stop_reason = "stall"
                    if run_valid == 0:
                        _debug_dump_feed(driver)
                    break

                if _delta_reached():
                    stop_reason = "target_valid_delta"
                    break

                if batch_index > 0 and batch_index % BROWSER_RESTART_EVERY_BATCHES == 0:
                    try:
                        session_cookies = driver.get_cookies() or session_cookies
                    except Exception:
                        pass
                    try:
                        driver = _soft_restart_browser(
                            driver=driver,
                            remote_url=remote_url,
                            headless=headless,
                            page_load_timeout=page_load_timeout,
                            group_url=media_url if phase == "media" else feed_url,
                            cookies=session_cookies,
                            reason=f"every {BROWSER_RESTART_EVERY_BATCHES} batches",
                            fb_username=fb_username,
                            fb_password=fb_password,
                            fb_totp_secret=fb_totp_secret,
                        )
                    except RuntimeError as exc:
                        # Cookie/session loss after soft-restart — do not 2FA-login /
                        # Mất cookie/session sau soft-restart — không login 2FA
                        print(f"[final_exam_nlp_crawl] soft-restart aborted: {exc}", flush=True)
                        raise
                    _clear_browser_caches(driver)

            except InvalidSessionIdException:
                session_recoveries += 1
                if session_recoveries > SESSION_RECOVERY_LIMIT:
                    raise
                driver = _soft_restart_browser(
                    driver=driver,
                    remote_url=remote_url,
                    headless=headless,
                    page_load_timeout=page_load_timeout,
                    group_url=media_url if phase == "media" else feed_url,
                    cookies=session_cookies,
                    reason=f"dead session {session_recoveries}/{SESSION_RECOVERY_LIMIT}",
                    fb_username=fb_username,
                    fb_password=fb_password,
                    fb_totp_secret=fb_totp_secret,
                )
                continue
        else:
            if _delta_reached():
                stop_reason = "target_valid_delta"
            elif batch_index >= max_batches:
                stop_reason = "max_batches"
    finally:
        _safe_quit_driver(driver)

    _flush_buffers(partial=True)

    should_continue = stop_reason not in {"bottom_year_reached"} and phase != "done"
    if stop_reason == "stall" and phase == "feed":
        should_continue = False
        phase = "done"

    # Final checkpoint before SUCCESS / Checkpoint cuối trước khi SUCCESS
    stop_reason_final = stop_reason
    if upload_minio:
        _persist_index_and_logs(
            bucket=bucket,
            source_prefix=source_prefix,
            group_id=group_id,
            run_id=resolved_run_id,
            posts=posts,
            upserts=upserts,
        )
        export_info = (
            _rebuild_exports(
                bucket=bucket, source_prefix=source_prefix, group_id=group_id, posts=posts
            )
            if merge_dataset
            else {}
        )
        progress_payload = {
            "phase": phase,
            "should_continue": should_continue,
            "stop_reason": stop_reason_final,
            "oldest_posted_at": oldest_posted_at,
            "valid_with_images_count": valid_with_images,
        }
        _persist_progress(progress_payload)
    else:
        export_info = {}

    total_valid = sum(1 for r in posts.values() if r.get("is_valid"))
    result = {
        "schema_version": SCHEMA_VERSION,
        "source": SOURCE_NAME,
        "group_id": group_id,
        "run_id": resolved_run_id,
        "bucket": bucket,
        "phase": phase,
        "stop_reason": stop_reason_final,
        "should_continue": should_continue,
        "batch_count": len(batches),
        "valid_count": run_valid,
        "invalid_count": run_invalid,
        "skipped_count": run_skipped,
        "image_count": image_count,
        "total_valid_after_run": total_valid,
        "target_valid_count": target_valid_delta,
        "target_valid_delta": target_valid_delta,
        "valid_with_images_count": valid_with_images,
        "max_image_valid_count": max_image_valid_count,
        "oldest_posted_at": oldest_posted_at,
        "bottom_year": bottom_year,
        "batches": batches,
        "dataset": export_info,
        "finished_at": utc_now_iso(),
    }
    if upload_minio:
        upload_json_payload(
            bucket, _run_result_key(source_prefix, group_id, resolved_run_id), result
        )
        # Ensure progress mirrors result for Airflow ShortCircuit /
        # Đồng bộ progress với result cho ShortCircuit Airflow
        _persist_progress(
            {
                "phase": phase,
                "should_continue": should_continue,
                "stop_reason": stop_reason_final,
            }
        )
    print(
        f"[final_exam_nlp_crawl] done reason={stop_reason_final} "
        f"should_continue={should_continue} phase={phase} "
        f"run_valid={run_valid} total_valid={total_valid} "
        f"valid_with_images={valid_with_images}",
        flush=True,
    )
    return result
