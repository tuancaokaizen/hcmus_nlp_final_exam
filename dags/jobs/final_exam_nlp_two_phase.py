"""Two-phase Facebook crawl: media tile harvest → permalink caption+image URLs.

Hai phase: thu tile_id trên media → mở permalink lấy caption + URL ảnh (không download).

Schema 3.14:
  - Media walk: deep-link checkpoint photo URL (no grid seek-to-fbid)
  - Adaptive harvest rounds: bump on empty+not-bottom, rollover; RAM hard-stop ends batch
  - caption = post; sub_caption = photo text; clear cache on resume
  - Legacy gallery arrow retained in git history only
"""
from __future__ import annotations

import json
import random
import re
import time
from typing import Any

from common.chau_ban_schema import utc_now_iso
from common.io_storage import (
    ensure_bucket,
    get_minio_client,
    object_exists,
    read_object_text,
    upload_json_payload,
)

from final_exam_nlp_crawl_runner import (
    DEFAULT_BOTTOM_YEAR,
    DEFAULT_GALLERY_BASELINE_FBID,
    DEFAULT_GALLERY_DIRECTION,
    DEFAULT_GALLERY_STEP_PAUSE_SEC,
    DEFAULT_GALLERY_WALK_COUNT,
    DEFAULT_MATCH_SOFT_RESTART_EVERY,
    DEFAULT_MAX_IMAGE_VALID,
    DEFAULT_MEDIA_BATCH_SIZE,
    DEFAULT_MEDIA_HARVEST_ROUNDS,
    DEFAULT_MEDIA_SOFT_RESTART_EVERY,
    DEFAULT_PERMALINK_PAUSE_SEC,
    DEFAULT_SCROLL_PAUSE_SEC,
    DEFAULT_TARGET_VALID_DELTA,
    FB_BASE_URL,
    MEDIA_HARVEST_ROUNDS_MAX,
    MEDIA_HARVEST_ROUNDS_STEP,
    SCHEMA_VERSION,
    SOURCE_NAME,
    _advance_media_scroll,
    _atomic_upload_json,
    _atomic_upload_text,
    _batch_meta_key,
    _batch_pending_key,
    _build_driver,
    _build_record,
    _classify_record,
    _close_dialogs,
    _collect_media_hrefs,
    _cookies_key,
    _dedupe_image_urls,
    _extract_from_dialog_or_page,
    _discover_story_permalink,
    _discover_story_permalink_from_html,
    _extract_meta_caption,
    _extract_scoped_cdn_urls,
    _clear_browser_caches,
    _harvest_media_fbids_after,
    _harvest_unseen_media_fbids,
    _normalize_fbid_list,
    _open_group_media_photos,
    _ram_guard_selenium,
    _seek_next_media_fbid,
    _seek_to_media_fbid,
    _click_view_post,
    _peek_view_post_href,
    _canonicalize_story_url,
    _gallery_advance_with_refresh,
    _gallery_arrow_step,
    _gallery_photo_url,
    _resolve_gallery_start_fbid,
    _has_story_junk_query,
    _is_story_permalink_url,
    _is_ui_noise_label,
    _is_content_caption,
    _navigate_photo_tile,
    _navigate_story_permalink,
    _photo_url_variants,
    _posts_to_permalink_url,
    _is_homepage_redirect,
    _is_logged_in,
    _require_logged_in_session,
    _wait_logged_in,
    _is_video_url,
    _load_posts_index,
    _meta_key,
    _persist_index_and_logs,
    _post_id_from_url,
    _progress_key,
    _rebuild_exports,
    _restore_cookies,
    _restore_session_profile_first,
    _warmup_facebook_session,
    _restore_media_scroll,
    _run_result_key,
    _selenium_preflight,
    _settings,
    _soft_restart_browser,
    _safe_quit_driver,
    _to_jsonl,
    _upsert_post,
    _year_from_posted_at,
    _is_recheckable_invalid,
    _is_recheck_eligible,
    _recheck_attempt_count,
    DEFAULT_INVALID_RECHECK_LIMIT,
    DEFAULT_INVALID_RECHECK_MAX_ATTEMPTS,
    DEFAULT_SKIPPED_RECHECK_LIMIT,
    DEFAULT_SKIPPED_RECHECK_MAX_ATTEMPTS,
    INVALID_RECHECK_EXHAUSTED_REASON,
    RECHECKABLE_INVALID_REASONS,
)


LOG = "[final_exam_nlp_two_phase]"


def _human_pause(base_sec: float, jitter_lo: float = 0.4, jitter_hi: float = 1.8) -> None:
    """Sleep with jitter to look less bot-like / Nghỉ có jitter để bớt giống bot."""
    time.sleep(max(0.5, float(base_sec) + random.uniform(jitter_lo, jitter_hi)))


def _reopen_photo_tile(
    driver,
    *,
    group_id: str,
    tile_id: str,
    photo_url: str,
    pause_sec: float,
) -> str:
    """Re-open the photo viewer after a bad story redirect.
    Mở lại photo viewer sau khi redirect story lỗi.
    """
    for url in _photo_url_variants(group_id, tile_id, photo_url):
        try:
            driver.get(url)
            _human_pause(pause_sec * 0.7, 0.3, 1.0)
            landed = driver.current_url or ""
        except Exception:
            landed = ""
        if landed and not _is_homepage_redirect(landed) and not _is_junk_facebook_url(landed):
            return landed
    return ""


def _apply_story_redirect_fallback(
    *,
    tile_id: str,
    photo_url: str,
    group_id: str,
    permalink_pause_sec: float,
    driver,
    sub_caption: str,
    image_urls: list[str],
    story_url: str,
    best_url: str,
    story_post_id: str,
) -> tuple[str, str, list[str], str, str, bool]:
    """Use photo-viewer snapshot when story navigation bounced to homepage.
    Dùng snapshot photo viewer khi navigate story bị đẩy về homepage.
    """
    caption = (sub_caption or "").strip()
    merged_images = list(image_urls)
    if not caption or len(merged_images) < 1:
        reopened = _reopen_photo_tile(
            driver,
            group_id=group_id,
            tile_id=tile_id,
            photo_url=photo_url,
            pause_sec=permalink_pause_sec,
        )
        if reopened:
            photo_extracted = _extract_from_dialog_or_page(driver, min_width=50)
            if not caption:
                caption = (photo_extracted.get("caption") or "").strip()
            if not caption:
                meta = _extract_meta_caption(driver)
                if meta and "facebook" not in meta.lower():
                    caption = meta
            merged_images = _dedupe_image_urls(
                merged_images
                + [u for u in (photo_extracted.get("images") or []) if not _is_video_url(u)]
                + _extract_scoped_cdn_urls(driver, max_images=8)
            )
    if not story_post_id and story_url:
        story_post_id = _post_id_from_url(story_url) or ""
    if story_url:
        best_url = _pick_best_url(story_url, best_url, photo_url, group_id=group_id)
    found = bool(caption.strip() and merged_images)
    if found:
        print(
            f"{LOG} story_redirect_fallback tile={tile_id} story={story_post_id or tile_id} "
            f"caption={caption[:60]!r} image_count={len(merged_images)}",
            flush=True,
        )
    return caption, "", merged_images, best_url, story_post_id, found


def _ensure_driver_login(
    *,
    driver,
    bucket: str,
    source_prefix: str,
    group_id: str,
    start_url: str,
    cookies: list[dict[str, Any]],
    fb_username: str,
    fb_password: str,
    fb_totp_secret: str,
    upload_minio: bool,
) -> list[dict[str, Any]]:
    """Profile-first login: reuse Chrome profile session across rollover jobs; MinIO cookies as fallback.

    Login ưu tiên profile Chrome (sống qua rollover); cookie MinIO chỉ fallback.
    """
    session_cookies = list(cookies or [])
    logged_in, session_source = _restore_session_profile_first(
        driver, session_cookies, log_prefix=LOG
    )
    # Profile or prior MinIO seed counts as an auth source for error messages /
    # Profile hoặc cookie MinIO cũ được coi là nguồn auth cho thông báo lỗi
    had_cookies = bool(session_cookies) or session_source == "profile"

    # Soft-land on group feed before photo URL (photo deep-link often drops session) /
    # Vào feed group trước rồi mới tới photo (deep-link ảnh hay làm mất session)
    group_url = f"https://www.facebook.com/groups/{group_id}"
    try:
        driver.get(group_url)
        _human_pause(2.5, 0.4, 1.2)
    except Exception as exc:
        print(f"{LOG} group_warmup_err={type(exc).__name__}", flush=True)
    if not _is_logged_in(driver) and session_source != "profile" and session_cookies:
        print(f"{LOG} session lost on group; re-restore MinIO cookies", flush=True)
        _restore_cookies(driver, session_cookies)
        _warmup_facebook_session(driver)
        try:
            driver.get(group_url)
            _human_pause(2.0, 0.3, 1.0)
        except Exception:
            pass

    try:
        driver.get(start_url)
    except Exception as exc:
        print(f"{LOG} start_url_nav_err={type(exc).__name__}", flush=True)
    _human_pause(3.0, 0.5, 1.5)
    if not _is_logged_in(driver):
        print(
            f"{LOG} session lost on start_url={(getattr(driver, 'current_url', '') or '')[:120]}; "
            "retry navigation",
            flush=True,
        )
        _wait_logged_in(driver, 5)
    if not _is_logged_in(driver) and session_source == "profile":
        # Profile had c_user at probe but dropped on deep-link — re-probe without inject /
        # Profile có c_user lúc probe nhưng rơi ở deep-link — thử lại không inject
        print(f"{LOG} profile session dropped on photo URL; re-open group feed", flush=True)
        try:
            driver.get(group_url)
            _human_pause(2.0, 0.3, 1.0)
            driver.get(start_url)
            _human_pause(2.5, 0.4, 1.0)
        except Exception:
            pass
        _wait_logged_in(driver, 8)
    elif not _is_logged_in(driver) and session_cookies:
        # MinIO fallback retry / Retry fallback MinIO
        _restore_cookies(driver, session_cookies)
        _warmup_facebook_session(driver)
        try:
            driver.get(group_url)
            _human_pause(2.0, 0.3, 1.0)
            driver.get(start_url)
            _human_pause(2.5, 0.4, 1.0)
        except Exception:
            pass
        _wait_logged_in(driver, 8)
    _require_logged_in_session(
        driver,
        had_cookies=had_cookies,
        fb_username=fb_username,
        fb_password=fb_password,
        fb_totp_secret=fb_totp_secret,
        log_prefix=LOG,
    )
    if upload_minio and _is_logged_in(driver):
        try:
            session_cookies = driver.get_cookies() or session_cookies
        except Exception:
            pass
        upload_json_payload(
            bucket,
            _cookies_key(source_prefix, group_id),
            {"saved_at": utc_now_iso(), "cookies": session_cookies},
        )
    try:
        session_cookies = driver.get_cookies() or session_cookies
    except Exception:
        pass
    print(f"{LOG} logged in, opened {driver.current_url}", flush=True)
    return session_cookies


def _permalink_candidates(group_id: str, post_id: str, photo_url: str = "") -> list[str]:
    """Open media tile first; story permalink is discovered from the page.
    Mở ô media trước; permalink bài viết được tìm từ trang đang mở.
    """
    urls: list[str] = []
    if photo_url:
        urls.append(photo_url)
    return list(dict.fromkeys(u for u in urls if u))


def _return_to_gallery_photo(
    driver,
    *,
    group_id: str,
    tile_id: str,
    pause_sec: float,
) -> str:
    """Re-open group album photo so Arrow Right still works.
    Mở lại ảnh album group để Arrow Right vẫn chạy được.
    """
    reopen = _gallery_photo_url(group_id, tile_id)
    if not reopen:
        return ""
    try:
        landed = driver.current_url or ""
    except Exception:
        landed = ""
    if "/photo" in landed.lower() and tile_id and tile_id in landed:
        return landed
    print(f"{LOG} gallery_return_photo fbid={tile_id} from={landed[:90]}", flush=True)
    try:
        driver.get(reopen)
        _human_pause(pause_sec, 0.3, 1.0)
        return driver.current_url or reopen
    except Exception:
        return reopen


def _open_and_extract_post(
    driver,
    *,
    group_id: str,
    tile_id: str,
    photo_url: str,
    permalink_pause_sec: float,
    skip_navigation: bool = False,
    reuse_caption: str = "",
    reuse_story_url: str = "",
    photo_only: bool = False,
) -> dict[str, Any]:
    """Photo tile → click 'Xem bài viết' → post caption + images; photo text → sub_caption.
    Ô ảnh → bấm 'Xem bài viết' → caption bài + ảnh; chữ trên ảnh → sub_caption.
    """
    caption = ""
    sub_caption = ""
    image_urls: list[str] = []
    author = ""
    posted_at: str | None = None
    story_post_id = ""
    story_url = ""
    best_url = photo_url
    found = False
    last_current_url = ""
    tried_urls: list[str] = []
    clicked = False

    empty = {
        "caption": "",
        "sub_caption": "",
        "image_urls": [],
        "author": "",
        "posted_at": None,
        "story_post_id": "",
        "story_url": "",
        "best_url": "",
        "found": False,
        "last_current_url": "",
        "tried_urls": [],
        "view_post_clicked": False,
    }

    if not photo_url and not skip_navigation:
        return empty

    if skip_navigation:
        # Gallery walk: already on photo viewer — extract in place /
        # Gallery walk: đã ở photo viewer — extract tại chỗ
        try:
            last_current_url = driver.current_url or photo_url
        except Exception:
            last_current_url = photo_url or ""
        if not tile_id:
            tile_id = _post_id_from_url(last_current_url) or tile_id
        if not photo_url:
            photo_url = last_current_url
        nav_strategy = "gallery_in_place"
        _human_pause(1.0, 0.2, 0.5)
        if _is_junk_facebook_url(last_current_url) or _is_homepage_redirect(last_current_url):
            return {
                **empty,
                "best_url": photo_url,
                "last_current_url": last_current_url,
                "tried_urls": tried_urls,
                "invalid_reason": "redirect_homepage",
                "nav_strategy": nav_strategy,
            }
    else:
        tried_urls.extend(_photo_url_variants(group_id, tile_id, photo_url))
        last_current_url, nav_strategy = _navigate_photo_tile(
            driver,
            group_id=group_id,
            tile_id=tile_id,
            photo_url=photo_url,
            pause_sec=permalink_pause_sec,
            log_prefix=LOG,
        )
        # Do NOT ESC-close here — photo viewer is often role=dialog and holds "Xem bài viết" /
        # Không ESC đóng ở đây — photo viewer thường là role=dialog và chứa "Xem bài viết"
        _human_pause(1.5, 0.3, 0.8)

        if _is_junk_facebook_url(last_current_url) or _is_homepage_redirect(last_current_url):
            print(
                f"{LOG} redirect_junk tile={tile_id} strategy={nav_strategy} "
                f"tried={photo_url[:90]} landed={last_current_url}",
                flush=True,
            )
            return {
                **empty,
                "best_url": photo_url,
                "last_current_url": last_current_url,
                "tried_urls": tried_urls,
                "invalid_reason": "redirect_homepage",
                "nav_strategy": nav_strategy,
            }

    photo_extracted = _extract_from_dialog_or_page(driver, min_width=50)
    # Photo-viewer text becomes sub_caption (per-image) /
    # Chữ trên photo viewer thành sub_caption (theo từng ảnh)
    sub_caption = (photo_extracted.get("caption") or "").strip()
    if not sub_caption:
        meta_on_photo = _extract_meta_caption(driver)
        if meta_on_photo and "facebook" not in meta_on_photo.lower():
            sub_caption = meta_on_photo

    image_urls = _dedupe_image_urls(
        [u for u in (photo_extracted.get("images") or []) if not _is_video_url(u)]
        + _extract_scoped_cdn_urls(driver, max_images=5)
    )
    author = photo_extracted.get("author") or ""
    posted_at = photo_extracted.get("posted_at")
    best_url = _pick_best_url(
        photo_extracted.get("post_url") or "",
        last_current_url,
        photo_url,
        group_id=group_id,
    )

    # Recheck path: photo viewer only — skip story nav that often redirects /
    # Recheck: chỉ photo viewer — bỏ navigate story hay bị redirect
    if photo_only:
        caption = (photo_extracted.get("caption") or "").strip()
        if not caption and sub_caption and not _is_ui_noise_label(sub_caption):
            caption = sub_caption
            sub_caption = ""
        if not caption:
            meta_on_photo = _extract_meta_caption(driver)
            if meta_on_photo and not _is_ui_noise_label(meta_on_photo):
                caption = meta_on_photo
        story_url = _canonicalize_story_url(
            _discover_story_permalink(driver, group_id), group_id
        ) or ""
        story_url = _posts_to_permalink_url(story_url, group_id) or story_url
        story_post_id = _post_id_from_url(story_url) or tile_id
        best_url = _pick_best_url(story_url, best_url, photo_url, group_id=group_id)
        if caption and _is_ui_noise_label(caption):
            caption = ""
        found = bool(caption.strip() and image_urls)
        return {
            "caption": caption,
            "sub_caption": sub_caption,
            "image_urls": image_urls,
            "author": author,
            "posted_at": posted_at,
            "story_post_id": story_post_id,
            "story_url": story_url,
            "best_url": best_url,
            "found": found,
            "last_current_url": last_current_url,
            "tried_urls": tried_urls,
            "view_post_clicked": False,
            "nav_strategy": "photo_only_recheck",
        }

    if skip_navigation:
        # Gallery: keep photo CDN; click View post unless this story already has caption /
        # Gallery: giữ ảnh CDN; bấm Xem bài viết trừ khi story đã có caption
        if sub_caption and _is_ui_noise_label(sub_caption):
            sub_caption = ""
        view_href = _peek_view_post_href(driver)
        peeked_story = _canonicalize_story_url(view_href, group_id) or ""
        peeked_story = _posts_to_permalink_url(peeked_story, group_id) or peeked_story
        reuse = (reuse_caption or "").strip()
        if reuse and _is_ui_noise_label(reuse):
            reuse = ""
        if reuse:
            story_url = (
                _canonicalize_story_url(reuse_story_url, group_id)
                or peeked_story
                or story_url
            )
            story_url = _posts_to_permalink_url(story_url, group_id) or story_url
            caption = reuse
            story_post_id = (
                _post_id_from_url(story_url)
                or _post_id_from_url(last_current_url)
                or tile_id
            )
            best_url = _pick_best_url(story_url, last_current_url, photo_url, group_id=group_id)
            found = bool(caption and image_urls)
            print(
                f"{LOG} gallery_reuse_story tile={tile_id} story={story_post_id} "
                f"images={len(image_urls)} caption={caption[:50]!r}",
                flush=True,
            )
            return {
                "caption": caption,
                "sub_caption": sub_caption,
                "image_urls": image_urls,
                "author": author,
                "posted_at": posted_at,
                "story_post_id": story_post_id,
                "story_url": story_url,
                "best_url": best_url,
                "found": found,
                "last_current_url": last_current_url,
                "tried_urls": tried_urls,
                "view_post_clicked": False,
                "nav_strategy": "gallery_reuse_story",
            }
        print(
            f"{LOG} gallery_click_view_post tile={tile_id} "
            f"peek={(peeked_story or view_href or '-')[:80]}",
            flush=True,
        )

    # Primary path: click "Xem bài viết" banner on photo viewer /
    # Đường chính: bấm banner "Xem bài viết" trên photo viewer
    clicked, view_href = _click_view_post(driver, pause_sec=permalink_pause_sec)
    if clicked:
        try:
            last_current_url = driver.current_url or last_current_url
        except Exception:
            pass
        # Prefer href from the button, then landed URL, only if real permalink /
        # Ưu tiên href nút, rồi URL landed, chỉ khi là permalink thật
        story_url = ""
        for candidate in (view_href, last_current_url):
            canon = _canonicalize_story_url(candidate, group_id)
            if canon:
                story_url = canon
                break
        if not story_url:
            story_url = _discover_story_permalink(driver, group_id)
        story_url = _canonicalize_story_url(story_url, group_id) or story_url
        story_url = _posts_to_permalink_url(story_url, group_id) or story_url
        # If click stayed on photo, or landed on a comment deep-link, open the clean post /
        # Nếu vẫn ở ảnh, hoặc land comment deep-link, mở bài sạch
        need_story_nav = bool(story_url) and (
            not _is_story_permalink_url(last_current_url, group_id)
            or _has_story_junk_query(last_current_url)
        )
        if need_story_nav:
            last_current_url, _ = _navigate_story_permalink(
                driver, story_url, group_id, permalink_pause_sec
            )
        print(
            f"{LOG} view_post_clicked tile={tile_id} "
            f"href={(view_href or '-')[:80]} landed={last_current_url[:90]} "
            f"story={(story_url or '-')[:80]}",
            flush=True,
        )
    else:
        story_url = _canonicalize_story_url(
            _discover_story_permalink(driver, group_id), group_id
        )
        story_url = _posts_to_permalink_url(story_url, group_id) or story_url
        if story_url:
            print(
                f"{LOG} story_link tile={tile_id} discovered={story_url[:90]}",
                flush=True,
            )
        else:
            print(f"{LOG} no_view_post tile={tile_id} (banner not found)", flush=True)

    on_story = _is_story_permalink_url(story_url, group_id) or _is_story_permalink_url(
        last_current_url, group_id
    )
    if on_story and not _is_story_permalink_url(story_url, group_id):
        story_url = _canonicalize_story_url(last_current_url, group_id) or last_current_url
    story_url = _canonicalize_story_url(story_url, group_id) or story_url
    story_url = _posts_to_permalink_url(story_url, group_id) or story_url

    if on_story:
        story_post_id = _post_id_from_url(story_url) or _post_id_from_url(last_current_url)
        if story_url and story_url not in tried_urls:
            tried_urls.append(story_url)
        # Re-open if not on the story yet, or if the landed URL still has comment_id /
        # Mở lại nếu chưa vào bài, hoặc URL landed vẫn còn comment_id
        need_nav = (
            not _is_story_permalink_url(last_current_url, group_id)
            or _has_story_junk_query(last_current_url)
            or _is_homepage_redirect(last_current_url)
        )
        if need_nav and story_url:
            last_current_url, nav_ok = _navigate_story_permalink(
                driver, story_url, group_id, permalink_pause_sec
            )
            if not nav_ok:
                last_current_url = last_current_url or ""

        on_story_page = _is_story_permalink_url(last_current_url, group_id) and not _is_homepage_redirect(
            last_current_url
        )
        if on_story_page:
            story_extracted = _extract_from_dialog_or_page(driver, min_width=50)
            caption = (story_extracted.get("caption") or "").strip()
            if not caption:
                caption = _extract_meta_caption(driver)
            if not caption and sub_caption:
                # No separate post caption — use photo text as label /
                # Không có caption bài riêng — dùng chữ ảnh làm label
                caption = sub_caption
                sub_caption = ""
            image_urls = _dedupe_image_urls(
                image_urls
                + [u for u in (story_extracted.get("images") or []) if not _is_video_url(u)]
                + _extract_scoped_cdn_urls(driver, max_images=12)
            )
            author = story_extracted.get("author") or author
            posted_at = story_extracted.get("posted_at") or posted_at
            best_url = _pick_best_url(
                story_url,
                story_extracted.get("post_url") or "",
                last_current_url,
                best_url,
                group_id=group_id,
            )
            story_post_id = _post_id_from_url(best_url) or story_post_id
            found = bool(caption or image_urls)
        else:
            print(
                f"{LOG} story_redirect_junk tile={tile_id} story={(story_url or '')[:90]} "
                f"landed={last_current_url}",
                flush=True,
            )
            caption, sub_caption, image_urls, best_url, story_post_id, found = (
                _apply_story_redirect_fallback(
                    tile_id=tile_id,
                    photo_url=photo_url,
                    group_id=group_id,
                    permalink_pause_sec=permalink_pause_sec,
                    driver=driver,
                    sub_caption=sub_caption,
                    image_urls=image_urls,
                    story_url=story_url,
                    best_url=best_url,
                    story_post_id=story_post_id,
                )
            )
    else:
        # Stay on photo: use photo text as post caption if no story link /
        # Ở lại ảnh: dùng chữ ảnh làm caption post nếu không có link bài
        if sub_caption and not _is_ui_noise_label(sub_caption):
            caption = sub_caption
            sub_caption = ""
            story_post_id = _post_id_from_url(best_url) or tile_id
            found = True
        else:
            if sub_caption and _is_ui_noise_label(sub_caption):
                sub_caption = ""
            found = bool(caption.strip() and image_urls)

    if skip_navigation and tile_id:
        last_current_url = _return_to_gallery_photo(
            driver,
            group_id=group_id,
            tile_id=tile_id,
            pause_sec=permalink_pause_sec,
        ) or last_current_url

    if caption and _is_ui_noise_label(caption):
        caption = ""
        found = bool(caption and image_urls)

    return {
        "caption": caption,
        "sub_caption": sub_caption,
        "image_urls": image_urls,
        "author": author,
        "posted_at": posted_at,
        "story_post_id": story_post_id,
        "story_url": story_url,
        "best_url": best_url,
        "found": found,
        "last_current_url": last_current_url,
        "tried_urls": tried_urls,
        "view_post_clicked": clicked,
    }


def _extract_photo_cdn_urls(driver) -> list[str]:
    """Collect scontent CDN urls from the open page (no width filter).
    Lấy URL CDN scontent từ trang đang mở (không lọc theo width).
    """
    script = """
    const out = [];
    const seen = new Set();
    const add = (u) => {
      if (!u || typeof u !== 'string') return;
      if (!u.startsWith('http') || !u.includes('scontent')) return;
      if (/p24x24|p32x32|p50x50|s32x32|s60x60|p64x64/.test(u)) return;
      const clean = u.split('?')[0];
      if (seen.has(clean)) return;
      seen.add(clean);
      out.push(u);
    };
    for (const img of document.querySelectorAll('img')) {
      add(img.currentSrc || img.src);
      const ss = img.getAttribute('srcset') || '';
      for (const part of ss.split(',')) add(part.trim().split(' ')[0]);
    }
    for (const el of document.querySelectorAll('[style*="scontent"]')) {
      const m = (el.getAttribute('style') || '').match(/url\\([\"']?(https:[^\"')]+)/);
      if (m) add(m[1]);
    }
    return out;
    """
    try:
        urls = driver.execute_script(script) or []
    except Exception:
        urls = []
    return _dedupe_image_urls(
        [u for u in urls if isinstance(u, str) and not _is_video_url(u)]
    )


def _is_junk_facebook_url(url: str) -> bool:
    """True for homepage / login / empty redirects (not a real post).
    True với homepage / login / redirect rỗng (không phải bài thật).
    """
    u = (url or "").strip().rstrip("/")
    if not u:
        return True
    lower = u.lower()
    if lower in {
        FB_BASE_URL.lower(),
        f"{FB_BASE_URL.lower()}/",
        "https://facebook.com",
        "https://m.facebook.com",
    }:
        return True
    # Bare host with no post/photo markers / Host trần không có marker bài/ảnh
    if re.search(r"^https?://(www\.|m\.)?facebook\.com/?$", lower):
        return True
    if any(x in lower for x in ("/login", "/checkpoint", "/recover", "login.php")):
        return True
    return False


def _is_usable_post_url(url: str) -> bool:
    """URL must look like photo/post/permalink with an id.
    URL phải giống photo/post/permalink và có id.
    """
    if _is_junk_facebook_url(url):
        return False
    lower = (url or "").lower()
    return any(
        marker in lower
        for marker in (
            "/photo",
            "/posts/",
            "/permalink",
            "story_fbid=",
            "fbid=",
            "pfbid",
            "set=g.",
            "set=p.",
        )
    )


def _pick_best_url(*candidates: str, group_id: str = "") -> str:
    """Prefer clean group permalinks, then photo URLs, never homepage.
    Ưu tiên permalink group sạch, rồi URL ảnh, không lấy homepage.
    """
    for url in candidates:
        canon = _canonicalize_story_url(url, group_id)
        if canon:
            return canon
    for url in candidates:
        if url and _is_usable_post_url(url):
            return url
    for url in candidates:
        if url and not _is_junk_facebook_url(url):
            return url
    return next((u for u in candidates if u), "")


def _resolve_story_post_id(driver, fallback_id: str = "") -> str:
    """Read story post id from current URL after navigation.
    Đọc story post id từ URL hiện tại sau khi điều hướng.
    """
    try:
        current = driver.current_url or ""
    except Exception:
        current = ""
    if _is_junk_facebook_url(current):
        return fallback_id
    story = _post_id_from_url(current)
    if "/posts/" in current or "/permalink/" in current or "story_fbid=" in current:
        return story or fallback_id
    return story or fallback_id


def run_media_image_batch(
    *,
    group_id: str,
    media_batch_size: int = DEFAULT_MEDIA_BATCH_SIZE,
    max_image_valid_count: int = DEFAULT_MAX_IMAGE_VALID,
    bottom_year: int = DEFAULT_BOTTOM_YEAR,
    scroll_pause_sec: float = DEFAULT_SCROLL_PAUSE_SEC,
    stall_limit: int = 10,
    headless: bool = True,
    upload_minio: bool = True,
    download_images: bool = False,
    force: bool = False,
    fb_username: str = "",
    fb_password: str = "",
    fb_totp_secret: str = "",
    run_id: str | None = None,
    soft_restart_every: int = DEFAULT_MEDIA_SOFT_RESTART_EVERY,
) -> dict[str, Any]:
    """Task 1: scroll media grid and collect tile_ids only (no image download).
    Task 1: cuộn lưới media chỉ lấy tile_id (không tải ảnh).
    """
    _ = download_images
    if not group_id.strip():
        raise ValueError("group_id is required")
    if media_batch_size < 1:
        raise ValueError("media_batch_size must be >= 1")

    settings = _settings()
    bucket = settings["bucket_raw"]
    source_prefix = settings["source_prefix"]
    remote_url = settings["selenium_remote_url"]
    page_load_timeout = int(settings["page_load_timeout_sec"])
    resolved_run_id = (run_id or "").strip() or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    batch_id = f"media_{resolved_run_id}"

    if upload_minio:
        ensure_bucket(get_minio_client(), bucket)

    progress = _read_progress(bucket, source_prefix, group_id)
    posts = _load_posts_index(bucket, source_prefix, group_id) if upload_minio else {}
    media_seen: set[str] = set(progress.get("media_seen_ids") or [])
    if force:
        media_seen = set()
    media_scroll_y = 0 if force else int(progress.get("media_scroll_y") or 0)

    cookies_payload = (
        _read_json_safe(bucket, _cookies_key(source_prefix, group_id)) if upload_minio else None
    )
    cookies = list((cookies_payload or {}).get("cookies") or [])

    media_url = f"{FB_BASE_URL}/groups/{group_id}/media/photos"
    media_fallback_url = f"{FB_BASE_URL}/groups/{group_id}/media"
    pending_rows: list[dict[str, Any]] = []
    upserts: list[dict[str, Any]] = []
    stall_rounds = 0
    scroll_round = 0
    stop_reason = "media_batch_full"
    media_exhausted = False
    session_cookies = cookies

    _selenium_preflight(remote_url)
    driver = _build_driver(remote_url, headless, page_load_timeout)
    try:
        session_cookies = _ensure_driver_login(
            driver=driver,
            bucket=bucket,
            source_prefix=source_prefix,
            group_id=group_id,
            start_url=media_url,
            cookies=cookies,
            fb_username=fb_username,
            fb_password=fb_password,
            fb_totp_secret=fb_totp_secret,
            upload_minio=upload_minio,
        )
        if "photo" not in driver.current_url.lower():
            try:
                driver.get(media_fallback_url)
                _human_pause(2.0, 0.3, 1.0)
            except Exception:
                pass

        print(
            f"{LOG} media_batch start schema={SCHEMA_VERSION} batch_id={batch_id} "
            f"size={media_batch_size} scrollY={media_scroll_y} seen={len(media_seen)} "
            f"pause={scroll_pause_sec}s",
            flush=True,
        )

        while len(pending_rows) < media_batch_size:
            scroll_round += 1
            _close_dialogs(driver)
            if "media" not in driver.current_url:
                driver.get(media_url)
                _human_pause(2.0, 0.3, 1.0)
                if media_scroll_y > 200:
                    _restore_media_scroll(driver, media_scroll_y, scroll_pause_sec)
            elif media_scroll_y > 200:
                _restore_media_scroll(driver, media_scroll_y, scroll_pause_sec)

            grew, scrolled_y = _advance_media_scroll(driver, scroll_pause_sec, steps=3)
            media_scroll_y = max(media_scroll_y, scrolled_y) + 900
            _human_pause(1.2, 0.2, 0.8)

            hrefs = _collect_media_hrefs(driver)
            new_in_round = 0
            for href in hrefs:
                if len(pending_rows) >= media_batch_size:
                    break
                if _is_video_url(href):
                    continue
                tile_id = _post_id_from_url(href)
                if not tile_id:
                    continue
                if tile_id in media_seen:
                    continue
                existing = posts.get(tile_id)
                if existing and existing.get("match_status") in {"valid", "invalid"}:
                    media_seen.add(tile_id)
                    continue
                already_matched = False
                for rec in posts.values():
                    tids = set(rec.get("tile_ids") or [])
                    if tile_id in tids and rec.get("match_status") in {"valid", "invalid"}:
                        already_matched = True
                        break
                if already_matched:
                    media_seen.add(tile_id)
                    continue

                record = _build_record(
                    post_id=tile_id,
                    post_url=href,
                    author="",
                    label="",
                    image_urls=[],
                    posted_at=None,
                    group_id=group_id,
                    run_id=resolved_run_id,
                    phase="media_batch",
                    image_local_keys=[],
                    images_downloaded=False,
                    images_download_skipped=True,
                    is_valid=False,
                    invalid_reason="pending_caption",
                    tile_id=tile_id,
                    tile_href=href,
                    story_post_id="",
                )
                record["match_status"] = "pending"
                record["photo_url"] = href
                record["batch_id"] = batch_id

                _, merged = _upsert_post(posts, record)
                pending_rows.append(merged)
                upserts.append(
                    {
                        "event": "pending_tile",
                        "tile_id": tile_id,
                        "batch_id": batch_id,
                        "run_id": resolved_run_id,
                        "at": utc_now_iso(),
                        "record": merged,
                    }
                )
                media_seen.add(tile_id)
                new_in_round += 1
                print(
                    f"{LOG} +pending_tile {len(pending_rows)}/{media_batch_size} "
                    f"tile={tile_id} url={href}",
                    flush=True,
                )

            print(
                f"{LOG} media round={scroll_round} tiles={len(hrefs)} "
                f"new_ids={new_in_round} pending={len(pending_rows)}/{media_batch_size} "
                f"scrollY≈{media_scroll_y}",
                flush=True,
            )

            if upload_minio:
                _write_media_checkpoint(
                    bucket=bucket,
                    source_prefix=source_prefix,
                    group_id=group_id,
                    batch_id=batch_id,
                    run_id=resolved_run_id,
                    media_scroll_y=media_scroll_y,
                    media_seen=media_seen,
                    posts=posts,
                    upserts=upserts,
                    pending_rows=pending_rows,
                    media_batch_size=media_batch_size,
                    media_exhausted=False,
                    should_continue=False,
                    stop_reason="media_in_progress",
                    max_image_valid_count=max_image_valid_count,
                    bottom_year=bottom_year,
                    session_cookies=session_cookies,
                    driver=driver,
                )
                upserts.clear()

            if new_in_round == 0 and not grew:
                stall_rounds += 1
            else:
                stall_rounds = 0
            if stall_rounds >= stall_limit:
                stop_reason = "media_exhausted"
                media_exhausted = True
                break

            if scroll_round % 4 == 0:
                _human_pause(5.0, 1.0, 3.0)

            if scroll_round > 0 and scroll_round % max(1, soft_restart_every) == 0:
                try:
                    session_cookies = driver.get_cookies() or session_cookies
                except Exception:
                    pass
                driver = _soft_restart_browser(
                    driver=driver,
                    remote_url=remote_url,
                    headless=headless,
                    page_load_timeout=page_load_timeout,
                    group_url=media_url,
                    cookies=session_cookies,
                    reason="media tile batch soft-restart",
                    fb_username=fb_username,
                    fb_password=fb_password,
                    fb_totp_secret=fb_totp_secret,
                )
                _human_pause(3.0, 0.5, 1.5)
                if media_scroll_y > 200:
                    _restore_media_scroll(driver, media_scroll_y, scroll_pause_sec)

        if len(pending_rows) >= media_batch_size:
            stop_reason = "media_batch_full"
        elif not media_exhausted:
            stop_reason = "media_batch_partial"

    finally:
        try:
            session_cookies = driver.get_cookies() or session_cookies
        except Exception:
            pass
        _safe_quit_driver(driver)

    if upload_minio:
        _write_media_checkpoint(
            bucket=bucket,
            source_prefix=source_prefix,
            group_id=group_id,
            batch_id=batch_id,
            run_id=resolved_run_id,
            media_scroll_y=media_scroll_y,
            media_seen=media_seen,
            posts=posts,
            upserts=upserts,
            pending_rows=pending_rows,
            media_batch_size=media_batch_size,
            media_exhausted=media_exhausted,
            should_continue=False,
            stop_reason=stop_reason,
            max_image_valid_count=max_image_valid_count,
            bottom_year=bottom_year,
            session_cookies=session_cookies,
            driver=None,
            finalize=True,
        )

    result = {
        "schema_version": SCHEMA_VERSION,
        "phase": "media_batch",
        "group_id": group_id,
        "run_id": resolved_run_id,
        "batch_id": batch_id,
        "pending_count": len(pending_rows),
        "media_batch_size": media_batch_size,
        "media_scroll_y": media_scroll_y,
        "media_exhausted": media_exhausted,
        "stop_reason": stop_reason,
        "should_continue": False,
        "download_images": False,
    }
    print(f"{LOG} media_batch done {result}", flush=True)
    return result


def run_caption_permalink_match(
    *,
    group_id: str,
    batch_id: str | None = None,
    target_valid_delta: int = DEFAULT_TARGET_VALID_DELTA,
    max_image_valid_count: int = DEFAULT_MAX_IMAGE_VALID,
    bottom_year: int = DEFAULT_BOTTOM_YEAR,
    headless: bool = True,
    upload_minio: bool = True,
    fb_username: str = "",
    fb_password: str = "",
    fb_totp_secret: str = "",
    run_id: str | None = None,
    permalink_pause_sec: float = DEFAULT_PERMALINK_PAUSE_SEC,
    soft_restart_every: int = DEFAULT_MATCH_SOFT_RESTART_EVERY,
) -> dict[str, Any]:
    """Task 2: permalink by tile → caption + image URLs; upsert by story_post_id.
    Task 2: permalink theo tile → caption + URL ảnh; upsert theo story_post_id.
    """
    _ = max_image_valid_count
    if not group_id.strip():
        raise ValueError("group_id is required")

    settings = _settings()
    bucket = settings["bucket_raw"]
    source_prefix = settings["source_prefix"]
    remote_url = settings["selenium_remote_url"]
    page_load_timeout = int(settings["page_load_timeout_sec"])
    resolved_run_id = (run_id or "").strip() or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())

    if upload_minio:
        ensure_bucket(get_minio_client(), bucket)

    progress = _read_progress(bucket, source_prefix, group_id)
    resolved_batch = (batch_id or "").strip() or str(progress.get("last_batch_id") or "")
    if not resolved_batch:
        raise RuntimeError("No batch_id: run media batch first or set FEN_BATCH_ID")

    posts = _load_posts_index(bucket, source_prefix, group_id)
    pending_key = _batch_pending_key(source_prefix, group_id, resolved_batch)
    if not object_exists(bucket, pending_key):
        raise RuntimeError(f"Missing pending batch: {pending_key}")

    pending_rows = _load_jsonl_records(bucket, pending_key)
    work: list[dict[str, Any]] = []
    for row in pending_rows:
        tile_id = str(row.get("tile_id") or row.get("post_id") or "")
        if not tile_id:
            continue
        current = posts.get(tile_id) or row
        for rec in posts.values():
            if tile_id in set(rec.get("tile_ids") or []) or rec.get("tile_id") == tile_id:
                current = rec
                break
        # Still process pending tiles so more photo URLs merge into the story /
        # Vẫn xử lý tile pending để gộp thêm URL ảnh vào story
        status = current.get("match_status")
        if status == "pending" or current.get("invalid_reason") == "pending_caption":
            work.append(current if current.get("post_id") or current.get("tile_id") else row)
        elif status == "valid" and tile_id != str(current.get("story_post_id") or ""):
            # Extra media tiles of an already-valid post → merge more image URLs /
            # Tile media phụ của bài đã valid → gộp thêm URL ảnh
            work.append(row if row.get("tile_id") else current)

    cookies_payload = _read_json_safe(bucket, _cookies_key(source_prefix, group_id))
    cookies = list((cookies_payload or {}).get("cookies") or [])
    media_url = f"{FB_BASE_URL}/groups/{group_id}/media/photos"

    run_valid = 0
    run_invalid = 0
    merge_hits = 0
    bottom_hits = 0
    upserts: list[dict[str, Any]] = []
    media_exhausted = bool(progress.get("media_exhausted"))
    media_scroll_y = int(progress.get("media_scroll_y") or 0)
    media_seen = set(progress.get("media_seen_ids") or [])
    session_cookies = cookies

    _selenium_preflight(remote_url)
    driver = _build_driver(remote_url, headless, page_load_timeout)
    try:
        session_cookies = _ensure_driver_login(
            driver=driver,
            bucket=bucket,
            source_prefix=source_prefix,
            group_id=group_id,
            start_url=media_url,
            cookies=cookies,
            fb_username=fb_username,
            fb_password=fb_password,
            fb_totp_secret=fb_totp_secret,
            upload_minio=upload_minio,
        )
        print(
            f"{LOG} caption_match schema={SCHEMA_VERSION} batch={resolved_batch} "
            f"pending={len(work)} pause={permalink_pause_sec}s (no download)",
            flush=True,
        )

        for index, row in enumerate(work, start=1):
            if run_valid >= target_valid_delta:
                break
            # Re-warm session if Facebook dropped cookies mid-run /
            # Làm nóng lại session nếu Facebook mất cookie giữa chừng
            if not _is_logged_in(driver):
                print(f"{LOG} session lost before tile {index}; re-login", flush=True)
                session_cookies = _ensure_driver_login(
                    driver=driver,
                    bucket=bucket,
                    source_prefix=source_prefix,
                    group_id=group_id,
                    start_url=media_url,
                    cookies=session_cookies,
                    fb_username=fb_username,
                    fb_password=fb_password,
                    fb_totp_secret=fb_totp_secret,
                    upload_minio=upload_minio,
                )
            tile_id = str(row.get("tile_id") or row.get("post_id") or "")
            photo_url = str(
                row.get("tile_href") or row.get("photo_url") or row.get("permalink") or ""
            )
            extracted = _open_and_extract_post(
                driver,
                group_id=group_id,
                tile_id=tile_id,
                photo_url=photo_url,
                permalink_pause_sec=permalink_pause_sec,
            )
            caption = extracted.get("caption") or ""
            sub_caption = extracted.get("sub_caption") or ""
            image_urls = extracted.get("image_urls") or []
            author = extracted.get("author") or ""
            posted_at = extracted.get("posted_at")
            best_url = extracted.get("best_url") or photo_url
            found = bool(extracted.get("found"))
            story_post_id = extracted.get("story_post_id") or ""
            last_current_url = extracted.get("last_current_url") or ""
            tried_urls = extracted.get("tried_urls") or []
            story_url = extracted.get("story_url") or ""
            story_url = _canonicalize_story_url(story_url, group_id) or story_url

            # Never persist homepage as permalink — keep the tile photo URL /
            # Không lưu homepage làm permalink — giữ URL ô ảnh
            best_url = _pick_best_url(
                best_url, story_url, photo_url, *(tried_urls or []), group_id=group_id
            )
            image_urls = _dedupe_image_urls(image_urls)

            # Prefer caption already stored on the story post /
            # Ưu tiên caption đã lưu trên bài story
            prior = None
            if story_post_id and story_post_id in posts:
                prior = posts.get(story_post_id)
            elif tile_id in posts:
                prior = posts.get(tile_id)
            if prior and not caption.strip():
                caption = (prior.get("label") or "").strip()
            if prior:
                image_urls = _dedupe_image_urls(
                    list(prior.get("image_urls") or []) + image_urls
                )

            is_valid, invalid_reason = _classify_record(caption, image_urls)
            if extracted.get("invalid_reason") == "redirect_homepage":
                is_valid = False
                invalid_reason = "redirect_homepage"
            elif not caption.strip():
                is_valid = False
                invalid_reason = "missing_caption"
            elif not image_urls:
                is_valid = False
                invalid_reason = "missing_image"

            already_valid = bool(prior and prior.get("match_status") == "valid")
            if is_valid:
                if not already_valid:
                    run_valid += 1
                    merge_hits += 1
                match_status = "valid"
                print(
                    f"{LOG} +valid {run_valid}/{target_valid_delta} "
                    f"tile={tile_id} story={story_post_id or tile_id} "
                    f"image_count={len(image_urls)} url={best_url} "
                    f"story_url={story_url[:80] if story_url else '-'} "
                    f"label={caption[:50]!r} "
                    f"sub={sub_caption[:40]!r}"
                    f"{' (merge_images)' if already_valid else ''}",
                    flush=True,
                )
            else:
                run_invalid += 1
                match_status = "invalid"
                print(
                    f"{LOG} +invalid {run_invalid} tile={tile_id} "
                    f"story={story_post_id or tile_id} reason={invalid_reason} "
                    f"image_count={len(image_urls)} url={best_url} "
                    f"story_url={story_url[:80] if story_url else '-'} "
                    f"landed={last_current_url or '-'} resolved={found}",
                    flush=True,
                )

            year = _year_from_posted_at(posted_at if isinstance(posted_at, str) else None)
            if year is not None and year < bottom_year:
                bottom_hits += 1

            record = _build_record(
                post_id=story_post_id or tile_id,
                post_url=best_url,
                author=author,
                label=caption,
                image_urls=image_urls,
                posted_at=posted_at if isinstance(posted_at, str) else None,
                group_id=group_id,
                run_id=resolved_run_id,
                phase="caption_match",
                image_local_keys=[],
                images_downloaded=False,
                images_download_skipped=True,
                is_valid=is_valid,
                invalid_reason=None if is_valid else invalid_reason,
                tile_id=tile_id,
                tile_href=photo_url,
                story_post_id=story_post_id,
                sub_caption=sub_caption,
            )
            record["match_status"] = match_status
            record["batch_id"] = resolved_batch
            record["photo_url"] = photo_url
            record["permalink_resolved"] = found
            record["story_url"] = story_url or None
            record["image_count"] = len(image_urls)
            _, merged = _upsert_post(posts, record)
            post_image_count = int(merged.get("image_count") or len(merged.get("image_urls") or []))
            print(
                f"{LOG} post_images story={merged.get('story_post_id') or story_post_id or tile_id} "
                f"image_count={post_image_count} tiles={len(merged.get('tile_ids') or [])}",
                flush=True,
            )
            upserts.append(
                {
                    "event": "match",
                    "tile_id": tile_id,
                    "story_post_id": story_post_id or None,
                    "match_status": match_status,
                    "invalid_reason": None if is_valid else invalid_reason,
                    "image_count": post_image_count,
                    "batch_id": resolved_batch,
                    "run_id": resolved_run_id,
                    "at": utc_now_iso(),
                    "record": merged,
                }
            )

            if upload_minio and index % 10 == 0:
                _persist_index_and_logs(
                    bucket=bucket,
                    source_prefix=source_prefix,
                    group_id=group_id,
                    run_id=resolved_run_id,
                    posts=posts,
                    upserts=upserts,
                )
                upserts.clear()
                _write_match_progress(
                    bucket=bucket,
                    source_prefix=source_prefix,
                    group_id=group_id,
                    batch_id=resolved_batch,
                    run_id=resolved_run_id,
                    media_scroll_y=media_scroll_y,
                    media_seen=media_seen,
                    media_exhausted=media_exhausted,
                    run_valid=run_valid,
                    run_invalid=run_invalid,
                    should_continue=False,
                    stop_reason="caption_match_in_progress",
                    bottom_year=bottom_year,
                    bottom_hits=bottom_hits,
                )

            if index % 12 == 0:
                _human_pause(8.0, 2.0, 5.0)

            if index % max(1, soft_restart_every) == 0:
                try:
                    session_cookies = driver.get_cookies() or session_cookies
                except Exception:
                    pass
                driver = _soft_restart_browser(
                    driver=driver,
                    remote_url=remote_url,
                    headless=headless,
                    page_load_timeout=page_load_timeout,
                    group_url=media_url,
                    cookies=session_cookies,
                    reason="caption match soft-restart",
                    fb_username=fb_username,
                    fb_password=fb_password,
                    fb_totp_secret=fb_totp_secret,
                )
                _human_pause(3.0, 0.5, 1.5)

    finally:
        try:
            session_cookies = driver.get_cookies() or session_cookies
        except Exception:
            pass
        _safe_quit_driver(driver)

    bottom_reached = bottom_hits >= 3
    should_continue = (not media_exhausted) and (not bottom_reached)
    stop_reason = (
        "bottom_year_reached"
        if bottom_reached
        else (
            "target_valid_delta"
            if run_valid >= target_valid_delta
            else ("media_exhausted" if media_exhausted else "batch_matched")
        )
    )
    if stop_reason == "target_valid_delta" and not media_exhausted and not bottom_reached:
        should_continue = True

    if upload_minio:
        _persist_index_and_logs(
            bucket=bucket,
            source_prefix=source_prefix,
            group_id=group_id,
            run_id=resolved_run_id,
            posts=posts,
            upserts=upserts,
        )
        export_info = _rebuild_exports(
            bucket=bucket, source_prefix=source_prefix, group_id=group_id, posts=posts
        )
        upload_json_payload(
            bucket,
            _batch_meta_key(source_prefix, group_id, resolved_batch),
            {
                "batch_id": resolved_batch,
                "matched_at": utc_now_iso(),
                "run_valid": run_valid,
                "run_invalid": run_invalid,
                "merge_hits": merge_hits,
                "pending_input": len(work),
                "download_images": False,
                "schema_version": SCHEMA_VERSION,
            },
        )
        if session_cookies:
            upload_json_payload(
                bucket,
                _cookies_key(source_prefix, group_id),
                {"saved_at": utc_now_iso(), "cookies": session_cookies},
            )
        _write_match_progress(
            bucket=bucket,
            source_prefix=source_prefix,
            group_id=group_id,
            batch_id=resolved_batch,
            run_id=resolved_run_id,
            media_scroll_y=media_scroll_y,
            media_seen=media_seen,
            media_exhausted=media_exhausted,
            run_valid=run_valid,
            run_invalid=run_invalid,
            should_continue=should_continue,
            stop_reason=stop_reason,
            bottom_year=bottom_year,
            bottom_hits=bottom_hits,
        )
        result = {
            "schema_version": SCHEMA_VERSION,
            "phase": "caption_match",
            "group_id": group_id,
            "run_id": resolved_run_id,
            "batch_id": resolved_batch,
            "valid_count": run_valid,
            "invalid_count": run_invalid,
            "merge_hits": merge_hits,
            "pending_input": len(work),
            "should_continue": should_continue,
            "stop_reason": stop_reason,
            "media_scroll_y": media_scroll_y,
            "media_exhausted": media_exhausted,
            "download_images": False,
            "export": export_info,
        }
        upload_json_payload(
            bucket, _run_result_key(source_prefix, group_id, resolved_run_id), result
        )
    else:
        result = {
            "schema_version": SCHEMA_VERSION,
            "phase": "caption_match",
            "batch_id": resolved_batch,
            "valid_count": run_valid,
            "invalid_count": run_invalid,
            "should_continue": should_continue,
            "stop_reason": stop_reason,
            "download_images": False,
        }

    print(f"{LOG} caption_match done {result}", flush=True)
    return result


def _read_progress(bucket: str, source_prefix: str, group_id: str) -> dict[str, Any]:
    from final_exam_nlp_crawl_runner import _read_json_object

    return _read_json_object(bucket, _progress_key(source_prefix, group_id)) or {}


def _read_json_safe(bucket: str, key: str) -> dict[str, Any] | None:
    from final_exam_nlp_crawl_runner import _read_json_object

    return _read_json_object(bucket, key)


def _load_jsonl_records(bucket: str, key: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in read_object_text(bucket, key).splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _write_media_checkpoint(
    *,
    bucket: str,
    source_prefix: str,
    group_id: str,
    batch_id: str,
    run_id: str,
    media_scroll_y: int,
    media_seen: set[str],
    posts: dict[str, dict[str, Any]],
    upserts: list[dict[str, Any]],
    pending_rows: list[dict[str, Any]],
    media_batch_size: int,
    media_exhausted: bool,
    should_continue: bool,
    stop_reason: str,
    max_image_valid_count: int,
    bottom_year: int,
    session_cookies: list[dict[str, Any]],
    driver,
    finalize: bool = False,
) -> None:
    """Persist tile batch + progress for rollover.
    Ghi batch tile + progress để rollover.
    """
    _persist_index_and_logs(
        bucket=bucket,
        source_prefix=source_prefix,
        group_id=group_id,
        run_id=run_id,
        posts=posts,
        upserts=upserts,
    )
    _atomic_upload_text(
        bucket, _batch_pending_key(source_prefix, group_id, batch_id), _to_jsonl(pending_rows)
    )
    upload_json_payload(
        bucket,
        _batch_meta_key(source_prefix, group_id, batch_id),
        {
            "batch_id": batch_id,
            "group_id": group_id,
            "run_id": run_id,
            "pending_count": len(pending_rows),
            "media_batch_size": media_batch_size,
            "media_scroll_y": media_scroll_y,
            "media_exhausted": media_exhausted,
            "mode": "tile_id_then_permalink",
            "download_images": False,
            "updated_at": utc_now_iso(),
            "schema_version": SCHEMA_VERSION,
            "finalized": finalize,
        },
    )
    if session_cookies:
        upload_json_payload(
            bucket,
            _cookies_key(source_prefix, group_id),
            {"saved_at": utc_now_iso(), "cookies": session_cookies},
        )
    elif driver is not None:
        try:
            cookies = driver.get_cookies() or []
            if cookies:
                upload_json_payload(
                    bucket,
                    _cookies_key(source_prefix, group_id),
                    {"saved_at": utc_now_iso(), "cookies": cookies},
                )
        except Exception:
            pass

    payload = {
        "updated_at": utc_now_iso(),
        "group_id": group_id,
        "last_run_id": run_id,
        "phase": "awaiting_caption_match" if finalize else "media_batch",
        "last_batch_id": batch_id,
        "media_scroll_y": media_scroll_y,
        "media_seen_ids": sorted(media_seen)[-80000:],
        "media_exhausted": media_exhausted,
        "pending_count": len(pending_rows),
        "media_batch_size": media_batch_size,
        "mode": "tile_id_then_permalink",
        "download_images": False,
        "max_image_valid_count": max_image_valid_count,
        "bottom_year": bottom_year,
        "should_continue": should_continue,
        "stop_reason": stop_reason,
        "schema_version": SCHEMA_VERSION,
        "source": SOURCE_NAME,
    }
    _atomic_upload_json(bucket, _progress_key(source_prefix, group_id), payload)
    upload_json_payload(
        bucket,
        _meta_key(source_prefix, group_id),
        {
            "group_id": group_id,
            "bottom_year": bottom_year,
            "pipeline": "two_phase_tile_then_permalink",
            "download_images": False,
            "schema_version": SCHEMA_VERSION,
            "updated_at": utc_now_iso(),
        },
    )


def _write_match_progress(
    *,
    bucket: str,
    source_prefix: str,
    group_id: str,
    batch_id: str,
    run_id: str,
    media_scroll_y: int,
    media_seen: set[str],
    media_exhausted: bool,
    run_valid: int,
    run_invalid: int,
    should_continue: bool,
    stop_reason: str,
    bottom_year: int,
    bottom_hits: int,
) -> None:
    payload = {
        "updated_at": utc_now_iso(),
        "group_id": group_id,
        "last_run_id": run_id,
        "last_batch_id": batch_id,
        "media_scroll_y": media_scroll_y,
        "media_seen_ids": sorted(media_seen)[-80000:],
        "media_exhausted": media_exhausted,
        "run_valid": run_valid,
        "run_invalid": run_invalid,
        "bottom_year": bottom_year,
        "bottom_hits": bottom_hits,
        "should_continue": should_continue,
        "stop_reason": stop_reason,
        "mode": "tile_id_then_permalink",
        "download_images": False,
        "schema_version": SCHEMA_VERSION,
        "source": SOURCE_NAME,
    }
    if should_continue:
        payload["phase"] = "ready_for_next_media_batch"
    elif stop_reason == "bottom_year_reached" or media_exhausted:
        payload["phase"] = "done"
    else:
        payload["phase"] = "caption_match_done"
    _atomic_upload_json(bucket, _progress_key(source_prefix, group_id), payload)


def _write_gallery_progress(
    *,
    bucket: str,
    source_prefix: str,
    group_id: str,
    run_id: str,
    baseline_fbid: str,
    cursor_fbid: str,
    gallery_total_steps: int,
    gallery_direction: str,
    run_valid: int,
    run_invalid: int,
    should_continue: bool,
    stop_reason: str,
    bottom_year: int,
    bottom_hits: int,
    steps_done: int,
    skipped_fbids: list[str] | None = None,
    checkpoint_photo_url: str = "",
    checkpoint_posted_at: str | None = None,
    checkpoint_story_url: str = "",
    media_scroll_y: int = 0,
    media_harvest_rounds: int = DEFAULT_MEDIA_HARVEST_ROUNDS,
    media_at_bottom: bool = False,
    crawl_mode: str = "media_walk",
) -> None:
    """Persist media/gallery-walk checkpoint for cursor resume.
    Lưu checkpoint media/gallery-walk để resume.
    """
    mode = (crawl_mode or "media_walk").strip() or "media_walk"
    rounds = max(
        int(DEFAULT_MEDIA_HARVEST_ROUNDS),
        min(int(media_harvest_rounds or DEFAULT_MEDIA_HARVEST_ROUNDS), int(MEDIA_HARVEST_ROUNDS_MAX)),
    )
    payload = {
        "updated_at": utc_now_iso(),
        "group_id": group_id,
        "last_run_id": run_id,
        "crawl_mode": mode,
        "gallery_baseline_fbid": baseline_fbid,
        "gallery_cursor_fbid": cursor_fbid,
        "gallery_total_steps": gallery_total_steps,
        "gallery_direction": gallery_direction,
        "run_posts": run_valid,
        "run_valid": run_valid,
        "run_invalid": run_invalid,
        "bottom_year": bottom_year,
        "bottom_hits": bottom_hits,
        "gallery_steps_last_run": steps_done,
        "should_continue": should_continue,
        "stop_reason": stop_reason,
        "mode": mode,
        "download_images": False,
        "schema_version": SCHEMA_VERSION,
        "source": SOURCE_NAME,
        "media_scroll_y": int(media_scroll_y or 0),
        # Adaptive harvest depth across Airflow rollovers /
        # Độ sâu harvest thích ứng qua các lần rollover Airflow
        "media_harvest_rounds": rounds,
        "media_at_bottom": bool(media_at_bottom),
        # Checkpoint: fbid + photo link + time / Checkpoint: fbid + link ảnh + thời gian
        "checkpoint_photo_url": (checkpoint_photo_url or "").strip()
        or (_gallery_photo_url(group_id, cursor_fbid) or ""),
        "checkpoint_posted_at": checkpoint_posted_at,
        "checkpoint_story_url": (checkpoint_story_url or "").strip() or None,
        "skipped_fbids": _normalize_fbid_list(skipped_fbids or []),
    }
    if should_continue:
        payload["phase"] = "ready_for_next_media_walk"
    elif stop_reason in ("bottom_year_reached", "media_exhausted", "gallery_stuck"):
        payload["phase"] = "done"
    else:
        payload["phase"] = "media_walk_done"
    _atomic_upload_json(bucket, _progress_key(source_prefix, group_id), payload)


def run_media_walk_batch(
    *,
    group_id: str,
    gallery_walk_count: int = DEFAULT_GALLERY_WALK_COUNT,
    gallery_baseline_fbid: str = DEFAULT_GALLERY_BASELINE_FBID,
    gallery_seek_fbid: str = "",
    gallery_step_pause_sec: float = DEFAULT_GALLERY_STEP_PAUSE_SEC,
    gallery_direction: str = DEFAULT_GALLERY_DIRECTION,
    bottom_year: int = DEFAULT_BOTTOM_YEAR,
    headless: bool = True,
    upload_minio: bool = True,
    force: bool = False,
    fb_username: str = "",
    fb_password: str = "",
    fb_totp_secret: str = "",
    run_id: str | None = None,
    permalink_pause_sec: float = DEFAULT_PERMALINK_PAUSE_SEC,
    soft_restart_every: int = DEFAULT_MATCH_SOFT_RESTART_EVERY,
    scroll_pause_sec: float = DEFAULT_SCROLL_PAUSE_SEC,
) -> dict[str, Any]:
    """Media walk: deep-link checkpoint, then harvest unseen Media tiles only.

    Media walk: deep-link checkpoint, rồi chỉ harvest ô Media chưa seen.
    """
    if not group_id.strip():
        raise ValueError("group_id is required")

    settings = _settings()
    bucket = settings["bucket_raw"]
    source_prefix = settings["source_prefix"]
    remote_url = settings["selenium_remote_url"]
    page_load_timeout = int(settings["page_load_timeout_sec"])
    resolved_run_id = (run_id or "").strip() or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    baseline = (gallery_baseline_fbid or DEFAULT_GALLERY_BASELINE_FBID).strip()
    window_size = 12

    if upload_minio:
        ensure_bucket(get_minio_client(), bucket)

    progress = _read_progress(bucket, source_prefix, group_id)
    start_fbid, start_reason = _resolve_gallery_start_fbid(
        progress=progress,
        baseline_fbid=baseline,
        seek_fbid=gallery_seek_fbid,
        force=force,
    )
    posts = _load_posts_index(bucket, source_prefix, group_id)
    cookies_payload = _read_json_safe(bucket, _cookies_key(source_prefix, group_id))
    cookies = list((cookies_payload or {}).get("cookies") or [])
    prior_total = int(progress.get("gallery_total_steps") or 0)
    skipped_fbids = _normalize_fbid_list(progress.get("skipped_fbids") or [])
    checkpoint_posted_at: str | None = progress.get("checkpoint_posted_at")
    checkpoint_story_url = str(progress.get("checkpoint_story_url") or "")
    checkpoint_photo_url = str(progress.get("checkpoint_photo_url") or "")
    media_scroll_y = int(progress.get("media_scroll_y") or 0)
    # Resume adaptive harvest depth (bump on empty+not-bottom) /
    # Resume độ sâu harvest thích ứng (tăng khi trống + chưa đáy)
    media_harvest_rounds = int(
        progress.get("media_harvest_rounds") or DEFAULT_MEDIA_HARVEST_ROUNDS
    )
    media_harvest_rounds = max(
        int(DEFAULT_MEDIA_HARVEST_ROUNDS),
        min(media_harvest_rounds, int(MEDIA_HARVEST_ROUNDS_MAX)),
    )
    media_at_bottom = bool(progress.get("media_at_bottom") or False)
    # Known ids from corpus + skipped — Media harvest skips these /
    # Id đã có trong corpus + skipped — harvest Media bỏ qua
    known_fbids: set[str] = set(skipped_fbids)
    for pid, rec in posts.items():
        if pid:
            known_fbids.add(str(pid))
        tid = str(rec.get("tile_id") or "").strip()
        if tid:
            known_fbids.add(tid)
        for t in rec.get("tile_ids") or []:
            tt = str(t or "").strip()
            if tt:
                known_fbids.add(tt)

    run_valid = 0
    run_invalid = 0
    bottom_hits = 0
    upserts: list[dict[str, Any]] = []
    session_cookies = cookies
    steps_done = 0
    media_exhausted = False
    stop_reason = "media_walk_in_progress"
    # End batch after draining current window when harvest hit RAM hard-stop /
    # Kết batch sau khi xử lý xong window hiện tại khi harvest chạm RAM hard-stop
    ram_end_after_window = False
    cursor_fbid = start_fbid
    skip_checkpoint_tile = start_reason in ("cursor", "seek", "skipped_seek") and not force
    if start_fbid in skipped_fbids:
        skip_checkpoint_tile = True
        known_fbids.add(start_fbid)
        print(f"{LOG} start_fbid skipped — harvest unseen only ({start_fbid})", flush=True)
    elif skip_checkpoint_tile and start_fbid:
        known_fbids.add(start_fbid)

    group_start = f"{FB_BASE_URL}/groups/{group_id}/"
    seek_pause = max(1.2, min(float(scroll_pause_sec), 2.5))
    _selenium_preflight(remote_url)
    driver = _build_driver(remote_url, headless, page_load_timeout)
    try:
        session_cookies = _ensure_driver_login(
            driver=driver,
            bucket=bucket,
            source_prefix=source_prefix,
            group_id=group_id,
            start_url=group_start,
            cookies=cookies,
            fb_username=fb_username,
            fb_password=fb_password,
            fb_totp_secret=fb_totp_secret,
            upload_minio=upload_minio,
        )

        print(f"{LOG} media_walk clear_cache…", flush=True)
        _clear_browser_caches(driver)

        pending: list[str] = []
        if not skip_checkpoint_tile and start_fbid and start_fbid not in skipped_fbids:
            pending = [start_fbid]

        print(
            f"{LOG} media_walk schema={SCHEMA_VERSION} start={start_fbid} "
            f"reason={start_reason} skip_cp={skip_checkpoint_tile} "
            f"known={len(known_fbids)} count={gallery_walk_count} "
            f"scrollY0≈{media_scroll_y} harvest_rounds={media_harvest_rounds} "
            f"mode=deep_link+unseen_harvest",
            flush=True,
        )

        while steps_done < int(gallery_walk_count):
            if not _is_logged_in(driver):
                session_cookies = _ensure_driver_login(
                    driver=driver,
                    bucket=bucket,
                    source_prefix=source_prefix,
                    group_id=group_id,
                    start_url=group_start,
                    cookies=session_cookies,
                    fb_username=fb_username,
                    fb_password=fb_password,
                    fb_totp_secret=fb_totp_secret,
                    upload_minio=upload_minio,
                )
                _clear_browser_caches(driver)
                pending.clear()

            if not pending:
                if ram_end_after_window:
                    stop_reason = "selenium_ram_limit"
                    print(
                        f"{LOG} media_ram_stop after_window steps={steps_done} "
                        f"scrollY≈{media_scroll_y} rounds={media_harvest_rounds}",
                        flush=True,
                    )
                    break
                want = min(window_size, int(gallery_walk_count) - steps_done)
                # Always reopen Media after photo deep-links; restore scroll depth /
                # Luôn reopen Media sau deep-link ảnh; restore độ sâu cuộn
                pending, media_scroll_y, media_at_bottom, ram_stop = _harvest_unseen_media_fbids(
                    driver,
                    group_id=group_id,
                    known_fbids=known_fbids,
                    want=want,
                    max_scroll_rounds=media_harvest_rounds,
                    scroll_pause_sec=seek_pause,
                    reopen_media=True,
                    start_scroll_y=media_scroll_y,
                    log_prefix=LOG,
                )
                pending = [f for f in pending if f and f not in skipped_fbids]
                if not pending:
                    if ram_stop:
                        # Near selenium RAM ceiling — end batch to avoid OOM /
                        # Gần trần RAM selenium — kết batch tránh OOM
                        stop_reason = "selenium_ram_limit"
                        media_exhausted = False
                        print(
                            f"{LOG} media_ram_stop end_batch known={len(known_fbids)} "
                            f"steps={steps_done} scrollY≈{media_scroll_y} "
                            f"at_bottom={media_at_bottom} rounds={media_harvest_rounds}",
                            flush=True,
                        )
                        break
                    if media_at_bottom:
                        media_exhausted = True
                        stop_reason = "media_exhausted"
                        print(
                            f"{LOG} media_exhausted at_bottom known={len(known_fbids)} "
                            f"steps={steps_done} scrollY≈{media_scroll_y}",
                            flush=True,
                        )
                        break
                    # Not at bottom — bump rounds and rollover for deeper scroll /
                    # Chưa đáy — tăng round và rollover để cuộn sâu hơn
                    bumped = min(
                        int(MEDIA_HARVEST_ROUNDS_MAX),
                        int(media_harvest_rounds) + int(MEDIA_HARVEST_ROUNDS_STEP),
                    )
                    print(
                        f"{LOG} media_scroll_deeper known={len(known_fbids)} "
                        f"steps={steps_done} scrollY≈{media_scroll_y} "
                        f"rounds {media_harvest_rounds}→{bumped} (rollover)",
                        flush=True,
                    )
                    media_harvest_rounds = bumped
                    media_exhausted = False
                    stop_reason = "media_scroll_deeper"
                    break
                if ram_stop:
                    # Process this window then end batch / Xử lý window này rồi kết batch
                    ram_end_after_window = True
                print(
                    f"{LOG} media_window n={len(pending)} first={pending[0]} "
                    f"scrollY≈{media_scroll_y} known={len(known_fbids)} "
                    f"rounds={media_harvest_rounds} at_bottom={media_at_bottom} "
                    f"ram_stop={ram_stop}",
                    flush=True,
                )

            tile_id = pending.pop(0)
            if tile_id in known_fbids and steps_done > 0:
                continue
            if tile_id == start_fbid and checkpoint_photo_url and "fbid=" in checkpoint_photo_url:
                photo_url = checkpoint_photo_url
            else:
                photo_url = _gallery_photo_url(group_id, tile_id)
            if not photo_url:
                if tile_id not in skipped_fbids:
                    skipped_fbids.append(tile_id)
                known_fbids.add(tile_id)
                continue

            if steps_done > 0 and steps_done % 5 == 0:
                _ram_guard_selenium(driver, log_prefix=LOG)

            try:
                driver.get(photo_url)
                _human_pause(permalink_pause_sec, 0.3, 1.0)
            except Exception as exc:
                print(
                    f"{LOG} open_photo_fail tile={tile_id} err={type(exc).__name__}",
                    flush=True,
                )
                if tile_id not in skipped_fbids:
                    skipped_fbids.append(tile_id)
                known_fbids.add(tile_id)
                cursor_fbid = tile_id
                continue

            landed = driver.current_url or ""
            if _is_junk_facebook_url(landed) or _is_homepage_redirect(landed):
                print(
                    f"{LOG} open_photo_redirect tile={tile_id} landed={landed[:90]}",
                    flush=True,
                )
                if tile_id not in skipped_fbids:
                    skipped_fbids.append(tile_id)
                known_fbids.add(tile_id)
                cursor_fbid = tile_id
                _clear_browser_caches(driver)
                continue

            peek_href = _peek_view_post_href(driver)
            peek_story = _canonicalize_story_url(peek_href, group_id) or ""
            peek_story = _posts_to_permalink_url(peek_story, group_id) or peek_story
            peek_story_id = _post_id_from_url(peek_story)
            prior_story = posts.get(peek_story_id) if peek_story_id else None
            reuse_caption = ""
            reuse_story_url = ""
            if prior_story:
                lab = str(prior_story.get("label") or "").strip()
                if lab and not _is_ui_noise_label(lab):
                    reuse_caption = lab
                    reuse_story_url = str(
                        prior_story.get("permalink")
                        or prior_story.get("story_url")
                        or peek_story
                        or ""
                    )

            extracted = _open_and_extract_post(
                driver,
                group_id=group_id,
                tile_id=tile_id,
                photo_url=photo_url,
                permalink_pause_sec=permalink_pause_sec,
                skip_navigation=True,
                reuse_caption=reuse_caption,
                reuse_story_url=reuse_story_url,
            )
            caption = (extracted.get("caption") or "").strip()
            sub_caption = (extracted.get("sub_caption") or "").strip()
            image_urls = list(extracted.get("image_urls") or [])
            author = extracted.get("author") or ""
            posted_at = extracted.get("posted_at")
            story_post_id = (extracted.get("story_post_id") or "").strip()
            story_url = (extracted.get("story_url") or "").strip()
            best_url = extracted.get("best_url") or photo_url
            found = bool(extracted.get("found"))
            invalid_reason = extracted.get("invalid_reason") or ""

            is_valid = (
                found
                and bool(caption)
                and bool(image_urls)
                and not _is_ui_noise_label(caption)
                and _is_content_caption(caption)
            )
            if not is_valid:
                if invalid_reason == "redirect_homepage":
                    pass
                elif not caption:
                    invalid_reason = invalid_reason or "missing_caption"
                elif _is_ui_noise_label(caption):
                    invalid_reason = invalid_reason or "ui_noise_caption"
                elif not _is_content_caption(caption):
                    invalid_reason = invalid_reason or "not_content_caption"
                elif not image_urls:
                    invalid_reason = invalid_reason or "missing_image"
                else:
                    invalid_reason = invalid_reason or "media_extract_failed"

            prior = None
            lookup_id = story_post_id or tile_id
            if lookup_id in posts:
                prior = posts[lookup_id]
            else:
                for rec in posts.values():
                    if tile_id in set(rec.get("tile_ids") or []) or rec.get("tile_id") == tile_id:
                        prior = rec
                        break

            step = steps_done + 1
            if is_valid:
                match_status = "valid"
                already_post = bool(
                    prior
                    and (prior.get("match_status") == "valid" or prior.get("is_valid"))
                    and str(prior.get("label") or "").strip()
                )
                post_image_count = len(image_urls)
                if not already_post:
                    run_valid += 1
                    print(
                        f"{LOG} +post {run_valid} step={step} tile={tile_id} "
                        f"story={story_post_id or tile_id} images={post_image_count} "
                        f"caption={caption[:50]!r}",
                        flush=True,
                    )
                else:
                    print(
                        f"{LOG} +image step={step} tile={tile_id} "
                        f"story={story_post_id or tile_id} images={post_image_count} "
                        f"(same post, merged)",
                        flush=True,
                    )
            else:
                run_invalid += 1
                match_status = "invalid"
                print(
                    f"{LOG} +invalid {run_invalid} step={step} tile={tile_id} "
                    f"reason={invalid_reason}",
                    flush=True,
                )

            year = _year_from_posted_at(posted_at if isinstance(posted_at, str) else None)
            if year is not None and year < bottom_year:
                bottom_hits += 1

            record = _build_record(
                post_id=story_post_id or tile_id,
                post_url=best_url,
                author=author,
                label=caption,
                image_urls=image_urls,
                posted_at=posted_at if isinstance(posted_at, str) else None,
                group_id=group_id,
                run_id=resolved_run_id,
                phase="media_walk",
                image_local_keys=[],
                images_downloaded=False,
                images_download_skipped=True,
                is_valid=is_valid,
                invalid_reason=None if is_valid else invalid_reason,
                tile_id=tile_id,
                tile_href=photo_url,
                story_post_id=story_post_id,
                sub_caption=sub_caption,
            )
            record["match_status"] = match_status
            record["photo_url"] = photo_url
            record["permalink_resolved"] = found
            record["story_url"] = story_url or None
            record["image_count"] = len(image_urls)
            _, merged = _upsert_post(posts, record)
            upserts.append(
                {
                    "event": "media_walk",
                    "tile_id": tile_id,
                    "story_post_id": story_post_id or None,
                    "match_status": match_status,
                    "invalid_reason": None if is_valid else invalid_reason,
                    "image_count": int(
                        merged.get("image_count") or len(merged.get("image_urls") or [])
                    ),
                    "run_id": resolved_run_id,
                    "at": utc_now_iso(),
                    "record": merged,
                }
            )

            known_fbids.add(tile_id)
            if story_post_id:
                known_fbids.add(story_post_id)

            steps_done = step
            cursor_fbid = tile_id
            checkpoint_photo_url = str(photo_url or "")
            if isinstance(posted_at, str) and posted_at.strip():
                checkpoint_posted_at = posted_at.strip()
            if story_url:
                checkpoint_story_url = story_url

            if upload_minio and step % 10 == 0:
                _persist_index_and_logs(
                    bucket=bucket,
                    source_prefix=source_prefix,
                    group_id=group_id,
                    run_id=resolved_run_id,
                    posts=posts,
                    upserts=upserts,
                )
                upserts.clear()
                _write_gallery_progress(
                    bucket=bucket,
                    source_prefix=source_prefix,
                    group_id=group_id,
                    run_id=resolved_run_id,
                    baseline_fbid=baseline,
                    cursor_fbid=cursor_fbid,
                    gallery_total_steps=prior_total + steps_done,
                    gallery_direction=gallery_direction,
                    run_valid=run_valid,
                    run_invalid=run_invalid,
                    should_continue=True,
                    stop_reason="media_walk_in_progress",
                    bottom_year=bottom_year,
                    bottom_hits=bottom_hits,
                    steps_done=steps_done,
                    skipped_fbids=skipped_fbids,
                    checkpoint_photo_url=checkpoint_photo_url,
                    checkpoint_posted_at=checkpoint_posted_at
                    if isinstance(checkpoint_posted_at, str)
                    else None,
                    checkpoint_story_url=checkpoint_story_url,
                    media_scroll_y=media_scroll_y,
                    media_harvest_rounds=media_harvest_rounds,
                    media_at_bottom=media_at_bottom,
                    crawl_mode="media_walk",
                )

            if bottom_hits >= 3:
                stop_reason = "bottom_year_reached"
                break

            _human_pause(gallery_step_pause_sec * 0.35, 0.2, 0.6)

            if step % max(1, soft_restart_every) == 0:
                try:
                    session_cookies = driver.get_cookies() or session_cookies
                except Exception:
                    pass
                driver = _soft_restart_browser(
                    driver=driver,
                    remote_url=remote_url,
                    headless=headless,
                    page_load_timeout=page_load_timeout,
                    group_url=group_start,
                    cookies=session_cookies,
                    reason="media walk soft-restart (RAM)",
                    fb_username=fb_username,
                    fb_password=fb_password,
                    fb_totp_secret=fb_totp_secret,
                )
                _clear_browser_caches(driver)
                pending.clear()

    finally:
        try:
            session_cookies = driver.get_cookies() or session_cookies
        except Exception:
            pass
        _safe_quit_driver(driver)

    bottom_reached = bottom_hits >= 3 or stop_reason == "bottom_year_reached"
    # Continue when deeper scroll / RAM end needs soft restart / batch partial /
    # Tiếp khi cần cuộn sâu hơn / RAM end cần soft restart / batch dở
    if bottom_reached:
        should_continue = False
        if stop_reason == "media_walk_in_progress":
            stop_reason = "bottom_year_reached"
    elif stop_reason == "media_exhausted":
        should_continue = False
    elif stop_reason in ("media_scroll_deeper", "selenium_ram_limit"):
        should_continue = True
    elif steps_done > 0:
        should_continue = True
        if stop_reason == "media_walk_in_progress":
            stop_reason = "media_batch_done"
    else:
        # Empty first harvest but not bottom — already set media_scroll_deeper /
        # Harvest đầu trống nhưng chưa đáy — đã set media_scroll_deeper
        should_continue = stop_reason in ("media_scroll_deeper", "selenium_ram_limit")
        if stop_reason == "media_walk_in_progress":
            should_continue = False
            stop_reason = "media_walk_finished"

    gallery_total_steps = prior_total + steps_done

    if upload_minio:
        _persist_index_and_logs(
            bucket=bucket,
            source_prefix=source_prefix,
            group_id=group_id,
            run_id=resolved_run_id,
            posts=posts,
            upserts=upserts,
        )
        export_info = _rebuild_exports(
            bucket=bucket, source_prefix=source_prefix, group_id=group_id, posts=posts
        )
        if session_cookies:
            upload_json_payload(
                bucket,
                _cookies_key(source_prefix, group_id),
                {"saved_at": utc_now_iso(), "cookies": session_cookies},
            )
        _write_gallery_progress(
            bucket=bucket,
            source_prefix=source_prefix,
            group_id=group_id,
            run_id=resolved_run_id,
            baseline_fbid=baseline,
            cursor_fbid=cursor_fbid,
            gallery_total_steps=gallery_total_steps,
            gallery_direction=gallery_direction,
            run_valid=run_valid,
            run_invalid=run_invalid,
            should_continue=should_continue,
            stop_reason=stop_reason,
            bottom_year=bottom_year,
            bottom_hits=bottom_hits,
            steps_done=steps_done,
            skipped_fbids=skipped_fbids,
            checkpoint_photo_url=checkpoint_photo_url,
            checkpoint_posted_at=checkpoint_posted_at
            if isinstance(checkpoint_posted_at, str)
            else None,
            checkpoint_story_url=checkpoint_story_url,
            media_scroll_y=media_scroll_y,
            media_harvest_rounds=media_harvest_rounds,
            media_at_bottom=media_at_bottom,
            crawl_mode="media_walk",
        )
        upload_json_payload(
            bucket,
            _meta_key(source_prefix, group_id),
            {
                "group_id": group_id,
                "bottom_year": bottom_year,
                "pipeline": "media_walk",
                "download_images": False,
                "schema_version": SCHEMA_VERSION,
                "updated_at": utc_now_iso(),
            },
        )
        _ = export_info

    result = {
        "schema_version": SCHEMA_VERSION,
        "phase": "media_walk",
        "group_id": group_id,
        "run_id": resolved_run_id,
        "start_fbid": start_fbid,
        "start_reason": start_reason,
        "cursor_fbid": cursor_fbid,
        "steps_done": steps_done,
        "gallery_total_steps": gallery_total_steps,
        "run_posts": run_valid,
        "run_valid": run_valid,
        "run_invalid": run_invalid,
        "should_continue": should_continue,
        "stop_reason": stop_reason,
        "skipped_fbids": skipped_fbids[:50],
        "known_fbids": len(known_fbids),
        "media_scroll_y": media_scroll_y,
        "media_harvest_rounds": media_harvest_rounds,
        "media_at_bottom": media_at_bottom,
        "download_images": False,
    }
    print(f"{LOG} media_walk done {result}", flush=True)
    return result



def run_gallery_walk_batch(
    *,
    group_id: str,
    gallery_walk_count: int = DEFAULT_GALLERY_WALK_COUNT,
    gallery_baseline_fbid: str = DEFAULT_GALLERY_BASELINE_FBID,
    gallery_seek_fbid: str = "",
    gallery_step_pause_sec: float = DEFAULT_GALLERY_STEP_PAUSE_SEC,
    gallery_direction: str = DEFAULT_GALLERY_DIRECTION,
    bottom_year: int = DEFAULT_BOTTOM_YEAR,
    headless: bool = True,
    upload_minio: bool = True,
    force: bool = False,
    fb_username: str = "",
    fb_password: str = "",
    fb_totp_secret: str = "",
    run_id: str | None = None,
    permalink_pause_sec: float = DEFAULT_PERMALINK_PAUSE_SEC,
    soft_restart_every: int = DEFAULT_MATCH_SOFT_RESTART_EVERY,
) -> dict[str, Any]:
    """Entry point: schema 3.11 uses media-walk (seek checkpoint → extract).
    Entry: schema 3.11 dùng media-walk (seek checkpoint → extract).
    """
    return run_media_walk_batch(
        group_id=group_id,
        gallery_walk_count=gallery_walk_count,
        gallery_baseline_fbid=gallery_baseline_fbid,
        gallery_seek_fbid=gallery_seek_fbid,
        gallery_step_pause_sec=gallery_step_pause_sec,
        gallery_direction=gallery_direction,
        bottom_year=bottom_year,
        headless=headless,
        upload_minio=upload_minio,
        force=force,
        fb_username=fb_username,
        fb_password=fb_password,
        fb_totp_secret=fb_totp_secret,
        run_id=run_id,
        permalink_pause_sec=permalink_pause_sec,
        soft_restart_every=soft_restart_every,
    )


def _recheck_extract_via_permalink(
    driver,
    *,
    group_id: str,
    story_url: str,
    permalink_pause_sec: float,
    existing_images: list[str],
) -> dict[str, Any]:
    """Second-pass extract from permalink when photo viewer was incomplete.
    Extract lần 2 từ permalink khi photo viewer thiếu dữ liệu.
    """
    empty: dict[str, Any] = {
        "caption": "",
        "image_urls": [],
        "author": "",
        "posted_at": None,
        "story_post_id": "",
        "story_url": story_url,
        "best_url": story_url,
        "found": False,
        "nav_ok": False,
        "landed_url": "",
    }
    canon = _canonicalize_story_url(story_url, group_id) or story_url
    canon = _posts_to_permalink_url(canon, group_id) or canon
    if not canon:
        return empty
    last_url, nav_ok = _navigate_story_permalink(
        driver, canon, group_id, permalink_pause_sec * 1.5
    )
    if not nav_ok or _is_homepage_redirect(last_url) or _is_junk_facebook_url(last_url):
        print(
            f"{LOG} recheck_permalink_junk story={canon[:90]} landed={last_url or '-'}",
            flush=True,
        )
        empty["landed_url"] = last_url or ""
        empty["story_url"] = canon
        empty["story_post_id"] = _post_id_from_url(canon) or ""
        return empty
    story_extracted = _extract_from_dialog_or_page(driver, min_width=50)
    caption = (story_extracted.get("caption") or "").strip()
    if not caption:
        caption = _extract_meta_caption(driver)
    image_urls = _dedupe_image_urls(
        list(existing_images)
        + [u for u in (story_extracted.get("images") or []) if not _is_video_url(u)]
        + _extract_scoped_cdn_urls(driver, max_images=12)
    )
    author = story_extracted.get("author") or ""
    posted_at = story_extracted.get("posted_at")
    story_post_id = _post_id_from_url(canon) or ""
    best_url = _pick_best_url(canon, story_extracted.get("post_url") or "", last_url, group_id=group_id)
    if caption and _is_ui_noise_label(caption):
        caption = ""
    found = bool(caption.strip() and image_urls)
    return {
        "caption": caption,
        "image_urls": image_urls,
        "author": author,
        "posted_at": posted_at,
        "story_post_id": story_post_id,
        "story_url": canon,
        "best_url": best_url,
        "found": found,
        "nav_ok": True,
        "landed_url": last_url or "",
    }


def _collect_recheck_candidates(
    posts: dict[str, dict[str, Any]],
    *,
    limit: int,
    max_attempts: int = DEFAULT_INVALID_RECHECK_MAX_ATTEMPTS,
) -> list[dict[str, Any]]:
    """Pick invalid posts that may succeed on recheck / Chọn invalid có thể recheck thành công."""
    candidates: list[dict[str, Any]] = []
    for record in posts.values():
        if record.get("is_valid") or record.get("match_status") == "valid":
            continue
        if not _is_recheck_eligible(record, max_attempts=max_attempts):
            continue
        tile_id = str(record.get("tile_id") or "").strip()
        photo_url = str(
            record.get("photo_url")
            or record.get("tile_href")
            or _gallery_photo_url(str(record.get("group_id") or ""), tile_id)
            or ""
        ).strip()
        if not tile_id and not photo_url:
            continue
        candidates.append(record)
    # Oldest invalid first — stable order / Invalid cũ trước — thứ tự ổn định
    candidates.sort(key=lambda r: str(r.get("first_seen_at") or r.get("updated_at") or ""))
    return candidates[:limit]


def _recheck_skipped_fbids(
    driver,
    *,
    group_id: str,
    posts: dict[str, Any],
    progress: dict[str, Any],
    skipped_limit: int,
    skipped_max_attempts: int,
    permalink_pause_sec: float,
    resolved_run_id: str,
) -> tuple[list[dict[str, Any]], list[str], dict[str, int], int, int]:
    """Re-open skipped (viewer-broken) photo URLs; refresh images without arrow.

    Mở lại URL ảnh đã skip (viewer gãy); làm mới ảnh, không cần mũi tên.
    Returns upserts, remaining_skipped, attempt_map, rechecked, recovered.
    """
    skipped_fbids = _normalize_fbid_list(progress.get("skipped_fbids") or [])
    skip_attempt_map: dict[str, int] = {}
    raw_attempts = progress.get("skipped_recheck_attempts") or {}
    if isinstance(raw_attempts, dict):
        for key, val in raw_attempts.items():
            try:
                skip_attempt_map[str(key)] = max(0, int(val))
            except (TypeError, ValueError):
                continue

    queue = [
        fid for fid in skipped_fbids if skip_attempt_map.get(fid, 0) < max(1, skipped_max_attempts)
    ][: max(1, skipped_limit)]
    remaining = list(skipped_fbids)
    upserts: list[dict[str, Any]] = []
    rechecked = 0
    recovered = 0
    if not queue:
        return upserts, remaining, skip_attempt_map, rechecked, recovered

    print(
        f"{LOG} skipped_recheck start queue={len(queue)} remaining={len(remaining)}",
        flush=True,
    )
    for sidx, tile_id in enumerate(queue, start=1):
        if sidx % 4 == 0:
            _clear_browser_caches(driver)
            _ram_guard_selenium(driver, log_prefix=LOG)
        photo_url = _gallery_photo_url(group_id, tile_id) or ""
        attempt_count = int(skip_attempt_map.get(tile_id, 0)) + 1
        skip_attempt_map[tile_id] = attempt_count
        rechecked += 1
        print(
            f"{LOG} skipped_recheck {sidx}/{len(queue)} fbid={tile_id} "
            f"attempt={attempt_count}/{skipped_max_attempts}",
            flush=True,
        )
        extracted = _open_and_extract_post(
            driver,
            group_id=group_id,
            tile_id=tile_id,
            photo_url=photo_url,
            permalink_pause_sec=permalink_pause_sec,
            skip_navigation=False,
            photo_only=True,
        )
        caption = (extracted.get("caption") or "").strip()
        image_urls = list(extracted.get("image_urls") or [])
        author = extracted.get("author") or ""
        posted_at = extracted.get("posted_at")
        story_post_id = (extracted.get("story_post_id") or "").strip()
        best_url = extracted.get("best_url") or photo_url
        got_images = bool(image_urls)
        is_valid = bool(
            caption
            and image_urls
            and not _is_ui_noise_label(caption)
            and _is_content_caption(caption)
        )
        if got_images:
            prior = posts.get(story_post_id) or posts.get(tile_id) or {}
            record = _build_record(
                post_id=story_post_id or tile_id,
                post_url=best_url,
                author=author or prior.get("author") or "",
                label=caption or str(prior.get("label") or ""),
                image_urls=_dedupe_image_urls(list(prior.get("image_urls") or []) + image_urls),
                posted_at=(
                    posted_at
                    if isinstance(posted_at, str)
                    else (
                        prior.get("posted_at")
                        if isinstance(prior.get("posted_at"), str)
                        else None
                    )
                ),
                group_id=group_id,
                run_id=resolved_run_id,
                phase="skipped_recheck",
                image_local_keys=list(prior.get("image_local_keys") or []),
                images_downloaded=bool(prior.get("images_downloaded")),
                images_download_skipped=True,
                is_valid=is_valid or bool(prior.get("is_valid")),
                invalid_reason=(
                    None
                    if (is_valid or prior.get("is_valid"))
                    else "skipped_viewer_broken"
                ),
                tile_id=tile_id,
                tile_href=photo_url,
                story_post_id=story_post_id,
                sub_caption=(extracted.get("sub_caption") or "").strip(),
            )
            record["match_status"] = "valid" if record.get("is_valid") else "invalid"
            record["photo_url"] = photo_url
            record["skipped_rechecked_at"] = utc_now_iso()
            _, merged = _upsert_post(posts, record)
            upserts.append(
                {
                    "event": "skipped_recheck",
                    "tile_id": tile_id,
                    "recovered": is_valid or got_images,
                    "image_count": len(image_urls),
                    "attempt": attempt_count,
                    "post_id": merged.get("post_id"),
                }
            )
        if got_images or attempt_count >= max(1, skipped_max_attempts):
            remaining = [f for f in remaining if f != tile_id]
            if got_images:
                recovered += 1
                print(
                    f"{LOG} skipped_recovered fbid={tile_id} images={len(image_urls)}",
                    flush=True,
                )
            else:
                print(
                    f"{LOG} skipped_exhausted fbid={tile_id} attempt={attempt_count}",
                    flush=True,
                )
        _human_pause(permalink_pause_sec * 0.4, 0.2, 0.6)
        try:
            driver.get("about:blank")
        except Exception:
            pass
    return upserts, remaining, skip_attempt_map, rechecked, recovered


def run_invalid_recheck_batch(
    *,
    group_id: str,
    recheck_limit: int = DEFAULT_INVALID_RECHECK_LIMIT,
    max_attempts: int = DEFAULT_INVALID_RECHECK_MAX_ATTEMPTS,
    skipped_recheck_limit: int = DEFAULT_SKIPPED_RECHECK_LIMIT,
    skipped_max_attempts: int = DEFAULT_SKIPPED_RECHECK_MAX_ATTEMPTS,
    enabled: bool = True,
    headless: bool = True,
    upload_minio: bool = True,
    fb_username: str = "",
    fb_password: str = "",
    fb_totp_secret: str = "",
    run_id: str | None = None,
    permalink_pause_sec: float = DEFAULT_PERMALINK_PAUSE_SEC,
) -> dict[str, Any]:
    """Re-open invalid posts (photo-first) and skipped viewer-broken tiles.
    Mở lại invalid (photo-first) và tile skip (viewer gãy).
    """
    if not group_id.strip():
        raise ValueError("group_id is required")

    settings = _settings()
    bucket = settings["bucket_raw"]
    source_prefix = settings["source_prefix"]
    remote_url = settings["selenium_remote_url"]
    page_load_timeout = int(settings["page_load_timeout_sec"])
    resolved_run_id = (run_id or "").strip() or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    skipped_limit = max(
        1,
        int(
            skipped_recheck_limit
            or settings.get("skipped_recheck_limit")
            or DEFAULT_SKIPPED_RECHECK_LIMIT
        ),
    )
    skipped_max = max(
        1,
        int(
            skipped_max_attempts
            or settings.get("skipped_recheck_max_attempts")
            or DEFAULT_SKIPPED_RECHECK_MAX_ATTEMPTS
        ),
    )

    if not enabled:
        print(f"{LOG} invalid_recheck skipped (disabled)", flush=True)
        return {
            "schema_version": SCHEMA_VERSION,
            "phase": "invalid_recheck",
            "group_id": group_id,
            "run_id": resolved_run_id,
            "enabled": False,
            "candidates": 0,
            "promoted": 0,
            "still_invalid": 0,
            "skipped_rechecked": 0,
            "skipped_recovered": 0,
        }

    if upload_minio:
        ensure_bucket(get_minio_client(), bucket)

    posts = _load_posts_index(bucket, source_prefix, group_id)
    progress = _read_progress(bucket, source_prefix, group_id)
    candidates = _collect_recheck_candidates(
        posts, limit=recheck_limit, max_attempts=max_attempts
    )
    attempt_raw = progress.get("skipped_recheck_attempts") or {}
    pending_skipped = [
        fid
        for fid in _normalize_fbid_list(progress.get("skipped_fbids") or [])
        if int(attempt_raw.get(fid, 0) or 0) < skipped_max
    ][:skipped_limit]
    if not candidates and not pending_skipped:
        print(f"{LOG} invalid_recheck no candidates / no skipped", flush=True)
        return {
            "schema_version": SCHEMA_VERSION,
            "phase": "invalid_recheck",
            "group_id": group_id,
            "run_id": resolved_run_id,
            "enabled": True,
            "candidates": 0,
            "promoted": 0,
            "still_invalid": 0,
            "skipped_rechecked": 0,
            "skipped_recovered": 0,
        }

    promoted = 0
    still_invalid = 0
    remaining_skipped = _normalize_fbid_list(progress.get("skipped_fbids") or [])
    skip_attempt_map: dict[str, int] = {}
    skipped_rechecked = 0
    skipped_recovered = 0
    upserts: list[dict[str, Any]] = []
    session_cookies: list[dict[str, Any]] = []

    _selenium_preflight(remote_url)
    driver = _build_driver(remote_url, headless, page_load_timeout)
    try:
        session_cookies = _ensure_driver_login(
            driver=driver,
            bucket=bucket,
            source_prefix=source_prefix,
            group_id=group_id,
            start_url=f"{FB_BASE_URL}/groups/{group_id}",
            cookies=[],
            fb_username=fb_username,
            fb_password=fb_password,
            fb_totp_secret=fb_totp_secret,
            upload_minio=upload_minio,
        )
        print(
            f"{LOG} invalid_recheck start candidates={len(candidates)} "
            f"skipped_pending={len(pending_skipped)} limit={recheck_limit} "
            f"max_attempts={max_attempts}",
            flush=True,
        )
        for idx, prior in enumerate(candidates, start=1):
            if idx % 5 == 0:
                _ram_guard_selenium(driver, log_prefix=LOG)
            tile_id = str(prior.get("tile_id") or "").strip()
            story_url = str(
                prior.get("story_url") or prior.get("permalink") or prior.get("post_link") or ""
            ).strip()
            photo_url = str(
                prior.get("photo_url")
                or prior.get("tile_href")
                or _gallery_photo_url(group_id, tile_id)
                or ""
            ).strip()
            prior_reason = str(prior.get("invalid_reason") or "")
            attempt_count = _recheck_attempt_count(prior) + 1

            extracted = _open_and_extract_post(
                driver,
                group_id=group_id,
                tile_id=tile_id,
                photo_url=photo_url,
                permalink_pause_sec=permalink_pause_sec,
                skip_navigation=False,
                photo_only=True,
            )
            caption = (extracted.get("caption") or "").strip()
            image_urls = list(extracted.get("image_urls") or [])
            author = extracted.get("author") or prior.get("author") or ""
            posted_at = extracted.get("posted_at") or prior.get("posted_at")
            story_post_id = (extracted.get("story_post_id") or prior.get("story_post_id") or "").strip()
            story_url = (extracted.get("story_url") or story_url).strip()
            best_url = extracted.get("best_url") or photo_url

            is_valid = (
                bool(caption)
                and bool(image_urls)
                and not _is_ui_noise_label(caption)
                and _is_content_caption(caption)
            )
            invalid_reason = ""

            if not is_valid and story_url:
                perm = _recheck_extract_via_permalink(
                    driver,
                    group_id=group_id,
                    story_url=story_url,
                    permalink_pause_sec=permalink_pause_sec,
                    existing_images=image_urls,
                )
                if perm.get("found"):
                    caption = (perm.get("caption") or caption).strip()
                    image_urls = list(perm.get("image_urls") or image_urls)
                    author = perm.get("author") or author
                    posted_at = perm.get("posted_at") or posted_at
                    story_post_id = (perm.get("story_post_id") or story_post_id).strip()
                    best_url = perm.get("best_url") or best_url
                    is_valid = (
                        bool(caption)
                        and bool(image_urls)
                        and not _is_ui_noise_label(caption)
                        and _is_content_caption(caption)
                    )

            if not is_valid:
                if not caption:
                    invalid_reason = "missing_caption"
                elif _is_ui_noise_label(caption):
                    invalid_reason = "ui_noise_caption"
                elif not _is_content_caption(caption):
                    invalid_reason = "not_content_caption"
                elif not image_urls:
                    invalid_reason = "missing_image"
                else:
                    invalid_reason = "gallery_extract_failed"
                if attempt_count >= max(1, max_attempts):
                    invalid_reason = INVALID_RECHECK_EXHAUSTED_REASON

            lookup_id = story_post_id or tile_id or str(prior.get("post_id") or "")
            record = _build_record(
                post_id=lookup_id,
                post_url=best_url,
                author=author,
                label=caption,
                image_urls=image_urls,
                posted_at=posted_at if isinstance(posted_at, str) else None,
                group_id=group_id,
                run_id=resolved_run_id,
                phase="invalid_recheck",
                image_local_keys=list(prior.get("image_local_keys") or []),
                images_downloaded=bool(prior.get("images_downloaded")),
                images_download_skipped=True,
                is_valid=is_valid,
                invalid_reason=None if is_valid else invalid_reason,
                tile_id=tile_id,
                tile_href=photo_url,
                story_post_id=story_post_id,
                sub_caption=(extracted.get("sub_caption") or "").strip(),
            )
            record["match_status"] = "valid" if is_valid else "invalid"
            record["first_seen_at"] = prior.get("first_seen_at") or record.get("first_seen_at")
            record["recheck_from_reason"] = prior_reason
            record["rechecked_at"] = utc_now_iso()
            record["recheck_attempts"] = attempt_count if not is_valid else _recheck_attempt_count(prior)
            record["recheck_exhausted"] = bool(
                not is_valid
                and (
                    attempt_count >= max(1, max_attempts)
                    or invalid_reason == INVALID_RECHECK_EXHAUSTED_REASON
                )
            )

            _, merged = _upsert_post(posts, record)
            upserts.append(
                {
                    "event": "invalid_recheck",
                    "post_id": merged.get("post_id"),
                    "tile_id": tile_id,
                    "prior_reason": prior_reason,
                    "attempt": attempt_count,
                    "max_attempts": max_attempts,
                    "recheck_exhausted": merged.get("recheck_exhausted"),
                    "new_status": merged.get("match_status"),
                    "invalid_reason": merged.get("invalid_reason"),
                    "image_count": len(image_urls),
                    "caption_preview": caption[:80],
                }
            )
            if is_valid:
                promoted += 1
                print(
                    f"{LOG} recheck_promoted {promoted}/{len(candidates)} "
                    f"tile={tile_id} was={prior_reason} images={len(image_urls)} "
                    f"caption={caption[:50]!r}",
                    flush=True,
                )
            else:
                still_invalid += 1
                exhausted_note = " exhausted" if merged.get("recheck_exhausted") else ""
                print(
                    f"{LOG} recheck_still_invalid {idx}/{len(candidates)} "
                    f"tile={tile_id} was={prior_reason} now={invalid_reason} "
                    f"attempt={attempt_count}/{max_attempts}{exhausted_note}",
                    flush=True,
                )
            _human_pause(permalink_pause_sec * 0.5, 0.2, 0.8)

        skip_upserts, remaining_skipped, skip_attempt_map, skipped_rechecked, skipped_recovered = (
            _recheck_skipped_fbids(
                driver,
                group_id=group_id,
                posts=posts,
                progress=progress,
                skipped_limit=skipped_limit,
                skipped_max_attempts=skipped_max,
                permalink_pause_sec=permalink_pause_sec,
                resolved_run_id=resolved_run_id,
            )
        )
        upserts.extend(skip_upserts)

        try:
            session_cookies = driver.get_cookies() or session_cookies
        except Exception:
            pass
    finally:
        _safe_quit_driver(driver)

    export_info: dict[str, Any] = {}
    if upload_minio:
        if upserts:
            _persist_index_and_logs(
                bucket=bucket,
                source_prefix=source_prefix,
                group_id=group_id,
                run_id=resolved_run_id,
                posts=posts,
                upserts=upserts,
            )
            export_info = _rebuild_exports(
                bucket=bucket, source_prefix=source_prefix, group_id=group_id, posts=posts
            )
        if session_cookies:
            upload_json_payload(
                bucket,
                _cookies_key(source_prefix, group_id),
                {"saved_at": utc_now_iso(), "cookies": session_cookies},
            )
        progress_out = dict(progress)
        progress_out["skipped_fbids"] = _normalize_fbid_list(remaining_skipped)
        progress_out["skipped_recheck_attempts"] = {
            str(k): int(v) for k, v in (skip_attempt_map or {}).items()
        }
        progress_out["updated_at"] = utc_now_iso()
        progress_out["schema_version"] = SCHEMA_VERSION
        _atomic_upload_json(bucket, _progress_key(source_prefix, group_id), progress_out)

    result = {
        "schema_version": SCHEMA_VERSION,
        "phase": "invalid_recheck",
        "group_id": group_id,
        "run_id": resolved_run_id,
        "enabled": True,
        "candidates": len(candidates),
        "promoted": promoted,
        "still_invalid": still_invalid,
        "skipped_rechecked": skipped_rechecked,
        "skipped_recovered": skipped_recovered,
        "skipped_remaining": len(remaining_skipped),
        "export": export_info,
        "recheckable_reasons": sorted(RECHECKABLE_INVALID_REASONS),
        "max_attempts_per_post": max_attempts,
    }
    if upload_minio:
        upload_json_payload(
            bucket, _run_result_key(source_prefix, group_id, resolved_run_id), result
        )
    print(f"{LOG} invalid_recheck done {result}", flush=True)
    return result
