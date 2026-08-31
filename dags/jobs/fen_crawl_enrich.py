"""Job2 Enrich — pass-through Job1 valid; Selenium fill incomplete (crawling.py).

Job2 Enrich — pass-through bài Job1 valid; Selenium fill incomplete (crawling.py).
"""
from __future__ import annotations

import os
import re
import time
from typing import Any

from common.chau_ban_schema import utc_now_iso
from common.io_storage import upload_json_payload

from final_exam_nlp_crawl_runner import (
    DEFAULT_MATCH_SOFT_RESTART_EVERY,
    DEFAULT_PERMALINK_PAUSE_SEC,
    FB_BASE_URL,
    _build_driver,
    _classify_record,
    _cookies_key,
    _dedupe_image_urls,
    _drain_selenium_sessions,
    _is_facebook_checkpoint,
    _is_logged_in,
    _is_video_url,
    _read_json_object,
    _safe_quit_driver,
    _selenium_preflight,
    _settings,
)
from final_exam_nlp_two_phase import _ensure_driver_login, _human_pause
from fen_crawl_common import (
    DEFAULT_SOFT_RESTART_EVERY,
    LOG_PIPELINE,
    SCHEMA_CRAWL,
    classify_then_maybe_download,
    download_cdn_immediately,
    enrich_batch_key,
    enrich_invalid_key,
    enrich_valid_key,
    ensure_raw_bucket,
    get_active_batch,
    incomplete_reason,
    load_checkpoint,
    read_jsonl,
    save_checkpoint,
    upsert_task_exports_with_shared,
    write_jsonl,
)

LOG = "[fen_crawl_enrich]"


def _row_already_valid(row: dict[str, Any]) -> bool:
    """True when Job1 already marked valid with caption + CDN images.
    True khi Job1 đã valid với caption + ảnh CDN.
    """
    if row.get("valid") is True:
        caption = str(row.get("caption") or "").strip()
        images = [
            u
            for u in (row.get("image_urls") or row.get("cdn_urls") or [])
            if u and not _is_video_url(str(u))
        ]
        return bool(caption) and bool(images)
    return False


def _attach_immediate_download(
    record: dict[str, Any],
    *,
    bucket: str,
    source_prefix: str,
    group_id: str,
) -> dict[str, Any]:
    """Classify calligraphy then download CDN only when keep.
    Classify thư pháp rồi chỉ tải CDN khi được giữ.
    """
    return classify_then_maybe_download(
        record,
        bucket=bucket,
        source_prefix=source_prefix,
        group_id=group_id,
        log_prefix=LOG,
    )


def _pass_through_record(
    row: dict[str, Any],
    *,
    batch_seq: int,
    run_id: str,
) -> dict[str, Any]:
    """Build enrich record from Job1-valid row / Tạo enrich record từ row Job1 valid."""
    images = _dedupe_image_urls(
        [
            u
            for u in (row.get("image_urls") or row.get("cdn_urls") or [])
            if u and not _is_video_url(str(u))
        ]
    )
    return {
        "schema_version": SCHEMA_CRAWL,
        "post_id": str(row.get("post_id") or ""),
        "permalink": str(row.get("permalink") or ""),
        "batch_seq": batch_seq,
        "valid": True,
        "invalid_reason": None,
        "reason": None,
        "author": str(row.get("author") or row.get("author_hint") or ""),
        "caption": str(row.get("caption") or "").strip(),
        "image_urls": images,
        "cdn_urls": images,
        "photo_urls": [],
        "posted_at": row.get("posted_at"),
        "enrich_source": "job1_passthrough",
        "updated_at": utc_now_iso(),
        "run_id": run_id,
    }


def _inject_anti_popup_css(driver) -> None:
    """Hide chat/dialog overlays (from tests/crawling.py).
    Ẩn overlay chat/dialog (từ tests/crawling.py).
    """
    try:
        driver.execute_script(
            """
            if (!document.getElementById('anti-popup-style')) {
              const style = document.createElement('style');
              style.id = 'anti-popup-style';
              style.innerHTML = `
                div[aria-label*='Chat'], div[aria-label*='Đoạn chat'],
                div[aria-label*='Messenger'], div[data-pagelet='ChatTab'] {
                  display: none !important; visibility: hidden !important;
                  pointer-events: none !important;
                }
              `;
              document.head.appendChild(style);
            }
            """
        )
    except Exception:
        pass


def _expand_see_more(driver, root) -> None:
    """Expand 'See more' via JS without opening photo popup.
    Mở 'Xem thêm' bằng JS, không bấm mở popup ảnh.
    """
    try:
        driver.execute_script(
            """
            const post = arguments[0];
            if (!post) return;
            post.querySelectorAll('span, div[role="button"]').forEach(el => {
              if (el.children.length === 0 && el.innerText) {
                const t = el.innerText.trim();
                if (t === 'Xem thêm' || t === 'See more') el.click();
              }
            });
            """,
            root,
        )
    except Exception:
        pass


def _extract_caption_crawling(root) -> str:
    """Caption from story_message / dir=auto; skip FB chrome.
    Caption từ story_message / dir=auto; bỏ chrome FB.
    """
    try:
        from selenium.webdriver.common.by import By

        from final_exam_nlp_crawl_runner import _is_content_caption, _is_ui_noise_label

        msg = root.find_elements(By.CSS_SELECTOR, "div[data-ad-rendering-role='story_message']")
        if msg and msg[0].text.strip():
            text = msg[0].text.strip()
            if _is_content_caption(text):
                return text
        candidates = root.find_elements(By.CSS_SELECTOR, "div[dir='auto']")
        # Prefer real post text over privacy badge chrome /
        # Ưu tiên nội dung bài thật hơn badge quyền riêng tư
        texts = [
            c.text.strip()
            for c in candidates
            if len(c.text.strip()) > 10
            and not _is_ui_noise_label(c.text.strip())
            and _is_content_caption(c.text.strip())
        ]
        if texts:
            return max(texts, key=len)
    except Exception:
        pass
    return ""


# CDN paths that are emoji/profile avatars, not post photos /
# Path CDN emoji/avatar — không phải ảnh bài
# NOTE: t39.99422 can be real uploads (large); only t39.1997 is emoji pack /
# Lưu ý: t39.99422 có thể là ảnh upload lớn; chỉ t39.1997 là emoji
_NON_POST_CDN_RE = re.compile(
    r"(static\.xx\.fbcdn\.net|rsrc\.php|"
    r"/s24x24/|/s32x32/|/s50x50/|/s60x60/|"
    r"ctp=s24x24|ctp=s32x32|ctp=s50x50|ctp=s60x60|"
    # Profile / default silhouette (…-1/) / Avatar · silhouette
    r"/t39\.30808-1/|/t1\.30497-1/|"
    # Emoji / reaction sticker pack / Gói emoji · reaction
    r"/t39\.1997-)",
    re.I,
)


def _is_post_photo_cdn(url: str) -> bool:
    """True for real post photo CDN URLs / True với URL ảnh bài thật."""
    if not url or not isinstance(url, str):
        return False
    if not url.startswith("http") or "scontent" not in url:
        return False
    if _is_video_url(url) or "video" in url.lower():
        return False
    if _NON_POST_CDN_RE.search(url):
        return False
    return True


def _extract_images_crawling(root) -> list[str]:
    """Post-only CDN photos: skip comment/reply media and stickers.
    Chỉ ảnh bài: bỏ media comment/reply và sticker.
    """
    # JS: prefer /photo anchors; exclude comment-list subtrees only /
    # JS: ưu tiên anchor /photo; chỉ loại subtree danh sách comment
    script = """
    const root = arguments[0];
    const minW = arguments[1] || 100;
    if (!root) return [];

    // Match comment LIST / thread — not the single "Comment" action button /
    // Khớp danh sách/thread comment — không khớp nút "Bình luận" đơn
    const commentListRe = /(comments list|all comments|danh sách bình luận|comment_list|commentsList|UFI2Comments|CommentList|reply_comment)/i;
    const commentItemRe = /^(comment by|bình luận của|reply by|phản hồi của)\\b/i;

    const isCommentSubtree = (el) => {
      let n = el;
      while (n && n !== root) {
        const al = ((n.getAttribute && n.getAttribute('aria-label')) || '');
        const tid = ((n.getAttribute && n.getAttribute('data-testid')) || '');
        const role = ((n.getAttribute && n.getAttribute('role')) || '').toLowerCase();
        if (commentListRe.test(al) || commentListRe.test(tid) || commentItemRe.test(al)) return true;
        // Nested articles under a comments list / Article lồng trong list comment
        if (role === 'article' && n !== root) {
          const parentAl = ((n.parentElement && n.parentElement.getAttribute('aria-label')) || '');
          if (commentListRe.test(parentAl) || /comment/i.test(parentAl)) return true;
        }
        n = n.parentElement;
      }
      return false;
    };

    let cutoff = null;
    for (const el of root.querySelectorAll('[aria-label], [data-testid]')) {
      const al = (el.getAttribute('aria-label') || '').trim();
      const tid = (el.getAttribute('data-testid') || '').trim();
      if (commentListRe.test(al) || commentListRe.test(tid)) {
        cutoff = el;
        break;
      }
    }

    const beforeCutoff = (el) => {
      if (!cutoff) return true;
      if (cutoff.contains(el)) return false;
      const pos = cutoff.compareDocumentPosition(el);
      return (pos & Node.DOCUMENT_POSITION_FOLLOWING) === 0;
    };

    // Drop emoji pack + tiny profiles; keep large uploads (incl. t39.99422) /
    // Bỏ emoji + avatar nhỏ; giữ upload lớn (kể cả t39.99422)
    const badCdn = /static\\.xx\\.fbcdn\\.net|rsrc\\.php|\\/s24x24\\/|\\/s32x32\\/|\\/s50x50\\/|\\/t39\\.30808-1\\/|\\/t1\\.30497-1\\/|\\/t39\\.1997-/i;
    const push = (items, src, wBonus) => {
      if (!src || !src.startsWith('http') || !src.includes('scontent')) return;
      if (badCdn.test(src)) return;
      items.push({ u: src, w: wBonus || 0 });
    };

    const items = [];

    // 1) Post media: images wrapped in /photo or fbid links (highest signal) /
    // 1) Media bài: ảnh bọc trong link /photo hoặc fbid
    for (const a of root.querySelectorAll(
      "a[href*='/photo'], a[href*='fbid='], a[href*='/photos/'], a[href*='multi_permalinks']"
    )) {
      if (isCommentSubtree(a) || !beforeCutoff(a)) continue;
      const href = a.getAttribute('href') || '';
      // Skip comment deep-links / Bỏ deep-link comment
      if (/comment_id=|reply_comment_id=/i.test(href)) continue;
      for (const img of a.querySelectorAll('img')) {
        const src = img.currentSrc || img.src || '';
        const w = img.naturalWidth || img.width || 0;
        const rendered = (img.getBoundingClientRect && img.getBoundingClientRect().width) || 0;
        push(items, src, Math.max(w, rendered, 500));
      }
    }

    // 2) Other large imgs still in post body (before comments list) /
    // 2) Ảnh lớn khác trong thân bài (trước list comment)
    for (const img of root.querySelectorAll('img')) {
      if (isCommentSubtree(img) || !beforeCutoff(img)) continue;
      const src = img.currentSrc || img.src || '';
      const w = img.naturalWidth || img.width || 0;
      const rendered = (img.getBoundingClientRect && img.getBoundingClientRect().width) || 0;
      if (Math.max(w, rendered) < minW) continue;
      push(items, src, Math.max(w, rendered));
    }

    items.sort((a, b) => b.w - a.w);
    const seen = new Set();
    const out = [];
    for (const row of items) {
      const base = row.u.split('?')[0];
      if (seen.has(base)) continue;
      seen.add(base);
      out.push(row.u);
      if (out.length >= 12) break;
    }
    return out;
    """
    images: list[str] = []
    try:
        from selenium.webdriver.remote.webelement import WebElement

        if isinstance(root, WebElement):
            raw = root.parent.execute_script(script, root, 100) or []
            images = [u for u in raw if isinstance(u, str) and _is_post_photo_cdn(u)]
            print(
                f"{LOG} extract_images js_raw={len(raw or [])} kept={len(images)}",
                flush=True,
            )
    except Exception as exc:
        print(f"{LOG} extract_images js_fail err={type(exc).__name__}", flush=True)
        images = []

    # Fallback: large scontent imgs, skip comment-list ancestors /
    # Fallback: ảnh scontent lớn, bỏ tổ tiên comment-list
    if not images:
        try:
            from selenium.webdriver.common.by import By

            seen: set[str] = set()
            for img in root.find_elements(By.CSS_SELECTOR, "img"):
                try:
                    in_comment = bool(
                        img.parent.execute_script(
                            """
                            const el = arguments[0];
                            const listRe = /(comments list|all comments|danh sách bình luận|CommentList|UFI2Comments)/i;
                            const itemRe = /^(comment by|bình luận của)\\b/i;
                            let n = el;
                            while (n) {
                              const al = ((n.getAttribute && n.getAttribute('aria-label')) || '');
                              const tid = ((n.getAttribute && n.getAttribute('data-testid')) || '');
                              if (listRe.test(al) || listRe.test(tid) || itemRe.test(al)) return true;
                              n = n.parentElement;
                            }
                            return false;
                            """,
                            img,
                        )
                    )
                    if in_comment:
                        continue
                except Exception:
                    pass
                src = img.get_attribute("src") or ""
                if not _is_post_photo_cdn(src):
                    continue
                try:
                    if int(img.size.get("width") or 0) < 100:
                        continue
                except Exception:
                    pass
                clean = src.split("?")[0]
                if clean in seen:
                    continue
                seen.add(clean)
                images.append(src)
            print(f"{LOG} extract_images fallback kept={len(images)}", flush=True)
        except Exception:
            pass
    return images


def _extract_author_crawling(root) -> str:
    """Author from heading links (tests/crawling.py)."""
    try:
        from selenium.webdriver.common.by import By

        for sel in ["h2 a", "h3 a", "h4 a", "strong a"]:
            for el in root.find_elements(By.CSS_SELECTOR, sel):
                name = (el.text or "").strip()
                if name and len(name) > 1 and "http" not in name.lower():
                    return name
    except Exception:
        pass
    return ""


def _find_post_root(driver, post_id: str):
    """Find dialog/article root for post_id / Tìm root dialog/article của post_id."""
    try:
        from selenium.webdriver.common.by import By

        # Prefer dialog / article that mentions this post id in links /
        # Ưu tiên dialog/article có link chứa post id
        for sel in ["div[role='dialog']", "div[role='article']", "div[role='feed'] > div"]:
            for el in driver.find_elements(By.CSS_SELECTOR, sel):
                try:
                    html = el.get_attribute("innerHTML") or ""
                    if post_id and post_id in html:
                        return el
                except Exception:
                    continue
        arts = driver.find_elements(By.CSS_SELECTOR, "div[role='dialog'], div[role='article']")
        return arts[0] if arts else driver.find_element(By.TAG_NAME, "body")
    except Exception:
        return None


def _collect_post_photo_hrefs(root) -> list[str]:
    """Href of post photos (exclude comment deep-links).
    Href ảnh bài (bỏ deep-link comment).
    """
    script = """
    const root = arguments[0];
    if (!root) return [];
    const out = [];
    const seen = new Set();
    for (const a of root.querySelectorAll(
      "a[href*='/photo'], a[href*='fbid='], a[href*='/photos/']"
    )) {
      let href = a.href || a.getAttribute('href') || '';
      if (!href || /comment_id=|reply_comment_id=/i.test(href)) continue;
      // Absolute-ify / Chuẩn hoá absolute
      try { href = new URL(href, location.href).href; } catch (e) {}
      const base = href.split('&__cft__')[0].split('&__tn__')[0];
      if (seen.has(base)) continue;
      seen.add(base);
      out.push(base);
      if (out.length >= 8) break;
    }
    return out;
    """
    try:
        from selenium.webdriver.remote.webelement import WebElement

        if isinstance(root, WebElement):
            raw = root.parent.execute_script(script, root) or []
            return [u for u in raw if isinstance(u, str) and u.startswith("http")]
    except Exception:
        pass
    return []


def _extract_images_from_photo_viewer(driver, *, max_images: int = 8) -> list[str]:
    """Large CDN photos from open photo dialog — not comment thumbs.
    Ảnh CDN lớn từ photo dialog đang mở — không lấy thumb comment.
    """
    script = """
    const maxN = arguments[0];
    const dialog = document.querySelector('div[role="dialog"]');
    const dialogHasPhoto = dialog && [...dialog.querySelectorAll('img')].some(
      i => ((i.currentSrc || i.src || '').includes('scontent'))
    );
    // Prefer dialog only when it actually holds CDN photos /
    // Chỉ dùng dialog khi thật sự chứa ảnh CDN
    const root = (dialogHasPhoto && dialog)
      || document.querySelector('[data-pagelet="MediaViewerPhoto"]')
      || document.querySelector('[aria-label*="Photo" i]')
      || document.body;
    const commentItemRe = /^(comment by|bình luận của|reply by)\\b/i;
    const commentListRe = /(comments list|all comments|danh sách bình luận|CommentList|UFI2Comments)/i;
    // Drop emoji pack + tiny profiles; keep large uploads (incl. t39.99422) /
    // Bỏ emoji + avatar nhỏ; giữ upload lớn (kể cả t39.99422)
    const badCdn = /static\\.xx\\.fbcdn\\.net|rsrc\\.php|\\/s24x24\\/|\\/s32x32\\/|\\/s50x50\\/|\\/t39\\.30808-1\\/|\\/t1\\.30497-1\\/|\\/t39\\.1997-/i;
    const inComment = (el) => {
      let n = el;
      while (n && n !== root) {
        const al = ((n.getAttribute && n.getAttribute('aria-label')) || '');
        const tid = ((n.getAttribute && n.getAttribute('data-testid')) || '');
        if (commentListRe.test(al) || commentListRe.test(tid) || commentItemRe.test(al)) return true;
        n = n.parentElement;
      }
      return false;
    };
    const all = [];
    const items = [];
    for (const img of root.querySelectorAll('img')) {
      const src = img.currentSrc || img.src || '';
      if (!src.startsWith('http') || !src.includes('scontent')) continue;
      const w = img.naturalWidth || img.width || 0;
      const rendered = (img.getBoundingClientRect && img.getBoundingClientRect().width) || 0;
      all.push({ u: src.slice(0, 140), w: Math.max(w, rendered), comment: inComment(img), bad: badCdn.test(src) });
      if (inComment(img)) continue;
      if (badCdn.test(src)) continue;
      if (Math.max(w, rendered) < 80) continue;
      items.push({ u: src, w: Math.max(w, rendered) });
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
    return {
      out,
      all: all.slice(0, 12),
      landed: location.href,
      rootTag: root && (root.getAttribute('role') || root.tagName),
      dialogHasPhoto: !!dialogHasPhoto
    };
    """
    try:
        payload = driver.execute_script(script, max_images) or {}
    except Exception as exc:
        print(f"{LOG} photo_viewer_js_fail err={type(exc).__name__}", flush=True)
        return []
    if isinstance(payload, list):
        raw = payload
        debug_all = []
    else:
        raw = payload.get("out") or []
        debug_all = payload.get("all") or []
        print(
            f"{LOG} photo_viewer_debug landed={(payload.get('landed') or '')[:90]} "
            f"candidates={len(debug_all)} kept={len(raw)}",
            flush=True,
        )
        for row in debug_all[:6]:
            print(
                f"{LOG}   cand w={row.get('w')} comment={row.get('comment')} "
                f"bad={row.get('bad')} u={row.get('u')}",
                flush=True,
            )
    return [u for u in raw if isinstance(u, str) and _is_post_photo_cdn(u)]


def _enrich_with_photo_viewer(
    driver,
    root,
    *,
    pause_sec: float,
) -> list[str]:
    """Open first post photo (in-place click preferred) and collect dialog images.
    Mở ảnh bài đầu tiên (ưu tiên click tại chỗ) và gom ảnh từ dialog.
    """
    hrefs = _collect_post_photo_hrefs(root)
    print(f"{LOG} photo_hrefs n={len(hrefs)}", flush=True)

    # Prefer in-place click so session/cookies stay on the group feed /
    # Ưu tiên click tại chỗ để giữ session trên feed group
    try:
        clicked = bool(
            driver.execute_script(
                """
                const root = arguments[0];
                const anchors = [...root.querySelectorAll(
                  "a[href*='/photo'], a[href*='fbid='], a[href*='/photos/']"
                )].filter(a => {
                  const href = a.href || a.getAttribute('href') || '';
                  return href && !/comment_id=|reply_comment_id=/i.test(href);
                });
                if (!anchors.length) return false;
                anchors[0].click();
                return true;
                """,
                root,
            )
        )
        print(f"{LOG} photo_click_inplace ok={clicked}", flush=True)
        if clicked:
            time.sleep(max(2.0, float(pause_sec) + 1.0))
            _inject_anti_popup_css(driver)
            urls = _extract_images_from_photo_viewer(driver)
            if urls:
                return urls
            # Also try og:image / meta fallback / Thử thêm og:image
            meta = driver.execute_script(
                """
                const out = [];
                for (const sel of [
                  'meta[property="og:image"]',
                  'meta[name="twitter:image"]',
                  'link[rel="image_src"]'
                ]) {
                  const el = document.querySelector(sel);
                  const v = el && (el.content || el.href);
                  if (v && v.includes('scontent')) out.push(v);
                }
                // background-image on large layers / background-image lớp lớn
                for (const el of document.querySelectorAll('div[role="dialog"] div, div[role="dialog"] img, img')) {
                  const bg = getComputedStyle(el).backgroundImage || '';
                  const m = bg.match(/url\\(["']?(https:\\/\\/[^"')]+)/);
                  if (m && m[1].includes('scontent')) out.push(m[1]);
                }
                return out;
                """
            ) or []
            meta_urls = [u for u in meta if isinstance(u, str) and _is_post_photo_cdn(u)]
            print(f"{LOG} photo_meta_fallback n={len(meta_urls)}", flush=True)
            if meta_urls:
                return _dedupe_image_urls(meta_urls)
    except Exception as exc:
        print(f"{LOG} photo_click_fail err={type(exc).__name__}", flush=True)

    # Soft-land: group feed then photo URL (deep-link alone often blank) /
    # Đậu mềm: feed group rồi mới photo URL (deep-link đơn thường trống)
    for href in hrefs[:2]:
        try:
            m = re.search(r"idorvanity=(\d+)", href)
            if m:
                driver.get(f"{FB_BASE_URL}/groups/{m.group(1)}/")
                time.sleep(max(1.0, float(pause_sec) * 0.5))
            driver.get(href)
            time.sleep(max(2.5, float(pause_sec) + 1.2))
            _inject_anti_popup_css(driver)
            # Dump img inventory for diagnosis / Dump inventory img để chẩn đoán
            inv = driver.execute_script(
                """
                return {
                  url: location.href,
                  imgs: document.images.length,
                  scontent: [...document.images]
                    .map(i => i.currentSrc || i.src || '')
                    .filter(s => s.includes('scontent')),
                };
                """
            ) or {}
            print(f"{LOG} photo_nav_inv url={str(inv.get('url') or '')[:90]}", flush=True)
            for u in (inv.get("scontent") or [])[:8]:
                print(f"{LOG}   scontent {u[:130]}", flush=True)
            urls = _extract_images_from_photo_viewer(driver)
            print(
                f"{LOG} photo_viewer href={href[:90]} images={len(urls)}",
                flush=True,
            )
            if urls:
                return urls
        except Exception as exc:
            print(
                f"{LOG} photo_viewer_fail err={type(exc).__name__} href={href[:70]}",
                flush=True,
            )
    return []


def _enrich_incomplete_via_crawling(
    driver,
    *,
    group_id: str,
    post_id: str,
    permalink: str,
    seed_caption: str,
    seed_images: list[str],
    seed_author: str,
    pause_sec: float,
) -> dict[str, Any]:
    """Open one post URL (permalink or multi_permalinks) and extract media.
    Mở đúng 1 URL post (permalink hoặc multi_permalinks) rồi extract media.
    """
    nav_mode = (os.environ.get("FEN_ENRICH_NAV") or "multi_permalinks").strip().lower()
    # Seed2: hit the post URL only — no feed GraphQL / Seed2: chỉ mở URL post, không GraphQL feed
    if nav_mode in {"permalink", "permalink_only", "direct"}:
        target = (permalink or "").strip() or (
            f"{FB_BASE_URL}/groups/{group_id}/permalink/{post_id}/"
        )
    else:
        target = f"{FB_BASE_URL}/groups/{group_id}/?multi_permalinks={post_id}"
    landed = ""
    try:
        driver.get(target)
        time.sleep(max(1.2, float(pause_sec) + 0.8))
        _inject_anti_popup_css(driver)
        landed = driver.current_url or ""
    except Exception as exc:
        return {
            "nav_ok": False,
            "landed_url": landed,
            "caption": seed_caption,
            "image_urls": list(seed_images),
            "author": seed_author,
            "error": type(exc).__name__,
        }

    # Stay on group/post context / Phải còn trong context group/post
    if group_id not in (landed or "") and post_id not in (landed or ""):
        print(
            f"{LOG} crawling_nav_junk id={post_id} landed={landed[:90] or '-'}",
            flush=True,
        )
        return {
            "nav_ok": False,
            "landed_url": landed,
            "caption": seed_caption,
            "image_urls": list(seed_images),
            "author": seed_author,
        }

    root = _find_post_root(driver, post_id)
    if root is None:
        return {
            "nav_ok": False,
            "landed_url": landed,
            "caption": seed_caption,
            "image_urls": list(seed_images),
            "author": seed_author,
        }

    _expand_see_more(driver, root)
    time.sleep(0.4)
    caption = _extract_caption_crawling(root) or seed_caption
    # Post photos only — never comment/sticker CDN /
    # Chỉ ảnh bài — không lấy CDN comment/sticker
    images = _dedupe_image_urls(
        [
            u
            for u in (_extract_images_crawling(root) + list(seed_images))
            if _is_post_photo_cdn(u)
        ]
    )
    # Feed card often has stickers only — open photo viewer for real media /
    # Feed card thường chỉ có sticker — mở photo viewer lấy media thật
    if not images:
        images = _dedupe_image_urls(
            _enrich_with_photo_viewer(driver, root, pause_sec=pause_sec)
        )
    author = _extract_author_crawling(root) or seed_author
    return {
        "nav_ok": True,
        "landed_url": landed,
        "caption": caption,
        "image_urls": images,
        "author": author,
        "permalink": permalink,
    }


def _soft_restart(
    *,
    remote_url: str,
    headless: bool,
    page_load_timeout: int,
    bucket: str,
    source_prefix: str,
    group_id: str,
    cookies: list[dict[str, Any]],
) -> tuple[Any, list[dict[str, Any]]]:
    """Quit Chrome and open a fresh session / Quit Chrome và mở session mới."""
    print(f"{LOG} soft_restart every — quit chrome", flush=True)
    _drain_selenium_sessions(remote_url)
    _selenium_preflight(remote_url)
    driver = _build_driver(remote_url, headless, page_load_timeout, enable_perf_logs=False)
    group_feed = f"https://www.facebook.com/groups/{group_id}/"
    cookies = _ensure_driver_login(
        driver=driver,
        bucket=bucket,
        source_prefix=source_prefix,
        group_id=group_id,
        start_url=group_feed,
        cookies=cookies,
        fb_username="",
        fb_password="",
        fb_totp_secret="",
        upload_minio=True,
    )
    _inject_anti_popup_css(driver)
    return driver, cookies


def _persist_enrich_record(
    *,
    bucket: str,
    source_prefix: str,
    group_id: str,
    record: dict[str, Any],
) -> None:
    pid = str(record.get("post_id") or "")
    if record.get("valid"):
        upload_json_payload(bucket, enrich_valid_key(source_prefix, group_id, pid), record)
    else:
        upload_json_payload(bucket, enrich_invalid_key(source_prefix, group_id, pid), record)


def run_enrich_batch(
    *,
    group_id: str,
    soft_restart_every: int = DEFAULT_SOFT_RESTART_EVERY,
    permalink_pause_sec: float = DEFAULT_PERMALINK_PAUSE_SEC,
    headless: bool = False,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Pass-through Job1 valid; Selenium-fill incomplete with crawling extractors.

    Pass-through Job1 valid; Selenium fill incomplete bằng extractor crawling.
    """
    if not group_id.strip():
        raise ValueError("group_id is required")
    soft_restart_every = max(1, int(soft_restart_every or DEFAULT_MATCH_SOFT_RESTART_EVERY))
    permalink_pause_sec = float(permalink_pause_sec or DEFAULT_PERMALINK_PAUSE_SEC)
    resolved_run_id = (run_id or "").strip() or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())

    bucket, source_prefix = ensure_raw_bucket()
    active = get_active_batch(bucket, source_prefix, group_id)
    batch_seq = int(active.get("batch_seq") or 0)
    discover_key = str(active.get("discover_key") or "")
    rows = read_jsonl(bucket, discover_key) if discover_key else []

    already_valid = [r for r in rows if _row_already_valid(r)]
    need_selenium = [r for r in rows if not _row_already_valid(r)]
    print(
        f"{LOG} start batch_seq={batch_seq} pending={len(rows)} "
        f"passthrough={len(already_valid)} incomplete={len(need_selenium)} "
        f"soft_restart_every={soft_restart_every}",
        flush=True,
    )

    if not rows:
        print(f"{LOG} empty batch — skip", flush=True)
        write_jsonl(bucket, enrich_batch_key(source_prefix, group_id, batch_seq), [])
        return {
            "ok": True,
            "batch_seq": batch_seq,
            "valid": 0,
            "invalid": 0,
            "skipped": True,
        }

    enrich_rows: list[dict[str, Any]] = []
    n_valid = 0
    n_invalid = 0
    n_passthrough = 0
    processed = 0
    stop_reason = "ok"
    session_lost = False

    # Pass-through Job1 valid without Selenium /
    # Pass-through Job1 valid không cần Selenium
    for row in already_valid:
        record = _pass_through_record(row, batch_seq=batch_seq, run_id=resolved_run_id)
        # CDN expires fast — classify then download /
        # CDN hết hạn nhanh — classify rồi tải
        record = _attach_immediate_download(
            record, bucket=bucket, source_prefix=source_prefix, group_id=group_id
        )
        enrich_rows.append(record)
        if record.get("valid"):
            n_valid += 1
        else:
            n_invalid += 1
        if record.get("enrich_source") == "job1_passthrough" and record.get("valid"):
            n_passthrough += 1
        _persist_enrich_record(
            bucket=bucket, source_prefix=source_prefix, group_id=group_id, record=record
        )
        print(
            f"{LOG} passthrough id={record['post_id']} valid={record.get('valid')} "
            f"images={len(record.get('cdn_urls') or [])} "
            f"calligraphy={record.get('is_calligraphy')} kind={record.get('calligraphy_kind')} "
            f"downloaded={record.get('images_downloaded')} "
            f"caption={(str(record.get('caption') or '')[:40] + '…') if len(str(record.get('caption') or '')) > 40 else record.get('caption')!r}"
            + (
                f" reason={record.get('invalid_reason')}"
                if not record.get("valid")
                else ""
            ),
            flush=True,
        )

    if not need_selenium:
        write_jsonl(bucket, enrich_batch_key(source_prefix, group_id, batch_seq), enrich_rows)
        upsert_task_exports_with_shared(
            bucket=bucket,
            source_prefix=source_prefix,
            group_id=group_id,
            records=enrich_rows,
        )
        checkpoint = load_checkpoint(bucket, source_prefix, group_id)
        stats = dict(checkpoint.get("stats") or {})
        stats["enriched"] = int(stats.get("enriched") or 0) + len(enrich_rows)
        stats["valid"] = int(stats.get("valid") or 0) + n_valid
        stats["invalid"] = int(stats.get("invalid") or 0) + n_invalid
        stats["passthrough"] = int(stats.get("passthrough") or 0) + n_passthrough
        checkpoint["stats"] = stats
        checkpoint["last_run_id"] = resolved_run_id
        save_checkpoint(bucket, source_prefix, group_id, checkpoint)
        print(
            f"{LOG} end batch_seq={batch_seq} valid={n_valid} invalid={n_invalid} "
            f"passthrough={n_passthrough} (no selenium)",
            flush=True,
        )
        print(
            f"{LOG_PIPELINE} enrich_done batch_seq={batch_seq} valid={n_valid} invalid={n_invalid}",
            flush=True,
        )
        return {
            "ok": True,
            "batch_seq": batch_seq,
            "valid": n_valid,
            "invalid": n_invalid,
            "passthrough": n_passthrough,
            "stop_reason": "ok",
        }

    settings = _settings()
    remote_url = settings["selenium_remote_url"]
    page_load_timeout = int(settings["page_load_timeout_sec"])
    cookies_raw = _read_json_object(bucket, _cookies_key(source_prefix, group_id)) or {}
    cookies = list(cookies_raw.get("cookies") or [])

    _drain_selenium_sessions(remote_url)
    driver = None
    try:
        driver, cookies = _soft_restart(
            remote_url=remote_url,
            headless=headless,
            page_load_timeout=page_load_timeout,
            bucket=bucket,
            source_prefix=source_prefix,
            group_id=group_id,
            cookies=cookies,
        )

        for idx, row in enumerate(need_selenium, start=1):
            pid = str(row.get("post_id") or "").strip()
            permalink = str(row.get("permalink") or "").strip()
            if not pid or not permalink:
                continue

            if processed > 0 and processed % soft_restart_every == 0:
                _safe_quit_driver(driver)
                driver, cookies = _soft_restart(
                    remote_url=remote_url,
                    headless=headless,
                    page_load_timeout=page_load_timeout,
                    bucket=bucket,
                    source_prefix=source_prefix,
                    group_id=group_id,
                    cookies=cookies,
                )
                print(f"{LOG} soft_restart after={processed}", flush=True)

            if _is_facebook_checkpoint(driver):
                stop_reason = "facebook_checkpoint"
                session_lost = True
                print(f"{LOG} facebook_checkpoint — stop enrich", flush=True)
                break
            if not _is_logged_in(driver):
                stop_reason = "session_lost"
                session_lost = True
                print(f"{LOG} session_lost — HITL login on noVNC", flush=True)
                break

            seed_images = [
                u
                for u in (row.get("image_urls") or [])
                if u and not _is_video_url(str(u))
            ]
            extracted = _enrich_incomplete_via_crawling(
                driver,
                group_id=group_id,
                post_id=pid,
                permalink=permalink,
                seed_caption=str(row.get("caption") or ""),
                seed_images=seed_images,
                seed_author=str(row.get("author") or row.get("author_hint") or ""),
                pause_sec=permalink_pause_sec,
            )
            caption = str(extracted.get("caption") or "").strip()
            author = str(extracted.get("author") or "").strip()
            cdn_urls = _dedupe_image_urls(
                [u for u in (extracted.get("image_urls") or []) if not _is_video_url(u)]
            )
            nav_ok = bool(extracted.get("nav_ok"))
            if nav_ok:
                is_valid, reason = _classify_record(caption, cdn_urls)
                if not is_valid and reason:
                    mapping = {
                        "missing_label": "missing_caption",
                        "missing_label_and_image": "missing_caption_and_image",
                    }
                    reason = mapping.get(reason, reason)
            else:
                is_valid, reason = False, "crawling_nav_failed"
                if not reason:
                    reason = incomplete_reason(caption, cdn_urls)

            record = {
                "schema_version": SCHEMA_CRAWL,
                "post_id": pid,
                "permalink": permalink,
                "batch_seq": batch_seq,
                "valid": bool(is_valid),
                "invalid_reason": None if is_valid else reason,
                "reason": None if is_valid else reason,
                "author": author,
                "caption": caption,
                "image_urls": cdn_urls,
                "cdn_urls": cdn_urls,
                "photo_urls": [],
                "posted_at": row.get("posted_at"),
                "enrich_source": "selenium_crawling",
                "landed_url": extracted.get("landed_url") or "",
                "updated_at": utc_now_iso(),
                "run_id": resolved_run_id,
            }
            # Download CDN immediately while URL still live /
            # Tải CDN ngay khi URL còn sống
            if is_valid:
                record = _attach_immediate_download(
                    record, bucket=bucket, source_prefix=source_prefix, group_id=group_id
                )
            enrich_rows.append(record)
            if record.get("valid"):
                n_valid += 1
            else:
                n_invalid += 1
            _persist_enrich_record(
                bucket=bucket, source_prefix=source_prefix, group_id=group_id, record=record
            )
            print(
                f"{LOG} {idx}/{len(need_selenium)} id={pid} valid={bool(record.get('valid'))} "
                f"nav_ok={nav_ok} author={author!r} images={len(cdn_urls)} "
                f"calligraphy={record.get('is_calligraphy')} kind={record.get('calligraphy_kind')} "
                f"downloaded={record.get('images_downloaded')} "
                f"caption={(caption[:40] + '…') if len(caption) > 40 else caption!r}"
                + (
                    f" reason={record.get('invalid_reason') or reason}"
                    if not record.get("valid")
                    else ""
                ),
                flush=True,
            )
            processed += 1
            _human_pause(permalink_pause_sec * 0.4, 0.2, 0.5)

    finally:
        print(f"{LOG} refresh quit_chrome after={processed}", flush=True)
        _safe_quit_driver(driver)
        _drain_selenium_sessions(remote_url)

    write_jsonl(bucket, enrich_batch_key(source_prefix, group_id, batch_seq), enrich_rows)

    # Task.xlsx B1: upsert export/valid_post.jsonl + invalid_post.jsonl /
    # Task.xlsx B1: upsert file nộp bài
    upsert_task_exports_with_shared(
        bucket=bucket,
        source_prefix=source_prefix,
        group_id=group_id,
        records=enrich_rows,
    )

    checkpoint = load_checkpoint(bucket, source_prefix, group_id)
    stats = dict(checkpoint.get("stats") or {})
    stats["enriched"] = int(stats.get("enriched") or 0) + n_passthrough + processed
    stats["valid"] = int(stats.get("valid") or 0) + n_valid
    stats["invalid"] = int(stats.get("invalid") or 0) + n_invalid
    stats["passthrough"] = int(stats.get("passthrough") or 0) + n_passthrough
    checkpoint["stats"] = stats
    checkpoint["last_run_id"] = resolved_run_id
    if session_lost:
        checkpoint["should_continue"] = False
        checkpoint["stop_reason"] = stop_reason
    save_checkpoint(bucket, source_prefix, group_id, checkpoint)

    print(
        f"{LOG} end batch_seq={batch_seq} valid={n_valid} invalid={n_invalid} "
        f"passthrough={n_passthrough} reason={stop_reason}",
        flush=True,
    )
    print(
        f"{LOG_PIPELINE} enrich_done batch_seq={batch_seq} valid={n_valid} invalid={n_invalid}",
        flush=True,
    )
    return {
        "ok": not session_lost,
        "batch_seq": batch_seq,
        "valid": n_valid,
        "invalid": n_invalid,
        "passthrough": n_passthrough,
        "stop_reason": stop_reason,
    }


def run_enrich_one_permalink(
    *,
    group_id: str,
    permalink: str,
    permalink_pause_sec: float = DEFAULT_PERMALINK_PAUSE_SEC,
    headless: bool = False,
    seed_caption: str = "",
    seed_images: list[str] | None = None,
    seed_author: str = "",
    force_selenium: bool = True,
) -> dict[str, Any]:
    """Smoke-test Job2 on one permalink (crawling multi_permalinks path).

    Smoke-test Job2 trên 1 permalink (path crawling multi_permalinks).
    """
    permalink = (permalink or "").strip()
    group_id = (group_id or "").strip()
    if not group_id or not permalink:
        raise ValueError("group_id and permalink are required")
    pid_m = re.search(r"/(?:permalink|posts)/(\d+)", permalink)
    pid = pid_m.group(1) if pid_m else "unknown"
    permalink_pause_sec = float(permalink_pause_sec or DEFAULT_PERMALINK_PAUSE_SEC)

    # Optional Job1-valid smoke without browser /
    # Smoke Job1-valid không cần browser
    if not force_selenium and seed_caption.strip() and seed_images:
        is_valid, reason = _classify_record(seed_caption.strip(), list(seed_images))
        print(
            f"{LOG} smoke_one passthrough id={pid} valid={is_valid} "
            f"images={len(seed_images)} caption={seed_caption[:60]!r}",
            flush=True,
        )
        return {
            "ok": True,
            "post_id": pid,
            "valid": bool(is_valid),
            "reason": reason,
            "enrich_source": "job1_passthrough",
            "caption": seed_caption.strip(),
            "cdn_urls": list(seed_images),
            "author": seed_author,
        }

    bucket, source_prefix = ensure_raw_bucket()
    settings = _settings()
    remote_url = settings["selenium_remote_url"]
    page_load_timeout = int(settings["page_load_timeout_sec"])
    cookies_raw = _read_json_object(bucket, _cookies_key(source_prefix, group_id)) or {}
    cookies = list(cookies_raw.get("cookies") or [])

    print(
        f"{LOG} smoke_one start id={pid} permalink={permalink[:100]} mode=crawling",
        flush=True,
    )
    _drain_selenium_sessions(remote_url)
    driver = None
    try:
        driver, cookies = _soft_restart(
            remote_url=remote_url,
            headless=headless,
            page_load_timeout=page_load_timeout,
            bucket=bucket,
            source_prefix=source_prefix,
            group_id=group_id,
            cookies=cookies,
        )
        if _is_facebook_checkpoint(driver):
            print(f"{LOG} smoke_one facebook_checkpoint", flush=True)
            return {"ok": False, "reason": "facebook_checkpoint"}
        if not _is_logged_in(driver):
            print(f"{LOG} smoke_one session_lost", flush=True)
            return {"ok": False, "reason": "session_lost"}

        extracted = _enrich_incomplete_via_crawling(
            driver,
            group_id=group_id,
            post_id=pid,
            permalink=permalink,
            seed_caption=seed_caption,
            seed_images=list(seed_images or []),
            seed_author=seed_author,
            pause_sec=permalink_pause_sec,
        )
        caption = str(extracted.get("caption") or "").strip()
        author = str(extracted.get("author") or "").strip()
        nav_ok = bool(extracted.get("nav_ok"))
        landed = str(extracted.get("landed_url") or "")
        cdn_urls = _dedupe_image_urls(
            [u for u in (extracted.get("image_urls") or []) if not _is_video_url(u)]
        )
        if nav_ok:
            is_valid, reason = _classify_record(caption, cdn_urls)
        else:
            is_valid, reason = False, "crawling_nav_failed"

        result = {
            "ok": True,
            "post_id": pid,
            "valid": bool(is_valid),
            "reason": reason,
            "nav_ok": nav_ok,
            "landed_url": landed,
            "author": author,
            "caption": caption,
            "cdn_urls": cdn_urls,
            "enrich_source": "selenium_crawling",
        }
        # Smoke path: classify then download when valid /
        # Smoke: classify rồi tải khi valid
        if is_valid and cdn_urls:
            gated = classify_then_maybe_download(
                {
                    **result,
                    "batch_seq": 0,
                    "image_urls": cdn_urls,
                },
                bucket=bucket,
                source_prefix=source_prefix,
                group_id=group_id,
                log_prefix=LOG,
            )
            result.update(
                {
                    "valid": gated.get("valid"),
                    "reason": gated.get("invalid_reason") or gated.get("reason"),
                    "is_calligraphy": gated.get("is_calligraphy"),
                    "calligraphy_kind": gated.get("calligraphy_kind"),
                    "media_class": gated.get("media_class"),
                    "calligraphy_confidence": gated.get("calligraphy_confidence"),
                    "images_downloaded": gated.get("images_downloaded"),
                    "image_local_keys": gated.get("image_local_keys"),
                }
            )

        print(
            f"{LOG} smoke_one id={pid} valid={result.get('valid')} nav_ok={nav_ok} "
            f"landed={landed[:90] or '-'} author={author!r} images={len(cdn_urls)} "
            f"calligraphy={result.get('is_calligraphy')} kind={result.get('calligraphy_kind')} "
            f"downloaded={result.get('images_downloaded')} "
            f"caption={(caption[:80] + '…') if len(caption) > 80 else caption!r}"
            + (f" reason={result.get('reason')}" if not result.get("valid") else ""),
            flush=True,
        )
        return result
    finally:
        print(f"{LOG} smoke_one quit_chrome", flush=True)
        _safe_quit_driver(driver)
        _drain_selenium_sessions(remote_url)
