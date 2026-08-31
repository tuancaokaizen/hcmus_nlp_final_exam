"""Classify post images as handwritten calligraphy vs spam (Gemini vision).

Phân loại ảnh bài: thư pháp viết tay vs spam (Gemini vision).
Uses the same [gemini_opencv] OpenAI-compatible client as FEN OCR.
Dùng cùng client [gemini_opencv] như OCR FEN.
"""
from __future__ import annotations

import base64
import json
import os
import re
import time
from typing import Any

from common.chau_ban_schema import extract_json_object, utc_now_iso
from common.config import get_value, load_config
from common.ocr_helpers import resize_png_bytes

LOG = "[fen_calligraphy]"

# Handwritten / mixed count as keep; printed-only rejected for this corpus /
# Viết tay / mixed giữ; in sẵn thì loại khỏi corpus này
ACCEPT_KINDS = frozenset({"handwritten", "mixed"})
# 0 = classify every CDN URL of the post / 0 = classify mọi URL CDN của post
DEFAULT_MAX_CLASSIFY = 0

CLASSIFY_PROMPT = """You classify ONE image from a Chinese calligraphy Facebook group.

Decide if the main subject is HANDWRITTEN calligraphy (brush/pen ink characters on paper/scroll/board), or not (spam / selfie / meme / screenshot / landscape / printed poster only / emoji collage / product ad).

Return ONLY compact JSON (no markdown):
{
  "is_calligraphy": true,
  "calligraphy_kind": "handwritten",
  "media_class": "calligraphy",
  "confidence": 0.0,
  "reason": "short english reason"
}

Rules:
- calligraphy_kind: handwritten | printed | mixed | none
- media_class: calligraphy | spam | photo_other | unclear
- is_calligraphy=true ONLY if ink handwriting of CJK characters is clearly visible as the main subject
- printed-only posters without handwriting → is_calligraphy=false, calligraphy_kind=printed
- selfies, memes, chat screenshots, ads, pure landscapes → media_class=spam or photo_other
- confidence is 0..1
"""

_FALLBACK_MODELS = ("gemini-3.5-flash-low",)


def calligraphy_classify_enabled() -> bool:
    """Env FEN_CALLIGRAPHY_CLASSIFY (default on) / Biến môi trường, mặc định bật."""
    raw = os.environ.get("FEN_CALLIGRAPHY_CLASSIFY", "true").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def max_classify_images() -> int:
    """0 or unset = no cap (classify all URLs). Positive = hard cap.

    0 hoặc không set = không trần (classify hết URL). Số dương = trần cứng.
    """
    raw = os.environ.get("FEN_CALLIGRAPHY_MAX_IMAGES", str(DEFAULT_MAX_CLASSIFY)).strip()
    try:
        value = int(raw)
    except ValueError:
        return 0
    return max(0, value)


def _empty_result(*, error: str | None = None) -> dict[str, Any]:
    return {
        "is_calligraphy": None,
        "calligraphy_kind": None,
        "media_class": "unclear",
        "calligraphy_confidence": None,
        "classify_reason": error or "",
        "classify_model": None,
        "classified_at": utc_now_iso(),
        "classify_ok": False,
        "classify_error": error,
        "keep_as_valid": False,
        "keep": False,
        "image_labels": [],
        "keep_urls": [],
        "kept_image_count": 0,
        "classified_image_count": 0,
    }


def _normalize_kind(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    if text in {"handwritten", "handwriting", "brush", "ink"}:
        return "handwritten"
    if text in {"printed", "print", "typeset"}:
        return "printed"
    if text in {"mixed", "both"}:
        return "mixed"
    if text in {"none", "no", "n/a", "na"}:
        return "none"
    return text or None


def _normalize_media_class(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"calligraphy", "spam", "photo_other", "unclear"}:
        return text
    return "unclear"


def _decide_keep(is_calli: bool, kind: str | None, media: str, conf: float) -> bool:
    """True when this single image should be kept / True khi giữ ảnh này."""
    keep = bool(is_calli) and (kind in ACCEPT_KINDS)
    if media in {"spam", "photo_other"}:
        return False
    if media == "unclear" and conf < 0.55:
        return False
    if kind == "printed":
        return False
    return keep


def _rank_urls(urls: list[str]) -> list[str]:
    """Prefer larger-looking CDN URLs first / Ưu tiên URL trông lớn hơn trước."""
    return sorted(
        urls,
        key=lambda u: (
            0 if re.search(r"cstp=mx\d{3,}", u) else 1,
            0 if "t39.99422" in u or "t39.30808-6" in u else 1,
            len(u),
        ),
    )


def classify_calligraphy_bytes(image_bytes: bytes) -> dict[str, Any]:
    """Run Gemini vision classify on image bytes / Phân loại thư pháp từ byte ảnh."""
    if not image_bytes or len(image_bytes) < 1024:
        return _empty_result(error="image_too_small")

    try:
        from openai import OpenAI
    except ModuleNotFoundError as exc:
        return _empty_result(error=f"missing_openai:{exc}")

    cfg = load_config()
    from common.fen_stage_config import stage_api_key, stage_base_url, stage_model
    api_key = stage_api_key("fen_calligraphy")
    if not api_key:
        api_key = get_value(cfg, "gemini_opencv", "api_key", fallback="")
    if not api_key:
        return _empty_result(error="missing_gemini_api_key")

    base_url = stage_base_url("fen_calligraphy")
    primary = stage_model("fen_calligraphy", fallback="gemini-3.5-flash-low")
    max_retries = int(get_value(cfg, "gemini_opencv", "max_retries", fallback="4"))
    max_side = int(get_value(cfg, "gemini_opencv", "max_image_side", fallback="1024"))

    # Downscale for cheap classify / Thu nhỏ để classify rẻ
    try:
        image_bytes = resize_png_bytes(image_bytes, max_side=max_side)
    except Exception:
        # Non-PNG (jpeg): re-encode via Pillow / Không phải PNG: encode lại bằng Pillow
        try:
            from io import BytesIO
            from PIL import Image

            img = Image.open(BytesIO(image_bytes)).convert("RGB")
            w, h = img.size
            longest = max(w, h)
            if longest > max_side:
                scale = max_side / float(longest)
                img = img.resize(
                    (max(1, int(w * scale)), max(1, int(h * scale))),
                    Image.Resampling.LANCZOS,
                )
            buf = BytesIO()
            img.save(buf, format="PNG")
            image_bytes = buf.getvalue()
        except Exception as exc:
            return _empty_result(error=f"image_decode_failed:{type(exc).__name__}")

    image_b64 = base64.standard_b64encode(image_bytes).decode("ascii")
    client_kwargs: dict[str, Any] = {"api_key": api_key}
    if base_url:
        client_kwargs["base_url"] = base_url
    client = OpenAI(**client_kwargs)

    models: list[str] = []
    for name in [primary, *_FALLBACK_MODELS]:
        if name and name not in models:
            models.append(name)

    last_error: str | None = None
    for model_name in models:
        for attempt in range(max_retries):
            try:
                response = client.chat.completions.create(
                    model=model_name,
                    messages=[
                        {
                            "role": "user",
                            "content": [
                                {"type": "text", "text": CLASSIFY_PROMPT},
                                {
                                    "type": "image_url",
                                    "image_url": {
                                        "url": f"data:image/png;base64,{image_b64}"
                                    },
                                },
                            ],
                        }
                    ],
                    max_tokens=256,
                )
                text = (response.choices[0].message.content or "").strip()
                parsed = extract_json_object(text) or {}
                if not isinstance(parsed, dict) or not parsed:
                    # Try loose JSON extract / Thử tách JSON lỏng
                    match = re.search(r"\{[\s\S]*\}", text)
                    if match:
                        try:
                            parsed = json.loads(match.group(0))
                        except json.JSONDecodeError:
                            parsed = {}
                if not isinstance(parsed, dict) or not parsed:
                    last_error = "empty_or_bad_json"
                    if attempt < max_retries - 1:
                        time.sleep(1.5)
                        continue
                    break

                kind = _normalize_kind(parsed.get("calligraphy_kind"))
                is_calli = parsed.get("is_calligraphy")
                if isinstance(is_calli, str):
                    is_calli = is_calli.strip().lower() in {"1", "true", "yes"}
                else:
                    is_calli = bool(is_calli)
                media = _normalize_media_class(parsed.get("media_class"))
                try:
                    conf = float(parsed.get("confidence") or parsed.get("calligraphy_confidence") or 0)
                except (TypeError, ValueError):
                    conf = 0.0
                conf = max(0.0, min(1.0, conf))
                reason = str(parsed.get("reason") or "").strip()[:240]

                # Gate: handwritten calligraphy only / Cổng: chỉ thư pháp viết tay
                keep = _decide_keep(bool(is_calli), kind, media, conf)
                if media in {"spam", "photo_other"}:
                    is_calli = False

                return {
                    "is_calligraphy": bool(is_calli),
                    "calligraphy_kind": kind,
                    "media_class": media,
                    "calligraphy_confidence": conf,
                    "classify_reason": reason,
                    "classify_model": model_name,
                    "classified_at": utc_now_iso(),
                    "classify_ok": True,
                    "classify_error": None,
                    "keep": keep,
                    "keep_as_valid": keep,
                }
            except Exception as exc:
                last_error = f"{type(exc).__name__}:{exc}"
                msg = str(exc)
                if "429" in msg and attempt < max_retries - 1:
                    time.sleep(min(15.0 * (attempt + 1), 60.0))
                    continue
                # Try next model on unavailable / Thử model kế khi không khả dụng
                break
    return _empty_result(error=last_error or "classify_failed")


def classify_cdn_urls(image_urls: list[str], *, timeout: int = 25) -> dict[str, Any]:
    """Classify each CDN image; keep handwritten calligraphy URLs.

    Phân loại từng ảnh CDN; giữ URL thư pháp viết tay.
    Post valid if ≥1 image keep / Post valid nếu ≥1 ảnh keep.
    """
    from final_exam_nlp_crawl_runner import _fetch_image

    urls = [u for u in (image_urls or []) if isinstance(u, str) and u.startswith("http")]
    if not urls:
        return _empty_result(error="no_cdn_urls")

    ranked = _rank_urls(urls)
    cap = max_classify_images()
    to_classify = ranked if cap <= 0 else ranked[:cap]
    image_labels: list[dict[str, Any]] = []
    keep_urls: list[str] = []
    any_ok = False
    last_err: str | None = None
    best_keep: dict[str, Any] | None = None

    for idx, url in enumerate(to_classify, start=1):
        label: dict[str, Any] = {
            "index": idx,
            "url": url[:180],
            "keep": False,
            "classify_ok": False,
        }
        try:
            payload, _ctype = _fetch_image(url, timeout)
            if len(payload) < 4096:
                label["classify_error"] = f"too_small:{len(payload)}"
                last_err = label["classify_error"]
                image_labels.append(label)
                continue
            result = classify_calligraphy_bytes(payload)
            any_ok = any_ok or bool(result.get("classify_ok"))
            if result.get("classify_error"):
                last_err = str(result.get("classify_error"))
            label.update(
                {
                    "is_calligraphy": result.get("is_calligraphy"),
                    "calligraphy_kind": result.get("calligraphy_kind"),
                    "media_class": result.get("media_class"),
                    "calligraphy_confidence": result.get("calligraphy_confidence"),
                    "classify_reason": result.get("classify_reason"),
                    "classify_model": result.get("classify_model"),
                    "classify_ok": bool(result.get("classify_ok")),
                    "classify_error": result.get("classify_error"),
                    "keep": bool(result.get("keep")),
                }
            )
            if label["keep"]:
                keep_urls.append(url)
                if best_keep is None or float(
                    label.get("calligraphy_confidence") or 0
                ) > float(best_keep.get("calligraphy_confidence") or 0):
                    best_keep = label
        except Exception as exc:
            last_err = f"{type(exc).__name__}:{exc}"
            label["classify_error"] = last_err
        image_labels.append(label)
        print(
            f"{LOG} image[{idx}/{len(to_classify)}] keep={label.get('keep')} "
            f"kind={label.get('calligraphy_kind')} media={label.get('media_class')} "
            f"conf={label.get('calligraphy_confidence')} err={label.get('classify_error') or '-'}",
            flush=True,
        )

    skipped = len(ranked) - len(to_classify)
    return _finish_classify(
        image_labels=image_labels,
        keep_urls=keep_urls,
        any_ok=any_ok,
        last_err=last_err,
        skipped=skipped,
        classified_n=len(to_classify),
        best_keep=best_keep,
    )


def _finish_classify(
    *,
    image_labels: list[dict[str, Any]],
    keep_urls: list[str],
    any_ok: bool,
    last_err: str | None,
    skipped: int,
    classified_n: int,
    best_keep: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build post-level classify result from per-image labels.
    Gộp kết quả classify cấp post từ label từng ảnh.
    """
    if keep_urls and best_keep:
        return {
            "is_calligraphy": True,
            "calligraphy_kind": best_keep.get("calligraphy_kind"),
            "media_class": best_keep.get("media_class") or "calligraphy",
            "calligraphy_confidence": best_keep.get("calligraphy_confidence"),
            "classify_reason": best_keep.get("classify_reason") or "",
            "classify_model": best_keep.get("classify_model"),
            "classified_at": utc_now_iso(),
            "classify_ok": True,
            "classify_error": None,
            "keep_as_valid": True,
            "keep": True,
            "image_labels": image_labels,
            "keep_urls": keep_urls,
            "kept_image_count": len(keep_urls),
            "classified_image_count": classified_n,
            "skipped_unclassified": skipped,
        }

    if not any_ok:
        out = _empty_result(error=last_err or "classify_failed")
        out["image_labels"] = image_labels
        out["classified_image_count"] = classified_n
        out["skipped_unclassified"] = skipped
        return out

    medias = [str(x.get("media_class") or "") for x in image_labels]
    kinds = [str(x.get("calligraphy_kind") or "") for x in image_labels]
    if any(m == "spam" for m in medias) and not keep_urls:
        media = "spam"
        reason = "all_images_spam_or_non_calligraphy"
    elif all(k == "printed" for k in kinds if k) and kinds:
        media = "photo_other"
        reason = "printed_not_handwritten"
    else:
        media = "photo_other"
        reason = "not_calligraphy"

    best_any = max(
        (x for x in image_labels if x.get("classify_ok")),
        key=lambda x: float(x.get("calligraphy_confidence") or 0),
        default=image_labels[0] if image_labels else {},
    )
    return {
        "is_calligraphy": False,
        "calligraphy_kind": best_any.get("calligraphy_kind") or "none",
        "media_class": media,
        "calligraphy_confidence": best_any.get("calligraphy_confidence"),
        "classify_reason": reason,
        "classify_model": best_any.get("classify_model"),
        "classified_at": utc_now_iso(),
        "classify_ok": True,
        "classify_error": None,
        "keep_as_valid": False,
        "keep": False,
        "image_labels": image_labels,
        "keep_urls": [],
        "kept_image_count": 0,
        "classified_image_count": classified_n,
        "skipped_unclassified": skipped,
    }


def apply_calligraphy_gate(record: dict[str, Any], classify: dict[str, Any]) -> dict[str, Any]:
    """Stamp per-image labels; keep only calligraphy URLs; invalidate if none.
    Gắn label từng ảnh; chỉ giữ URL thư pháp; invalid nếu không có.
    """
    out = dict(record)
    keep_urls = list(classify.get("keep_urls") or [])
    image_labels = list(classify.get("image_labels") or [])

    out["is_calligraphy"] = classify.get("is_calligraphy")
    out["calligraphy_kind"] = classify.get("calligraphy_kind")
    out["media_class"] = classify.get("media_class")
    out["calligraphy_confidence"] = classify.get("calligraphy_confidence")
    out["classify_reason"] = classify.get("classify_reason")
    out["classify_model"] = classify.get("classify_model")
    out["classified_at"] = classify.get("classified_at") or utc_now_iso()
    out["classify_ok"] = bool(classify.get("classify_ok"))
    out["image_labels"] = image_labels
    out["kept_image_count"] = int(classify.get("kept_image_count") or len(keep_urls))
    out["classified_image_count"] = int(classify.get("classified_image_count") or 0)
    if classify.get("skipped_unclassified") is not None:
        out["skipped_unclassified"] = classify.get("skipped_unclassified")
    if classify.get("classify_error"):
        out["classify_error"] = classify.get("classify_error")

    keep = bool(classify.get("keep_as_valid")) and bool(keep_urls)
    if keep:
        out["valid"] = True
        out["invalid_reason"] = None
        out["reason"] = None
        # Replace CDN lists with kept calligraphy only /
        # Thay list CDN bằng chỉ ảnh thư pháp được giữ
        out["cdn_urls"] = keep_urls
        out["image_urls"] = keep_urls
        out["image_count"] = len(keep_urls)
        return out

    out["valid"] = False
    if not classify.get("classify_ok"):
        reason = "classify_failed"
    elif classify.get("media_class") == "spam":
        reason = "spam_image"
    elif classify.get("calligraphy_kind") == "printed" or (
        classify.get("classify_reason") == "printed_not_handwritten"
    ):
        reason = "printed_not_handwritten"
    else:
        reason = "not_calligraphy"
    out["invalid_reason"] = reason
    out["reason"] = reason
    out["images_downloaded"] = False
    out["image_local_keys"] = []
    out["cdn_urls"] = []
    out["image_urls"] = []
    out["image_count"] = 0
    return out