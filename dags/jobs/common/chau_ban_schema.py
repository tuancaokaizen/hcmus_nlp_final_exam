from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any


PIPELINE_VERSION = "v2.5"

_TOC_HEADER_LABEL_RE = re.compile(
    r"^\s*(tr[íi]ch\s+y[ếe]u|m[ụu]c\s+l[ụuựạ]c\s+ch[âa]u\s+b[ảa]n\s+tri[ềe]u\s+nguy[ễe]n)\s*$",
    flags=re.IGNORECASE,
)
_HAN_CHAR_RE = re.compile(r"[\u3400-\u9fff]")
_LATIN_LETTER_RE = re.compile(r"[A-Za-zÀ-ỹ]")
_WIDE_GAP_RE = re.compile(r"(?:\t+| {3,})")


def _canonical_toc_header(line: str) -> str | None:
    """Return a canonical known TOC header or None.

    Trả về header mục lục chuẩn đã biết hoặc None.
    """
    compact = re.sub(r"\s+", " ", str(line or "")).strip()
    upper = compact.upper()
    if upper == "TRIỀU GIA LONG":
        return "TRIỀU GIA LONG"
    if upper == "TRIỀU MINH MỆNH":
        return "TRIỀU MINH MỆNH"
    # Accept common OCR variants MỤC LỰC/MỤC LẠC but canonicalize them /
    # Chấp nhận biến thể OCR MỤC LỰC/MỤC LẠC nhưng chuẩn hóa lại
    if upper.startswith("MỤC L") and "CHÂU BẢN TRIỀU NGUYỄN" in upper:
        return "MỤC LỤC CHÂU BẢN TRIỀU NGUYỄN"
    return None


def _classify_header_leak(lines: list[str]) -> tuple[dict[str, str], bool]:
    """Split leaked header lines into Hán Nôm and Quốc ngữ when unambiguous.

    Tách các dòng lọt vào header sang Hán Nôm và Quốc ngữ khi không mơ hồ.
    """
    han_lines: list[str] = []
    quoc_lines: list[str] = []
    ambiguous = False
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        parts = [part.strip() for part in _WIDE_GAP_RE.split(line) if part.strip()]
        if len(parts) == 1:
            has_han = bool(_HAN_CHAR_RE.search(line))
            has_latin = bool(_LATIN_LETTER_RE.search(line))
            if has_han and has_latin:
                ambiguous = True
            elif has_han:
                han_lines.append(line)
            elif has_latin:
                quoc_lines.append(line)
            else:
                ambiguous = True
            continue
        for part in parts:
            has_han = bool(_HAN_CHAR_RE.search(part))
            has_latin = bool(_LATIN_LETTER_RE.search(part))
            if has_han and not has_latin:
                han_lines.append(part)
            elif has_latin and not has_han:
                quoc_lines.append(part)
            else:
                ambiguous = True
    return {"han_nom": "\n".join(han_lines), "quoc_ngu": "\n".join(quoc_lines)}, ambiguous


def inspect_toc_page_header(value: Any) -> dict[str, Any]:
    """Validate a TOC header and conservatively recover leaked body text.

    Kiểm tra header mục lục và phục hồi bảo thủ nội dung bị lọt vào header.
    """
    raw = str(value or "").strip()
    if not raw:
        return {
            "page_header": None,
            "raw_header": None,
            "leak": {"han_nom": "", "quoc_ngu": ""},
            "action": "none",
            "suspicious": False,
        }
    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    canonical = _canonical_toc_header(lines[0]) if lines else None
    if canonical is None:
        # Preserve unknown headers for review; never move uncertain text /
        # Giữ header lạ để review, tuyệt đối không chuyển nội dung khi chưa chắc
        return {
            "page_header": raw,
            "raw_header": raw,
            "leak": {"han_nom": "", "quoc_ngu": ""},
            "action": "review",
            "suspicious": True,
        }

    extras = lines[1:]
    if not extras:
        return {
            "page_header": canonical,
            "raw_header": raw,
            "leak": {"han_nom": "", "quoc_ngu": ""},
            "action": "normalized" if canonical != raw else "none",
            "suspicious": False,
        }
    # A standalone TRÍCH YẾU is a layout label, not entry content /
    # TRÍCH YẾU đứng riêng là nhãn bố cục, không phải nội dung entry
    content_lines = [line for line in extras if not _TOC_HEADER_LABEL_RE.match(line)]
    if not content_lines:
        return {
            "page_header": canonical,
            "raw_header": raw,
            "leak": {"han_nom": "", "quoc_ngu": ""},
            "action": "label_dropped",
            "suspicious": False,
        }
    leak, ambiguous = _classify_header_leak(content_lines)
    if ambiguous or not (leak["han_nom"] or leak["quoc_ngu"]):
        return {
            "page_header": raw,
            "raw_header": raw,
            "leak": {"han_nom": "", "quoc_ngu": ""},
            "action": "review",
            "suspicious": True,
        }
    return {
        "page_header": canonical,
        "raw_header": raw,
        "leak": leak,
        "action": "recovered_to_orphan",
        # Retry OCR first; parser recovery is the deterministic fallback /
        # Retry OCR trước; phục hồi parser là fallback xác định
        "suspicious": True,
    }


def _prepend_header_leak(orphan: dict[str, str], leak: dict[str, str]) -> dict[str, str]:
    """Prepend recovered header text once to orphan continuation.

    Chèn nội dung phục hồi từ header vào đầu orphan đúng một lần.
    """
    out = dict(orphan)
    for key in ("han_nom", "quoc_ngu"):
        leaked = str(leak.get(key) or "").strip()
        current = str(out.get(key) or "").strip()
        if not leaked:
            out[key] = current
            continue
        compact_leaked = re.sub(r"\s+", "", leaked)
        compact_current = re.sub(r"\s+", "", current)
        if compact_leaked and compact_leaked in compact_current:
            out[key] = current
        else:
            out[key] = f"{leaked}\n{current}" if current else leaked
    return out


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_pair_id(doc_id: str, page_no: int, pair_index: int) -> str:
    # Stable pair id across re-index / pair_id ổn định khi re-index
    raw = f"{doc_id}|{page_no}|{pair_index}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, raw))


def make_entry_id(doc_id: str, stt: int, tap_id: str | None = None) -> str:
    """Stable entry id scoped by volume tap (STT resets per tập).

    entry_id ổn định theo tập — STT reset mỗi tập nên cần tap_id.
    """
    if tap_id:
        return f"{doc_id}__{tap_id}__stt_{stt:04d}"
    # Legacy fallback without tap / Fallback cũ khi chưa có tap
    return f"{doc_id}_stt_{stt:04d}"


def make_entry_pair_id(entry_id: str, pair_index: int) -> str:
    # Pair id scoped to STT entry / pair_id gắn với entry STT
    raw = f"{entry_id}|pair|{pair_index}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, raw))


def make_pair_point_id(pair_id: str) -> str:
    # Qdrant point id derived from pair_id / Point id Qdrant suy ra từ pair_id
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"pair|{pair_id}"))


def _extract_json_object(raw: str) -> dict[str, Any] | None:
    # Parse JSON object from model reply / Parse object JSON từ câu trả lời model
    text = raw.strip()
    if not text:
        return None
    # Strip markdown fences if present / Bỏ fence markdown nếu có
    fence = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.DOTALL)
    if fence:
        text = fence.group(1)
    else:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            text = text[start : end + 1]
    # First try strict JSON / Thử JSON chặt trước
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        pass
    # Repair invalid backslash escapes from LLM (e.g. `\ `) /
    # Sửa escape backslash sai từ LLM (vd: `\ `)
    repaired = re.sub(r'\\(?!["\\/bfnrtu])', "", text)
    try:
        data = json.loads(repaired)
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def extract_json_object(raw: str) -> dict[str, Any] | None:
    # Public alias for LLM response parsing / Alias public để parse phản hồi LLM
    return _extract_json_object(raw)


def _as_optional_int(value: Any) -> int | None:
    # Coerce printed_page-like values / Ép kiểu số trang in
    if value is None or value == "":
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        match = re.search(r"\d+", str(value))
        return int(match.group(0)) if match else None


def _normalize_blocks(blocks: Any, raw_text: str) -> list[dict[str, str]]:
    if not isinstance(blocks, list):
        blocks = []
    normalized_blocks: list[dict[str, str]] = []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        text = str(block.get("text", "")).strip()
        if not text:
            continue
        script = str(block.get("script", "mixed")).strip().lower() or "mixed"
        if script not in {"han_nom", "quoc_ngu", "mixed"}:
            script = "mixed"
        normalized_blocks.append({"script": script, "text": text})
    if not normalized_blocks and raw_text.strip():
        normalized_blocks = [{"script": "mixed", "text": raw_text.strip()}]
    return normalized_blocks


def _normalize_trich_yeu(value: Any) -> dict[str, str]:
    from common.entry_quality import clean_trich_yeu

    return clean_trich_yeu(value)


def _normalize_toc_entry(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    stt_raw = item.get("stt")
    try:
        stt = int(stt_raw)
    except (TypeError, ValueError):
        return None
    if stt <= 0:
        return None
    de_tai = item.get("de_tai")
    de_tai_str = str(de_tai).strip() if de_tai is not None else ""
    from common.entry_quality import normalize_chi_muc

    chi_muc, _ = normalize_chi_muc(
        {
            "ngay_thang": item.get("ngay_thang"),
            "to_tap": item.get("to_tap"),
            "the_loai": item.get("the_loai"),
            "xuat_xu": item.get("xuat_xu"),
            "de_tai": de_tai_str or None,
        }
    )
    return {
        "stt": stt,
        "ngay_thang": chi_muc.get("ngay_thang"),
        "to_tap": chi_muc.get("to_tap"),
        "the_loai": chi_muc.get("the_loai"),
        "xuat_xu": chi_muc.get("xuat_xu"),
        "de_tai": chi_muc.get("de_tai"),
        "trich_yeu": _normalize_trich_yeu(item.get("trich_yeu")),
    }


def parse_ocr_structured_response(raw_text: str) -> dict[str, Any]:
    # Normalize body-page OCR JSON with fallback / Chuẩn hóa JSON OCR trang thân văn kèm fallback
    parsed = _extract_json_object(raw_text)
    if not parsed:
        return {
            "page_header": None,
            "printed_page": None,
            "ngay_thang": None,
            "the_loai": None,
            "de_tai": None,
            "blocks": [{"script": "mixed", "text": raw_text.strip()}],
            "parse_ok": False,
            "page_type": "body",
        }
    return {
        "page_header": parsed.get("page_header"),
        "printed_page": _as_optional_int(parsed.get("printed_page")),
        "ngay_thang": parsed.get("ngay_thang"),
        "the_loai": parsed.get("the_loai"),
        "de_tai": parsed.get("de_tai"),
        "blocks": _normalize_blocks(parsed.get("blocks"), raw_text),
        "parse_ok": True,
        "page_type": "body",
    }


def _normalize_entry_continuation(value: Any) -> dict[str, Any] | None:
    # Headless metadata+TY continuation of previous STT /
    # Phần metadata+TY không số STT — tiếp nối STT trang trước
    if not isinstance(value, dict):
        return None
    trich = _normalize_trich_yeu(value.get("trich_yeu"))
    from common.entry_quality import normalize_chi_muc

    chi_muc, _ = normalize_chi_muc(
        {
            "ngay_thang": value.get("ngay_thang"),
            "to_tap": value.get("to_tap"),
            "the_loai": value.get("the_loai"),
            "xuat_xu": value.get("xuat_xu"),
            "de_tai": (str(value.get("de_tai")).strip() or None)
            if value.get("de_tai") is not None
            else None,
        }
    )
    has_meta = any(chi_muc.get(k) for k in ("ngay_thang", "to_tap", "the_loai", "xuat_xu", "de_tai"))
    has_trich = bool(trich["han_nom"] or trich["quoc_ngu"])
    if not has_meta and not has_trich:
        return None
    return {"chi_muc": chi_muc, "trich_yeu": trich}


def parse_ocr_toc_response(raw_text: str) -> dict[str, Any]:
    # Normalize TOC OCR JSON with multiple STT entries / Chuẩn hóa JSON OCR mục lục nhiều entry STT
    parsed = _extract_json_object(raw_text)
    if not parsed:
        return {
            "page_type": "muc_luc",
            "page_header": None,
            "page_header_raw": None,
            "header_leak_action": "none",
            "header_content_leak": False,
            "header_leak_review": None,
            "printed_page": None,
            "orphan_head": {"han_nom": "", "quoc_ngu": ""},
            "entry_continuation": None,
            "entries": [],
            # JSON extraction itself failed / Bản thân việc parse JSON đã thất bại
            "parse_ok": False,
            "has_content": False,
        }
    entries_raw = parsed.get("entries") or []
    entries: list[dict[str, Any]] = []
    if isinstance(entries_raw, list):
        for item in entries_raw:
            normalized = _normalize_toc_entry(item)
            if normalized:
                entries.append(normalized)
    # Stable order by STT / Sắp ổn định theo STT
    entries.sort(key=lambda row: int(row["stt"]))
    orphan = _normalize_trich_yeu(parsed.get("orphan_head"))
    header_check = inspect_toc_page_header(parsed.get("page_header"))
    if header_check["action"] == "recovered_to_orphan":
        # Header leakage appears before numbered entries, so recover it as
        # orphan continuation from the previous page / Nội dung lọt vào header
        # nằm trước entry đánh số, nên phục hồi thành orphan của trang trước
        orphan = _prepend_header_leak(orphan, header_check["leak"])
    continuation = _normalize_entry_continuation(parsed.get("entry_continuation"))
    has_content = (
        bool(entries)
        or bool(orphan["han_nom"] or orphan["quoc_ngu"])
        or continuation is not None
    )
    return {
        "page_type": "muc_luc",
        "page_header": header_check["page_header"],
        "page_header_raw": header_check["raw_header"],
        "header_leak_action": header_check["action"],
        "header_content_leak": bool(header_check["suspicious"]),
        "header_leak_review": (
            header_check["raw_header"] if header_check["action"] == "review" else None
        ),
        "printed_page": _as_optional_int(parsed.get("printed_page")),
        "orphan_head": orphan,
        "entry_continuation": continuation,
        "entries": entries,
        # JSON extraction succeeded regardless of content (e.g. legit blank /
        # divider page); has_content flags whether any entries/orphan/continuation
        # were found on this page.
        # Parse JSON thành công dù trang có nội dung hay không (VD trang trắng
        # divider hợp lệ); has_content đánh dấu có tìm thấy entries/orphan/
        # continuation trên trang hay không.
        "parse_ok": True,
        "has_content": has_content,
    }


def parse_align_response(raw_text: str) -> list[dict[str, str]]:
    # Parse content_alignment array from align model / Parse mảng content_alignment từ model align
    parsed = _extract_json_object(raw_text)
    if not parsed:
        return []
    items = parsed.get("content_alignment")
    if not isinstance(items, list):
        return []
    pairs: list[dict[str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        han_nom = str(item.get("han_nom", "")).strip()
        quoc_ngu = str(item.get("quoc_ngu", "")).strip()
        if not han_nom and not quoc_ngu:
            continue
        pairs.append({"han_nom": han_nom, "quoc_ngu": quoc_ngu})
    return pairs
