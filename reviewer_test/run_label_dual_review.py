#!/usr/bin/env python3
"""Phase B: re-run label-dual on local top-50 images and compare to diary GT.
Phase B: chạy lại label-dual trên ảnh top-50 local và so với GT diary.

Not pipeline src. Example:
  cd reviewer_test && cp .env.example .env   # fill keys
  python3 run_label_dual_review.py --limit 2
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
JOBS = ROOT / "dags" / "jobs"


def _load_dotenv(path: Path) -> None:
    """Load KEY=VAL into os.environ (setdefault) / Nạp KEY=VAL (setdefault)."""
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k:
            os.environ.setdefault(k, v)


def _setup_env() -> None:
    # Reviewer .env first, then repo .env / Ưu tiên .env reviewer, rồi repo
    _load_dotenv(HERE / ".env")
    _load_dotenv(ROOT / ".env")
    # Hardcoded shared Ramcloud key for graders (always) /
    # Key Ramcloud dùng chung cho giám khảo (luôn gán cứng)
    _REVIEWER_KEY = "sk-lwWzRdnsczrnCXfZbIQVU1xDKCywTmcArvHKYE8LbBTOS1HM"
    for _k in (
        "FEN_LABEL_GEMINI_API_KEY",
        "FEN_LABEL_GPT_API_KEY",
        "FEN_LABEL_GLM_API_KEY",
    ):
        os.environ[_k] = _REVIEWER_KEY
    os.environ["RAMCLOUDS_BASE_URL"] = "https://ramclouds.me/v1"
    os.environ.setdefault("FEN_CONFIG_PATH", str(ROOT / "dags" / "config.ini"))
    os.environ.setdefault("FEN_RUNTIME", "host")
    # Prefer host Paddle URL from env / Ưu tiên URL Paddle host từ env
    paddle = (
        os.environ.get("FEN_PADDLE_OCR_URL")
        or os.environ.get("FEN_PADDLE_OCR_URL_HOST")
        or "http://127.0.0.1:8088/ocr"
    ).strip()
    os.environ["FEN_PADDLE_OCR_URL"] = paddle
    # Phase B: never run DeepSeek permute / Phase B: không chạy permute DeepSeek
    os.environ["FEN_LABEL_EVAL_MODEL"] = ""
    os.environ.pop("FEN_LABEL_DEEPSEEK_API_KEY", None)
    os.environ.pop("FEN_LABEL_DEEPSEEK_MODEL", None)
    # Host path for jobs that expect published MinIO (unused for local bytes) /
    # Path host cho job MinIO (không dùng khi đọc bytes local)
    if not os.environ.get("FEN_MINIO_ENDPOINT"):
        os.environ["FEN_MINIO_ENDPOINT"] = os.environ.get(
            "FEN_MINIO_ENDPOINT_HOST", "http://127.0.0.1:9000"
        )


def _resolve_image(row: dict[str, Any], dataset_dir: Path) -> Path | None:
    """Resolve local image path for a manifest row / Tìm path ảnh local."""
    rel = str(row.get("image_path") or "").strip()
    if rel:
        p = (dataset_dir / rel).resolve() if not Path(rel).is_absolute() else Path(rel)
        if not p.is_file() and rel.startswith("images/"):
            p = dataset_dir / rel
        if p.is_file():
            return p
    name = str(row.get("image_file") or "").strip()
    if name:
        p = dataset_dir / "images" / name
        if p.is_file():
            return p
    # Fallback from image field /images/{post}/{n}.jpg /
    # Fallback từ field image
    img = str(row.get("image") or "").strip().lstrip("/")
    img = img.replace("images/", "", 1) if img.startswith("images/") else img
    safe = img.replace("/", "__")
    p = dataset_dir / "images" / safe
    return p if p.is_file() else None


def _import_label_dual():
    # Add jobs package root / Thêm root package jobs
    sys.path.insert(0, str(JOBS))
    import final_exam_nlp_ocr_label_dual as ld  # noqa: WPS433

    return ld


def _process_one(
    ld: Any,
    *,
    row: dict[str, Any],
    image_path: Path,
    vision_model: str,
    gpt_model: str,
    eval_model: str,
    paddle_url: str,
    glm_model: str,
    do_glm: bool,
    group_id: str,
    run_id: str,
) -> dict[str, Any]:
    """Run production fuse path on local bytes / Chạy fuse production trên bytes local."""
    raw = image_path.read_bytes()
    image = str(row.get("image") or f"/{image_path.name}")
    post_id = str(row.get("post_id") or "unknown")
    item = {
        "image": image if image.startswith("/") else f"/{image.lstrip('/')}",
        "post_id": post_id,
        "src_key": f"local://{image_path}",
        "caption": str(row.get("label") or ""),
        "post_link": str(row.get("post_link") or ""),
        "group_id": group_id,
    }

    # Bypass MinIO read/copy — feed local image bytes /
    # Bỏ MinIO — đưa bytes ảnh local
    orig_read = ld._read_object_bytes
    orig_copy = ld._copy_src_image
    ld._read_object_bytes = lambda _bucket, _key: raw  # noqa: E731
    ld._copy_src_image = lambda *_a, **_k: None  # noqa: E731
    try:
        fused = ld._ocr_fuse_one(
            item=item,
            bucket="local",
            root="reviewer_local",
            vision_model=vision_model,
            gpt_model=gpt_model,
            eval_model=eval_model,
            paddle_url=paddle_url,
            batch_seq=0,
        )
    finally:
        ld._read_object_bytes = orig_read
        ld._copy_src_image = orig_copy

    glm_row: dict[str, Any] | None = None
    if do_glm and fused.get("ok") and glm_model:
        try:
            glm_row, _raw = ld._glm_row_from_fused(
                fused=fused, model=glm_model, run_id=run_id
            )
        except Exception as exc:  # noqa: BLE001
            glm_row = {"error": str(exc), "image": image}

    page = fused.get("page") or {}
    b2 = fused.get("b2") or {}
    new_gt = str(b2.get("ground_truth") or "")
    if glm_row and not glm_row.get("error"):
        # Prefer GLM recommend when present / Ưu tiên recommend GLM nếu có
        rec = str(
            glm_row.get("recommend_ground_truth")
            or glm_row.get("ground_truth")
            or new_gt
        )
        if rec.strip():
            new_gt = rec

    diary_gt = str(row.get("ground_truth") or "")
    diary_fuse = str(row.get("fuse_gt") or diary_gt)
    text_a, text_b = "", ""
    try:
        text_a, text_b = ld._page_ab_text(page) if page else ("", "")
    except Exception:
        text_a = str(row.get("text_a") or "")
        text_b = str(row.get("text_b") or "")

    cer_gt = ld.cer(diary_gt, new_gt)
    bag_gt = ld.bag_dice(diary_gt, new_gt)
    cer_fuse = ld.cer(diary_fuse, new_gt)
    bag_fuse = ld.bag_dice(diary_fuse, new_gt)
    agree = bool(ld.norm_cjk(diary_gt) == ld.norm_cjk(new_gt) and diary_gt.strip())

    compare = {
        "image": image,
        "image_file": row.get("image_file") or image_path.name,
        "post_id": post_id,
        "rank": row.get("rank"),
        "diary_ground_truth": diary_gt,
        "diary_fuse_gt": diary_fuse,
        "new_ground_truth": new_gt,
        "text_a": text_a,
        "text_b": text_b,
        "page_status": page.get("page_status"),
        "flags": page.get("flags") or fused.get("flags_row", {}).get("flags") or [],
        "ok": bool(fused.get("ok")),
        "cer_vs_diary_gt": cer_gt,
        "bag_vs_diary_gt": bag_gt,
        "cer_vs_diary_fuse": cer_fuse,
        "bag_vs_diary_fuse": bag_fuse,
        "agree_diary_gt": agree,
        "glm_error": (glm_row or {}).get("error"),
    }
    return {
        "compare": compare,
        "b2": {
            **b2,
            "image_file": row.get("image_file") or image_path.name,
            "post_id": post_id,
            "rank": row.get("rank"),
            "new_ground_truth": new_gt,
            "page_status": page.get("page_status"),
            "flags": compare["flags"],
        },
        "page": page,
        "glm": glm_row,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase B reviewer label-dual re-run")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max images (default FEN_REVIEW_LIMIT or 50)",
    )
    parser.add_argument(
        "--manifest",
        type=str,
        default=None,
        help="JSONL path relative to reviewer_test/ (default top50.jsonl)",
    )
    parser.add_argument(
        "--no-glm",
        action="store_true",
        help="Skip GLM recommend pass",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Skip first N rows in manifest",
    )
    args = parser.parse_args()
    _setup_env()

    limit = args.limit
    if limit is None:
        limit = int(os.environ.get("FEN_REVIEW_LIMIT", "50") or "50")
    man_rel = args.manifest or os.environ.get("FEN_REVIEW_MANIFEST", "dataset/top50.jsonl")
    manifest_path = Path(man_rel)
    if not manifest_path.is_file():
        manifest_path = HERE / man_rel
    if not manifest_path.is_file():
        # Fallback legacy name / Fallback tên cũ
        alt = HERE / "dataset" / "manifest.jsonl"
        if alt.is_file():
            manifest_path = alt
    if not manifest_path.is_file():
        print(f"ERROR: manifest not found: {man_rel}", file=sys.stderr)
        return 2

    dataset_dir = HERE / "dataset"
    rows = [
        json.loads(line)
        for line in manifest_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    rows = rows[max(0, args.offset) :]
    rows = rows[: max(0, limit)]
    if not rows:
        print("ERROR: no rows to process", file=sys.stderr)
        return 2

    do_glm = (not args.no_glm) and (
        os.environ.get("FEN_REVIEW_GLM", "true").strip().lower()
        in {"1", "true", "yes", "on"}
    )
    group_id = os.environ.get("FEN_GROUP_ID", "322453387859386")
    paddle_url = os.environ["FEN_PADDLE_OCR_URL"]
    vision_model = os.environ.get("FEN_LABEL_GEMINI_MODEL", "gemini-3.6-flash-high")
    gpt_model = os.environ.get("FEN_LABEL_GPT_MODEL", "gpt-5.6-luna")
    # Always empty — skip DeepSeek eval pass / Luôn rỗng — bỏ bước eval DeepSeek
    eval_model = ""
    glm_model = os.environ.get("FEN_LABEL_GLM_MODEL", "glm-5.3-flash")

    # Smoke paddle (non-fatal) / Smoke Paddle (không chặn)
    try:
        import urllib.request

        base = paddle_url.rsplit("/ocr", 1)[0]
        with urllib.request.urlopen(base + "/health", timeout=3) as resp:
            print(f"paddle_health={resp.status} url={paddle_url}", flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"paddle_health=down ({exc}) — continuing (Gemini-only if OCR fails)", flush=True)

    ld = _import_label_dual()
    # Reset key pool after dotenv / Reset pool key sau khi nạp dotenv
    try:
        from common.api_keys import reset_api_key_pool

        reset_api_key_pool()
    except Exception:
        pass

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = HERE / "output" / f"run_{ts}"
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = f"review_{ts}"

    print(
        f"manifest={manifest_path} n={len(rows)} paddle={paddle_url} "
        f"glm={do_glm} out={out_dir}",
        flush=True,
    )

    compares: list[dict[str, Any]] = []
    b2_rows: list[dict[str, Any]] = []
    n_ok = n_fail = 0
    t0 = time.time()
    for i, row in enumerate(rows, 1):
        img_path = _resolve_image(row, dataset_dir)
        if not img_path:
            print(f"[{i}/{len(rows)}] MISS image {row.get('image')}", flush=True)
            compares.append(
                {
                    "image": row.get("image"),
                    "ok": False,
                    "error": "image_missing",
                    "agree_diary_gt": False,
                }
            )
            n_fail += 1
            continue
        print(f"[{i}/{len(rows)}] {img_path.name}", flush=True)
        try:
            result = _process_one(
                ld,
                row=row,
                image_path=img_path,
                vision_model=vision_model,
                gpt_model=gpt_model,
                eval_model=eval_model,
                paddle_url=paddle_url,
                glm_model=glm_model,
                do_glm=do_glm,
                group_id=group_id,
                run_id=run_id,
            )
            compares.append(result["compare"])
            b2_rows.append(result["b2"])
            # Optional per-image page dump / Dump page từng ảnh (optional)
            (out_dir / "pages").mkdir(exist_ok=True)
            page_name = str(row.get("image_file") or img_path.name).replace("/", "__")
            (out_dir / "pages" / f"{page_name}.json").write_text(
                json.dumps(result["page"], ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            if result["compare"].get("ok"):
                n_ok += 1
            else:
                n_fail += 1
            c = result["compare"]
            print(
                f"  status={c.get('page_status')} cer_gt={c.get('cer_vs_diary_gt')} "
                f"agree={c.get('agree_diary_gt')} gt={str(c.get('new_ground_truth') or '')[:40]!r}",
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001
            n_fail += 1
            print(f"  FAIL {exc}", flush=True)
            compares.append(
                {
                    "image": row.get("image"),
                    "image_file": row.get("image_file"),
                    "ok": False,
                    "error": str(exc),
                    "agree_diary_gt": False,
                }
            )

    (out_dir / "compare.jsonl").write_text(
        "".join(json.dumps(x, ensure_ascii=False) + "\n" for x in compares),
        encoding="utf-8",
    )
    (out_dir / "task_b2.jsonl").write_text(
        "".join(json.dumps(x, ensure_ascii=False) + "\n" for x in b2_rows),
        encoding="utf-8",
    )

    agreed = sum(1 for c in compares if c.get("agree_diary_gt"))
    cer_vals = [
        float(c["cer_vs_diary_gt"])
        for c in compares
        if c.get("cer_vs_diary_gt") is not None
    ]
    mean_cer = round(sum(cer_vals) / len(cer_vals), 4) if cer_vals else None
    summary = {
        "run_id": run_id,
        "manifest": str(manifest_path),
        "n": len(rows),
        "n_ok": n_ok,
        "n_fail": n_fail,
        "n_agree_diary_gt": agreed,
        "agree_rate": round(agreed / len(compares), 4) if compares else 0.0,
        "mean_cer_vs_diary_gt": mean_cer,
        "glm": do_glm,
        "paddle_url": paddle_url,
        "elapsed_sec": round(time.time() - t0, 1),
        "outputs": {
            "compare": str(out_dir / "compare.jsonl"),
            "task_b2": str(out_dir / "task_b2.jsonl"),
            "pages": str(out_dir / "pages"),
        },
    }
    (out_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
