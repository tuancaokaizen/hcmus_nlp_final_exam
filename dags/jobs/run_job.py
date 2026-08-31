#!/usr/bin/env python3
"""CLI entrypoint for FEN exam jobs (Docker Compose / DockerOperator).

Điểm vào CLI cho job FEN exam chạy qua Docker Compose / DockerOperator.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path


def _optional_pages(value: str | None) -> str | None:
    # Normalize optional pages filter / Chuẩn hóa filter trang; rỗng = tất cả
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_bool(value: str | None, default: bool = False) -> bool:
    # Parse common truthy strings from env / Parse chuỗi boolean từ biến môi trường
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _ensure_jobs_on_path() -> None:
    # Ensure jobs package imports resolve in pod / Đảm bảo import jobs trong pod
    jobs_dir = Path(__file__).resolve().parent
    if str(jobs_dir) not in sys.path:
        sys.path.insert(0, str(jobs_dir))


def _run_opencv_preprocess_pages() -> None:
    from preprocess_opencv_runner import preprocess_page_loop_from_split_minio

    doc_id = os.environ.get("FEN_DOC_ID", "fen_base").strip() or "fen_base"
    pages = _optional_pages(os.environ.get("FEN_PAGES"))
    upload_minio = _as_bool(os.environ.get("FEN_UPLOAD_MINIO"), default=True)
    preprocess_page_loop_from_split_minio(
        doc_id=doc_id,
        pages=pages,
        upload_minio=upload_minio,
    )


def _run_ocr_v2_pages() -> None:
    from ocr_v2_runner import ocr_page_loop_v2

    doc_id = os.environ.get("FEN_DOC_ID", "fen_base").strip() or "fen_base"
    pages = _optional_pages(os.environ.get("FEN_PAGES"))
    upload_minio = _as_bool(os.environ.get("FEN_UPLOAD_MINIO"), default=True)
    page_kind = os.environ.get("FEN_PAGE_KIND", "toc").strip() or "toc"
    ocr_page_loop_v2(
        doc_id=doc_id,
        pages=pages,
        upload_minio=upload_minio,
        page_kind=page_kind,
    )


def _run_align_v2_pages() -> None:
    from align_v2_runner import align_page_loop_v2

    doc_id = os.environ.get("FEN_DOC_ID", "fen_base").strip() or "fen_base"
    pages = _optional_pages(os.environ.get("FEN_PAGES"))
    upload_minio = _as_bool(os.environ.get("FEN_UPLOAD_MINIO"), default=True)
    align_page_loop_v2(doc_id=doc_id, pages=pages, upload_minio=upload_minio)


def _run_build_catalog() -> None:
    from build_catalog_runner import build_catalog_from_ocr

    doc_id = os.environ.get("FEN_DOC_ID", "fen_base").strip() or "fen_base"
    pages = _optional_pages(os.environ.get("FEN_PAGES"))
    upload_minio = _as_bool(os.environ.get("FEN_UPLOAD_MINIO"), default=True)
    build_catalog_from_ocr(doc_id=doc_id, pages=pages, upload_minio=upload_minio)


def _run_refine_entries() -> None:
    from refine_entries_runner import refine_catalog_entries

    doc_id = os.environ.get("FEN_DOC_ID", "fen_base").strip() or "fen_base"
    pages = _optional_pages(os.environ.get("FEN_PAGES"))
    force_all = _as_bool(os.environ.get("FEN_FORCE"), default=False)
    upload_minio = _as_bool(os.environ.get("FEN_UPLOAD_MINIO"), default=True)
    refine_catalog_entries(
        doc_id=doc_id,
        pages=pages,
        force_all=force_all,
        upload_minio=upload_minio,
    )


def _run_stitch_entries() -> None:
    from stitch_entries_runner import stitch_incomplete_entries

    doc_id = os.environ.get("FEN_DOC_ID", "fen_base").strip() or "fen_base"
    pages = _optional_pages(os.environ.get("FEN_PAGES"))
    upload_minio = _as_bool(os.environ.get("FEN_UPLOAD_MINIO"), default=True)
    stitch_incomplete_entries(doc_id=doc_id, pages=pages, upload_minio=upload_minio)


def _run_index_pairs_qdrant() -> None:
    from index_pairs_qdrant import index_aligned_pairs_from_minio

    doc_id = os.environ.get("FEN_DOC_ID", "fen_base").strip() or "fen_base"
    pages = _optional_pages(os.environ.get("FEN_PAGES"))
    index_aligned_pairs_from_minio(doc_id=doc_id, pages=pages)


def _run_index_catalog_qdrant() -> None:
    from index_catalog_qdrant import index_catalog_entries_to_qdrant

    doc_id = os.environ.get("FEN_DOC_ID", "fen_base").strip() or "fen_base"
    pages = _optional_pages(os.environ.get("FEN_PAGES"))
    recreate = _as_bool(os.environ.get("FEN_QDRANT_RECREATE"), default=False)
    index_catalog_entries_to_qdrant(doc_id=doc_id, pages=pages, recreate=recreate)


def _run_cleanup_toc_state() -> None:
    from cleanup_toc_state import cleanup_toc_migration_state

    doc_id = os.environ.get("FEN_DOC_ID", "fen_base").strip() or "fen_base"
    pages = _optional_pages(os.environ.get("FEN_PAGES")) or "49-58"
    recreate = _as_bool(os.environ.get("FEN_QDRANT_RECREATE"), default=True)
    cleanup_toc_migration_state(doc_id=doc_id, pages=pages, recreate_qdrant=recreate)


def _run_export_entries_excel() -> None:
    from export_entries_excel_runner import export_entries_to_excel

    doc_id = os.environ.get("FEN_DOC_ID", "fen_base").strip() or "fen_base"
    tap_ids = os.environ.get("FEN_TAP_IDS", "").strip() or None
    bucket = os.environ.get("FEN_EXCEL_BUCKET", "").strip() or None
    object_key = os.environ.get("FEN_EXCEL_OBJECT_KEY", "").strip() or None
    result = export_entries_to_excel(
        doc_id=doc_id,
        tap_ids=tap_ids,
        bucket=bucket,
        object_key=object_key,
    )
    print(f"[export_entries_excel] done {result}")


def _run_export_parallel_corpus() -> None:
    from export_parallel_corpus_runner import export_parallel_corpus

    doc_id = os.environ.get("FEN_DOC_ID", "fen_base").strip() or "fen_base"
    tap_ids = os.environ.get("FEN_TAP_IDS", "").strip() or None
    bucket = os.environ.get("FEN_PARALLEL_BUCKET", "").strip() or None
    include_raw = _as_bool(os.environ.get("FEN_PARALLEL_INCLUDE_RAW"), default=True)
    raw_object_key = os.environ.get("FEN_PARALLEL_RAW_KEY", "").strip() or None
    raw_plain_object_key = os.environ.get("FEN_PARALLEL_RAW_PLAIN_KEY", "").strip() or None
    tsv_object_key = os.environ.get("FEN_PARALLEL_TSV_KEY", "").strip() or None
    xlsx_object_key = os.environ.get("FEN_PARALLEL_XLSX_KEY", "").strip() or None
    result = export_parallel_corpus(
        doc_id=doc_id,
        tap_ids=tap_ids,
        bucket=bucket,
        include_raw=include_raw,
        raw_object_key=raw_object_key,
        raw_plain_object_key=raw_plain_object_key,
        tsv_object_key=tsv_object_key,
        xlsx_object_key=xlsx_object_key,
    )
    print(f"[export_parallel_corpus] done {result}")


def _run_repair_entries() -> None:
    from repair_entries_runner import repair_catalog_entries

    doc_id = os.environ.get("FEN_DOC_ID", "fen_base").strip() or "fen_base"
    pages = _optional_pages(os.environ.get("FEN_PAGES"))
    upload_minio = _as_bool(os.environ.get("FEN_UPLOAD_MINIO"), default=True)
    dry_run = _as_bool(os.environ.get("FEN_DRY_RUN"), default=False)
    repair_catalog_entries(
        doc_id=doc_id,
        pages=pages,
        upload_minio=upload_minio,
        dry_run=dry_run,
    )


def _run_refresh_catalog() -> None:
    from refresh_catalog_runner import refresh_catalog_from_entries

    doc_id = os.environ.get("FEN_DOC_ID", "fen_base").strip() or "fen_base"
    upload_minio = _as_bool(os.environ.get("FEN_UPLOAD_MINIO"), default=True)
    refresh_catalog_from_entries(doc_id=doc_id, upload_minio=upload_minio)


def _run_compare_ocr_corpus() -> None:
    from compare_ocr_corpus import run_from_minio

    bucket = os.environ.get("FEN_COMPARE_BUCKET", "").strip() or None
    ours_key = os.environ.get("FEN_COMPARE_OURS_KEY", "").strip() or None
    ref_key = os.environ.get("FEN_COMPARE_REF_KEY", "").strip() or None
    ref_sheet = os.environ.get("FEN_COMPARE_REF_SHEET", "").strip() or None
    output_prefix = os.environ.get("FEN_COMPARE_OUTPUT_PREFIX", "").strip() or None
    model = os.environ.get("FEN_COMPARE_MODEL", "").strip() or None
    limit_raw = os.environ.get("FEN_COMPARE_LIMIT", "").strip()
    limit = int(limit_raw) if limit_raw else None
    workers = int(os.environ.get("FEN_COMPARE_WORKERS", "4").strip() or "4")
    dry_run = _as_bool(os.environ.get("FEN_COMPARE_DRY_RUN"), default=False)
    result = run_from_minio(
        bucket=bucket,
        ours_key=ours_key,
        ref_key=ref_key,
        ref_sheet=ref_sheet,
        output_prefix=output_prefix,
        limit=limit,
        workers=workers,
        model_override=model,
        dry_run=dry_run,
    )
    print(f"[compare_ocr_corpus] done {result}")


def _run_final_exam_nlp_crawl() -> None:
    from final_exam_nlp_crawl_runner import crawl_group_batches

    group_id = os.environ.get("FEN_GROUP_ID", "").strip()
    batch_size = int(os.environ.get("FEN_BATCH_SIZE", "25").strip() or "25")
    max_batches = int(os.environ.get("FEN_MAX_BATCHES", "20").strip() or "20")
    # Prefer per-run delta; fall back to legacy FEN_TARGET_VALID_COUNT as delta /
    # Ưu tiên delta mỗi run; fallback env cũ thành delta
    target_valid_delta = int(
        (
            os.environ.get("FEN_TARGET_VALID_DELTA")
            or os.environ.get("FEN_TARGET_VALID_COUNT")
            or "100"
        ).strip()
        or "100"
    )
    max_image_valid_count = int(
        os.environ.get("FEN_MAX_IMAGE_VALID_COUNT", "20000").strip() or "20000"
    )
    bottom_year = int(os.environ.get("FEN_BOTTOM_YEAR", "2013").strip() or "2013")
    start_phase = (os.environ.get("FEN_START_PHASE", "media").strip() or "media").lower()
    scroll_pause_sec = float(os.environ.get("FEN_SCROLL_PAUSE_SEC", "3").strip() or "3")
    cooldown_sec = float(os.environ.get("FEN_COOLDOWN_SEC", "35").strip() or "35")
    stall_limit = int(os.environ.get("FEN_STALL_LIMIT", "12").strip() or "12")
    headless = _as_bool(os.environ.get("FEN_HEADLESS"), default=True)
    upload_minio = _as_bool(os.environ.get("FEN_UPLOAD_MINIO"), default=True)
    force = _as_bool(os.environ.get("FEN_FORCE"), default=False)
    download_images = _as_bool(os.environ.get("FEN_DOWNLOAD_IMAGES"), default=True)
    merge_dataset = _as_bool(os.environ.get("FEN_MERGE_DATASET"), default=True)
    debug_dom = _as_bool(os.environ.get("FEN_DEBUG_DOM"), default=False)
    run_id = os.environ.get("FEN_RUN_ID", "").strip() or None
    result = crawl_group_batches(
        group_id=group_id,
        batch_size=batch_size,
        max_batches=max_batches,
        target_valid_delta=target_valid_delta,
        max_image_valid_count=max_image_valid_count,
        bottom_year=bottom_year,
        start_phase=start_phase,
        scroll_pause_sec=scroll_pause_sec,
        cooldown_sec=cooldown_sec,
        stall_limit=stall_limit,
        headless=headless,
        upload_minio=upload_minio,
        force=force,
        download_images=download_images,
        merge_dataset=merge_dataset,
        debug_dom=debug_dom,
        # Credentials come from env/.env, never from DAG params / Credential lấy từ env/.env, không đặt trong DAG
        fb_username=os.environ.get("FB_USERNAME", "").strip(),
        fb_password=os.environ.get("FB_PASSWORD", "").strip(),
        fb_totp_secret=os.environ.get("FB_TOTP_SECRET", "").strip(),
        run_id=run_id,
    )
    print(
        f"[final_exam_nlp_crawl] done reason={result.get('stop_reason')} "
        f"should_continue={result.get('should_continue')} phase={result.get('phase')} "
        f"{result['batch_count']} batches, "
        f"run_valid={result['valid_count']}/{result.get('target_valid_delta')} "
        f"total_valid={result.get('total_valid_after_run')} "
        f"valid_with_images={result.get('valid_with_images_count')}/"
        f"{result.get('max_image_valid_count')} "
        f"invalid={result['invalid_count']} images={result['image_count']}"
    )


def _run_final_exam_nlp_media_batch() -> None:
    from final_exam_nlp_two_phase import run_media_image_batch

    group_id = os.environ.get("FEN_GROUP_ID", "").strip()
    # Light default 50 tiles/run / Mặc định nhẹ 50 tile/run
    media_batch_size = int(os.environ.get("FEN_MEDIA_BATCH_SIZE", "50").strip() or "50")
    max_image_valid_count = int(
        os.environ.get("FEN_MAX_IMAGE_VALID_COUNT", "20000").strip() or "20000"
    )
    bottom_year = int(os.environ.get("FEN_BOTTOM_YEAR", "2013").strip() or "2013")
    scroll_pause_sec = float(os.environ.get("FEN_SCROLL_PAUSE_SEC", "4.5").strip() or "4.5")
    stall_limit = int(os.environ.get("FEN_STALL_LIMIT", "10").strip() or "10")
    headless = _as_bool(os.environ.get("FEN_HEADLESS"), default=True)
    upload_minio = _as_bool(os.environ.get("FEN_UPLOAD_MINIO"), default=True)
    force = _as_bool(os.environ.get("FEN_FORCE"), default=False)
    download_images = _as_bool(os.environ.get("FEN_DOWNLOAD_IMAGES"), default=False)
    run_id = os.environ.get("FEN_RUN_ID", "").strip() or None
    result = run_media_image_batch(
        group_id=group_id,
        media_batch_size=media_batch_size,
        max_image_valid_count=max_image_valid_count,
        bottom_year=bottom_year,
        scroll_pause_sec=scroll_pause_sec,
        stall_limit=stall_limit,
        headless=headless,
        upload_minio=upload_minio,
        download_images=download_images,
        force=force,
        fb_username=os.environ.get("FB_USERNAME", "").strip(),
        fb_password=os.environ.get("FB_PASSWORD", "").strip(),
        fb_totp_secret=os.environ.get("FB_TOTP_SECRET", "").strip(),
        run_id=run_id,
    )
    print(f"[final_exam_nlp_media_batch] done {result}")


def _run_final_exam_nlp_caption_match() -> None:
    from final_exam_nlp_two_phase import run_caption_permalink_match

    group_id = os.environ.get("FEN_GROUP_ID", "").strip()
    batch_id = os.environ.get("FEN_BATCH_ID", "").strip() or None
    target_valid_delta = int(
        (
            os.environ.get("FEN_TARGET_VALID_DELTA")
            or os.environ.get("FEN_TARGET_VALID_COUNT")
            or "30"
        ).strip()
        or "30"
    )
    max_image_valid_count = int(
        os.environ.get("FEN_MAX_IMAGE_VALID_COUNT", "20000").strip() or "20000"
    )
    bottom_year = int(os.environ.get("FEN_BOTTOM_YEAR", "2013").strip() or "2013")
    permalink_pause_sec = float(
        os.environ.get("FEN_PERMALINK_PAUSE_SEC", "3.5").strip() or "3.5"
    )
    headless = _as_bool(os.environ.get("FEN_HEADLESS"), default=True)
    upload_minio = _as_bool(os.environ.get("FEN_UPLOAD_MINIO"), default=True)
    run_id = os.environ.get("FEN_RUN_ID", "").strip() or None
    result = run_caption_permalink_match(
        group_id=group_id,
        batch_id=batch_id,
        target_valid_delta=target_valid_delta,
        max_image_valid_count=max_image_valid_count,
        bottom_year=bottom_year,
        headless=headless,
        upload_minio=upload_minio,
        fb_username=os.environ.get("FB_USERNAME", "").strip(),
        fb_password=os.environ.get("FB_PASSWORD", "").strip(),
        fb_totp_secret=os.environ.get("FB_TOTP_SECRET", "").strip(),
        run_id=run_id,
        permalink_pause_sec=permalink_pause_sec,
    )
    print(f"[final_exam_nlp_caption_match] done {result}")


def _run_final_exam_nlp_graphql_batch() -> None:
    from final_exam_nlp_crawl_runner import DEFAULT_BOTTOM_YEAR
    from final_exam_nlp_graphql_batch import (
        DEFAULT_BATCH_TARGET,
        DEFAULT_DISCOVER_CHUNK,
        DEFAULT_SKIP_STREAK,
        run_graphql_enrich_batch,
    )

    group_id = os.environ.get("FEN_GROUP_ID", "").strip()
    batch_target = int(
        os.environ.get("FEN_BATCH_TARGET", os.environ.get("FEN_GALLERY_WALK_COUNT", str(DEFAULT_BATCH_TARGET))).strip()
        or str(DEFAULT_BATCH_TARGET)
    )
    discover_chunk = int(
        os.environ.get("FEN_DISCOVER_CHUNK", str(DEFAULT_DISCOVER_CHUNK)).strip()
        or str(DEFAULT_DISCOVER_CHUNK)
    )
    permalink_pause_sec = float(
        os.environ.get("FEN_PERMALINK_PAUSE_SEC", "3.5").strip() or "3.5"
    )
    soft_restart_every = int(os.environ.get("FEN_SOFT_RESTART_EVERY", "10").strip() or "10")
    bottom_year = int(os.environ.get("FEN_BOTTOM_YEAR", str(DEFAULT_BOTTOM_YEAR)).strip() or str(DEFAULT_BOTTOM_YEAR))
    skip_streak = int(
        os.environ.get("FEN_SKIP_STREAK", str(DEFAULT_SKIP_STREAK)).strip() or str(DEFAULT_SKIP_STREAK)
    )
    download_images = _as_bool(os.environ.get("FEN_DOWNLOAD_IMAGES"), default=True)
    headless = _as_bool(os.environ.get("FEN_HEADLESS"), default=False)
    upload_minio = _as_bool(os.environ.get("FEN_UPLOAD_MINIO"), default=True)
    reset_crawl_data = _as_bool(os.environ.get("FEN_RESET_CRAWL_DATA"), default=False)
    run_id = os.environ.get("FEN_RUN_ID", "").strip() or None
    # Never forward FB password/TOTP into this job (avoid 2FA) /
    # Không truyền password/TOTP vào job này (tránh 2FA)
    result = run_graphql_enrich_batch(
        group_id=group_id,
        batch_target=batch_target,
        discover_chunk=discover_chunk,
        permalink_pause_sec=permalink_pause_sec,
        soft_restart_every=soft_restart_every,
        bottom_year=bottom_year,
        skip_streak_threshold=skip_streak,
        download_images=download_images,
        headless=headless,
        upload_minio=upload_minio,
        reset_crawl_data=reset_crawl_data,
        run_id=run_id,
    )
    print(f"[final_exam_nlp_graphql_batch] done {result}")


def _run_fen_crawl_discover() -> None:
    from fen_crawl_common import DEFAULT_BATCH_SIZE, DEFAULT_SKIP_STREAK
    from fen_crawl_discover import run_discover_batch
    from final_exam_nlp_crawl_runner import DEFAULT_BOTTOM_YEAR

    group_id = os.environ.get("FEN_GROUP_ID", "").strip()
    batch_size = int(
        os.environ.get("FEN_BATCH_TARGET", str(DEFAULT_BATCH_SIZE)).strip() or str(DEFAULT_BATCH_SIZE)
    )
    bottom_year = int(
        os.environ.get("FEN_BOTTOM_YEAR", str(DEFAULT_BOTTOM_YEAR)).strip() or str(DEFAULT_BOTTOM_YEAR)
    )
    skip_streak = int(
        os.environ.get("FEN_SKIP_STREAK", str(DEFAULT_SKIP_STREAK)).strip() or str(DEFAULT_SKIP_STREAK)
    )
    headless = _as_bool(os.environ.get("FEN_HEADLESS"), default=False)
    run_id = os.environ.get("FEN_RUN_ID", "").strip() or None
    reset_crawl_data = _as_bool(os.environ.get("FEN_RESET_CRAWL_DATA"), default=False)
    result = run_discover_batch(
        group_id=group_id,
        batch_size=batch_size,
        bottom_year=bottom_year,
        skip_streak=skip_streak,
        headless=headless,
        run_id=run_id,
        reset_crawl_data=reset_crawl_data,
    )
    print(f"[fen_crawl_discover] done {result}")


def _run_fen_crawl_enrich() -> None:
    from fen_crawl_common import DEFAULT_SOFT_RESTART_EVERY
    from fen_crawl_enrich import run_enrich_batch, run_enrich_one_permalink

    group_id = os.environ.get("FEN_GROUP_ID", "").strip()
    # One-permalink smoke test (Job2 path) / Smoke test 1 permalink (đúng path Job2)
    test_permalink = os.environ.get("FEN_TEST_PERMALINK", "").strip()
    if test_permalink:
        permalink_pause_sec = float(
            os.environ.get("FEN_PERMALINK_PAUSE_SEC", "3.5").strip() or "3.5"
        )
        headless = _as_bool(os.environ.get("FEN_HEADLESS"), default=False)
        result = run_enrich_one_permalink(
            group_id=group_id,
            permalink=test_permalink,
            permalink_pause_sec=permalink_pause_sec,
            headless=headless,
        )
        print(f"[fen_crawl_enrich] smoke_one done {result}")
        return

    soft_restart_every = int(
        os.environ.get("FEN_SOFT_RESTART_EVERY", str(DEFAULT_SOFT_RESTART_EVERY)).strip()
        or str(DEFAULT_SOFT_RESTART_EVERY)
    )
    permalink_pause_sec = float(
        os.environ.get("FEN_PERMALINK_PAUSE_SEC", "3.5").strip() or "3.5"
    )
    headless = _as_bool(os.environ.get("FEN_HEADLESS"), default=False)
    run_id = os.environ.get("FEN_RUN_ID", "").strip() or None
    result = run_enrich_batch(
        group_id=group_id,
        soft_restart_every=soft_restart_every,
        permalink_pause_sec=permalink_pause_sec,
        headless=headless,
        run_id=run_id,
    )
    print(f"[fen_crawl_enrich] done {result}")


def _run_fen_crawl_download() -> None:
    from fen_crawl_download import run_download_batch

    group_id = os.environ.get("FEN_GROUP_ID", "").strip()
    run_id = os.environ.get("FEN_RUN_ID", "").strip() or None
    result = run_download_batch(group_id=group_id, run_id=run_id)
    print(f"[fen_crawl_download] done {result}")


def _run_final_exam_nlp_seed2_discover() -> None:
    from fen_crawl_common import DEFAULT_BATCH_SIZE
    from final_exam_nlp_seed2_discover import run_seed2_discover_batch

    group_id = os.environ.get("FEN_GROUP_ID", "").strip()
    batch_size = int(
        os.environ.get("FEN_BATCH_TARGET", str(DEFAULT_BATCH_SIZE)).strip() or str(DEFAULT_BATCH_SIZE)
    )
    run_id = os.environ.get("FEN_RUN_ID", "").strip() or None
    a_prefix = os.environ.get("FEN_UNION_SEEN_PREFIX", "facebook").strip() or "facebook"
    result = run_seed2_discover_batch(
        group_id=group_id,
        batch_size=batch_size,
        run_id=run_id,
        a_prefix=a_prefix,
    )
    print(f"[final_exam_nlp_seed2_discover] done {result}")


def _run_final_exam_nlp_seed2_dump_ingest() -> None:
    from fen_crawl_common import DEFAULT_BATCH_SIZE
    from final_exam_nlp_seed2_dump_ingest import run_seed2_dump_ingest_batch

    group_id = os.environ.get("FEN_GROUP_ID", "").strip()
    batch_size = int(
        os.environ.get("FEN_BATCH_TARGET", str(DEFAULT_BATCH_SIZE)).strip() or str(DEFAULT_BATCH_SIZE)
    )
    run_id = os.environ.get("FEN_RUN_ID", "").strip() or None
    a_prefix = os.environ.get("FEN_UNION_SEEN_PREFIX", "facebook").strip() or "facebook"
    result = run_seed2_dump_ingest_batch(
        group_id=group_id,
        batch_size=batch_size,
        run_id=run_id,
        a_prefix=a_prefix,
    )
    print(f"[final_exam_nlp_seed2_dump_ingest] done {result}")


def _run_final_exam_nlp_cdn_refetch() -> None:
    from final_exam_nlp_cdn_refetch import DEFAULT_LIMIT, run_cdn_refetch_batch
    from fen_crawl_common import DEFAULT_SOFT_RESTART_EVERY

    group_id = os.environ.get("FEN_GROUP_ID", "").strip()
    limit = int(os.environ.get("FEN_REFETCH_LIMIT", str(DEFAULT_LIMIT)).strip() or str(DEFAULT_LIMIT))
    soft_restart_every = int(
        os.environ.get("FEN_SOFT_RESTART_EVERY", str(DEFAULT_SOFT_RESTART_EVERY)).strip()
        or str(DEFAULT_SOFT_RESTART_EVERY)
    )
    permalink_pause_sec = float(os.environ.get("FEN_PERMALINK_PAUSE_SEC", "3.5").strip() or "3.5")
    headless = _as_bool(os.environ.get("FEN_HEADLESS"), default=False)
    run_id = os.environ.get("FEN_RUN_ID", "").strip() or None
    result = run_cdn_refetch_batch(
        group_id=group_id,
        limit=limit,
        permalink_pause_sec=permalink_pause_sec,
        soft_restart_every=soft_restart_every,
        headless=headless,
        run_id=run_id,
    )
    print(f"[final_exam_nlp_cdn_refetch] done {result}")


def _run_final_exam_nlp_gallery_walk() -> None:
    # Route GraphQL mode to new batch runner / Chuyển mode GraphQL sang runner mới
    crawl_mode = os.environ.get("FEN_CRAWL_MODE", "media_walk").strip() or "media_walk"
    if crawl_mode in {"graphql_batch", "graphql", "graphql_enrich"}:
        _run_final_exam_nlp_graphql_batch()
        return

    from final_exam_nlp_two_phase import run_gallery_walk_batch
    from final_exam_nlp_crawl_runner import (
        DEFAULT_GALLERY_BASELINE_FBID,
        DEFAULT_GALLERY_DIRECTION,
        DEFAULT_GALLERY_STEP_PAUSE_SEC,
        DEFAULT_GALLERY_WALK_COUNT,
    )

    group_id = os.environ.get("FEN_GROUP_ID", "").strip()
    gallery_walk_count = int(
        os.environ.get("FEN_GALLERY_WALK_COUNT", str(DEFAULT_GALLERY_WALK_COUNT)).strip()
        or str(DEFAULT_GALLERY_WALK_COUNT)
    )
    gallery_baseline_fbid = (
        os.environ.get("FEN_GALLERY_BASELINE_FBID", DEFAULT_GALLERY_BASELINE_FBID).strip()
        or DEFAULT_GALLERY_BASELINE_FBID
    )
    gallery_seek_fbid = os.environ.get("FEN_GALLERY_SEEK_FBID", "").strip()
    gallery_step_pause_sec = float(
        os.environ.get("FEN_GALLERY_STEP_PAUSE_SEC", str(DEFAULT_GALLERY_STEP_PAUSE_SEC)).strip()
        or str(DEFAULT_GALLERY_STEP_PAUSE_SEC)
    )
    gallery_direction = (
        os.environ.get("FEN_GALLERY_DIRECTION", DEFAULT_GALLERY_DIRECTION).strip()
        or DEFAULT_GALLERY_DIRECTION
    )
    bottom_year = int(os.environ.get("FEN_BOTTOM_YEAR", "2013").strip() or "2013")
    permalink_pause_sec = float(
        os.environ.get("FEN_PERMALINK_PAUSE_SEC", "3.5").strip() or "3.5"
    )
    headless = _as_bool(os.environ.get("FEN_HEADLESS"), default=True)
    upload_minio = _as_bool(os.environ.get("FEN_UPLOAD_MINIO"), default=True)
    force = _as_bool(os.environ.get("FEN_FORCE"), default=False)
    run_id = os.environ.get("FEN_RUN_ID", "").strip() or None
    result = run_gallery_walk_batch(
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
        fb_username=os.environ.get("FB_USERNAME", "").strip(),
        fb_password=os.environ.get("FB_PASSWORD", "").strip(),
        fb_totp_secret=os.environ.get("FB_TOTP_SECRET", "").strip(),
        run_id=run_id,
        permalink_pause_sec=permalink_pause_sec,
    )
    print(f"[final_exam_nlp_gallery_walk] done {result}")


def _run_final_exam_nlp_invalid_recheck() -> None:
    from final_exam_nlp_crawl_runner import (
        DEFAULT_INVALID_RECHECK_LIMIT,
        DEFAULT_INVALID_RECHECK_MAX_ATTEMPTS,
        DEFAULT_SKIPPED_RECHECK_LIMIT,
        DEFAULT_SKIPPED_RECHECK_MAX_ATTEMPTS,
    )
    from final_exam_nlp_two_phase import run_invalid_recheck_batch

    group_id = os.environ.get("FEN_GROUP_ID", "").strip()
    recheck_limit = int(
        os.environ.get("FEN_INVALID_RECHECK_LIMIT", str(DEFAULT_INVALID_RECHECK_LIMIT)).strip()
        or str(DEFAULT_INVALID_RECHECK_LIMIT)
    )
    max_attempts = int(
        os.environ.get(
            "FEN_INVALID_RECHECK_MAX_ATTEMPTS", str(DEFAULT_INVALID_RECHECK_MAX_ATTEMPTS)
        ).strip()
        or str(DEFAULT_INVALID_RECHECK_MAX_ATTEMPTS)
    )
    skipped_recheck_limit = int(
        os.environ.get("FEN_SKIPPED_RECHECK_LIMIT", str(DEFAULT_SKIPPED_RECHECK_LIMIT)).strip()
        or str(DEFAULT_SKIPPED_RECHECK_LIMIT)
    )
    skipped_max_attempts = int(
        os.environ.get(
            "FEN_SKIPPED_RECHECK_MAX_ATTEMPTS", str(DEFAULT_SKIPPED_RECHECK_MAX_ATTEMPTS)
        ).strip()
        or str(DEFAULT_SKIPPED_RECHECK_MAX_ATTEMPTS)
    )
    enabled = _as_bool(os.environ.get("FEN_RECHECK_INVALID_ENABLED"), default=True)
    permalink_pause_sec = float(
        os.environ.get("FEN_PERMALINK_PAUSE_SEC", "3.5").strip() or "3.5"
    )
    headless = _as_bool(os.environ.get("FEN_HEADLESS"), default=True)
    upload_minio = _as_bool(os.environ.get("FEN_UPLOAD_MINIO"), default=True)
    run_id = os.environ.get("FEN_RUN_ID", "").strip() or None
    result = run_invalid_recheck_batch(
        group_id=group_id,
        recheck_limit=recheck_limit,
        max_attempts=max_attempts,
        skipped_recheck_limit=skipped_recheck_limit,
        skipped_max_attempts=skipped_max_attempts,
        enabled=enabled,
        headless=headless,
        upload_minio=upload_minio,
        fb_username=os.environ.get("FB_USERNAME", "").strip(),
        fb_password=os.environ.get("FB_PASSWORD", "").strip(),
        fb_totp_secret=os.environ.get("FB_TOTP_SECRET", "").strip(),
        run_id=run_id,
        permalink_pause_sec=permalink_pause_sec,
    )
    print(f"[final_exam_nlp_invalid_recheck] done {result}")


def _run_final_exam_nlp_bootstrap_login() -> None:
    """One-shot login on selenium Chrome profile volume; saves cookies to MinIO for fallback.

    Login một lần trên profile Chrome (volume selenium); lưu cookie MinIO làm fallback.
    Set FEN_MANUAL_LOGIN=true to wait for human login via noVNC (http://localhost:7900).
    Đặt FEN_MANUAL_LOGIN=true để chờ người login qua noVNC.
    """
    import time

    from common.chau_ban_schema import utc_now_iso
    from common.io_storage import ensure_bucket, get_minio_client, upload_json_payload
    from final_exam_nlp_crawl_runner import (
        FB_LOGIN_URL,
        _build_driver,
        _cookies_key,
        _is_logged_in,
        _login_with_credentials,
        _probe_profile_session,
        _require_logged_in_session,
        _safe_quit_driver,
        _selenium_preflight,
        _settings,
        _wait_logged_in,
    )

    group_id = os.environ.get("FEN_GROUP_ID", "").strip()
    cookie_slot = os.environ.get("FEN_COOKIES_SLOT", "").strip()
    # Optional per-worker Selenium URL (session B) / URL Selenium theo worker (session B)
    remote_override = os.environ.get("FEN_SELENIUM_REMOTE_URL", "").strip()
    if remote_override:
        os.environ["FEN_FINAL_EXAM_NLP_SELENIUM_REMOTE_URL"] = remote_override
    settings = _settings()
    bucket = settings["bucket_raw"]
    source_prefix = settings["source_prefix"]
    remote_url = settings["selenium_remote_url"]
    manual_login = _as_bool(os.environ.get("FEN_MANUAL_LOGIN"), default=False)
    # Manual login needs a visible browser (noVNC) / Login thủ công cần browser nhìn thấy được
    headless = False if manual_login else _as_bool(os.environ.get("FEN_HEADLESS"), default=True)
    upload_minio = _as_bool(os.environ.get("FEN_UPLOAD_MINIO"), default=True)
    fb_username = os.environ.get("FB_USERNAME", "").strip()
    fb_password = os.environ.get("FB_PASSWORD", "").strip()
    fb_totp_secret = os.environ.get("FB_TOTP_SECRET", "").strip()
    # How long to wait for human login via noVNC / Thời gian chờ người login qua noVNC
    manual_wait_sec = int(os.environ.get("FEN_MANUAL_LOGIN_WAIT_SEC", "600").strip() or "600")

    _selenium_preflight(remote_url)
    driver = _build_driver(remote_url, headless, page_load_timeout=90)
    try:
        if _probe_profile_session(driver):
            print("[final_exam_nlp_bootstrap_login] profile already logged in", flush=True)
        elif manual_login:
            print(
                "[final_exam_nlp_bootstrap_login] MANUAL LOGIN — open noVNC and sign in to Facebook",
                flush=True,
            )
            print(
                "[final_exam_nlp_bootstrap_login] noVNC: http://localhost:7900  password: secret",
                flush=True,
            )
            try:
                driver.get(FB_LOGIN_URL)
            except Exception:
                pass
            print(
                f"[final_exam_nlp_bootstrap_login] waiting up to {manual_wait_sec}s "
                "for you to finish login (2FA OK)…",
                flush=True,
            )
            if not _wait_logged_in(driver, manual_wait_sec):
                raise RuntimeError(
                    "Manual login timed out — finish Facebook login in noVNC "
                    f"(http://localhost:7900) within {manual_wait_sec}s"
                )
            print("[final_exam_nlp_bootstrap_login] manual login detected", flush=True)
        elif fb_username and fb_password:
            print("[final_exam_nlp_bootstrap_login] profile empty — credential login", flush=True)
            _login_with_credentials(driver, fb_username, fb_password, fb_totp_secret)
        _require_logged_in_session(
            driver,
            had_cookies=False,
            fb_username="" if manual_login else fb_username,
            fb_password="" if manual_login else fb_password,
            fb_totp_secret="" if manual_login else fb_totp_secret,
            log_prefix="[final_exam_nlp_bootstrap_login]",
        )
        group_url = f"https://www.facebook.com/groups/{group_id}"
        driver.get(group_url)
        time.sleep(4)
        logged = _is_logged_in(driver)
        print(
            f"[final_exam_nlp_bootstrap_login] group_check logged_in={logged} "
            f"url={(driver.current_url or '')[:120]}",
            flush=True,
        )
        if not logged:
            raise RuntimeError("Bootstrap login failed on group page")
        cookies = driver.get_cookies() or []
        if upload_minio:
            ensure_bucket(get_minio_client(), bucket)
            cookies_object = _cookies_key(source_prefix, group_id, cookie_slot)
            upload_json_payload(
                bucket,
                cookies_object,
                {"saved_at": utc_now_iso(), "cookies": cookies, "slot": cookie_slot or "a"},
            )
            print(
                f"[final_exam_nlp_bootstrap_login] saved {len(cookies)} cookies "
                f"to MinIO key={cookies_object} slot={cookie_slot or 'a'}",
                flush=True,
            )
        print("[final_exam_nlp_bootstrap_login] success — profile session ready for rollover", flush=True)
    finally:
        _safe_quit_driver(driver)


def _run_final_exam_nlp_ocr() -> None:
    from final_exam_nlp_ocr import run_ocr

    group_id = os.environ.get("FEN_GROUP_ID", "").strip() or "322453387859386"
    result = run_ocr(
        group_id=group_id,
        batch_seq=int(os.environ.get("FEN_OCR_BATCH_SEQ", "0").strip() or "0"),
        limit=int(os.environ.get("FEN_OCR_LIMIT", "0").strip() or "0"),
        force=_as_bool(os.environ.get("FEN_OCR_FORCE"), default=False),
    )
    print(f"[final_exam_nlp_ocr] done {result}")


def _run_final_exam_nlp_ocr_rebuild() -> None:
    # Recover records that killed batches never flushed / Cứu record batch bị kill chưa flush
    from final_exam_nlp_ocr import rebuild_ocr_jsonl

    group_id = os.environ.get("FEN_GROUP_ID", "").strip() or "322453387859386"
    result = rebuild_ocr_jsonl(group_id=group_id)
    print(f"[final_exam_nlp_ocr_rebuild] done {result}")


def _run_final_exam_nlp_ocr_eval() -> None:
    from final_exam_nlp_ocr_eval import run_ocr_eval

    group_id = os.environ.get("FEN_GROUP_ID", "").strip() or "322453387859386"
    result = run_ocr_eval(
        group_id=group_id,
        batch_size=int(os.environ.get("FEN_EVAL_BATCH_SIZE", "200").strip() or "200"),
        force=_as_bool(os.environ.get("FEN_EVAL_FORCE"), default=False),
        run_reocr=_as_bool(os.environ.get("FEN_EVAL_REOCR"), default=True),
        reocr_limit=int(os.environ.get("FEN_EVAL_REOCR_LIMIT", "15").strip() or "15"),
    )
    print(f"[final_exam_nlp_ocr_eval] done {result}")


def _run_final_exam_nlp_ocr_label_dual() -> None:
    from final_exam_nlp_ocr_label_dual import run_label_dual

    group_id = os.environ.get("FEN_GROUP_ID", "").strip() or "322453387859386"
    result = run_label_dual(
        group_id=group_id,
        limit=int(os.environ.get("FEN_LABEL_LIMIT", "0").strip() or "0"),
        flush_posts=int(os.environ.get("FEN_LABEL_FLUSH_POSTS", "10").strip() or "10"),
        force=_as_bool(os.environ.get("FEN_LABEL_FORCE"), default=False),
        replay=_as_bool(os.environ.get("FEN_LABEL_REPLAY"), default=False),
        glm=_as_bool(os.environ.get("FEN_LABEL_GLM"), default=True),
        batch_seq=int(os.environ.get("FEN_LABEL_BATCH_SEQ", "0").strip() or "0"),
        workers=int(os.environ.get("FEN_LABEL_WORKERS", "12").strip() or "12"),
        prepare_queues=_as_bool(os.environ.get("FEN_LABEL_PREPARE_QUEUES"), default=True),
    )
    print(f"[final_exam_nlp_ocr_label_dual] done {result}")


def _run_final_exam_nlp_ocr_label_dual_backfill() -> None:
    from final_exam_nlp_ocr_label_dual import run_label_dual_backfill

    group_id = os.environ.get("FEN_GROUP_ID", "").strip() or "322453387859386"
    result = run_label_dual_backfill(
        group_id=group_id,
        flush_posts=int(os.environ.get("FEN_LABEL_FLUSH_POSTS", "10").strip() or "10"),
        run_glm=_as_bool(os.environ.get("FEN_LABEL_GLM"), default=True),
        require_both=_as_bool(os.environ.get("FEN_LABEL_BACKFILL_REQUIRE_BOTH"), default=True),
    )
    print(f"[final_exam_nlp_ocr_label_dual_backfill] done {result}")


def _run_final_exam_nlp_ocr_label_dual_merge() -> None:
    from final_exam_nlp_ocr_label_dual import run_label_dual_merge

    group_id = os.environ.get("FEN_GROUP_ID", "").strip() or "322453387859386"
    min_bag = 0.7
    try:
        min_bag = float(os.environ.get("FEN_LABEL_MERGE_MIN_BAG_WARN", "0.7").strip() or "0.7")
    except ValueError:
        pass
    result = run_label_dual_merge(
        group_id=group_id,
        prefer=os.environ.get("FEN_LABEL_MERGE_PREFER", "fuse_first").strip() or "fuse_first",
        min_bag_warn=min_bag,
        dry_run=_as_bool(os.environ.get("FEN_LABEL_MERGE_DRY_RUN"), default=False),
    )
    print(f"[final_exam_nlp_ocr_label_dual_merge] done {result}")


def _run_final_exam_nlp_gt_confidence_judge() -> None:
    from final_exam_nlp_gt_confidence_judge import run_gt_confidence_judge

    group_id = os.environ.get("FEN_GROUP_ID", "").strip() or "322453387859386"
    result = run_gt_confidence_judge(group_id=group_id)
    print(f"[final_exam_nlp_gt_confidence_judge] done {result}")


def _run_final_exam_nlp_consensus_rollup() -> None:
    from final_exam_nlp_consensus_rollup import run_consensus_rollup

    group_id = os.environ.get("FEN_GROUP_ID", "").strip() or "322453387859386"
    min_bag = 0.7
    cluster_bag = 0.98
    try:
        min_bag = float(os.environ.get("FEN_CONSENSUS_MIN_BAG_WARN", "0.7").strip() or "0.7")
    except ValueError:
        pass
    try:
        cluster_bag = float(os.environ.get("FEN_CONSENSUS_CLUSTER_BAG", "0.98").strip() or "0.98")
    except ValueError:
        pass
    result = run_consensus_rollup(
        group_id=group_id,
        prefer=os.environ.get("FEN_CONSENSUS_PREFER", "fuse_first").strip() or "fuse_first",
        min_bag_warn=min_bag,
        cluster_bag=cluster_bag,
        dry_run=_as_bool(os.environ.get("FEN_CONSENSUS_DRY_RUN"), default=False),
    )
    print(f"[final_exam_nlp_consensus_rollup] done {result}")


def _run_final_exam_nlp_ocr_retry() -> None:
    from final_exam_nlp_ocr_retry import run_ocr_retry

    group_id = os.environ.get("FEN_GROUP_ID", "").strip() or "322453387859386"
    result = run_ocr_retry(
        group_id=group_id,
        limit=int(os.environ.get("FEN_OCR_RETRY_LIMIT", "80").strip() or "80"),
        batch_seq=int(os.environ.get("FEN_OCR_BATCH_SEQ", "0").strip() or "0"),
    )
    print(f"[final_exam_nlp_ocr_retry] done {result}")


def _run_index_fen_qdrant() -> None:
    from index_fen_qdrant import index_fen_ocr_batch

    group_id = os.environ.get("FEN_GROUP_ID", "").strip() or "322453387859386"
    batch_seq = int(os.environ.get("FEN_OCR_BATCH_SEQ", "0").strip() or "0")
    recreate = _as_bool(os.environ.get("FEN_QDRANT_RECREATE"), default=False)
    result = index_fen_ocr_batch(group_id=group_id, batch_seq=batch_seq, recreate=recreate)
    print(f"[index_fen_qdrant] done {result}")


def _run_reindex_fen_qdrant() -> None:
    # Repair batches whose index step never ran / Sửa batch chưa chạy bước index
    from index_fen_qdrant import DEFAULT_REINDEX_CHUNK, _parse_batch_seqs, reindex_from_ocr_result

    group_id = os.environ.get("FEN_GROUP_ID", "").strip() or "322453387859386"
    batch_seqs = _parse_batch_seqs(os.environ.get("FEN_REINDEX_BATCH_SEQS", ""))
    chunk_size = int(
        os.environ.get("FEN_REINDEX_CHUNK", "").strip() or DEFAULT_REINDEX_CHUNK
    )
    result = reindex_from_ocr_result(
        group_id=group_id, batch_seqs=batch_seqs, chunk_size=chunk_size
    )
    print(f"[reindex_fen_qdrant] done {result}")


def main() -> None:
    # Dispatch FEN job by FEN_JOB env var / Điều phối job theo biến FEN_JOB
    os.environ.setdefault("FEN_CONFIG_PATH", "/opt/fen-exam/dags/config.ini")
    os.environ.setdefault("FEN_PATHS_OUTPUT_DIR", "/tmp/fen-output")
    os.environ.setdefault("FEN_SKIP_LOCAL_OUTPUT", "true")

    _ensure_jobs_on_path()
    job = os.environ.get("FEN_JOB", "").strip().lower()
    if job == "opencv_preprocess_pages":
        _run_opencv_preprocess_pages()
        return
    if job == "ocr_v2_pages":
        _run_ocr_v2_pages()
        return
    if job == "align_v2_pages":
        _run_align_v2_pages()
        return
    if job == "build_catalog":
        _run_build_catalog()
        return
    if job == "refine_entries":
        _run_refine_entries()
        return
    if job == "stitch_entries":
        _run_stitch_entries()
        return
    if job == "index_pairs_qdrant":
        _run_index_pairs_qdrant()
        return
    if job == "index_catalog_qdrant":
        _run_index_catalog_qdrant()
        return
    if job == "cleanup_toc_state":
        _run_cleanup_toc_state()
        return
    if job == "export_entries_excel":
        _run_export_entries_excel()
        return
    if job == "export_parallel_corpus":
        _run_export_parallel_corpus()
        return
    if job == "repair_entries":
        _run_repair_entries()
        return
    if job == "refresh_catalog":
        _run_refresh_catalog()
        return
    if job == "compare_ocr_corpus":
        _run_compare_ocr_corpus()
        return
    if job == "final_exam_nlp_crawl":
        _run_final_exam_nlp_crawl()
        return
    if job == "final_exam_nlp_media_batch":
        _run_final_exam_nlp_media_batch()
        return
    if job == "final_exam_nlp_caption_match":
        _run_final_exam_nlp_caption_match()
        return
    if job == "final_exam_nlp_gallery_walk":
        _run_final_exam_nlp_gallery_walk()
        return
    if job == "final_exam_nlp_graphql_batch":
        _run_final_exam_nlp_graphql_batch()
        return

    if job == "fen_bootstrap_login":
        _run_final_exam_nlp_bootstrap_login()
        return
    if job == "fen_label_dual":
        _run_final_exam_nlp_ocr_label_dual()
        return
    if job == "fen_ocr":
        _run_final_exam_nlp_ocr()
        return
    if job == "fen_ocr_retry":
        _run_final_exam_nlp_ocr_retry()
        return
    if job == "fen_crawl_discover":
        _run_fen_crawl_discover()
        return
    if job == "fen_crawl_enrich":
        _run_fen_crawl_enrich()
        return
    if job == "fen_crawl_download":
        _run_fen_crawl_download()
        return
    if job == "final_exam_nlp_seed2_discover":
        _run_final_exam_nlp_seed2_discover()
        return
    if job == "final_exam_nlp_seed2_dump_ingest":
        _run_final_exam_nlp_seed2_dump_ingest()
        return
    if job == "final_exam_nlp_cdn_refetch":
        _run_final_exam_nlp_cdn_refetch()
        return
    if job == "final_exam_nlp_invalid_recheck":
        _run_final_exam_nlp_invalid_recheck()
        return
    if job == "final_exam_nlp_bootstrap_login":
        _run_final_exam_nlp_bootstrap_login()
        return
    if job == "final_exam_nlp_ocr":
        _run_final_exam_nlp_ocr()
        return
    if job == "index_fen_qdrant":
        _run_index_fen_qdrant()
        return
    if job == "final_exam_nlp_ocr_eval":
        _run_final_exam_nlp_ocr_eval()
        return
    if job == "final_exam_nlp_ocr_label_dual":
        _run_final_exam_nlp_ocr_label_dual()
        return
    if job == "final_exam_nlp_ocr_label_dual_backfill":
        _run_final_exam_nlp_ocr_label_dual_backfill()
        return
    if job == "final_exam_nlp_ocr_label_dual_merge":
        _run_final_exam_nlp_ocr_label_dual_merge()
        return
    if job == "final_exam_nlp_gt_confidence_judge":
        _run_final_exam_nlp_gt_confidence_judge()
        return
    if job == "final_exam_nlp_consensus_rollup":
        _run_final_exam_nlp_consensus_rollup()
        return
    if job == "final_exam_nlp_ocr_retry":
        _run_final_exam_nlp_ocr_retry()
        return
    if job == "final_exam_nlp_ocr_rebuild":
        _run_final_exam_nlp_ocr_rebuild()
        return
    if job == "reindex_fen_qdrant":
        _run_reindex_fen_qdrant()
        return
    raise ValueError(
        f"Unsupported FEN_JOB '{job}'. "
        "Use: opencv_preprocess_pages, ocr_v2_pages, align_v2_pages, "
        "build_catalog, refine_entries, stitch_entries, "
        "index_pairs_qdrant, index_catalog_qdrant, cleanup_toc_state, "
        "export_entries_excel, export_parallel_corpus, repair_entries, refresh_catalog, "
        "compare_ocr_corpus, final_exam_nlp_crawl, "
        "final_exam_nlp_media_batch, final_exam_nlp_caption_match, "
        "final_exam_nlp_gallery_walk, final_exam_nlp_graphql_batch, "
        "fen_crawl_discover, fen_crawl_enrich, fen_crawl_download, "
        "final_exam_nlp_seed2_discover, final_exam_nlp_seed2_dump_ingest, "
        "final_exam_nlp_cdn_refetch, "
        "final_exam_nlp_invalid_recheck, final_exam_nlp_bootstrap_login, "
        "final_exam_nlp_ocr, final_exam_nlp_ocr_eval, final_exam_nlp_ocr_label_dual, "
        "final_exam_nlp_ocr_label_dual_backfill, final_exam_nlp_ocr_label_dual_merge, "
        "final_exam_nlp_gt_confidence_judge, final_exam_nlp_consensus_rollup, final_exam_nlp_ocr_retry, "
        "final_exam_nlp_ocr_rebuild, index_fen_qdrant, reindex_fen_qdrant"
    )


if __name__ == "__main__":
    main()
