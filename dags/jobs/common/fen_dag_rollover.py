"""Dump rollover: queue next dump while current DAG waits for OCR (RWO PVC).

Rollover dump: dump lượt sau queued, DAG hiện tại chờ OCR xong rồi nhả PVC.
"""
from __future__ import annotations

import os
import time
from typing import Any


def should_continue_dump(
    *,
    seed_prefix: str,
    default_group_id: str,
    log_tag: str,
    **context: Any,
) -> bool:
    """Return True when checkpoint still has dump work left.
    True khi checkpoint vẫn còn việc dump.
    """
    os.environ["FEN_SOURCE_PREFIX"] = seed_prefix
    from final_exam_nlp_v5_common import ensure_raw_bucket, load_checkpoint

    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run else None) or {}
    params = context.get("params") or {}
    group_id = str(conf.get("group_id") or params.get("group_id") or default_group_id)
    bucket, source_prefix = ensure_raw_bucket()
    ck = load_checkpoint(bucket, source_prefix, group_id)
    should = bool(ck.get("should_continue"))
    print(
        f"[{log_tag}] should_continue={should} stop={ck.get('stop_reason')} "
        f"prefix={source_prefix}",
        flush=True,
    )
    return should


def cooldown_and_trigger_dump(
    *,
    crawl_dag_id: str,
    default_group_id: str,
    default_batch_target: int,
    default_cooldown_sec: int,
    log_tag: str,
    **context: Any,
) -> str:
    """Wait cooldown then trigger the next dump DAG run.
    Nghỉ cooldown rồi trigger dump DAG tiếp theo.
    """
    # pyrefly: ignore [missing-import]
    from airflow.api.common.trigger_dag import trigger_dag  # type: ignore

    dag_run = context.get("dag_run")
    conf = dict((dag_run.conf if dag_run else None) or {})
    params = context.get("params") or {}
    group_id = str(conf.get("group_id") or params.get("group_id") or default_group_id)
    try:
        cooldown = max(
            0,
            int(
                conf.get("rollover_cooldown_sec")
                or params.get("rollover_cooldown_sec")
                or default_cooldown_sec
            ),
        )
    except (TypeError, ValueError):
        cooldown = default_cooldown_sec
    try:
        batch_target = int(
            conf.get("batch_target") or params.get("batch_target") or default_batch_target
        )
    except (TypeError, ValueError):
        batch_target = default_batch_target
    print(f"[{log_tag}] cooldown_sec={cooldown} next={crawl_dag_id}", flush=True)
    time.sleep(cooldown)
    run_id = trigger_dag(
        dag_id=crawl_dag_id,
        conf={
            "group_id": group_id,
            "batch_target": batch_target,
            "force": False,
            "upload_minio": True,
            "rollover_cooldown_sec": cooldown,
        },
        replace_microseconds=False,
    )
    print(f"[{log_tag}] trigger_next_dump run_id={run_id}", flush=True)
    # trigger_dag returns a DagRun object; XCom must carry run_id only
    # trigger_dag trả object DagRun; XCom chỉ được mang run_id
    return str(getattr(run_id, "run_id", None) or "")


_OCR_DONE = frozenset({"success", "failed"})


def ocr_dag_run_done(*, ocr_dag_id: str, trigger_task_id: str, **context: Any) -> bool:
    """True when the OCR DagRun from XCom is success/failed (or skipped).
    True khi OCR DagRun từ XCom đã success/failed (hoặc skip).
    """
    run_id = context["ti"].xcom_pull(task_ids=trigger_task_id)
    if not run_id or run_id == "skip" or str(run_id).startswith("skipped:"):
        return True
    # pyrefly: ignore [missing-import]
    from airflow.models.dagrun import DagRun  # type: ignore
    from airflow.settings import Session

    session = Session()
    try:
        raw = str(run_id)
        dr = (
            session.query(DagRun)
            .filter(DagRun.dag_id == ocr_dag_id, DagRun.run_id == raw)
            .first()
        )
        # Legacy XCom stored repr(DagRun) instead of run_id; find which known
        # run_id is embedded in that string so old runs are not stuck forever.
        # XCom cũ lưu repr(DagRun) thay vì run_id; dò xem run_id nào nằm trong
        # chuỗi đó để các run cũ không bị treo vĩnh viễn.
        if dr is None and raw.startswith("<DagRun "):
            recent = (
                session.query(DagRun)
                .filter(DagRun.dag_id == ocr_dag_id)
                .order_by(DagRun.execution_date.desc())
                .limit(50)
                .all()
            )
            # Longest match wins so a shorter run_id cannot shadow a longer one
            # Lấy match dài nhất để run_id ngắn không che run_id dài
            hits = [c for c in recent if str(c.run_id) and str(c.run_id) in raw]
            if hits:
                dr = max(hits, key=lambda c: len(str(c.run_id)))
                print(
                    f"[wait_ocr] recovered run_id={dr.run_id} from legacy xcom",
                    flush=True,
                )
        state = getattr(dr, "state", None) if dr is not None else None
        print(
            f"[wait_ocr] dag={ocr_dag_id} run_id={getattr(dr, 'run_id', raw)} state={state}",
            flush=True,
        )
        return str(state or "") in _OCR_DONE
    finally:
        session.close()

