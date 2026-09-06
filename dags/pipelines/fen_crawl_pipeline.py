from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta

from airflow import DAG  # noqa: F401
from airflow.models.param import Param  # type: ignore
from airflow.operators.python import PythonOperator, ShortCircuitOperator  # type: ignore

_JOBS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "jobs"))
if _JOBS_DIR not in sys.path:
    sys.path.append(_JOBS_DIR)

from common.docker_executor import build_fen_job_task  # type: ignore

DEFAULT_ARGS = {
    "owner": "fen-exam",
    "depends_on_past": False,
    "start_date": datetime(2026, 8, 12),
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

DEFAULT_GROUP_ID = "322453387859386"
DEFAULT_BATCH_TARGET = 10
DEFAULT_ROLLOVER_COOLDOWN_SEC = 180


def _param_expr(name: str) -> str:
    return (
        "{% if dag_run and dag_run.conf %}"
        f"{{{{ dag_run.conf.get('{name}', params.{name}) }}}}"
        "{% else %}"
        f"{{{{ params.{name} }}}}"
        "{% endif %}"
    )


def _bool_expr(name: str) -> str:
    return (
        "{% if (dag_run.conf.get('" + name + "', params." + name + ") "
        "if dag_run and dag_run.conf else params." + name + ") %}true{% else %}false{% endif %}"
    )


def _resolve_group_id(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run else None) or {}
    params = context.get("params") or {}
    return str(conf.get("group_id") or params.get("group_id") or DEFAULT_GROUP_ID)


def _resolve_param(context, name: str, default):
    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run else None) or {}
    params = context.get("params") or {}
    return conf.get(name, params.get(name, default))


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _should_continue_crawl(**context) -> bool:
    from final_exam_nlp_crawl_runner import read_should_continue

    params = context.get("params") or {}
    conf = (context.get("dag_run").conf if context.get("dag_run") else None) or {}
    # Legacy alias: demo_mode=true == catch_bottom=false / Alias cũ
    if _truthy(conf.get("demo_mode") or params.get("demo_mode")):
        print("[fen_crawl] demo_mode=true (deprecated) → single batch only", flush=True)
        return False
    catch_bottom = _resolve_param(context, "catch_bottom", True)
    if not _truthy(catch_bottom):
        print("[fen_crawl] catch_bottom=false → single batch only (batch_target)", flush=True)
        return False
    group_id = _resolve_group_id(**context)
    should = read_should_continue(group_id)
    print(f"[fen_crawl] catch_bottom=true should_continue={should} group_id={group_id}", flush=True)
    return should


def _trigger_label_dual_batch(**context) -> str:
    from airflow.api.common.trigger_dag import trigger_dag  # type: ignore
    from fen_crawl_common import ensure_raw_bucket, get_active_batch

    group_id = _resolve_group_id(**context)
    bucket, source_prefix = ensure_raw_bucket()
    active = get_active_batch(bucket, source_prefix, group_id)
    batch_seq = int(active.get("batch_seq") or 0)
    if batch_seq <= 0:
        return "skip"
    label_limit = int(_resolve_param(context, "ocr_limit", 0))
    flush_posts = int(_resolve_param(context, "flush_posts", 5))
    force = bool(_resolve_param(context, "force", False))
    dag_run_id = f"fen_label_dual_g{group_id}_b{batch_seq:06d}"
    # Label dual batch_seq = quote shard (0 = all), not crawl batch_seq /
    # batch_seq của label dual = shard quote (0 = hết), không phải crawl batch_seq
    conf = {
        "group_id": group_id,
        "batch_seq": 0,
        "label_limit": label_limit,
        "flush_posts": flush_posts,
        "force": force,
        "prepare_queues": True,
        "glm": True,
        "crawl_batch_seq": batch_seq,
    }
    try:
        run_id = trigger_dag(
            dag_id="fen_label_dual_pipeline",
            run_id=dag_run_id,
            conf=conf,
            replace_microseconds=False,
        )
        return str(getattr(run_id, "run_id", None) or dag_run_id)
    except Exception as exc:
        print(f"[fen_crawl] trigger_label_dual skipped: {exc}", flush=True)
        return f"skipped:{type(exc).__name__}"


def _cooldown_and_trigger_next(**context) -> str:
    from airflow.api.common.trigger_dag import trigger_dag  # type: ignore
    from final_exam_nlp_crawl_runner import _drain_selenium_sessions, _settings

    try:
        remote = _settings()["selenium_remote_url"]
        drained = _drain_selenium_sessions(remote)
        print(f"[fen_crawl] drain_sessions={drained}", flush=True)
    except Exception as exc:
        print(f"[fen_crawl] drain skipped: {exc}", flush=True)

    dag_run = context.get("dag_run")
    conf = dict((dag_run.conf if dag_run else None) or {})
    params = context.get("params") or {}
    cooldown = int(conf.get("rollover_cooldown_sec") or params.get("rollover_cooldown_sec") or 0)
    if cooldown > 0:
        time.sleep(cooldown)
    conf["force"] = False
    conf["reset_crawl_data"] = False
    conf["crawl_mode"] = "split"
    conf["batch_target"] = int(
        _resolve_param(context, "batch_target", DEFAULT_BATCH_TARGET)
    )
    trigger_dag(dag_id="fen_crawl_pipeline", conf=conf, replace_microseconds=False)
    return "triggered"


with DAG(
    dag_id="fen_crawl_pipeline",
    default_args=DEFAULT_ARGS,
    description="Crawl: Discover → Enrich → Download → trigger label dual → optional rollover",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["fen-exam", "crawl", "docker"],
    params={
        "group_id": Param(default=DEFAULT_GROUP_ID, type="string"),
        "crawl_mode": Param(default="split", type="string"),
        "batch_target": Param(default=DEFAULT_BATCH_TARGET, type="integer", minimum=1),
        "catch_bottom": Param(
            default=True,
            type="boolean",
            description="true: rollover batches until bottom_year or feed end; false: one batch_target batch only",
        ),
        "demo_mode": Param(
            default=False,
            type="boolean",
            description="Deprecated — use catch_bottom=false instead",
        ),
        "soft_restart_every": Param(default=10, type="integer", minimum=1),
        "rollover_cooldown_sec": Param(default=DEFAULT_ROLLOVER_COOLDOWN_SEC, type="integer", minimum=0),
        "bottom_year": Param(default=2013, type="integer", minimum=2000),
        "permalink_pause_sec": Param(default=3.5, type="number", minimum=1),
        "skip_streak": Param(default=30, type="integer", minimum=10),
        "headless": Param(default=True, type="boolean"),
        "upload_minio": Param(default=True, type="boolean"),
        "force": Param(default=False, type="boolean"),
        "reset_crawl_data": Param(default=False, type="boolean"),
        "ocr_limit": Param(
            default=0,
            type="integer",
            minimum=0,
            description="Max images for label dual after crawl; 0 = full pending queue",
        ),
        "flush_posts": Param(
            default=5,
            type="integer",
            minimum=1,
            description="Passed to label dual: upsert jsonl every N images",
        ),
    },
) as dag:
    fen_env = {
        "FEN_GROUP_ID": _param_expr("group_id"),
        "FEN_CRAWL_MODE": _param_expr("crawl_mode"),
        "FEN_BATCH_TARGET": _param_expr("batch_target"),
        "FEN_SOFT_RESTART_EVERY": _param_expr("soft_restart_every"),
        "FEN_BOTTOM_YEAR": _param_expr("bottom_year"),
        "FEN_PERMALINK_PAUSE_SEC": _param_expr("permalink_pause_sec"),
        "FEN_SKIP_STREAK": _param_expr("skip_streak"),
        "FEN_RUN_ID": "{{ ts_nodash }}",
        "FEN_HEADLESS": _bool_expr("headless"),
        "SELENIUM_CHROME_PROFILE_DIR": "/data/chrome-profile",
        "FEN_UPLOAD_MINIO": _bool_expr("upload_minio"),
        "FEN_FORCE": _bool_expr("force"),
        "FEN_RESET_CRAWL_DATA": _bool_expr("reset_crawl_data"),
        "FEN_MINIO_ENDPOINT": "http://minio:9000",
        "SELENIUM_REMOTE_URL": "http://selenium-chrome:4444/wd/hub",
    }

    discover = build_fen_job_task(
        task_id="run_crawl_discover",
        job_name="fen_crawl_discover",
        execution_timeout=timedelta(hours=6),
        env_vars=fen_env,
    )
    enrich = build_fen_job_task(
        task_id="run_crawl_enrich",
        job_name="fen_crawl_enrich",
        execution_timeout=timedelta(hours=4),
        env_vars=fen_env,
    )
    download = build_fen_job_task(
        task_id="run_crawl_download",
        job_name="fen_crawl_download",
        execution_timeout=timedelta(hours=2),
        env_vars=fen_env,
    )
    trigger_label_dual = PythonOperator(
        task_id="trigger_label_dual_batch", python_callable=_trigger_label_dual_batch
    )
    should_continue = ShortCircuitOperator(
        task_id="should_continue", python_callable=_should_continue_crawl
    )
    trigger_next = PythonOperator(
        task_id="cooldown_and_trigger_next", python_callable=_cooldown_and_trigger_next
    )

    discover >> enrich >> download
    download >> trigger_label_dual
    download >> should_continue >> trigger_next
