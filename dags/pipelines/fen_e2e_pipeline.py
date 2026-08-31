from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG  # noqa: F401
from airflow.models.param import Param  # type: ignore
from airflow.operators.python import BranchPythonOperator, PythonOperator  # type: ignore

_JOBS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "jobs"))
if _JOBS_DIR not in sys.path:
    sys.path.append(_JOBS_DIR)

from common.docker_executor import build_fen_job_task  # type: ignore

DEFAULT_ARGS = {
    "owner": "fen-exam",
    "depends_on_past": False,
    "start_date": datetime(2026, 8, 31),
    "retries": 0,
}

DEFAULT_GROUP_ID = "322453387859386"


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


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _resolve_group_id(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run else None) or {}
    params = context.get("params") or {}
    return str(conf.get("group_id") or params.get("group_id") or DEFAULT_GROUP_ID)


def _choose_fb_login(**context) -> str:
    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run else None) or {}
    params = context.get("params") or {}
    run_login = conf.get("run_fb_login", params.get("run_fb_login", False))
    return "fb_login" if _truthy(run_login) else "crawl_discover"


def _resolve_ocr_batch(**context) -> int:
    from fen_crawl_common import ensure_raw_bucket, get_active_batch

    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run else None) or {}
    params = context.get("params") or {}
    group_id = _resolve_group_id(**context)
    batch_seq = int(conf.get("batch_seq") or params.get("batch_seq") or 0)
    if batch_seq <= 0:
        bucket, source_prefix = ensure_raw_bucket()
        active = get_active_batch(bucket, source_prefix, group_id)
        batch_seq = int(active.get("batch_seq") or 0)
    if batch_seq <= 0:
        raise ValueError(f"No active crawl batch for group_id={group_id}")
    context["ti"].xcom_push(key="batch_seq", value=str(batch_seq))
    print(f"[fen_e2e] ocr batch_seq={batch_seq} group_id={group_id}", flush=True)
    return batch_seq


with DAG(
    dag_id="fen_e2e_pipeline",
    default_args=DEFAULT_ARGS,
    description="E2E: optional fb-login → crawl batch → label dual (fuse_gt)",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["fen-exam", "e2e", "docker"],
    params={
        "group_id": Param(default=DEFAULT_GROUP_ID, type="string"),
        "batch_target": Param(default=10, type="integer"),
        "ocr_limit": Param(
            default=0,
            type="integer",
            minimum=0,
            description="Max pending images for label dual; 0 = full queue",
        ),
        "flush_posts": Param(
            default=5,
            type="integer",
            minimum=1,
            description="Upsert task_b2.jsonl every N images (label dual flush)",
        ),
        "batch_seq": Param(
            default=0,
            type="integer",
            description="OCR batch; 0 = auto from active crawl batch",
        ),
        "reset_crawl_data": Param(default=False, type="boolean"),
        "run_fb_login": Param(default=False, type="boolean"),
    },
) as dag:
    base_env = {
        "FEN_GROUP_ID": _param_expr("group_id"),
        "FEN_BATCH_TARGET": _param_expr("batch_target"),
        "FEN_CRAWL_MODE": "split",
        # Headed Chrome required for CDP GraphQL sniffer on FB /
        # Chrome headed bắt buộc cho CDP sniffer GraphQL trên FB
        "FEN_HEADLESS": "false",
        "FEN_UPLOAD_MINIO": "true",
        "FEN_RESET_CRAWL_DATA": _bool_expr("reset_crawl_data"),
        "FEN_MINIO_ENDPOINT": "http://minio:9000",
        "SELENIUM_REMOTE_URL": "http://selenium-chrome:4444/wd/hub",
    }

    branch_fb_login = BranchPythonOperator(
        task_id="branch_fb_login",
        python_callable=_choose_fb_login,
    )

    fb_login = build_fen_job_task(
        task_id="fb_login",
        job_name="fen_bootstrap_login",
        execution_timeout=timedelta(minutes=20),
        env_vars={
            **base_env,
            "FB_USERNAME": "{{ var.value.get('FB_USERNAME', '') }}",
            "FB_PASSWORD": "{{ var.value.get('FB_PASSWORD', '') }}",
        },
    )

    discover = build_fen_job_task(
        task_id="crawl_discover",
        job_name="fen_crawl_discover",
        execution_timeout=timedelta(hours=4),
        env_vars=base_env,
        # Branch skips fb_login → discover must not require all upstream success /
        # Branch bỏ fb_login → discover không được đòi all_success
        trigger_rule="none_failed_min_one_success",
    )
    enrich = build_fen_job_task(
        task_id="crawl_enrich",
        job_name="fen_crawl_enrich",
        execution_timeout=timedelta(hours=2),
        env_vars=base_env,
    )
    download = build_fen_job_task(
        task_id="crawl_download",
        job_name="fen_crawl_download",
        execution_timeout=timedelta(hours=2),
        env_vars=base_env,
    )

    resolve_ocr_batch = PythonOperator(
        task_id="resolve_ocr_batch",
        python_callable=_resolve_ocr_batch,
    )

    label_dual = build_fen_job_task(
        task_id="run_label_dual",
        job_name="fen_label_dual",
        execution_timeout=timedelta(hours=12),
        env_vars={
            "FEN_GROUP_ID": _param_expr("group_id"),
            # Quote queues 1–12 are shards of ALL valid images — not crawl batch_seq /
            # Queue quote 1–12 là shard toàn bộ ảnh valid — không phải crawl batch_seq
            "FEN_LABEL_BATCH_SEQ": "0",
            "FEN_LABEL_LIMIT": _param_expr("ocr_limit"),
            "FEN_LABEL_TARGET": "0",
            "FEN_LABEL_UNLIMITED": (
                "{% if (dag_run.conf.get('ocr_limit', params.ocr_limit) "
                "if dag_run and dag_run.conf else params.ocr_limit) | int == 0 %}"
                "true{% else %}false{% endif %}"
            ),
            "FEN_LABEL_PREPARE_QUEUES": "true",
            "FEN_LABEL_GLM": "true",
            "FEN_LABEL_FLUSH_POSTS": _param_expr("flush_posts"),
            "FEN_LABEL_WORKERS": "4",
            "FEN_PADDLE_OCR_URL": "http://paddle-ocr:8080/ocr",
            "FEN_MINIO_ENDPOINT": "http://minio:9000",
        },
    )

    branch_fb_login >> [fb_login, discover]
    fb_login >> discover
    discover >> enrich >> download >> resolve_ocr_batch >> label_dual
