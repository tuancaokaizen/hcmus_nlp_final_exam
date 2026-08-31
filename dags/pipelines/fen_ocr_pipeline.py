from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG  # noqa: F401
from airflow.models.param import Param  # type: ignore

_JOBS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "jobs"))
if _JOBS_DIR not in sys.path:
    sys.path.append(_JOBS_DIR)

from common.docker_executor import build_fen_job_task  # type: ignore

DEFAULT_ARGS = {
    "owner": "fen-exam",
    "depends_on_past": False,
    "start_date": datetime(2026, 8, 22),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
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


with DAG(
    dag_id="fen_ocr_pipeline",
    default_args=DEFAULT_ARGS,
    description="OCR text for one crawl batch (Gemini via fen_ocr API key)",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["fen-exam", "ocr", "docker"],
    params={
        "group_id": Param(default=DEFAULT_GROUP_ID, type="string"),
        "batch_seq": Param(default=0, type="integer"),
        "ocr_limit": Param(
            default=0,
            type="integer",
            minimum=0,
            description="Max posts per run; 0 = all posts in batch queue / export",
        ),
        "force": Param(default=False, type="boolean"),
    },
) as dag:
    ocr_env = {
        "FEN_GROUP_ID": _param_expr("group_id"),
        "FEN_OCR_BATCH_SEQ": _param_expr("batch_seq"),
        "FEN_OCR_LIMIT": _param_expr("ocr_limit"),
        "FEN_OCR_FORCE": _bool_expr("force"),
        "FEN_MINIO_ENDPOINT": "http://minio:9000",
    }
    ocr = build_fen_job_task(
        task_id="run_fen_ocr",
        job_name="fen_ocr",
        execution_timeout=timedelta(hours=4),
        env_vars=ocr_env,
    )
    retry = build_fen_job_task(
        task_id="run_fen_ocr_retry",
        job_name="fen_ocr_retry",
        execution_timeout=timedelta(hours=2),
        env_vars={
            "FEN_GROUP_ID": _param_expr("group_id"),
            "FEN_OCR_BATCH_SEQ": _param_expr("batch_seq"),
            "FEN_OCR_RETRY_LIMIT": "0",
            "FEN_MINIO_ENDPOINT": "http://minio:9000",
        },
    )
    ocr >> retry
