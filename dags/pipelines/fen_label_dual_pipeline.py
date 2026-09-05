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
    "start_date": datetime(2026, 8, 31),
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
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
    dag_id="fen_label_dual_pipeline",
    default_args=DEFAULT_ARGS,
    description="Label dual pilot: Gemini ∥ Paddle → fuse → GLM → task_b2.jsonl (fuse_gt)",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["fen-exam", "ocr", "label-dual", "docker"],
    params={
        "group_id": Param(default=DEFAULT_GROUP_ID, type="string"),
        "batch_seq": Param(
            default=0,
            type="integer",
            minimum=0,
            description="Quote shard 1–12; 0 = all shards (not crawl batch_seq)",
        ),
        "prepare_force": Param(
            default=False,
            type="boolean",
            description="Force rewrite quote_01..12 even if image set unchanged",
        ),
        "label_limit": Param(
            default=0,
            type="integer",
            minimum=0,
            description="Max pending images; 0 = full queue (maps to unlimited target)",
        ),
        "flush_posts": Param(default=5, type="integer", minimum=1),
        "force": Param(default=False, type="boolean"),
        "replay": Param(default=False, type="boolean"),
        "glm": Param(default=True, type="boolean"),
        "prepare_queues": Param(default=True, type="boolean"),
        "workers": Param(
            default=4,
            type="integer",
            minimum=1,
            description="Parallel workers (exam Docker: 4 is safe for single Paddle)",
        ),
    },
) as dag:
    label_env = {
        "FEN_GROUP_ID": _param_expr("group_id"),
        "FEN_SOURCE_PREFIX": "facebook",
        "FEN_LABEL_BATCH_SEQ": _param_expr("batch_seq"),
        "FEN_LABEL_LIMIT": _param_expr("label_limit"),
        "FEN_LABEL_TARGET": "0",
        "FEN_LABEL_UNLIMITED": (
            "{% if (dag_run.conf.get('label_limit', params.label_limit) "
            "if dag_run and dag_run.conf else params.label_limit) | int == 0 %}"
            "true{% else %}false{% endif %}"
        ),
        "FEN_LABEL_FLUSH_POSTS": _param_expr("flush_posts"),
        "FEN_LABEL_FORCE": _bool_expr("force"),
        "FEN_LABEL_REPLAY": _bool_expr("replay"),
        "FEN_LABEL_GLM": _bool_expr("glm"),
        "FEN_LABEL_PREPARE_QUEUES": _bool_expr("prepare_queues"),
        "FEN_LABEL_PREPARE_FORCE": _bool_expr("prepare_force"),
        "FEN_LABEL_WORKERS": _param_expr("workers"),
        "FEN_LABEL_VISION_MODEL": "gemini-3.6-flash-high",
        "FEN_LABEL_GPT_MODEL": "gpt-5.6-luna",
        "FEN_LABEL_GLM_MODEL": "glm-5.3-flash",
        # Empty disables DeepSeek permute / Chuỗi rỗng = tắt DeepSeek permute
        "FEN_LABEL_EVAL_MODEL": "",
        "FEN_PADDLE_OCR_URL": "http://paddle-ocr:8080/ocr",
        "FEN_PADDLE_TIMEOUT_SEC": "600",
        "FEN_PADDLE_MAX_INFLIGHT": "2",
        "FEN_MINIO_ENDPOINT": "http://minio:9000",
    }
    build_fen_job_task(
        task_id="run_fen_label_dual",
        job_name="fen_label_dual",
        execution_timeout=timedelta(hours=12),
        env_vars=label_env,
    )
