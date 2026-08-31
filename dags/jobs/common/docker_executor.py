"""Docker Compose job executor for Airflow (replaces KubernetesPodOperator).
Executor job Docker Compose cho Airflow (thay KubernetesPodOperator).
"""
from __future__ import annotations

import os
from datetime import timedelta
from typing import Any

from docker.types import Mount  # type: ignore

from common.config import get_value, load_config

# pyrefly: ignore [missing-import]
from airflow.providers.docker.operators.docker import DockerOperator  # type: ignore


def _docker_settings() -> dict[str, str]:
    cfg = load_config()
    host_project = os.environ.get("FEN_HOST_PROJECT_DIR", "").strip()
    project_dir = host_project or get_value(
        cfg, "docker", "project_dir", fallback="/opt/fen-exam"
    )
    return {
        "image": get_value(cfg, "docker", "fen_job_image", fallback="fen-exam-fen-job"),
        "compose_project": get_value(cfg, "docker", "compose_project", fallback="fen-exam"),
        "project_dir": project_dir,
    }


def build_fen_job_task(
    *,
    task_id: str,
    job_name: str,
    env_vars: dict[str, str] | None = None,
    execution_timeout: timedelta | None = None,
    **operator_kwargs: Any,
) -> DockerOperator:
    """Run fen-job container via DockerOperator / Chạy container fen-job qua DockerOperator."""
    settings = _docker_settings()
    environment = {
        "FEN_JOB": job_name,
        "FEN_CONFIG_PATH": "/opt/fen-exam/dags/config.ini",
        "FEN_SKIP_LOCAL_OUTPUT": "true",
        "FEN_PATHS_OUTPUT_DIR": "/tmp/fen-output",
        **(env_vars or {}),
    }
    return DockerOperator(
        task_id=task_id,
        image=settings["image"],
        api_version="auto",
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        network_mode=f"{settings['compose_project']}_default",
        environment=environment,
        mounts=[
            Mount(
                source=f"{settings['project_dir']}/dags",
                target="/opt/fen-exam/dags",
                type="bind",
                read_only=True,
            ),
        ],
        command=[],
        mount_tmp_dir=False,
        execution_timeout=execution_timeout,
        **operator_kwargs,
    )
