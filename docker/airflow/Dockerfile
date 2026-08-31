FROM apache/airflow:2.10.5-python3.12

USER root
# Docker CLI for fen-job via compose / CLI Docker để gọi fen-job
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
COPY dags/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    apache-airflow-providers-docker==3.14.0

ENV PYTHONPATH=/opt/fen-exam/dags/jobs
