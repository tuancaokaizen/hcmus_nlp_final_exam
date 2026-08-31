FROM python:3.12-slim

WORKDIR /opt/fen-exam

RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 libgl1 curl \
    && rm -rf /var/lib/apt/lists/*

COPY dags/requirements.txt /opt/fen-exam/dags/requirements.txt
RUN pip install --no-cache-dir -r /opt/fen-exam/dags/requirements.txt

COPY dags/jobs /opt/fen-exam/dags/jobs

ENV PYTHONPATH=/opt/fen-exam/dags/jobs
ENV FEN_CONFIG_PATH=/opt/fen-exam/dags/config.ini
ENV FEN_SKIP_LOCAL_OUTPUT=true
ENV FEN_PATHS_OUTPUT_DIR=/tmp/fen-output

WORKDIR /opt/fen-exam/dags/jobs
ENTRYPOINT ["python", "run_job.py"]
