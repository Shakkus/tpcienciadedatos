#!/usr/bin/env bash
# M8 — ejecutar job_gold.py con Java/Spark en Docker (Git Bash / WSL).
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export MSYS_NO_PATHCONV=1
docker run --rm --user root \
  --add-host=host.docker.internal:host-gateway \
  -v "${REPO_ROOT}:/opt/nexo" \
  -w /opt/nexo \
  -e MINIO_ENDPOINT=host.docker.internal:9000 \
  -e MYSQL_HOST=host.docker.internal \
  -e MYSQL_PORT=3307 \
  apache/spark:3.5.3 \
  bash -c "pip3 install -q minio && /opt/spark/bin/spark-submit --driver-memory 2g --packages com.mysql:mysql-connector-j:8.4.0 /opt/nexo/05-spark/batch/job_gold.py --source minio"
