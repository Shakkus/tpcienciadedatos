#!/usr/bin/env bash
# M9 — Structured Streaming gold_alerts con Spark en Docker (Git Bash / WSL).
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export MSYS_NO_PATHCONV=1
docker run --rm --user root \
  --add-host=host.docker.internal:host-gateway \
  -v "${REPO_ROOT}:/opt/nexo" \
  -w /opt/nexo \
  -e TZ=UTC \
  -e PYTHONUNBUFFERED=1 \
  apache/spark:3.5.3 \
  bash -c "/opt/spark/bin/spark-submit --driver-memory 1g /opt/nexo/05-spark/streaming/stream_alerts.py"
