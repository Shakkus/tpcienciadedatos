#!/usr/bin/env bash
# M7 — plan B de Sqoop: importar crm.riders a HDFS mediante Spark JDBC.
set -euo pipefail

export MSYS_NO_PATHCONV=1
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DOCKER_REPO_ROOT="$(cygpath -m "$REPO_ROOT")"
OUTPUT_PATH="${1:-hdfs://namenode:9000/nexo/bronze/riders_sqoop}"
NAMENODE_ID="$(docker compose -f "$DOCKER_REPO_ROOT/docker-compose.yml" ps -q hadoop-namenode)"
MYSQL_ID="$(docker compose -f "$DOCKER_REPO_ROOT/docker-compose.yml" ps -q mysql)"

if [[ -z "$NAMENODE_ID" ]]; then
  echo "Error: Hadoop no está iniciado. Arrancá MySQL y el perfil Hadoop con docker compose." >&2
  exit 1
fi

if [[ -z "$MYSQL_ID" ]]; then
  echo "Error: MySQL no está iniciado. Arrancá el servicio mysql con docker compose." >&2
  exit 1
fi

PROJECT_NAME="${COMPOSE_PROJECT_NAME:-$(basename "$REPO_ROOT")}"
NETWORK="${PROJECT_NAME}_default"
if ! docker network inspect "$NETWORK" >/dev/null 2>&1; then
  echo "Error: no existe la red Docker $NETWORK del proyecto Compose." >&2
  exit 1
fi

LOG_PATH="$REPO_ROOT/evidence/hadoop/sqoop.log"
mkdir -p "$(dirname "$LOG_PATH")"

docker run --rm --network "$NETWORK" \
-v "$DOCKER_REPO_ROOT:/opt/nexo" \
  -w /opt/nexo \
  -e HADOOP_CONF_DIR=/opt/nexo/04-hadoop/config \
  -e MYSQL_HOST=mysql \
  -e MYSQL_PORT=3306 \
  -e MYSQL_DATABASE="${MYSQL_DATABASE:-crm}" \
  -e MYSQL_USER="${MYSQL_USER:-root}" \
  -e MYSQL_PASSWORD="${MYSQL_PASSWORD:-nexo}" \
  apache/spark:3.5.3 \
  /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages com.mysql:mysql-connector-j:8.4.0 \
  /opt/nexo/04-hadoop/sqoop/import_riders_spark.py \
  --output "$OUTPUT_PATH" 2>&1 | tee "$LOG_PATH"
