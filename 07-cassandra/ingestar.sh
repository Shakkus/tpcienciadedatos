#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
DATASET="${REPO_ROOT}/data/lecturas_sensores.csv"
SCHEMA="${SCRIPT_DIR}/schema.cql"
LOAD="${SCRIPT_DIR}/load.cql"

echo "Iniciando ingesta de Cassandra (M12)..."

if [[ ! -f "${DATASET}" ]]; then
  echo "No se encontró el dataset: ${DATASET}" >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "Docker no está disponible o no está corriendo." >&2
  exit 1
fi

echo "Levantando contenedor Cassandra..."
docker compose -f "${REPO_ROOT}/docker-compose.yml" up -d cassandra

echo "Esperando que Cassandra esté listo..."
for i in $(seq 1 30); do
  if docker exec nexo-cassandra cqlsh -e "DESCRIBE KEYSPACES;" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "Copiando CSV al contenedor..."
MSYS_NO_PATHCONV=1 docker cp "${DATASET}" nexo-cassandra:/tmp/lecturas_sensores.csv

echo "Creando esquema..."
docker exec -i nexo-cassandra cqlsh < "${SCHEMA}"

echo "Cargando datos..."
docker exec -i nexo-cassandra cqlsh < "${LOAD}"

echo "Ingesta finalizada correctamente."
