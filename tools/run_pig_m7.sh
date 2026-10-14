#!/usr/bin/env bash
set -euo pipefail

export MSYS_NO_PATHCONV=1
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$(cygpath -m "$REPO_ROOT/docker-compose.yml" 2>/dev/null || printf '%s' "$REPO_ROOT/docker-compose.yml")"
OUTPUT_ROOT="$REPO_ROOT/.staging/pig"
OUTPUT_NAME="clean_viajes_$(date +%Y%m%d_%H%M%S)"
OUTPUT_PATH="$OUTPUT_ROOT/$OUTPUT_NAME"
NULL_KM_NAME="null_km_validation_$(date +%Y%m%d_%H%M%S)"
NULL_KM_PATH="$OUTPUT_ROOT/$NULL_KM_NAME"
LOG_PATH="$REPO_ROOT/evidence/hadoop/pig.log"

mkdir -p "$OUTPUT_ROOT" "$(dirname "$LOG_PATH")"
printf 'M7 Pig local run\nOutput: %s\n' "$OUTPUT_NAME" > "$LOG_PATH"

docker compose -f "$COMPOSE_FILE" --profile pig build pig
docker compose -f "$COMPOSE_FILE" --profile pig run --rm pig \
  -x local \
  -param INPUT=/opt/nexo/data/viajes.csv \
  -param OUTPUT="/output/$OUTPUT_NAME" \
  /opt/nexo/04-hadoop/pig/clean_viajes.pig 2>&1 | tee -a "$LOG_PATH"

ACTUAL_ROWS="$(find "$OUTPUT_PATH" -type f -name 'part-*' -exec cat {} + | wc -l | tr -d '[:space:]')"
EXPECTED_ROWS="$(awk -F, 'NR > 1 && $4 >= 30 && $4 <= 14400 { count++ } END { print count + 0 }' "$REPO_ROOT/data/viajes.csv")"
printf 'Rows in clean_viajes: %s\nRows expected by duration filter: %s\n' \
  "$ACTUAL_ROWS" "$EXPECTED_ROWS" | tee -a "$LOG_PATH"

if [[ "$ACTUAL_ROWS" != "$EXPECTED_ROWS" ]]; then
  echo "Error: el número de filas Pig no coincide con el filtro de duración." >&2
  exit 1
fi

docker compose -f "$COMPOSE_FILE" --profile pig run --rm pig \
  -x local \
  -param INPUT=/opt/nexo/04-hadoop/pig/testdata/km_null.csv \
  -param OUTPUT="/output/$NULL_KM_NAME" \
  /opt/nexo/04-hadoop/pig/clean_viajes.pig 2>&1 | tee -a "$LOG_PATH"

NULL_KM_FILE="$(find "$NULL_KM_PATH" -type f -name 'part-*' -print -quit)"
if [[ -z "$NULL_KM_FILE" ]] || ! awk -F '\t' 'NR != 1 { exit 1 } $15 <= 0 || $15 >= 20000 { exit 1 } END { if (NR != 1) exit 1 }' "$NULL_KM_FILE"; then
  echo "Error: Pig no calculó una distancia válida para el km nulo de prueba." >&2
  exit 1
fi

CALCULATED_KM="$(awk -F '\t' 'NR == 1 { print $15 }' "$NULL_KM_FILE")"
printf 'Null-km fixture: calculated km=%s (valid)\n' "$CALCULATED_KM" | tee -a "$LOG_PATH"
