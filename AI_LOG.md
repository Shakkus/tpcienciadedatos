# Bitácora de IA — NexoUrbano

Squad: Sebastian Albornoz, Mauricio Jurado, Alvaro Costa

Herramientas usadas (versiones): Cursor, Python 3.14, Docker Desktop, MinIO (Chainguard), `minio` Py SDK

Regla: una fila por sesión relevante. Mentir acá desaprueba el bloque A.

| Fecha | Módulo | Prompt (resumen) | Qué se aceptó | Qué se corrigió a mano | Qué falló |
|---|---|---|---|---|---|
| 2026-10-09 | M1 Discovery / 4 V | Completar fichas Netflix, red social e IoT mapeadas a NexoUrbano + arquitectura medallion | Estructura de tablas del enunciado, órdenes de magnitud diseño vs repo (semilla 67001), asignación V→motor | Revisar URL/dataset abierto real en ADR 000 cuando el squad lo baje; ajustar owners en acta | — |
| 2026-10-09 | Kick-off / M2 MinIO | Bloque D+E: bronze particionado, estructura repo, README squad | Script `tools/bronze_a_minio.py`, ADR 001, carpetas módulos, `.env.example` | Credenciales MinIO y puertos documentados a mano en README | Login MinIO / pull Hub resueltos con Chainguard + reset volumen |
| 2026-10-09 | M1 perfilado | Perfilar `Dataset_abierto_real.csv` ≥100k | `01-discovery/perfilado.py`, `contratos/dataset_abierto_real.yaml`, evidencia discovery | Encoding consola Windows en print del script | — |
| 2026-10-09 | M3 Silver / APIs | Ingesta re-ejecutable viajes × Open-Meteo + social, Parquet en MinIO silver, contratos y ADR 002 | Script `03-databricks/01_ingesta.py`, join por estación más cercana y hora UTC, fallback Mastodon, métricas y rechazo del contrato Bronze | `basename_template` de PyArrow exige `{i}`; stdout UTF-8; limpiar el prefijo Silver antes de resubir para no duplicar particiones; el log no lista los 91 Parquet | Bluesky `searchPosts` respondió 403; la API de precios Azure respondió 429 (el ADR cita lista pública, no una cotización) |
| 2026-10-09 | M4 Hadoop | Preparar entorno Hadoop reproducible con Docker para HDFS y MapReduce | Compose Hadoop 3.4.1, configuración HDFS/YARN y ADR 002 | Ajustar arranque para seleccionar solo servicios Hadoop; reducir memoria del AM y definir `HADOOP_MAPRED_HOME` tras smoke test | Arranque inicial activó servicios sin perfil; primer job falló por memoria y classpath; corregido y validado |
| 2026-10-10 | M10 MongoDB CRUD | Levantar contenedor Mongo, cargar productos.json, crear CRUD y medir índice con explain | Script crud.js, colección productos con documentos anidados, updateOne, deleteOne y export index_explain.txt | Ajustar el puerto del servicio Mongo en docker-compose.yml para que coincida con 27018 y validar el arranque antes del script | El contenedor se veía cerrado en 127.0.0.1:27018; el ajuste del compose y la re-ejecución del script lo resolvieron |
