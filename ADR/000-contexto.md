# ADR 000 — Contexto del squad

- Ciudad de operación: Buenos Aires (generador `tools/generar_datos.py`, `data/MANIFIESTO.txt`)
- Dataset abierto de movilidad (URL + fecha de descarga):https://www.kaggle.com/datasets/martnalegre/usuarios-ecobici-ciudad-de-buenos-aires-2019-2025 Fecha de descarga: 9/10/2026
- Semilla `--seed`: 67001 (67 * 100 + 1)
- Integrantes y owner por módulo: 

| Módulo | Owner | Backup |
|---|---|---|
| Discovery / 4 V | Sebastian Albornoz - Alvaro Costa - Mauricio Jurado| |
| Azure Data Lake |Sebastian Albornoz | |
| Databricks / APIs | Sebastian Albornoz | |
| Hadoop (HDFS, MR, Hive, HBase, Pig, Sqoop, Flume) | Mauricio Jurado | |
| Spark batch + streaming | Sebastian Albornoz | |
| MongoDB | Alvaro Costa | |
| Cassandra |Alvaro Costa | |
| Neo4j |Alvaro Costa | |
| Producto Gold + demo | Sebastian Albornoz - Alvaro Costa - Mauricio Jurado | |

## Recortes de alcance (si aplica)

Enunciado **§8** (orden aplicado):

1. **Azure ADLS Gen2 → MinIO local** (`cgr.dev/chainguard/minio`). Paridad en [ADR/001-datalake.md](001-datalake.md).
2. **Imágenes Docker Hub → `public.ecr.aws/docker/library/*`** por fallos de pull en `auth.docker.io`.
3. **Sqoop nativo → Spark JDBC** (plan B M7; ADR al entregar import CRM).
4. Sin Kafka; sin Bloom Neo4j; sin geo Mongo (extras opcionales).
5. Dataset abierto M1 = usuarios EcoBici (Kaggle); Bronze operativo de viajes = `data/viajes.csv` generado (`bronze_viajes.yaml`).
