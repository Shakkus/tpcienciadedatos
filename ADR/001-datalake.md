# ADR 001 — Data Lake (MinIO local, paridad ADLS)

## Decisión

Usar **MinIO** en Docker (`cgr.dev/chainguard/minio`) como landing zone medallion en desarrollo. En producción el mismo layout iría a **ADLS Gen2** (`abfss://`).

## Layout

| Bucket | Uso |
|--------|-----|
| `bronze` | Raw particionado |
| `silver` | Parquet/Delta post-ingesta (Databricks) |
| `gold` | Métricas Spark |

Bronze viajes (sintético NexoUrbano):

```text
s3a://bronze/viajes/yyyy/mm/dd/viajes.csv
```

Endpoint local:

- API S3: `http://localhost:9000`
- Consola: `http://localhost:9001`
- Credenciales: ver `docker-compose.yml` (`MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`)

## Ingesta

```bash
pip install minio
python tools/bronze_a_minio.py
```

Evidencia: `evidence/docker/minio_bronze_viajes.txt`

## Paridad cloud (resumen)

| Criterio | MinIO local | ADLS Gen2 prod |
|----------|-------------|----------------|
| Layout paths | `bronze/viajes/...` | Igual en contenedor `bronze` |
| IAM | root user + policy manual | Azure RBAC + ACLs |
| Costo 1 TB/mes | ~0 (disco local) | Calculador Azure (documentar en entrega) |

Recorte autorizado: enunciado §8 — Azure real → MinIO con ADR.
