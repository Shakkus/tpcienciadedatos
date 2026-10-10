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
pip install -r requirements.txt
python tools/bronze_a_minio.py
python 03-databricks/01_ingesta.py
```

Evidencia bronze: `evidence/docker/minio_bronze_viajes.txt`  
Evidencia silver: `evidence/databricks/01_ingesta_log.txt`

Silver viajes × clima:

```text
s3a://silver/viajes_clima/fecha=yyyy-mm-dd/part-0.parquet
```

## Paridad cloud

Fecha de la nota: **2026-10-09**. No hay cuenta Azure (recorte §8). La [API de precios retail](https://prices.azure.com/api/retail/prices) respondió **429** ese día, así que no hay cotización en vivo. Los números de cloud son orden de magnitud de lista pública, para confirmar en el [calculador](https://azure.microsoft.com/en-us/pricing/calculator/) eligiendo Storage, Hot, LRS, región de la suscripción.

| Criterio | MinIO local (medido o real) | ADLS Gen2 prod (estimado) |
|----------|-----------------------------|---------------------------|
| Layout | `s3a://` en buckets bronze, silver y gold | Mismo path en contenedores homónimos (`abfss://`) |
| Costo 1 TB/mes | ~0 (disco de la máquina) | Hot LRS, orden de **USD 0,021/GB** → **~USD 21,5 / TB / mes** ([resumen 2026](https://www.nops.io/blog/azure-storage-pricing/); lista oficial en [ADLS](https://azure.microsoft.com/en-us/pricing/details/storage/data-lake/)) |
| Egress | 0 (no sale de localhost) | Primeros **100 GB/mes** de salida a internet sin cargo; después, zona 1 (América del Norte / Europa), orden de **USD 0,087/GB**. Lectura del tier Hot dentro de la región: retrieval USD 0. Confirmar en [Bandwidth](https://azure.microsoft.com/en-us/pricing/details/bandwidth/) |
| IAM | un root user (`MINIO_ROOT_USER`) | Azure RBAC en la cuenta + ACL POSIX del hierarchical namespace. En prod no hay un root compartido |
| TTFB | GET de 64 bytes a `silver/viajes_clima/.../part-0.parquet`: **2,1 ms** (`evidence/databricks/01_ingesta_log.txt`) | Misma región: decenas de ms. No medido contra Azure |

Recorte autorizado: enunciado §8 — Azure real → MinIO con este ADR. En producción cambia el esquema de URI (`s3a` → `abfss`), el identity (RBAC) y el costo de egress; el layout de carpetas no.
