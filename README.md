# NexoUrbano 2026 — Squad (grupo 67)

Ciudad: **Buenos Aires** · Semilla datos sintéticos: **`67001`** · Contexto: [ADR/000-contexto.md](ADR/000-contexto.md)

Integrantes: Sebastian Albornoz, Mauricio Jurado, Alvaro Costa.

**Cola de trabajo por persona:** [PENDIENTES_POR_INTEGRANTE.md](PENDIENTES_POR_INTEGRANTE.md)

## Arranque local (< 20 min)

### 1. Datos

```bash
python tools/generar_datos.py --seed 67001 --n-viajes 250000
```

Dataset abierto real (M1): `data/Dataset_abierto_real.csv` (perfilado en `01-discovery/perfilado.py`).

### 2. Docker (mínimo kick-off)

```bash
docker compose up -d mysql mongo minio
docker compose ps
```

| Servicio | Puerto host | Notas |
|----------|-------------|--------|
| MinIO API | 9000 | S3 |
| MinIO consola | 9001 | usuario/clave en `docker-compose.yml` |
| MySQL | 3306 | DB `crm`, root password `nexo` |
| Mongo | 27018 | Un NoSQL a la vez si tenés 8 GB RAM |

```bash
cp .env.example .env
pip install -r requirements.txt
```

### 3. Bronze en el lake (MinIO)

```bash
python tools/bronze_a_minio.py
```

- **URI base:** `s3a://bronze/viajes/yyyy/mm/dd/viajes.csv`
- **ADR:** [ADR/001-datalake.md](ADR/001-datalake.md)
- **Evidencia:** [evidence/docker/minio_bronze_viajes.txt](evidence/docker/minio_bronze_viajes.txt)

### 4. Silver (M3, viajes × clima)

```bash
python 03-databricks/01_ingesta.py
```

- **URI:** `s3a://silver/viajes_clima/fecha=yyyy-mm-dd/part-0.parquet`
- **Contrato:** [contratos/silver_viajes.yaml](contratos/silver_viajes.yaml) (posts: [contratos/silver_social.yaml](contratos/silver_social.yaml))
- **ADR:** [ADR/002-databricks-apis.md](ADR/002-databricks-apis.md)
- **Evidencia:** [evidence/databricks/01_ingesta_log.txt](evidence/databricks/01_ingesta_log.txt)

## Recortes de alcance (enunciado §8)

Documentados en [ADR/000-contexto.md](ADR/000-contexto.md):

1. **Azure ADLS → MinIO** (Chainguard) + [ADR/001-datalake.md](ADR/001-datalake.md)
2. Imágenes Docker desde **public.ecr.aws** (Docker Hub inestable)
3. Spark batch objetivo **250k** filas (`viajes.csv`); dataset abierto = usuarios EcoBici (M1)
4. Sqoop nativo → **Spark JDBC** (pendiente M7, ADR al implementar)

## Estructura del repo

Ver [TP_NexoUrbano_2026.md](TP_NexoUrbano_2026.md) §3. Carpetas de módulos creadas vacías; cada owner trabaja en su rama `feat/...`.

| Owner | Módulos |
|-------|---------|
| Sebastian Albornoz | Lake, Databricks, Spark, contratos viajes |
| Mauricio Jurado | Hadoop, HDFS, MR, Hive, HBase, Pig, Flume/JDBC |
| Alvaro Costa | Mongo, Cassandra, Neo4j, agregaciones producto |

## Kick-off completado

- [x] M1: `01-discovery/4V.md`, `arquitectura.md`, perfilado dataset abierto
- [x] Contratos: `contratos/bronze_viajes.yaml`, `contratos/dataset_abierto_real.yaml`
- [x] MinIO bronze viajes particionado
- [ ] Video demo 8 min (link aquí al cerrar)
- [ ] Tag `entrega-final` en `main`

Checklist docente: [CHECKLIST_ENTREGA.md](CHECKLIST_ENTREGA.md)

## Git

- `main` solo por PR; un commit ≈ un módulo.
- Cada PR: código + `evidence/` + fila en [AI_LOG.md](AI_LOG.md).

## Referencia cátedra

[TP_NexoUrbano_2026.md](TP_NexoUrbano_2026.md)
