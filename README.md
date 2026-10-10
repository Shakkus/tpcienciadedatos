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


```bash
python tools/bronze_a_minio.py
```

- **URI base:** `s3a://bronze/viajes/yyyy/mm/dd/viajes.csv`
- **ADR:** [ADR/001-datalake.md](ADR/001-datalake.md)
- **Evidencia:** [evidence/docker/minio_bronze_viajes.txt](evidence/docker/minio_bronze_viajes.txt)

### M5.3 WordCount en Hadoop

Con el clúster Hadoop iniciado y `texto_ops.txt` cargado en HDFS, ejecutar
WordCount desde la raíz del repositorio:

```bash
docker compose exec -T hadoop-namenode hadoop jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.1.jar wordcount /nexo/bronze/logs/texto_ops.txt /nexo/gold/wordcount
powershell -ExecutionPolicy Bypass -File .\04-hadoop\mapreduce\generar_top20.ps1
```

El directorio de salida HDFS debe no existir antes de lanzar el job. Antes de
ejecutar el script, exportar `part-r-00000` desde HDFS a
`04-hadoop/mapreduce/part-r-00000`. El script genera el top 20 a partir de ese
resultado completo, normalizando mayúsculas y hashtags, y excluyendo palabras
funcionales, IDs y nombres genéricos de entidad. Las salidas y el log están en
`04-hadoop/mapreduce/` y `evidence/hadoop/`, respectivamente.

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
