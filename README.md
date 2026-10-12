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
| MySQL | 3307 | DB `crm`, root password `nexo` |
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
### 5. Spark batch Gold (M8)

Requiere **JDK 17+** en `PATH` para correr local:

```bash
python 05-spark/batch/job_gold.py --source minio
```

Sin Java en el host (Windows): `bash tools/run_job_gold_docker.sh` (imagen `apache/spark:3.5.3`, MinIO en `host.docker.internal:9000`).

- **Input:** `s3a://silver/viajes_clima/` (contrato [contratos/silver_viajes.yaml](contratos/silver_viajes.yaml))
- **Output:** `s3a://gold/ops_diaria/`, `gold/top_estaciones/`, `gold/repuesto_lluvia/`
- **Local:** `05-spark/batch/output/` · export pregunta 1: `09-producto/gold_repuesto_lluvia.csv`
- **Evidencia:** [evidence/spark/batch_explain.txt](evidence/spark/batch_explain.txt)

### 4. Silver (M3, viajes × clima)

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
4. Sqoop nativo → **Spark JDBC** (plan B M7 implementado y verificado en HDFS; ver ADR)

## Estructura del repo

Ver [TP_NexoUrbano_2026.md](TP_NexoUrbano_2026.md) §3. Carpetas de módulos creadas vacías; cada owner trabaja en su rama `feat/...`.

| Owner | Módulos |
|-------|---------|
| Sebastian Albornoz | Lake, Databricks, Spark, contratos viajes |
| Mauricio Jurado | Hadoop, HDFS, MR, Hive, HBase, Pig, Flume/JDBC |
| Alvaro Costa | Mongo, Cassandra, Neo4j, agregaciones producto |

### M7 — ejecutar Pig en Docker

No hace falta instalar Pig localmente; desde la raíz del repositorio:

```bash
bash tools/run_pig_m7.sh
```

El comando crea la imagen `nexo/pig:0.18.0-hadoop3.4.1`, ejecuta
`clean_viajes.pig` en modo local y valida el conteo de filas. Salida:
`.staging/pig/`; también prueba el cálculo de `km` nulo con un fixture.
Log y conteos: `evidence/hadoop/pig.log`.

### M7 — Flume y simulación reproducible

Con Hadoop iniciado, ejecutar desde la raíz:

```bash
python tools/simulate_flume_200.py
```

La simulación publica 200 eventos JSON (`ride_start`, `ride_end`,
`payment_fail`) en HDFS y compara el archivo persistido con lo enviado. La
configuración del agente real está en `04-hadoop/flume/flume.conf`; el script
simula el envío al sink HDFS y no requiere instalar Flume en la máquina.
Resultado: `evidence/hadoop/flume.log`. El contraste de ocho líneas entre
Flume, Structured Streaming y Kafka está en [ADR/002-hadoop-hoy.md](ADR/002-hadoop-hoy.md).

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
