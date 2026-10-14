# NexoUrbano 2026 — Squad (grupo 67)

Ciudad: **Buenos Aires** · Semilla: **`67001`** · Integrantes: Sebastian Albornoz, Mauricio Jurado, Alvaro Costa.

Contexto: [ADR/000-contexto.md](ADR/000-contexto.md) · Consigna: [TP_NexoUrbano_2026.md](TP_NexoUrbano_2026.md) · Checklist: [CHECKLIST_ENTREGA.md](CHECKLIST_ENTREGA.md)

Video demo 8 min: _link pendiente_.

En una máquina de 8 GB no levantar Hadoop, Cassandra y Neo4j a la vez. MinIO y MySQL pueden quedar arriba durante todo el recorrido. Los comandos se corren desde la raíz del repo, en Git Bash.

## 0. Preparar el entorno

Hacen falta Docker Desktop, Python 3.11+ y Node.js (Mongo y el export de Cassandra). Java 17 solo si Spark corre en el host; si no, los scripts de Docker alcanzan.

```bash
cp .env.example .env
pip install -r requirements.txt
npm install mongodb cassandra-driver
```

Los CSV ya están en `data/` (semilla `67001`, 250 000 viajes). Regenerarlos solo si hay que rehacer la semilla:

```bash
python tools/generar_datos.py --seed 67001 --n-viajes 250000
```

Perfilado del dataset abierto (M1, ya corrido): `python 01-discovery/perfilado.py`.

## 1. Lake: MySQL, MinIO y Silver

```bash
docker compose up -d mysql minio
python tools/bronze_a_minio.py
python 03-databricks/01_ingesta.py
```

La ingesta necesita red: Open-Meteo y, si Bluesky responde 403, Mastodon. Escribe Parquet en `s3a://silver/viajes_clima/`. El paso 5 la lee.

| Servicio | Puerto | Acceso |
|----------|--------|--------|
| MinIO API | 9000 | `s3a://` |
| MinIO consola | 9001 | `nexo` / `nexo-urban-2026` |
| MySQL | **3307** | DB `crm`, user `root`, password `nexo` |

- URI Bronze: `s3a://bronze/viajes/yyyy/mm/dd/viajes.csv`
- ADR: [ADR/001-datalake.md](ADR/001-datalake.md)
- Evidencia: [evidence/docker/minio_bronze_viajes.txt](evidence/docker/minio_bronze_viajes.txt), [evidence/databricks/01_ingesta_log.txt](evidence/databricks/01_ingesta_log.txt)
Con el clúster Hadoop iniciado y `texto_ops.txt` cargado en HDFS, ejecutar
WordCount desde la raíz del repositorio:

### M6. Hive sobre HDFS

Levantar Hadoop y Hive con perfiles opcionales, cargar el CSV completo en una
ruta aislada de M6 y ejecutar HiveQL (PowerShell desde la raíz):

```powershell
docker compose --profile hadoop --profile hive up -d hadoop-namenode hadoop-datanode hadoop-resourcemanager hadoop-nodemanager hive
docker cp .\data\viajes.csv nexo-hadoop-namenode:/tmp/m6_viajes.csv
docker compose exec -T hadoop-namenode hdfs dfs -mkdir -p /nexo/bronze/m6_viajes
docker compose exec -T hadoop-namenode hdfs dfs -put -f /tmp/m6_viajes.csv /nexo/bronze/m6_viajes/viajes.csv
docker cp .\04-hadoop\hive\hive_nexo.sql nexo-hive:/tmp/hive_nexo.sql
docker compose exec -T hive beeline -u jdbc:hive2://localhost:10000/default -f /tmp/hive_nexo.sql
```

El HiveServer2 queda en `localhost:10000`. Para probar `DROP`, ejecutar primero
`DROP TABLE bronze_viajes;` y comprobar que `/nexo/bronze/m6_viajes/viajes.csv`
sigue en HDFS; luego ejecutar `DROP TABLE silver_viajes;` y comprobar que
`/nexo/warehouse/silver_viajes/` ya no existe. El SQL de la demostración está en
[`04-hadoop/hive/hive_drop_demo.sql`](04-hadoop/hive/hive_drop_demo.sql);
scripts y resultados están en [`04-hadoop/hive/`](04-hadoop/hive) y
[`evidence/hadoop/`](evidence/hadoop).

### 5. Spark batch Gold (M8)

No hay workspace Databricks. `03-databricks/01_ingesta.py` es el job re-ejecutable (ciudad y fechas al inicio del archivo). Ver [ADR/002-databricks-apis.md](ADR/002-databricks-apis.md).

## 2. Hadoop (HDFS y WordCount)

Apagar Cassandra y Neo4j si estaban arriba. El clúster va en el profile `hadoop` (Hadoop 3.4.1: HDFS + YARN).

```bash
docker compose stop cassandra neo4j
docker compose --profile hadoop up -d
docker compose exec -T hadoop-namenode hdfs dfs -mkdir -p /nexo/bronze/viajes /nexo/bronze/logs /nexo/gold
docker compose exec -T hadoop-namenode hdfs dfs -put /opt/nexo/data/viajes_muestra.csv /nexo/bronze/viajes/
docker compose exec -T hadoop-namenode hdfs dfs -put /opt/nexo/data/texto_ops.txt /nexo/bronze/logs/
docker compose exec -T hadoop-namenode hdfs dfs -ls -R /nexo
docker compose exec -T hadoop-namenode hdfs dfs -du -h /nexo
```

WordCount sobre tickets y menciones. El directorio de salida no puede existir:

```bash
docker compose exec -T hadoop-namenode hdfs dfs -rm -r -f /nexo/gold/wordcount
docker compose exec -T hadoop-namenode hadoop jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.1.jar wordcount /nexo/bronze/logs/texto_ops.txt /nexo/gold/wordcount
docker compose exec -T hadoop-namenode hdfs dfs -cat /nexo/gold/wordcount/part-r-00000
```

Top 20 (en PowerShell, con `part-r-00000` ya exportado a `04-hadoop/mapreduce/`):

```powershell
powershell -ExecutionPolicy Bypass -File .\04-hadoop\mapreduce\generar_top20.ps1
```

Evidencia: [evidence/hadoop/bootstrap.log](evidence/hadoop/bootstrap.log), [evidence/hadoop/hdfs_practica.log](evidence/hadoop/hdfs_practica.log), [evidence/hadoop/mapreduce_job.log](evidence/hadoop/mapreduce_job.log), [evidence/hadoop/top20.txt](evidence/hadoop/top20.txt). En ese top, **cobro = 592**.

NombreNode UI: `http://localhost:9870`. ResourceManager: `http://localhost:8088`.

## 3. HBase y Hive

Scripts y resultados ya están en el repo. La imagen `apache/hadoop:3.4.1` del profile no trae los binarios de HBase ni Hive, así que estos dos no se relanzan con el compose.

- HBase: [04-hadoop/hbase/hbase_nexo.txt](04-hadoop/hbase/hbase_nexo.txt) · [evidence/hadoop/hbase_nexo_result.txt](evidence/hadoop/hbase_nexo_result.txt) · [ADR/003-hbase-rowkey.md](ADR/003-hbase-rowkey.md)
- Hive: [04-hadoop/hive/hive_nexo.sql](04-hadoop/hive/hive_nexo.sql) · [evidence/hadoop/hive_nexo_result.txt](evidence/hadoop/hive_nexo_result.txt) · DROP externa vs interna: [evidence/hadoop/hive_drop_demo.txt](evidence/hadoop/hive_drop_demo.txt)

La tabla `vehiculos` del script tiene 5 filas (V001–V005). La consigna pide al menos 50.

## 4. Pig, JDBC y Flume

Pig corre en local, sin el clúster:

```bash
bash tools/run_pig_m7.sh
```

Import de `crm.riders` a HDFS (plan B de Sqoop). MySQL y el profile `hadoop` tienen que estar arriba:

```bash
bash tools/import_riders_spark_docker.sh
```

Simulación Flume (200 eventos hacia HDFS). El namenode tiene que estar arriba:

```bash
python tools/simulate_flume_200.py
```

Contraste Pig/Spark y Flume/Streaming/Kafka: [ADR/002-hadoop-hoy.md](ADR/002-hadoop-hoy.md). Logs: `evidence/hadoop/pig.log`, `sqoop.log`, `flume.log`.

## 5. Spark batch Gold

Depende del Silver del paso 1. Si el JDBC a MySQL no responde, el job usa `data/riders.csv`.

```bash
python 05-spark/batch/job_gold.py --source minio
```

Sin Java en el host:

```bash
bash tools/run_job_gold_docker.sh
```

- Input: `s3a://silver/viajes_clima/` ([contratos/silver_viajes.yaml](contratos/silver_viajes.yaml))
- Output: `s3a://gold/ops_diaria/`, `gold/top_estaciones/`, `gold/repuesto_lluvia/`
- Local: `05-spark/batch/output/`
- Pregunta 1: [09-producto/gold_repuesto_lluvia.csv](09-producto/gold_repuesto_lluvia.csv)
- Revenue: tarifa plana **320 ARS** por viaje
- Explain: [evidence/spark/batch_explain.txt](evidence/spark/batch_explain.txt)

## 6. Spark Streaming

No depende de Silver ni de Hadoop. Puede correr con el clúster apagado.

```bash
bash tools/run_stream_alerts_docker.sh
```

Con JDK 17 en el host: `python 05-spark/streaming/stream_alerts.py`.

Ventana **2 min**, slide **30 s**, watermark **1 min**. Cuenta `payment_fail` y `battery_low`. El demo copia los JSON de `data/stream/` por tandas, manda un reloj a las 12:06:00Z para cerrar ventanas y después un `payment_fail` tardío (`VLATE`, 12:00:10Z) que no mueve los conteos.

Evidencia: [evidence/spark/streaming_console.txt](evidence/spark/streaming_console.txt), [evidence/spark/gold_alerts.csv](evidence/spark/gold_alerts.csv).

## 7. MongoDB

Un NoSQL por vez. Bajar Hadoop si la RAM no alcanza.

```bash
docker compose --profile hadoop stop
docker compose stop cassandra neo4j
docker compose up -d mongo
node 06-mongodb/crud.js
node 06-mongodb/ventas_pipeline.js
```

`crud.js` lee `./data/productos.json` desde la raíz. El pipeline escribe [09-producto/ventas_agg.json](09-producto/ventas_agg.json). Índice: [evidence/mongodb/index_explain.txt](evidence/mongodb/index_explain.txt). ADR: [ADR/004-mongodb-catalogo.md](ADR/004-mongodb-catalogo.md). Puerto **27018**.

## 8. Cassandra

```bash
docker compose stop mongo neo4j
bash 07-cassandra/ingestar.sh
docker exec -i nexo-cassandra cqlsh < 07-cassandra/queries.cql
node 09-producto/export_cassandra.js
```

La tercera query de `queries.cql` filtra solo por `ts` y tiene que fallar: no incluye `vehicle_id`. Captura: `evidence/cassandra_error.png.png`. Export: [09-producto/cassandra_ultima_lectura.json](09-producto/cassandra_ultima_lectura.json). ADR: [ADR/005-cassandra-timeseries.md](ADR/005-cassandra-timeseries.md). Puerto **9042**.

## 9. Neo4j

```bash
docker compose stop cassandra
docker compose up -d neo4j
docker exec -i nexo-neo4j cypher-shell -u neo4j -p nexo-urban-2026 --format verbose < 08-neo4j/grafo.cypher
```

Browser: `http://localhost:7474` (`neo4j` / `nexo-urban-2026`). Resultados: [evidence/neo4j/queries_result.txt](evidence/neo4j/queries_result.txt). Captura: [evidence/neo4j/browser.png](evidence/neo4j/browser.png). Detalle de las cuatro queries: [08-neo4j/README.md](08-neo4j/README.md).

## 10. Producto (M14) — falta `decision_ops.py`

Los módulos ya dejaron estos insumos. El script que cruza las cinco preguntas todavía no está.

| Pregunta | Insumo que ya existe | Qué falta decir en el script |
|----------|----------------------|------------------------------|
| 1. ¿Dónde reponer si llueve? | `09-producto/gold_repuesto_lluvia.csv` | Ordenar por `pct_clima_adverso`. El archivo es histórico, no un pronóstico de 3 h. |
| 2. ¿Qué vehículo retirar? | `09-producto/cassandra_ultima_lectura.json` | Aplicar un umbral de `battery_pct` y decir que es la última lectura, no dos lecturas seguidas. |
| 3. ¿Qué pase vender? | `ventas_agg.json` + recomendación de `R0000` en `evidence/neo4j/queries_result.txt` | `R0000` es plan `plus`. Los candidatos `free` (`R0034`, `R0076`) son el upsell. El top de pases es `SKU004`. |
| 4. ¿Hay collusion? | Misma evidencia Neo4j, query A22 | Hay 3 tríos con vínculo social y `ventanas_repetidas = 1`. Es alerta, no fraude recurrente. |
| 5. ¿Facturación vs “cobro”? | `evidence/hadoop/top20.txt` (`cobro` = 592) y revenue a 320 ARS en el log de Gold | No existe `09-producto/hive_facturacion_cobro.json`. No inventar el cruce por día. |

## Recortes (§8)

Documentados en [ADR/000-contexto.md](ADR/000-contexto.md):

1. Azure ADLS → MinIO ([ADR/001-datalake.md](ADR/001-datalake.md))
2. Imágenes desde `public.ecr.aws` (Docker Hub inestable)
3. Batch de 250 000 filas; el dataset abierto es usuarios EcoBici
4. Sqoop nativo → Spark JDBC
5. Sin Kafka, sin Bloom y sin geo de Mongo

## Demo de 8 minutos

1. **45 s.** NexoUrbano opera e-scooters en Buenos Aires y necesita decidir reposición, retiro, pases y fraude.
2. **60 s.** 4 V en [01-discovery/4V.md](01-discovery/4V.md) y medallion en [01-discovery/arquitectura.md](01-discovery/arquitectura.md).
3. **90 s.** MinIO bronze/silver y `03-databricks/01_ingesta.py` (Open-Meteo + Mastodon).
4. **90 s.** HDFS, WordCount de `texto_ops.txt` y una query Hive.
5. **90 s.** `gold_ops_diaria` y la consola de streaming con el evento tardío descartado.
6. **90 s.** Un documento de Mongo, una lectura Cassandra y el camino `R0000 → R0108` en Neo4j.
7. **45 s.** Las cinco respuestas de `decision_ops.py`, cuando exista.

## Owners

| Owner | Módulos |
|-------|---------|
| Sebastian Albornoz | Lake, ingesta, Spark batch y streaming, contratos de viajes |
| Mauricio Jurado | Hadoop, HDFS, MapReduce, Hive, HBase, Pig, JDBC, Flume |
| Alvaro Costa | Mongo, Cassandra, Neo4j |

`master` entra por PR. Cada PR lleva código, `evidence/` y una fila en [AI_LOG.md](AI_LOG.md).
