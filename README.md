# NexoUrbano 2026 — Squad (grupo 67)

Ciudad: **Buenos Aires** · Semilla: **`67001`** · Integrantes: Sebastian Albornoz, Mauricio Jurado, Alvaro Costa.

Contexto: [ADR/000-contexto.md](ADR/000-contexto.md) · Consigna: [TP_NexoUrbano_2026.md](TP_NexoUrbano_2026.md) · Checklist: [CHECKLIST_ENTREGA.md](CHECKLIST_ENTREGA.md)

Video demo 8 min: _link pendiente_.

Los comandos se corren desde la raíz del repo, en bash (Git Bash, WSL, macOS o Linux). En 8 GB de RAM no levantar Hadoop, Cassandra y Neo4j a la vez. MinIO y MySQL pueden quedar arriba durante todo el recorrido.

No uses `docker compose up -d` sin lista de servicios: arranca MySQL, Mongo, Cassandra, Neo4j y MinIO juntos. Con `--profile hadoop` suma el clúster. En 8 GB eso no entra.

## Levantar el entorno en menos de 20 minutos

Este tramo deja el lake usable. No regenera los 250 000 viajes, no llama a Open-Meteo y no baja Hadoop. Los CSV y la evidencia de cada módulo ya están en el repo.

Hacen falta Docker Desktop (o Docker Engine con Compose v2) y bash. Python 3.11+ y Node.js solo si se re-ejecutan los jobs de más abajo. Java 17 no hace falta: Spark entra por los scripts de Docker.

```bash
cp .env.example .env
docker compose up -d mysql minio
docker compose ps
```

`.env.example` ya trae las claves del laboratorio (`nexo` / `nexo-urban-2026`). Coinciden con `docker-compose.yml`. No hay que completar nada para esta corrida. Los scripts de Python no leen `.env`: si la variable no está exportada, usan ese mismo default.

Esperar a que `nexo-mysql` figure `healthy` (el healthcheck reintenta ~1 minuto). Comprobar:

```bash
docker compose exec -T mysql mysqladmin ping -pnexo
```

| Servicio | Puerto | Acceso |
|----------|--------|--------|
| MinIO API | 9000 | `s3a://` |
| MinIO consola | 9001 | `nexo` / `nexo-urban-2026` |
| MySQL | **3307** | DB `crm`, user `root`, password `nexo` |

URI Bronze: `s3a://bronze/viajes/yyyy/mm/dd/viajes.csv`

Imágenes de este tramo: MySQL desde `public.ecr.aws`, MinIO desde `cgr.dev/chainguard/minio`. Hadoop, Hive y Spark salen de Docker Hub (`apache/hadoop:3.4.1`, `apache/hive:4.0.1`, `apache/spark:3.5.3`). El primer pull de esas tres puede pasar los 20 minutos y no forma parte de este arranque.

Para re-ejecutar jobs en el host:

```bash
pip install -r requirements.txt
npm install mongodb cassandra-driver
```

`npm install` se corre en la raíz. No hay `package.json`: el comando instala los dos drivers en `node_modules/`.

## 1. Lake: Bronze y Silver

Solo si hay que volver a cargar el lake. Pide red (Open-Meteo y, si Bluesky responde 403, Mastodon) y varios minutos sobre 250 000 filas.

```bash
docker compose up -d mysql minio
python tools/bronze_a_minio.py
python 03-databricks/01_ingesta.py
```

Escribe Parquet en `s3a://silver/viajes_clima/`. El paso 5 lo lee. No hay workspace Databricks: `03-databricks/01_ingesta.py` es el job re-ejecutable (ciudad y fechas al inicio del archivo). Ver [ADR/002-databricks-apis.md](ADR/002-databricks-apis.md).

Evidencia ya generada: [evidence/docker/minio_bronze_viajes.txt](evidence/docker/minio_bronze_viajes.txt), [evidence/databricks/01_ingesta_log.txt](evidence/databricks/01_ingesta_log.txt). ADR: [ADR/001-datalake.md](ADR/001-datalake.md).

Los CSV ya están en `data/` (semilla `67001`, 250 000 viajes). Regenerarlos solo si hay que rehacer la semilla:

```bash
python tools/generar_datos.py --seed 67001 --n-viajes 250000
```

Perfilado del dataset abierto (M1, ya corrido): `python 01-discovery/perfilado.py`.

## 2. Hadoop (HDFS y WordCount)

Apagar los NoSQL pesados. El profile `hadoop` no arranca solo: hay que nombrar los cuatro servicios, si no Compose también levanta Mongo, Cassandra y Neo4j.

```bash
docker compose stop mongo cassandra neo4j
docker compose --profile hadoop up -d hadoop-namenode hadoop-datanode hadoop-resourcemanager hadoop-nodemanager
docker compose exec -T hadoop-namenode hdfs dfs -mkdir -p /nexo/bronze/viajes /nexo/bronze/logs /nexo/gold
docker compose exec -T hadoop-namenode hdfs dfs -put -f /opt/nexo/data/viajes_muestra.csv /nexo/bronze/viajes/
docker compose exec -T hadoop-namenode hdfs dfs -put -f /opt/nexo/data/texto_ops.txt /nexo/bronze/logs/
docker compose exec -T hadoop-namenode hdfs dfs -ls -R /nexo
```

WordCount sobre tickets y menciones. El directorio de salida no puede existir:

```bash
docker compose exec -T hadoop-namenode hdfs dfs -rm -r -f /nexo/gold/wordcount
docker compose exec -T hadoop-namenode hadoop jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.1.jar wordcount /nexo/bronze/logs/texto_ops.txt /nexo/gold/wordcount
docker compose exec -T hadoop-namenode hdfs dfs -cat /nexo/gold/wordcount/part-r-00000
```

El top 20 ya está en [evidence/hadoop/top20.txt](evidence/hadoop/top20.txt). **cobro = 592**. En Windows se puede regenerar con `04-hadoop/mapreduce/generar_top20.ps1` si `part-r-00000` fue copiado a `04-hadoop/mapreduce/`.

NombreNode: `http://localhost:9870`. ResourceManager: `http://localhost:8088`.

## 3. Hive

Hive es un servicio aparte (profile `hive`, imagen `apache/hive:4.0.1`). No viene dentro de `apache/hadoop:3.4.1`. El NameNode del paso 2 tiene que estar `healthy`. El primer arranque de HiveServer2 tarda unos minutos. El puerto es **10000**.

La tabla externa lee `/nexo/bronze/m6_viajes/viajes.csv` (el CSV completo, no la muestra del paso 2).

```bash
docker compose --profile hadoop --profile hive up -d hadoop-namenode hadoop-datanode hadoop-resourcemanager hadoop-nodemanager hive
docker compose cp data/viajes.csv hadoop-namenode:/tmp/m6_viajes.csv
docker compose exec -T hadoop-namenode hdfs dfs -mkdir -p /nexo/bronze/m6_viajes
docker compose exec -T hadoop-namenode hdfs dfs -put -f /tmp/m6_viajes.csv /nexo/bronze/m6_viajes/viajes.csv
docker compose cp 04-hadoop/hive/hive_nexo.sql hive:/tmp/hive_nexo.sql
docker compose exec -T hive beeline -u "jdbc:hive2://localhost:10000/default" -f /tmp/hive_nexo.sql
```

`hive_nexo.sql` hace `DROP TABLE IF EXISTS` al empezar: se puede volver a correr. El `INSERT` de ~245 000 filas no entra en los 20 minutos del arranque. La evidencia ya está en [evidence/hadoop/hive_nexo_result.txt](evidence/hadoop/hive_nexo_result.txt).

DROP externa vs interna, después de esa carga. Borrar `bronze_viajes` deja el CSV en HDFS. Borrar `silver_viajes` borra `/nexo/warehouse/silver_viajes/`.

```bash
docker compose cp 04-hadoop/hive/hive_drop_demo.sql hive:/tmp/hive_drop_demo.sql
docker compose exec -T hive beeline -u "jdbc:hive2://localhost:10000/default" -f /tmp/hive_drop_demo.sql
docker compose exec -T hadoop-namenode hdfs dfs -ls /nexo/bronze/m6_viajes
docker compose exec -T hadoop-namenode hdfs dfs -test -d /nexo/warehouse/silver_viajes
```

El último comando sale con error si el directorio interno ya no está: es el resultado esperado. Evidencia: [evidence/hadoop/hive_drop_demo.txt](evidence/hadoop/hive_drop_demo.txt).

## 4. HBase

HBase no está en `docker-compose.yml`. El shell y el resultado ya corridos están en [04-hadoop/hbase/hbase_nexo.txt](04-hadoop/hbase/hbase_nexo.txt) y [evidence/hadoop/hbase_nexo_result.txt](evidence/hadoop/hbase_nexo_result.txt). Rowkey: [ADR/003-hbase-rowkey.md](ADR/003-hbase-rowkey.md).

La tabla `vehiculos` tiene 50 rowkeys (V001–V050). El `count` de la evidencia devuelve 50.

## 5. Pig, JDBC y Flume

Pig corre en local, sin el clúster. La primera vez construye la imagen:

```bash
bash tools/run_pig_m7.sh
```

Import de `crm.riders` a HDFS (plan B de Sqoop). MySQL y el profile `hadoop` tienen que estar arriba. El script funciona en Git Bash y en Linux:

```bash
bash tools/import_riders_spark_docker.sh
```

Simulación Flume (200 eventos hacia HDFS). El namenode tiene que estar arriba:

```bash
python tools/simulate_flume_200.py
```

Contraste Pig/Spark y Flume/Streaming/Kafka: [ADR/002-hadoop-hoy.md](ADR/002-hadoop-hoy.md). Logs: `evidence/hadoop/pig.log`, `sqoop.log`, `flume.log`.

## 6. Spark batch Gold

Depende del Silver del paso 1. Si el JDBC a MySQL no responde, el job usa `data/riders.csv`. Sin Java en el host:

```bash
bash tools/run_job_gold_docker.sh
```

Con JDK 17 en el host: `python 05-spark/batch/job_gold.py --source minio`.

- Input: `s3a://silver/viajes_clima/` ([contratos/silver_viajes.yaml](contratos/silver_viajes.yaml))
- Output: `s3a://gold/ops_diaria/`, `gold/top_estaciones/`, `gold/repuesto_lluvia/`
- Local: `05-spark/batch/output/`
- Pregunta 1: [09-producto/gold_repuesto_lluvia.csv](09-producto/gold_repuesto_lluvia.csv)
- Revenue: tarifa plana **320 ARS** por viaje
- Explain: [evidence/spark/batch_explain.txt](evidence/spark/batch_explain.txt)

El contenedor de Spark llega a MinIO y a MySQL por `host.docker.internal`. En Linux el script agrega `--add-host=host.docker.internal:host-gateway`.

## 7. Spark Streaming

No depende de Silver ni de Hadoop. Puede correr con el clúster apagado. El demo por tandas dura más que un minuto: no entra en el arranque de 20 minutos. La consola ya está en la evidencia.

```bash
bash tools/run_stream_alerts_docker.sh
```

Con JDK 17 en el host: `python 05-spark/streaming/stream_alerts.py`.

Ventana **2 min**, slide **30 s**, watermark **1 min**. Cuenta `payment_fail` y `battery_low`. El demo copia los JSON de `data/stream/` por tandas, manda un reloj a las 12:06:00Z para cerrar ventanas y después un `payment_fail` tardío (`VLATE`, 12:00:10Z) que no mueve los conteos.

Evidencia: [evidence/spark/streaming_console.txt](evidence/spark/streaming_console.txt), [evidence/spark/gold_alerts.csv](evidence/spark/gold_alerts.csv).

## 8. MongoDB

Un NoSQL por vez. Bajar Hadoop si la RAM no alcanza.

```bash
docker compose --profile hadoop --profile hive stop
docker compose stop cassandra neo4j
docker compose up -d mongo
node 06-mongodb/crud.js
node 06-mongodb/ventas_pipeline.js
```

`crud.js` lee `./data/productos.json` desde la raíz. El pipeline escribe [09-producto/ventas_agg.json](09-producto/ventas_agg.json). Índice: [evidence/mongodb/index_explain.txt](evidence/mongodb/index_explain.txt). ADR: [ADR/004-mongodb-catalogo.md](ADR/004-mongodb-catalogo.md). Puerto **27018**.

## 9. Cassandra

```bash
docker compose stop mongo neo4j
bash 07-cassandra/ingestar.sh
docker exec -i nexo-cassandra cqlsh < 07-cassandra/queries.cql
node 09-producto/export_cassandra.js
```

La tercera query de `queries.cql` filtra solo por `ts` y tiene que fallar: no incluye `vehicle_id`. Captura: `evidence/cassandra_error.png.png`. Export: [09-producto/cassandra_ultima_lectura.json](09-producto/cassandra_ultima_lectura.json). ADR: [ADR/005-cassandra-timeseries.md](ADR/005-cassandra-timeseries.md). Puerto **9042**. Cassandra puede tardar uno o dos minutos en aceptar `cqlsh`. El script espera hasta ~60 s.

## 10. Neo4j

```bash
docker compose stop cassandra
docker compose up -d neo4j
docker exec -i nexo-neo4j cypher-shell -u neo4j -p nexo-urban-2026 --format verbose < 08-neo4j/grafo.cypher
```

`grafo.cypher` lee los 250 000 viajes para la collusion. En la máquina del squad tardó unos 13 s. Browser: `http://localhost:7474` (`neo4j` / `nexo-urban-2026`). Resultados: [evidence/neo4j/queries_result.txt](evidence/neo4j/queries_result.txt). Captura: [evidence/neo4j/browser.png](evidence/neo4j/browser.png). Detalle: [08-neo4j/README.md](08-neo4j/README.md).

## 11. Producto (M14)

No hace falta levantar motores. Desde la raíz:

```bash
python 09-producto/decision_ops.py
```

Lee los exports y escribe `09-producto/decision_ops_resultado.json`. El link del video sigue pendiente.

| Pregunta | Qué responde |
|----------|----------------|
| 1. ¿Dónde reponer si llueve? | **ST003**, 14,12 % de clima adverso, 10 012 viajes. Es histórico. No hay pronóstico de 3 h. |
| 2. ¿Qué vehículo retirar? | Nadie bajo 20 % de `battery_pct`. Las más bajas: **V0040** (34,1), **V0008** (35,4), **V0075** (39,7). La foto es del 2026-05-01, no es SOH. |
| 3. ¿Qué pase vender? | **SKU004** (Pase 4). Upsell free de `R0000`: **R0034** y **R0076**. No hay propensión por rider. |
| 4. ¿Hay collusion? | 3 tríos en ST000 con vínculo y una sola ventana. Alerta, no fraude recurrente. |
| 5. ¿Facturación vs “cobro”? | **592** ocurrencias (`cobro` + `#cobro`). **78 406 080 ARS** (245 019 viajes × 320). No hay cruce por día. |

## Recortes (§8)

Documentados en [ADR/000-contexto.md](ADR/000-contexto.md):

1. Azure ADLS → MinIO ([ADR/001-datalake.md](ADR/001-datalake.md))
2. MySQL, Mongo, Cassandra y Neo4j desde `public.ecr.aws` (Docker Hub inestable). Hadoop, Hive y Spark siguen en Docker Hub.
3. Batch de 250 000 filas; el dataset abierto es usuarios EcoBici
4. Sqoop nativo → Spark JDBC
5. Sin Kafka, sin Bloom y sin geo de Mongo
6. HBase no corre en el Compose



## Owners

| Owner | Módulos |
|-------|---------|
| Sebastian Albornoz | Lake, ingesta, Spark batch y streaming, contratos de viajes |
| Mauricio Jurado | Hadoop, HDFS, MapReduce, Hive, HBase, Pig, JDBC, Flume |
| Alvaro Costa | Mongo, Cassandra, Neo4j |

`master` entra por PR. Cada PR lleva código, `evidence/` y una fila en [AI_LOG.md](AI_LOG.md).
