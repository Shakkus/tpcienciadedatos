# Pendientes por integrante — NexoUrbano (post kick-off)

**Fecha de referencia:** 2026-10-09 · **Semilla:** `67001` · **Ciudad:** Buenos Aires  

Este documento es la **cola de trabajo** después del kick-off conjunto (M1 discovery, perfilado dataset abierto, bronze MinIO, estructura de repo). Cada uno trabaja en **su rama** y abre PRs chicos a `main`.

---

## Ya resuelto (no repetir)

| Ítem | Evidencia |
|------|-----------|
| 4 V + arquitectura medallion | `01-discovery/4V.md`, `01-discovery/arquitectura.md` |
| Dataset abierto ≥100k perfilado | `data/Dataset_abierto_real.csv`, `01-discovery/perfilado.py`, `evidence/discovery/perfilado_dataset_abierto_real.txt`, `contratos/dataset_abierto_real.yaml` |
| Contrato bronze viajes (plantilla) | `contratos/bronze_viajes.yaml` |
| Lake local MinIO + bronze viajes | `tools/bronze_a_minio.py`, `ADR/001-datalake.md`, `evidence/docker/minio_bronze_viajes.txt` |
| Contexto squad + recortes §8 | `ADR/000-contexto.md` |
| Esqueleto carpetas + README + `.env.example` | `README.md`, `02-azure-datalake/` … `09-producto/` |

**Pendiente global (los tres al final):** `AI_LOG.md` ≥ 6 filas (van 3), video 8 min, tag `entrega-final`, `09-producto/decision_ops.py` (5 preguntas). Ver sección [Cierre conjunto](#cierre-conjunto).

---

## Reglas para todos

1. **Rama:** `feat/<modulo>-<apellido>` (ej. `feat/hadoop-jurado`, `feat/mongodb-costa`).
2. **PR incluye:** código/scripts, salida en `evidence/`, **1 fila nueva en `AI_LOG.md`**.
3. **Revisor:** otro integrante; si no puede explicarlo en la demo, no merge.
4. **RAM 8 GB:** no levantar Hadoop + Cassandra + Neo4j a la vez. Mauricio = sandbox Hadoop; Álvaro = **un NoSQL por vez** (apagar contenedor al terminar).
5. **Contrato viajes:** solo **Sebastian** cambia grano/claves/timezone en `contratos/bronze_viajes.yaml` (avisar en el PR).
6. **Silver estable:** cuando Sebastian congele el esquema Silver (post M3), avisar en el grupo; Spark Gold (M8) depende de eso.

---

## Dependencias entre carriles

```text
Sebastian: Silver (M3) ──────────────────► Spark batch Gold (M8) ──► Pregunta 1 decision_ops
Mauricio:  HDFS + WordCount + Hive ──────► export CSV/JSON ───────► Pregunta 5 decision_ops
Álvaro:    Mongo agg (ventas_agg.json) ───► Preguntas 3 (+ catálogo)
Álvaro:    Cassandra última lectura ─────► Pregunta 2
Álvaro:    Neo4j Cypher fraude ───────────► Pregunta 4
```

| Entregable que otros necesitan | Owner | Archivo / lugar sugerido |
|-------------------------------|--------|---------------------------|
| Métricas Gold + clima/lluvia | Sebastian | `05-spark/batch/output/` o MinIO `gold/` + path en PR |
| Riders en HDFS (JDBC) | Mauricio | `/nexo/bronze/riders_sqoop/` + script en `04-hadoop/sqoop/` |
| WordCount / agregado “cobro” | Mauricio | `/nexo/gold/wordcount/` + copia en `evidence/hadoop/` |
| `ventas_agg.json` | Álvaro | `09-producto/ventas_agg.json` |
| Export Cassandra “última lectura por vehículo” | Álvaro | `09-producto/cassandra_ultima_lectura.json` (crear) |

---

# Sebastian Albornoz — Lakehouse + Spark (camino crítico)

**Ramas sugeridas:** `feat/databricks-ingesta`, `feat/spark-batch`, `feat/spark-streaming`  
**Carpetas:** `03-databricks/`, `05-spark/batch/`, `05-spark/streaming/`, `02-azure-datalake/`, `contratos/`, `ADR/`

### Prioridad 1 — M3 Databricks / ingesta Silver (bloquea Gold)

| # | Tarea | Detalle | Entregable |
|---|--------|---------|------------|
| S1 | Notebook ingesta | Crear `03-databricks/01_ingesta.py` (o `.ipynb` exportado) **re-ejecutable** | Script + outputs (HTML o logs) |
| S2 | Leer bronze viajes | Desde MinIO `s3a://bronze/viajes/...` **o** `data/viajes.csv` local con mismas columnas que `bronze_viajes.yaml` | Parámetros al inicio: ciudad, rango fechas |
| S3 | Open-Meteo | Histórico/forecast para **3 estaciones** de `data/estaciones.csv` (usar `lat`, `lon`) | Tabla clima por estación + hora/día |
| S4 | API social | Bluesky `app.bsky.feed.searchPosts` **o** Mastodon hashtag (sin X de pago) | JSON normalizado (fecha, texto, autor) |
| S5 | Persistir Silver | Join clima × viajes (por estación origen + ventana temporal acordada). Escribir Parquet en MinIO bucket **`silver`** (ej. `silver/viajes_clima/`) | **Congelar esquema** y documentar en PR |
| S6 | Exploración M3 | `shape`, nulos, **5 métricas**: viajes/hora, duración p50/p95, top estaciones origen, % nulos km, filas post-rechazo Bronze | Celdas con salida visible |
| S7 | Evidencia | | `evidence/databricks/01_ingesta_log.txt` o capturas + URI Silver en README |

**Criterio “Silver listo”:** otro integrante puede leer la ruta Silver sin preguntar el esquema; columnas documentadas en comentario o `contratos/silver_viajes.yaml` (opcional pero recomendado).

### Prioridad 2 — Contrato bronze viajes (cerrar M1 ítem 3)

| # | Tarea | Detalle |
|---|--------|---------|
| S8 | Perfilado viajes sintéticos | Opcional pero recomendado: extender `perfilado.py` o script corto sobre `data/viajes.csv` (nulos, duplicados `ride_id`, outliers `duration_s`) |
| S9 | Actualizar YAML | Ajustar `contratos/bronze_viajes.yaml` solo con lo medido; alinear `reglas_rechazo_silver` con lo que implementen en Silver |

### Prioridad 3 — M8 Spark batch Gold

| # | Tarea | Detalle | Entregable |
|---|--------|---------|------------|
| S10 | Job batch | `05-spark/batch/job_gold.py` — leer Silver (Parquet) + **250k** viajes |
| S11 | Joins | Clima (Silver) + riders (cuando Mauricio deje JDBC en HDFS o leer MySQL con Spark JDBC) |
| S12 | Filtros calidad | Aplicar reglas del contrato Bronze (`duration_s`, `fare_ars`, etc.) |
| S13 | Tabla `gold_ops_diaria` | viajes, minutos, km, % clima adverso, top 10 estaciones origen, revenue (tarifa plana documentada si Mongo no está) |
| S14 | Sink | Parquet/Delta en MinIO `gold/` o path local `05-spark/batch/output/` |
| S15 | Explain | Pegar salida `df.explain(True)` o equivalente en `evidence/spark/batch_explain.txt` |

### Prioridad 4 — M9 Spark Streaming

| # | Tarea | Detalle | Entregable |
|---|--------|---------|------------|
| S16 | Fuente | **Opción B recomendada:** leer `data/stream/*.json` (file source) |
| S17 | Ventana | **2 min** ventana, slide **30 s**; contar `payment_fail` y `battery_low` |
| S18 | Watermark | **1 min**; documentar 1 evento tardío descartado |
| S19 | Sink | Consola + Parquet `gold_alerts` |
| S20 | Script | `05-spark/streaming/stream_alerts.py` + captura 30 s en `evidence/spark/streaming_console.txt` |

### Prioridad 5 — ADR y lake

| # | Tarea | Archivo |
|---|--------|---------|
| S21 | Completar costos / paridad | Ampliar `ADR/001-datalake.md` (egress, IAM, TTFB — aunque sea estimado + link calculador Azure) |
| S22 | ADR Databricks | `ADR/002-databricks-apis.md` (por qué Open-Meteo + social, fallos de API) |

### Pregunta de negocio (M14)

| # | Pregunta | Tu dato |
|---|----------|---------|
| S23 | **1. ¿Dónde reponer scooters si llueve?** | Export desde Gold: estaciones × % lluvia / clima adverso (archivo para `decision_ops.py`) |

### Demo video (tu tramo)

- ~90 s ingesta lakehouse (MinIO + notebook M3).  
- ~90 s Spark batch + streaming (M8 + M9).

---

# Mauricio Jurado — Hadoop (infra + batch legado)

**Rama sugerida:** `feat/hadoop-jurado` (PRs separados por submódulo si querés: `feat/hdfs`, `feat/hive`, …)  
**Carpetas:** `04-hadoop/hdfs`, `mapreduce`, `hbase`, `hive`, `pig`, `sqoop`, `flume`, `evidence/hadoop/`, `ADR/`

### Prioridad 1 — M4 Entorno Hadoop (empezar ya)

| # | Tarea | Detalle | Entregable |
|---|--------|---------|------------|
| M1 | Sandbox | Levantar Hadoop (sandbox cátedra o imagen documentada). Versiones en README o ADR | |
| M2 | Bootstrap | `hdfs dfs -ls /` OK | `evidence/hadoop/bootstrap.log` |
| M3 | ADR | `ADR/002-hadoop-hoy.md` — por qué Hadoop en el diseño 2026 y qué no harían en MapReduce hoy | |

*Quiz interno 4V (`04-hadoop/quiz.csv`): opcional si no hay tiempo; no bloquea HMV.*

### Prioridad 2 — M5 HDFS + MapReduce

| # | Tarea | Comandos / archivos |
|---|--------|---------------------|
| M4 | Árbol HDFS | `hdfs dfs -mkdir -p /nexo/bronze/viajes /nexo/bronze/logs` |
| M5 | Carga | `hdfs dfs -put data/viajes_muestra.csv /nexo/bronze/viajes/` (muestra; no hace falta subir 250k) |
| M6 | Práctica | `-ls -R`, `-cp`, `-mv`, `-du -h` — log completo en `evidence/hadoop/hdfs_practica.log` |
| M7 | Comentario 8 líneas | Bloque tamaño de bloque, replicación, fallo de 1 DataNode (en el mismo log) |
| M8 | WordCount | Input: `data/texto_ops.txt` → output `/nexo/gold/wordcount/` |
| M9 | Evidencia MR | Comando `hadoop jar ...` + `part-r-00000` (top términos: batería, cobro, app…) en `04-hadoop/mapreduce/` y copia en `evidence/hadoop/` |

### Prioridad 3 — M6 HBase + Hive

| # | Tarea | Detalle | Entregable |
|---|--------|---------|------------|
| M10 | HBase tabla | `nexo:vehiculos` (CF `id`, `telemetry`), ≥50 filas desde `data/vehiculos.csv` | `04-hadoop/hbase/hbase_nexo.txt` |
| M11 | HBase queries | `get` por rowkey + `scan` con filtro `estado = mantenimiento` | Salida en evidencia |
| M12 | ADR rowkey | `ADR/003-hbase-rowkey.md` (~5 líneas anti-hotspot) | |
| M13 | Hive externa | `bronze_viajes` → path HDFS del CSV de viajes | `04-hadoop/hive/hive_nexo.sql` |
| M14 | Hive interna | `silver_viajes` particionada por `fecha` | mismo SQL |
| M15 | Queries | Top destinos; viajes > 45 min; GROUP BY hora × barrio/station | Resultados en `evidence/hadoop/hive_results.txt` |
| M16 | DROP demo | **Obligatorio:** DROP tabla externa vs interna — qué pasó con los datos | Párrafo en evidencia o ADR |

### Prioridad 4 — M7 Pig + JDBC + Flume

| # | Tarea | Detalle | Entregable |
|---|--------|---------|------------|
| M17 | Pig | Filtrar `duration_s < 30` o `> 14400`, escribir `clean_viajes` | `04-hadoop/pig/clean_viajes.pig` |
| M18 | ADR Pig vs Spark | Mismas reglas en 10 líneas Spark en ADR (duplicado a propósito del enunciado) | |
| M19 | CRM → HDFS | **Plan B:** Spark JDBC o script Python leyendo MySQL `crm.riders` (compose ya carga SQL) → `/nexo/bronze/riders_sqoop/` | `04-hadoop/sqoop/` + nota ADR |
| M20 | Flume | Tail `data/app.log` → HDFS `/nexo/bronze/logs/` (o simulación documentada con 200 líneas) | `04-hadoop/flume/` + `evidence/hadoop/flume.log` |
| M21 | Contraste 8 líneas | Flume vs Structured Streaming vs Kafka (greenfield 2026) | En ADR o README hadoop |

### Pregunta de negocio (M14)

| # | Pregunta | Tu dato |
|---|----------|---------|
| M22 | **5. Facturación vs tickets “cobro”** | Export: conteos WordCount + agregado Hive por día → `09-producto/hive_facturacion_cobro.json` (o CSV) |

### Demo video (tu tramo)

- ~90 s Hadoop: HDFS, WordCount, 1 query Hive.

### MySQL para JDBC

- Host `127.0.0.1`, puerto **3306** (ver `docker-compose.yml`; si falla conexión, revisar mapping de puertos).
- DB `crm`, user `root`, password `nexo`.

---

# Alvaro Costa — Polyglot (Mongo, Cassandra, Neo4j)

**Ramas sugeridas:** `feat/mongodb-costa`, `feat/cassandra-costa`, `feat/neo4j-costa`  
**Carpetas:** `06-mongodb/`, `07-cassandra/`, `08-neo4j/`, `09-producto/`, `evidence/`

### Prioridad 1 — M10 MongoDB CRUD

| # | Tarea | Detalle | Entregable |
|---|--------|---------|------------|
| A1 | Contenedor | `docker compose up -d mongo` (apagar otros NoSQL pesados) | |
| A2 | Carga | Colección `productos` desde `data/productos.json` (documentos **anidados**) | |
| A3 | CRUD | `insertMany` ≥30, `find` filtrado, `updateOne` precio, `deleteOne` SKU | `06-mongodb/crud.js` |
| A4 | Índice | `tags` o `ciudad` + `categoria`; `explain("executionStats")` **antes y después** | Tiempos en `evidence/mongodb/index_explain.txt` |
| A5 | Texto 8 líneas | SQL 3 joins vs 1 documento Mongo | En comentario al inicio del script o `06-mongodb/README.md` |
| A6 | ADR | `ADR/004-mongodb-catalogo.md` (opcional, suma puntos) | |

### Prioridad 2 — M11 Agregaciones + export producto

| # | Tarea | Detalle | Entregable |
|---|--------|---------|------------|
| A7 | Pipeline ventas | `$match`, totales por categoría, top 5 productos, ventas por mes, `$lookup` productos, `$project` | `06-mongodb/ventas_pipeline.js` |
| A8 | Export | | **`09-producto/ventas_agg.json`** (obligatorio para pregunta 3) |
| A9 | Carga ventas | Importar `data/ventas.csv` a colección `ventas` | script de carga o mismo pipeline |

### Prioridad 3 — M12 Cassandra

| # | Tarea | Detalle | Entregable |
|---|--------|---------|------------|
| A10 | Contenedor | `docker compose up -d cassandra` (**solo** Cassandra; bajar Neo4j/Hadoop si compiten por RAM) | |
| A11 | Schema | Keyspace `nexo`, tabla `lecturas_vehiculo`, **PK `vehicle_id`**, clustering `ts DESC` | `07-cassandra/schema.cql` |
| A12 | Carga | ≥5000 filas desde `data/lecturas_sensores.csv` | `07-cassandra/load.cql` o script |
| A13 | Queries OK | Últimas 20 lecturas de un vehículo; rango horario | `07-cassandra/queries.cql` |
| A14 | Query que **falla** | Ej. “todas las lecturas de la última hora de **toda** la flota” sin PK | Captura del error en evidencia |
| A15 | ADR consistencia | `ONE` vs `QUORUM` (batería vs facturación) | `ADR/005-cassandra-timeseries.md` |
| A16 | Export pregunta 2 | Última lectura por vehículo (umbral batería) | `09-producto/cassandra_ultima_lectura.json` |

### Prioridad 4 — M13 Neo4j

| # | Tarea | Detalle | Entregable |
|---|--------|---------|------------|
| [x] A17 | Contenedor | `docker compose up -d neo4j` — Browser `http://localhost:7474` (sin Bloom) | |
| [x] A18 | Carga | `data/grafo.json` → nodos `Rider`, `Interes`, `Estacion` y relaciones del enunciado | `08-neo4j/grafo.cypher` |
| [x] A19 | Cypher 1 | Amigos en común | |
| [x] A20 | Cypher 2 | **Camino más corto** entre 2 riders | |
| [x] A21 | Cypher 3 | Recomendación (mismas estaciones, no amigos) | |
| [x] A22 | Cypher 4 | Patrón fraude / collusion (3 riders, misma estación, <2 min) | Resultados en `evidence/neo4j/queries_result.txt` |
| [x] A23 | Captura | Grafo en Browser | `evidence/neo4j/browser.png` |

### Preguntas de negocio (M14)

| # | Pregunta | Tu dato |
|---|----------|---------|
| A24 | **2. ¿Qué vehículo retirar hoy?** | Cassandra export (A16) |
| A25 | **3. ¿Qué pase vender?** | `ventas_agg.json` + resultado Cypher recomendación |
| A26 | **4. ¿Collusion / fraude?** | Salida Cypher fraude (A22) |

### Demo video (tu tramo)

- ~90 s Mongo + Cassandra + Neo4j (CRUD, 1 query Cypher, 1 lectura Cassandra).

---

## Cierre conjunto

Cuando cada carril tenga **exports mínimos** en `09-producto/`:

| # | Tarea | Quién lidera | Archivo |
|---|--------|--------------|---------|
| C1 | Notebook/script 5 preguntas | Los tres; **Sebastian** integra repo | `09-producto/decision_ops.py` |
| C2 | Si falta dato → mensaje explícito, **no inventar** | Todos | |
| C3 | `AI_LOG.md` ≥ 6 filas | Cada uno en sus PRs | |
| C4 | README: link video + checklist | Sebastian o rotación | `README.md` |
| C5 | Tag `entrega-final` en `main` | Después del último PR | |

### Guion video 8 min (reparto)

1. Problema + 4V (45s + 60s) — cualquiera  
2. Ingesta lake — Sebastian  
3. Hadoop — Mauricio  
4. Spark batch + streaming — Sebastian  
5. Polyglot — Álvaro  
6. Métricas Gold / decision_ops — los tres (45s)  
7. **Edición final video:** Álvaro (acordado en plan de trabajo)

---

## Orden sugerido si hay pocos días

| Día | Sebastian | Mauricio | Álvaro |
|-----|-----------|----------|--------|
| 1 | M3 ingesta + Silver | M4 + M5 HDFS + WordCount | M10 Mongo CRUD |
| 2 | Congelar Silver + empezar M8 | M6 Hive + HBase | M11 agg + `ventas_agg.json` |
| 3 | Terminar M8 + M9 streaming | M7 Pig + JDBC + Flume | M12 Cassandra + export |
| 4 | Exports Gold + ayuda `decision_ops` | Export pregunta 5 | M13 Neo4j + exports |
| 5 | PR finales, video, tag | Video tramo Hadoop | Video tramo + corte |

---

## Checklist rápida → owner

| Ítem `CHECKLIST_ENTREGA.md` | Owner principal |
|-----------------------------|-----------------|
| URI Data Lake en README | Sebastian ✅ (revisar tras Silver) |
| Notebook Databricks re-ejecutable | Sebastian |
| Log HDFS + WordCount | Mauricio |
| HBase + Hive DROP | Mauricio |
| Pig + JDBC + Flume | Mauricio |
| `job_gold.py` + explain | Sebastian |
| Streaming 2 min + watermark | Sebastian |
| Mongo CRUD + pipeline + JSON | Álvaro |
| Cassandra PK + query fallida | Álvaro |
| Cypher común, camino, recomendación | Álvaro |
| `09-producto` 5 preguntas | Los tres |
| `AI_LOG` 6 filas | Los tres |

---

*Actualizar este archivo cuando se mergee un HMV importante (Silver congelado, exports en `09-producto/`, etc.).*
