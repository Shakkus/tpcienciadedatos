# TP evaluativo — NexoUrbano 2026
## Plataforma de inteligencia de movilidad urbana (Big Data · Lakehouse · NoSQL)

**Asignatura:** Big Data y bases de datos no relacionales  
**Modalidad:** 100 % práctico · trabajo en squad  
**Duración:** 4 semanas (1 mes calendario)  
**Carga estimada:** 12–15 h/semana por squad (≈ 50 h totales)  
**Entrega:** un repositorio Git + demo de 8 minutos  
**Stack de referencia 2026:** Azure Data Lake Gen2 · Databricks · Spark 4 · Delta Lake · Hadoop 3 · Hive · HBase · MongoDB · Cassandra · Neo4j

---

## 0. Qué se evalúa (y qué no)

Este TP **no pide teoría copiada**. Se evalúa que el squad sepa **construir un producto de datos** como se trabaja en 2026:

| Se evalúa | No se evalúa |
|---|---|
| Pipelines reproducibles (repo, Docker, notebooks ejecutados) | Resúmenes de Wikipedia / slides |
| Decisiones de arquitectura justificadas con evidencia | Definiciones memorizadas de las 4 V |
| Calidad, contrato y linaje del dato | Capturas sueltas sin contexto |
| Uso correcto de cada motor para su carga de trabajo | “Hice el comando y funcionó” sin producto |
| Demo que un stakeholder entendería | Informe académico de 30 páginas |

**Uso de IA (obligatorio declarar):** Cursor, Copilot, ChatGPT u otras herramientas **están permitidas**. Cada carpeta debe incluir un archivo `AI_LOG.md` con: prompt usado, qué se aceptó, qué se corrigió a mano y qué falló. Un pipeline que “compila” pero el squad no puede explicar en la demo **desaprueba esa parte**.

---

## 1. El caso: NexoUrbano

NexoUrbano es una startup de **micromovilidad eléctrica** (e-scooters y e-bikes) que opera en una ciudad latinoamericana. En 2026 el board pide un **lakehouse operacional** para tres frentes:

1. **Operaciones en vivo:** batería, GPS, fallas, clima, fraude de viajes.
2. **Comercial:** pases, recargas, accesorios (catálogo documental).
3. **Crecimiento:** grafo social de riders (amigos, viajes compartidos, intereses) para recomendaciones y detección de collusion.

El squad **es el equipo de datos**. No hay un “informe final”: hay un **producto** con capas Bronze / Silver / Gold, evidencia de cada motor y una demo.

### 1.1 Fuentes reales que deben usarse

El producto se alimenta de **datos reales + datos sintéticos controlados** (el generador del anexo). Mínimo obligatorio:

| Fuente | Tipo | Para qué |
|---|---|---|
| [OpenWeather](https://openweathermap.org/api) o [Open-Meteo](https://open-meteo.com/) (sin API key) | API REST | Velocidad + veracidad (clima vs. demanda) |
| Dataset abierto de bici pública (Citi Bike NYC, EcoBici CABA, o [GBFS](https://github.com/MobilityData/gbfs) de una ciudad) | CSV / JSON | Volumen + variedad |
| Feed social público (Bluesky search API o Mastodon hashtag; **no X/Twitter de pago**) | API / JSON | Velocidad + variedad no estructurada |
| Generador `tools/generar_datos.py` (incluido) | CSV, JSON, logs, SQL | Sensores IoT, catálogo, ventas, grafos, logs de app |
| MySQL local (Docker) | RDBMS | Sqoop / puente relacional |

### 1.2 Los tres casos de las 4 V (obligatorio, aplicados)

Antes de picar código, el squad analiza **tres casos reales** y los **mapea a NexoUrbano**. No es un ensayo: es una **ficha de una página por caso** en `01-discovery/4V.md`.

| Caso ancla | Qué mirar | Transferencia a NexoUrbano |
|---|---|---|
| **Netflix** | catálogo, telemetría de reproducción, personalización | recomendaciones de estaciones / pases |
| **Twitter / X (hoy: Bluesky o Mastodon)** | firehose, texto, bots, veracidad | menciones de la marca, incidentes, sentimiento |
| **Sensores IoT** (flotas, smart city, industrial) | alta frecuencia, drift, fallos de sensor | GPS + batería + temperatura de cada vehículo |

Para **cada caso** completar esta tabla (sin párrafos de relleno):

```
Volumen     → orden de magnitud + unidad (eventos/día, TB/año)
Variedad    → formatos y esquemas (estructurado / semi / no)
Velocidad   → latencia aceptable (batch diario vs. < 5 s)
Veracidad   → 3 riesgos concretos + cómo se mitigan en el pipeline
Complejidad → 1 decisión de modelado que el caso obliga (no “es complejo”)
```

Al final: **una sola arquitectura medallion** de NexoUrbano (diagrama en `01-discovery/arquitectura.md`) donde cada V queda asignada a un motor del TP.

---

## 2. Reglas de trabajo 2026 (obligatorias)

1. **Git es la fuente de verdad.** Commits atómicos, PRs internos (aunque el repo sea de 3 personas), `main` protegida por convención. Sin ZIP de última hora.
2. **Infra como código local.** Todo lo on-prem (Hadoop, Hive, HBase, MySQL, Mongo, Cassandra, Neo4j, Flume) corre con el `docker-compose.yml` del repo o un sandbox documentado. El README debe permitir a un docente levantar el entorno en **< 20 minutos**.
3. **Secretos fuera del repo.** `.env` + `.env.example`. Ninguna API key commiteada.
4. **Medallion + contrato de datos.** Cada dataset Silver/Gold declara schema, grano, clave, SLA de frescura y owner en `contratos/`.
5. **Evidencia ejecutable, no screenshot-only.** Notebooks con celdas ejecutadas (o `evidence/*.log` de comandos). Las capturas del portal Azure van en `evidence/azure/` **acompañadas** del comando o paso.
6. **Un motor, una carga de trabajo.** Prohibido usar Mongo “para todo”. Cada tecnología del temario **debe aparecer en el producto** con una justificación de 5 líneas en `ADR/`.
7. **Costo cero o free tier.** Azure Free / Databricks Community o trial. Si un servicio pide tarjeta, documentar el plan B (Azurite / MinIO como Data Lake local + equivalencia).

---

## 3. Entregable único

Un repositorio con esta estructura (nombres exactos):

```
nexourbano/
├── README.md                 # cómo levantar todo + recorte de 8 min de demo
├── AI_LOG.md                 # bitácora de IA del squad
├── contratos/                # data contracts YAML
├── ADR/                      # 6–8 decisiones de arquitectura
├── 01-discovery/
├── 02-azure-datalake/
├── 03-databricks/
├── 04-hadoop/
│   ├── hdfs/
│   ├── mapreduce/
│   ├── hbase/
│   ├── hive/
│   ├── pig/
│   ├── sqoop/
│   └── flume/
├── 05-spark/
│   ├── batch/
│   └── streaming/
├── 06-mongodb/
├── 07-cassandra/
├── 08-neo4j/
├── 09-producto/              # notebook Gold + métricas de negocio
├── data/                     # muestras pequeñas versionadas (< 20 MB)
├── tools/                    # generador y utilidades
├── docker-compose.yml
├── .env.example
└── evidence/
```

**Además:** video o walkthrough de **8 minutos** (link en el README). Guion obligatorio:

1. Problema de negocio (45 s)
2. Arquitectura y 4 V (60 s)
3. Ingesta lakehouse (Azure + Databricks) (90 s)
4. Hadoop como capa batch heredada que todavía mueve valor (90 s)
5. Spark batch + streaming (90 s)
6. Polyglot: Mongo + Cassandra + Neo4j, cada uno con 1 query que importe (90 s)
7. Métricas Gold y qué decidiría un ops manager mañana (45 s)

---

## 4. Calendario de 4 sprints

Fechas relativas al **día 0 = publicación del TP**. Ajustar a la fecha real del curso.

| Sprint | Días | Resultado que debe existir en `main` |
|---|---|---|
| **S1 · Lakehouse y descubrimiento** | 0–7 | 4 V + arquitectura + contenedor Azure + notebook Databricks con API y CSV |
| **S2 · Hadoop que todavía paga la cuenta** | 8–14 | HDFS + WordCount + HBase + Hive + Pig + Sqoop + Flume sobre datos de NexoUrbano |
| **S3 · Spark batch y tiempo real** | 15–21 | DataFrames Gold + Structured Streaming con ventanas |
| **S4 · Polyglot + producto** | 22–28 | Mongo + Cassandra + Neo4j + notebook de producto + video |
| **Cierre duro** | día 30 | tag `entrega-final` · README listo · nada en branches sueltas |

**Checkpoint de docentes (recomendado, no bloquea):** review de 15 min al cierre de S1 y S2. Si el squad va tarde en S1, recortar alcance Gold, **no** saltarse motores.

---

## 5. Consignas por módulo (todas prácticas)

Cada módulo tiene: **objetivo de producto**, **hecho mínimo verificable (HMV)** y **señal de 2026**. Si falta el HMV, el módulo vale 0.

---

### M1 · Big Data: 4 V y complejidad del dato
**Producto:** ficha operativa + backlog del lakehouse.

**Hacer:**
1. Completar las 3 fichas 4 V (Netflix, red social, IoT) en `01-discovery/4V.md`.
2. Bajar **un dataset abierto real** de movilidad (mínimo 100 k filas o justificar por qué no) y perfilarlo: nulos, tipos, cardinalidad, outliers, drift temporal. Script en Python (`01-discovery/perfilado.py` o notebook).
3. Escribir el **contrato Bronze** del dataset de viajes: grano, timezone, claves, reglas de rechazo.
4. Discusión de squad (acta de 15 líneas): qué V es la más cara de mentir en NexoUrbano y qué control van a implementar.

**HMV:** un dataset perfilado + contrato YAML + 3 fichas. Sin PDF de teoría.

**Señal 2026:** el perfilado se versiona; el contrato se trata como código.

---

### M2 · Azure Data Lake
**Producto:** zona de landing cloud (o equivalente local documentado).

**Hacer:**
1. Exploración del portal Azure (cuenta free). Capturar **suscripción, región y SKU** usados.
2. Crear Storage Account con **hierarchical namespace** (ADLS Gen2).
3. Crear contenedores `bronze`, `silver`, `gold` (nombres exactos).
4. Subir el CSV perfilado a `bronze/viajes/yyyy/mm/dd/`.
5. Documentar **on-prem vs. cloud** en `ADR/001-datalake.md` con 4 criterios medibles: costo de 1 TB/mes, egress, IAM, time-to-first-byte. Números reales o cotización del calculador de Azure (link + fecha).

**HMV:** URI `abfss://bronze@<cuenta>.dfs.core.windows.net/viajes/...` (o `s3a://` / MinIO equivalente) pegada en el README, más captura de listado del contenedor.

**Plan B:** Azurite o MinIO con layout idéntico. El ADR debe decir qué cambiaría en producción Azure.

---

### M3 · Databricks y volcado de Internet
**Producto:** primer job de ingesta desde la web.

**Hacer:**
1. Workspace Databricks (Community Edition, Free, o Azure Databricks trial).
2. Notebook colaborativo `03-databricks/01_ingesta.py` (Python) que:
   - lea el CSV de viajes (DBFS / Volume / ADLS montado);
   - consulte **Open-Meteo** (histórico o forecast) para las coordenadas de 3 estaciones;
   - consulte un **endpoint social público** (Bluesky `app.bsky.feed.searchPosts` o Mastodon);
   - persista resultado en Delta (o Parquet si CE no da Delta) en Silver.
3. Exploración con Python mínimo: `shape`, nulos, 5 métricas de negocio (viajes/hora, duración p50/p95, estaciones top).
4. El notebook debe ser **re-ejecutable**. Parámetros de ciudad y fecha arriba.

**HMV:** notebook exportado (`*.ipynb` o `*.py` + HTML) con outputs visibles y una tabla Silver de clima×viajes.

**Señal 2026:** ingestión por API + dataset abierto, no Excel bajado a mano como único origen.

---

### M4 · Cierre U1 + Hadoop (entorno)
**Producto:** sandbox Hadoop reproducible + quiz interno.

**Hacer:**
1. Levantar Hadoop con Docker (sandbox del curso o `apache/hadoop` + servicios del compose). Documentar versiones.
2. `hdfs dfs -ls /` funcionando. Pegar log en `evidence/hadoop/bootstrap.log`.
3. Quiz interno de 4 V (Kahoot, Mentimeter o formulario). Exportar ranking a `04-hadoop/quiz.csv`. Es una **actividad de squad**, no un informe.
4. En `ADR/002-hadoop-hoy.md`: **por qué Hadoop sigue en el diseño** (almacenamiento barato, Hive legado, cumplimiento) y **qué no volverían a poner en MapReduce** en 2026.

**HMV:** compose healthy + log HDFS + ADR.

---

### M5 · HDFS y MapReduce
**Producto:** lago on-prem con un job batch canónico **sobre datos de NexoUrbano**.

**Hacer:**
1. Práctica HDFS (comandos reales, log pegado):
   ```text
   hdfs dfs -mkdir -p /nexo/bronze/viajes /nexo/bronze/logs
   hdfs dfs -put data/viajes_muestra.csv /nexo/bronze/viajes/
   hdfs dfs -ls -R /nexo
   hdfs dfs -cp ... /nexo/bronze/viajes/backup/
   hdfs dfs -mv ...
   hdfs dfs -du -h /nexo
   ```
2. Explicar en 8 líneas (comentario en el log): tamaño de bloque que usaron, factor de replicación, qué pasa si muere 1 DataNode.
3. **WordCount no sobre lorem ipsum.** Input: archivo de **tickets de soporte + menciones sociales** (`data/texto_ops.txt` generado). Output en `/nexo/gold/wordcount/`. Top 20 términos operativos (batería, freno, app, cobro, etc.).
4. Entregar el comando de submit y el output (`part-r-00000`).

**HMV:** árbol HDFS + output MapReduce de texto de negocio.

---

### M6 · HBase y Hive
**Producto:** serving layer columnar + warehouse SQL sobre HDFS.

**Hacer — HBase:**
1. Tabla `nexo:vehiculos` (o namespace default `vehiculos`) con column families `id` y `telemetry`.
2. Insertar ≥ 50 vehículos (id, modelo, soh_bateria, ultimo_gps, estado).
3. `get` de un rowkey y `scan` con filtro de estado `mantenimiento`.
4. Justificar rowkey (evitar hotspot). 5 líneas en `ADR/003-hbase-rowkey.md`.

**Hacer — Hive:**
1. Tabla **externa** `bronze_viajes` apuntando al CSV en HDFS.
2. Tabla **interna** `silver_viajes` particionada por `fecha`.
3. HiveQL:
   - `SELECT` estaciones con más destinos;
   - `WHERE` viajes > 45 min;
   - `GROUP BY` hora × barrio (o station_id) con conteo y duración media.
4. Explicar externa vs. interna **con lo que les pasó al hacer DROP** (probarlo).

**HMV:** scripts `hbase_nexo.txt` + `hive_nexo.sql` + resultados.

---

### M7 · Pig, Sqoop y Flume
**Producto:** tres conectores “legado que sigue en bancos y telcos”.

**Hacer — Pig:**
1. Script Pig Latin que lea viajes, filtre duraciones absurdas (`duration_s < 30` o `> 4 h`), calcule distancia nula y escriba `clean_viajes`.
2. En el mismo ADR: **reimplementar esas 10 líneas en Spark** (sí, duplicado a propósito) y decir cuál dejarían en producción 2026.

**Hacer — Sqoop:**
1. MySQL Docker con tabla `crm.riders` (el generador la crea).
2. Import a HDFS `/nexo/bronze/riders_sqoop/`.
3. Si Sqoop no levanta en el sandbox: **plan B documentado** = `spark.read.jdbc` equivalente, más nota de por qué Sqoop aparece en RFPs viejos. El HMV es la tabla en HDFS, no el logo de Sqoop.

**Hacer — Flume:**
1. Config que tailee `data/app.log` (el generador lo produce en loop opcional) hacia HDFS `/nexo/bronze/logs/`.
2. Simular 200 líneas de eventos `ride_start`, `ride_end`, `payment_fail`.
3. Contraste de 8 líneas: Flume vs. Structured Streaming vs. Kafka (qué usarían en un diseño greenfield 2026).

**HMV:** script Pig + datos CRM en HDFS + agent Flume o simulación equivalente con evidencia.

---

### M8 · Spark: batch y DataFrames
**Producto:** capa Gold analítica.

**Hacer:**
1. Cargar viajes (CSV grande generado, objetivo **≥ 1 millón de filas**; si el hardware no da, 200 k y justificar). Preferir Parquet/Delta en Silver.
2. Transformaciones: filtros de calidad, join con clima (M3) y con riders (M7), agrupaciones, ordenamiento.
3. Métricas Gold mínimas (tabla `gold_ops_diaria`):
   - viajes, minutos, km (si hay coords);
   - % viajes con clima adverso;
   - top 10 estaciones origen;
   - revenue estimado (join a pases/productos si ya está Mongo; si no, tarifa plana documentada).
4. Escribir a Delta o Parquet en Gold (HDFS o ADLS o Volume).
5. Mostrar el **plan físico** (`explain`) de 1 query y marcar 1 cosa que Catalyst reescribió o que ustedes cachearon.

**HMV:** job Spark reproducible (`05-spark/batch/job_gold.py`) + tabla Gold + `explain` pegado.

---

### M9 · Spark Streaming
**Producto:** alerta operacional en ventana de tiempo.

**Hacer:**
1. Structured Streaming leyendo:
   - **opción A:** socket (`nc -lk 9999`) con eventos JSON; o
   - **opción B:** directorio de archivos que el generador va soltando cada 2 s (`data/stream/`).
2. Parseo JSON: `ts`, `vehicle_id`, `event`, `battery_pct`.
3. Ventana de **2 minutos**, slide **30 s**. Contar `payment_fail` y `battery_low`.
4. Watermark de 1 minuto. Documentar qué evento llega tarde y se descarta.
5. Sink: consola + parquet/delta `gold_alerts`.
6. Caso de uso escrito en 6 líneas: fraude de “viaje fantasma” o alerta de flota caída.

**HMV:** captura de 30 s de la consola con ventanas cambiando + código.

**No hace falta Kafka** (opcional extra). Si lo usan, mejor, no obligatorio.

---

### M10 · MongoDB: documentos y CRUD
**Producto:** catálogo comercial.

**Hacer:**
1. Colección `productos` (pases, recargas, cascos, seguros) con documentos **anidados** (precios por ciudad, tags, stock).
2. CRUD completo: `insertMany` ≥ 30 docs, `find` con filtro, `updateOne` de precios, `deleteOne` de un SKU descontinuado.
3. Índice en `tags` o `ciudad` + `categoria`. Medir `explain("executionStats")` **antes y después**. Pegar times.
4. Comparativa SQL vs. documento: 1 query que en SQL sería 3 joins y en Mongo es un doc. 8 líneas.

**HMV:** script `06-mongodb/crud.js` (o pymongo) + stats de índice.

---

### M11 · MongoDB: agregaciones
**Producto:** analítica comercial.

**Hacer:** pipeline sobre `ventas` (generador):
1. `$match` último trimestre (o último mes sintético).
2. total por categoría.
3. top 5 productos.
4. ventas por mes.
5. `$lookup` a `productos`.
6. `$project` del documento de salida limpio.
7. Export a `09-producto/ventas_agg.json`.

Extra bienvenido (no obligatorio): 1 query geoespacial (`2dsphere` de tiendas / estaciones).

**HMV:** pipeline en archivo + JSON exportado.

---

### M12 · Cassandra
**Producto:** series temporales de sensores.

**Hacer:**
1. Keyspace `nexo` con RF=1 en sandbox (documentar que en prod RF≥3).
2. Tabla `lecturas_vehiculo` modelada **por query**:
   - partition key: `vehicle_id`
   - clustering: `ts DESC`
   - columnas: `lat`, `lon`, `battery_pct`, `temp_c`, `status`
3. Insertar ≥ 5 000 lecturas (generador).
4. CQL:
   - últimas 20 lecturas de un vehículo;
   - rango horario de un vehículo;
   - **mostrar el error** de una query que no respeta la PK (eso es el punto).
5. En ADR: consistencia `ONE` vs. `QUORUM` para “batería en vivo” vs. “facturación”.

**HMV:** `07-cassandra/schema.cql` + `queries.cql` + un COPY o script de carga.

---

### M13 · Neo4j
**Producto:** grafo social y recomendaciones.

**Hacer:**
1. Nodos: `Rider`, `Interes`, `Estacion` (u `Zona`).
2. Relaciones: `AMIGO_DE`, `VIAJO_CON`, `LE_GUSTA`, `SALE_DE`.
3. Carga ≥ 80 riders (generador).
4. Cypher:
   - amigos en común;
   - camino más corto entre 2 riders;
   - recomendación: “riders que viajan las mismas estaciones y no son amigos”;
   - (extra) patrón de fraude: 3 riders que siempre empiezan viajes en la misma estación en < 2 min.
5. Captura del grafo (Bloom o Browser) en `evidence/neo4j/`.

**HMV:** `08-neo4j/grafo.cypher` + 4 queries con resultado pegado.

---

### M14 · Producto Gold (cierre, no es teoría)
**Producto:** una sola historia de decisión.

Notebook `09-producto/decision_ops.py` (o sql + markdown) que cruce **al menos 4 motores** y responda **estas 5 preguntas de negocio**:

1. ¿Dónde reponer scooters en las próximas 3 h si llueve? (Spark Gold + clima)
2. ¿Qué vehículo hay que retirar hoy? (Cassandra última lectura + umbral SOH)
3. ¿Qué pase venderle a un rider? (Mongo aggregations + grafo de intereses)
4. ¿Hay collusion / fraude social? (Neo4j)
5. ¿Cuánto se facturó vs. tickets de soporte “cobro”? (Hive/Spark + WordCount)

Si una pregunta queda floja, decir **qué dato faltó**, no inventar el número.

---

## 6. Rubrica (100 puntos)

| Bloque | Pts | 0 | 6–7 / 10 relativo | 10 / 10 relativo |
|---|---|---|---|---|
| **A. Repo, contratos, ADR, AI_LOG** | 10 | ZIP, secretos, sin README | Corre con pasos a mano | `docker compose up` + README de demo |
| **B. 4 V + arquitectura medallion** | 8 | Texto genérico | Tablas completas sin transferencia | Cada V ancla un motor del producto |
| **C. Azure Data Lake** | 8 | No hay landing | Contenedores y 1 CSV | Layout `bronze/silver/gold` + ADR costo |
| **D. Databricks + APIs** | 10 | Notebook vacío | CSV explorado | API clima + social + persistencia Silver |
| **E. HDFS + MapReduce** | 8 | Comandos sueltos | HDFS ok, WordCount genérico | WordCount de ops NexoUrbano |
| **F. HBase + Hive** | 8 | Una de las dos | Ambas básicas | Externa vs interna demostrada + rowkey |
| **G. Pig + Sqoop + Flume** | 8 | 1 de 3 | 3 con plan B honesto | Contraste 2026 Spark/JDBC/Kafka |
| **H. Spark batch** | 10 | Filtros de tutorial | Joins y agrupaciones | ≥ 200 k filas, Gold, `explain` |
| **I. Spark Streaming** | 8 | Batch disfrazado | Ventanas visibles | Watermark + alerta de negocio |
| **J. MongoDB CRUD + agg** | 8 | CRUD de tutorial | Catálogo + pipeline | Índice medido + JSON Gold |
| **K. Cassandra** | 6 | SQL-like mal modelado | PK correcta | Query anti-patrón mostrada |
| **L. Neo4j** | 6 | Grafo de juguete | Amigos en común | Recomendación o fraude |
| **M. Producto + demo 8 min** | 12 | Recital de herramientas | Métricas aisladas | 5 preguntas de negocio contestadas |

**Aprobado:** 60. **Notable:** 80. **Sobresaliente:** 90 + demo que un gerente de ops seguiría.

**Penalizaciones:**
- API key en Git: **−15**
- Módulo copiado de tutorial sin datos NexoUrbano: **ese módulo vale como máximo 40 %**
- No poder explicar una celda que escribió la IA: **0 en ese bloque**
- Entrega fuera del tag `entrega-final` en fecha: **−10 / día**, tope −30

---

## 7. Criterios de “manera de trabajo 2026” (tie-break)

El docente usa esto para desempatar 8/10 vs 10/10:

1. **Lakehouse, no zoo de dumps.** Bronze crudo, Silver validado, Gold de decisión.
2. **Polyglot con criterio.** Time series → Cassandra; catálogo → Mongo; relaciones → Neo4j; SQL analítico → Spark/Hive; landing → ADLS.
3. **Streaming como ciudadano de primera**, no un `readStream` de 10 segundos para la nota.
4. **Calidad de dato como código** (nulos, rangos, late data), no un párrafo de “veracidad”.
5. **Reproducibilidad.** El docente clona y corre.
6. **Costo y free tier** conscientes.
7. **IA como copiloto auditado**, no como autor fantasma.

---

## 8. Qué puede faltar sin desaprobar (alcance recortable)

Si el hardware o el free tier se caen, recortar **en este orden** (dejar nota en README):

1. Extra geoespacial Mongo
2. Kafka
3. Bloom de Neo4j (Browser alcanza)
4. 1 M de filas → 200 k
5. Azure real → MinIO/Azurite (ADR obligatorio)
6. Sqoop nativo → Spark JDBC (ADR obligatorio)

**No recortable:** 4 V aplicadas, HDFS, un job Spark batch, un streaming con ventanas, un CRUD Mongo, una tabla Cassandra bien modelada, un Cypher de camino, demo.

---

## 9. Integridad académica

- El dataset sintético **debe** generarse con la semilla del padron / nro de grupo (`--seed`) para que no haya dos squads con los mismos IDs.
- Código de tutoriales: citar URL en el header del archivo.
- El video de demo debe mostrar caras o voces del squad (o walkthrough en vivo).

---

## 10. Primera hora (kickoff)

1. Crear repo, agregar compañeros, pegar esta estructura de carpetas.
2. Correr `python tools/generar_datos.py --seed <NRO_GRUPO>`.
3. Escribir `ADR/000-contexto.md`: ciudad elegida, dataset abierto elegido, quién es owner de cada módulo.
4. Reservar cuentas: Azure free, Databricks, Open-Meteo (sin key) u OpenWeather.
5. Levantar `docker compose up -d` y no apagarlo hasta ver MySQL + Mongo + (el resto según máquina).

Cuando el generador termina, el squad ya tiene materia prima para **todos** los módulos. A partir de ahí, el TP es un mes de **ingeniería de producto de datos**, no de coleccionar pantallazos.
