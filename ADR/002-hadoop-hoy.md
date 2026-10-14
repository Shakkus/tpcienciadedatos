# ADR 002 — Hadoop en el diseño de NexoUrbano

## Contexto

El TP requiere practicar un stack on-prem heredado que sigue apareciendo en
plataformas de datos existentes. El entorno local usa la imagen oficial
`apache/hadoop:3.4.1` con un NameNode, un DataNode, un ResourceManager y un
NodeManager.

## Decisión

Se conserva Hadoop para demostrar almacenamiento distribuido compatible con
HDFS y ejecutar procesamiento batch MapReduce sobre datos operativos. También
permite integrar y consultar formatos y herramientas heredadas del ecosistema,
como Hive y HBase, relevantes al mantener plataformas ya instaladas.

El Compose local es de un solo nodo, con replicación 1: sirve para aprendizaje
y reproducibilidad del TP, no como diseño de alta disponibilidad.

## Qué no elegiríamos para un sistema nuevo

No elegiríamos MapReduce para analítica interactiva, procesamiento de baja
latencia, streaming ni transformaciones iterativas. Para esos casos preferimos
servicios administrados, motores SQL interactivos o Spark/Structured
Streaming, según el requisito. En este TP mantenemos WordCount porque permite
mostrar el modelo batch y la ejecución distribuida de forma verificable.

## M7 — Pig vs. Spark para limpiar viajes

El script `04-hadoop/pig/clean_viajes.pig` carga el CSV de viajes, elimina el
encabezado, conserva duraciones entre 30 y 14.400 segundos inclusive y calcula
`km` con Haversine cuando el valor de origen es nulo o vacío. Escribe el
resultado delimitado por tabuladores en la ruta `OUTPUT` indicada al ejecutar
Pig; en HDFS usamos `/nexo/silver/clean_viajes/`.

La misma transformación en Spark queda así (se asume una sesión `spark`
disponible):

```python
from pyspark.sql import functions as F

trips = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
clean = trips.filter(F.col("duration_s").between(30, 14400))
a = F.pow(F.sin(F.radians(F.col("lat_end") - F.col("lat_start")) / 2), 2) + (
    F.cos(F.radians(F.col("lat_start"))) * F.cos(F.radians(F.col("lat_end")))
    * F.pow(F.sin(F.radians(F.col("lon_end") - F.col("lon_start")) / 2), 2))
km_calc = 6371.0 * 2 * F.asin(F.sqrt(a))
missing_km = F.col("km").isNull() | (F.trim(F.col("km").cast("string")) == "")
clean = clean.withColumn("km", F.when(missing_km, km_calc).otherwise(F.col("km")))
clean.write.mode("overwrite").option("header", True).csv(output_path)
```

Para producción en 2026 elegiríamos Spark: permite integrar esta regla con el
resto de las transformaciones batch y el ecosistema analítico del proyecto. Pig
queda como demostración de mantenimiento e interoperabilidad con plataformas
Hadoop heredadas. En la semilla actual hay 250.000 viajes; el contrato reporta
3.323 duraciones menores de 30 s y 1.658 mayores de 14.400 s, por lo que se
esperan 245.019 filas válidas. El perfilado no encontró `km` nulos en esa
semilla, así que esa imputación se implementa, pero no se ejercita con esos
datos.

### Reproducibilidad de Pig

Para ejecutar el script no es requisito instalar Pig en la máquina de cada
integrante. `04-hadoop/pig/Dockerfile` fija Pig 0.18.0 sobre Hadoop 3.4.1 y
Java incluido en la imagen base. Ejecutar `bash tools/run_pig_m7.sh` desde la
raíz del repositorio construye el contenedor si hace falta, monta el CSV y el
script, y deja cada resultado en `.staging/pig/clean_viajes_<fecha>/`. El
comando compara el número de filas generadas con las que cumplen el filtro de
duración en el CSV. También ejecuta el fixture de `km` vacío en
`04-hadoop/pig/testdata/km_null.csv` y valida que el resultado sea una distancia
positiva. La última corrida produjo 245.019 filas (igual al conteo esperado)
y calculó 2,141 km para el fixture de coordenadas. Registra ambas pruebas en
`evidence/hadoop/pig.log`.

## M7 — Sqoop: importación CRM a HDFS

MySQL Compose inicializa la base `crm` y su tabla `riders` desde
`data/mysql/crm_riders.sql`. En este entorno el plan B acordado es Spark JDBC:
`04-hadoop/sqoop/import_riders_spark.py` lee la tabla y la escribe en Parquet
en `hdfs://namenode:9000/nexo/bronze/riders_sqoop`. El lanzador
`tools/import_riders_spark_docker.sh` ejecuta Spark en un contenedor conectado
a la misma red Docker que Hadoop, por lo que puede resolver `mysql` y
`namenode`. Requiere que los servicios MySQL y Hadoop estén iniciados.
MySQL publica el puerto del contenedor 3306 como puerto host 3307 para evitar
conflictos con servicios locales; dentro de la red Docker se usa el puerto 3306.

Ejecutar desde la raíz del repositorio:

```bash
docker compose up -d mysql hadoop-namenode hadoop-datanode hadoop-resourcemanager hadoop-nodemanager
docker compose ps
bash tools/import_riders_spark_docker.sh
```

Esperar a que MySQL figure como `healthy` antes de lanzar el import.
La salida se crea solo si todavía no existe; para repetir una importación,
elegir otra ruta o borrar deliberadamente el destino existente en HDFS. Se
verifica el resultado con:

```bash
docker compose exec hadoop-namenode hdfs dfs -ls /nexo/bronze/riders_sqoop
docker compose exec hadoop-namenode hdfs dfs -count /nexo/bronze/riders_sqoop
```

Sqoop aparece en RFPs antiguos porque fue una herramienta habitual para
transferencias batch de bases relacionales a Hadoop/HDFS, integrando lecturas
JDBC con trabajos MapReduce. Para este TP se elige Spark JDBC como alternativa
ejecutable y coherente con el procesamiento analítico restante; el HMV exigido
es la copia de `crm.riders` en HDFS, no la instalación de Sqoop. La ejecución
importó 120 filas, coincidentes con el conteo de `crm.riders`; HDFS contiene
el archivo Parquet y `_SUCCESS`. El log de la ejecución está en
`evidence/hadoop/sqoop.log`.

## M7 — Flume para eventos operativos

`04-hadoop/flume/flume.conf` define el agente que sigue las nuevas líneas de
`/opt/nexo/data/app.log` con `tail -F` y las escribe como texto en HDFS bajo
`/nexo/bronze/logs/`, particionadas por fecha. El agente necesita Flume 1.11,
los jars cliente de HDFS y acceso de red a `namenode:9000`. Para verificar el
HMV sin instalar Flume localmente, `tools/simulate_flume_200.py` selecciona
eventos del JSONL generado, escribe 200 líneas en HDFS y compara el contenido
leído con lo enviado. Requiere el Hadoop Compose iniciado y Docker disponible:

```bash
python tools/simulate_flume_200.py
```

La simulación verifica 67 `ride_start`, 67 `ride_end` y 66 `payment_fail`;
cada ejecución crea un directorio único debajo de
`/nexo/bronze/logs/simulation/` y registra el resultado en
`evidence/hadoop/flume.log`. Es una reproducción del flujo hacia el sink HDFS,
no una afirmación de que el agente Flume se ejecutó.

Comparación greenfield 2026 (ocho líneas):

```text
1. Flume: agente ligero para seguir archivos de log y enviar eventos a HDFS.
2. Flume: útil al mantener Hadoop legado; el canal en memoria puede perder eventos al fallar.
3. Structured Streaming: procesamiento continuo con estado, ventanas y salida integrada a Spark.
4. Structured Streaming: preferible cuando las reglas analíticas ya viven en Spark.
5. Kafka: plataforma distribuida de eventos con retención, replay y desacoplamiento productor-consumidor.
6. Kafka: elección para múltiples consumidores y eventos que deban reproducirse.
7. Flume no aporta el log durable ni el ecosistema de consumidores de Kafka.
8. Para greenfield elegiríamos Kafka como backbone y Structured Streaming para transformar; no Flume.
```
