# Arquitectura medallion — NexoUrbano

Lakehouse operacional para operaciones en vivo, comercial y crecimiento (grafo social). Cada **V** queda anclada en al menos un motor del TP.

## Diagrama (vista lógica)

```mermaid
flowchart TB
  subgraph Fuentes
    GEN[generar_datos.py CSV JSON logs]
    API_CLI[Open-Meteo REST]
    API_SOC[Bluesky / Mastodon]
    MYSQL[(MySQL crm.riders)]
    OPEN[Dataset abierto movilidad GBFS / EcoBici]
  end

  subgraph Bronze["Bronze — volumen + variedad"]
    ADLS[(ADLS Gen2 / MinIO bronze/)]
    HDFS[(HDFS /nexo/bronze)]
    FLUME[Flume app.log]
  end

  subgraph Silver["Silver — veracidad"]
    DBX[Databricks Delta/Parquet]
    PIG[Pig clean_viajes]
    HIVE_EXT[Hive bronze_viajes externa]
  end

  subgraph Gold["Gold — decisión"]
    SPARK_G[Spark gold_ops_diaria]
    STREAM[Spark gold_alerts ventanas]
    WC[MapReduce wordcount ops]
  end

  subgraph Serving["Polyglot — variedad + velocidad"]
    HBASE[(HBase vehiculos estado)]
    CASS[(Cassandra lecturas_vehiculo)]
    MONGO[(Mongo productos ventas)]
    NEO[(Neo4j riders grafo)]
  end

  GEN --> ADLS
  GEN --> HDFS
  OPEN --> ADLS
  API_CLI --> DBX
  API_SOC --> DBX
  MYSQL --> HDFS
  FLUME --> HDFS

  ADLS --> DBX
  HDFS --> HIVE_EXT
  HDFS --> PIG
  PIG --> HIVE_EXT
  DBX --> SPARK_G
  HIVE_EXT --> SPARK_G
  HDFS --> WC

  GEN --> CASS
  GEN --> MONGO
  GEN --> NEO
  HDFS --> STREAM

  SPARK_G --> Gold
  STREAM --> Gold
  CASS --> Serving
  HBASE --> Serving
  MONGO --> Serving
  NEO --> Serving
```

## Tabla V → motor

| V | Rol en NexoUrbano | Motor principal | Capa |
|---|---|---|---|
| **Volumen** | Millones de viajes y decenas de millones de lecturas/año | MinIO/ADLS `bronze`, HDFS, Spark batch | Bronze → Gold |
| **Variedad** | Tabular, documentos, series, grafo, texto, APIs | Mongo, Cassandra, Neo4j, Hive, MapReduce, Databricks | Bronze + serving |
| **Velocidad** | Batería baja, `payment_fail`, última posición | Spark Structured Streaming, Cassandra | Silver/Gold + serving |
| **Veracidad** | Contrato, rechazos, watermark, cuarentena sensores | YAML `bronze_viajes`, Pig/Spark filters, streaming watermark | Silver |

## Flujo por caso de negocio

| Frente | Entrada | Proceso | Salida |
|---|---|---|---|
| Operaciones | `lecturas_sensores`, `stream/`, `app.log` | Cassandra + Streaming + HBase | Retiro de vehículo, `gold_alerts` |
| Comercial | `productos.json`, `ventas.csv`, MySQL riders | Mongo CRUD + aggs, Sqoop/JDBC | Recomendación de pase, revenue Gold |
| Crecimiento | `grafo.json` | Neo4j Cypher | Amigos en común, fraude collusion, estaciones sugeridas |
| Contexto externo | Open-Meteo, posts sociales | Databricks M3 | Silver clima×viajes, señal de incidente |

## Convenciones de rutas

| Zona | Cloud (objetivo) | Local (repo / sandbox) |
|---|---|---|
| Bronze viajes | `abfss://bronze@<cuenta>.dfs.core.windows.net/viajes/yyyy/mm/dd/` | `s3a://bronze/viajes/` (MinIO) o `hdfs:///nexo/bronze/viajes` |
| Silver | Delta en contenedor `silver` | Parquet en HDFS o Volume Databricks |
| Gold | `gold/ops_diaria`, `gold/alerts` | `05-spark/batch`, `05-spark/streaming` outputs |

Frescura referencia: viajes **batch diario 06:00 ART** (`contratos/bronze_viajes.yaml`); alertas streaming **ventana 2 min**.
