-- M6 · Hive — NexoUrbano
--
-- Objetivo:
-- - external Bronze sobre HDFS
-- - internal Silver particionada por fecha
-- - consultas que cubren exactamente el enunciado de M6

DROP TABLE IF EXISTS silver_viajes;
DROP TABLE IF EXISTS bronze_viajes;

CREATE EXTERNAL TABLE bronze_viajes (
    ride_id STRING,
    ts_start STRING,
    ts_end STRING,
    duration_s INT,
    rider_id STRING,
    vehicle_id STRING,
    station_start STRING,
    station_end STRING,
    barrio_start STRING,
    barrio_end STRING,
    lat_start DOUBLE,
    lon_start DOUBLE,
    lat_end DOUBLE,
    lon_end DOUBLE,
    km DOUBLE,
    fare_ars DOUBLE,
    clima_codigo INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/nexo/bronze/m6_viajes/'
TBLPROPERTIES ("skip.header.line.count"="1");

DESCRIBE FORMATTED bronze_viajes;

CREATE TABLE silver_viajes (
    ride_id STRING,
    ts_start STRING,
    ts_end STRING,
    duration_s INT,
    rider_id STRING,
    vehicle_id STRING,
    station_start STRING,
    station_end STRING,
    barrio_start STRING,
    barrio_end STRING,
    lat_start DOUBLE,
    lon_start DOUBLE,
    lat_end DOUBLE,
    lon_end DOUBLE,
    km DOUBLE,
    fare_ars DOUBLE,
    clima_codigo INT
)
PARTITIONED BY (fecha STRING)
STORED AS PARQUET;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE silver_viajes PARTITION (fecha)
SELECT
    ride_id,
    ts_start,
    ts_end,
    duration_s,
    rider_id,
    vehicle_id,
    station_start,
    station_end,
    barrio_start,
    barrio_end,
    lat_start,
    lon_start,
    lat_end,
    lon_end,
    km,
    fare_ars,
    clima_codigo,
    substr(ts_start, 1, 10) AS fecha
FROM bronze_viajes
WHERE duration_s >= 30
  AND duration_s <= 14400
  AND ts_end >= ts_start;

SELECT station_start AS estacion,
       COUNT(*) AS destinos_distintos
FROM (
    SELECT DISTINCT station_start, station_end
    FROM silver_viajes
) destinos
GROUP BY station_start
ORDER BY destinos_distintos DESC, estacion
LIMIT 10;

SELECT vehicle_id, AVG(duration_s) AS duracion_promedio_seg,
       COUNT(*) AS viajes_largos
FROM silver_viajes
WHERE duration_s > 2700
GROUP BY vehicle_id
ORDER BY viajes_largos DESC, duracion_promedio_seg DESC
LIMIT 10;

SELECT substr(ts_start, 12, 2) AS hora_utc,
       barrio_start,
       COUNT(*) AS viajes,
       ROUND(AVG(duration_s)/60.0, 2) AS minutos_promedio
FROM silver_viajes
GROUP BY substr(ts_start, 12, 2), barrio_start
ORDER BY viajes DESC, hora_utc, barrio_start
LIMIT 20;

SHOW TABLES;
DESCRIBE FORMATTED silver_viajes;
