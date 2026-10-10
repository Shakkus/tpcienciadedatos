CREATE KEYSPACE IF NOT EXISTS nexo
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE nexo;

CREATE TABLE IF NOT EXISTS lecturas_vehiculo (
    vehicle_id text,
    ts timestamp,
    lat double,
    lon double,
    battery_pct double,
    temp_c double,
    status text,
    PRIMARY KEY ((vehicle_id), ts)
) WITH CLUSTERING ORDER BY (ts DESC);
