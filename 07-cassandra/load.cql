USE nexo;

COPY lecturas_vehiculo (
    vehicle_id,
    ts,
    lat,
    lon,
    battery_pct,
    temp_c,
    status
)
FROM '/tmp/lecturas_sensores.csv'
WITH HEADER = TRUE
AND DELIMITER = ',';
