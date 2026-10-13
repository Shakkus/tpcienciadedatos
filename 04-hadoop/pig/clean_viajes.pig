-- Ejecucion reproducible (construye la imagen, procesa el dataset y valida resultados):
-- bash tools/run_pig_m7.sh
-- INPUT y OUTPUT son parametros Pig; el lanzador los resuelve a rutas montadas en el contenedor.

RAW = LOAD '$INPUT' USING PigStorage(',')
    AS (
        ride_id:chararray,
        ts_start:chararray,
        ts_end:chararray,
        duration_s:chararray,
        rider_id:chararray,
        vehicle_id:chararray,
        station_start:chararray,
        station_end:chararray,
        barrio_start:chararray,
        barrio_end:chararray,
        lat_start:chararray,
        lon_start:chararray,
        lat_end:chararray,
        lon_end:chararray,
        km:chararray,
        fare_ars:chararray,
        clima_codigo:chararray
    );

NO_HEADER = FILTER RAW BY ride_id != 'ride_id';

TYPED = FOREACH NO_HEADER GENERATE
    ride_id,
    ts_start,
    ts_end,
    (int)duration_s AS duration_s,
    rider_id,
    vehicle_id,
    station_start,
    station_end,
    barrio_start,
    barrio_end,
    (double)lat_start AS lat_start,
    (double)lon_start AS lon_start,
    (double)lat_end AS lat_end,
    (double)lon_end AS lon_end,
    km,
    fare_ars,
    clima_codigo;

VALID = FILTER TYPED BY duration_s >= 30 AND duration_s <= 14400;

CLEAN = FOREACH VALID GENERATE
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
    (
        km IS NULL OR km == ''
        ? 6371.0 * 2.0 * ASIN(SQRT(
            SIN((lat_end - lat_start) * 0.017453292519943295 / 2.0)
            * SIN((lat_end - lat_start) * 0.017453292519943295 / 2.0)
            + COS(lat_start * 0.017453292519943295) * COS(lat_end * 0.017453292519943295)
            * SIN((lon_end - lon_start) * 0.017453292519943295 / 2.0)
            * SIN((lon_end - lon_start) * 0.017453292519943295 / 2.0)
        ))
        : (double)km
    ) AS km,
    fare_ars,
    clima_codigo;

STORE CLEAN INTO '$OUTPUT' USING PigStorage('\t');
