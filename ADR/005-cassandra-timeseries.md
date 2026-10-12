# ADR 005: Consistencia para series temporales en Cassandra

## Contexto
La tabla `lecturas_vehiculo` almacena telemetría de la flota por vehículo y timestamp. Para la operación en vivo, el valor crítico es la última lectura por vehículo y la disponibilidad del dato. Para procesos de facturación o auditoría, la consistencia exacta es más importante.

## Decisión
Se usa `ONE` para lecturas en vivo de batería y estado operativo, priorizando latencia y disponibilidad local. Para procesos con mayor requerimiento de exactitud, como facturación o eventos de negocio de control, se recomienda `QUORUM` en la capa de escritura/lectura transaccional.

## Consecuencia
La consulta por vehículo tiene buen rendimiento y la consulta global por tiempo sin partición queda explícitamente descartada como patrón incorrecto para este modelo.
