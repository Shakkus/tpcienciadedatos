# ADR 003 — Rowkey en HBase para NexoUrbano

## Contexto

La tabla `vehiculos` en HBase debe servir al estado actual de la flota y a consultas por vehículo y tiempo. Si el rowkey no está pensado para la lectura, se producen hotspots y scans costosos.

## Decisión

Usamos rowkeys en formato `vehicle_id|yyyyMMdd|HHmmss` para agrupar por vehículo y, al mismo tiempo, mantener una secuencia temporal por cada entidad. Esto permite `get` por vehículo y `scan` por rango de fechas sin concentrar toda la carga en el mismo región.

## Justificación

- El patrón `vehicle_id` al inicio guía el acceso por clave primaria y evita que un solo vehículo domine toda la tabla.
- La parte temporal (`yyyyMMdd|HHmmss`) permite ordenar lecturas históricas y filtrar rangos de tiempo sin analizar el dataset completo.
- Un rowkey aleatorio o secuencial por ID sin fecha empeoraría la lectura de historial y haría menos útil la operación de mantenimiento.
- En un entorno de 1.000+ scooters, esta estrategia reduce la probabilidad de hotspot y mantiene consultas razonables para “últimas lecturas” y “estado actual”.

## Consecuencia

La tabla está modelada por consulta: “estado actual” y “últimas N lecturas de un vehículo” son accesos directos; “todas las lecturas de la flota en tiempo real” sigue siendo un anti-patrón para HBase y es preferible para Cassandra.
