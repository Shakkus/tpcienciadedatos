# M12 - Cassandra

## Objetivo
Este módulo carga la telemetría de scooters en Cassandra usando la tabla `lecturas_vehiculo`, con una partición por `vehicle_id` y clustering por `ts` descendente.

## Requisitos
- Docker Desktop
- Docker Compose
- Windows (CMD o PowerShell), Linux, Mac o Git Bash
- Dataset disponible en:
  `data/lecturas_sensores.csv`

## Ejecución
Desde la raíz del proyecto, elegí el comando según tu terminal:

**En Windows (CMD):**
```bat
07-cassandra\ingestar.bat
```

**En Windows (PowerShell):**
```powershell
.\07-cassandra\ingestar.bat
```

**En Linux, Mac o Git Bash:**
```bash
bash 07-cassandra/ingestar.sh
```

## Qué hace el script
1. Verifica que exista el CSV de entrada.
2. Levanta el contenedor de Cassandra con Docker Compose.
3. Espera a que Cassandra responda (Health Check).
4. Copia el CSV al contenedor en `/tmp/lecturas_sensores.csv`.
5. Ejecuta el schema:
   - `CREATE KEYSPACE IF NOT EXISTS nexo`
   - `CREATE TABLE IF NOT EXISTS lecturas_vehiculo (...)`
6. Ejecuta la carga de datos con el comando `COPY`.

## Archivos relevantes
- `schema.cql`: Definición de keyspace y tabla.
- `load.cql`: Comando de ingesta.
- `queries.cql`: Consultas de prueba y antipatrones.
- `ingestar.bat` / `ingestar.sh`: Scripts de automatización y reproducibilidad.
- `export_cassandra.js`: Script Node.js para extraer el último estado de la flota.
- `cassandra_ultima_lectura.json`: Entregable final exportado.

## Resultado esperado
La tabla queda poblada con al menos 5000 filas y lista para consultas por vehículo, rango horario y export para la pregunta de negocio:

> ¿Qué vehículo retirar hoy?

## Query de validación
Se recomienda probar en la consola iteractiva (`docker exec -it nexo-cassandra cqlsh`):

```sql
SELECT * FROM nexo.lecturas_vehiculo WHERE vehicle_id = 'V0064' LIMIT 20;
```

Y también la consulta que debe fallar (antipatrón sin partición):

```sql
SELECT * FROM nexo.lecturas_vehiculo WHERE ts >= '2026-05-01 08:00:00+0000';
```

Esto sirve para demostrar la razón del modelo de partición y la importancia de la Primary Key en series temporales.