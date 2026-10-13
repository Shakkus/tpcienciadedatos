-- Hive — demo de DROP externa vs. interna para M6
-- Ejecutar después de cargar bronze_viajes y silver_viajes.

DROP TABLE bronze_viajes;

-- Validación esperada fuera de Hive:
-- hdfs dfs -ls /nexo/bronze/m6_viajes/
-- La carpeta y el CSV deben seguir existiendo porque bronze_viajes es EXTERNAL.

DROP TABLE silver_viajes;

-- Validación esperada fuera de Hive:
-- hdfs dfs -test -e /nexo/warehouse/silver_viajes
-- El directorio administrado debe desaparecer porque silver_viajes es interna.
