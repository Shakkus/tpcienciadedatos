// ============================================================
// M13 · Neo4j — carga idempotente del grafo NexoUrbano
// ============================================================
// Requisitos del enunciado:
// - Nodos: Rider, Interes, Estacion
// - Relaciones: AMIGO_DE, VIAJO_CON, LE_GUSTA, SALE_DE
// - Carga >= 80 riders (el generador usa 120 riders)
// - Idempotencia: MERGE para no duplicar nodos/relaciones
// - Validación: conteos de nodos y relaciones
// ============================================================

// 1) Constraints base
CREATE CONSTRAINT rider_id IF NOT EXISTS
FOR (r:Rider) REQUIRE r.id IS UNIQUE;

CREATE CONSTRAINT interes_id IF NOT EXISTS
FOR (i:Interes) REQUIRE i.id IS UNIQUE;

CREATE CONSTRAINT estacion_id IF NOT EXISTS
FOR (e:Estacion) REQUIRE e.id IS UNIQUE;

// 2) Carga base de Riders desde data/riders.csv
//    Columnas reales del CSV: rider_id,nombre,email,plan,alta,ciudad,interes_1,interes_2
LOAD CSV WITH HEADERS FROM 'file:///riders.csv' AS row
MERGE (r:Rider {id: row.rider_id})
SET r.nombre = row.nombre,
    r.email = row.email,
    r.plan = row.plan,
    r.alta = row.alta,
    r.ciudad = row.ciudad,
    r.interes_1 = row.interes_1,
    r.interes_2 = row.interes_2;

// 3) Carga base de Estaciones desde data/estaciones.csv
//    Columnas reales del CSV: station_id,nombre,barrio,lat,lon,capacidad
LOAD CSV WITH HEADERS FROM 'file:///estaciones.csv' AS row
MERGE (e:Estacion {id: row.station_id})
SET e.nombre = row.nombre,
    e.barrio = row.barrio,
    e.lat = toFloat(row.lat),
    e.lon = toFloat(row.lon),
    e.capacidad = toInteger(row.capacidad);

// 4) Carga de amistades desde data/grafo.json
//    Estructura real: value.amigos = [{"a":"R...","b":"R..."}]
CALL apoc.load.json('file:///grafo.json') YIELD value
UNWIND value.amigos AS rel
MERGE (a:Rider {id: rel.a})
MERGE (b:Rider {id: rel.b})
MERGE (a)-[:AMIGO_DE]->(b);

// 5) Carga de gustos desde data/grafo.json
//    Estructura real: value.le_gusta = [{"rider_id":"R...","interes":"..."}]
CALL apoc.load.json('file:///grafo.json') YIELD value
UNWIND value.le_gusta AS rel
MERGE (r:Rider {id: rel.rider_id})
MERGE (i:Interes {id: rel.interes})
SET i.nombre = rel.interes
MERGE (r)-[:LE_GUSTA]->(i);

// 6) Carga de viajes compartidos y salidas desde data/grafo.json
//    Estructura real: value.viajo_con = [{"a":"R...","b":"R...","station_id":"ST...","n":N}]
//    Requisito: ambos riders deben salir de la misma estación y conservar propiedades útiles del vínculo.
CALL apoc.load.json('file:///grafo.json') YIELD value
UNWIND value.viajo_con AS rel
MERGE (r1:Rider {id: rel.a})
MERGE (r2:Rider {id: rel.b})
MERGE (e:Estacion {id: rel.station_id})
MERGE (r1)-[:SALE_DE]->(e)
MERGE (r2)-[:SALE_DE]->(e)
MERGE (r1)-[v:VIAJO_CON]->(r2)
ON CREATE SET v.station_id = rel.station_id,
              v.n = toInteger(rel.n)
ON MATCH SET  v.station_id = CASE WHEN v.station_id IS NULL THEN rel.station_id ELSE v.station_id END,
              v.n = CASE WHEN coalesce(v.n, 0) < toInteger(rel.n) THEN toInteger(rel.n) ELSE v.n END;

// 7) Verificaciones básicas de integridad / conteos
MATCH (r:Rider) RETURN count(r) AS riders_total;
MATCH (e:Estacion) RETURN count(e) AS estaciones_total;
MATCH (i:Interes) RETURN count(i) AS intereses_total;
MATCH ()-[r:AMIGO_DE]->() RETURN count(r) AS amistades_total;
MATCH ()-[r:VIAJO_CON]->() RETURN count(r) AS viajes_compartidos_total;
MATCH ()-[r:SALE_DE]->(:Estacion) RETURN count(r) AS salidas_total;

// 8) Consulta de sanity check: riders sin estación de salida
MATCH (r:Rider)
WHERE NOT EXISTS {
  MATCH (r)-[:SALE_DE]->(:Estacion)
}
RETURN count(r) AS riders_sin_salida;

// 9) Consulta de sanity check: estaciones con más salidas
MATCH (e:Estacion)<-[:SALE_DE]-(r:Rider)
RETURN e.id AS station_id, e.nombre AS nombre, count(r) AS riders_que_salieron
ORDER BY riders_que_salieron DESC
LIMIT 10;

// 10) Consulta de sanity check: muestra algunos vínculos de amistad y viajes compartidos
MATCH (r1:Rider)-[:AMIGO_DE]->(r2:Rider)
RETURN r1.id AS rider_a, r2.id AS rider_b
LIMIT 10;

MATCH (r1:Rider)-[v:VIAJO_CON]->(r2:Rider)
RETURN r1.id AS rider_a, r2.id AS rider_b, v.station_id AS station_id, v.n AS n
LIMIT 10;

// ============================================================
// A19 — Amigos en común
// ============================================================
MATCH (r1:Rider)-[:AMIGO_DE]-(common:Rider)-[:AMIGO_DE]-(r2:Rider)
WHERE r1 <> r2
  AND r1.id < r2.id
WITH r1, r2, common
ORDER BY r1.id, r2.id, common.id
WITH r1, r2, collect(DISTINCT common.id) AS amigos_en_comun_ids
WITH r1, r2, amigos_en_comun_ids, size(amigos_en_comun_ids) AS cantidad_amigos_en_comun
WHERE cantidad_amigos_en_comun > 0
RETURN
  r1.id AS rider_a,
  r2.id AS rider_b,
  cantidad_amigos_en_comun AS cantidad_amigos_en_comun,
  amigos_en_comun_ids AS amigos_en_comun_ids
ORDER BY cantidad_amigos_en_comun DESC, rider_a, rider_b
LIMIT 10;

// ============================================================
// A20 — Camino más corto entre dos riders
// ============================================================
MATCH (r1:Rider {id: 'R0000'}), (r2:Rider {id: 'R0108'})
WHERE r1 <> r2
  AND NOT (r1)-[:AMIGO_DE]-(r2)
WITH r1, r2
MATCH p = shortestPath((r1)-[:AMIGO_DE*..10]-(r2))
RETURN
  r1.id AS rider_origen,
  r2.id AS rider_destino,
  length(p) AS saltos,
  [n IN nodes(p) | n.id] AS camino_riders;

// ============================================================
// A21 — Recomendación: riders con estaciones compartidas y no amigos
// ============================================================
MATCH (r1:Rider {id: 'R0000'})-[:SALE_DE]->(e:Estacion)<-[:SALE_DE]-(r2:Rider)
WHERE r1 <> r2
  AND NOT (r1)-[:AMIGO_DE]-(r2)
WITH r1, r2, e
WITH r1, r2, collect(DISTINCT e.id) AS estaciones_compartidas
WITH r1, r2, estaciones_compartidas, size(estaciones_compartidas) AS cantidad_estaciones_compartidas
WHERE cantidad_estaciones_compartidas > 0
WITH r1, r2, cantidad_estaciones_compartidas, estaciones_compartidas
OPTIONAL MATCH (r2)-[:LE_GUSTA]->(i:Interes)
OPTIONAL MATCH (r1)-[:LE_GUSTA]->(ri:Interes)
RETURN
  r1.id AS rider_base,
  r1.plan AS plan_base,
  r2.id AS rider_recomendado,
  r2.plan AS plan_recomendado,
  cantidad_estaciones_compartidas AS estaciones_compartidas,
  estaciones_compartidas AS estaciones_compartidas_ids,
  collect(DISTINCT i.id) AS intereses_recomendado,
  collect(DISTINCT ri.id) AS intereses_base
ORDER BY cantidad_estaciones_compartidas DESC, rider_recomendado
LIMIT 20;

// ============================================================
// A22 — Patrón de fraude / collusion
// ============================================================
LOAD CSV WITH HEADERS FROM 'file:///viajes.csv' AS row
WITH row.station_start AS station_id,
     toInteger(datetime(row.ts_start).epochSeconds / 120) AS window_bucket,
     collect(DISTINCT row.rider_id) AS riders_ventana,
     collect(row.ride_id)[0..3] AS rides_muestra,
     min(row.ts_start) AS ts_min,
     max(row.ts_start) AS ts_max
WHERE size(riders_ventana) >= 3
UNWIND riders_ventana AS r1_id
UNWIND riders_ventana AS r2_id
UNWIND riders_ventana AS r3_id
WITH station_id, window_bucket, rides_muestra, ts_min, ts_max, r1_id, r2_id, r3_id
WHERE r1_id < r2_id AND r2_id < r3_id
WITH station_id,
     [r1_id, r2_id, r3_id] AS riders_trio,
     count(DISTINCT window_bucket) AS ventanas_repetidas,
     collect(rides_muestra)[0..3] AS ejemplos_rides,
     min(ts_min) AS primer_registro,
     max(ts_max) AS ultimo_registro
ORDER BY ventanas_repetidas DESC, station_id ASC
LIMIT 20
MATCH (e:Estacion {id: station_id})
OPTIONAL MATCH (r1:Rider {id: riders_trio[0]})
OPTIONAL MATCH (r2:Rider {id: riders_trio[1]})
OPTIONAL MATCH (r3:Rider {id: riders_trio[2]})
RETURN
  station_id AS estacion,
  e.nombre AS nombre_estacion,
  riders_trio AS riders_en_trio,
  ventanas_repetidas,
  ejemplos_rides,
  primer_registro,
  ultimo_registro,
  EXISTS { (r1)-[:AMIGO_DE|VIAJO_CON]-(r2) } OR
  EXISTS { (r1)-[:AMIGO_DE|VIAJO_CON]-(r3) } OR
  EXISTS { (r2)-[:AMIGO_DE|VIAJO_CON]-(r3) } AS tiene_relacion_social
ORDER BY ventanas_repetidas DESC, estacion ASC;
