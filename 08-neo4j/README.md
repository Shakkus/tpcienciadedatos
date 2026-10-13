# Módulo 13 — Neo4j: Grafo Social, Recomendaciones y Fraude

**Owner:** Alvaro Costa · **Semilla:** `67001` · **Ciudad:** Buenos Aires

Implementación del grafo social en **Neo4j 5 Community** (+ APOC) para resolver cercanía social, recomendaciones y detección de *collusion*, alimentando las **Preguntas 3 y 4** de `09-producto/decision_ops.py`.

## 1. Ejecución reproducible (< 2 min)

Levantar solo Neo4j (cuidando el límite de 8 GB de RAM y usando `user: "1000:1000"` para leer `./data` en modo `:ro`):

    sudo docker compose up -d neo4j
    mkdir -p evidence/neo4j
    sudo docker exec -i nexo-neo4j cypher-shell -u neo4j -p nexo-urban-2026 --format verbose < 08-neo4j/grafo.cypher | tee evidence/neo4j/queries_result.txt

* **Browser:** `http://localhost:7474` (`neo4j` / `nexo-urban-2026`) · **Captura:** `evidence/neo4j/browser.png`

---

## 2. Resumen del Pipeline y Resultados (Semilla `67001`)

| Paso | Valor de Negocio | Implementación Técnica (Cypher) | Resultado Verificado |
| :--- | :--- | :--- | :--- |
| **A18 · Carga** | Modela usuarios, gustos y estaciones como red para evitar *self-joins* costosos. | `CONSTRAINT` de unicidad + `MERGE` idempotente desde `riders.csv`, `estaciones.csv` y `grafo.json` (`apoc.load.json`). | **120 `Rider`** (HMV >= 80), **24 `Estacion`**, **10 `Interes`**, 220 `AMIGO_DE`, 160 `VIAJO_CON`, 301 `SALE_DE`. |
| **A19 · Amigos en común** | Detecta micro-comunidades de confianza para referidos o pases grupales. | Match no dirigido `(r1)-[:AMIGO_DE]-(c)-[:AMIGO_DE]-(r2)` con `r1.id < r2.id` y `ORDER BY` previo a `collect()`. | Top 3 pares con **3 amigos en común**: `R0013-R0060`, `R0025-R0053` y `R0047-R0060`. |
| **A20 · Camino más corto** | Mide grados de separación entre usuarios sin amistad directa. | `shortestPath((r1)-[:AMIGO_DE*..10]-(r2))` no dirigido excluyendo amigos directos. | `R0000 -> R0108` conectado en **4 saltos**: `["R0000", "R0033", "R0007", "R0006", "R0108"]`. |
| **A21 · Recomendación** | Sugiere contactos y pases cruzando rutinas geográficas e intereses (**Pregunta 3 M14**). | Riders que comparten `[:SALE_DE]->(:Estacion)` con `R0000` y `NOT [:AMIGO_DE]`, sumando plan y `[:LE_GUSTA]`. | Top 4 comparte **2 estaciones** (`ST002`, `ST015`, `ST007`): `R0019` (`plus`), `R0031` (`pro`), `R0034` y `R0076` (`free`, candidatos a *upsell*). |
| **A22 · Fraude / Collusion** | Audita si 3 riders inician viajes en la misma estación en `< 120 s` y si tienen vínculo social (**Pregunta 4 M14**). | Lectura al vuelo de `viajes.csv` (250k filas) agrupando por `station_start` y cubetas de `120 s` antes de cruzar con el grafo (12,7 s). | **3 tríos con vínculo social (`TRUE`)** en `ST000` (`R0055-R0093-R0108`, `R0028-R0060-R0101` a las 02:50 AM y `R0009-R0036-R0080` a las 04:42 AM). Máximo `ventanas_repetidas = 1` (alerta operativa, sin fraude recurrente). |

* **Nota técnica de arquitectura (A22):** Para procesar 250.000 viajes en ~12 segundos sin provocar un desborde de memoria (evitando el producto cartesiano masivo de una ventana deslizante iterativa), se optó por discretizar el tiempo en bloques fijos (*tumbling windows* de 120s). Se asume un ligero riesgo de falsos negativos en los bordes de cada cubeta temporal a favor de la estabilidad del motor y los límites de RAM del entorno.

---

## 3. Correcciones manuales sobre salidas de IA (Resumen `AI_LOG.md`)

1. **Permisos Docker (`:ro`):** Se agregó `user: "1000:1000"` en `docker-compose.yml` para evitar `AccessDeniedException` del UID `7474`.
2. **Esquema real JSON (A18):** Se corrigió la propiedad `rel.rider` por `rel.rider_id` en `le_gusta` y se usó `SALE_DE` en lugar de `USA_ESTACION`.
3. **Sintaxis Cypher (A19):** Se reemplazó la función inexistente `sort()` por `ORDER BY` antes de `collect(DISTINCT ...)`.
4. **Memoria 1.3 GiB (A22):** Se eliminó el `MATCH` previo a `LOAD CSV` que causaba un producto cartesiano (`120 x 24 x 250.000` filas) y agotaba `dbms.memory.transaction.total.max`.
