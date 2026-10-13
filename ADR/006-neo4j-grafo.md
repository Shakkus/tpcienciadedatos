# ADR 006 — Neo4j para grafo social y detección de collusion

## Contexto
NexoUrbano requiere modelar amistades, estaciones, intereses y recorridos para responder recomendaciones sociales, caminos multi-salto y fraude temporal. Las consultas de cercanía y red social no encajan bien en joins recursivos de SQL ni en documentos de MongoDB.

## Decisión
Usamos Neo4j para representar Riders, Estaciones e Intereses como nodos y relaciones `AMIGO_DE`, `SALE_DE`, `LE_GUSTA` y `VIAJO_CON`. Los patrones de recomendación y de fraude se resuelven con `MATCH`, `shortestPath` y filtros temporales por estación.

## Consecuencias
La red social queda legible y fácil de consultar en un solo motor. Las recomendaciones multi-salto y la detección de collusion se resuelven con consultas focalizadas sobre la estación y el tiempo, sin duplicar el dataset de viajes. El sistema mantiene separación de responsabilidades: Mongo para catálogo, Cassandra para series temporales y Neo4j para relaciones complejas.
