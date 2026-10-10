# ADR 004: Uso de MongoDB para el Catálogo Comercial

**Contexto:** NexoUrbano requiere una base de datos para almacenar el catálogo de productos (pases, recargas, accesorios) y gestionar las ventas.
**Decisión:** Se eligió MongoDB porque su modelo de documentos anidados (Embedding) permite almacenar toda la información jerárquica de un producto (precios por ciudad, stock, características) en un solo documento BSON.
**Consecuencias:** 
- Evitamos realizar múltiples uniones (JOINs) costosas que requeriría un modelo relacional tradicional.
- Aceleramos las lecturas para los riders en la app.
- Su Aggregation Framework es ideal para generar reportes analíticos de ventas (ingresos por categoría) para el negocio.
