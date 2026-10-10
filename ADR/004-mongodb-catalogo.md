ADR — MongoDB como catálogo comercial

Contexto
El catálogo de productos de NexoUrbano combina datos que se consultan juntos: nombre, categoría, tags, precios por ciudad y stock por ciudad. El acceso principal es buscar un SKU o filtrar por categoría con toda la información del producto en una sola lectura.

Decisión
Guardar cada SKU como un documento de la colección productos en MongoDB. El documento mantiene los campos de precio, stock y tags dentro del mismo registro.

Consecuencias
Las lecturas de catálogo son más simples y directas.
Se evitan joins para recuperar un producto completo.
Las actualizaciones de precio y stock quedan acopladas a la misma entidad.
El diseño resulta claro para catálogo operativo, sin pretender resolver todas las cargas de trabajo del sistema.

Nota de diseño
MongoDB se usa aquí como capa de catálogo comercial. Otras cargas de trabajo, como series temporales o relaciones complejas, siguen teniendo su motor específico.