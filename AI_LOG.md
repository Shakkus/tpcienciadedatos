# Bitácora de IA — NexoUrbano

Squad: Sebastian Albornoz, Mauricio Jurado, Alvaro Costa

Herramientas usadas (versiones): Cursor, Python 3.14, Docker Desktop, MinIO (Chainguard), `minio` Py SDK

Regla: una fila por sesión relevante. Mentir acá desaprueba el bloque A.

| Fecha | Módulo | Prompt (resumen) | Qué se aceptó | Qué se corrigió a mano | Qué falló |
|---|---|---|---|---|---|
| 2026-10-09 | M1 Discovery / 4 V | Completar fichas Netflix, red social e IoT mapeadas a NexoUrbano + arquitectura medallion | Estructura de tablas del enunciado, órdenes de magnitud diseño vs repo (semilla 67001), asignación V→motor | Revisar URL/dataset abierto real en ADR 000 cuando el squad lo baje; ajustar owners en acta | — |
| 2026-10-09 | Kick-off / M2 MinIO | Bloque D+E: bronze particionado, estructura repo, README squad | Script `tools/bronze_a_minio.py`, ADR 001, carpetas módulos, `.env.example` | Credenciales MinIO y puertos documentados a mano en README | Login MinIO / pull Hub resueltos con Chainguard + reset volumen |
| 2026-10-09 | M1 perfilado | Perfilar `Dataset_abierto_real.csv` ≥100k | `01-discovery/perfilado.py`, `contratos/dataset_abierto_real.yaml`, evidencia discovery | Encoding consola Windows en print del script | — |


ALvaro M10-M14
# Bitácora de uso de IA (AI_LOG.md)

## Módulo 10 y M11: MongoDB y Agregaciones (Owner: Alvaro Costa)
- **Herramienta:** Gemini (IA conversacional).
- **Prompt usado:** "Eres un experto en MongoDB. Estoy haciendo el entregable 'M10 y M11' de mi proyecto NexoUrbano. Dame el código JS exacto para la consola mongosh que haga lo siguiente: 1) Un insertMany de 30 documentos en la colección productos... 2) Un find, updateOne y deleteOne... 3) Crear un índice compuesto en ciudad+categoria y ejecutar explain... 4) Código para ventas_pipeline.js con $match,$group, $lookup y$project."
- **Qué se aceptó:** Se utilizó intacto el script BSON de documentos anidados y la estructura del aggregation framework.
- **Qué se corrigió a mano:** El pipeline del `docker exec` que generó un problema de autenticación por terminal en Git Bash. Se solucionó copiando el script dentro del contenedor (`docker cp`).
- **Qué falló / Notas:** Hubo un error de sintaxis al inyectar código directo con `<` en Windows/Bash.