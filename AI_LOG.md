# Bitácora de IA — NexoUrbano

Squad: Sebastian Albornoz, Mauricio Jurado, Alvaro Costa

Herramientas usadas (versiones): Cursor, Python 3.14, Docker Desktop, MinIO (Chainguard), `minio` Py SDK

Regla: una fila por sesión relevante. Mentir acá desaprueba el bloque A.

| Fecha | Módulo | Prompt (resumen) | Qué se aceptó | Qué se corrigió a mano | Qué falló |
|---|---|---|---|---|---|
| 2026-10-09 | M1 Discovery / 4 V | Completar fichas Netflix, red social e IoT mapeadas a NexoUrbano + arquitectura medallion | Estructura de tablas del enunciado, órdenes de magnitud diseño vs repo (semilla 67001), asignación V→motor | Revisar URL/dataset abierto real en ADR 000 cuando el squad lo baje; ajustar owners en acta | — |
| 2026-10-09 | Kick-off / M2 MinIO | Bloque D+E: bronze particionado, estructura repo, README squad | Script `tools/bronze_a_minio.py`, ADR 001, carpetas módulos, `.env.example` | Credenciales MinIO y puertos documentados a mano en README | Login MinIO / pull Hub resueltos con Chainguard + reset volumen |
| 2026-10-09 | M1 perfilado | Perfilar `Dataset_abierto_real.csv` ≥100k | `01-discovery/perfilado.py`, `contratos/dataset_abierto_real.yaml`, evidencia discovery | Encoding consola Windows en print del script | — |
