# ADR 002 — APIs de ingesta (Open-Meteo + red social)

## Decisión

La ingesta Silver (`03-databricks/01_ingesta.py`) corre en Python contra el MinIO local. No hay workspace Databricks: el recorte §8 ya reemplazó ADLS por MinIO, y el script es el notebook re-ejecutable (mismos parámetros de ciudad y fechas arriba del archivo).

Clima: **Open-Meteo Archive**, sin API key, horario, `timezone=UTC`.

Red social: primero **Bluesky** `app.bsky.feed.searchPosts`. Si falla, **Mastodon** `mastodon.social` hashtag `ecobici`.

## Por qué estas fuentes

| Fuente | Por qué | Por qué no la alternativa |
|--------|---------|---------------------------|
| Open-Meteo | Histórico horario, sin key, no entra un secreto al repo | OpenWeather pide API key |
| Bluesky search | Es el endpoint que pide el enunciado | X/Twitter es de pago |
| Mastodon `#ecobici` | Público, sin key, y el 2026-10-09 Bluesky respondió **403** | Quedarse sin posts tumba el HMV de “API social” |

`clima_codigo` del generador es un código WMO **sorteado**. No entra al join. Sirve para contrastar veracidad: el clima de Silver es el de Open-Meteo.

## Cómo se junta el clima con el viaje

Hay 24 estaciones y el enunciado pide clima de **3**. Esas tres son `ST000` (Palermo), `ST002` (San Telmo) y `ST010` (Microcentro). Cada viaje se asigna a la más cercana por haversine sobre `lat_start`/`lon_start`, y se matchea por la **hora UTC** de `ts_start`.

Es un proxy de tres puntos sobre CABA, no una estación meteorológica real por `station_id`. La distancia queda en `km_a_estacion_clima`.

Clima adverso, para que Gold no lo redefina: `precip_mm > 0` o `weather_code >= 51` (códigos WMO de llovizna en adelante).

## Si la API falla

- Open-Meteo con error o sin serie horaria: el job **termina con error**. No se imputa clima ni se copia `clima_codigo`.
- Bluesky 403 o vacío: se intenta Mastodon. Si los dos fallan, el job termina con error y no publica un Silver “completo”.
- Una re-ejecución borra los prefijos `viajes_clima/`, `viajes_rechazados/` y `social_posts/` antes de subir, para no duplicar Parquet en la misma partición.

Esquema congelado: `contratos/silver_viajes.yaml` y `contratos/silver_social.yaml`.
