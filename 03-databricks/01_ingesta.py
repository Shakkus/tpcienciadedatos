#!/usr/bin/env python3
"""
M3 — Ingesta Silver NexoUrbano (re-ejecutable).

Lee viajes Bronze, consulta Open-Meteo (3 estaciones) y un feed social
público, aplica las reglas de rechazo del contrato Bronze y escribe
Parquet en el bucket MinIO ``silver``.

Uso:
  python 03-databricks/01_ingesta.py
  python 03-databricks/01_ingesta.py --source minio
  python 03-databricks/01_ingesta.py --skip-upload
"""

from __future__ import annotations

import argparse
import html
import os
import re
import sys
import tempfile
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests

# --- Parámetros de corrida (ciudad y fechas arriba, como pide M3) ---
CIUDAD = "Buenos Aires"
FECHA_DESDE = "2026-03-01"
FECHA_HASTA = "2026-05-30"
# Tres estaciones del catálogo local. El clima de cada viaje es el de la más cercana.
ESTACIONES_CLIMA = ("ST000", "ST002", "ST010")
SOURCE = "local"  # local | minio

REPO_ROOT = Path(__file__).resolve().parents[1]
VIAJES_CSV = REPO_ROOT / "data" / "viajes.csv"
ESTACIONES_CSV = REPO_ROOT / "data" / "estaciones.csv"
EVIDENCE_PATH = REPO_ROOT / "evidence" / "databricks" / "01_ingesta_log.txt"
BUCKET_SILVER = "silver"
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
BLUESKY_URL = "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts"
MASTODON_TAG_URL = "https://mastodon.social/api/v1/timelines/tag/{tag}"
USER_AGENT = "NexoUrbano-ingesta/1.0 (grupo 67; trabajo academico)"


def log_line(lines: list[str], text: str = "") -> None:
    lines.append(text)
    print(text)


def configure_stdout() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")


def haversine_km(lat1: np.ndarray, lon1: np.ndarray, lat2: float, lon2: float) -> np.ndarray:
    r = 6371.0
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlmb = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2.0) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlmb / 2.0) ** 2
    return 2 * r * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))


def minio_client():
    from minio import Minio

    endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
    return Minio(
        endpoint,
        access_key=os.environ.get("MINIO_ACCESS_KEY", "nexo"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "nexo-urban-2026"),
        secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
    ), endpoint


def read_viajes_local(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(path)
    return pd.read_csv(path)


def read_viajes_minio(client) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    with tempfile.TemporaryDirectory(prefix="nexo_bronze_read_") as tmp:
        staging = Path(tmp)
        for obj in client.list_objects("bronze", prefix="viajes/", recursive=True):
            if not obj.object_name.endswith(".csv"):
                continue
            dest = staging / obj.object_name.replace("/", "__")
            client.fget_object("bronze", obj.object_name, str(dest))
            frames.append(pd.read_csv(dest))
    if not frames:
        raise RuntimeError("MinIO bronze/viajes/ no tiene CSV. Correr tools/bronze_a_minio.py")
    return pd.concat(frames, ignore_index=True)


def prepare_viajes(df: pd.DataFrame, fecha_desde: str, fecha_hasta: str) -> pd.DataFrame:
    out = df.copy()
    out["ts_start"] = pd.to_datetime(out["ts_start"], utc=True, errors="coerce")
    out["ts_end"] = pd.to_datetime(out["ts_end"], utc=True, errors="coerce")
    for col in ("station_start", "station_end"):
        out[col] = out[col].replace("", pd.NA)
    start = pd.Timestamp(fecha_desde, tz="UTC")
    end = pd.Timestamp(fecha_hasta, tz="UTC") + pd.Timedelta(days=1)
    out = out[(out["ts_start"] >= start) & (out["ts_start"] < end)].copy()
    return out


def rejection_masks(df: pd.DataFrame) -> dict[str, pd.Series]:
    station_null = df["station_start"].isna() | df["station_end"].isna()
    return {
        "duration_s < 30": df["duration_s"] < 30,
        "duration_s > 14400": df["duration_s"] > 14400,
        "ts_end < ts_start": df["ts_end"].isna() | df["ts_start"].isna() | (df["ts_end"] < df["ts_start"]),
        "station_start o station_end nulos": station_null,
        "fare_ars < 0": df["fare_ars"] < 0,
    }


def apply_rejection(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    masks = rejection_masks(df)
    counts = {name: int(mask.sum()) for name, mask in masks.items()}
    motivo = np.array([""] * len(df), dtype=object)
    reject = np.zeros(len(df), dtype=bool)
    for name, mask in masks.items():
        hit = mask.to_numpy()
        reject |= hit
        both = hit & (motivo != "")
        motivo = np.where(both, motivo + " | " + name, np.where(hit, name, motivo))
    rejected = df.loc[reject].copy()
    rejected["motivo_rechazo"] = motivo[reject]
    accepted = df.loc[~reject].copy()
    return accepted, rejected, counts


def load_estaciones(ids: tuple[str, ...]) -> pd.DataFrame:
    est = pd.read_csv(ESTACIONES_CSV)
    est["station_id"] = est["station_id"].astype(str)
    chosen = est[est["station_id"].isin(ids)].set_index("station_id")
    missing = [sid for sid in ids if sid not in chosen.index]
    if missing:
        raise RuntimeError(f"Estaciones de clima no están en estaciones.csv: {missing}")
    return chosen.loc[list(ids)]


def assign_nearest_station(df: pd.DataFrame, estaciones: pd.DataFrame) -> pd.DataFrame:
    lat = df["lat_start"].to_numpy(dtype=float)
    lon = df["lon_start"].to_numpy(dtype=float)
    ids = list(estaciones.index)
    dist = np.column_stack(
        [
            haversine_km(lat, lon, float(estaciones.loc[sid, "lat"]), float(estaciones.loc[sid, "lon"]))
            for sid in ids
        ]
    )
    nearest = dist.argmin(axis=1)
    out = df.copy()
    out["station_clima"] = np.array(ids)[nearest]
    out["km_a_estacion_clima"] = np.round(dist[np.arange(len(out)), nearest], 3)
    out["hora_utc"] = out["ts_start"].dt.floor("h")
    out["fecha"] = out["ts_start"].dt.strftime("%Y-%m-%d")
    return out


def fetch_open_meteo(estaciones: pd.DataFrame, fecha_desde: str, fecha_hasta: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for sid, row in estaciones.iterrows():
        params = {
            "latitude": float(row["lat"]),
            "longitude": float(row["lon"]),
            "start_date": fecha_desde,
            "end_date": fecha_hasta,
            "hourly": "temperature_2m,precipitation,weather_code",
            "timezone": "UTC",
        }
        response = requests.get(OPEN_METEO_URL, params=params, timeout=90, headers={"User-Agent": USER_AGENT})
        response.raise_for_status()
        payload = response.json()
        hourly = payload.get("hourly") or {}
        if not hourly.get("time"):
            raise RuntimeError(f"Open-Meteo no devolvió serie horaria para {sid}")
        frames.append(
            pd.DataFrame(
                {
                    "station_clima": sid,
                    "hora_utc": pd.to_datetime(hourly["time"], utc=True),
                    "temp_c": hourly["temperature_2m"],
                    "precip_mm": hourly["precipitation"],
                    "weather_code": hourly["weather_code"],
                }
            )
        )
    weather = pd.concat(frames, ignore_index=True)
    weather["weather_code"] = pd.to_numeric(weather["weather_code"], errors="coerce").astype("Int64")
    weather["temp_c"] = pd.to_numeric(weather["temp_c"], errors="coerce")
    weather["precip_mm"] = pd.to_numeric(weather["precip_mm"], errors="coerce")
    return weather


def mark_adverse(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    has_weather = out["weather_code"].notna() | out["precip_mm"].notna()
    adverse = (out["precip_mm"].fillna(0) > 0) | (out["weather_code"].fillna(-1) >= 51)
    out["clima_adverso"] = pd.Series(pd.NA, index=out.index, dtype="boolean")
    out.loc[has_weather, "clima_adverso"] = adverse.loc[has_weather].to_numpy()
    return out


def strip_html(raw: str) -> str:
    text = re.sub(r"<[^>]+>", " ", raw or "")
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def fetch_bluesky(ciudad: str, limit: int = 25) -> pd.DataFrame:
    query = f"ecobici {ciudad}"
    response = requests.get(
        BLUESKY_URL,
        params={"q": query, "limit": limit},
        timeout=30,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
    )
    response.raise_for_status()
    posts = response.json().get("posts") or []
    rows = []
    for post in posts:
        record = post.get("record") or {}
        author = post.get("author") or {}
        rows.append(
            {
                "fecha": record.get("createdAt") or post.get("indexedAt"),
                "texto": (record.get("text") or "").strip(),
                "autor": author.get("handle") or "",
                "fuente": "bluesky",
            }
        )
    return pd.DataFrame(rows)


def fetch_mastodon(tag: str = "buenosaires", limit: int = 40) -> pd.DataFrame:
    response = requests.get(
        MASTODON_TAG_URL.format(tag=tag),
        params={"limit": limit},
        timeout=30,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
    )
    response.raise_for_status()
    rows = []
    for status in response.json():
        account = status.get("account") or {}
        rows.append(
            {
                "fecha": status.get("created_at"),
                "texto": strip_html(status.get("content") or ""),
                "autor": account.get("acct") or "",
                "fuente": "mastodon",
            }
        )
    return pd.DataFrame(rows)


def fetch_social(ciudad: str) -> tuple[pd.DataFrame, str]:
    errors: list[str] = []
    try:
        posts = fetch_bluesky(ciudad)
        if len(posts) > 0:
            return posts, "bluesky app.bsky.feed.searchPosts q='ecobici {0}'".format(ciudad)
        errors.append("Bluesky respondió 200 con 0 posts")
    except requests.RequestException as exc:
        errors.append(f"Bluesky: {exc}")
    tags = ["ecobici", "buenosaires"] if "aires" in ciudad.lower() else ["bici"]
    for tag in tags:
        try:
            posts = fetch_mastodon(tag)
        except requests.RequestException as exc:
            errors.append(f"Mastodon #{tag}: {exc}")
            continue
        if len(posts) == 0:
            errors.append(f"Mastodon #{tag} respondió 0 posts")
            continue
        fuente = f"mastodon.social tag #{tag}"
        if errors:
            fuente += f" (fallback; {'; '.join(errors)})"
        return posts, fuente
    raise RuntimeError("Sin posts sociales. " + " | ".join(errors))


def null_report(df: pd.DataFrame) -> list[str]:
    lines = []
    n = max(len(df), 1)
    for col in df.columns:
        pct = 100.0 * float(df[col].isna().sum()) / n
        if pct > 0:
            lines.append(f"  {col}: {pct:.4f}%")
    if not lines:
        lines.append("  (ninguna columna con nulos)")
    return lines


def write_parquet(df: pd.DataFrame, dest: Path, partition_cols: list[str] | None = None) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    frame = df.copy()
    for col in frame.columns:
        if isinstance(frame[col].dtype, pd.DatetimeTZDtype):
            frame[col] = frame[col].dt.tz_convert("UTC").dt.tz_localize(None)
    kwargs = {}
    if partition_cols:
        kwargs["partition_cols"] = partition_cols
        kwargs["basename_template"] = "part-0.parquet"
    frame.to_parquet(dest, index=False, **kwargs)


def upload_tree(client, local_root: Path, bucket: str, prefix: str) -> list[str]:
    uris: list[str] = []
    for path in sorted(local_root.rglob("*")):
        if not path.is_file() or path.name.startswith(".") or path.name == "_SUCCESS":
            continue
        rel = path.relative_to(local_root).as_posix()
        key = f"{prefix}/{rel}" if prefix else rel
        client.fput_object(bucket, key, str(path))
        uris.append(f"s3a://{bucket}/{key}")
    return uris


def ensure_bucket(client, name: str) -> None:
    if not client.bucket_exists(name):
        client.make_bucket(name)


def clear_prefix(client, bucket: str, prefix: str) -> int:
    from minio.deleteobjects import DeleteObject

    keys = [obj.object_name for obj in client.list_objects(bucket, prefix=prefix, recursive=True)]
    if not keys:
        return 0
    errors = list(client.remove_objects(bucket, [DeleteObject(key) for key in keys]))
    if errors:
        raise RuntimeError(f"No se pudo limpiar s3a://{bucket}/{prefix}: {errors[0]}")
    return len(keys)


def measure_get_ms(client, bucket: str, key: str) -> float:
    started = time.perf_counter()
    response = client.get_object(bucket, key)
    try:
        response.read(64)
    finally:
        response.close()
        response.release_conn()
    return (time.perf_counter() - started) * 1000.0


def metrics_block(silver: pd.DataFrame, bronze_km_null_pct: float, filas_bronze: int) -> list[str]:
    horas_rango = int((silver["ts_start"].max() - silver["ts_start"].min()).total_seconds() // 3600) + 1
    viajes_por_hora = len(silver) / max(horas_rango, 1)
    hora_art = silver["ts_start"].dt.tz_convert("America/Argentina/Buenos_Aires").dt.hour
    por_hora_art = hora_art.value_counts().sort_index()
    pico = int(por_hora_art.idxmax())
    p50 = float(silver["duration_s"].quantile(0.50))
    p95 = float(silver["duration_s"].quantile(0.95))
    top = silver["station_start"].value_counts().head(10)
    lines = [
        f"shape silver: {silver.shape[0]} filas x {silver.shape[1]} columnas",
        f"1. viajes/hora (tasa sobre el rango UTC): {viajes_por_hora:.2f}",
        f"   hora ART con más viajes: {pico:02d}h ({int(por_hora_art.loc[pico])} viajes)",
        f"2. duración p50: {p50:.1f} s",
        f"3. duración p95: {p95:.1f} s",
        "4. top estaciones origen:",
    ]
    for station, count in top.items():
        lines.append(f"   {station}: {int(count)}")
    lines.append(f"5. % nulos km (bronze en rango, antes del rechazo): {bronze_km_null_pct:.4f}%")
    lines.append(f"   filas post-rechazo (silver): {len(silver)} de {filas_bronze}")
    match = float(silver["weather_code"].notna().mean() * 100)
    adv = silver["clima_adverso"].dropna()
    adv_pct = float(adv.mean() * 100) if len(adv) else float("nan")
    lines.append(f"match clima (weather_code no nulo): {match:.2f}%")
    lines.append(f"% clima adverso entre filas con clima: {adv_pct:.2f}%")
    return lines


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingesta Silver viajes x clima + social")
    parser.add_argument("--ciudad", default=CIUDAD)
    parser.add_argument("--fecha-desde", default=FECHA_DESDE)
    parser.add_argument("--fecha-hasta", default=FECHA_HASTA)
    parser.add_argument("--source", choices=("local", "minio"), default=SOURCE)
    parser.add_argument("--csv", type=Path, default=VIAJES_CSV)
    parser.add_argument("--skip-upload", action="store_true")
    return parser


def main() -> int:
    configure_stdout()
    args = build_parser().parse_args()
    lines: list[str] = []
    log_line(lines, "NexoUrbano — M3 ingesta Silver")
    log_line(lines, f"ciudad: {args.ciudad}")
    log_line(lines, f"rango: {args.fecha_desde} .. {args.fecha_hasta} (UTC, extremo inclusive)")
    log_line(lines, f"estaciones clima: {', '.join(ESTACIONES_CLIMA)}")
    log_line(lines, f"source: {args.source}")

    client = None
    endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
    if args.source == "minio" or not args.skip_upload:
        try:
            client, endpoint = minio_client()
            client.list_buckets()
        except Exception as exc:
            if args.source == "minio" or not args.skip_upload:
                print(f"ERROR: MinIO en {endpoint} no responde ({exc})", file=sys.stderr)
                print("Levantar: docker compose up -d minio", file=sys.stderr)
                return 1

    if args.source == "minio":
        viajes = read_viajes_minio(client)
        origen = "s3a://bronze/viajes/"
    else:
        csv_path = args.csv if args.csv.is_absolute() else REPO_ROOT / args.csv
        viajes = read_viajes_local(csv_path)
        origen = csv_path.relative_to(REPO_ROOT).as_posix()
    log_line(lines, f"origen viajes: {origen}")
    log_line(lines, f"shape leído: {viajes.shape[0]} filas x {viajes.shape[1]} columnas")

    viajes = prepare_viajes(viajes, args.fecha_desde, args.fecha_hasta)
    log_line(lines, f"shape en rango: {viajes.shape[0]} filas x {viajes.shape[1]} columnas")
    dup = int(viajes["ride_id"].duplicated().sum())
    log_line(lines, f"duplicados ride_id: {dup}")
    km_null_pct = 100.0 * float(viajes["km"].isna().mean()) if len(viajes) else 0.0
    log_line(lines, "nulos por columna (solo las que tienen):")
    for row in null_report(viajes):
        log_line(lines, row)

    accepted, rejected, counts = apply_rejection(viajes)
    log_line(lines, "rechazo por regla (una fila puede contar en más de una):")
    for name, count in counts.items():
        log_line(lines, f"  {name}: {count}")
    log_line(lines, f"filas rechazadas: {len(rejected)}")
    log_line(lines, f"filas aceptadas: {len(accepted)}")

    estaciones = load_estaciones(ESTACIONES_CLIMA)
    log_line(lines, "estaciones Open-Meteo:")
    for sid, row in estaciones.iterrows():
        log_line(lines, f"  {sid} {row['nombre']} ({row['barrio']}) lat={row['lat']} lon={row['lon']}")

    try:
        weather = fetch_open_meteo(estaciones, args.fecha_desde, args.fecha_hasta)
    except requests.RequestException as exc:
        print(f"ERROR: Open-Meteo falló, no se inventa clima ({exc})", file=sys.stderr)
        return 1
    log_line(lines, f"filas clima horario: {len(weather)} ({weather['station_clima'].nunique()} estaciones)")

    silver = assign_nearest_station(accepted, estaciones)
    silver = silver.merge(weather, on=["station_clima", "hora_utc"], how="left")
    silver = mark_adverse(silver)
    log_line(lines, "")
    log_line(lines, "métricas:")
    for row in metrics_block(silver, km_null_pct, len(viajes)):
        log_line(lines, row)
    log_line(lines, "nulos silver (solo columnas con nulos):")
    for row in null_report(silver):
        log_line(lines, row)

    try:
        social, social_fuente = fetch_social(args.ciudad)
    except (RuntimeError, requests.RequestException) as exc:
        print(f"ERROR: API social falló ({exc})", file=sys.stderr)
        return 1
    log_line(lines, "")
    log_line(lines, f"social: {len(social)} posts vía {social_fuente}")
    log_line(lines, "muestra social (hasta 3):")
    for _, post in social.head(3).iterrows():
        texto = (post["texto"] or "").replace("\n", " ")
        log_line(lines, f"  [{post['fuente']}] {post['fecha']} @{post['autor']}: {texto[:140]}")

    with tempfile.TemporaryDirectory(prefix="nexo_silver_") as tmp:
        staging = Path(tmp)
        viajes_dir = staging / "viajes_clima"
        write_parquet(silver, viajes_dir, partition_cols=["fecha"])
        write_parquet(rejected, staging / "viajes_rechazados" / "rechazados.parquet")
        write_parquet(social, staging / "social_posts" / "posts.parquet")
        parquet_files = list(staging.rglob("*.parquet"))
        parquet_bytes = sum(path.stat().st_size for path in parquet_files)
        log_line(lines, "")
        log_line(lines, f"parquet staging: {len(parquet_files)} archivos, {parquet_bytes} bytes (no se versiona)")

        if args.skip_upload:
            log_line(lines, "upload: omitido (--skip-upload)")
        else:
            ensure_bucket(client, BUCKET_SILVER)
            for prefix in ("viajes_clima/", "viajes_rechazados/", "social_posts/"):
                removed = clear_prefix(client, BUCKET_SILVER, prefix)
                if removed:
                    log_line(lines, f"limpieza previa s3a://silver/{prefix}: {removed} objetos")
            uris = []
            uris += upload_tree(client, viajes_dir, BUCKET_SILVER, "viajes_clima")
            uris += upload_tree(client, staging / "viajes_rechazados", BUCKET_SILVER, "viajes_rechazados")
            uris += upload_tree(client, staging / "social_posts", BUCKET_SILVER, "social_posts")
            log_line(lines, f"objetos subidos a silver: {len(uris)}")
            log_line(lines, "URI base: s3a://silver/viajes_clima/fecha=yyyy-mm-dd/")
            log_line(lines, "URI social: s3a://silver/social_posts/posts.parquet")
            log_line(lines, "URI rechazados: s3a://silver/viajes_rechazados/rechazados.parquet")
            log_line(lines, "muestra de URIs:")
            for uri in uris[:8]:
                log_line(lines, f"  {uri}")
            if len(uris) > 8:
                log_line(lines, f"  ... y {len(uris) - 8} objetos más")
            sample_key = uris[0].removeprefix("s3a://silver/")
            ttfb_ms = measure_get_ms(client, BUCKET_SILVER, sample_key)
            log_line(lines, f"lectura MinIO (GET 64 bytes) de {uris[0]}: {ttfb_ms:.1f} ms")
            log_line(lines, f"endpoint: http://{endpoint}")

    EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Evidencia: {EVIDENCE_PATH.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
