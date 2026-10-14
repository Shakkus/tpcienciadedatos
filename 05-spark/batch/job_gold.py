#!/usr/bin/env python3
"""
M8 — Spark batch Gold NexoUrbano.

Lee Silver viajes×clima (Parquet), aplica reglas de calidad del contrato Bronze,
join con riders (JDBC MySQL o CSV), agrega gold_ops_diaria y top estaciones,
export para pregunta 1 (reposición si llueve) y escribe Parquet en output + MinIO gold.

Revenue estimado: tarifa plana documentada (sin join Mongo pases); ver TARIFA_PLANA_ARS.

Uso:
  python 05-spark/batch/job_gold.py
  python 05-spark/batch/job_gold.py --source minio --skip-upload
  spark-submit --packages com.mysql:mysql-connector-j:8.4.0 05-spark/batch/job_gold.py
"""

from __future__ import annotations

import argparse
import io
import os
import sys
from contextlib import redirect_stdout
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

# Tarifa plana operativa (ARS/viaje) cuando no hay join a catálogo Mongo — enunciado M8.
TARIFA_PLANA_ARS = 320.0

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SILVER_LOCAL = REPO_ROOT / ".staging" / "silver" / "viajes_clima"
OUTPUT_ROOT = REPO_ROOT / "05-spark" / "batch" / "output"
RIDERS_CSV = REPO_ROOT / "data" / "riders.csv"
EVIDENCE_DIR = REPO_ROOT / "evidence" / "spark"
EXPLAIN_PATH = EVIDENCE_DIR / "batch_explain.txt"
RUN_LOG_PATH = EVIDENCE_DIR / "batch_run_log.txt"
BUCKET_GOLD = "gold"

MYSQL_JDBC_PACKAGES = "com.mysql:mysql-connector-j:8.4.0"


def log_line(lines: list[str], text: str = "") -> None:
    lines.append(text)
    print(text)


def configure_stdout() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")


def minio_client():
    from minio import Minio

    endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
    return Minio(
        endpoint,
        access_key=os.environ.get("MINIO_ACCESS_KEY", "nexo"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "nexo-urban-2026"),
        secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
    ), endpoint


def sync_silver_from_minio(dest: Path) -> int:
    client, endpoint = minio_client()
    dest.mkdir(parents=True, exist_ok=True)
    n = 0
    for obj in client.list_objects("silver", prefix="viajes_clima/", recursive=True):
        if not obj.object_name.endswith(".parquet"):
            continue
        rel = obj.object_name[len("viajes_clima/") :]
        local = dest / rel
        local.parent.mkdir(parents=True, exist_ok=True)
        client.fget_object("silver", obj.object_name, str(local))
        n += 1
    if n == 0:
        raise RuntimeError(
            f"MinIO {endpoint} bucket silver sin viajes_clima/. Correr 03-databricks/01_ingesta.py"
        )
    return n


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


def build_spark(use_jdbc: bool) -> SparkSession:
    builder = (
        SparkSession.builder.appName("nexo_gold_m8")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
    )
    if use_jdbc:
        builder = builder.config("spark.jars.packages", MYSQL_JDBC_PACKAGES)
    return builder.getOrCreate()


def read_riders(spark: SparkSession, source: str) -> tuple[DataFrame, str]:
    if source == "jdbc":
        host = os.environ.get("MYSQL_HOST", "127.0.0.1")
        port = os.environ.get("MYSQL_PORT", "3307")
        db = os.environ.get("MYSQL_DATABASE", "crm")
        user = os.environ.get("MYSQL_USER", "root")
        password = os.environ.get("MYSQL_PASSWORD", "nexo")
        url = f"jdbc:mysql://{host}:{port}/{db}?useSSL=false&allowPublicKeyRetrieval=true"
        df = (
            spark.read.format("jdbc")
            .option("url", url)
            .option("dbtable", "riders")
            .option("user", user)
            .option("password", password)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .load()
            .select("rider_id", "nombre", "plan", "ciudad")
        )
        return df, f"jdbc mysql://{host}:{port}/{db}.riders"
    df = (
        spark.read.option("header", True)
        .csv(str(RIDERS_CSV))
        .select("rider_id", "nombre", "plan", "ciudad")
    )
    return df, str(RIDERS_CSV)


def apply_quality_filters(df: DataFrame) -> DataFrame:
    """Reglas alineadas con contratos/bronze_viajes.yaml (Silver ya las aplicó; re-chequeo Gold)."""
    return df.filter(
        (F.col("duration_s") >= 30)
        & (F.col("duration_s") <= 14400)
        & (F.col("ts_end") >= F.col("ts_start"))
        & F.col("station_start").isNotNull()
        & F.col("station_end").isNotNull()
        & (F.coalesce(F.col("fare_ars"), F.lit(0.0)) >= 0)
    )


def build_gold_ops_diaria(trips: DataFrame) -> DataFrame:
    adverse = F.when(F.col("clima_adverso").isNull(), F.lit(False)).otherwise(F.col("clima_adverso"))
    return trips.groupBy("fecha").agg(
        F.count(F.lit(1)).alias("viajes"),
        (F.sum("duration_s") / F.lit(60.0)).alias("minutos"),
        F.sum("km").alias("km"),
        (F.avg(adverse.cast("double")) * 100.0).alias("pct_clima_adverso"),
        F.sum("fare_ars").alias("fare_observado_ars"),
        (F.count(F.lit(1)) * F.lit(TARIFA_PLANA_ARS)).alias("revenue_estimado_ars"),
    ).orderBy("fecha")


def build_top_estaciones(trips: DataFrame) -> DataFrame:
    return (
        trips.groupBy("station_start")
        .agg(F.count(F.lit(1)).alias("viajes"))
        .orderBy(F.desc("viajes"))
        .limit(10)
    )


def build_repuesto_lluvia(trips: DataFrame) -> DataFrame:
    """Pregunta 1 decision_ops: estaciones × volumen y % clima adverso."""
    adverse = F.when(F.col("clima_adverso").isNull(), F.lit(False)).otherwise(F.col("clima_adverso"))
    return (
        trips.groupBy("station_start")
        .agg(
            F.count(F.lit(1)).alias("viajes"),
            (F.avg(adverse.cast("double")) * 100.0).alias("pct_clima_adverso"),
            F.avg("precip_mm").alias("precip_mm_prom"),
        )
        .orderBy(F.desc("pct_clima_adverso"), F.desc("viajes"))
    )


def capture_explain(df: DataFrame) -> str:
    buf = io.StringIO()
    with redirect_stdout(buf):
        df.explain(mode="extended")
    return buf.getvalue()


def write_explain(df: DataFrame, trips_cached: DataFrame, note: str) -> None:
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    body = [
        "NexoUrbano — M8 Spark batch Gold",
        "Query: agregación gold_ops_diaria (groupBy fecha)",
        "",
        capture_explain(df),
        "",
        "--- Anotación docente ---",
        note,
        "",
        f"Filas en cache (viajes calidad + join riders): {trips_cached.count()}",
    ]
    EXPLAIN_PATH.write_text("\n".join(body), encoding="utf-8")


def main() -> int:
    configure_stdout()
    parser = argparse.ArgumentParser(description="Spark batch Gold M8")
    parser.add_argument("--source", choices=("local", "minio"), default="local")
    parser.add_argument("--silver-path", type=Path, default=DEFAULT_SILVER_LOCAL)
    parser.add_argument("--riders-source", choices=("jdbc", "csv", "auto"), default="auto")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--skip-minio-sync", action="store_true")
    args = parser.parse_args()

    lines: list[str] = []
    log_line(lines, "NexoUrbano — M8 Spark batch Gold")
    log_line(lines, f"tarifa_plana_ars: {TARIFA_PLANA_ARS} (revenue_estimado sin Mongo pases)")

    silver_path = args.silver_path
    if args.source == "minio" and not args.skip_minio_sync:
        log_line(lines, f"sync MinIO silver → {silver_path}")
        n_files = sync_silver_from_minio(silver_path)
        log_line(lines, f"archivos parquet descargados: {n_files}")
    if not silver_path.exists():
        raise SystemExit(
            f"No hay Silver en {silver_path}. Usar --source minio o correr ingesta M3."
        )

    use_jdbc_packages = args.riders_source in ("jdbc", "auto")

    spark = None
    try:
        spark = build_spark(use_jdbc=use_jdbc_packages)
        silver = spark.read.parquet(str(silver_path))
        silver_count = silver.count()
        log_line(lines, f"silver leído: {silver_count} filas")

        if args.riders_source == "csv":
            riders, riders_origin = read_riders(spark, "csv")
        elif args.riders_source == "jdbc":
            riders, riders_origin = read_riders(spark, "jdbc")
        else:
            try:
                riders, riders_origin = read_riders(spark, "jdbc")
            except Exception as exc:
                log_line(lines, f"JDBC riders falló ({exc}); fallback data/riders.csv")
                riders, riders_origin = read_riders(spark, "csv")
        riders_count = riders.count()
        log_line(lines, f"riders: {riders_count} filas desde {riders_origin}")

        joined = silver.join(riders, on="rider_id", how="left")
        filtered = apply_quality_filters(joined)
        rejected = silver_count - filtered.count()
        log_line(lines, f"filas post-filtro calidad (rechazo adicional Gold): {rejected}")

        trips = filtered.cache()
        trips.count()

        gold_ops = build_gold_ops_diaria(trips)
        top_stations = build_top_estaciones(trips)
        repuesto = build_repuesto_lluvia(trips)

        ops_rows = gold_ops.count()
        log_line(lines, f"gold_ops_diaria: {ops_rows} particiones (fechas)")
        log_line(lines, "top 10 station_start:")
        for row in top_stations.collect():
            log_line(lines, f"  {row['station_start']}: {row['viajes']}")

        note = (
            "Tras .cache() en viajes filtrados, las agregaciones deberían leer "
            "InMemoryTableScan / InMemoryRelation. El join a riders (~120 filas) suele "
            "planificarse como BroadcastHashJoin (Catalyst empuja la tabla chica)."
        )
        write_explain(gold_ops, trips, note)

        OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
        ops_out = OUTPUT_ROOT / "gold_ops_diaria"
        top_out = OUTPUT_ROOT / "gold_top_estaciones"
        repuesto_out = OUTPUT_ROOT / "gold_repuesto_lluvia"
        producto_out = REPO_ROOT / "09-producto"

        gold_ops.write.mode("overwrite").parquet(str(ops_out))
        top_stations.write.mode("overwrite").parquet(str(top_out))
        repuesto.write.mode("overwrite").parquet(str(repuesto_out))
        repuesto.coalesce(1).write.mode("overwrite").option("header", True).csv(
            str(producto_out / "_tmp_repuesto_lluvia")
        )
        producto_out.mkdir(parents=True, exist_ok=True)
        csv_parts = list((producto_out / "_tmp_repuesto_lluvia").glob("part-*.csv"))
        if csv_parts:
            dest = producto_out / "gold_repuesto_lluvia.csv"
            dest.write_text(csv_parts[0].read_text(encoding="utf-8"), encoding="utf-8")
            for p in (producto_out / "_tmp_repuesto_lluvia").iterdir():
                p.unlink()
            (producto_out / "_tmp_repuesto_lluvia").rmdir()

        log_line(lines, f"output local: {OUTPUT_ROOT}")

        if not args.skip_upload:
            client, endpoint = minio_client()
            ensure_bucket(client, BUCKET_GOLD)
            for prefix in ("ops_diaria", "top_estaciones", "repuesto_lluvia"):
                removed = clear_prefix(client, BUCKET_GOLD, prefix)
                log_line(lines, f"limpieza gold/{prefix}: {removed} objetos")
            uris = []
            uris += upload_tree(client, ops_out, BUCKET_GOLD, "ops_diaria")
            uris += upload_tree(client, top_out, BUCKET_GOLD, "top_estaciones")
            uris += upload_tree(client, repuesto_out, BUCKET_GOLD, "repuesto_lluvia")
            log_line(lines, f"MinIO ({endpoint}) objetos gold subidos: {len(uris)}")
            log_line(lines, "URI base: s3a://gold/ops_diaria/")
            log_line(lines, "URI repuesto: s3a://gold/repuesto_lluvia/")
        else:
            log_line(lines, "upload MinIO: omitido (--skip-upload)")

        log_line(lines, f"explain: {EXPLAIN_PATH}")
    finally:
        if spark is not None:
            spark.stop()

    RUN_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    RUN_LOG_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
