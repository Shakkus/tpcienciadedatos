#!/usr/bin/env python3
"""
M9 — Spark Structured Streaming, alertas de flota NexoUrbano.

Caso de uso:
Una ráfaga de payment_fail en dos minutos indica cobros que no cierran (viaje fantasma o pasarela caída).
battery_low en la misma ventana marca scooters que no llegan a una estación con carga.
Operaciones mira la ventana de 2 min con slide de 30 s y frena desbloqueos si el conteo sube.
El watermark de 1 min tira eventos con reloj atrasado: no reabren una alerta ya emitida.
Esas filas quedan en gold_alerts (consola + Parquet) y no se mezclan con el batch diario.
Si el evento llega más de un minuto tarde, se documenta y se descarta.

Los 40 JSON de data/stream cubren 117 s (12:00:00Z–12:01:57Z). El file source, si ve
el directorio completo al arrancar, los mete en un solo microbatch y ninguno es tardío.
El modo demo los copia por tandas. Después manda event_clock.json (ride_start 12:06:00Z)
para llevar el watermark a 12:05:00Z y cerrar las ventanas de 2 min. Recién ahí suelta
event_late.json (payment_fail 12:00:10Z, vehicle_id VLATE): llega fuera de toda ventana
abierta y Spark lo descarta.

Uso:
  python 05-spark/streaming/stream_alerts.py
  bash tools/run_stream_alerts_docker.sh
"""

from __future__ import annotations

import argparse
import shutil
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = REPO_ROOT / "data" / "stream"
STAGING_ROOT = REPO_ROOT / ".staging" / "streaming"
EVIDENCE_PATH = REPO_ROOT / "evidence" / "spark" / "streaming_console.txt"
CSV_PATH = REPO_ROOT / "evidence" / "spark" / "gold_alerts.csv"
OUTPUT_DIR = REPO_ROOT / "05-spark" / "streaming" / "output" / "gold_alerts"

ALERT_EVENTS = ("payment_fail", "battery_low")
WINDOW = "2 minutes"
SLIDE = "30 seconds"
WATERMARK = "1 minute"
TRIGGER = "5 seconds"
WAVE_SIZE = 8
WAVE_PAUSE_S = 8

# event_039.json es payment_fail a las 12:01:57Z. Watermark = max(ts) - 1 min.
WATERMARK_READY = datetime(2026, 5, 20, 12, 0, 57, tzinfo=timezone.utc)
# ride_start de control a las 12:06:00Z. Cierra las ventanas (fin <= 12:05:00Z).
WATERMARK_CLOSED = datetime(2026, 5, 20, 12, 5, 0, tzinfo=timezone.utc)
CLOCK_NAME = "event_clock.json"
CLOCK_EVENT = {
    "ts": "2026-05-20T12:06:00+00:00",
    "event": "ride_start",
    "vehicle_id": "VCLOCK",
    "rider_id": "RCLOCK",
    "battery_pct": 80.0,
}
LATE_NAME = "event_late.json"
LATE_EVENT = {
    "ts": "2026-05-20T12:00:10+00:00",
    "event": "payment_fail",
    "vehicle_id": "VLATE",
    "rider_id": "RLATE",
    "battery_pct": 4.0,
}

EVENT_SCHEMA = StructType(
    [
        StructField("ts", StringType()),
        StructField("event", StringType()),
        StructField("vehicle_id", StringType()),
        StructField("rider_id", StringType()),
        StructField("battery_pct", DoubleType()),
    ]
)

GOLD_SCHEMA = StructType(
    [
        StructField("window_start", TimestampType()),
        StructField("window_end", TimestampType()),
        StructField("event", StringType()),
        StructField("conteo", LongType()),
    ]
)


class RunLog:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.lines: list[str] = []
        self.counts: dict[tuple[int, int, str], int] = {}
        self.seen_clock = False
        self.seen_late = False

    def log(self, text: str = "") -> None:
        with self._lock:
            self.lines.append(text)
        print(text, flush=True)

    def update(self, rows: list[tuple[int, int, str, int]], epoch_id: int) -> None:
        with self._lock:
            before = dict(self.counts)
            for start, end, event, conteo in rows:
                self.counts[(start, end, event)] = conteo
            changed = self.counts != before
            snapshot = dict(self.counts)
        stamp = datetime.now(timezone.utc).strftime("%H:%M:%SZ")
        if not changed:
            self.log(f"epoch={epoch_id} wall={stamp} sin cambio en alertas")
            return
        self.log(f"epoch={epoch_id} wall={stamp}")
        self.log(format_counts(snapshot))

    def mark_vehicle(self, vehicle_id: str | None) -> None:
        with self._lock:
            if vehicle_id == CLOCK_EVENT["vehicle_id"]:
                self.seen_clock = True
            elif vehicle_id == LATE_EVENT["vehicle_id"]:
                self.seen_late = True

    def flagged(self, name: str) -> bool:
        with self._lock:
            return bool(getattr(self, name))

    def snapshot(self) -> dict[tuple[int, int, str], int]:
        with self._lock:
            return dict(self.counts)

    def text(self) -> str:
        with self._lock:
            return "\n".join(self.lines) + "\n"


def as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def fmt_ts(epoch_s: int) -> str:
    return datetime.fromtimestamp(epoch_s, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def format_counts(counts: dict[tuple[int, int, str], int]) -> str:
    if not counts:
        return "  (sin filas de alerta todavía)"
    lines = ["  ventana_inicio        ventana_fin           event         conteo"]
    for start, end, event in sorted(counts):
        lines.append(f"  {fmt_ts(start)}  {fmt_ts(end)}  {event:<12}  {counts[(start, end, event)]}")
    return "\n".join(lines)


def parse_spark_ts(value: str | None) -> datetime | None:
    if not value or value.startswith("1970-") or value.startswith("1969-"):
        return None
    text = value.replace("Z", "")
    if "." in text:
        text = text.split(".", 1)[0]
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)


def reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def build_spark() -> SparkSession:
    derby = STAGING_ROOT / "derby"
    warehouse = STAGING_ROOT / "warehouse"
    derby.mkdir(parents=True, exist_ok=True)
    warehouse.mkdir(parents=True, exist_ok=True)
    return (
        SparkSession.builder.appName("nexo_alerts_m9")
        .master("local[4]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", warehouse.as_posix())
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={derby.as_posix()}")
        .getOrCreate()
    )


def parse_events(raw: DataFrame) -> DataFrame:
    event_ts = F.to_timestamp(
        F.regexp_replace(F.col("ts"), r"(Z|\+00:00)$", ""),
        "yyyy-MM-dd'T'HH:mm:ss",
    )
    return (
        raw.withColumn("event_ts", event_ts)
        .filter(F.col("event_ts").isNotNull())
        .drop("ts")
        .withColumnRenamed("event_ts", "ts")
    )


def alert_counts(events: DataFrame) -> DataFrame:
    # Watermark sobre todo el stream: un ride_start también avanza el reloj.
    # El sink se queda solo con payment_fail y battery_low.
    return (
        events.withWatermark("ts", WATERMARK)
        .groupBy(F.window("ts", WINDOW, SLIDE).alias("ventana"), F.col("event"))
        .agg(F.count(F.lit(1)).alias("conteo"))
    )


def source_end(query: StreamingQuery) -> str:
    progress = query.lastProgress or {}
    sources = progress.get("sources") or [{}]
    return str(sources[0].get("endOffset"))


def current_watermark(query: StreamingQuery) -> datetime | None:
    progress = query.lastProgress
    if not progress:
        return None
    event_time = progress.get("eventTime") or {}
    return parse_spark_ts(event_time.get("watermark"))


def wait_until(predicate, timeout_s: float) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(1)
    return False


def copy_waves(src_dir: Path, dest_dir: Path, log: RunLog) -> int:
    files = sorted(src_dir.glob("event_*.json"))
    if not files:
        raise RuntimeError(f"No hay JSON en {src_dir}")
    copied = 0
    for offset in range(0, len(files), WAVE_SIZE):
        wave = files[offset : offset + WAVE_SIZE]
        for src in wave:
            shutil.copyfile(src, dest_dir / src.name)
        copied += len(wave)
        log.log(f"tanda: {copied}/{len(files)} archivos en {dest_dir.name}")
        time.sleep(WAVE_PAUSE_S)
    return copied


def write_event(dest_dir: Path, name: str, event: dict) -> None:
    payload = (
        "{"
        f'"ts": "{event["ts"]}", '
        f'"event": "{event["event"]}", '
        f'"vehicle_id": "{event["vehicle_id"]}", '
        f'"rider_id": "{event["rider_id"]}", '
        f'"battery_pct": {event["battery_pct"]}'
        "}\n"
    )
    (dest_dir / name).write_text(payload, encoding="utf-8")


def rows_from_batch(batch_df: DataFrame) -> list[tuple[int, int, str, int]]:
    parsed: list[tuple[int, int, str, int]] = []
    for row in batch_df.collect():
        event = row["event"]
        if event not in ALERT_EVENTS:
            continue
        ventana = row["ventana"]
        start = int(as_utc(ventana["start"]).timestamp())
        end = int(as_utc(ventana["end"]).timestamp())
        parsed.append((start, end, event, int(row["conteo"])))
    return parsed


def expected_counts(spark: SparkSession, input_dir: Path) -> dict[tuple[int, int, str], int]:
    """Mismas ventanas, sin el JSON tardío. Sirve para probar que Spark lo descartó."""
    raw = spark.read.schema(EVENT_SCHEMA).json(str(input_dir))
    name = F.input_file_name()
    events = parse_events(raw.filter(~name.contains(LATE_NAME) & ~name.contains(CLOCK_NAME)))
    counted = (
        events.filter(F.col("event").isin(*ALERT_EVENTS))
        .groupBy(F.window("ts", WINDOW, SLIDE).alias("ventana"), F.col("event"))
        .agg(F.count(F.lit(1)).alias("conteo"))
    )
    return {
        (start, end, event): conteo
        for start, end, event, conteo in rows_from_batch(counted)
    }


def write_gold(spark: SparkSession, counts: dict[tuple[int, int, str], int]) -> None:
    reset_dir(OUTPUT_DIR)
    rows = [
        (
            datetime.fromtimestamp(start, tz=timezone.utc).replace(tzinfo=None),
            datetime.fromtimestamp(end, tz=timezone.utc).replace(tzinfo=None),
            event,
            int(counts[(start, end, event)]),
        )
        for start, end, event in sorted(counts)
    ]
    frame = spark.createDataFrame(rows, schema=GOLD_SCHEMA)
    frame.coalesce(1).write.mode("overwrite").parquet(str(OUTPUT_DIR))
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    header = "window_start,window_end,event,conteo\n"
    body = "".join(
        f"{fmt_ts(start)},{fmt_ts(end)},{event},{counts[(start, end, event)]}\n"
        for start, end, event in sorted(counts)
    )
    CSV_PATH.write_text(header + body, encoding="utf-8")


def read_stream(spark: SparkSession, input_dir: Path) -> DataFrame:
    return (
        spark.readStream.schema(EVENT_SCHEMA)
        .option("maxFilesPerTrigger", WAVE_SIZE)
        .json(str(input_dir))
    )


def start_queries(
    spark: SparkSession, input_dir: Path, log: RunLog
) -> tuple[StreamingQuery, StreamingQuery]:
    alerts = alert_counts(parse_events(read_stream(spark, input_dir)))
    observed = parse_events(read_stream(spark, input_dir))

    def on_batch(batch_df: DataFrame, epoch_id: int) -> None:
        log.update(rows_from_batch(batch_df), epoch_id)

    def on_raw(batch_df: DataFrame, epoch_id: int) -> None:
        del epoch_id
        for row in batch_df.select("vehicle_id").collect():
            log.mark_vehicle(row["vehicle_id"])

    alerts_q = (
        alerts.writeStream.outputMode("update")
        .foreachBatch(on_batch)
        .option("checkpointLocation", str(STAGING_ROOT / "checkpoint_alerts"))
        .trigger(processingTime=TRIGGER)
        .queryName("gold_alerts")
        .start()
    )
    raw_q = (
        observed.writeStream.outputMode("append")
        .foreachBatch(on_raw)
        .option("checkpointLocation", str(STAGING_ROOT / "checkpoint_raw"))
        .trigger(processingTime=TRIGGER)
        .queryName("stream_raw")
        .start()
    )
    return alerts_q, raw_q


def run_demo(spark: SparkSession, log: RunLog) -> tuple[int, dict[tuple[int, int, str], int]]:
    input_dir = STAGING_ROOT / "input"
    reset_dir(input_dir)
    reset_dir(STAGING_ROOT / "checkpoint_alerts")
    reset_dir(STAGING_ROOT / "checkpoint_raw")

    log.log("NexoUrbano — M9 Spark Structured Streaming")
    log.log(f"ventana={WINDOW} slide={SLIDE} watermark={WATERMARK} trigger={TRIGGER}")
    log.log(f"fuente canónica: {SOURCE_DIR}")
    log.log(f"staging: {input_dir}")
    log.log(
        "Caso de uso: payment_fail y battery_low en ventana de 2 min; "
        "un evento con reloj atrasado no reabre la alerta."
    )

    alerts_q, raw_q = start_queries(spark, input_dir, log)
    try:
        time.sleep(3)
        copied = copy_waves(SOURCE_DIR, input_dir, log)
        log.log(f"archivos del generador copiados: {copied}")

        def watermark_at_least(moment: datetime) -> bool:
            wm = current_watermark(alerts_q)
            return wm is not None and wm >= moment

        ready = wait_until(lambda: watermark_at_least(WATERMARK_READY), timeout_s=90)
        watermark = current_watermark(alerts_q)
        log.log(f"watermark tras las tandas: {watermark.isoformat() if watermark else 'aún vacío'}")
        if not ready or watermark is None:
            raise RuntimeError(
                "El watermark no llegó a 12:00:57Z. El sample no se agregó completo "
                "o el parseo de ts falló."
            )

        stable = log.snapshot()
        write_event(input_dir, CLOCK_NAME, CLOCK_EVENT)
        log.log(
            f"inyectado {CLOCK_NAME}: {CLOCK_EVENT['event']} ts={CLOCK_EVENT['ts']} "
            "(no es alerta; solo cierra ventanas)"
        )
        clock_seen = wait_until(
            lambda: log.flagged("seen_clock") and watermark_at_least(WATERMARK_CLOSED),
            timeout_s=40,
        )
        watermark = current_watermark(alerts_q)
        log.log(f"watermark tras el reloj: {watermark.isoformat() if watermark else 'aún vacío'}")
        if not clock_seen or watermark is None or log.snapshot() != stable:
            raise RuntimeError(
                "event_clock.json no cerró las ventanas sin tocar las alertas. "
                f"visto={log.flagged('seen_clock')} watermark={watermark} "
                f"conteos_cambiaron={log.snapshot() != stable}"
            )

        end_before = source_end(alerts_q)
        before = log.snapshot()
        log.log(f"estado estable antes del tardío: {len(before)} ventanas, offset={end_before}")
        write_event(input_dir, LATE_NAME, LATE_EVENT)
        log.log(
            f"inyectado {LATE_NAME}: {LATE_EVENT['event']} ts={LATE_EVENT['ts']} "
            f"vehicle_id={LATE_EVENT['vehicle_id']}"
        )
        seen = wait_until(
            lambda: log.flagged("seen_late") and source_end(alerts_q) != end_before,
            timeout_s=40,
        )
        if not seen:
            raise RuntimeError(
                f"El source no leyó {LATE_NAME}. "
                f"visto={log.flagged('seen_late')} offset={source_end(alerts_q)}"
            )
        time.sleep(6)
        after = log.snapshot()
        if after != before:
            raise RuntimeError(
                "El payment_fail tardío cambió gold_alerts. El watermark no lo descartó.\n"
                f"antes={before}\ndespués={after}"
            )
        log.log(
            "evento tardío descartado: "
            f"{LATE_NAME} payment_fail {LATE_EVENT['ts']} vehicle_id={LATE_EVENT['vehicle_id']}. "
            f"Llegó con watermark en {watermark.isoformat()} "
            "(las ventanas de 2 min que lo contenían ya tenían fin <= watermark). "
            "Los conteos no se movieron."
        )
        log.log(f"offset alerts después del tardío: {source_end(alerts_q)}")
        return copied, before
    finally:
        alerts_q.stop()
        raw_q.stop()


def verify_against_batch(
    spark: SparkSession,
    counts: dict[tuple[int, int, str], int],
    log: RunLog,
) -> None:
    expected = expected_counts(spark, STAGING_ROOT / "input")
    if counts != expected:
        raise RuntimeError(
            "gold_alerts no coincide con el groupBy batch de los 40 JSON (sin el tardío).\n"
            f"streaming={counts}\nbatch={expected}"
        )
    payment = sum(value for (*_, event), value in counts.items() if event == "payment_fail")
    battery = sum(value for (*_, event), value in counts.items() if event == "battery_low")
    log.log(
        "verificación batch OK: mismas ventanas que los 40 JSON, sin VLATE. "
        f"suma de conteos (un evento cae en varias ventanas) "
        f"payment_fail={payment} battery_low={battery}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Spark streaming gold_alerts M9")
    parser.add_argument(
        "--no-demo",
        action="store_true",
        help="Leer data/stream ya poblado y no cortar (no inyecta el tardío).",
    )
    args = parser.parse_args()
    if args.no_demo:
        raise SystemExit("El modo continuo no arma la evidencia del tardío. Correr sin --no-demo.")

    STAGING_ROOT.mkdir(parents=True, exist_ok=True)
    log = RunLog()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    exit_code = 1
    try:
        log.log(f"spark={spark.version}")
        _copied, counts = run_demo(spark, log)
        verify_against_batch(spark, counts, log)
        write_gold(spark, counts)
        log.log(f"parquet: {OUTPUT_DIR}")
        log.log(f"csv: {CSV_PATH}")
        exit_code = 0
        log.log("M9 OK")
    except Exception as exc:
        log.log(f"M9 FALLÓ: {exc}")
        exit_code = 1
    finally:
        spark.stop()
        EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
        EVIDENCE_PATH.write_text(log.text(), encoding="utf-8")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
