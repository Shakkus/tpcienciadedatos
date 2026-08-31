#!/usr/bin/env python3
"""Genera el universo sintético de NexoUrbano para el TP.

Uso:
    python tools/generar_datos.py --seed 101 --n-viajes 250000

La semilla debe ser única por squad (nro de grupo). Reproduce IDs y eventos.
No requiere dependencias externas (stdlib only).
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path

CIUDAD = {
    "nombre": "Buenos Aires",
    "lat": -34.6037,
    "lon": -58.3816,
}

INTERESES = [
    "ciclovia",
    "cafe",
    "musica",
    "running",
    "fotografia",
    "clima",
    "tecnologia",
    "futbol",
    "diseno",
    "gastronomia",
]

CATEGORIAS = ["pase", "recarga", "accesorio", "seguro"]

MODELOS = ["Nexo S1", "Nexo S2", "Nexo Bike A", "Nexo Bike C"]

TICKET_VOCAB = [
    ("bateria", "La bateria no carga en la estacion {est}"),
    ("freno", "Freno irregular en vehiculo {vid}"),
    ("app", "La app cierra al iniciar viaje {vid}"),
    ("cobro", "Doble cobro en pase mensual rider {rid}"),
    ("gps", "GPS saltando en {est}"),
    ("candado", "Candado no libera en estacion {est}"),
    ("pago", "payment_fail repetido rider {rid}"),
    ("rueda", "Rueda pinchada scooter {vid}"),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generador NexoUrbano")
    p.add_argument("--seed", type=int, required=True, help="Semilla unica del squad")
    p.add_argument("--n-viajes", type=int, default=250_000)
    p.add_argument("--n-riders", type=int, default=120)
    p.add_argument("--n-vehiculos", type=int, default=80)
    p.add_argument("--n-estaciones", type=int, default=24)
    p.add_argument("--n-lecturas", type=int, default=6_000)
    p.add_argument("--n-ventas", type=int, default=4_000)
    p.add_argument("--out", type=Path, default=Path("data"))
    return p.parse_args()


def jitter(rng: random.Random, lat: float, lon: float, scale: float = 0.04) -> tuple[float, float]:
    return round(lat + rng.uniform(-scale, scale), 6), round(lon + rng.uniform(-scale, scale), 6)


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def estaciones(rng: random.Random, n: int) -> list[dict]:
    barrios = [
        "Palermo",
        "Recoleta",
        "San Telmo",
        "Belgrano",
        "Caballito",
        "Almagro",
        "Villa Crespo",
        "Nunez",
        "Colegiales",
        "Retiro",
        "Microcentro",
        "Chacarita",
    ]
    rows = []
    for i in range(n):
        lat, lon = jitter(rng, CIUDAD["lat"], CIUDAD["lon"])
        rows.append(
            {
                "station_id": f"ST{i:03d}",
                "nombre": f"Estacion {barrios[i % len(barrios)]} {i}",
                "barrio": barrios[i % len(barrios)],
                "lat": lat,
                "lon": lon,
                "capacidad": rng.randint(8, 28),
            }
        )
    return rows


def riders(rng: random.Random, n: int) -> list[dict]:
    rows = []
    for i in range(n):
        rows.append(
            {
                "rider_id": f"R{i:04d}",
                "nombre": f"Rider_{i:04d}",
                "email": f"rider{i:04d}@nexo.example",
                "plan": rng.choice(["free", "plus", "pro"]),
                "alta": (datetime(2025, 1, 1) + timedelta(days=rng.randint(0, 500))).date().isoformat(),
                "ciudad": CIUDAD["nombre"],
                "interes_1": rng.choice(INTERESES),
                "interes_2": rng.choice(INTERESES),
            }
        )
    return rows


def vehiculos(rng: random.Random, n: int) -> list[dict]:
    rows = []
    for i in range(n):
        rows.append(
            {
                "vehicle_id": f"V{i:04d}",
                "modelo": rng.choice(MODELOS),
                "soh_bateria": round(rng.uniform(62, 99), 1),
                "estado": rng.choices(
                    ["disponible", "en_viaje", "mantenimiento", "bateria_baja"],
                    weights=[55, 25, 10, 10],
                )[0],
            }
        )
    return rows


def viajes(
    rng: random.Random,
    n: int,
    estaciones_rows: list[dict],
    riders_rows: list[dict],
    vehiculos_rows: list[dict],
) -> list[dict]:
    start = datetime(2026, 3, 1, tzinfo=timezone.utc)
    rows: list[dict] = []
    for i in range(n):
        o = rng.choice(estaciones_rows)
        d = rng.choice(estaciones_rows)
        ts = start + timedelta(minutes=rng.randint(0, 60 * 24 * 90))
        duration = max(45, int(rng.lognormvariate(5.5, 0.7)))
        if rng.random() < 0.02:
            duration = rng.choice([8, 12, 20000])
        dist = max(0.1, rng.random() * 8.5)
        if o["station_id"] == d["station_id"]:
            dist = round(rng.uniform(0.0, 0.4), 3)
        fare = round(80 + duration * 0.35 + dist * 40, 2)
        rows.append(
            {
                "ride_id": f"T{i:07d}",
                "ts_start": ts.isoformat(),
                "ts_end": (ts + timedelta(seconds=duration)).isoformat(),
                "duration_s": duration,
                "rider_id": rng.choice(riders_rows)["rider_id"],
                "vehicle_id": rng.choice(vehiculos_rows)["vehicle_id"],
                "station_start": o["station_id"],
                "station_end": d["station_id"],
                "barrio_start": o["barrio"],
                "barrio_end": d["barrio"],
                "lat_start": o["lat"],
                "lon_start": o["lon"],
                "lat_end": d["lat"],
                "lon_end": d["lon"],
                "km": round(dist, 3),
                "fare_ars": fare,
                "clima_codigo": rng.choice([0, 0, 0, 1, 2, 3, 61, 63, 80]),
            }
        )
    return rows


def lecturas(rng: random.Random, n: int, vehiculos_rows: list[dict], estaciones_rows: list[dict]) -> list[dict]:
    start = datetime(2026, 5, 1, tzinfo=timezone.utc)
    rows = []
    for i in range(n):
        v = rng.choice(vehiculos_rows)
        e = rng.choice(estaciones_rows)
        ts = start + timedelta(seconds=i * 12 + rng.randint(0, 8))
        batt = max(3.0, min(100.0, rng.gauss(64, 18)))
        rows.append(
            {
                "vehicle_id": v["vehicle_id"],
                "ts": ts.isoformat(),
                "lat": e["lat"] + rng.uniform(-0.002, 0.002),
                "lon": e["lon"] + rng.uniform(-0.002, 0.002),
                "battery_pct": round(batt, 1),
                "temp_c": round(rng.gauss(21, 6), 1),
                "status": "battery_low" if batt < 18 else rng.choice(["ok", "ok", "ok", "idle"]),
            }
        )
    return rows


def productos(rng: random.Random) -> list[dict]:
    docs = []
    sku_n = 0
    for cat in CATEGORIAS:
        for i in range(10):
            sku_n += 1
            docs.append(
                {
                    "sku": f"SKU{sku_n:03d}",
                    "nombre": f"{cat.title()} {i+1}",
                    "categoria": cat,
                    "tags": rng.sample(["urbano", "noche", "lluvia", "estudiante", "pro"], k=2),
                    "precios": {
                        "Buenos Aires": rng.choice([1990, 3490, 5990, 9990, 14990]),
                        "Cordoba": rng.choice([1890, 3290, 5790, 9790]),
                    },
                    "stock": {"Buenos Aires": rng.randint(0, 400), "Cordoba": rng.randint(0, 120)},
                    "activo": sku_n != 7,
                }
            )
    return docs


def ventas(rng: random.Random, n: int, productos_docs: list[dict], riders_rows: list[dict]) -> list[dict]:
    start = datetime(2026, 1, 15, tzinfo=timezone.utc)
    rows = []
    activos = [p for p in productos_docs if p["activo"]]
    for i in range(n):
        p = rng.choice(activos)
        ciudad = rng.choice(["Buenos Aires", "Cordoba"])
        ts = start + timedelta(hours=rng.randint(0, 24 * 150))
        rows.append(
            {
                "venta_id": f"S{i:06d}",
                "ts": ts.isoformat(),
                "sku": p["sku"],
                "categoria": p["categoria"],
                "rider_id": rng.choice(riders_rows)["rider_id"],
                "ciudad": ciudad,
                "monto_ars": p["precios"][ciudad],
                "canal": rng.choice(["app", "app", "web", "estacion"]),
            }
        )
    return rows


def grafo(rng: random.Random, riders_rows: list[dict], estaciones_rows: list[dict]) -> dict:
    ids = [r["rider_id"] for r in riders_rows]
    amigos = set()
    while len(amigos) < min(220, len(ids) * 2):
        a, b = rng.sample(ids, 2)
        if a > b:
            a, b = b, a
        amigos.add((a, b))
    viajo = []
    for _ in range(160):
        a, b = rng.sample(ids, 2)
        viajo.append(
            {
                "a": a,
                "b": b,
                "station_id": rng.choice(estaciones_rows)["station_id"],
                "n": rng.randint(1, 7),
            }
        )
    gustos = [
        {"rider_id": r["rider_id"], "interes": r["interes_1"]} for r in riders_rows
    ] + [
        {"rider_id": r["rider_id"], "interes": r["interes_2"]}
        for r in riders_rows
        if r["interes_2"] != r["interes_1"]
    ]
    return {
        "amigos": [{"a": a, "b": b} for a, b in sorted(amigos)],
        "viajo_con": viajo,
        "le_gusta": gustos,
    }


def texto_ops(rng: random.Random, estaciones_rows: list[dict], vehiculos_rows: list[dict], riders_rows: list[dict]) -> str:
    lineas = []
    for _ in range(2_400):
        key, plantilla = rng.choice(TICKET_VOCAB)
        lineas.append(
            plantilla.format(
                est=rng.choice(estaciones_rows)["station_id"],
                vid=rng.choice(vehiculos_rows)["vehicle_id"],
                rid=rng.choice(riders_rows)["rider_id"],
            )
            + f" #{key}"
        )
    return "\n".join(lineas) + "\n"


def app_log(rng: random.Random, n: int, vehiculos_rows: list[dict], riders_rows: list[dict]) -> str:
    events = ["ride_start", "ride_end", "payment_fail", "battery_low", "unlock_ok"]
    weights = [30, 30, 8, 10, 22]
    start = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
    lineas = []
    for i in range(n):
        ev = rng.choices(events, weights=weights)[0]
        ts = start + timedelta(seconds=i * 3)
        payload = {
            "ts": ts.isoformat(),
            "event": ev,
            "vehicle_id": rng.choice(vehiculos_rows)["vehicle_id"],
            "rider_id": rng.choice(riders_rows)["rider_id"],
            "battery_pct": round(max(4.0, rng.gauss(58, 20)), 1),
        }
        lineas.append(json.dumps(payload, ensure_ascii=True))
    return "\n".join(lineas) + "\n"


def mysql_seed(riders_rows: list[dict]) -> str:
    lines = [
        "CREATE DATABASE IF NOT EXISTS crm;",
        "USE crm;",
        "DROP TABLE IF EXISTS riders;",
        """CREATE TABLE riders (
            rider_id VARCHAR(16) PRIMARY KEY,
            nombre VARCHAR(64),
            email VARCHAR(128),
            plan VARCHAR(16),
            alta DATE,
            ciudad VARCHAR(64)
        );""",
    ]
    for r in riders_rows:
        lines.append(
            "INSERT INTO riders VALUES ('{rider_id}','{nombre}','{email}','{plan}','{alta}','{ciudad}');".format(
                **r
            )
        )
    return "\n".join(lines) + "\n"


def stream_samples(log_text: str, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for i, line in enumerate(log_text.strip().split("\n")[:40]):
        (out_dir / f"event_{i:03d}.json").write_text(line + "\n", encoding="utf-8")


def manifiesto(args: argparse.Namespace, out: Path, n_viajes: int) -> None:
    txt = textwrap.dedent(
        f"""\
        NexoUrbano dataset sintético
        seed={args.seed}
        ciudad={CIUDAD["nombre"]}
        n_viajes={n_viajes}
        generado_utc={datetime.now(timezone.utc).isoformat()}

        Archivos:
        - estaciones.csv, riders.csv, vehiculos.csv
        - viajes.csv (usar este como Bronze de movilidad)
        - viajes_muestra.csv (2 000 filas, HDFS / Azure manual)
        - lecturas_sensores.csv (Cassandra)
        - productos.json, ventas.csv (Mongo)
        - grafo.json (Neo4j)
        - texto_ops.txt (MapReduce WordCount)
        - app.log (Flume / Streaming)
        - stream/*.json (Structured Streaming filesource)
        - mysql/crm_riders.sql (Sqoop / JDBC)
        """
    )
    (out / "MANIFIESTO.txt").write_text(txt, encoding="utf-8")


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    out: Path = args.out
    out.mkdir(parents=True, exist_ok=True)

    est = estaciones(rng, args.n_estaciones)
    rid = riders(rng, args.n_riders)
    veh = vehiculos(rng, args.n_vehiculos)
    via = viajes(rng, args.n_viajes, est, rid, veh)
    lec = lecturas(rng, args.n_lecturas, veh, est)
    prods = productos(rng)
    ven = ventas(rng, args.n_ventas, prods, rid)
    g = grafo(rng, rid, est)
    ops = texto_ops(rng, est, veh, rid)
    log = app_log(rng, 800, veh, rid)

    write_csv(out / "estaciones.csv", est, list(est[0].keys()))
    write_csv(out / "riders.csv", rid, list(rid[0].keys()))
    write_csv(out / "vehiculos.csv", veh, list(veh[0].keys()))
    write_csv(out / "viajes.csv", via, list(via[0].keys()))
    write_csv(out / "viajes_muestra.csv", via[:2000], list(via[0].keys()))
    write_csv(out / "lecturas_sensores.csv", lec, list(lec[0].keys()))
    write_csv(out / "ventas.csv", ven, list(ven[0].keys()))

    (out / "productos.json").write_text(json.dumps(prods, indent=2, ensure_ascii=False), encoding="utf-8")
    (out / "grafo.json").write_text(json.dumps(g, indent=2, ensure_ascii=False), encoding="utf-8")
    (out / "texto_ops.txt").write_text(ops, encoding="utf-8")
    (out / "app.log").write_text(log, encoding="utf-8")
    (out / "mysql").mkdir(exist_ok=True)
    (out / "mysql" / "crm_riders.sql").write_text(mysql_seed(rid), encoding="utf-8")
    stream_samples(log, out / "stream")
    manifiesto(args, out, len(via))

    print(f"OK seed={args.seed} → {out.resolve()}")
    print(f"viajes={len(via):,} lecturas={len(lec):,} ventas={len(ven):,} riders={len(rid)}")


if __name__ == "__main__":
    main()
