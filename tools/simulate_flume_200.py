#!/usr/bin/env python3
"""Replay 200 synthetic app events to HDFS as a Flume-equivalent simulation."""

from __future__ import annotations

import json
import subprocess
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data" / "app.log"
EVIDENCE = ROOT / "evidence" / "hadoop" / "flume.log"
CONTAINER = "nexo-hadoop-namenode"
HDFS_ROOT = "/nexo/bronze/logs/simulation"
QUOTAS = {"ride_start": 67, "ride_end": 67, "payment_fail": 66}


def docker_exec(*args: str, input_data: bytes | None = None) -> bytes:
    result = subprocess.run(
        ["docker", "exec", "-i", CONTAINER, *args],
        input=input_data,
        capture_output=True,
        check=False,
    )
    if result.returncode:
        error = result.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"docker exec {' '.join(args)} falló: {error}")
    return result.stdout


def main() -> None:
    if not SOURCE.is_file():
        raise FileNotFoundError(f"No existe el log de entrada: {SOURCE}")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    hdfs_dir = f"{HDFS_ROOT}/{run_id}"
    hdfs_file = f"{hdfs_dir}/app_events.jsonl"

    selected: list[dict[str, object]] = []
    counts: Counter[str] = Counter()
    with SOURCE.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"JSON inválido en {SOURCE}:{line_number}") from exc
            event_type = event.get("event")
            if event_type in QUOTAS and counts[event_type] < QUOTAS[event_type]:
                selected.append(event)
                counts[event_type] += 1
            if len(selected) == sum(QUOTAS.values()):
                break

    if len(selected) != 200 or any(counts[name] != count for name, count in QUOTAS.items()):
        raise ValueError(
            f"{SOURCE} no contiene suficientes eventos requeridos: "
            f"obtenidos={dict(counts)}, esperados={QUOTAS}"
        )

    payload = "".join(json.dumps(event, separators=(",", ":")) + "\n" for event in selected).encode()
    docker_exec("hdfs", "dfs", "-mkdir", "-p", hdfs_dir)
    docker_exec("hdfs", "dfs", "-put", "-", hdfs_file, input_data=payload)

    persisted = docker_exec("hdfs", "dfs", "-cat", hdfs_file)
    persisted_events = [json.loads(line) for line in persisted.splitlines()]
    persisted_counts = Counter(event["event"] for event in persisted_events)
    if len(persisted_events) != 200 or persisted_counts != counts or persisted != payload:
        raise RuntimeError("La lectura de HDFS no coincide con los 200 eventos simulados.")

    listing = docker_exec("hdfs", "dfs", "-ls", hdfs_file).decode().strip()
    evidence_lines = [
        "M7 Flume equivalent simulation",
        f"source={SOURCE.relative_to(ROOT).as_posix()}",
        "mode=deterministic replay of generated JSONL events to HDFS (Flume-compatible sink target)",
        "events_total=200",
        *(f"event_{name}={counts[name]}" for name in QUOTAS),
        f"hdfs_file={hdfs_file}",
        f"hdfs_lines_verified={len(persisted_events)}",
        "hdfs_content_matches_input=true",
        listing,
    ]
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE.write_text("\n".join(evidence_lines) + "\n", encoding="utf-8")
    print("\n".join(evidence_lines))


if __name__ == "__main__":
    main()
