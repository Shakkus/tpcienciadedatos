#!/usr/bin/env python3
"""
Particiona data/viajes.csv por ts_start (yyyy/mm/dd) y sube a MinIO bucket bronze.

Uso:
  python tools/bronze_a_minio.py
  python tools/bronze_a_minio.py --dry-run
"""

from __future__ import annotations

import argparse
import csv
import sys
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CSV = REPO_ROOT / "data" / "viajes.csv"
BUCKETS = ("bronze", "silver", "gold")
OBJECT_PREFIX = "viajes"


def parse_ts_start(raw: str) -> datetime:
    s = raw.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def partition_key(ts: datetime) -> str:
    return f"{OBJECT_PREFIX}/{ts.year}/{ts.month:02d}/{ts.day:02d}/viajes.csv"


def partition_csv(
    src: Path, staging: Path
) -> tuple[dict[str, Path], dict[str, int], list[str]]:
    writers: dict[str, csv.DictWriter] = {}
    files: dict[str, Path] = {}
    handles: dict[str, object] = {}
    counts: defaultdict[str, int] = defaultdict(int)
    fieldnames: list[str] = []

    with src.open(newline="", encoding="utf-8") as f_in:
        reader = csv.DictReader(f_in)
        if not reader.fieldnames:
            raise ValueError("CSV sin encabezado")
        fieldnames = list(reader.fieldnames)
        for row in reader:
            ts = parse_ts_start(row["ts_start"])
            key = partition_key(ts)
            counts[key] += 1
            if key not in writers:
                rel = Path(*key.split("/"))
                out = staging / rel
                out.parent.mkdir(parents=True, exist_ok=True)
                fh = out.open("w", newline="", encoding="utf-8")
                handles[key] = fh
                writers[key] = csv.DictWriter(fh, fieldnames=fieldnames)
                writers[key].writeheader()
                files[key] = out
            writers[key].writerow(row)

    for fh in handles.values():
        fh.close()

    return files, dict(counts), fieldnames


def ensure_buckets(client, dry_run: bool) -> None:
    from minio.error import S3Error

    for name in BUCKETS:
        if dry_run:
            print(f"[dry-run] bucket: {name}")
            continue
        try:
            if not client.bucket_exists(name):
                client.make_bucket(name)
                print(f"Bucket creado: {name}")
            else:
                print(f"Bucket existe: {name}")
        except S3Error as e:
            raise SystemExit(f"Error bucket {name}: {e}") from e


def upload_files(client, files: dict[str, Path], dry_run: bool) -> list[str]:
    uploaded: list[str] = []
    for key, path in sorted(files.items()):
        uri = f"s3a://bronze/{key}"
        if dry_run:
            print(f"[dry-run] subir {path.name} -> {uri} ({path.stat().st_size} bytes)")
            uploaded.append(uri)
            continue
        client.fput_object("bronze", key, str(path), content_type="text/csv")
        print(f"Subido: {uri}")
        uploaded.append(uri)
    return uploaded


def write_evidence(
    client,
    csv_path: Path,
    endpoint: str,
    total: int,
    n_parts: int,
    uploaded: list[str],
) -> Path:
    evidence = REPO_ROOT / "evidence" / "docker" / "minio_bronze_viajes.txt"
    evidence.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "NexoUrbano — Bronze viajes en MinIO",
        f"Origen: {csv_path.relative_to(REPO_ROOT)}",
        f"Endpoint API: http://{endpoint}",
        f"Consola: http://localhost:9001",
        f"Filas subidas: {total:,}",
        f"Particiones (días): {n_parts}",
        "",
        "URI base: s3a://bronze/viajes/yyyy/mm/dd/viajes.csv",
        "",
        "Muestra de URIs:",
    ]
    for u in uploaded[:10]:
        lines.append(f"  {u}")
    if len(uploaded) > 10:
        lines.append(f"  ... y {len(uploaded) - 10} particiones más")
    lines.append("")
    lines.append("Objetos en bronze (prefijo viajes/):")
    n_obj = 0
    for obj in client.list_objects("bronze", prefix=f"{OBJECT_PREFIX}/", recursive=True):
        lines.append(f"  bronze/{obj.object_name} ({obj.size} bytes)")
        n_obj += 1
    lines.append(f"Total objetos listados: {n_obj}")
    evidence.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return evidence


def main() -> int:
    parser = argparse.ArgumentParser(description="Bronze viajes -> MinIO")
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--endpoint", default="localhost:9000")
    parser.add_argument("--access-key", default="nexo")
    parser.add_argument("--secret-key", default="nexo-urban-2026")
    parser.add_argument("--secure", action="store_true", help="HTTPS")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-upload", action="store_true", help="Solo particionar (debug)")
    args = parser.parse_args()

    csv_path = args.csv if args.csv.is_absolute() else REPO_ROOT / args.csv
    if not csv_path.is_file():
        print(f"ERROR: no existe {csv_path}", file=sys.stderr)
        return 1

    try:
        from minio import Minio
    except ImportError:
        print("Instalar: pip install minio", file=sys.stderr)
        return 1

    client = Minio(
        args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        secure=args.secure,
    )

    ensure_buckets(client, args.dry_run)

    with tempfile.TemporaryDirectory(prefix="nexo_bronze_") as tmp:
        staging = Path(tmp)
        files, counts, _ = partition_csv(csv_path, staging)
        total = sum(counts.values())
        print(f"Particiones: {len(files)} archivos, {total:,} filas")
        if args.skip_upload:
            return 0
        uploaded = upload_files(client, files, args.dry_run)

    if args.dry_run:
        return 0

    evidence = write_evidence(client, csv_path, args.endpoint, total, len(files), uploaded)
    print(f"Evidencia: {evidence.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
