#!/usr/bin/env python3
"""
Perfilado del dataset abierto (M1 NexoUrbano).
Uso:
  python 01-discovery/perfilado.py
  python 01-discovery/perfilado.py --csv data/Dataset_abierto_real.csv --out evidence/discovery/perfilado_dataset_abierto_real.txt
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from statistics import mean, median

MIN_FILAS = 100_000
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CSV = REPO_ROOT / "data" / "Dataset_abierto_real.csv"
DEFAULT_OUT = REPO_ROOT / "evidence" / "discovery" / "perfilado_dataset_abierto_real.txt"


@dataclass
class ColumnProfile:
    name: str
    null_count: int = 0
    empty_count: int = 0
    distinct: set[str] = field(default_factory=set)
    distinct_cap: int = 50_000
    numeric_values: list[float] = field(default_factory=list)
    truncated_distinct: bool = False

    def observe(self, raw: str | None) -> None:
        if raw is None:
            self.null_count += 1
            return
        s = raw.strip()
        if s == "":
            self.empty_count += 1
            return
        if len(self.distinct) < self.distinct_cap:
            self.distinct.add(s)
        elif s not in self.distinct:
            self.truncated_distinct = True
        try:
            self.numeric_values.append(float(s.replace(",", ".")))
        except ValueError:
            pass


def infer_kind(name: str, col: ColumnProfile, n_rows: int) -> str:
    if col.numeric_values and len(col.numeric_values) >= n_rows * 0.9:
        return "numeric"
    if name in {"fecha_alta"} or "fecha" in name:
        return "date"
    if name in {"hora_alta"} or "hora" in name:
        return "time"
    card = len(col.distinct)
    if card <= max(20, n_rows // 1000):
        return "categorical"
    return "string"


def parse_date(s: str) -> datetime | None:
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            continue
    return None


def profile_csv(path: Path) -> tuple[int, list[str], dict[str, ColumnProfile]]:
    profiles: dict[str, ColumnProfile] = {}
    n_rows = 0
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"CSV sin encabezado: {path}")
        columns = list(reader.fieldnames)
        for c in columns:
            cap = 2_000_000 if c == "id" else 50_000
            profiles[c] = ColumnProfile(name=c, distinct_cap=cap)
        for row in reader:
            n_rows += 1
            for c in columns:
                profiles[c].observe(row.get(c))
    return n_rows, columns, profiles


def numeric_outliers(values: list[float], label: str) -> list[str]:
    lines: list[str] = []
    if not values:
        return [f"  {label}: sin valores numéricos parseables"]
    lo, hi = min(values), max(values)
    lines.append(f"  {label}: min={lo}, max={hi}, media={mean(values):.2f}, mediana={median(values):.2f}")
    if label == "edad":
        bad = sum(1 for v in values if v < 16 or v > 100)
        lines.append(f"  edad fuera de [16,100] (outliers operativos): {bad} ({100 * bad / len(values):.2f}%)")
    return lines


def temporal_drift(path: Path, n_rows: int) -> list[str]:
    """Muestra drift por año/mes usando fecha_alta."""
    by_year: Counter[int] = Counter()
    by_month: Counter[tuple[int, int]] = Counter()
    dates: list[datetime] = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            d = parse_date(row.get("fecha_alta") or "")
            if d:
                dates.append(d)
                by_year[d.year] += 1
                by_month[(d.year, d.month)] += 1
    lines = ["Drift temporal (fecha_alta):"]
    if not dates:
        lines.append("  No se pudieron parsear fechas.")
        return lines
    lines.append(f"  Rango: {min(dates).date()} -> {max(dates).date()}")
    lines.append(f"  Filas con fecha parseable: {len(dates)} / {n_rows}")
    lines.append("  Conteo por año:")
    for y in sorted(by_year):
        lines.append(f"    {y}: {by_year[y]:,}")
    lines.append("  Top 6 meses (año-mes):")
    for (y, m), cnt in by_month.most_common(6):
        lines.append(f"    {y}-{m:02d}: {cnt:,}")
    return lines


def business_metrics(path: Path) -> list[str]:
    """Cinco métricas de negocio sobre el dataset abierto (altas de usuarios)."""
    genero = Counter()
    tipo_dia = Counter()
    by_hour: Counter[int] = Counter()
    edades: list[float] = []
    n = 0
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            n += 1
            genero[row.get("genero") or ""] += 1
            tipo_dia[row.get("tipo_dia") or ""] += 1
            h = (row.get("hora_alta") or "").split(":")[0]
            if h.isdigit():
                by_hour[int(h)] += 1
            try:
                edades.append(float(row["edad"]))
            except (KeyError, ValueError):
                pass
    lines = [
        "Métricas de negocio (dataset abierto — altas / usuarios):",
        f"  1) Total registros perfilados: {n:,}",
        f"  2) Distribución género: {dict(genero.most_common())}",
        f"  3) Edad p50 / p95: {median(edades):.0f} / {sorted(edades)[int(0.95 * len(edades)) - 1]:.0f} años"
        if edades
        else "  3) Edad: N/A",
        f"  4) Hora de alta más frecuente (moda): {by_hour.most_common(1)[0] if by_hour else 'N/A'}",
        f"  5) Tipo de día: {dict(tipo_dia.most_common())}",
    ]
    return lines


def render_report(path: Path, n_rows: int, columns: list[str], profiles: dict[str, ColumnProfile]) -> str:
    ok_min = n_rows >= MIN_FILAS
    lines = [
        "NexoUrbano — perfilado dataset abierto",
        f"Archivo: {path.relative_to(REPO_ROOT) if path.is_relative_to(REPO_ROOT) else path}",
        f"Generado: {datetime.now().isoformat(timespec='seconds')}",
        "",
        f"Filas (sin encabezado): {n_rows:,}",
        f"Requisito mínimo 100k filas: {'CUMPLE' if ok_min else 'NO CUMPLE'} ({MIN_FILAS:,} requeridas)",
        f"Columnas ({len(columns)}): {', '.join(columns)}",
        "",
        "=== Nulos y vacíos ===",
    ]
    for c in columns:
        p = profiles[c]
        null_pct = 100 * p.null_count / n_rows if n_rows else 0
        empty_pct = 100 * p.empty_count / n_rows if n_rows else 0
        card = len(p.distinct)
        kind = infer_kind(c, p, n_rows)
        trunc = " (cardinalidad truncada en muestra)" if p.truncated_distinct else ""
        lines.append(
            f"  {c} [{kind}]: nulos={p.null_count} ({null_pct:.2f}%), "
            f"vacios={p.empty_count} ({empty_pct:.2f}%), cardinalidad~{card}{trunc}"
        )
        if c == "edad":
            lines.extend(numeric_outliers(p.numeric_values, "edad"))
        if c == "id" and n_rows:
            dup = n_rows - card if not p.truncated_distinct else None
            if dup is not None:
                lines.append(f"  id duplicados aparentes: {dup}")

    lines.append("")
    lines.extend(temporal_drift(path, n_rows))
    lines.append("")
    lines.extend(business_metrics(path))
    lines.append("")
    lines.append(
        "Nota: este CSV describe altas de usuarios (dataset abierto real). "
        "El Bronze operativo de viajes del TP sigue en data/viajes.csv (contrato bronze_viajes.yaml)."
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Perfilado dataset abierto NexoUrbano")
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV, help="Ruta al CSV abierto")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Informe de salida")
    args = parser.parse_args()
    csv_path = args.csv if args.csv.is_absolute() else REPO_ROOT / args.csv
    out_path = args.out if args.out.is_absolute() else REPO_ROOT / args.out

    if not csv_path.is_file():
        print(f"ERROR: no existe {csv_path}", file=sys.stderr)
        return 1

    n_rows, columns, profiles = profile_csv(csv_path)
    report = render_report(csv_path, n_rows, columns, profiles)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report, encoding="utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass
    print(report)
    if n_rows < MIN_FILAS:
        print(f"ERROR: menos de {MIN_FILAS:,} filas.", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
