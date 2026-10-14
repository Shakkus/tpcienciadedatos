#!/usr/bin/env python3
"""M14 — cinco decisiones de operaciones.

Lee los exports ya generados. No recalcula Spark, Cassandra ni Neo4j.
Correr desde la raíz del repo:

    python 09-producto/decision_ops.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
PATH_CASSANDRA = REPO / "09-producto" / "cassandra_ultima_lectura.json"
PATH_MONGO = REPO / "09-producto" / "ventas_agg.json"
PATH_NEO4J = REPO / "evidence" / "neo4j" / "queries_result.txt"
PATH_SPARK = REPO / "09-producto" / "gold_repuesto_lluvia.csv"
PATH_WORDCOUNT = REPO / "04-hadoop" / "mapreduce" / "part-r-00000"
PATH_TOP20 = REPO / "evidence" / "hadoop" / "top20.txt"
PATH_HIVE_JSON = REPO / "09-producto" / "hive_facturacion_cobro.json"
PATH_HIVE_CSV = REPO / "09-producto" / "hive_facturacion_cobro.csv"
OUT = REPO / "09-producto" / "decision_ops_resultado.json"

TARIFA_PLANA_ARS = 320.0
UMBRAL_BATERIA = 20.0
RIDER = "R0000"


def configurar_stdout() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")


def limpiar_celda(valor: str) -> str:
    return valor.strip().strip('"').strip("'")


def leer_tabla_texto(ruta: Path, columnas_requeridas: set[str]) -> list[dict[str, str]]:
    """Extrae filas de una tabla ASCII de cypher-shell por sus nombres de columna."""
    lineas = ruta.read_text(encoding="utf-8").splitlines()
    for indice, linea in enumerate(lineas):
        if "|" not in linea:
            continue
        encabezado = [limpiar_celda(c) for c in linea.strip().strip("|").split("|")]
        encabezado_normalizado = [c.lower() for c in encabezado]
        if not columnas_requeridas.issubset(encabezado_normalizado):
            continue
        filas = []
        for fila_texto in lineas[indice + 1 :]:
            if not fila_texto.strip():
                break
            if "|" not in fila_texto:
                continue
            celdas = [limpiar_celda(c) for c in fila_texto.strip().strip("|").split("|")]
            if len(celdas) != len(encabezado):
                continue
            filas.append(dict(zip(encabezado_normalizado, celdas)))
        return filas
    return []


def conteo_cobro(ruta: Path) -> int | None:
    """Suma cobro y #cobro. La primera línea sola vale 296; el top normalizado es 592."""
    if not ruta.is_file():
        return None
    total = 0
    encontrado = False
    for linea in ruta.read_text(encoding="utf-8").splitlines():
        if linea.startswith("#") and not linea.startswith("#cobro") and "\t" not in linea[:8]:
            continue
        partes = linea.split()
        if len(partes) < 2:
            continue
        palabra = partes[0].lstrip("#").lower()
        if palabra != "cobro":
            continue
        try:
            total += int(partes[-1])
            encontrado = True
        except ValueError:
            continue
    return total if encontrado else None


def conteo_top20(ruta: Path) -> int | None:
    if not ruta.is_file():
        return None
    for linea in ruta.read_text(encoding="utf-8").splitlines():
        partes = linea.split()
        if len(partes) == 2 and partes[0] == "cobro":
            return int(partes[1])
    return None


def sep() -> None:
    print("-" * 60)


def pregunta_reponer() -> dict:
    print("1. ¿Dónde reponer scooters en las próximas 3 h si llueve?")
    if not PATH_SPARK.is_file():
        print("  [DATO FALTANTE] No se encontró gold_repuesto_lluvia.csv.")
        return {"decision": "sin csv de Gold"}
    df = pd.read_csv(PATH_SPARK)
    for col in ("pct_clima_adverso", "viajes", "precip_mm_prom"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["station_start", "pct_clima_adverso"])
    df = df.sort_values(
        ["pct_clima_adverso", "viajes"],
        ascending=[False, False],
    )
    mejor = df.iloc[0]
    viajes = int(df["viajes"].sum())
    print(
        f"  [REFERENCIA HISTÓRICA] {mejor['station_start']}: "
        f"{mejor['pct_clima_adverso']:.2f}% de clima adverso, "
        f"{int(mejor['viajes'])} viajes."
    )
    print(
        "  [DATO FALTANTE] No hay pronóstico de las próximas 3 horas "
        "ni ocupación actual por estación. M3 usó Open-Meteo histórico."
    )
    print("  [TOP 3]")
    print(df[["station_start", "viajes", "pct_clima_adverso"]].head(3).to_string(index=False))
    return {
        "estacion": str(mejor["station_start"]),
        "pct_clima_adverso": round(float(mejor["pct_clima_adverso"]), 2),
        "viajes_estacion": int(mejor["viajes"]),
        "viajes_historicos": viajes,
        "limite": "Historico de Gold, no pronostico de 3 horas.",
    }


def pregunta_retirar() -> dict:
    print("2. ¿Qué vehículo hay que retirar hoy?")
    lecturas = json.loads(PATH_CASSANDRA.read_text(encoding="utf-8"))
    df = pd.DataFrame(lecturas)
    df["battery_pct"] = pd.to_numeric(df["battery_pct"], errors="coerce")
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    print(
        "  [SUPUESTO] El export no trae soh_bateria. "
        f"El umbral operativo es battery_pct < {UMBRAL_BATERIA:g}. "
        "No es salud de batería."
    )
    if df["ts"].notna().any():
        ultima = df["ts"].max()
        print(f"  [FRESCURA] Lectura del export: {ultima.isoformat()}. No es el día de hoy.")
    criticos = df[df["battery_pct"] < UMBRAL_BATERIA].sort_values("battery_pct")
    mas_bajas = df.nsmallest(3, "battery_pct")
    if criticos.empty:
        print(
            f"  [RESULTADO] Nadie está bajo {UMBRAL_BATERIA:g}%. "
            "No se retira flota con este umbral."
        )
    else:
        print(f"  [ACCIÓN] {len(criticos)} vehículo(s) bajo {UMBRAL_BATERIA:g}%.")
    print("  [MÁS BAJAS DEL EXPORT]")
    for _, fila in mas_bajas.iterrows():
        print(f"    {fila['vehicle_id']}: {fila['battery_pct']}% ({fila['status']})")
    print(
        "  [LÍMITE] Es una sola foto por vehículo, no dos lecturas consecutivas. "
        "El SOH de vehiculos.csv está entre 62 y 99 y no cambia esta lista."
    )
    return {
        "umbral_battery_pct": UMBRAL_BATERIA,
        "bajo_umbral": int(len(criticos)),
        "mas_bajas": [
            {"vehicle_id": str(r.vehicle_id), "battery_pct": float(r.battery_pct)}
            for r in mas_bajas.itertuples()
        ],
    }


def pregunta_pase() -> dict:
    print("3. ¿Qué pase venderle a un rider?")
    ventas = json.loads(PATH_MONGO.read_text(encoding="utf-8"))
    pases = [
        p
        for p in ventas.get("top5Productos", [])
        if str(p.get("categoria", "")).lower() == "pase"
    ]
    if not pases:
        print("  [DATO INSUFICIENTE] El top no trae categoría pase.")
        sku = {}
    else:
        sku = max(pases, key=lambda p: float(p.get("totalVentas") or 0))
        print(
            f"  [MONGO] {sku.get('sku')} {sku.get('nombre')}: "
            f"ARS {sku.get('totalVentas')} en el top exportado."
        )
    recomendaciones = leer_tabla_texto(
        PATH_NEO4J,
        {"rider_base", "rider_recomendado", "plan_recomendado", "estaciones_compartidas"},
    )
    de_rider = [f for f in recomendaciones if f.get("rider_base") == RIDER]
    upsell = [
        f
        for f in de_rider
        if f.get("plan_recomendado", "").lower() == "free"
        and int(f.get("estaciones_compartidas") or 0) >= 2
    ]
    upsell.sort(key=lambda f: int(f["estaciones_compartidas"]), reverse=True)
    if not upsell:
        print(f"  [DATO INSUFICIENTE] No hay upsell free con 2 estaciones para {RIDER}.")
    else:
        print(f"  [NEO4J] {RIDER} es plus. Upsell free que comparten 2 estaciones y no son amigos:")
        for fila in upsell:
            print(
                f"    {fila['rider_recomendado']} "
                f"({fila['estaciones_compartidas']} estaciones)"
            )
    print("  [LÍMITE] No hay propensión de compra por rider. El agregado de ventas es global.")
    return {
        "rider": RIDER,
        "sku": sku.get("sku"),
        "nombre": sku.get("nombre"),
        "total_ventas_ars": sku.get("totalVentas"),
        "upsell_free": [f["rider_recomendado"] for f in upsell],
    }


def pregunta_collusion() -> dict:
    print("4. ¿Hay collusion / fraude social?")
    filas = leer_tabla_texto(
        PATH_NEO4J,
        {"estacion", "riders_en_trio", "ventanas_repetidas", "tiene_relacion_social"},
    )
    alertas = [f for f in filas if f.get("tiene_relacion_social", "").upper() == "TRUE"]
    if not alertas:
        print("  [RESULTADO] No hay tríos con tiene_relacion_social = TRUE.")
        return {"trios": 0}
    max_ventanas = max(int(f.get("ventanas_repetidas") or 0) for f in alertas)
    print(f"  [ALERTA] {len(alertas)} trío(s) con vínculo social:")
    for fila in alertas:
        print(
            f"    {fila['estacion']}: {fila['riders_en_trio']}; "
            f"ventanas_repetidas={fila['ventanas_repetidas']}"
        )
    if max_ventanas <= 1:
        print("  [CONCLUSIÓN] Cada patrón aparece una sola vez. Alerta, no fraude recurrente.")
    else:
        print(
            f"  [CONCLUSIÓN] Hay hasta {max_ventanas} ventanas repetidas. "
            "Pide revisión y no prueba fraude."
        )
    return {
        "trios_con_vinculo": len(alertas),
        "max_ventanas_repetidas": max_ventanas,
        "trios": [
            {"estacion": f["estacion"], "riders": f["riders_en_trio"]} for f in alertas
        ],
    }


def pregunta_facturacion(viajes_historicos: int | None) -> dict:
    print("5. ¿Cuánto se facturó vs. menciones de 'cobro'?")
    cobro_partes = conteo_cobro(PATH_WORDCOUNT)
    cobro_top = conteo_top20(PATH_TOP20)
    if cobro_partes is None:
        print("  [DATO FALTANTE] No se pudo sumar cobro en part-r-00000.")
    else:
        print(
            f"  [WORDCOUNT] cobro + #cobro = {cobro_partes} ocurrencias en part-r-00000. "
            f"top20.txt normalizado = {cobro_top}. "
            "No son tickets únicos: texto_ops.txt no tiene fecha ni id de ticket."
        )
    revenue = None
    if viajes_historicos is not None:
        revenue = viajes_historicos * TARIFA_PLANA_ARS
        print(
            f"  [SPARK] {viajes_historicos} viajes x {TARIFA_PLANA_ARS:g} ARS "
            f"= {revenue:,.0f} ARS en todo el histórico."
        )
    if PATH_HIVE_JSON.is_file() or PATH_HIVE_CSV.is_file():
        print("  [HIVE] Hay export diario. Este script no lo interpreta todavía.")
    else:
        print(
            "  [DATO FALTANTE] No existe hive_facturacion_cobro.json ni .csv. "
            "No hay cruce por día: las menciones no tienen fecha."
        )
    return {
        "menciones_cobro": cobro_partes,
        "menciones_cobro_top20": cobro_top,
        "viajes": viajes_historicos,
        "tarifa_plana_ars": TARIFA_PLANA_ARS,
        "revenue_estimado_ars": revenue,
        "cruce_por_dia": False,
    }


def main() -> int:
    configurar_stdout()
    print("=" * 60)
    print("NEXOURBANO — DECISIONES DE OPERACIONES (M14)")
    print("=" * 60)
    bloques = {}
    try:
        bloques["1_reponer_si_llueve"] = pregunta_reponer()
    except (OSError, ValueError, KeyError) as exc:
        print(f"  [ERROR] {exc}")
        bloques["1_reponer_si_llueve"] = {"error": str(exc)}
    sep()
    try:
        bloques["2_retirar_vehiculo"] = pregunta_retirar()
    except (OSError, json.JSONDecodeError, ValueError, KeyError) as exc:
        print(f"  [ERROR] {exc}")
        bloques["2_retirar_vehiculo"] = {"error": str(exc)}
    sep()
    try:
        bloques["3_pase_a_vender"] = pregunta_pase()
    except (OSError, json.JSONDecodeError, ValueError, KeyError) as exc:
        print(f"  [ERROR] {exc}")
        bloques["3_pase_a_vender"] = {"error": str(exc)}
    sep()
    try:
        bloques["4_collusion"] = pregunta_collusion()
    except (OSError, ValueError, KeyError) as exc:
        print(f"  [ERROR] {exc}")
        bloques["4_collusion"] = {"error": str(exc)}
    sep()
    viajes = bloques.get("1_reponer_si_llueve", {}).get("viajes_historicos")
    try:
        bloques["5_facturacion_vs_cobro"] = pregunta_facturacion(viajes)
    except (OSError, ValueError) as exc:
        print(f"  [ERROR] {exc}")
        bloques["5_facturacion_vs_cobro"] = {"error": str(exc)}
    print("=" * 60)
    OUT.write_text(json.dumps(bloques, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"escrito: {OUT.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
