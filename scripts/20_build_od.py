#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Construcción de pares Origen–Destino (OD) por tarjeta, a partir del limpio:
- Entrada: data/interim/cleaned.parquet
- Salida:  data/processed/od_pairs.parquet (y CSV)
Criterio: para cada id_tarjeta, toma el PRIMER par (origen,destino) tal que
hora_destino - hora_origen >= min_gap_horas (default: 3 horas).
Además filtra etapa_red_sube == 0.
"""

from pathlib import Path
import argparse
import pandas as pd

IN_PATH = Path("data/interim/cleaned.parquet")
OUT_DIR = Path("data/processed")
OUT_PARQUET = OUT_DIR / "od_pairs.parquet"
OUT_CSV = OUT_DIR / "od_pairs.csv"


def build_pairs(df: pd.DataFrame, min_gap_hours: int) -> pd.DataFrame:
    # Asegurar columnas necesarias
    needed = {
        "id_tarjeta", "lat", "lon", "hora", "modo", "interno_bus",
        "id_ramal", "etapa_red_sube"
    }
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas en cleaned.parquet: {missing}")

    # Filtrar por etapa_red_sube == 0 
    df = df[df["etapa_red_sube"] == 0].copy()

    # Tipos esperados
    df["hora"] = pd.to_numeric(df["hora"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["id_tarjeta", "lat", "lon", "hora"]).copy()

    # Orden
    df = df.sort_values(["id_tarjeta", "hora"]).reset_index(drop=True)

    resultados = []
    for tarjeta, g in df.groupby("id_tarjeta", sort=False):
        g = g.reset_index(drop=True)
        if len(g) < 2:
            continue

        # Tomamos el PRIMER origen y buscamos el primer destino con gap >= min_gap_hours
        origen = g.iloc[0]
        candidatos = g.loc[(g["hora"] >= origen["hora"] + min_gap_hours) &
                           ~((g["lat"] == origen["lat"]) & (g["lon"] == origen["lon"]))]

        if candidatos.empty:
            continue

        destino = candidatos.iloc[0]

        resultados.append({
            "id_tarjeta": tarjeta,

            "lat_origen": float(origen["lat"]),
            "lon_origen": float(origen["lon"]),
            "hora_origen": int(origen["hora"]),
            "modo_origen": origen.get("modo"),
            "interno_origen": origen.get("interno_bus"),
            "ramal_origen": origen.get("id_ramal"),

            "lat_destino": float(destino["lat"]),
            "lon_destino": float(destino["lon"]),
            "hora_destino": int(destino["hora"]),
            "modo_destino": destino.get("modo"),
            "interno_destino": destino.get("interno_bus"),
            "ramal_destino": destino.get("id_ramal"),
        })

    return pd.DataFrame(resultados)


def main():
    parser = argparse.ArgumentParser(description="Construye pares OD por tarjeta.")
    parser.add_argument("--min-gap-horas", type=int, default=3,
                        help="Separación mínima (horas) entre origen y destino (default: 3).")
    args = parser.parse_args()

    if not IN_PATH.exists():
        raise FileNotFoundError(f"No se encontró {IN_PATH}. Corré antes scripts/10_clean_sube.py")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(IN_PATH)
    od = build_pairs(df, min_gap_hours=args.min_gap_horas)

    # Guardar
    od.to_parquet(OUT_PARQUET, index=False)
    od.to_csv(OUT_CSV, index=False)

    print(f"✔ Pares OD generados: {len(od):,}")
    print(f"   - {OUT_PARQUET}")
    print(f"   - {OUT_CSV}")


if __name__ == "__main__":
    main()
