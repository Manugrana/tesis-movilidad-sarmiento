#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Crea un modelo nulo recableando destinos entre pares OD preservando
aproximadamente la distribución de distancias (por bins con tolerancia).

Entrada:
  - data/processed/od_pairs.parquet  (columns: lat_origen, lon_origen, lat_destino, lon_destino, ...)

Salida:
  - data/processed/od_pairs_null.parquet
  - data/processed/null_model_summary.csv (frecuencias por bin real vs. nulo)

Parámetros:
  --bin-km    (ancho de bin en km, default 1)
  --tol-bins  (tolerancia en bins, default 1)
  --seed      (semilla aleatoria)
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd
from random import Random

IN_PATH = Path("data/processed/od_pairs.parquet")
OUT_DIR = Path("data/processed")
OUT_PARQUET = OUT_DIR / "od_pairs_null.parquet"
OUT_SUMMARY = OUT_DIR / "null_model_summary.csv"

# ---------- Utilidades de distancia ----------
def haversine_km_vec(lat1, lon1, lat2, lon2):
    """
    Distancia Haversine vectorizada en km (lat/lon en grados).
    Acepta escalares o arrays; usa broadcasting NumPy.
    """
    R = 6371.0088
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2.0)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2.0)**2
    return 2 * R * np.arcsin(np.sqrt(a))

def indice_bin(dist_km, bins):
    """Ubica la distancia en un bin 0-based; intervalos (a, b]."""
    return np.searchsorted(bins, dist_km, side="right") - 1

# ---------- Core del modelo nulo ----------
def posibles_destinos_por_origen(df, bins, bin_real, tol_bins=1, allow_keep=True):
    """
    Para cada origen i, retorna índices j de destinos compatibles por bin ± tol_bins.
    df debe tener columnas lat_origen/lon_origen/lat_destino/lon_destino.
    """
    n = len(df)
    latO = df["lat_origen"].to_numpy()
    lonO = df["lon_origen"].to_numpy()
    latD = df["lat_destino"].to_numpy()
    lonD = df["lon_destino"].to_numpy()

    destinos_por_origen = {}
    for i in range(n):
        # distancia del origen i a TODOS los destinos j (vectorizada)
        dist_i = haversine_km_vec(latO[i], lonO[i], latD, lonD)
        bins_j = np.searchsorted(bins, dist_i, side="right") - 1

        mask = np.abs(bins_j - bin_real[i]) <= tol_bins
        if not allow_keep:
            mask &= (np.arange(n) != i)  # evita quedarse con su mismo destino

        destinos_por_origen[i] = np.where(mask)[0].tolist()

    return destinos_por_origen

def recableo_matching(df, ancho_bin_km=1, tol_bins=1, allow_keep=True, seed=None):
    """
    Retorna:
      df_rec       : DataFrame con nuevos destinos y distancia_km recalculada
      emparejados  : cantidad de orígenes con destino asignado
      sin_posibles : lista de índices i sin destino compatible disponible
      summary      : DataFrame con comparación de distribución de distancias (real vs nulo)
    """
    rng = Random(seed)

    # Asegurar columna distancia_km (real)
    if "distancia_km" not in df.columns:
        df = df.copy()
        df["distancia_km"] = haversine_km_vec(
            df["lat_origen"].to_numpy(), df["lon_origen"].to_numpy(),
            df["lat_destino"].to_numpy(), df["lon_destino"].to_numpy()
        )

    # Construcción de bins
    max_d = float(np.nanmax(df["distancia_km"].to_numpy()))
    if max_d == 0:
        max_d = 0.5
    bins = np.arange(0, max_d + ancho_bin_km, ancho_bin_km)

    # Bin real de cada trayecto
    bin_real = df["distancia_km"].apply(lambda d: indice_bin(d, bins)).to_numpy()

    # Pre-cálculo de candidatos por origen
    destinos_por_origen = posibles_destinos_por_origen(
        df, bins=bins, bin_real=bin_real, tol_bins=tol_bins, allow_keep=allow_keep
    )

    # Conjuntos para matching 1–a–1
    disponibles_dest = set(range(len(df)))
    pendientes = list(range(len(df)))
    rng.shuffle(pendientes)

    nuevo_latD = df["lat_destino"].copy()
    nuevo_lonD = df["lon_destino"].copy()
    sin_posibles = []

    while pendientes:
        i = pendientes.pop()
        candidatos = [j for j in destinos_por_origen[i] if j in disponibles_dest]
        if not candidatos:
            sin_posibles.append(i)
            continue
        j = rng.choice(candidatos)
        nuevo_latD.iat[i] = df.at[j, "lat_destino"]
        nuevo_lonD.iat[i] = df.at[j, "lon_destino"]
        disponibles_dest.remove(j)

    # Crear df resultante
    df_rec = df.copy()
    df_rec["lat_destino"] = nuevo_latD
    df_rec["lon_destino"] = nuevo_lonD

    # Recalcular distancias para el nulo
    df_rec["distancia_km"] = haversine_km_vec(
        df_rec["lat_origen"].to_numpy(), df_rec["lon_origen"].to_numpy(),
        df_rec["lat_destino"].to_numpy(), df_rec["lon_destino"].to_numpy()
    )

    emparejados = len(df) - len(sin_posibles)

    # Comparación de distribuciones (frecuencia por bin)
    real_bins = pd.cut(df["distancia_km"], bins=bins, include_lowest=True)
    nulo_bins = pd.cut(df_rec["distancia_km"], bins=bins, include_lowest=True)
    freq_real = real_bins.value_counts().sort_index()
    freq_nulo = nulo_bins.value_counts().sort_index()

    prob_real = (freq_real / max(freq_real.sum(), 1)).rename("prob_real")
    prob_nulo = (freq_nulo / max(freq_nulo.sum(), 1)).rename("prob_nulo")
    summary = pd.concat([prob_real, prob_nulo], axis=1)
    summary.index.name = "bin"

    return df_rec, emparejados, sin_posibles, summary

# ---------- Main CLI ----------
def main():
    parser = argparse.ArgumentParser(description="Crea un modelo nulo recableando destinos preservando distancia.")
    parser.add_argument("--bin-km", type=float, default=1.0, help="Ancho de bin en km (default: 1 km).")
    parser.add_argument("--tol-bins", type=int, default=1, help="Tolerancia en bins (default: 1).")
    parser.add_argument("--seed", type=int, default=123, help="Semilla aleatoria (default: 123).")
    parser.add_argument("--allow-keep", action="store_true",
                        help="Permite que un origen conserve su mismo destino si cae dentro del bin (off por default).")
    args = parser.parse_args()

    if not IN_PATH.exists():
        raise FileNotFoundError(f"No se encontró {IN_PATH}. Corré antes scripts/20_build_od.py")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(IN_PATH)

    # Validaciones mínimas de columnas
    needed = {"lat_origen", "lon_origen", "lat_destino", "lon_destino"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas en OD: {missing}")

    df_null, emp, sin_pos, summary = recableo_matching(
        df,
        ancho_bin_km=args.bin_km,
        tol_bins=args.tol_bins,
        allow_keep=args.allow_keep,
        seed=args.seed,
    )

    df_null.to_parquet(OUT_PARQUET, index=False)
    summary.to_csv(OUT_SUMMARY)

    print(f"✔ Modelo nulo generado: {len(df_null):,} filas")
    print(f"   Emparejados: {emp} | Sin posibles: {len(sin_pos)}")
    print(f"   - {OUT_PARQUET}")
    print(f"   - {OUT_SUMMARY}")

if __name__ == "__main__":
    main()

