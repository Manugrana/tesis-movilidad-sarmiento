#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Limpieza de transacciones SUBE:
- Lee data/raw/transacciones.txt (descargado por scripts/00_download_data.sh)
- Valida filas con 13 columnas
- Asigna nombres de columnas
- Convierte tipos (numéricos, datetime)
- Filtra estudiantes primarios (id_tarifa == 11)
- Guarda data/interim/cleaned.parquet  (y opcional: data/interim/cleaned.csv)
"""

import os
import sys
import csv
import pandas as pd
from pathlib import Path

RAW_PATH = Path("data/raw/transacciones.txt")
TMP_PATH = Path("data/interim/transacciones_limpio.txt")
OUT_PARQUET = Path("data/interim/cleaned.parquet")
OUT_CSV = Path("data/interim/cleaned.csv")  # opcional

COLUMNS = [
    "id", "id_tarjeta", "modo", "lat", "lon", "sexo",
    "interno_bus", "tipo_trx_tren", "etapa_red_sube",
    "id_linea", "id_ramal", "id_tarifa", "hora"
]

def ensure_paths():
    TMP_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)

def filter_valid_rows():
    """Filtra filas con exactamente 13 campos (formato esperado)."""
    if not RAW_PATH.exists():
        print(f"[ERROR] No existe {RAW_PATH}. Corré primero: make data", file=sys.stderr)
        sys.exit(1)

    with RAW_PATH.open("r", encoding="utf-8") as fin, TMP_PATH.open("w", encoding="utf-8", newline="") as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)
        for row in reader:
            if len(row) == 13:
                writer.writerow(row)

def load_and_clean() -> pd.DataFrame:
    """Carga el txt ya filtrado, asigna columnas, castea tipos y filtra id_tarifa == 11."""
    # Cargar sin encabezado y asignar nombres
    df = pd.read_csv(
        TMP_PATH,
        header=None,
        names=COLUMNS,
        low_memory=False
    )

    # Tipos básicos
    # Nota: los originales suelen venir como texto; convertimos con coerción segura
    numeric_cols = ["lat", "lon", "hora"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Normalizar strings clave (modo, id_tarifa, etc.)
    df["id_tarifa"] = pd.to_numeric(df["id_tarifa"], errors="coerce").astype("Int64")
    df["etapa_red_sube"] = pd.to_numeric(df["etapa_red_sube"], errors="coerce").astype("Int64")

    # Filtrar estudiantes primarios (id_tarifa == 11)
    df = df[df["id_tarifa"] == 11].copy()

    # Validar mínimos: tarjeta, coordenadas válidas
    df = df.dropna(subset=["id_tarjeta", "lat", "lon"]).copy()

    # Rango razonable de lat/lon (CABA/AMBA aprox.)
    df = df[(df["lat"].between(-35.5, -34.0)) & (df["lon"].between(-59.5, -57.0))].copy()

    # Hora a datetime (HH → 00:00 del día ficticio)
    # Mantengo también 'hora' como int para operaciones rápidas
    df["hora"] = df["hora"].astype("Int64")
    df["hora_dt"] = pd.to_datetime(df["hora"].astype("float"), unit="h", origin=pd.Timestamp("1970-01-01"), errors="coerce")

    # Orden sugerido
    ordered_cols = [
        "id", "id_tarjeta", "modo", "lat", "lon", "sexo",
        "interno_bus", "tipo_trx_tren", "etapa_red_sube",
        "id_linea", "id_ramal", "id_tarifa", "hora", "hora_dt"
    ]
    df = df[[c for c in ordered_cols if c in df.columns]]

    return df

def main():
    ensure_paths()
    print("→ Filtrando filas válidas (13 columnas)…")
    filter_valid_rows()

    print("→ Cargando y limpiando…")
    df = load_and_clean()

    print(f"→ Guardando {OUT_PARQUET} …")
    df.to_parquet(OUT_PARQUET, index=False)

    # CSV opcional (útil para inspección rápida)
    print(f"→ Guardando {OUT_CSV} …")
    df.to_csv(OUT_CSV, index=False)

    print("✔ Listo.")
    print(f"Filas finales (id_tarifa=11, coords válidas): {len(df):,}")

if __name__ == "__main__":
    main()

