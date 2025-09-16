#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Calcula Barrier Score global y direccional usando las funciones provistas por el usuario.

Entradas:
  --obs data/processed/routes_osmnx.pkl
  --null-glob "data/processed/routes_null*.pkl"
  --barreras data/external/trenes_caba.geojson

Salidas:
  data/processed/barrier_scores_global.json
  data/processed/barrier_scores_directional.json
"""

import argparse
import glob
import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiLineString


def direccion_geometrica(ruta):
    coords = list(ruta.coords)
    lat_inicio = coords[0][1]
    lat_fin = coords[-1][1]
    if lat_fin > lat_inicio:
        return "sur_norte"
    elif lat_fin < lat_inicio:
        return "norte_sur"
    else:
        return "horizontal"

def calcular_barrier_scores(df_real, dict_df_nulo, lista_de_barreras):
    resultados = {}
    for nombre_barrera, barrera in lista_de_barreras:
        print(f"\n📍 Barrier Score para '{nombre_barrera}'")

        nombre_columna = f"cruza_{nombre_barrera.lower().replace(' ', '_')}"
        df_real[nombre_columna] = df_real["ruta"].apply(lambda r: r.crosses(barrera))
        cruces_reales = df_real[nombre_columna].sum()

        df_cruza_real = df_real[df_real[nombre_columna] == True]
        cruces_sur_norte = (df_cruza_real["lat_destino"] > df_cruza_real["lat_origen"]).sum()
        cruces_norte_sur = (df_cruza_real["lat_destino"] < df_cruza_real["lat_origen"]).sum()

        print(f"→ Cruces modelo real: {cruces_reales} (Sur a norte: {cruces_sur_norte}, Norte a sur: {cruces_norte_sur})")

        resultados[nombre_barrera] = {
            "cruces_reales": int(cruces_reales),
            "norte_sur": int(cruces_norte_sur),
            "sur_norte": int(cruces_sur_norte),
            "modelos_nulos": {}
        }

        for nombre_modelo, df_nulo in dict_df_nulo.items():
            df_nulo[nombre_columna] = df_nulo["ruta"].apply(lambda r: r.crosses(barrera))
            cruces_nulo = df_nulo[nombre_columna].sum()

            if cruces_nulo == 0:
                print(f"⚠️ {nombre_modelo}: Sin cruces en el modelo nulo.")
                resultados[nombre_barrera]["modelos_nulos"][nombre_modelo] = {
                    "cruces_nulo": 0,
                    "barrier_score": None
                }
                continue

            score = (cruces_nulo - cruces_reales) / cruces_reales
            print(f"→ {nombre_modelo}: {cruces_nulo} cruces → Barrier Score = {score:.3f}")

            resultados[nombre_barrera]["modelos_nulos"][nombre_modelo] = {
                "cruces_nulo": int(cruces_nulo),
                "barrier_score": round(score, 3)
            }

    return resultados

def calcular_barrier_scores_direccion(df_real, dict_df_nulo, lista_de_barreras):
    resultados = {}
    for nombre_barrera, barrera in lista_de_barreras:
        print(f"\n📍 Barrier Score para '{nombre_barrera}' (según dirección geométrica)")

        nombre_columna = f"cruza_{nombre_barrera.lower().replace(' ', '_')}"
        df_real[nombre_columna] = df_real["ruta"].apply(lambda r: r.crosses(barrera))
        df_cruza_real = df_real[df_real[nombre_columna]].copy()
        df_cruza_real["direccion"] = df_cruza_real["ruta"].apply(direccion_geometrica)

        cruces_sur_norte_real = df_cruza_real[df_cruza_real["direccion"] == "sur_norte"]
        cruces_norte_sur_real = df_cruza_real[df_cruza_real["direccion"] == "norte_sur"]

        print(f"→ Modelo real:")
        print(f"   • Sur → Norte: {len(cruces_sur_norte_real)}")
        print(f"   • Norte → Sur: {len(cruces_norte_sur_real)}")

        resultados[nombre_barrera] = {
            "cruces_reales": {
                "sur_norte": int(len(cruces_sur_norte_real)),
                "norte_sur": int(len(cruces_norte_sur_real)),
            },
            "modelos_nulos": {}
        }

        for nombre_modelo, df_nulo in dict_df_nulo.items():
            df_nulo[nombre_columna] = df_nulo["ruta"].apply(lambda r: r.crosses(barrera))
            df_cruza_nulo = df_nulo[df_nulo[nombre_columna]].copy()
            df_cruza_nulo["direccion"] = df_cruza_nulo["ruta"].apply(direccion_geometrica)

            cruces_sur_norte_nulo = df_cruza_nulo[df_cruza_nulo["direccion"] == "sur_norte"]
            cruces_norte_sur_nulo = df_cruza_nulo[df_cruza_nulo["direccion"] == "norte_sur"]

            n_sur_norte = len(cruces_sur_norte_nulo)
            n_norte_sur = len(cruces_norte_sur_nulo)

            bs_sur_norte = (n_sur_norte - len(cruces_sur_norte_real)) / len(cruces_sur_norte_real) if n_sur_norte > 0 else None
            bs_norte_sur = (n_norte_sur - len(cruces_norte_sur_real)) / len(cruces_norte_sur_real) if n_norte_sur > 0 else None

            print(f"→ {nombre_modelo}:")
            print(f"   • Sur → Norte: {n_sur_norte} → Barrier Score = {bs_sur_norte if bs_sur_norte is not None else '–'}")
            print(f"   • Norte → Sur: {n_norte_sur} → Barrier Score = {bs_norte_sur if bs_norte_sur is not None else '–'}")

            resultados[nombre_barrera]["modelos_nulos"][nombre_modelo] = {
                "sur_norte": {
                    "cruces_nulo": int(n_sur_norte),
                    "barrier_score": round(bs_sur_norte, 3) if bs_sur_norte is not None else None
                },
                "norte_sur": {
                    "cruces_nulo": int(n_norte_sur),
                    "barrier_score": round(bs_norte_sur, 3) if bs_norte_sur is not None else None
                }
            }

    return resultados

# -------------------------------------------------------------------
# Helpers de IO / barreras
# -------------------------------------------------------------------

def load_observed(path: Path) -> pd.DataFrame:
    return pd.read_pickle(path)

def load_nulls(glob_pat: str) -> dict[str, pd.DataFrame]:
    paths = sorted(glob.glob(glob_pat))
    if not paths:
        raise FileNotFoundError(f"No se encontraron nulos con patrón: {glob_pat}")
    return {Path(p).stem: pd.read_pickle(p) for p in paths}

def load_barreras(path: Path) -> list[tuple[str, LineString | MultiLineString]]:
    gdf = gpd.read_file(path)
    if "Linea" in gdf.columns:
        lista = []
        for name, sub in gdf.groupby("Linea"):
            lista.append((str(name), sub.unary_union))
        return lista
    else:
        return [(f"barrera_{i}", geom) for i, geom in enumerate(gdf.geometry)]

# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Compute BS global y direccional (todas las líneas).")
    ap.add_argument("--obs", default="data/processed/routes_osmnx.pkl", help="Pickle de rutas observadas")
    ap.add_argument("--null-glob", default="data/processed/routes_null*.pkl", help="Patrón de pickles de nulos")
    ap.add_argument("--barreras", default="data/external/trenes_caba.geojson", help="GeoJSON de ferrocarriles")
    ap.add_argument("--out-global", default="data/processed/barrier_scores_global.json", help="Salida JSON (BS global)")
    ap.add_argument("--out-dir", default="data/processed/barrier_scores_directional.json", help="Salida JSON (BS direccional)")
    args = ap.parse_args()

    obs = load_observed(Path(args.obs))
    nulls = load_nulls(args.null_glob)
    barreras = load_barreras(Path(args.barreras))

    res_global = calcular_barrier_scores(obs.copy(), {k: v.copy() for k, v in nulls.items()}, barreras)
    res_dir = calcular_barrier_scores_direccion(obs.copy(), {k: v.copy() for k, v in nulls.items()}, barreras)

    Path(args.out_global).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out_global, "w", encoding="utf-8") as f:
        json.dump(res_global, f, ensure_ascii=False, indent=2)

    with open(args.out_dir, "w", encoding="utf-8") as f:
        json.dump(res_dir, f, ensure_ascii=False, indent=2)

    print("✔ Listo.")
    print(f"  - Global     → {args.out_global}")
    print(f"  - Direccional→ {args.out_dir}")


if __name__ == "__main__":
    main()
