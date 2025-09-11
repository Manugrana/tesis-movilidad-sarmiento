#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Genera rutas más cortas entre pares OD usando OSMnx/NetworkX
y detecta si cruzan la traza del ferrocarril Sarmiento.
Entradas:
  - data/processed/od_pairs.parquet
  - data/external/trenes_caba.geojson (o trenes_amba_unificados.geojson)
Salidas:
  - data/processed/routes_osmnx.pkl
  - data/processed/routes_osmnx.geojson
"""

import os
import sys
import pandas as pd
import geopandas as gpd
import networkx as nx
import osmnx as ox
from shapely.geometry import LineString
from tqdm import tqdm
from pathlib import Path

OD_PATH = Path("data/processed/od_pairs.parquet")
SARMIENTO_PATH = Path("data/external/trenes_caba.geojson")
OUT_PKL = Path("data/processed/routes_osmnx.pkl")
OUT_GEOJSON = Path("data/processed/routes_osmnx.geojson")


def load_sarmiento(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"No se encontró {path}. Corré scripts/01_download_shapes.sh")

    gdf = gpd.read_file(path)

    # Filtro flexible: Linea que contenga 'sarmiento'
    mask = gdf["Linea"].str.contains("sarmiento", case=False, na=False)
    gdf_sarm = gdf[mask].copy()

    if gdf_sarm.empty:
        print("[Aviso] No se encontró 'Sarmiento' en el dataset. Usando todas las líneas como fallback.")
        traza = gdf.unary_union
    else:
        traza = gdf_sarm.unary_union

    return traza


def main():
    if not OD_PATH.exists():
        print(f"[ERROR] No existe {OD_PATH}. Corré scripts/20_build_od.py primero.", file=sys.stderr)
        sys.exit(1)

    df_od = pd.read_parquet(OD_PATH)
    print(f"→ Pares OD cargados: {len(df_od):,}")

    # Cargar traza del Sarmiento
    traza_sarmiento = load_sarmiento(SARMIENTO_PATH)

    # Calcular centro geográfico promedio
    centro_lat = df_od[["lat_origen", "lat_destino"]].stack().mean()
    centro_lon = df_od[["lon_origen", "lon_destino"]].stack().mean()

    print("→ Descargando grafo de OSM (esto puede tardar)…")
    G = ox.graph_from_point(
        (centro_lat, centro_lon),
        dist=12000,             # radio en metros
        network_type="drive",   # solo red vial
        simplify=True
    )

    rutas = []
    for _, row in tqdm(df_od.iterrows(), total=len(df_od)):
        origen = (row["lat_origen"], row["lon_origen"])
        destino = (row["lat_destino"], row["lon_destino"])

        try:
            nodo_origen = ox.nearest_nodes(G, X=origen[1], Y=origen[0])
            nodo_destino = ox.nearest_nodes(G, X=destino[1], Y=destino[0])

            camino = nx.shortest_path(G, nodo_origen, nodo_destino, weight="length")
            ruta_geom = LineString([(G.nodes[n]["x"], G.nodes[n]["y"]) for n in camino])

            cruza = ruta_geom.crosses(traza_sarmiento)

            rutas.append({
                "id_tarjeta": row["id_tarjeta"],
                "hora_origen": row["hora_origen"],
                "hora_destino": row["hora_destino"],
                "lat_origen": row["lat_origen"],
                "lon_origen": row["lon_origen"],
                "lat_destino": row["lat_destino"],
                "lon_destino": row["lon_destino"],
                "ruta": ruta_geom,
                "cruza_sarmiento": cruza,
            })

        except Exception as e:
            print(f"[Aviso] Error con tarjeta {row['id_tarjeta']}: {e}")
            continue

        if len(rutas) % 100 == 0 and len(rutas) > 0:
            pd.DataFrame(rutas).to_pickle("data/processed/routes_backup.pkl")
            print(f"Backup parcial guardado ({len(rutas)} rutas).")

    # Guardar resultados finales
    df_rutas = pd.DataFrame(rutas)
    df_rutas.to_pickle(OUT_PKL)

    gdf_rutas = gpd.GeoDataFrame(df_rutas, geometry="ruta", crs="EPSG:4326")
    gdf_rutas.to_file(OUT_GEOJSON, driver="GeoJSON")

    print(f"✔ Rutas guardadas: {len(df_rutas):,}")
    print(f"   - {OUT_PKL}")
    print(f"   - {OUT_GEOJSON}")


if __name__ == "__main__":
    main()
