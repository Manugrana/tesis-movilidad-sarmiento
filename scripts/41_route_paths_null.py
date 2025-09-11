#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Genera rutas OSMnx para los pares OD recableados (modelo nulo) y chequea
cruces con la traza del ferrocarril Sarmiento.

Entradas:
  - data/processed/od_pairs_null.parquet   (de 40_create_null_model.py)
  - data/external/trenes_caba.geojson      (o trenes_amba_unificados.geojson)

Salidas:
  - data/processed/routes_null.pkl
  - data/processed/routes_null.geojson
"""

import sys
from pathlib import Path
import argparse
import pandas as pd
import geopandas as gpd
import networkx as nx
import osmnx as ox
from shapely.geometry import LineString
from tqdm import tqdm


IN_NULL = Path("data/processed/od_pairs_null.parquet")
SARMIENTO_PATH = Path("data/external/trenes_caba.geojson")
OUT_PKL = Path("data/processed/routes_null.pkl")
OUT_GEOJSON = Path("data/processed/routes_null.geojson")


def load_sarmiento(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"No se encontró {path}. Corré scripts/01_download_shapes.sh")

    gdf = gpd.read_file(path)
    # Filtro flexible por nombre de línea
    if "Linea" in gdf.columns:
        mask = gdf["Linea"].str.contains("sarmiento", case=False, na=False)
        gdf_sarm = gdf[mask].copy()
        traza = gdf_sarm.unary_union if not gdf_sarm.empty else gdf.unary_union
    else:
        # por si el proveedor no trae la columna esperada
        traza = gdf.unary_union

    return traza


def build_graph(df_od: pd.DataFrame, dist_m: int = 12000, network_type: str = "drive"):
    # Centroide simple a partir de OD (WGS84)
    centro_lat = df_od[["lat_origen", "lat_destino"]].stack().mean()
    centro_lon = df_od[["lon_origen", "lon_destino"]].stack().mean()

    G = ox.graph_from_point(
        (centro_lat, centro_lon),
        dist=dist_m,
        network_type=network_type,
        simplify=True
    )
    return G


def rutas_para_df(df: pd.DataFrame, G, traza_sarmiento):
    rutas = []
    for _, row in tqdm(df.iterrows(), total=len(df)):
        origen = (row["lat_origen"], row["lon_origen"])
        destino = (row["lat_destino"], row["lon_destino"])
        try:
            n_o = ox.nearest_nodes(G, X=origen[1], Y=origen[0])
            n_d = ox.nearest_nodes(G, X=destino[1], Y=destino[0])
            path = nx.shortest_path(G, n_o, n_d, weight="length")
            ruta_geom = LineString([(G.nodes[n]["x"], G.nodes[n]["y"]) for n in path])
            cruza = ruta_geom.crosses(traza_sarmiento)
            rutas.append({
                "id_tarjeta": row.get("id_tarjeta"),
                "hora_origen": row.get("hora_origen"),
                "hora_destino": row.get("hora_destino"),
                "lat_origen": row["lat_origen"],
                "lon_origen": row["lon_origen"],
                "lat_destino": row["lat_destino"],
                "lon_destino": row["lon_destino"],
                "distancia_km": row.get("distancia_km"),
                "ruta": ruta_geom,
                "cruza_sarmiento": cruza
            })
        except Exception as e:
            # Continuar ante casos sin nodo cercano o rutas imposibles
            print(f"[Aviso] Error con tarjeta {row.get('id_tarjeta')}: {e}")
            continue

        # Backup simple cada 100 rutas por si cortás
        if len(rutas) and len(rutas) % 100 == 0:
            pd.DataFrame(rutas).to_pickle("data/processed/routes_null_backup.pkl")

    return pd.DataFrame(rutas)


def main():
    parser = argparse.ArgumentParser(description="Ruteo OSMnx para el modelo nulo (cruce Sarmiento).")
    parser.add_argument("--in", dest="in_path", default=str(IN_NULL),
                        help="Ruta a od_pairs_null.parquet (default: data/processed/od_pairs_null.parquet)")
    parser.add_argument("--sarmiento", dest="sarmiento_path", default=str(SARMIENTO_PATH),
                        help="GeoJSON de ferrocarriles (default: data/external/trenes_caba.geojson)")
    parser.add_argument("--out-pkl", dest="out_pkl", default=str(OUT_PKL),
                        help="Salida pickle (default: data/processed/routes_null.pkl)")
    parser.add_argument("--out-geojson", dest="out_geojson", default=str(OUT_GEOJSON),
                        help="Salida GeoJSON (default: data/processed/routes_null.geojson)")
    parser.add_argument("--dist-m", type=int, default=12000,
                        help="Radio para grafo OSMnx desde el centro (m) (default: 12000)")
    parser.add_argument("--network", type=str, default="drive",
                        help="Tipo de red OSMnx (drive, walk, all, all_private) (default: drive)")
    args = parser.parse_args()

    in_path = Path(args.in_path)
    if not in_path.exists():
        print(f"[ERROR] No existe {in_path}. Corré antes 40_create_null_model.py", file=sys.stderr)
        sys.exit(1)

    df_null = pd.read_parquet(in_path)
    needed = {"lat_origen", "lon_origen", "lat_destino", "lon_destino"}
    missing = needed - set(df_null.columns)
    if missing:
        raise ValueError(f"Faltan columnas en od_pairs_null: {missing}")

    traza_sarmiento = load_sarmiento(Path(args.sarmiento_path))
    G = build_graph(df_null, dist_m=args.dist_m, network_type=args.network)

    df_rutas = rutas_para_df(df_null, G, traza_sarmiento)

    # Guardar
    Path(args.out_pkl).parent.mkdir(parents=True, exist_ok=True)
    df_rutas.to_pickle(args.out_pkl)

    gdf_rutas = gpd.GeoDataFrame(df_rutas, geometry="ruta", crs="EPSG:4326")
    gdf_rutas.to_file(args.out_geojson, driver="GeoJSON")

    print(f"✔ Rutas (modelo nulo) guardadas: {len(df_rutas):,}")
    print(f"   - {args.out_pkl}")
    print(f"   - {args.out_geojson}")


if __name__ == "__main__":
    main()
