#!/usr/bin/env bash
set -euo pipefail

RAW_DIR="data/raw/shapes"
OUT_DIR="data/external"
mkdir -p "${RAW_DIR}" "${OUT_DIR}"

echo "==> Descargando líneas ferroviarias del AMBA y generando capas…"

python - << 'PYCODE'
import os, urllib.request
import geopandas as gpd
from shapely.ops import unary_union
import osmnx as ox

RAW_DIR = "data/raw/shapes"
OUT_DIR = "data/external"
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

# 1) Descargar ZIP oficial (Ministerio de Transporte)
url_trenes = "https://datos.transporte.gob.ar/dataset/f87b93d4-ade2-44fc-a409-d3736ba9f3ba/resource/87fb1c41-89b5-42db-b45a-992672931b2d/download/rmba-ferrocarril-lineas.zip"
zip_path = os.path.join(RAW_DIR, url_trenes.split("/")[-1])
if not os.path.exists(zip_path):
    print("Descargando:", url_trenes)
    urllib.request.urlretrieve(url_trenes, zip_path)
else:
    print("Ya existe:", zip_path)

# 2) Leer shapefile directamente del ZIP
d_trenes = gpd.read_file(f"zip://{zip_path}")
print("Registros leídos:", len(d_trenes))

# 3) Homologar columna de línea (LINEA / Linea / línea… → 'Linea')
candidatas = [c for c in d_trenes.columns if c.lower() in ("linea", "línea")]
if not candidatas:
    raise ValueError(f"No se encontró columna de línea en {list(d_trenes.columns)}")
d_trenes = d_trenes.rename(columns={candidatas[0]: "Linea"})
d_trenes["modo"] = "tren"

# 4) Asegurar geometrías válidas
d_trenes = d_trenes.set_geometry(d_trenes.geometry.buffer(0))

# 5) Unificar tramos por 'Linea'
gdf_unificadas = (
    d_trenes
    .groupby("Linea", dropna=False)["geometry"]
    .apply(lambda g: unary_union([geom for geom in g if geom is not None]))
    .reset_index()
)
gdf_unificadas = gpd.GeoDataFrame(gdf_unificadas, geometry="geometry", crs=d_trenes.crs)
gdf_unificadas["modo"] = "tren"

# 6) Guardar TODAS las líneas unificadas (AMBA)
full_out = os.path.join(OUT_DIR, "trenes_amba_unificados.geojson")
# reproyectamos a EPSG:4326 por interoperabilidad
gdf_unificadas.to_crs(epsg=4326).to_file(full_out, driver="GeoJSON")
print("Guardado:", full_out)

# 7) Obtener polígono de CABA (OSM)
nombre_caba = "Ciudad Autónoma de Buenos Aires, Argentina"
gdf_caba = ox.geocode_to_gdf(nombre_caba)
poligono_caba = gdf_caba.geometry.iloc[0]

# 8) Recortar a CABA
gdf_caba_clip = gdf_unificadas.copy()
gdf_caba_clip["geometry"] = gdf_caba_clip.geometry.intersection(poligono_caba)
gdf_caba_clip = gdf_caba_clip[~gdf_caba_clip.is_empty]

caba_out = os.path.join(OUT_DIR, "trenes_caba.geojson")
gdf_caba_clip.to_crs(epsg=4326).to_file(caba_out, driver="GeoJSON")
print("Guardado:", caba_out)
PYCODE

echo "==> Listo. Capas en data/external/: trenes_amba_unificados.geojson y trenes_caba.geojson"
