from pathlib import Path
import pandas as pd
import geopandas as gpd

def read_csv(path, **kwargs):
    return pd.read_csv(path, **kwargs)

def to_parquet(df, path, **kwargs):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return df.to_parquet(path, **kwargs)

def read_geojson(path):
    return gpd.read_file(path)
