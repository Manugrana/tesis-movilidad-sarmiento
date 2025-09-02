from src.od_builder import infer_home_school_pairs
import pandas as pd

def test_infer_pairs_empty():
    df = pd.DataFrame()
    out = infer_home_school_pairs(df)
    assert set(out.columns) == {'id_tarjeta','home_lat','home_lon','school_lat','school_lon'}
