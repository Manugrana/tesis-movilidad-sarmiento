import pandas as pd

def filter_primary_students(df: pd.DataFrame) -> pd.DataFrame:
    # id_tarifa == 11 → estudiantes de primario (según tus criterios)
    return df[df.get('id_tarifa') == 11].copy()
