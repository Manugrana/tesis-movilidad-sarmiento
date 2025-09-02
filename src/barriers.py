import numpy as np
import pandas as pd

def barrier_score(observed:int, expected:float) -> float:
    # BS > 0 → menos cruces que los esperados (barrera más fuerte)
    if expected <= 0: 
        return np.nan
    return (expected - observed) / expected

def directional_bs(obs_ns:int, exp_ns:float, obs_sn:int, exp_sn:float) -> dict:
    return {
        'BS_NS': barrier_score(obs_ns, exp_ns),
        'BS_SN': barrier_score(obs_sn, exp_sn),
    }
