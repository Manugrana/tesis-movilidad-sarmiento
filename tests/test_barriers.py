from src.barriers import barrier_score

def test_barrier_score_simple():
    assert abs(barrier_score(8, 10) - 0.2) < 1e-9
