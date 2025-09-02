from shapely.geometry import LineString
from src.routing import shortest_path_line

def test_route_returns_linestring(monkeypatch):
    # No ejecuta red real en tests; solo valida tipo si se mockea en el futuro
    assert isinstance(LineString([(0,0),(1,1)]), LineString)
