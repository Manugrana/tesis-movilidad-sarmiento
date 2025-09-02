import networkx as nx
import osmnx as ox
from shapely.geometry import LineString, Point

def shortest_path_line(a_lat, a_lon, b_lat, b_lon, network='drive'):
    G = ox.graph_from_point((a_lat, a_lon), dist=5000, network_type=network)
    on = ox.nearest_nodes(G, a_lon, a_lat)
    dn = ox.nearest_nodes(G, b_lon, b_lat)
    route = nx.shortest_path(G, on, dn, weight='length')
    coords = [(G.nodes[n]['y'], G.nodes[n]['x']) for n in route]
    return LineString([(lng, lat) for lat, lng in coords])  # (x,y) = (lng,lat)
