"""Database helpers for gridpoint/station mappings and temps."""

import logging
from typing import Dict, List, Tuple

from db.conn import db_conn
from settings import Settings

log = logging.getLogger("weatherapp-ml")

# Load mapping of gridpoints to their primary NOAA station
def load_gridpoint_primary_map(settings: Settings) -> List[Tuple[str, float, float, str]]:
    """Return gridpoints mapped to their primary NOAA station."""
    sql = """
        SELECT g.grid_id, ST_Y(g.geom) AS lat, ST_X(g.geom) AS lon, m.station_id
        FROM geo_gridpoint g
        JOIN gridpoint_station_map m ON m.grid_id = g.grid_id AND m.is_primary = true
        WHERE g.geom IS NOT NULL
    """

    rows: List[Tuple[str, float, float, str]] = []
    with db_conn(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            for grid_id, lat, lon, station_id in cur.fetchall():
                rows.append((grid_id, float(lat), float(lon), station_id))
    return rows

# Load current temperature by station from latest hourly forecast
def load_current_nws_temp_by_station(settings: Settings) -> Dict[str, float]:
    """Return current NWS temperature by station ID."""
    sql = """
        SELECT m.station_id, f.temperature_c
        FROM gridpoint_station_map m
        JOIN v_latest_hourly_forecast f ON f.grid_id = m.grid_id
        WHERE m.is_primary = true
          AND f.start_time <= now()
          AND f.end_time > now()
          AND f.temperature_c IS NOT NULL
    """

    # Return mapping of station_id to temperature in Celsius
    out: Dict[str, float] = {}
    with db_conn(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            for station_id, temp_c in cur.fetchall():
                out[station_id] = float(temp_c)
    return out


def load_gridpoint_station_neighbors(
    settings: Settings,
    *,
    max_km: float = 5.0,
    max_stations: int = 4,
) -> Dict[str, List[Tuple[str, float]]]:
    """Return nearest stations for each gridpoint with distances in km."""
    sql = """
        SELECT grid_id, station_id, distance_m
        FROM gridpoint_station_map
        WHERE distance_m IS NOT NULL
          AND distance_m <= %s
        ORDER BY grid_id, distance_m ASC
    """

    neighbors: Dict[str, List[Tuple[str, float]]] = {}
    with db_conn(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (max_km * 1000.0,))
            for grid_id, station_id, distance_m in cur.fetchall():
                items = neighbors.setdefault(grid_id, [])
                if len(items) >= max_stations:
                    continue
                if station_id:
                    items.append((station_id, float(distance_m) / 1000.0))
    return neighbors
