"""Read NOAA-related training data from the database."""

import datetime as dt
import logging
import threading
import time
from typing import Dict, Optional, Tuple

import numpy as np

from db.conn import db_conn
from settings import Settings

# Available logger for this module
log = logging.getLogger("weatherapp-ml")

# Cache for the latest NOAA date to reduce database queries
_LATEST_DATE_CACHE = {"date": None, "ts": None}
_LATEST_DATE_LOCK = threading.Lock()

# Get the latest date for which NOAA data is available
def latest_noaa_date(settings: Settings) -> Optional[dt.date]:
    """Return the most recent date in noaa_daily_summary (cached for 10 minutes)."""
    with _LATEST_DATE_LOCK:
        cached = _LATEST_DATE_CACHE.get("date")
        ts = _LATEST_DATE_CACHE.get("ts")
        if cached and ts and (dt.datetime.utcnow() - ts).total_seconds() < 600:
            return cached

    sql = "SELECT MAX(date) FROM noaa_daily_summary"
    t0 = time.time()
    with db_conn(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            latest = row[0] if row else None

    if settings.log_sql:
        log.info("SQL latest_noaa_date hit=%s (%.1f ms)", bool(latest), (time.time() - t0) * 1000.0)

    if latest:
        with _LATEST_DATE_LOCK:
            _LATEST_DATE_CACHE["date"] = latest
            _LATEST_DATE_CACHE["ts"] = dt.datetime.utcnow()
        return latest
    return None

# Load training rows for a specific day from NOAA daily summary
def load_training_rows_for_day(settings: Settings, as_of: dt.date):
    """Load feature/target arrays for a single day of NOAA data."""
    sql = """
        SELECT ST_Y(s.geom) AS lat,
               ST_X(s.geom) AS lon,
                             s.elevation_m,
                             d.tmax_c,
                             d.tmin_c,
                             d.prcp_mm
        FROM noaa_station s
        JOIN noaa_daily_summary d ON d.station_id = s.station_id
        WHERE s.geom IS NOT NULL
          AND d.date = %s
    """

    t0 = time.time()
    with db_conn(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (as_of,))
            rows = cur.fetchall()

    if settings.log_sql:
        log.info("SQL load_training_rows_for_day date=%s rows=%d (%.1f ms)", as_of.isoformat(), len(rows), (time.time() - t0) * 1000.0)

    X = []
    y = []
    for lat, lon, elevation_m, tmax, tmin, prcp in rows:
        if lat is None or lon is None:
            continue
        if tmax is None or tmin is None:
            continue
        tmean = (float(tmax) + float(tmin)) / 2.0
        prcp_mm = float(prcp) if prcp is not None else 0.0
        elev = float(elevation_m) if elevation_m is not None else 0.0
        X.append([float(lat), float(lon), abs(float(lat)), elev])
        y.append([tmean, prcp_mm])

    # if the X array is empty log warning and return None
    if not X:
        log.warning("No training rows found for date=%s", as_of.isoformat())
        return None, None

    return np.array(X), np.array(y)


def load_nearest_station_elevation(settings: Settings, lat: float, lon: float) -> float:
    """Find the closest station elevation to a point."""
    sql = """
        SELECT elevation_m
        FROM noaa_station
        WHERE geom IS NOT NULL
        ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)
        LIMIT 1
    """
    with db_conn(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (lon, lat))
            row = cur.fetchone()
    if row and row[0] is not None:
        try:
            return float(row[0])
        except Exception:
            return 0.0
    return 0.0


# Load training rows for a date window from NOAA daily summary
def load_training_rows_window(settings: Settings, start_date: dt.date):
    """Load station history rows for dates on/after a start date."""
    sql = """
        SELECT station_id, date, tmax_c, tmin_c, prcp_mm
        FROM noaa_daily_summary
        WHERE date >= %s
        ORDER BY station_id, date
    """

    rows_by_station: Dict[str, list] = {}
    with db_conn(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (start_date,))
            for station_id, date, tmax, tmin, prcp in cur.fetchall():
                rows_by_station.setdefault(station_id, []).append((date, tmax, tmin, prcp))

    if not rows_by_station:
        log.warning("No training rows found for window starting %s", start_date.isoformat())
        return None

    return rows_by_station


# Load recent station history for the past 'days' days
def load_recent_station_history(settings: Settings, days: int = 7):
    """Load the most recent N days of history per station."""
    sql = """
        SELECT station_id, date, tmax_c, tmin_c, prcp_mm
        FROM (
            SELECT station_id, date, tmax_c, tmin_c, prcp_mm,
                   ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY date DESC) AS rn
            FROM noaa_daily_summary
        ) x
        WHERE rn <= %s
        ORDER BY station_id, date
    """

    rows_by_station: Dict[str, list] = {}
    with db_conn(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (days,))
            for station_id, date, tmax, tmin, prcp in cur.fetchall():
                rows_by_station.setdefault(station_id, []).append((date, tmax, tmin, prcp))
    return rows_by_station

# Load station coordinates from noaa_station table   Tuple[float, float] is the type hint for (lat, lon)
def load_station_coords(settings: Settings) -> Dict[str, Tuple[float, float]]:
    """Return a map of station_id -> (lat, lon)."""
    sql = """
        SELECT station_id, ST_Y(geom) AS lat, ST_X(geom) AS lon
        FROM noaa_station
        WHERE geom IS NOT NULL
    """

    coordinates: Dict[str, Tuple[float, float]] = {}
    with db_conn(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            for station_id, lat, lon in cur.fetchall():
                coordinates[station_id] = (float(lat), float(lon))
    return coordinates