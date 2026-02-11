"""Store and retrieve ML weather predictions in PostgreSQL."""

import datetime as dt
import logging
import time
from typing import Any, Dict, List, Optional

from db.conn import db_conn
from settings import Settings

# this module handles storing and retrieving weather predictions from the database

# Available logger for this module
log = logging.getLogger("weatherapp-ml")

# Store a weather prediction into the database
def store_prediction(
    settings: Settings,
    *,
    source_type: str,
    source_id: Optional[str],
    lat: float,
    lon: float,
    as_of: dt.date,
    horizon_hours: Optional[int],
    tmean_c: float,
    prcp_mm: float,
    model_name: str,
    tmin_c: Optional[float] = None,
    tmax_c: Optional[float] = None,
    delta_c: Optional[float] = None,
    model_detail: Optional[str] = None,
    confidence: Optional[float] = None,
) -> Optional[int]:
    """Insert or update a prediction row and return its ID."""
    sql = """
        INSERT INTO ml_weather_prediction
                    (source_type, source_id, lat, lon, as_of_date, horizon_hours, tmean_c, prcp_mm, model_name,
                     tmin_c, tmax_c, delta_c, model_detail, confidence)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_type, source_id, lat, lon, as_of_date, horizon_hours)
        DO UPDATE SET
          tmean_c = EXCLUDED.tmean_c,
          prcp_mm = EXCLUDED.prcp_mm,
          model_name = EXCLUDED.model_name,
          tmin_c = EXCLUDED.tmin_c,
          tmax_c = EXCLUDED.tmax_c,
          delta_c = EXCLUDED.delta_c,
          model_detail = EXCLUDED.model_detail,
          confidence = EXCLUDED.confidence,
          created_at = now()
        RETURNING id
    """
    # set t0 to current time for logging
    t0 = time.time()

    # Execute the insert/update, returning the id
    with db_conn(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    source_type,
                    source_id,
                    lat,
                    lon,
                    as_of,
                    horizon_hours,
                    tmean_c,
                    prcp_mm,
                    model_name,
                    tmin_c,
                    tmax_c,
                    delta_c,
                    model_detail,
                    confidence,
                ),
            )
            row = cur.fetchone()
            conn.commit()

    # Log the SQL execution time if logging is enabled
    if settings.log_sql:
        log.info(
            "SQL store_prediction source_type=%s source_id=%s horizon=%s (%.1f ms)",
            source_type,
            source_id,
            horizon_hours,
            (time.time() - t0) * 1000.0,
        )

    # Debug, log the stored prediction details
    log.debug(
        "Stored prediction source_type=%s source_id=%s lat=%.4f lon=%.4f date=%s horizon=%s",
        source_type,
        source_id,
        lat,
        lon,
        as_of.isoformat(),
        horizon_hours,
    )
    return row[0] if row else None

# Check if forecasts exist for a station for the given as_of_date and number of days
def station_forecast_exists(settings: Settings, source_type: str, source_id: str, as_of_date: dt.date, days: int) -> bool:
    """Check whether a full multi-day forecast exists for a station."""
    sql = """
        SELECT COUNT(*)
        FROM ml_weather_prediction
        WHERE source_type = %s
          AND source_id = %s
          AND as_of_date = %s
          AND horizon_hours BETWEEN 0 AND ((%s - 1) * 24)
    """
    with db_conn(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (source_type, source_id, as_of_date, days))
            row = cur.fetchone()
            return bool(row and row[0] >= days)


def forecast_exists(settings: Settings, source_type: str, source_id: str, as_of_date: dt.date, days: int) -> bool:
    """Check whether a full multi-day forecast exists for a source."""
    sql = """
        SELECT COUNT(*)
        FROM ml_weather_prediction
        WHERE source_type = %s
          AND source_id = %s
          AND as_of_date = %s
          AND horizon_hours BETWEEN 0 AND ((%s - 1) * 24)
    """
    with db_conn(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (source_type, source_id, as_of_date, days))
            row = cur.fetchone()
            return bool(row and row[0] >= days)


def any_forecast_exists(settings: Settings, source_type: str, as_of_date: dt.date) -> bool:
    """Return True if any forecast rows exist for a date and source type."""
    sql = """
        SELECT 1
        FROM ml_weather_prediction
        WHERE source_type = %s
          AND as_of_date = %s
          AND horizon_hours = 0
        LIMIT 1
    """
    with db_conn(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (source_type, as_of_date))
            return cur.fetchone() is not None


def load_forecast_rows(
    settings: Settings,
    *,
    source_type: str,
    source_id: str,
    as_of_date: dt.date,
    days: int = 10,
) -> List[Dict[str, Any]]:
    """Load a list of forecast rows for a source and date window."""
    sql = """
        SELECT as_of_date, horizon_hours, tmean_c, prcp_mm, tmin_c, tmax_c, delta_c,
               model_name, model_detail, confidence
        FROM ml_weather_prediction
        WHERE source_type = %s
          AND source_id = %s
          AND as_of_date = %s
          AND horizon_hours BETWEEN 0 AND ((%s - 1) * 24)
        ORDER BY horizon_hours ASC
    """

    out: List[Dict[str, Any]] = []
    with db_conn(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (source_type, source_id, as_of_date, days))
            for row in cur.fetchall():
                as_of = row[0]
                horizon = row[1] or 0
                target_date = (as_of + dt.timedelta(hours=horizon)) if as_of else None
                out.append(
                    {
                        "date": target_date,
                        "as_of": as_of,
                        "horizon_hours": horizon,
                        "tmean_c": row[2],
                        "prcp_mm": row[3],
                        "tmin_c": row[4],
                        "tmax_c": row[5],
                        "delta_c": row[6],
                        "model_name": row[7],
                        "model_detail": row[8],
                        "confidence": row[9],
                    }
                )
    return out

# Retrieve the latest prediction for a given source
def predictions_latest(
    settings: Settings,
    *,
    source_type: str,
    source_id: Optional[str] = None,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
) -> Optional[Dict[str, Any]]:
    """Return the most recently stored prediction matching the filters."""
    sql = """
        SELECT source_type, source_id, lat, lon, as_of_date, horizon_hours, tmean_c, prcp_mm, model_name, created_at
        FROM ml_weather_prediction
        WHERE source_type = %s
          AND (%s IS NULL OR source_id = %s)
          AND (%s IS NULL OR lat = %s)
          AND (%s IS NULL OR lon = %s)
        ORDER BY created_at DESC
        LIMIT 1
    """

# 

    t0 = time.time()
    with db_conn(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (source_type, source_id, source_id, lat, lat, lon, lon))
            row = cur.fetchone()

    if settings.log_sql:
        log.info(
            "SQL predictions_latest source_type=%s source_id=%s hit=%s (%.1f ms)",
            source_type,
            source_id,
            bool(row),
            (time.time() - t0) * 1000.0,
        )

    if not row:
        return None

    return {
        "source_type": row[0],
        "source_id": row[1],
        "lat": row[2],
        "lon": row[3],
        "as_of_date": row[4].isoformat() if row[4] else None,
        "horizon_hours": row[5],
        "tmean_c": row[6],
        "prcp_mm": row[7],
        "model_name": row[8],
        "created_at": row[9].isoformat() if row[9] else None,
    }


def weather_latest(
    settings: Settings,
    *,
    source_type: str,
    source_id: Optional[str] = None,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
) -> Optional[Dict[str, Any]]:
    """Return the most recent weather prediction for a source or location."""
    # If source_id is provided, avoid lat/lon filtering
    if source_id:
        lat = None
        lon = None

    sql = """
        SELECT source_type, source_id, lat, lon, as_of_date, horizon_hours,
               tmean_c, prcp_mm, tmin_c, tmax_c, delta_c, model_name, model_detail, confidence, created_at
        FROM ml_weather_prediction
        WHERE source_type = %s
          AND (%s IS NULL OR source_id = %s)
          AND (%s IS NULL OR lat BETWEEN %s AND %s)
          AND (%s IS NULL OR lon BETWEEN %s AND %s)
        ORDER BY created_at DESC
        LIMIT 1
    """

    eps = 0.01
    t0 = time.time()
    with db_conn(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    source_type,
                    source_id,
                    source_id,
                    lat,
                    None if lat is None else lat - eps,
                    None if lat is None else lat + eps,
                    lon,
                    None if lon is None else lon - eps,
                    None if lon is None else lon + eps,
                ),
            )
            row = cur.fetchone()

    if settings.log_sql:
        log.info(
            "SQL weather_latest source_type=%s source_id=%s hit=%s (%.1f ms)",
            source_type,
            source_id,
            bool(row),
            (time.time() - t0) * 1000.0,
        )

    if not row:
        return None

    return {
        "source_type": row[0],
        "source_id": row[1],
        "lat": row[2],
        "lon": row[3],
        "as_of_date": row[4].isoformat() if row[4] else None,
        "horizon_hours": row[5],
        "tmean_c": row[6],
        "prcp_mm": row[7],
        "tmin_c": row[8],
        "tmax_c": row[9],
        "delta_c": row[10],
        "model_name": row[11],
        "model_detail": row[12],
        "confidence": row[13],
        "created_at": row[14].isoformat() if row[14] else None,
    }


def weather_forecast(
    settings: Settings,
    *,
    source_type: str,
    source_id: Optional[str] = None,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    days: int = 10,
) -> List[Dict[str, Any]]:
    # If source_id is provided, avoid lat/lon filtering
    if source_id:
        lat = None
        lon = None

    sql = """
        SELECT as_of_date, horizon_hours, tmean_c, prcp_mm, tmin_c, tmax_c, delta_c,
               model_name, model_detail, confidence, created_at
        FROM ml_weather_prediction
        WHERE source_type = %s
          AND (%s IS NULL OR source_id = %s)
          AND (%s IS NULL OR lat BETWEEN %s AND %s)
          AND (%s IS NULL OR lon BETWEEN %s AND %s)
          AND horizon_hours BETWEEN 0 AND ((%s - 1) * 24)
        ORDER BY horizon_hours ASC
    """

    eps = 0.01
    out: List[Dict[str, Any]] = []
    t0 = time.time()
    with db_conn(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    source_type,
                    source_id,
                    source_id,
                    lat,
                    None if lat is None else lat - eps,
                    None if lat is None else lat + eps,
                    lon,
                    None if lon is None else lon - eps,
                    None if lon is None else lon + eps,
                    days,
                ),
            )
            for row in cur.fetchall():
                out.append(
                    {
                        "as_of_date": row[0].isoformat() if row[0] else None,
                        "horizon_hours": row[1],
                        "tmean_c": row[2],
                        "prcp_mm": row[3],
                        "tmin_c": row[4],
                        "tmax_c": row[5],
                        "delta_c": row[6],
                        "model_name": row[7],
                        "model_detail": row[8],
                        "confidence": row[9],
                        "created_at": row[10].isoformat() if row[10] else None,
                    }
                )

    if settings.log_sql:
        log.info(
            "SQL weather_forecast source_type=%s source_id=%s days=%s rows=%d (%.1f ms)",
            source_type,
            source_id,
            days,
            len(out),
            (time.time() - t0) * 1000.0,
        )

    return out


def accuracy_metrics(
    settings: Settings,
    *,
    source_type: str,
    start_date: dt.date,
    model_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Compute MAE/RMSE for predictions against NOAA daily actuals."""
    sql = """
        WITH preds AS (
            SELECT source_id,
                   as_of_date,
                   horizon_hours,
                   tmean_c,
                   prcp_mm,
                   model_name
            FROM ml_weather_prediction
            WHERE source_type = %s
              AND as_of_date >= %s
              AND horizon_hours IS NOT NULL
              AND (horizon_hours %% 24) = 0
              AND (%s IS NULL OR model_name = %s)
        )
        SELECT
            COUNT(*) AS n,
            AVG(ABS(p.tmean_c - ((d.tmax_c + d.tmin_c) / 2.0))) AS mae_tmean,
            SQRT(AVG(POWER(p.tmean_c - ((d.tmax_c + d.tmin_c) / 2.0), 2))) AS rmse_tmean,
            AVG(ABS(p.prcp_mm - COALESCE(d.prcp_mm, 0.0))) AS mae_prcp,
            SQRT(AVG(POWER(p.prcp_mm - COALESCE(d.prcp_mm, 0.0), 2))) AS rmse_prcp
        FROM preds p
        JOIN noaa_daily_summary d
          ON d.station_id = p.source_id
         AND d.date = (p.as_of_date + (p.horizon_hours / 24) * INTERVAL '1 day')
        WHERE p.tmean_c IS NOT NULL
    """

    t0 = time.time()
    with db_conn(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (source_type, start_date, model_name, model_name))
            row = cur.fetchone()

    if settings.log_sql:
        log.info(
            "SQL accuracy_metrics source_type=%s model=%s (%.1f ms)",
            source_type,
            model_name,
            (time.time() - t0) * 1000.0,
        )

    if not row:
        return {
            "count": 0,
            "mae_tmean": None,
            "rmse_tmean": None,
            "mae_prcp": None,
            "rmse_prcp": None,
        }

    return {
        "count": int(row[0] or 0),
        "mae_tmean": row[1],
        "rmse_tmean": row[2],
        "mae_prcp": row[3],
        "rmse_prcp": row[4],
    }
