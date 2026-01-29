"""
WeatherApp ML Service - FastAPI-based machine learning weather prediction service.

Features:
- Ridge regression model trained on NOAA historical data with seasonal encoding
- 10-day weather forecasts for all stations with recent data
- Automatic gridpoint forecast propagation from primary stations
- NWS current temperature bias adjustment
- Confidence scoring based on data freshness
"""

import os
import threading
import datetime as dt
import logging
from typing import Optional, Dict, List, Tuple, Any

import numpy as np
import psycopg2
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from sklearn.linear_model import Ridge
from sklearn.neighbors import KNeighborsRegressor
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI(title="WeatherApp ML", version="0.2.0")

logging.basicConfig(
    level=os.getenv("ML_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
log = logging.getLogger("weatherapp-ml")

# CORS (enable frontend + API domain)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://api.ketterling.space",
        "https://map.ketterling.space",
        "https://ketterling.space",
        "http://localhost",
        "http://localhost:5500",
        "http://127.0.0.1",
        "http://127.0.0.1:5500",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============ CACHES ============
MODEL_CACHE: Dict[tuple, Any] = {}
MODEL_LOCK = threading.Lock()
FORECAST_MODEL_CACHE: Dict[tuple, Any] = {}
FORECAST_MODEL_LOCK = threading.Lock()
LATEST_DATE_CACHE: Dict[str, Any] = {"date": None, "ts": None}
LATEST_DATE_LOCK = threading.Lock()

# ============ REQUEST LOGGING MIDDLEWARE ============
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = dt.datetime.utcnow()
    try:
        response = await call_next(request)
        return response
    finally:
        dur_ms = int((dt.datetime.utcnow() - start).total_seconds() * 1000)
        status = None
        try:
            status = response.status_code
        except Exception:
            status = "?"
        log.info("%s %s -> %s (%d ms)", request.method, request.url.path, status, dur_ms)


# ============ DATABASE CONNECTION ============
def _db_conn():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return psycopg2.connect(db_url)

    host = os.getenv("DB_HOST", "127.0.0.1")
    port = int(os.getenv("DB_PORT", "5432"))
    name = os.getenv("DB_NAME", "weatherdb")
    user = os.getenv("DB_USER", "weatherapp")
    password = os.getenv("DB_PASSWORD", "")

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=name,
        user=user,
        password=password,
    )


def _latlon_valid(lat: float, lon: float) -> bool:
    try:
        return -90.0 <= float(lat) <= 90.0 and -180.0 <= float(lon) <= 180.0
    except Exception:
        return False


# ============ DATABASE SCHEMA SETUP ============
def _ensure_tables():
    sql = """
        CREATE TABLE IF NOT EXISTS tracked_point (
            id BIGSERIAL PRIMARY KEY,
            name TEXT,
            lat DOUBLE PRECISION NOT NULL,
            lon DOUBLE PRECISION NOT NULL,
            created_at TIMESTAMPTZ DEFAULT now(),
            UNIQUE(lat, lon)
        );

        CREATE TABLE IF NOT EXISTS ml_weather_prediction (
            id BIGSERIAL PRIMARY KEY,
            source_type TEXT NOT NULL,
            source_id TEXT,
            lat DOUBLE PRECISION NOT NULL,
            lon DOUBLE PRECISION NOT NULL,
            as_of_date DATE NOT NULL,
            horizon_hours INT,
            tmean_c DOUBLE PRECISION,
            prcp_mm DOUBLE PRECISION,
            model_name TEXT,
            created_at TIMESTAMPTZ DEFAULT now(),
            UNIQUE (source_type, source_id, lat, lon, as_of_date, horizon_hours)
        );
    """

    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()
    log.info("Ensured ML tables exist")

    # Add optional columns for expanded forecasts
    alter_sql = """
        ALTER TABLE ml_weather_prediction ADD COLUMN IF NOT EXISTS tmin_c DOUBLE PRECISION;
        ALTER TABLE ml_weather_prediction ADD COLUMN IF NOT EXISTS tmax_c DOUBLE PRECISION;
        ALTER TABLE ml_weather_prediction ADD COLUMN IF NOT EXISTS delta_c DOUBLE PRECISION;
        ALTER TABLE ml_weather_prediction ADD COLUMN IF NOT EXISTS model_detail TEXT;
        ALTER TABLE ml_weather_prediction ADD COLUMN IF NOT EXISTS confidence REAL;
    """
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(alter_sql)
            conn.commit()
    log.info("Ensured ML forecast columns exist")


# ============ DATE HELPERS ============
def _latest_noaa_date() -> Optional[dt.date]:
    with LATEST_DATE_LOCK:
        cached = LATEST_DATE_CACHE.get("date")
        ts = LATEST_DATE_CACHE.get("ts")
        if cached and ts and (dt.datetime.utcnow() - ts).total_seconds() < 600:
            return cached

    sql = "SELECT MAX(date) FROM noaa_daily_summary"
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            latest = row[0] if row else None

    if latest:
        with LATEST_DATE_LOCK:
            LATEST_DATE_CACHE["date"] = latest
            LATEST_DATE_CACHE["ts"] = dt.datetime.utcnow()
        return latest
    return None


def _default_date() -> dt.date:
    latest = _latest_noaa_date()
    if latest:
        return latest
    return dt.date.today() - dt.timedelta(days=1)


def _as_of_window(years: int = 2) -> dt.date:
    return dt.date.today() - dt.timedelta(days=365 * years)


# ============ TRAINING DATA LOADERS ============
def _load_training_rows(as_of: dt.date):
    """Load training data for a specific date (KNN model)."""
    sql = """
        SELECT ST_Y(s.geom) AS lat,
               ST_X(s.geom) AS lon,
               d.tmax_c,
               d.tmin_c,
               d.prcp_mm
        FROM noaa_station s
        JOIN noaa_daily_summary d ON d.station_id = s.station_id
        WHERE s.geom IS NOT NULL
          AND d.date = %s
    """

    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (as_of,))
            rows = cur.fetchall()

    X = []
    y = []
    for lat, lon, tmax, tmin, prcp in rows:
        if lat is None or lon is None:
            continue
        if tmax is None or tmin is None:
            continue
        tmean = (float(tmax) + float(tmin)) / 2.0
        prcp_mm = float(prcp) if prcp is not None else 0.0
        X.append([float(lat), float(lon)])
        y.append([tmean, prcp_mm])

    if not X:
        log.warning("No training rows found for date=%s", as_of.isoformat())
        return None, None

    return np.array(X), np.array(y)


def _load_training_rows_window(start_date: dt.date) -> Tuple[Optional[Dict], int]:
    """Load all NOAA history from start_date for Ridge model training."""
    sql = """
        SELECT station_id, date, tmax_c, tmin_c, prcp_mm
        FROM noaa_daily_summary
        WHERE date >= %s
        ORDER BY station_id, date
    """

    rows_by_station: Dict[str, List] = {}
    total_rows = 0
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (start_date,))
            for station_id, date, tmax, tmin, prcp in cur.fetchall():
                rows_by_station.setdefault(station_id, []).append((date, tmax, tmin, prcp))
                total_rows += 1

    if not rows_by_station:
        log.warning("No training rows found for window starting %s", start_date.isoformat())
        return None, 0

    return rows_by_station, total_rows


def _build_forecast_dataset(rows_by_station: Dict[str, List]) -> Tuple[Optional[np.ndarray], Optional[np.ndarray]]:
    """
    Build feature matrix for Ridge regression model.
    
    Features:
    - sin(day_of_year): Captures seasonal wave pattern
    - cos(day_of_year): Captures seasonal wave pattern (orthogonal)
    - last 7-day mean temperature: Recent temperature baseline
    - last 7-day mean precipitation: Recent precipitation pattern
    - temperature trend (last 7 days): Direction of change
    - horizon (0-9): Days into future
    
    Targets:
    - tmin_c, tmax_c, prcp_mm for target day
    """
    X = []
    y = []
    
    for station_id, rows in rows_by_station.items():
        if len(rows) < 20:
            continue
            
        # rows are (date, tmax, tmin, prcp) sorted by date
        dates = [r[0] for r in rows]
        tmaxs = [r[1] for r in rows]
        tmins = [r[2] for r in rows]
        prcps = [r[3] if r[3] is not None else 0.0 for r in rows]

        # Compute mean temperatures
        tmeans = []
        for tmax, tmin in zip(tmaxs, tmins):
            if tmax is None or tmin is None:
                tmeans.append(None)
            else:
                tmeans.append((float(tmax) + float(tmin)) / 2.0)

        # Build training samples: use last 7 days to predict next 1-10 days
        for i in range(7, len(rows) - 10):
            last7 = tmeans[i - 7:i]
            if any(v is None for v in last7):
                continue
            last7_prcp = prcps[i - 7:i]
            last7_mean = float(np.mean(last7))
            last7_prcp_mean = float(np.mean(last7_prcp))
            trend = float(last7[-1] - last7[0])

            # Generate samples for horizon 0-9 (today through 9 days ahead)
            for h in range(0, 10):
                idx = i + h
                if idx >= len(rows):
                    break
                tmax_t = tmaxs[idx]
                tmin_t = tmins[idx]
                if tmax_t is None or tmin_t is None:
                    continue
                prcp_t = prcps[idx]
                target_date = dates[idx]
                
                # Seasonal encoding using sin/cos for cyclic pattern
                doy = target_date.timetuple().tm_yday
                angle = 2.0 * np.pi * (doy / 365.25)
                sin_doy = float(np.sin(angle))
                cos_doy = float(np.cos(angle))
                
                X.append([sin_doy, cos_doy, last7_mean, last7_prcp_mean, trend, float(h)])
                y.append([float(tmin_t), float(tmax_t), float(prcp_t)])

    if not X:
        return None, None
    return np.array(X), np.array(y)


# ============ MODEL TRAINING ============
def _train_model(as_of: dt.date, neighbors: int):
    """Train KNN model for a specific date."""
    X, y = _load_training_rows(as_of)
    if X is None or y is None:
        return None

    n = min(neighbors, len(X))
    model = KNeighborsRegressor(n_neighbors=max(n, 1), weights="distance")
    model.fit(X, y)

    trained = {
        "date": as_of,
        "neighbors": model.n_neighbors,
        "model": model,
        "rows": len(X),
    }
    log.info("Trained model date=%s neighbors=%s rows=%s", as_of.isoformat(), model.n_neighbors, len(X))
    return trained


def _get_or_train(as_of: dt.date, neighbors: int):
    """Get or train KNN model."""
    key = (as_of.isoformat(), neighbors)
    with MODEL_LOCK:
        cached = MODEL_CACHE.get(key)
        if cached:
            log.debug("Using cached model date=%s neighbors=%s", as_of.isoformat(), neighbors)
            return cached

        trained = _train_model(as_of, neighbors)
        if not trained:
            return None

        MODEL_CACHE[key] = trained
        return trained


def _get_or_train_forecast(years: int = 2):
    """Get or train Ridge regression forecast model."""
    key = ("forecast", years)
    with FORECAST_MODEL_LOCK:
        cached = FORECAST_MODEL_CACHE.get(key)
        if cached:
            log.debug("Using cached forecast model years=%s", years)
            return cached

        start_date = _as_of_window(years)
        rows_by_station, total_rows = _load_training_rows_window(start_date)
        if not rows_by_station:
            return None

        X, y = _build_forecast_dataset(rows_by_station)
        if X is None or y is None:
            log.warning("No forecast training samples after feature build")
            return None

        # Use Ridge regression for stability with multicollinear features
        model = Ridge(alpha=1.0)
        model.fit(X, y)

        trained = {
            "model": model,
            "years": years,
            "rows": len(X),
            "total_history_rows": total_rows,
            "start_date": start_date,
            "model_name": "ridge-seasonal-v2",
            "model_detail": "ridge-seasonal doy_sin_cos+7day_mean+trend h=0-9",
        }
        FORECAST_MODEL_CACHE[key] = trained
        log.info("Trained forecast model years=%s samples=%s history_rows=%s", years, len(X), total_rows)
        return trained


# ============ PREDICTION STORAGE ============
def _store_prediction(
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
):
    """Store a prediction in the database."""
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

    with _db_conn() as conn:
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
            log.debug("Stored prediction source_type=%s source_id=%s lat=%.4f lon=%.4f date=%s horizon=%s",
                      source_type, source_id, lat, lon, as_of.isoformat(), horizon_hours)
            return row[0] if row else None


# ============ DATA LOADERS FOR BOOTSTRAP ============
def _load_station_coords() -> Dict[str, Tuple[float, float]]:
    """Load all station coordinates."""
    sql = """
        SELECT station_id, ST_Y(geom) AS lat, ST_X(geom) AS lon
        FROM noaa_station
        WHERE geom IS NOT NULL
    """
    coords = {}
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            for station_id, lat, lon in cur.fetchall():
                coords[station_id] = (float(lat), float(lon))
    return coords


def _load_gridpoint_primary_map() -> List[Tuple[str, float, float, str]]:
    """Load gridpoint to primary station mappings."""
    sql = """
        SELECT g.grid_id, ST_Y(g.geom) AS lat, ST_X(g.geom) AS lon, m.station_id
        FROM geo_gridpoint g
        JOIN gridpoint_station_map m ON m.grid_id = g.grid_id AND m.is_primary = true
        WHERE g.geom IS NOT NULL
    """
    rows = []
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            for grid_id, lat, lon, station_id in cur.fetchall():
                rows.append((grid_id, float(lat), float(lon), station_id))
    return rows


def _load_current_nws_temp_by_station() -> Dict[str, float]:
    """Load current NWS temperature for stations via gridpoint mapping."""
    sql = """
        SELECT m.station_id, f.temperature_c
        FROM gridpoint_station_map m
        JOIN v_latest_hourly_forecast f ON f.grid_id = m.grid_id
        WHERE m.is_primary = true
          AND f.start_time <= now()
          AND f.end_time > now()
          AND f.temperature_c IS NOT NULL
    """
    out = {}
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            for station_id, temp_c in cur.fetchall():
                out[station_id] = float(temp_c)
    return out


def _load_recent_station_history(days: int = 14) -> Dict[str, List]:
    """
    Load recent history for stations (last N days).
    Returns dict keyed by station_id with list of (date, tmax, tmin, prcp).
    """
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
    rows_by_station: Dict[str, List] = {}
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (days,))
            for station_id, date, tmax, tmin, prcp in cur.fetchall():
                rows_by_station.setdefault(station_id, []).append((date, tmax, tmin, prcp))
    return rows_by_station


def _load_stations_with_recent_data(days: int = 14) -> set:
    """Get set of station_ids that have data within the last N days."""
    cutoff = dt.date.today() - dt.timedelta(days=days)
    sql = """
        SELECT DISTINCT station_id
        FROM noaa_daily_summary
        WHERE date >= %s
    """
    station_ids = set()
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (cutoff,))
            for (sid,) in cur.fetchall():
                station_ids.add(sid)
    return station_ids


def _normalize_station_id(station_id: str) -> str:
    """Normalize station_id by removing common prefixes."""
    if station_id.startswith("GHCND:"):
        return station_id[6:]
    return station_id


# ============ FORECAST PREDICTION ============
def _predict_station_forecast(
    model,
    station_rows: List,
    model_name: str,
    model_detail: str,
    current_temp_c: Optional[float] = None,
    as_of_date: Optional[dt.date] = None
) -> Optional[List[Dict]]:
    """
    Generate 10-day forecast for a station.
    
    Args:
        model: Trained Ridge model
        station_rows: List of (date, tmax, tmin, prcp) tuples
        model_name: Model name string
        model_detail: Model detail string
        current_temp_c: Current NWS temperature for bias adjustment
        as_of_date: Base date for forecast
        
    Returns:
        List of prediction dicts for days 0-9
    """
    if len(station_rows) < 7:
        return None
        
    # Sort by date
    station_rows = sorted(station_rows, key=lambda r: r[0])
    dates = [r[0] for r in station_rows]
    tmaxs = [r[1] for r in station_rows]
    tmins = [r[2] for r in station_rows]
    prcps = [r[3] if r[3] is not None else 0.0 for r in station_rows]

    # Compute means
    tmeans = []
    for tmax, tmin in zip(tmaxs, tmins):
        if tmax is None or tmin is None:
            tmeans.append(None)
        else:
            tmeans.append((float(tmax) + float(tmin)) / 2.0)

    # Need at least 7 valid days for feature computation
    last7 = tmeans[-7:]
    if any(v is None for v in last7):
        return None
        
    last7_prcp = prcps[-7:]
    last7_mean = float(np.mean(last7))
    last7_prcp_mean = float(np.mean(last7_prcp))
    trend = float(last7[-1] - last7[0])
    last_date = dates[-1]
    base_date = as_of_date or dt.date.today()

    # Compute NWS bias (adjust predictions toward current observed temperature)
    bias = 0.0
    if current_temp_c is not None and np.isfinite(current_temp_c):
        # Blend bias: 60% weight on NWS current temp difference
        bias = (float(current_temp_c) - last7_mean) * 0.6

    # Confidence based on data freshness
    days_since_truth = max(0, (base_date - last_date).days)
    confidence = max(0.1, 1.0 - (days_since_truth / 30.0))
    if current_temp_c is not None and np.isfinite(current_temp_c):
        confidence = max(0.1, min(1.0, confidence + 0.1))

    preds = []
    for h in range(0, 10):
        target_date = base_date + dt.timedelta(days=h)
        doy = target_date.timetuple().tm_yday
        angle = 2.0 * np.pi * (doy / 365.25)
        sin_doy = float(np.sin(angle))
        cos_doy = float(np.cos(angle))
        
        feat = np.array([[sin_doy, cos_doy, last7_mean, last7_prcp_mean, trend, float(h)]])
        pred = model.predict(feat)[0]
        
        tmin_pred = float(pred[0])
        tmax_pred = float(pred[1])
        prcp_pred = max(0.0, float(pred[2]))  # Precipitation can't be negative
        
        # Ensure tmin <= tmax
        if tmin_pred > tmax_pred:
            tmin_pred, tmax_pred = tmax_pred, tmin_pred
            
        # Apply NWS bias
        tmin_pred = tmin_pred + bias
        tmax_pred = tmax_pred + bias
        tmean_pred = (tmin_pred + tmax_pred) / 2.0
        delta_pred = tmax_pred - tmin_pred
        
        preds.append({
            "date": target_date,
            "as_of": base_date,
            "horizon_hours": h * 24,
            "tmin_c": tmin_pred,
            "tmax_c": tmax_pred,
            "tmean_c": tmean_pred,
            "prcp_mm": prcp_pred,
            "delta_c": delta_pred,
            "model_name": model_name,
            "model_detail": model_detail,
            "confidence": confidence,
        })
        
    return preds


def _station_forecast_exists(source_type: str, source_id: str, as_of_date: dt.date, days: int) -> bool:
    """Check if forecast already exists for this source/date."""
    sql = """
        SELECT COUNT(*)
        FROM ml_weather_prediction
        WHERE source_type = %s
          AND source_id = %s
          AND as_of_date = %s
          AND horizon_hours BETWEEN 0 AND ((%s - 1) * 24)
    """
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (source_type, source_id, as_of_date, days))
            row = cur.fetchone()
            return row and row[0] >= days


# ============ BOOTSTRAP FORECASTS ============
def _bootstrap_forecasts():
    """
    Bootstrap ML forecasts for all stations with recent data and propagate to gridpoints.
    
    This runs on startup to ensure all stations have 10-day forecasts available.
    """
    try:
        log.info("Bootstrapping ML forecasts for stations and gridpoints")
        
        # Train the forecast model
        trained = _get_or_train_forecast(years=2)
        if not trained:
            log.warning("Forecast bootstrap skipped: no training data")
            return

        model = trained["model"]
        model_name = trained["model_name"]
        model_detail = trained["model_detail"]
        today = dt.date.today()

        # Load station coordinates and recent history
        station_coords = _load_station_coords()
        recent_by_station = _load_recent_station_history(days=14)
        current_temp_by_station = _load_current_nws_temp_by_station()
        
        # Find stations with recent data (within 14 days)
        stations_with_recent = _load_stations_with_recent_data(days=14)
        
        log.info("Bootstrap data loaded: stations=%d recent_history=%d with_recent=%d nws_temps=%d",
                 len(station_coords), len(recent_by_station), len(stations_with_recent), len(current_temp_by_station))

        # Generate forecasts for stations with recent data
        station_preds: Dict[str, List[Dict]] = {}
        stored_station = 0
        skipped_station = 0
        
        for station_id, rows in recent_by_station.items():
            # Skip if station doesn't have recent data
            if station_id not in stations_with_recent:
                continue
                
            coords = station_coords.get(station_id)
            if not coords:
                continue
                
            # Check if forecast already exists for today
            if _station_forecast_exists("station", station_id, today, 10):
                skipped_station += 1
                # Still load existing predictions for gridpoint propagation
                station_preds[station_id] = []  # Marker that we skipped
                continue
                
            current_temp = current_temp_by_station.get(station_id)
            
            # Also try normalized station_id for NWS temp lookup
            if current_temp is None:
                norm_id = _normalize_station_id(station_id)
                for k, v in current_temp_by_station.items():
                    if _normalize_station_id(k) == norm_id:
                        current_temp = v
                        break
            
            preds = _predict_station_forecast(
                model,
                rows,
                model_name,
                model_detail,
                current_temp_c=current_temp,
                as_of_date=today,
            )
            
            if not preds:
                continue
                
            station_preds[station_id] = preds
            lat, lon = coords
            
            for p in preds:
                _store_prediction(
                    "station",
                    station_id,
                    lat,
                    lon,
                    p["as_of"],
                    p["horizon_hours"],
                    p["tmean_c"],
                    p["prcp_mm"],
                    p["model_name"],
                    tmin_c=p["tmin_c"],
                    tmax_c=p["tmax_c"],
                    delta_c=p["delta_c"],
                    model_detail=p["model_detail"],
                    confidence=p["confidence"],
                )
                stored_station += 1

            if stored_station % 500 == 0 and stored_station > 0:
                log.info("Forecast bootstrap progress: station_preds=%d skipped=%d", stored_station, skipped_station)

        log.info("Station forecasts complete: stored=%d skipped=%d", stored_station, skipped_station)

        # Propagate to gridpoints
        grid_rows = _load_gridpoint_primary_map()
        stored_grid = 0
        skipped_grid = 0
        
        # Build lookup for normalized station IDs
        normalized_preds: Dict[str, List[Dict]] = {}
        for station_id, preds in station_preds.items():
            norm_id = _normalize_station_id(station_id)
            # Only add if we have actual predictions (not skipped markers)
            if preds:
                normalized_preds[norm_id] = preds
                normalized_preds[station_id] = preds  # Also keep original
        
        log.info("Gridpoint propagation: grid_rows=%d normalized_preds=%d", len(grid_rows), len(normalized_preds))
        
        for grid_id, lat, lon, station_id in grid_rows:
            # Try to find predictions for this station
            preds = normalized_preds.get(station_id)
            
            # Try normalized lookup if not found
            if not preds:
                norm_id = _normalize_station_id(station_id)
                preds = normalized_preds.get(norm_id)
                
            # Try with GHCND: prefix if not found
            if not preds and not station_id.startswith("GHCND:"):
                preds = normalized_preds.get(f"GHCND:{station_id}")
                
            if not preds:
                continue
                
            # Check if gridpoint forecast already exists
            if _station_forecast_exists("gridpoint", grid_id, today, 10):
                skipped_grid += 1
                continue
                
            for p in preds:
                _store_prediction(
                    "gridpoint",
                    grid_id,
                    lat,
                    lon,
                    p["as_of"],
                    p["horizon_hours"],
                    p["tmean_c"],
                    p["prcp_mm"],
                    p["model_name"],
                    tmin_c=p["tmin_c"],
                    tmax_c=p["tmax_c"],
                    delta_c=p["delta_c"],
                    model_detail=p["model_detail"],
                    confidence=p["confidence"],
                )
                stored_grid += 1

        log.info("Forecast bootstrap complete: station_preds=%d grid_preds=%d station_skipped=%d grid_skipped=%d",
                 stored_station, stored_grid, skipped_station, skipped_grid)
                 
    except Exception:
        log.exception("Forecast bootstrap failed")


# ============ PYDANTIC MODELS ============
class PredictRequest(BaseModel):
    lat: float
    lon: float
    date: Optional[dt.date] = None
    neighbors: Optional[int] = None
    source_type: Optional[str] = None
    source_id: Optional[str] = None
    horizon_hours: Optional[int] = None
    model_name: Optional[str] = None


class PredictBatchRequest(BaseModel):
    date: Optional[dt.date] = None
    neighbors: Optional[int] = None
    limit: Optional[int] = None
    model_name: Optional[str] = None


# ============ API ENDPOINTS ============
@app.get("/health")
def health():
    try:
        with _db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"status": "ok", "db": "ok"}
    except Exception as exc:
        log.exception("Health check failed")
        return {"status": "degraded", "db": "fail", "error": str(exc)}


@app.on_event("startup")
def startup():
    _ensure_tables()
    log.info("ML service startup complete")
    thread = threading.Thread(target=_bootstrap_forecasts, daemon=True)
    thread.start()


@app.post("/train")
def train(date_str: Optional[str] = None, neighbors: int = 8):
    try:
        as_of = dt.date.fromisoformat(date_str) if date_str else _default_date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format")

    log.info("Train request date=%s neighbors=%s", as_of.isoformat(), neighbors)

    if neighbors < 1:
        neighbors = 1

    trained = _get_or_train(as_of, neighbors)
    if not trained:
        log.warning("Train failed: not enough data date=%s neighbors=%s", as_of.isoformat(), neighbors)
        raise HTTPException(status_code=503, detail="Not enough training data")

    return {
        "status": "trained",
        "date": trained["date"].isoformat(),
        "neighbors": trained["neighbors"],
        "rows": trained["rows"],
    }


@app.get("/predict")
def predict(lat: float, lon: float, date_str: Optional[str] = None, neighbors: int = 8):
    try:
        as_of = dt.date.fromisoformat(date_str) if date_str else _default_date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format")

    log.info("Predict request date=%s neighbors=%s", as_of.isoformat(), neighbors)

    if neighbors < 1:
        neighbors = 1

    trained = _get_or_train(as_of, neighbors)
    if not trained:
        log.warning("Predict failed: not enough data date=%s neighbors=%s", as_of.isoformat(), neighbors)
        raise HTTPException(status_code=503, detail="Not enough training data")

    model = trained["model"]
    pred = model.predict(np.array([[lat, lon]], dtype=float))[0]

    return {
        "date": trained["date"].isoformat(),
        "neighbors": trained["neighbors"],
        "tmean_c": float(pred[0]),
        "prcp_mm": float(pred[1]),
    }


@app.post("/predict")
def predict_post(req: PredictRequest):
    as_of = req.date or _default_date()
    neighbors = req.neighbors or 8
    if neighbors < 1:
        neighbors = 1

    log.info("Predict POST request date=%s neighbors=%s", as_of.isoformat(), neighbors)

    trained = _get_or_train(as_of, neighbors)
    if not trained:
        log.warning("Predict POST failed: not enough data date=%s neighbors=%s", as_of.isoformat(), neighbors)
        raise HTTPException(status_code=503, detail="Not enough training data")

    model = trained["model"]
    pred = model.predict(np.array([[req.lat, req.lon]], dtype=float))[0]

    result = {
        "date": trained["date"].isoformat(),
        "neighbors": trained["neighbors"],
        "tmean_c": float(pred[0]),
        "prcp_mm": float(pred[1]),
        "model_name": "knn-distance",
    }

    if req.source_type:
        model_name = req.model_name or "knn-distance"
        _store_prediction(
            req.source_type,
            req.source_id,
            float(req.lat),
            float(req.lon),
            as_of,
            req.horizon_hours,
            float(pred[0]),
            float(pred[1]),
            model_name,
        )

    return result


@app.post("/predict/store")
def predict_store(req: PredictRequest):
    if not req.source_type:
        raise HTTPException(status_code=400, detail="source_type required")

    as_of = req.date or _default_date()
    neighbors = req.neighbors or 8
    if neighbors < 1:
        neighbors = 1

    log.info("Predict store request date=%s neighbors=%s source_type=%s", as_of.isoformat(), neighbors, req.source_type)

    trained = _get_or_train(as_of, neighbors)
    if not trained:
        log.warning("Predict store failed: not enough data date=%s neighbors=%s", as_of.isoformat(), neighbors)
        raise HTTPException(status_code=503, detail="Not enough training data")

    model = trained["model"]
    pred = model.predict(np.array([[req.lat, req.lon]], dtype=float))[0]
    model_name = req.model_name or "knn-distance"

    pred_id = _store_prediction(
        req.source_type,
        req.source_id,
        float(req.lat),
        float(req.lon),
        as_of,
        req.horizon_hours,
        float(pred[0]),
        float(pred[1]),
        model_name,
    )

    return {
        "id": pred_id,
        "date": trained["date"].isoformat(),
        "neighbors": trained["neighbors"],
        "tmean_c": float(pred[0]),
        "prcp_mm": float(pred[1]),
        "source_type": req.source_type,
        "source_id": req.source_id,
        "model_name": model_name,
    }


@app.post("/predict/tracked")
def predict_tracked(req: PredictBatchRequest):
    as_of = req.date or _default_date()
    neighbors = req.neighbors or 8
    if neighbors < 1:
        neighbors = 1
    limit = req.limit or 500
    model_name = req.model_name or "knn-distance"

    trained = _get_or_train(as_of, neighbors)
    if not trained:
        log.warning("Predict tracked failed: not enough data date=%s neighbors=%s", as_of.isoformat(), neighbors)
        raise HTTPException(status_code=503, detail="Not enough training data")

    model = trained["model"]
    sql = "SELECT id, name, lat, lon FROM tracked_point ORDER BY id LIMIT %s"
    stored = 0

    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            rows = cur.fetchall()

    for tid, name, lat, lon in rows:
        pred = model.predict(np.array([[lat, lon]], dtype=float))[0]
        _store_prediction(
            "tracked",
            str(tid),
            float(lat),
            float(lon),
            as_of,
            24,
            float(pred[0]),
            float(pred[1]),
            model_name,
        )
        stored += 1

    return {"status": "ok", "stored": stored}


@app.post("/predict/stations")
def predict_stations(req: PredictBatchRequest):
    as_of = req.date or _default_date()
    neighbors = req.neighbors or 8
    if neighbors < 1:
        neighbors = 1
    limit = req.limit or 1000
    model_name = req.model_name or "knn-distance"

    trained = _get_or_train(as_of, neighbors)
    if not trained:
        log.warning("Predict stations failed: not enough data date=%s neighbors=%s", as_of.isoformat(), neighbors)
        raise HTTPException(status_code=503, detail="Not enough training data")

    model = trained["model"]
    sql = "SELECT station_id, lat, lon FROM noaa_station WHERE geom IS NOT NULL ORDER BY station_id LIMIT %s"

    stored = 0
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            rows = cur.fetchall()

    for station_id, lat, lon in rows:
        pred = model.predict(np.array([[lat, lon]], dtype=float))[0]
        _store_prediction(
            "station",
            station_id,
            float(lat),
            float(lon),
            as_of,
            24,
            float(pred[0]),
            float(pred[1]),
            model_name,
        )
        stored += 1

    return {"status": "ok", "stored": stored}


@app.get("/predictions/latest")
def predictions_latest(source_type: str, source_id: Optional[str] = None, lat: Optional[float] = None, lon: Optional[float] = None):
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

    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (source_type, source_id, source_id, lat, lat, lon, lon))
            row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="No predictions")

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


@app.get("/weather/latest")
def weather_latest(sourceType: str, sourceId: Optional[str] = None, lat: Optional[float] = None, lon: Optional[float] = None):
    source_type = sourceType
    source_id = sourceId

    if not source_type:
        raise HTTPException(status_code=400, detail="source_type required")

    # If source_id is provided, avoid lat/lon filtering
    if source_id:
        lat = None
        lon = None

    if lat is not None and lon is not None and not _latlon_valid(lat, lon):
        raise HTTPException(status_code=400, detail="invalid lat/lon")

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

    eps = 0.001
    with _db_conn() as conn:
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

    if not row:
        raise HTTPException(status_code=404, detail="No predictions")

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


@app.get("/weather/forecast")
def weather_forecast(
    sourceType: str,
    sourceId: Optional[str] = None,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    days: int = 10,
):
    source_type = sourceType
    source_id = sourceId

    if not source_type:
        raise HTTPException(status_code=400, detail="source_type required")

    try:
        days = int(days)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid days")

    days = max(1, min(days, 10))

    # If source_id is provided, avoid lat/lon filtering
    if source_id:
        lat = None
        lon = None

    if lat is not None and lon is not None and not _latlon_valid(lat, lon):
        raise HTTPException(status_code=400, detail="invalid lat/lon")

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

    eps = 0.001
    out = []
    with _db_conn() as conn:
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
                out.append({
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
                })

    return out


@app.post("/forecast/generate")
def forecast_generate(station_id: Optional[str] = None, grid_id: Optional[str] = None, force: bool = False):
    """Manually trigger forecast generation for a station or gridpoint."""
    trained = _get_or_train_forecast(years=2)
    if not trained:
        raise HTTPException(status_code=503, detail="No forecast model available")

    model = trained["model"]
    model_name = trained["model_name"]
    model_detail = trained["model_detail"]
    today = dt.date.today()

    if station_id:
        # Generate for specific station
        station_coords = _load_station_coords()
        recent_by_station = _load_recent_station_history(days=14)
        current_temp_by_station = _load_current_nws_temp_by_station()

        if station_id not in recent_by_station:
            raise HTTPException(status_code=404, detail=f"No recent data for station {station_id}")

        coords = station_coords.get(station_id)
        if not coords:
            raise HTTPException(status_code=404, detail=f"Station coordinates not found for {station_id}")

        if not force and _station_forecast_exists("station", station_id, today, 10):
            return {"status": "skipped", "reason": "forecast already exists"}

        rows = recent_by_station[station_id]
        current_temp = current_temp_by_station.get(station_id)
        preds = _predict_station_forecast(model, rows, model_name, model_detail, current_temp, today)

        if not preds:
            raise HTTPException(status_code=500, detail="Failed to generate predictions")

        lat, lon = coords
        stored = 0
        for p in preds:
            _store_prediction(
                "station", station_id, lat, lon,
                p["as_of"], p["horizon_hours"], p["tmean_c"], p["prcp_mm"], p["model_name"],
                tmin_c=p["tmin_c"], tmax_c=p["tmax_c"], delta_c=p["delta_c"],
                model_detail=p["model_detail"], confidence=p["confidence"],
            )
            stored += 1

        return {"status": "ok", "stored": stored, "station_id": station_id}

    elif grid_id:
        # Generate for specific gridpoint (find primary station and propagate)
        grid_rows = _load_gridpoint_primary_map()
        grid_info = next((r for r in grid_rows if r[0] == grid_id), None)
        if not grid_info:
            raise HTTPException(status_code=404, detail=f"Gridpoint {grid_id} not found")

        _, lat, lon, station_id = grid_info
        recent_by_station = _load_recent_station_history(days=14)
        current_temp_by_station = _load_current_nws_temp_by_station()

        # Try to find station with normalized ID
        rows = recent_by_station.get(station_id)
        if not rows:
            norm_id = _normalize_station_id(station_id)
            for k, v in recent_by_station.items():
                if _normalize_station_id(k) == norm_id:
                    rows = v
                    break
        if not rows and not station_id.startswith("GHCND:"):
            rows = recent_by_station.get(f"GHCND:{station_id}")

        if not rows:
            raise HTTPException(status_code=404, detail=f"No recent data for primary station {station_id}")

        if not force and _station_forecast_exists("gridpoint", grid_id, today, 10):
            return {"status": "skipped", "reason": "forecast already exists"}

        current_temp = current_temp_by_station.get(station_id)
        preds = _predict_station_forecast(model, rows, model_name, model_detail, current_temp, today)

        if not preds:
            raise HTTPException(status_code=500, detail="Failed to generate predictions")

        stored = 0
        for p in preds:
            _store_prediction(
                "gridpoint", grid_id, lat, lon,
                p["as_of"], p["horizon_hours"], p["tmean_c"], p["prcp_mm"], p["model_name"],
                tmin_c=p["tmin_c"], tmax_c=p["tmax_c"], delta_c=p["delta_c"],
                model_detail=p["model_detail"], confidence=p["confidence"],
            )
            stored += 1

        return {"status": "ok", "stored": stored, "grid_id": grid_id}

    else:
        raise HTTPException(status_code=400, detail="station_id or grid_id required")


@app.post("/forecast/bootstrap")
def forecast_bootstrap_trigger():
    """Manually trigger the bootstrap process."""
    thread = threading.Thread(target=_bootstrap_forecasts, daemon=True)
    thread.start()
    return {"status": "started", "message": "Bootstrap process started in background"}
