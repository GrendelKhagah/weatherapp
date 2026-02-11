"""FastAPI routes for ML training and prediction endpoints."""

import datetime as dt
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException

from api.deps import get_settings
from db.conn import db_conn
from db.noaa import latest_noaa_date
from db.predictions import accuracy_metrics, predictions_latest, store_prediction, weather_forecast, weather_latest
from ml.knn import get_or_train, predict_point
from models import PredictBatchRequest, PredictRequest
from settings import Settings
from utils import clamp_int, latlon_valid

log = logging.getLogger("weatherapp-ml")

# Create API router instance
router = APIRouter()

def default_date(settings: Settings) -> dt.date:
    """Return the latest NOAA date or fall back to yesterday."""
    latest = latest_noaa_date(settings)
    if latest:
        return latest
    return dt.date.today() - dt.timedelta(days=1)

# API Endpoints for ML Service using FastAPI

@router.get("/health")
def health(settings: Settings = Depends(get_settings)):
    """Simple health check that also validates DB connectivity."""
    # Simple health check endpoint, try connecting to the database or fail and log error
    try:
        with db_conn(settings) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"status": "ok", "db": "ok"}
    except Exception as exc:
        log.exception("Health check failed")
        return {"status": "degraded", "db": "fail", "error": str(exc)}


@router.post("/train")
# Train a KNN model for the specified date and number of neighbors
def train(date_str: Optional[str] = None, neighbors: int = 8, settings: Settings = Depends(get_settings)):
    """Train a KNN model for the given date and neighbor count."""
    try:
        as_of = dt.date.fromisoformat(date_str) if date_str else default_date(settings)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format")

    log.info("Train request date=%s neighbors=%s", as_of.isoformat(), neighbors)
    neighbors = max(1, int(neighbors))

    trained = get_or_train(settings, as_of, neighbors)
    if not trained:
        log.warning("Train failed: not enough data date=%s neighbors=%s", as_of.isoformat(), neighbors)
        raise HTTPException(status_code=503, detail="Not enough training data")

    return {
        "status": "trained",
        "date": trained["date"].isoformat(),
        "neighbors": trained["neighbors"],
        "rows": trained["rows"],
    }


@router.get("/predict")
def predict(lat: float, lon: float, date_str: Optional[str] = None, neighbors: int = 8, settings: Settings = Depends(get_settings)):
    """Predict mean temp and precipitation for a lat/lon point."""
    try:
        as_of = dt.date.fromisoformat(date_str) if date_str else default_date(settings)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format")

    log.info("Predict request date=%s neighbors=%s", as_of.isoformat(), neighbors)
    neighbors = max(1, int(neighbors))

    trained = get_or_train(settings, as_of, neighbors)
    if not trained:
        log.warning("Predict failed: not enough data date=%s neighbors=%s", as_of.isoformat(), neighbors)
        raise HTTPException(status_code=503, detail="Not enough training data")

    tmean_c, prcp_mm = predict_point(trained, settings, lat, lon)
    return {
        "date": trained["date"].isoformat(),
        "neighbors": trained["neighbors"],
        "tmean_c": tmean_c,
        "prcp_mm": prcp_mm,
    }


@router.post("/predict")
def predict_post(req: PredictRequest, settings: Settings = Depends(get_settings)):
    """Predict for a single request body, optionally storing the result."""
    as_of = req.date or default_date(settings)
    neighbors = max(1, int(req.neighbors or 8))

    log.info("Predict POST request date=%s neighbors=%s", as_of.isoformat(), neighbors)

    trained = get_or_train(settings, as_of, neighbors)
    if not trained:
        log.warning("Predict POST failed: not enough data date=%s neighbors=%s", as_of.isoformat(), neighbors)
        raise HTTPException(status_code=503, detail="Not enough training data")

    tmean_c, prcp_mm = predict_point(trained, settings, float(req.lat), float(req.lon))

    result = {
        "date": trained["date"].isoformat(),
        "neighbors": trained["neighbors"],
        "tmean_c": tmean_c,
        "prcp_mm": prcp_mm,
        "model_name": "knn-distance",
    }

    if req.source_type:
        model_name = req.model_name or "knn-distance"
        store_prediction(
            settings,
            source_type=req.source_type,
            source_id=req.source_id,
            lat=float(req.lat),
            lon=float(req.lon),
            as_of=as_of,
            horizon_hours=req.horizon_hours,
            tmean_c=tmean_c,
            prcp_mm=prcp_mm,
            model_name=model_name,
        )

    return result


@router.post("/predict/store")
def predict_store(req: PredictRequest, settings: Settings = Depends(get_settings)):
    """Predict and always store the result in the predictions table."""
    if not req.source_type:
        raise HTTPException(status_code=400, detail="source_type required")

    as_of = req.date or default_date(settings)
    neighbors = max(1, int(req.neighbors or 8))

    log.info(
        "Predict store request date=%s neighbors=%s source_type=%s",
        as_of.isoformat(),
        neighbors,
        req.source_type,
    )

    trained = get_or_train(settings, as_of, neighbors)
    if not trained:
        log.warning("Predict store failed: not enough data date=%s neighbors=%s", as_of.isoformat(), neighbors)
        raise HTTPException(status_code=503, detail="Not enough training data")

    tmean_c, prcp_mm = predict_point(trained, settings, float(req.lat), float(req.lon))
    model_name = req.model_name or "knn-distance"

    pred_id = store_prediction(
        settings,
        source_type=req.source_type,
        source_id=req.source_id,
        lat=float(req.lat),
        lon=float(req.lon),
        as_of=as_of,
        horizon_hours=req.horizon_hours,
        tmean_c=tmean_c,
        prcp_mm=prcp_mm,
        model_name=model_name,
    )

    return {
        "id": pred_id,
        "date": trained["date"].isoformat(),
        "neighbors": trained["neighbors"],
        "tmean_c": tmean_c,
        "prcp_mm": prcp_mm,
        "source_type": req.source_type,
        "source_id": req.source_id,
        "model_name": model_name,
    }


@router.post("/predict/tracked")
def predict_tracked(req: PredictBatchRequest, settings: Settings = Depends(get_settings)):
    """Predict for tracked points and store each result."""
    as_of = req.date or default_date(settings)
    neighbors = max(1, int(req.neighbors or 8))
    limit = int(req.limit or 500)
    model_name = req.model_name or "knn-distance"

    trained = get_or_train(settings, as_of, neighbors)
    if not trained:
        log.warning("Predict tracked failed: not enough data date=%s neighbors=%s", as_of.isoformat(), neighbors)
        raise HTTPException(status_code=503, detail="Not enough training data")

    sql = "SELECT id, name, lat, lon FROM tracked_point ORDER BY id LIMIT %s"

    with db_conn(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            rows = cur.fetchall()

    stored = 0
    for tid, _name, lat, lon in rows:
        tmean_c, prcp_mm = predict_point(trained, settings, float(lat), float(lon))
        store_prediction(
            settings,
            source_type="tracked",
            source_id=str(tid),
            lat=float(lat),
            lon=float(lon),
            as_of=as_of,
            horizon_hours=24,
            tmean_c=tmean_c,
            prcp_mm=prcp_mm,
            model_name=model_name,
        )
        stored += 1

    return {"status": "ok", "stored": stored}


@router.post("/predict/stations")
def predict_stations(req: PredictBatchRequest, settings: Settings = Depends(get_settings)):
    """Predict for NOAA stations and store each result."""
    as_of = req.date or default_date(settings)
    neighbors = max(1, int(req.neighbors or 8))
    limit = int(req.limit or 1000)
    model_name = req.model_name or "knn-distance"

    trained = get_or_train(settings, as_of, neighbors)
    if not trained:
        log.warning("Predict stations failed: not enough data date=%s neighbors=%s", as_of.isoformat(), neighbors)
        raise HTTPException(status_code=503, detail="Not enough training data")

    sql = "SELECT station_id, lat, lon, elevation_m FROM noaa_station WHERE geom IS NOT NULL ORDER BY station_id LIMIT %s"

    with db_conn(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            rows = cur.fetchall()

    stored = 0
    for station_id, lat, lon, elevation_m in rows:
        elev = float(elevation_m) if elevation_m is not None else None
        tmean_c, prcp_mm = predict_point(trained, settings, float(lat), float(lon), elev)
        store_prediction(
            settings,
            source_type="station",
            source_id=station_id,
            lat=float(lat),
            lon=float(lon),
            as_of=as_of,
            horizon_hours=24,
            tmean_c=tmean_c,
            prcp_mm=prcp_mm,
            model_name=model_name,
        )
        stored += 1

    return {"status": "ok", "stored": stored}


@router.get("/predictions/latest")
def get_predictions_latest(
    source_type: str,
    source_id: Optional[str] = None,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    settings: Settings = Depends(get_settings),
):
    """Return the latest stored prediction for a source or location."""
    row = predictions_latest(settings, source_type=source_type, source_id=source_id, lat=lat, lon=lon)
    if not row:
        raise HTTPException(status_code=404, detail="No predictions")
    return row


@router.get("/weather/latest")
def get_weather_latest(
    sourceType: str,
    sourceId: Optional[str] = None,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    settings: Settings = Depends(get_settings),
):
    """Return the latest weather row for a source or point."""
    if not sourceType:
        raise HTTPException(status_code=400, detail="source_type required")

    source_type = sourceType

    if lat is not None and lon is not None and not latlon_valid(lat, lon):
        raise HTTPException(status_code=400, detail="invalid lat/lon")

    if source_type == "point":
        row = weather_latest(settings, source_type="point", source_id=sourceId, lat=lat, lon=lon)
        if row:
            return row
        source_type = "gridpoint"

    row = weather_latest(settings, source_type=source_type, source_id=sourceId, lat=lat, lon=lon)
    if not row:
        raise HTTPException(status_code=404, detail="No predictions")
    return row


@router.get("/weather/forecast")
def get_weather_forecast(
    sourceType: str,
    sourceId: Optional[str] = None,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    days: int = 11,
    settings: Settings = Depends(get_settings),
):
    """Return a multi-day forecast for a source or location."""
    if not sourceType:
        raise HTTPException(status_code=400, detail="source_type required")

    source_type = sourceType
    if source_type == "point":
        source_type = "gridpoint"

    if lat is not None and lon is not None and not latlon_valid(lat, lon):
        raise HTTPException(status_code=400, detail="invalid lat/lon")

    days = clamp_int(days, 1, 11)
    return weather_forecast(settings, source_type=source_type, source_id=sourceId, lat=lat, lon=lon, days=days)


@router.get("/metrics/accuracy")
def get_accuracy_metrics(
    sourceType: str,
    days: int = 30,
    modelName: Optional[str] = None,
    settings: Settings = Depends(get_settings),
):
    """Return MAE/RMSE accuracy metrics against NOAA daily actuals."""
    if not sourceType:
        raise HTTPException(status_code=400, detail="source_type required")

    source_type = sourceType
    if source_type != "station":
        raise HTTPException(status_code=400, detail="accuracy metrics require source_type=station")

    days = clamp_int(days, 1, 365)
    latest = latest_noaa_date(settings)
    if not latest:
        raise HTTPException(status_code=404, detail="No NOAA data available")
    start_date = latest - dt.timedelta(days=days - 1)

    metrics = accuracy_metrics(
        settings,
        source_type=source_type,
        start_date=start_date,
        model_name=modelName,
    )

    return {
        "source_type": source_type,
        "model_name": modelName,
        "days": days,
        "start_date": start_date.isoformat(),
        "end_date": latest.isoformat(),
        **metrics,
    }

