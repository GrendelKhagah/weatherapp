"""KNN-based model training and prediction helpers."""

import datetime as dt
import logging
import os
import threading
from typing import Optional

import numpy as np
import joblib
from sklearn.neighbors import KNeighborsRegressor

from db.noaa import load_nearest_station_elevation, load_training_rows_for_day
from settings import Settings

log = logging.getLogger("weatherapp-ml")

_MODEL_CACHE = {}
_MODEL_LOCK = threading.Lock()


def _cache_path(settings: Settings, as_of: dt.date, neighbors: int) -> str:
    """Build a cache file path for a trained KNN model."""
    key = f"knn_{as_of.isoformat()}_{neighbors}.joblib"
    return os.path.join(settings.model_cache_dir, key)


def _load_cached_model(settings: Settings, as_of: dt.date, neighbors: int):
    """Load a cached model payload from disk if present."""
    path = _cache_path(settings, as_of, neighbors)
    if not os.path.exists(path):
        return None
    try:
        payload = joblib.load(path)
        if not payload or payload.get("date") != as_of or payload.get("neighbors") != neighbors:
            return None
        return payload
    except Exception:
        log.warning("Failed to load cached KNN model %s", path)
        return None


def _save_cached_model(settings: Settings, payload: dict) -> None:
    """Persist a trained model payload to disk for reuse."""
    try:
        os.makedirs(settings.model_cache_dir, exist_ok=True)
        path = _cache_path(settings, payload["date"], payload["neighbors"])
        joblib.dump(payload, path)
    except Exception as exc:
        log.warning("Failed to save cached KNN model path=%s err=%s", settings.model_cache_dir, exc)


def train_model(settings: Settings, as_of: dt.date, neighbors: int):
    """Train a KNN regressor on NOAA daily rows for one date."""
    X, y = load_training_rows_for_day(settings, as_of)
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
    log.info("Trained KNN model date=%s neighbors=%s rows=%s", as_of.isoformat(), model.n_neighbors, len(X))
    return trained


def get_or_train(settings: Settings, as_of: dt.date, neighbors: int):
    """Return a cached model or train a new one."""
    key = (as_of.isoformat(), neighbors)
    with _MODEL_LOCK:
        cached = _MODEL_CACHE.get(key)
        if cached:
            log.debug("Using cached KNN model date=%s neighbors=%s", as_of.isoformat(), neighbors)
            return cached

        disk_cached = _load_cached_model(settings, as_of, neighbors)
        if disk_cached:
            _MODEL_CACHE[key] = disk_cached
            log.info("Loaded cached KNN model date=%s neighbors=%s", as_of.isoformat(), neighbors)
            return disk_cached

        trained = train_model(settings, as_of, neighbors)
        if not trained:
            return None

        _MODEL_CACHE[key] = trained
        _save_cached_model(settings, trained)
        return trained


def predict_point(trained: dict, settings: Settings, lat: float, lon: float, elevation_m: Optional[float] = None):
    """Predict mean temperature and precipitation for a point."""
    model = trained["model"]
    elev = elevation_m
    if elev is None:
        elev = load_nearest_station_elevation(settings, lat, lon)
    features = np.array([[float(lat), float(lon), abs(float(lat)), float(elev)]], dtype=float)
    pred = model.predict(features)[0]
    return float(pred[0]), float(pred[1])
