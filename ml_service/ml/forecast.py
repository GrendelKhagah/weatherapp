"""Multi-model daily forecast logic for station time series."""

import datetime as dt
import logging
from typing import Optional

import numpy as np

from ml.models import GprForecastModel, StationSeries, ridge_models
from settings import Settings
from ml.models.features import rolling_means

log = logging.getLogger("weatherapp-ml")


def _base_confidence(series: StationSeries, base_date: dt.date, current_temp_c: Optional[float]) -> float:
    """Compute a simple confidence score based on data recency."""
    days_since_truth = max(0, (base_date - series.last_date).days)
    confidence = max(0.1, 1.0 - (days_since_truth / 30.0))
    if current_temp_c is not None and np.isfinite(current_temp_c):
        confidence = max(0.1, min(1.0, confidence + 0.1))
    return confidence


def predict_station_forecast(
    *,
    station_rows,
    current_temp_c=None,
    as_of_date: Optional[dt.date] = None,
    max_points: int = 120,
    settings: Optional[Settings] = None,
):
    """Generate an 11-day forecast using GPR and ridge ensemble models."""
    station_rows = list(station_rows or [])
    series = StationSeries.from_rows(station_rows, min_rows=30)
    if not series:
        return None

    base_date = as_of_date or dt.date.today()
    _, last7_mean = rolling_means(series.tmean_c, window=7)

    confidence = _base_confidence(series, base_date, current_temp_c)
    horizon_days = settings.forecast_days if settings is not None else 11

    preds = []
    gpr_enabled = settings.gpr_enabled if settings is not None else True
    gpr_max_points = settings.gpr_max_points if settings is not None else max_points
    gpr_restarts = settings.gpr_restarts if settings is not None else 2
    if gpr_enabled:
        gpr = GprForecastModel(max_points=gpr_max_points, restarts=gpr_restarts).fit(series)
        preds.extend(
            gpr.predict(
                base_date=base_date,
                horizon_days=horizon_days,
                current_temp_c=current_temp_c,
                last7_mean=last7_mean,
                confidence_base=confidence,
            )
        )

    for ridge in ridge_models():
        ridge.fit(series)
        preds.extend(
            ridge.predict(
                base_date=base_date,
                horizon_days=horizon_days,
                current_temp_c=current_temp_c,
                confidence_base=confidence,
            )
        )

    return preds
