"""Feature engineering helpers for ML models."""

import datetime as dt
from typing import List, Tuple

import numpy as np


def _doy(date: dt.date) -> int:
    """Return day-of-year for a date."""
    return int(date.timetuple().tm_yday)


def build_seasonal_features(dates: List[dt.date], base_date: dt.date) -> np.ndarray:
    """Build seasonal sine/cosine and trend features."""
    xs = []
    for d in dates:
        day_index = (d - base_date).days
        doy = _doy(d)
        sin_doy = np.sin(2 * np.pi * doy / 365.25)
        cos_doy = np.cos(2 * np.pi * doy / 365.25)
        trend = day_index / 365.25
        xs.append([sin_doy, cos_doy, trend])
    return np.array(xs, dtype=float)


def build_ridge_features(
    dates: List[dt.date],
    base_date: dt.date,
    last7_tmean: float,
    last7_prcp: float,
    include_prcp: bool = True,
) -> np.ndarray:
    """Build the full feature matrix for ridge models."""
    seasonal = build_seasonal_features(dates, base_date)
    last7_t = np.full((len(dates), 1), float(last7_tmean), dtype=float)
    if include_prcp:
        last7_p = np.full((len(dates), 1), float(last7_prcp), dtype=float)
        return np.hstack([seasonal, last7_t, last7_p])
    return np.hstack([seasonal, last7_t])


def rolling_means(values: np.ndarray, window: int = 7) -> Tuple[np.ndarray, float]:
    """Compute rolling means and return the series plus last value."""
    if len(values) < window:
        mean = float(np.mean(values)) if len(values) else 0.0
        return np.full_like(values, mean, dtype=float), mean
    out = np.zeros_like(values, dtype=float)
    for i in range(len(values)):
        start = max(0, i - window + 1)
        out[i] = float(np.mean(values[start : i + 1]))
    return out, float(out[-1])
