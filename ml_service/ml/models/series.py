"""Time series container for station daily weather values."""

import datetime as dt
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

import numpy as np


@dataclass
class StationSeries:
    """Holds aligned station time series arrays for model training."""
    dates: List[dt.date]
    tmin_c: np.ndarray
    tmax_c: np.ndarray
    prcp_mm: np.ndarray
    tmean_c: np.ndarray
    last_date: dt.date

    @classmethod
    def from_rows(cls, rows: Iterable[Tuple[dt.date, float, float, Optional[float]]], min_rows: int = 30):
        """Create a StationSeries from raw DB rows if enough data exists."""
        cleaned = [r for r in rows if r[0] is not None and r[1] is not None and r[2] is not None]
        if len(cleaned) < min_rows:
            return None
        cleaned.sort(key=lambda r: r[0])
        dates = [r[0] for r in cleaned]
        tmax = np.array([float(r[1]) for r in cleaned], dtype=float)
        tmin = np.array([float(r[2]) for r in cleaned], dtype=float)
        prcp = np.array([float(r[3]) if r[3] is not None else 0.0 for r in cleaned], dtype=float)
        tmean = (tmax + tmin) / 2.0
        return cls(
            dates=dates,
            tmin_c=tmin,
            tmax_c=tmax,
            prcp_mm=prcp,
            tmean_c=tmean,
            last_date=dates[-1],
        )

    def downsample(self, max_points: int) -> "StationSeries":
        """Downsample to at most max_points for faster model fits."""
        if len(self.dates) <= max_points:
            return self
        idx = np.linspace(0, len(self.dates) - 1, max_points).astype(int)
        return StationSeries(
            dates=[self.dates[i] for i in idx],
            tmin_c=self.tmin_c[idx],
            tmax_c=self.tmax_c[idx],
            prcp_mm=self.prcp_mm[idx],
            tmean_c=self.tmean_c[idx],
            last_date=self.last_date,
        )
