"""Gaussian Process Regression forecasting model."""

import datetime as dt
from typing import List, Optional

import numpy as np
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import ConstantKernel, ExpSineSquared, RBF, WhiteKernel

from ml.models.series import StationSeries


def _fit_gp(x: np.ndarray, y: np.ndarray, restarts: int) -> GaussianProcessRegressor:
    """Train a Gaussian Process model with a seasonal kernel."""
    kernel = ConstantKernel(1.0, (0.1, 10.0)) * (
        RBF(length_scale=30.0, length_scale_bounds=(2.0, 365.0))
        + ExpSineSquared(length_scale=30.0, periodicity=365.25, periodicity_bounds=(180.0, 730.0))
    ) + WhiteKernel(noise_level=1.0, noise_level_bounds=(1e-3, 10.0))

    model = GaussianProcessRegressor(
        kernel=kernel,
        alpha=0.5,
        normalize_y=True,
        n_restarts_optimizer=max(0, int(restarts)),
        random_state=17,
    )
    model.fit(x, y)
    return model


class GprForecastModel:
    """GPR-based forecaster for daily temperature and precipitation."""
    name = "gpr-v1"
    detail = "gpr-v1 kernel=rbf+periodic+white"

    def __init__(self, max_points: int = 120, restarts: int = 2):
        """Create a GPR forecaster with optional downsampling."""
        self.max_points = max_points
        self.restarts = restarts
        self.base_date: Optional[dt.date] = None
        self.last_date: Optional[dt.date] = None
        self.gp_tmin: Optional[GaussianProcessRegressor] = None
        self.gp_tmax: Optional[GaussianProcessRegressor] = None
        self.gp_prcp: Optional[GaussianProcessRegressor] = None

    def fit(self, series: StationSeries) -> "GprForecastModel":
        """Fit the model to a station time series."""
        series = series.downsample(self.max_points)
        self.base_date = series.dates[0]
        self.last_date = series.last_date
        xs = np.array([(d - self.base_date).days for d in series.dates], dtype=float).reshape(-1, 1)
        self.gp_tmin = _fit_gp(xs, series.tmin_c, self.restarts)
        self.gp_tmax = _fit_gp(xs, series.tmax_c, self.restarts)
        self.gp_prcp = _fit_gp(xs, np.log1p(series.prcp_mm), self.restarts)
        return self

    def predict(
        self,
        *,
        base_date: dt.date,
        horizon_days: int,
        current_temp_c: Optional[float],
        last7_mean: Optional[float],
        confidence_base: float,
    ) -> List[dict]:
        """Predict daily values for the requested horizon."""
        if not (self.gp_tmin and self.gp_tmax and self.gp_prcp and self.base_date):
            return []

        base_x = float((base_date - self.base_date).days)
        bias = 0.0
        if current_temp_c is not None and last7_mean is not None and np.isfinite(current_temp_c):
            bias = (float(current_temp_c) - float(last7_mean)) * 0.6

        preds = []
        for h in range(0, horizon_days):
            target_date = base_date + dt.timedelta(days=h)
            x = np.array([[base_x + h]], dtype=float)

            tmin_pred, tmin_std = self.gp_tmin.predict(x, return_std=True)
            tmax_pred, tmax_std = self.gp_tmax.predict(x, return_std=True)
            prcp_pred, prcp_std = self.gp_prcp.predict(x, return_std=True)

            tmin_pred = float(tmin_pred[0]) + bias
            tmax_pred = float(tmax_pred[0]) + bias
            prcp_pred = float(np.expm1(prcp_pred[0]))

            if tmin_pred > tmax_pred:
                tmin_pred, tmax_pred = tmax_pred, tmin_pred

            tmean_pred = (tmin_pred + tmax_pred) / 2.0
            delta_pred = tmax_pred - tmin_pred

            std_scale = float(np.mean([tmin_std[0], tmax_std[0], prcp_std[0]])) if np.isfinite(prcp_std[0]) else 0.0
            conf = max(0.1, confidence_base - min(0.7, std_scale / 25.0))

            preds.append(
                {
                    "date": target_date,
                    "as_of": base_date,
                    "horizon_hours": h * 24,
                    "tmin_c": tmin_pred,
                    "tmax_c": tmax_pred,
                    "tmean_c": tmean_pred,
                    "prcp_mm": max(0.0, prcp_pred),
                    "delta_c": delta_pred,
                    "model_name": self.name,
                    "model_detail": self.detail,
                    "confidence": conf,
                }
            )

        return preds
