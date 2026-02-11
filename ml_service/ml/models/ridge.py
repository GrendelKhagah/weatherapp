"""Ridge regression forecasters for daily weather."""

import datetime as dt
from typing import List, Optional, Tuple

import numpy as np
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from ml.models.features import build_ridge_features, rolling_means
from ml.models.series import StationSeries


def _fit_ridge(x: np.ndarray, y: np.ndarray) -> Ridge:
    """Fit a ridge regression model with standard scaling."""
    model = make_pipeline(StandardScaler(), Ridge(alpha=1.0, random_state=17))
    model.fit(x, y)
    return model


def _residual_std(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Compute residual standard deviation for model diagnostics."""
    if not len(y_true):
        return 0.0
    resid = y_true - y_pred
    return float(np.std(resid))


class RidgeForecastModel:
    """Ridge-based forecaster using seasonal and trend features."""
    def __init__(self, name: str, detail: str, include_prcp_feature: bool):
        """Create a ridge model with configurable feature set."""
        self.name = name
        self.detail = detail
        self.include_prcp_feature = include_prcp_feature
        self.base_date: Optional[dt.date] = None
        self.last_date: Optional[dt.date] = None
        self.last7_tmean: Optional[float] = None
        self.last7_prcp: Optional[float] = None
        self.model_tmin: Optional[Ridge] = None
        self.model_tmax: Optional[Ridge] = None
        self.model_prcp: Optional[Ridge] = None
        self.resid_std: float = 0.0

    def fit(self, series: StationSeries) -> "RidgeForecastModel":
        """Fit ridge models to the station time series."""
        self.base_date = series.dates[0]
        self.last_date = series.last_date
        _, last7_tmean = rolling_means(series.tmean_c, window=7)
        _, last7_prcp = rolling_means(series.prcp_mm, window=7)
        self.last7_tmean = last7_tmean
        self.last7_prcp = last7_prcp

        x = build_ridge_features(series.dates, self.base_date, last7_tmean, last7_prcp, self.include_prcp_feature)

        self.model_tmin = _fit_ridge(x, series.tmin_c)
        self.model_tmax = _fit_ridge(x, series.tmax_c)
        self.model_prcp = _fit_ridge(x, np.log1p(series.prcp_mm))

        pred_tmin = self.model_tmin.predict(x)
        pred_tmax = self.model_tmax.predict(x)
        pred_prcp = self.model_prcp.predict(x)
        self.resid_std = float(np.mean([
            _residual_std(series.tmin_c, pred_tmin),
            _residual_std(series.tmax_c, pred_tmax),
            _residual_std(np.log1p(series.prcp_mm), pred_prcp),
        ]))
        return self

    def predict(
        self,
        *,
        base_date: dt.date,
        horizon_days: int,
        current_temp_c: Optional[float],
        confidence_base: float,
    ) -> List[dict]:
        """Predict daily values for the requested horizon."""
        if not (self.model_tmin and self.model_tmax and self.model_prcp and self.base_date):
            return []

        last7_tmean = self.last7_tmean if self.last7_tmean is not None else 0.0
        last7_prcp = self.last7_prcp if self.last7_prcp is not None else 0.0

        bias = 0.0
        if current_temp_c is not None and np.isfinite(current_temp_c):
            bias = (float(current_temp_c) - float(last7_tmean)) * 0.4

        future_dates = [base_date + dt.timedelta(days=h) for h in range(0, horizon_days)]
        x_future = build_ridge_features(future_dates, self.base_date, last7_tmean, last7_prcp, self.include_prcp_feature)

        tmin = self.model_tmin.predict(x_future) + bias
        tmax = self.model_tmax.predict(x_future) + bias
        prcp = np.expm1(self.model_prcp.predict(x_future))

        preds = []
        for idx, target_date in enumerate(future_dates):
            tmin_pred = float(tmin[idx])
            tmax_pred = float(tmax[idx])
            if tmin_pred > tmax_pred:
                tmin_pred, tmax_pred = tmax_pred, tmin_pred
            tmean_pred = (tmin_pred + tmax_pred) / 2.0
            delta_pred = tmax_pred - tmin_pred

            conf = max(0.1, confidence_base - min(0.6, self.resid_std / 12.0))

            preds.append(
                {
                    "date": target_date,
                    "as_of": base_date,
                    "horizon_hours": idx * 24,
                    "tmin_c": tmin_pred,
                    "tmax_c": tmax_pred,
                    "tmean_c": tmean_pred,
                    "prcp_mm": max(0.0, float(prcp[idx])),
                    "delta_c": delta_pred,
                    "model_name": self.name,
                    "model_detail": self.detail,
                    "confidence": conf,
                }
            )
        return preds


def ridge_models() -> List[RidgeForecastModel]:
    """Return the set of ridge model variants to run."""
    return [
        RidgeForecastModel(
            name="ridge-v1",
            detail="ridge-v1 features=doy_sin_cos,last7_tmean,last7_prcp,trend",
            include_prcp_feature=True,
        ),
        RidgeForecastModel(
            name="ridge-seasonal-v2",
            detail="ridge-seasonal doy_sin_cos+7day_mean+trend",
            include_prcp_feature=False,
        ),
    ]
