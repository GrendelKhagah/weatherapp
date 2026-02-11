"""Pydantic request models for ML endpoints."""

import datetime as dt
from typing import Optional

from pydantic import BaseModel


class PredictRequest(BaseModel):
    """Request body for single-point prediction endpoints."""
    lat: float
    lon: float
    date: Optional[dt.date] = None
    neighbors: Optional[int] = None
    source_type: Optional[str] = None
    source_id: Optional[str] = None
    horizon_hours: Optional[int] = None
    model_name: Optional[str] = None


class PredictBatchRequest(BaseModel):
    """Request body for batch prediction endpoints."""
    date: Optional[dt.date] = None
    neighbors: Optional[int] = None
    limit: Optional[int] = None
    model_name: Optional[str] = None
