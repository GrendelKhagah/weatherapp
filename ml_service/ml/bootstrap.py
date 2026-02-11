"""Bootstrap forecasts for stations and gridpoints."""

import datetime as dt
import logging
from typing import Dict, List, Optional, Tuple

from db.mapping import load_current_nws_temp_by_station, load_gridpoint_primary_map, load_gridpoint_station_neighbors
from db.noaa import latest_noaa_date, load_recent_station_history, load_station_coords
from db.predictions import any_forecast_exists, forecast_exists, load_forecast_rows, store_prediction
from ml.forecast import predict_station_forecast
from settings import Settings

# log log log log log
log = logging.getLogger("weatherapp-ml")

# Bootstrap ML forecasts for stations and gridpoints
# This function trains a forecast model and generates predictions
# for all stations and gridpoints, storing them in the database.
def bootstrap_forecasts(settings: Settings) -> None:
    """Generate and store forecast rows for stations and gridpoints."""
    # wrapped in a try/except to log any errors
    try:
        if not settings.bootstrap_enabled:
            log.info("Forecast bootstrap disabled via ML_BOOTSTRAP_ENABLED")
            return
        log.info("Bootstrapping ML forecasts for stations and gridpoints")
        data_date = latest_noaa_date(settings) or (dt.date.today() - dt.timedelta(days=1))
        forecast_date = dt.date.today()
        log.info("Forecast bootstrap data_date=%s forecast_date=%s", data_date.isoformat(), forecast_date.isoformat())

        station_coords = load_station_coords(settings)
        recent_by_station = load_recent_station_history(settings, days=730)
        current_temp_by_station = load_current_nws_temp_by_station(settings)

        station_preds: Dict[str, List[dict]] = {}
        stored_station = 0
        skipped_station = 0
        station_items = list(recent_by_station.items())
        if settings.bootstrap_station_limit and settings.bootstrap_station_limit > 0:
            station_items = station_items[: settings.bootstrap_station_limit]
        for station_id, rows in station_items:
            coords = station_coords.get(station_id)
            if not coords:
                continue

            current_temp = current_temp_by_station.get(station_id)
            preds = predict_station_forecast(
                station_rows=rows,
                current_temp_c=current_temp,
                as_of_date=forecast_date,
                settings=settings,
            )
            if not preds:
                continue # no predictions generated
            
            # store predictions by station_id
            station_preds[station_id] = preds
            lat, lon = coords
            for p in preds:
                store_prediction(
                    settings,
                    source_type="station",
                    source_id=station_id,
                    lat=lat,
                    lon=lon,
                    as_of=p["as_of"],
                    horizon_hours=p["horizon_hours"],
                    tmean_c=p["tmean_c"],
                    prcp_mm=p["prcp_mm"],
                    model_name=p["model_name"],
                    tmin_c=p["tmin_c"],
                    tmax_c=p["tmax_c"],
                    delta_c=p["delta_c"],
                    model_detail=p["model_detail"],
                    confidence=p["confidence"],
                )
                stored_station += 1 # increment stored predictions for station counter

            # log progress every 200 stations
            if stored_station and stored_station % 200 == 0:
                log.info("Forecast bootstrap progress: station_preds=%d skipped=%d", stored_station, skipped_station)

        grid_rows = load_gridpoint_primary_map(settings)
        if settings.bootstrap_grid_limit and settings.bootstrap_grid_limit > 0:
            grid_rows = grid_rows[: settings.bootstrap_grid_limit]
        neighbor_map = load_gridpoint_station_neighbors(settings, max_km=5.0, max_stations=4)
        stored_grid = 0

        # store predictions for gridpoints based on primary station mapping
        # gridpoint_primary_map: List[Tuple[grid_id, lat, lon, station_id]]
        # 
        for grid_id, lat, lon, station_id in grid_rows:

            neighbor_list = neighbor_map.get(grid_id)
            if neighbor_list:
                station_list = neighbor_list
            else:
                station_list = [(station_id, None)] if station_id else []

            station_forecasts: List[Tuple[List[dict], float]] = []
            for sid, dist_km in station_list:
                preds = station_preds.get(sid)
                if not preds:
                    preds = load_forecast_rows(settings, source_type="station", source_id=sid, as_of_date=forecast_date, days=11)
                if not preds:
                    continue
                weight = 1.0
                if dist_km is not None and dist_km > 0:
                    weight = 1.0 / max(0.5, float(dist_km))
                station_forecasts.append((preds, weight))

            if not station_forecasts:
                continue

            if len(station_forecasts) == 1:
                preds = station_forecasts[0][0]
                for p in preds:
                    store_prediction(
                        settings,
                        source_type="gridpoint",
                        source_id=grid_id,
                        lat=lat,
                        lon=lon,
                        as_of=p["as_of"],
                        horizon_hours=p["horizon_hours"],
                        tmean_c=p["tmean_c"],
                        prcp_mm=p["prcp_mm"],
                        model_name=p["model_name"],
                        tmin_c=p["tmin_c"],
                        tmax_c=p["tmax_c"],
                        delta_c=p["delta_c"],
                        model_detail=p.get("model_detail"),
                        confidence=p.get("confidence"),
                    )
                    stored_grid += 1
                continue

            total_weight = sum(w for _, w in station_forecasts) or 1.0
            by_model: Dict[str, Dict[int, List[Tuple[dict, float]]]] = {}
            for preds, w in station_forecasts:
                for p in preds:
                    model_name = p.get("model_name") or "model"
                    horizon = int(p.get("horizon_hours") or 0)
                    by_model.setdefault(model_name, {}).setdefault(horizon, []).append((p, w))

            for model_name, by_horizon in by_model.items():
                for horizon_hours, items in by_horizon.items():
                    tmin = tmax = tmean = prcp = delta = conf = 0.0
                    for p, w in items:
                        tmin += (p.get("tmin_c") or 0.0) * w
                        tmax += (p.get("tmax_c") or 0.0) * w
                        tmean += (p.get("tmean_c") or 0.0) * w
                        prcp += (p.get("prcp_mm") or 0.0) * w
                        delta += (p.get("delta_c") or 0.0) * w
                        conf += (p.get("confidence") or 0.0) * w

                    tmin /= total_weight
                    tmax /= total_weight
                    tmean /= total_weight
                    prcp /= total_weight
                    delta /= total_weight
                    conf /= total_weight

                    store_prediction(
                        settings,
                        source_type="gridpoint",
                        source_id=grid_id,
                        lat=lat,
                        lon=lon,
                        as_of=forecast_date,
                        horizon_hours=horizon_hours,
                        tmean_c=tmean,
                        prcp_mm=prcp,
                        model_name=model_name,
                        tmin_c=tmin,
                        tmax_c=tmax,
                        delta_c=delta,
                        model_detail=f"cluster km<=5 stations={len(station_forecasts)} data_date={data_date.isoformat()}",
                        confidence=conf,
                    )
                    stored_grid += 1

    # final log of results
        log.info(
            "Forecast bootstrap complete: station_preds=%d grid_preds=%d skipped=%d",
            stored_station,
            stored_grid,
            skipped_station,
        )
    except Exception:
        log.exception("Forecast bootstrap failed 😔")
