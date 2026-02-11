# WeatherApp Program + Schema Guide

This document explains how the program works end‑to‑end and summarizes the core database schema used by the system.

## Program Flow

1. **Ingest (Java backend)**

- Scheduled jobs call NOAA/NWS APIs and store results in PostgreSQL.
- If local historic CSVs are present, they are ingested at startup.
- The Java API also caches responses for commonly requested endpoints.

2. **Serve (Java backend)**

- Javalin exposes REST endpoints for map layers, hourly forecasts, alerts, stations, and history.
- The UI (public_html) calls these endpoints directly.

3. **Predict (Python ML service)**

- The ML service trains from NOAA daily history and writes predictions to `ml_weather_prediction`.
- On startup, it generates 10‑day forecasts for stations and mapped gridpoints.

4. **Render (Front‑end)**

- The browser UI renders gridpoints, stations, and alerts on the map.
- Forecast panels show hourly (NWS) and daily/ML results.

## Key Database Tables / Views

### Core geo + forecast

- `geo_gridpoint`
  - Stores gridpoint geometry and NWS grid metadata.
- `gridpoint_station_map`
  - Maps gridpoints to nearby stations (primary mapping).
- `nws_forecast_hourly`
  - Raw hourly forecast data ingested from NWS.
- `v_latest_hourly_forecast` (view)
  - Latest hourly values per gridpoint for fast API reads.

### NOAA station history

- `noaa_station`
  - Station metadata and location.
- `noaa_daily_summary`
  - Daily historic observations (tmin/tmax/prcp) by station.

### Alerts

- `nws_alert`
  - Raw alert feed from NWS.
- `v_active_alerts` (view)
  - Active alert polygons + metadata.

### Tracked points

- `tracked_point`
  - Saved points of interest for repeat queries.

### ML outputs

- `ml_weather_prediction`
  - ML predictions and forecast rows (per source, horizon).

### Ingest telemetry

- `ingest_run`
  - Tracks ingestion job runs.
- `ingest_event`
  - API call metrics and errors per ingest step.

## API/Data Contracts (high level)

- **Hourly forecast**: `/api/forecast/hourly` and `/api/forecast/hourly/point`
- **Daily forecast**: `/api/forecast/daily` (derived from hourly view)
- **Stations**: `/api/stations/near`, `/api/stations/all`
- **History**: `/api/history/daily`, `/api/history/gridpoint`
- **ML**: `/api/ml/weather/latest`, `/api/ml/weather/forecast`

## Notes

- NOAA daily data can lag; ML defaults to latest available NOAA date.
- Gridpoints are the primary unit for hourly forecast data.
