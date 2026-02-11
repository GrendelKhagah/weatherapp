# ML Service

The ML service is a FastAPI application that predicts station/gridpoint weather (tmean, precip) and generates forecasts using a Gaussian Process regression model (seasonal + smooth kernel) plus ridge regression baselines. It reads NOAA daily history from PostgreSQL and stores predictions in `ml_weather_prediction` for downstream use by the Java API and UI.

## Program Distribution

- Entry points
  - [app.py](app.py) – thin uvicorn entrypoint (`app:app`)
  - [api/app.py](api/app.py) – FastAPI factory and startup hooks
- Runtime configuration
  - [settings.py](settings.py) – environment-driven settings
  - [logging_setup.py](logging_setup.py) – logging configuration
- API layer
  - [api/routes.py](api/routes.py) – REST endpoints
  - [api/middleware.py](api/middleware.py) – request logging
  - [api/errors.py](api/errors.py) – error responses
  - [api/deps.py](api/deps.py) – dependency injection (settings)
- Data access
  - [db/conn.py](db/conn.py) – PostgreSQL connections
  - [db/schema.py](db/schema.py) – table creation/alter
  - [db/noaa.py](db/noaa.py) – NOAA data queries
  - [db/mapping.py](db/mapping.py) – gridpoint/station mapping
  - [db/predictions.py](db/predictions.py) – prediction storage + reads
- ML logic
  - [ml/knn.py](ml/knn.py) – KNN point predictions
  - [ml/forecast.py](ml/forecast.py) – GPR + ridge forecast orchestration
  - [ml/models/gpr.py](ml/models/gpr.py) – GPR seasonal model
  - [ml/models/ridge.py](ml/models/ridge.py) – ridge baselines
  - [ml/models/features.py](ml/models/features.py) – feature builders
  - [ml/models/series.py](ml/models/series.py) – series parsing helpers
  - [ml/bootstrap.py](ml/bootstrap.py) – startup forecast generation
- Schemas + utilities
  - [models.py](models.py) – request schemas
  - [utils.py](utils.py) – shared helpers

## Run

- Install deps: `pip install -r requirements.txt`
- Start: `python -m uvicorn app:app --host 0.0.0.0 --port 8000`

Linux helper script: [run_ml_service.sh](run_ml_service.sh)

## Configuration (Env Vars)

- `DATABASE_URL` (optional, overrides DB\_\* below)
- `DB_HOST` (default `127.0.0.1`)
- `DB_PORT` (default `5432`)
- `DB_NAME` (default `weatherdb`)
- `DB_USER` (default `weatherapp`)
- `DB_PASSWORD` (default empty)

Logging / debugging:

- `ML_LOG_LEVEL` (default `INFO`)
- `ML_LOG_FORMAT` (default `%(asctime)s %(levelname)s %(name)s - %(message)s`)
- `ML_LOG_SQL` (set `true` to log SQL timings)
- `ML_DEBUG_ERRORS` (set `true` to return exception messages in 500 responses)
- `ML_CORS_ORIGINS` (comma‑separated extra CORS origins)

## Linux / systemd

Typical environment file (example) for `/opt/weather-app/env/weatherapp-ml.env`:

```
HOST=0.0.0.0
PORT=8000
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=weatherdb
DB_USER=weatherapp
DB_PASSWORD=***
ML_LOG_LEVEL=INFO
ML_LOG_SQL=false
```

Systemd unit example is included in [README.md](../README.md).

## Endpoints

- `GET /health`
- `POST /train`
- `GET /predict`
- `POST /predict`
- `POST /predict/store`
- `POST /predict/tracked`
- `POST /predict/stations`
- `GET /predictions/latest`
- `GET /weather/latest`
- `GET /weather/forecast`

## Forecast Bootstrap

On startup, the service:

1. Trains a seasonal Gaussian Process model and ridge baselines on NOAA daily history.
2. Computes forecasts for today + 10 days for stations with sufficient history.
3. Stores station forecasts and mapped gridpoint forecasts into `ml_weather_prediction`.

This bootstrap runs in a background thread and logs progress.

## Notes

- NOAA daily data can lag; the service uses the latest available NOAA history but still forecasts from today.
- Confidence decreases when history is stale; current NWS temperature can bias forecasts.
- The KNN point model uses station coordinates (plus derived features) as features for tmean/precip.
