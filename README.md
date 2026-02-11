# WeatherApp

WeatherApp is a full‑stack weather platform that ingests NOAA/NWS data, stores it in PostgreSQL, serves a Java REST API, and renders a browser UI with interactive maps and forecasts. A Python ML service generates predictions and 10‑day station forecasts that are displayed in the UI.

## Stacks at a Glance

- Front‑end: static site in [public_html/](public_html) (HTML/CSS/JS + canvas charts)
- Backend API: Java (Javalin) in [weatherapp/](weatherapp)
- ML Service: Python (FastAPI + scikit‑learn) in [ml_service/](ml_service)
- Database: PostgreSQL (+ PostGIS) initialized with [Database init/01_init_weatherapp.sql](Database%20init/01_init_weatherapp.sql)
- Reverse proxy: Caddy ([Caddyfile](Caddyfile))

## Repository Layout

- [public_html/](public_html) – UI and client‑side API calls
- [weatherapp/](weatherapp) – Java backend, ingestion, and API server
- [ml_service/](ml_service) – Python ML service
- [Database init/](Database%20init) – SQL initialization scripts

## Support Documents

- [docs/PROGRAM_AND_SCHEMA.md](docs/PROGRAM_AND_SCHEMA.md) – program flow and database schema summary

## How the Program Works

1. **Ingestion (Java backend)**

- Scheduled jobs pull NWS hourly forecasts and NOAA daily summaries into PostgreSQL.
- Local historic CSVs (if present) are ingested on startup.

2. **Serving (Java backend)**

- The API exposes endpoints for gridpoints, stations, alerts, hourly forecasts, and daily summaries.

3. **ML predictions (Python service)**

- Uses NOAA daily history for training and generates station/gridpoint forecasts.
- Stores predictions in `ml_weather_prediction` for the UI to display.

4. **Front‑end**

- Reads API endpoints and renders charts/tables in the browser.

## Quick Start (Local)

1. **PostgreSQL + PostGIS**

- Create a database (default `weatherdb`) and run [Database init/01_init_weatherapp.sql](Database%20init/01_init_weatherapp.sql).

2. **Java Backend**

- Configure DB + API settings in [weatherapp/src/main/resources/application.properties](weatherapp/src/main/resources/application.properties).
- Build and run:
  - `mvn -f weatherapp/pom.xml -DskipTests package`
  - `java -jar weatherapp/target/weatherapp_backend-1.0.0-all.jar`

3. **ML Service (optional; front‑end runs without it)**

- Install Python deps from [ml_service/requirements.txt](ml_service/requirements.txt).
- Run:
  - `python -m uvicorn app:app --host 0.0.0.0 --port 8000`

4. **Front‑end**

- Open [public_html/index.html](public_html/index.html) in a browser (or host it via Caddy).

## ML Service Overview

The ML service is split into small modules for clarity:

- db/ – database connection helpers and query modules
- ml/ – model training + forecasting logic
- api/ – FastAPI routes, middleware, and error handling
- models.py – request models
- settings.py – environment configuration

See [ml_service/README.md](ml_service/README.md) for ML details.

## Java Backend Overview

The Java backend handles ingestion and serves API responses used by the UI. It uses scheduled jobs for NOAA/NWS refreshes and caches data in PostgreSQL.

See [weatherapp/README.md](weatherapp/README.md) for details.

## LattePanda (Linux) Setup Guide

This project is designed to run on a LattePanda or similar small Linux device. These steps set up a headless Ubuntu Server environment.

### 1) OS and Base Packages

- Install Ubuntu Server 22.04 or newer.
- Update:
  - `sudo apt update && sudo apt upgrade -y`
- Install core tools:
  - `sudo apt install -y git unzip curl build-essential`

### 2) Java 17 (Backend)

- `sudo apt install -y openjdk-17-jre-headless`
- Verify: `java -version`

### 3) Python 3.11 (ML Service)

- `sudo apt install -y python3 python3-venv python3-pip`
- Create venv in [ml_service/](ml_service):
  - `python3 -m venv .venv`
  - `source .venv/bin/activate`
  - `pip install -r requirements.txt`

### 4) PostgreSQL + PostGIS

- `sudo apt install -y postgresql postgis`
- Create DB/user and run [Database init/01_init_weatherapp.sql](Database%20init/01_init_weatherapp.sql).

### 5) Caddy (Reverse Proxy)

- `sudo apt install -y caddy`
- Update [Caddyfile](Caddyfile) for your domain/IP and start Caddy:
  - `sudo systemctl enable --now caddy`

### 6) Run Services

- **Java backend**: `java -jar weatherapp_backend-1.0.0-all.jar`
- **ML service**: `uvicorn app:app --host 0.0.0.0 --port 8000`
- **Front‑end**: serve [public_html/](public_html) via Caddy or a static server

### 7) Systemd (Optional)

Create unit files so services restart on boot:

- `weatherapp.service` (Java API)
- `weatherapp-ml.service` (ML service)

Include `Environment=` lines for DB creds and any ML variables listed in [ml_service/README.md](ml_service/README.md).

Example unit files used in production:

weatherapp.service

```
[Unit]
Description=WeatherApp API + Ingest
After=network.target postgresql.service

[Service]
User=appadmin
WorkingDirectory=/opt/weather-app/WeatherApp/weatherapp
EnvironmentFile=/opt/weather-app/env/weatherapp.env
ExecStart=/usr/bin/java -jar /opt/weather-app/WeatherApp/weatherapp/target/weatherapp_backend-1.0.0-all.jar
Restart=always
RestartSec=3
Environment=JAVA_OPTS=-Xms128m -Xmx512m
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true

[Install]
WantedBy=multi-user.target
```

weatherapp-ml.service

```
[Unit]
Description=WeatherApp ML Service (FastAPI)
After=network.target postgresql.service
Wants=postgresql.service

[Service]
User=appadmin
WorkingDirectory=/opt/weather-app/WeatherApp/ml_service
EnvironmentFile=/opt/weather-app/env/weatherapp-ml.env
ExecStart=/opt/weather-app/WeatherApp/ml_service/.venv/bin/python -m uvicorn app:app --host ${HOST} --port ${PORT}
Restart=always
RestartSec=3
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true

[Install]
WantedBy=multi-user.target
```

## Notes

- The ML service is optional; the app still works with NOAA/NWS data alone.
- Credentials in [weatherapp/src/main/resources/application.properties](weatherapp/src/main/resources/application.properties) should be replaced for production.
