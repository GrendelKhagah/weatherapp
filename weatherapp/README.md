# Java Backend

The Java backend ingests NOAA/NWS data, stores that data in PostgreSQL, and serves the REST API consumed by the front‑end and the ML service. It uses Javalin for the HTTP server, HikariCP for database pooling, and Jackson for JSON serialization.

## Plain-language overview

This program is the “middle layer” of the app. It regularly pulls weather data from NOAA and the National Weather Service, saves it in a database, and then provides clean API endpoints so the website and ML service can ask for forecasts, history, and alerts.

## Build

- `mvn -DskipTests package`

This produces a runnable JAR:

- `target/weatherapp_backend-1.0.0-all.jar`

## Run

- `java -jar target/weatherapp_backend-1.0.0-all.jar`

## Configuration

Edit [src/main/resources/application.properties](src/main/resources/application.properties) or override with environment variables.

Key settings:

- `api.port` – HTTP port (default 8080)
- `db.jdbcUrl`, `db.username`, `db.password`
- NOAA/NWS tokens and schedule intervals

Example environment file for Linux deployments:

```
API_PORT=8080
DB_JDBC_URL=jdbc:postgresql://127.0.0.1:5432/weatherdb
DB_USERNAME=weatherapp
DB_PASSWORD=***Depends what you set during setup***
NWS_USER_AGENT=(yourWebsite.space, youremail@example.com)
NOAA_API_ENABLED=true
NOAA_TOKEN=YOUR_NOAA_TOKEN
```

## Main Components

- [src/main/java/space/ketterling/Main.java](src/main/java/space/ketterling/Main.java) – startup, ingestion, and API server orchestration
- [src/main/java/space/ketterling/api/ApiServer.java](src/main/java/space/ketterling/api/ApiServer.java) – server bootstrap + shared helpers
- API route modules:
  - [src/main/java/space/ketterling/api/ApiRoutesRoot.java](src/main/java/space/ketterling/api/ApiRoutesRoot.java)
  - [src/main/java/space/ketterling/api/ApiRoutesMetrics.java](src/main/java/space/ketterling/api/ApiRoutesMetrics.java)
  - [src/main/java/space/ketterling/api/ApiRoutesTracked.java](src/main/java/space/ketterling/api/ApiRoutesTracked.java)
  - [src/main/java/space/ketterling/api/ApiRoutesGeo.java](src/main/java/space/ketterling/api/ApiRoutesGeo.java)
  - [src/main/java/space/ketterling/api/ApiRoutesForecast.java](src/main/java/space/ketterling/api/ApiRoutesForecast.java)
  - [src/main/java/space/ketterling/api/ApiRoutesIngest.java](src/main/java/space/ketterling/api/ApiRoutesIngest.java)
  - [src/main/java/space/ketterling/api/ApiRoutesMl.java](src/main/java/space/ketterling/api/ApiRoutesMl.java)
  - [src/main/java/space/ketterling/api/ApiRoutesLayer.java](src/main/java/space/ketterling/api/ApiRoutesLayer.java)
  - [src/main/java/space/ketterling/api/ApiRoutesHistory.java](src/main/java/space/ketterling/api/ApiRoutesHistory.java)
  - [src/main/java/space/ketterling/api/ApiRoutesPointSummary.java](src/main/java/space/ketterling/api/ApiRoutesPointSummary.java)
- `ingest/` – NOAA/NWS ingest jobs and schedulers
- `db/` – repository and data access helpers

## ML Integration

If the ML service is running, the UI can request predictions through the Java API endpoints that read from `ml_weather_prediction`.

## Support Docs

- [docs/PROGRAM_AND_SCHEMA.md](../docs/PROGRAM_AND_SCHEMA.md)
