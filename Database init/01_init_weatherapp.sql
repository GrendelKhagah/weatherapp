-- =========================================================
-- 001_init_weather_app.sql
-- use to initialize the database schema
-- =========================================================
-- Gridpoint-first weather w/ ML schema for Postgres/PostGIS
-- Includes: NWS live data, NOAA historical, ML outputs,
-- ingest monitoring, constraints, and useful views.
-- =========================================================

-- 0) Extensions
CREATE EXTENSION IF NOT EXISTS postgis;

-- =========================================================
-- 1) INGEST MONITORING (operations + Part D)
-- =========================================================
CREATE TABLE IF NOT EXISTS ingest_run (
    run_id UUID PRIMARY KEY,
    job_name TEXT NOT NULL, -- example: "nws_alerts", "nws_hourly_forecast", "noaa_history_load"
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'RUNNING', -- RUNNING / SUCCESS / FAILED
    notes TEXT
);

CREATE TABLE IF NOT EXISTS ingest_event (
    event_id BIGSERIAL PRIMARY KEY,
    run_id UUID REFERENCES ingest_run(run_id) ON DELETE SET NULL,
    source TEXT NOT NULL,  -- "NWS" or "NOAA"
    endpoint TEXT NOT NULL,-- requested URL
    http_status INT,
    response_ms INT,
    error TEXT,
    response_headers JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);



CREATE INDEX IF NOT EXISTS idx_ingest_event_run
    ON ingest_event(run_id);

-- =========================================================
-- 1b) API USAGE LOGGING (external API calls)
-- =========================================================
CREATE TABLE IF NOT EXISTS api_service (
    service_id BIGSERIAL PRIMARY KEY,
    service_name TEXT NOT NULL UNIQUE,
    base_url TEXT,
    rate_limit_per_hour INT,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS api_call_log (
    call_id BIGSERIAL PRIMARY KEY,
    service_id BIGINT NOT NULL REFERENCES api_service(service_id) ON DELETE CASCADE,
    requested_at TIMESTAMPTZ DEFAULT now(),
    method TEXT,
    endpoint TEXT,
    request_url TEXT,
    http_status INT,
    response_ms INT,
    rate_limit_remaining INT,
    response_headers JSONB,
    error_text TEXT
);

CREATE INDEX IF NOT EXISTS idx_api_call_log_service
    ON api_call_log(service_id, requested_at DESC);

CREATE INDEX IF NOT EXISTS idx_api_call_log_requested
    ON api_call_log(requested_at DESC);

CREATE OR REPLACE VIEW v_api_usage_hourly AS
SELECT s.service_name,
       s.rate_limit_per_hour,
       date_trunc('hour', l.requested_at) AS hour_bucket,
       COUNT(l.call_id) AS call_count
FROM api_service s
LEFT JOIN api_call_log l
  ON l.service_id = s.service_id
GROUP BY s.service_name, s.rate_limit_per_hour, hour_bucket
ORDER BY hour_bucket DESC;

-- =========================================================
-- 2) GRIDPOINT REFERENCE (core spatial unit)
-- =========================================================
CREATE TABLE IF NOT EXISTS geo_gridpoint (
    grid_id TEXT PRIMARY KEY,  
    office TEXT NOT NULL,
    grid_x INT NOT NULL,
    grid_y INT NOT NULL,
    geom geometry(Point, 4326) NOT NULL, -- representative lat/lon point
    forecast_grid_data_url TEXT NOT NULL,
    forecast_hourly_url TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    last_refreshed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_geo_gridpoint_geom
    ON geo_gridpoint
    USING GIST (geom);

-- =========================================================
-- 3) NWS HOURLY FORECAST CACHE
-- =========================================================
CREATE TABLE IF NOT EXISTS nws_forecast_hourly (
    grid_id TEXT NOT NULL REFERENCES geo_gridpoint(grid_id) ON DELETE CASCADE,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,

    -- normalized units (recommended):
    -- temperature: C, wind: m/s, precip: probability 0..1
    temperature_c REAL,
    wind_speed_mps REAL,
    wind_gust_mps REAL,
    wind_dir_deg REAL,
    precip_prob REAL, -- 0..1
    relative_humidity REAL,
    short_forecast TEXT,

    -- forecast issuance time supports backtesting:
    issued_at TIMESTAMPTZ, -- when this forecast set was issued
    raw_json JSONB,
    ingested_at TIMESTAMPTZ DEFAULT now(),

    PRIMARY KEY (grid_id, start_time)
);

CREATE INDEX IF NOT EXISTS idx_nws_forecast_time
    ON nws_forecast_hourly (start_time);

CREATE INDEX IF NOT EXISTS idx_nws_forecast_issued
    ON nws_forecast_hourly (grid_id, issued_at DESC);

-- Range check
ALTER TABLE nws_forecast_hourly
    DROP CONSTRAINT IF EXISTS chk_precip_prob_range;

ALTER TABLE nws_forecast_hourly
    ADD CONSTRAINT chk_precip_prob_range
    CHECK (precip_prob IS NULL OR (precip_prob >= 0 AND precip_prob <= 1));

-- =========================================================
-- 4) NWS ALERT POLYGONS (map layer)
-- =========================================================
CREATE TABLE IF NOT EXISTS nws_alert (
    alert_id TEXT PRIMARY KEY, -- NWS identifier/uri
    event TEXT,
    severity TEXT,
    certainty TEXT,
    urgency TEXT,
    headline TEXT,
    description TEXT,
    instruction TEXT,
    effective TIMESTAMPTZ,
    onset TIMESTAMPTZ,
    expires TIMESTAMPTZ,
    ends TIMESTAMPTZ,
    status TEXT,
    message_type TEXT,
    area_desc TEXT,

    geom geometry(Geometry, 4326), -- polygon/multipolygon when present
    raw_json JSONB,
    ingested_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_nws_alert_geom
    ON nws_alert
    USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_nws_alert_expires
    ON nws_alert (expires);

-- =========================================================
-- 5) NOAA STATIONS (historical baseline)
-- =========================================================
CREATE TABLE IF NOT EXISTS noaa_station (
    station_id TEXT PRIMARY KEY, -- example: "GHCND:USW00023174"
    name TEXT,
    geom geometry(Point, 4326),
    elevation_m REAL,
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS idx_noaa_station_geom
    ON noaa_station
    USING GIST (geom);

-- =========================================================
-- 6) NOAA DAILY HISTORY (training data)
-- =========================================================
CREATE TABLE IF NOT EXISTS noaa_daily_summary (
    station_id TEXT NOT NULL REFERENCES noaa_station(station_id) ON DELETE CASCADE,
    date DATE NOT NULL,

    -- normalized units : C for temps, mm for precip
    tmax_c REAL,
    tmin_c REAL,
    prcp_mm REAL,

    raw_json JSONB,
    PRIMARY KEY (station_id, date)
);

-- =========================================================
-- 7) GRIDPOINT to STATION MAPPING (ties history to gridpoints)
-- =========================================================
CREATE TABLE IF NOT EXISTS gridpoint_station_map (
    grid_id TEXT NOT NULL REFERENCES geo_gridpoint(grid_id) ON DELETE CASCADE,
    station_id TEXT NOT NULL REFERENCES noaa_station(station_id) ON DELETE CASCADE,
    distance_m REAL,
    is_primary BOOLEAN DEFAULT false,
    PRIMARY KEY (grid_id, station_id)
);

CREATE INDEX IF NOT EXISTS idx_gridpoint_station_primary
    ON gridpoint_station_map (grid_id, is_primary);

-- =========================================================
-- 8) ML MODEL RUNS
-- =========================================================
CREATE TABLE IF NOT EXISTS ml_model_run (
    run_id UUID PRIMARY KEY,
    model_name TEXT NOT NULL,
    feature_version TEXT NOT NULL,

    -- training window for repeatability
    train_start DATE,
    train_end DATE,

    -- extra traceability
    dataset_version TEXT,
    label_policy JSONB,    -- document how labels/risk rules were created
    split_strategy TEXT,   -- temporal / random / etc.

    hyperparams JSONB,
    artifact_path TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- =========================================================
-- 9) ML PREDICTIONS (map + time)
-- =========================================================
CREATE TABLE IF NOT EXISTS ml_prediction (
    run_id UUID NOT NULL REFERENCES ml_model_run(run_id) ON DELETE CASCADE,
    grid_id TEXT NOT NULL REFERENCES geo_gridpoint(grid_id) ON DELETE CASCADE,

    valid_time TIMESTAMPTZ NOT NULL,
    horizon_hours INT NOT NULL,

    -- outputs
    risk_score REAL, -- 0..1
    risk_class TEXT, -- "LOW"/"MED"/"HIGH"
    explain JSONB,   -- feature contributions / explanation text
    created_at TIMESTAMPTZ DEFAULT now(),

    PRIMARY KEY (run_id, grid_id, valid_time)
);

CREATE INDEX IF NOT EXISTS idx_ml_prediction_time
    ON ml_prediction (grid_id, valid_time);

-- Range check
ALTER TABLE ml_prediction
    DROP CONSTRAINT IF EXISTS chk_risk_score_range;

ALTER TABLE ml_prediction
    ADD CONSTRAINT chk_risk_score_range
    CHECK (risk_score IS NULL OR (risk_score >= 0 AND risk_score <= 1));

-- =========================================================
-- 10) ML METRICS (accuracy reporting)
-- =========================================================
CREATE TABLE IF NOT EXISTS ml_metric (
    run_id UUID NOT NULL REFERENCES ml_model_run(run_id) ON DELETE CASCADE,
    metric_name TEXT NOT NULL, -- accuracy, precision, recall, f1, mae, rmse, etc.
    metric_value REAL,
    computed_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (run_id, metric_name)
);

-- =========================================================
-- 11) API-FRIENDLY VIEWS
-- =========================================================

-- Active alerts "right now"
CREATE OR REPLACE VIEW v_active_alerts AS
SELECT *
FROM nws_alert
WHERE (expires IS NULL OR expires > now())
  AND (effective IS NULL OR effective <= now());

-- Latest forecast per gridpoint per hour based on issued_at (fallback to ingested_at)
CREATE OR REPLACE VIEW v_latest_hourly_forecast AS
SELECT f.*
FROM nws_forecast_hourly f
JOIN (
    SELECT grid_id,
           start_time,
           MAX(COALESCE(issued_at, ingested_at)) AS max_issue
    FROM nws_forecast_hourly
    GROUP BY grid_id, start_time
) x
ON f.grid_id = x.grid_id
AND f.start_time = x.start_time
AND COALESCE(f.issued_at, f.ingested_at) = x.max_issue;

-- Convenience view for “latest predictions” (filtered by run_id in query)
CREATE OR REPLACE VIEW v_latest_predictions AS
SELECT p.*
FROM ml_prediction p;

-- =========================================================
-- 12) CACHED GRID AGGREGATIONS
-- Optional table to store precomputed layer aggregates (for faster API responses)
-- =========================================================
-- Ensure canonical `cached_grid_agg` table (app expects this schema).
-- This block is idempotent and will attempt to make the existing
-- table compatible with the application's expectations.

-- Create table if it does not exist with the canonical schema used by the app.
CREATE TABLE IF NOT EXISTS cached_grid_agg (
    grid_id TEXT PRIMARY KEY REFERENCES geo_gridpoint(grid_id) ON DELETE CASCADE,
    as_of DATE NOT NULL,
    tmean_c REAL,
    prcp_30d_mm REAL,
    last_updated TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cached_grid_agg_asof ON cached_grid_agg (as_of);


-- Best-effort migration steps to make an existing, differently-shaped
-- `cached_grid_agg` table compatible with the application. These steps
-- are safe (use IF NOT EXISTS) and wrapped in a transaction below.

ALTER TABLE cached_grid_agg ADD COLUMN IF NOT EXISTS as_of DATE;
ALTER TABLE cached_grid_agg ADD COLUMN IF NOT EXISTS tmean_c REAL;
ALTER TABLE cached_grid_agg ADD COLUMN IF NOT EXISTS prcp_30d_mm REAL;
ALTER TABLE cached_grid_agg ADD COLUMN IF NOT EXISTS last_updated TIMESTAMPTZ DEFAULT now();

-- Create a unique index on grid_id to support a primary key if needed.
CREATE UNIQUE INDEX IF NOT EXISTS idx_cached_grid_agg_grid_id_unique ON cached_grid_agg (grid_id);


-- Ownership and grants: tuned for the default app role weatherapp.
-- If the  app uses a different DB role, update the role name before running.
-- These operations must be run by a superuser or the current table owner.
ALTER TABLE cached_grid_agg OWNER TO weatherapp;
GRANT SELECT, INSERT, UPDATE, DELETE ON cached_grid_agg TO weatherapp;


CREATE INDEX IF NOT EXISTS idx_cached_grid_agg_grid_asof_desc ON cached_grid_agg (grid_id, as_of DESC);


-- inspect distinct station id formats
SELECT substring(station_id from '^[A-Z]+:') AS prefix, count(*) FROM noaa_daily_summary GROUP BY prefix;
SELECT DISTINCT station_id FROM noaa_daily_summary LIMIT 20;

-- update mappings to use the same prefix (only where the prefixed value exists)
UPDATE gridpoint_station_map m
SET station_id = 'GHCND:' || m.station_id
WHERE NOT EXISTS (SELECT 1 FROM noaa_daily_summary d WHERE d.station_id = m.station_id)
  AND EXISTS (SELECT 1 FROM noaa_daily_summary d WHERE d.station_id = 'GHCND:' || m.station_id);

-- verify join would now find rows
SELECT COUNT(DISTINCT g.grid_id) AS grids_with_data
FROM geo_gridpoint g
JOIN gridpoint_station_map m ON g.grid_id = m.grid_id AND m.is_primary = true
JOIN noaa_daily_summary d ON d.station_id = m.station_id
  AND d.date > '2026-01-24'::date - (30 * INTERVAL '1 day') AND d.date <= '2026-01-24'::date;