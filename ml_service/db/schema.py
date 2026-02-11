"""Ensure ML service database tables and columns exist."""

from settings import Settings
from db.conn import db_conn
import logging

# More logging =) 
log = logging.getLogger("weatherapp-ml")

# Ensure the necessary database tables for ML predictions exist before later use
# if they do not exist, create them; if they do exist, ensure they have the necessary columns
# if everything is fine, nothing happens to pre existing data as a result of this function
def ensure_tables(settings: Settings) -> None:
    """Create/alter ML tables needed by the service."""
    sql = """
        CREATE TABLE IF NOT EXISTS tracked_point (
            id BIGSERIAL PRIMARY KEY,
            name TEXT,
            lat DOUBLE PRECISION NOT NULL,
            lon DOUBLE PRECISION NOT NULL,
            created_at TIMESTAMPTZ DEFAULT now(),
            UNIQUE(lat, lon)
        );

        CREATE TABLE IF NOT EXISTS ml_weather_prediction (
            id BIGSERIAL PRIMARY KEY,
            source_type TEXT NOT NULL,
            source_id TEXT,
            lat DOUBLE PRECISION NOT NULL,
            lon DOUBLE PRECISION NOT NULL,
            as_of_date DATE NOT NULL,
            horizon_hours INT,
            tmean_c DOUBLE PRECISION,
            prcp_mm DOUBLE PRECISION,
            model_name TEXT,
            created_at TIMESTAMPTZ DEFAULT now(),
            UNIQUE (source_type, source_id, lat, lon, as_of_date, horizon_hours)
        );
    """

    alter_sql = """
        ALTER TABLE ml_weather_prediction ADD COLUMN IF NOT EXISTS tmin_c DOUBLE PRECISION;
        ALTER TABLE ml_weather_prediction ADD COLUMN IF NOT EXISTS tmax_c DOUBLE PRECISION;
        ALTER TABLE ml_weather_prediction ADD COLUMN IF NOT EXISTS delta_c DOUBLE PRECISION;
        ALTER TABLE ml_weather_prediction ADD COLUMN IF NOT EXISTS model_detail TEXT;
        ALTER TABLE ml_weather_prediction ADD COLUMN IF NOT EXISTS confidence REAL;
    """

    with db_conn(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()

    log.info("Ensured ML tables exist")

    with db_conn(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(alter_sql)
            conn.commit()

    log.info("Ensured ML forecast columns exist")
