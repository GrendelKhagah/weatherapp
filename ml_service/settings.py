"""App configuration for the ML service."""

import os
from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class Settings:
    """Holds environment-driven settings for the ML service."""
    # DB
    database_url: Optional[str]
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str

    # Logging
    log_level: str
    log_format: str
    log_sql: bool
    debug_errors: bool

    # API
    cors_allow_origins: List[str]

    # ML cache
    model_cache_dir: str

    # GPR forecast tuning
    gpr_enabled: bool
    gpr_max_points: int
    gpr_restarts: int

    # Forecast/bootstrapping limits
    forecast_days: int
    bootstrap_enabled: bool
    bootstrap_station_limit: int
    bootstrap_grid_limit: int

    @staticmethod
    def load() -> "Settings":
        """Load settings from environment variables with safe defaults."""
        log_level = os.getenv("ML_LOG_LEVEL", "INFO").upper()
        log_format = os.getenv(
            "ML_LOG_FORMAT",
            "%(asctime)s %(levelname)s %(name)s - %(message)s",
        )

        default_cache_dir = os.path.join(os.path.expanduser("~"), ".weatherapp-ml-cache")

        # adjust CORS origins with some safe defaults and allow extra from env var
        cors_allow_origins = [
            "https://api.ketterling.space",
            "https://map.ketterling.space",
            "https://ketterling.space",
            "http://localhost",
            "http://localhost:5500",
            "http://127.0.0.1",
            "http://127.0.0.1:5500",
        ]
        extra_origins = os.getenv("ML_CORS_ORIGINS")
        if extra_origins:
            for part in extra_origins.split(","):
                v = part.strip()
                if v:
                    cors_allow_origins.append(v)

        def _truthy(env: str, default: str = "false") -> bool:
            """Parse typical truthy strings from the environment."""
            return os.getenv(env, default).strip().lower() in {"1", "true", "yes", "y", "on"}

        return Settings(
            database_url=os.getenv("DATABASE_URL"),
            db_host=os.getenv("DB_HOST", "127.0.0.1"),
            db_port=int(os.getenv("DB_PORT", "5432")),
            db_name=os.getenv("DB_NAME", "weatherdb"),
            db_user=os.getenv("DB_USER", "weatherapp"),
            db_password=os.getenv("DB_PASSWORD", ""),
            log_level=log_level,
            log_format=log_format,
            log_sql=_truthy("ML_LOG_SQL"),
            debug_errors=_truthy("ML_DEBUG_ERRORS"),
            cors_allow_origins=cors_allow_origins,
            model_cache_dir=os.getenv("ML_MODEL_CACHE_DIR", default_cache_dir),
            gpr_enabled=_truthy("ML_GPR_ENABLED", "true"),
            gpr_max_points=int(os.getenv("ML_GPR_MAX_POINTS", "80")),
            gpr_restarts=int(os.getenv("ML_GPR_RESTARTS", "0")),
            forecast_days=int(os.getenv("ML_FORECAST_DAYS", "11")),
            bootstrap_enabled=_truthy("ML_BOOTSTRAP_ENABLED", "true"),
            bootstrap_station_limit=int(os.getenv("ML_BOOTSTRAP_STATION_LIMIT", "0")),
            bootstrap_grid_limit=int(os.getenv("ML_BOOTSTRAP_GRID_LIMIT", "0")),
        )
