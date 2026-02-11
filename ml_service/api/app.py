"""FastAPI application factory for the ML service."""

import logging
import threading

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.errors import install_error_handlers
from api.middleware import request_logger
from api.routes import router
from db.schema import ensure_tables
from logging_setup import configure_logging
from ml.bootstrap import bootstrap_forecasts
from settings import Settings


def create_app() -> FastAPI:
    """Create and configure the FastAPI app instance."""
    settings = Settings.load()
    configure_logging(settings)

    log = logging.getLogger("weatherapp-ml")

    app = FastAPI(title="WeatherApp ML", version="0.1.0")
    app.state.settings = settings

    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Request logging
    app.middleware("http")(request_logger)

    # Error handler
    install_error_handlers(app, settings)

    app.include_router(router)

    @app.on_event("startup")
    def _startup():
        """Initialize DB tables and kick off forecast bootstrap thread."""
        ensure_tables(settings)
        log.info("ML service startup complete")
        thread = threading.Thread(target=bootstrap_forecasts, args=(settings,), daemon=True)
        thread.start()

    return app


app = create_app()
