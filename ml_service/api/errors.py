"""Error handlers for the ML FastAPI app."""

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from settings import Settings

log = logging.getLogger("weatherapp-ml")

def install_error_handlers(app: FastAPI, settings: Settings) -> None:
    """Register a global exception handler for unexpected errors."""
    @app.exception_handler(Exception)
    # Generic handler for unhandled exceptions
    async def unhandled_exception_handler(request: Request, exc: Exception):
        """Return a safe error payload, optionally with debug details."""
        log.exception("Unhandled error on %s %s", request.method, request.url.path)
        payload = {"error": "internal_error"}
        if settings.debug_errors:
            payload["message"] = str(exc)
        return JSONResponse(status_code=500, content=payload)
