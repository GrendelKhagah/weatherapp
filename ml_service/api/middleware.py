"""HTTP middleware utilities for the ML service."""

import datetime as dt
import logging
from fastapi import Request

log = logging.getLogger("weatherapp-ml")

# simple middleware to log incoming requests and their durations
async def request_logger(request: Request, call_next):
    """Log request method/path and total duration."""
    start = dt.datetime.now()
    response = None
    try:
        response = await call_next(request)
        return response
    finally:
        dur_ms = int((dt.datetime.now() - start).total_seconds() * 1000)
        status = getattr(response, "status_code", "?")
        # Log the request details
        log.info("%s %s -> %s (%d ms)", request.method, request.url.path, status, dur_ms)



"""
Jan 27 09:59:20 taylor-LattePanda-Sigma java[74509]: 2026-01-27 09:59:20.351 [] [] INFO  space.ketterling.api.ApiServer - Incoming GET /api/metrics/summary from 127.0.0.1
Jan 27 09:59:20 taylor-LattePanda-Sigma java[74509]: 2026-01-27 09:59:20.351 [] [] INFO  space.ketterling.api.ApiServer - Handled GET /api/metrics/external -> 200 OK (0 ms)
Jan 27 09:59:23 taylor-LattePanda-Sigma java[74509]: 2026-01-27 09:59:23.557 [] [] INFO  space.ketterling.api.ApiServer - Handled GET /api/metrics/summary -> 200 OK (3206 ms)
Jan 27 10:00:20 taylor-LattePanda-Sigma java[74509]: 2026-01-27 10:00:20.357 [] [] INFO  space.ketterling.api.ApiServer - Incoming GET /api/metrics/summary from 127.0.0.1
Jan 27 10:00:20 taylor-LattePanda-Sigma java[74509]: 2026-01-27 10:00:20.357 [] [] INFO  space.ketterling.api.ApiServer - Incoming GET /api/metrics/external from 127.0.0.1
Jan 27 10:00:20 taylor-LattePanda-Sigma java[74509]: 2026-01-27 10:00:20.357 [] [] INFO  space.ketterling.api.ApiServer - Handled GET /api/metrics/external -> 200 OK (0 ms)
Jan 27 10:00:23 taylor-LattePanda-Sigma java[74509]: 2026-01-27 10:00:23.560 [] [] INFO  space.ketterling.api.ApiServer - Handled GET /api/metrics/summary -> 200 OK (3203 ms)
"""