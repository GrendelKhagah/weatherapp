"""Logging configuration helpers for the ML service."""

import logging
import logging.config
from typing import Optional

from settings import Settings

# Configure logging based on settings

def configure_logging(settings: Settings) -> None:
    """Configure root logging based on Settings."""
    # Keep it simple, standard logging but centrally configured
    logging.basicConfig(level=settings.log_level, format=settings.log_format)


def get_logger(name: str, settings: Optional[Settings] = None) -> logging.Logger:
    """Get a named logger, optionally reconfiguring first."""
    # Optional convenience to adjust per-module levels
    if settings is not None:
        configure_logging(settings)
    return logging.getLogger(name)
