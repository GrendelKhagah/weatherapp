"""FastAPI dependency helpers."""

from fastapi import Request

from settings import Settings


# Dependency to get Settings from the FastAPI request
def get_settings(request: Request) -> Settings:
    """Pull Settings from FastAPI app state."""
    return request.app.state.settings
