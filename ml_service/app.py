"""Uvicorn entrypoint for the ML service.

This module stays intentionally small so the main logic lives in readable modules.

Run with:
	python -m uvicorn app:app --host 0.0.0.0 --port 8000
"""

from api.app import app  # noqa: F401
