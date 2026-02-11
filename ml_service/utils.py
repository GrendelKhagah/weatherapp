"""Small utility helpers used across the ML service."""

import datetime as dt


def latlon_valid(lat: float, lon: float) -> bool:
    """Return True if lat/lon are within valid ranges."""
    try:
        return -90.0 <= float(lat) <= 90.0 and -180.0 <= float(lon) <= 180.0
    except Exception:
        return False


def clamp_int(value: int, min_value: int, max_value: int) -> int:
    """Clamp an integer to the inclusive range [min_value, max_value]."""
    return max(min_value, min(max_value, int(value)))


def as_of_window(years: int = 2) -> dt.date:
    """Return a date window start N years before today."""
    return dt.date.today() - dt.timedelta(days=365 * years)
