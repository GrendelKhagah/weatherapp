"""Database connection helpers for the ML service."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import psycopg2


from settings import Settings


# Connect to the PostgreSQL database using settings from the Settings object.
# Provides a context manager `db_conn` for obtaining a database connection.


def connect(settings: Settings):
    """Open a raw psycopg2 connection using Settings."""
    if settings.database_url:
        return psycopg2.connect(settings.database_url)

    return psycopg2.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
    )


@contextmanager
def db_conn(settings: Settings) -> Iterator[psycopg2.extensions.connection]:
    """Context manager that yields a DB connection and always closes it."""
    conn = connect(settings)
    try:
        yield conn
    finally:
        conn.close()
