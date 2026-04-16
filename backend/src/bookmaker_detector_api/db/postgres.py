from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from bookmaker_detector_api.config import settings


@contextmanager
def postgres_connection() -> Iterator[Any]:
    # Imported lazily so unit tests do not require the driver.
    import psycopg

    with psycopg.connect(settings.database_url) as connection:
        yield connection

