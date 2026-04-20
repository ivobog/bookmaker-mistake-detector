from __future__ import annotations

from bookmaker_detector_api.config import settings
from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.repositories import (
    PostgresIngestionRepository,
)


def build_postgres_ingestion_repository() -> tuple[PostgresIngestionRepository, object]:
    connection_context = postgres_connection()
    connection = connection_context.__enter__()
    return (
        PostgresIngestionRepository(
            connection,
            allow_runtime_schema_mutation=settings.resolved_postgres_allow_runtime_schema_mutation,
        ),
        connection_context,
    )


def build_bootstrap_postgres_ingestion_repository(
    connection: object,
) -> PostgresIngestionRepository:
    try:
        return PostgresIngestionRepository(
            connection,
            allow_runtime_schema_mutation=True,
        )
    except TypeError:
        return PostgresIngestionRepository(connection)
