from __future__ import annotations

from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.repositories import InMemoryIngestionRepository, PostgresIngestionRepository
from bookmaker_detector_api.repositories.ingestion import IngestionRepository


def build_ingestion_repository(repository_mode: str) -> tuple[IngestionRepository, object | None]:
    if repository_mode == "postgres":
        connection_context = postgres_connection()
        connection = connection_context.__enter__()
        return PostgresIngestionRepository(connection), connection_context
    if repository_mode == "in_memory":
        return InMemoryIngestionRepository(), None
    raise ValueError(f"Unsupported repository mode: {repository_mode}")

