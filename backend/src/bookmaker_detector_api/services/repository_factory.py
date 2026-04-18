from __future__ import annotations

from bookmaker_detector_api.config import settings
from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.repositories import (
    FeatureDatasetStore,
    IngestionRepository,
    InMemoryIngestionRepository,
    PhaseThreeModelingStore,
    PostgresIngestionRepository,
)


def build_ingestion_repository(repository_mode: str) -> tuple[IngestionRepository, object | None]:
    if repository_mode == "postgres":
        connection_context = postgres_connection()
        connection = connection_context.__enter__()
        return (
            PostgresIngestionRepository(
                connection,
                allow_runtime_schema_mutation=settings.resolved_postgres_allow_runtime_schema_mutation,
            ),
            connection_context,
        )
    if repository_mode == "in_memory":
        return InMemoryIngestionRepository(), None
    raise ValueError(f"Unsupported repository mode: {repository_mode}")


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


def build_in_memory_feature_dataset_store() -> FeatureDatasetStore:
    return InMemoryIngestionRepository()


def build_in_memory_phase_three_modeling_store() -> PhaseThreeModelingStore:
    return InMemoryIngestionRepository()
