"""Legacy compatibility exports for ingestion-repository imports."""

from .ingestion_postgres_repository import PostgresIngestionRepository
from .ingestion_types import IngestionRepository

__all__ = [
    "IngestionRepository",
    "PostgresIngestionRepository",
]
