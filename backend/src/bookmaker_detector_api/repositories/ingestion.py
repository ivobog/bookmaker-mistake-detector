"""Legacy compatibility exports for older ingestion-repository imports."""

from .ingestion_in_memory_repository import InMemoryIngestionRepository
from .ingestion_postgres_repository import PostgresIngestionRepository
from .ingestion_types import IngestionRepository

__all__ = [
    "InMemoryIngestionRepository",
    "IngestionRepository",
    "PostgresIngestionRepository",
]
