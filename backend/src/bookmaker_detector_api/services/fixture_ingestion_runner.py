from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.ingestion.providers import CoversHistoricalTeamPageProvider
from bookmaker_detector_api.repositories import InMemoryIngestionRepository, PostgresIngestionRepository
from bookmaker_detector_api.services.ingestion_pipeline import (
    HistoricalIngestionRequest,
    ingest_historical_team_page,
)


FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def run_fixture_ingestion(
    *,
    repository_mode: str,
    team_code: str,
    season_label: str,
    source_url: str,
    requested_by: str,
) -> dict[str, object]:
    provider = CoversHistoricalTeamPageProvider()
    fixture_html = provider.load_fixture(FIXTURE_DIR / "covers_sample_team_page.html")
    request = HistoricalIngestionRequest(
        provider_name=provider.provider_name,
        team_code=team_code,
        season_label=season_label,
        source_url=source_url,
        requested_by=requested_by,
        html=fixture_html,
    )

    if repository_mode == "postgres":
        with postgres_connection() as connection:
            repository = PostgresIngestionRepository(connection)
            result = ingest_historical_team_page(
                request=request,
                provider=provider,
                repository=repository,
            )
    elif repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        result = ingest_historical_team_page(
            request=request,
            provider=provider,
            repository=repository,
        )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "result": asdict(result),
    }

