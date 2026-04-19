from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.ingestion.providers import CoversHistoricalTeamPageProvider
from bookmaker_detector_api.repositories import (
    InMemoryIngestionRepository,
)
from bookmaker_detector_api.services.ingestion_pipeline import (
    HistoricalIngestionRequest,
    ingest_historical_team_page,
)
from bookmaker_detector_api.services.repository_factory import (
    build_bootstrap_postgres_ingestion_repository,
)
from bookmaker_detector_api.services.workflow_logging import start_workflow_span

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures"
FIXTURE_BY_TEAM_CODE: dict[str, str] = {
    "CHI": "covers_team_page_chi_2024_2025.html",
    "DAL": "covers_team_page_dal_2024_2025.html",
    "LAL": "covers_sample_team_page.html",
    "PHX": "covers_team_page_phx_2024_2025.html",
    "NYK": "covers_team_page_nyk_2023_2024.html",
}


def run_fixture_ingestion(
    *,
    repository_mode: str,
    team_code: str,
    season_label: str,
    source_url: str,
    requested_by: str,
) -> dict[str, object]:
    provider = CoversHistoricalTeamPageProvider()
    fixture_name = FIXTURE_BY_TEAM_CODE.get(team_code.upper(), "covers_sample_team_page.html")
    fixture_html = provider.load_fixture(FIXTURE_DIR / fixture_name)
    span = start_workflow_span(
        workflow_name="ingestion.fixture_ingestion",
        repository_mode=repository_mode,
        provider_name=provider.provider_name,
        team_code=team_code,
        season_label=season_label,
        source_url=source_url,
    )
    request = HistoricalIngestionRequest(
        provider_name=provider.provider_name,
        team_code=team_code,
        season_label=season_label,
        source_url=source_url,
        source_page_url=source_url,
        requested_by=requested_by,
        html=fixture_html,
    )

    try:
        if repository_mode == "postgres":
            with postgres_connection() as connection:
                repository = build_bootstrap_postgres_ingestion_repository(connection)
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
    except Exception as exc:
        span.failure(exc)
        raise

    response = {
        "repository_mode": repository_mode,
        "result": asdict(result),
    }
    span.success(
        job_id=response["result"]["job_id"],
        page_retrieval_id=response["result"]["page_retrieval_id"],
        raw_rows_saved=response["result"]["raw_rows_saved"],
        canonical_games_saved=response["result"]["canonical_games_saved"],
        metrics_saved=response["result"]["metrics_saved"],
    )
    return response
