from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from bookmaker_detector_api.ingestion.providers import CoversHistoricalTeamPageProvider
from bookmaker_detector_api.repositories import InMemoryIngestionRepository
from bookmaker_detector_api.services.admin_diagnostics import get_admin_diagnostics
from bookmaker_detector_api.services.fetch_ingestion_runner import run_fetch_and_ingest
from bookmaker_detector_api.services.fixture_ingestion_runner import run_fixture_ingestion
from bookmaker_detector_api.services.ingestion_pipeline import (
    HistoricalIngestionRequest,
    ingest_historical_team_page,
)
from bookmaker_detector_api.services.canonical import canonicalize_rows
from bookmaker_detector_api.services.metrics import calculate_game_metric


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"


def run_phase_one_demo() -> dict[str, object]:
    provider = CoversHistoricalTeamPageProvider()
    fixture_html = provider.load_fixture(FIXTURE_DIR / "covers_sample_team_page.html")
    raw_rows = provider.parse_team_page(
        html=fixture_html,
        team_code="LAL",
        season_label="2024-2025",
        source_url="https://example.com/covers/lal/2024-2025",
    )
    canonical_games = canonicalize_rows(raw_rows)

    return {
        "provider": provider.provider_name,
        "raw_row_count": len(raw_rows),
        "canonical_game_count": len(canonical_games),
        "raw_rows": [row.as_dict() for row in raw_rows],
        "canonical_games": [
            {
                **game.as_dict(),
                "metrics": calculate_game_metric(game).as_dict(),
            }
            for game in canonical_games
        ],
    }


def run_phase_one_persistence_demo() -> dict[str, object]:
    provider = CoversHistoricalTeamPageProvider()
    repository = InMemoryIngestionRepository()
    fixture_html = provider.load_fixture(FIXTURE_DIR / "covers_sample_team_page.html")

    result = ingest_historical_team_page(
        request=HistoricalIngestionRequest(
            provider_name=provider.provider_name,
            team_code="LAL",
            season_label="2024-2025",
            source_url="https://example.com/covers/lal/2024-2025",
            requested_by="phase-1-demo",
            html=fixture_html,
        ),
        provider=provider,
        repository=repository,
    )

    return {
        "job_id": result.job_id,
        "page_retrieval_id": result.page_retrieval_id,
        "raw_rows_saved": result.raw_rows_saved,
        "canonical_games_saved": result.canonical_games_saved,
        "metrics_saved": result.metrics_saved,
        "warnings": result.warnings,
        "job_runs": repository.job_runs,
        "page_retrievals": [
            {
                **entry,
                "record": {
                    "provider_name": entry["record"].provider_name,
                    "team_code": entry["record"].team_code,
                    "season_label": entry["record"].season_label,
                    "source_url": entry["record"].source_url,
                    "status": entry["record"].status,
                },
            }
            for entry in repository.page_retrievals
        ],
    }


def run_phase_one_worker_demo() -> dict[str, object]:
    return run_fixture_ingestion(
        repository_mode="in_memory",
        team_code="LAL",
        season_label="2024-2025",
        source_url="https://example.com/covers/lal/2024-2025",
        requested_by="phase-1-worker-demo",
    )


def run_phase_one_fetch_demo() -> dict[str, object]:
    fixture_url = (FIXTURE_DIR / "covers_sample_team_page.html").resolve().as_uri()
    return run_fetch_and_ingest(
        repository_mode="in_memory",
        team_code="LAL",
        season_label="2024-2025",
        source_url=fixture_url,
        requested_by="phase-1-fetch-demo",
        persist_payload=True,
    )


def run_phase_one_fetch_failure_demo() -> dict[str, object]:
    missing_fixture_url = (FIXTURE_DIR / "missing_team_page.html").resolve().as_uri()
    return run_fetch_and_ingest(
        repository_mode="in_memory",
        team_code="LAL",
        season_label="2024-2025",
        source_url=missing_fixture_url,
        requested_by="phase-1-fetch-failure-demo",
        persist_payload=True,
    )


def run_phase_one_fetch_reporting_demo(*, repository_mode: str = "in_memory") -> dict[str, object]:
    fixture_url = (FIXTURE_DIR / "covers_sample_team_page.html").resolve().as_uri()
    window_start = datetime.now(timezone.utc)
    run_label = "phase-1-fetch-reporting-demo"
    ingestion_source_url = _build_validation_ingestion_source_url(
        fixture_url=fixture_url,
        run_label=run_label,
        started_at=window_start,
    )

    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        fetch_result = run_fetch_and_ingest(
            repository_mode=repository_mode,
            team_code="LAL",
            season_label="2024-2025",
            source_url=fixture_url,
            ingestion_source_url=ingestion_source_url,
            requested_by="phase-1-fetch-reporting-demo",
            run_label=run_label,
            persist_payload=True,
            repository_override=repository,
        )
        diagnostics = get_admin_diagnostics(
            repository_mode=repository_mode,
            seed_demo=False,
            repository_override=repository,
            provider_name="covers",
            team_code="LAL",
            season_label="2024-2025",
            run_label=run_label,
            started_from=window_start,
        )
    else:
        fetch_result = run_fetch_and_ingest(
            repository_mode=repository_mode,
            team_code="LAL",
            season_label="2024-2025",
            source_url=fixture_url,
            ingestion_source_url=ingestion_source_url,
            requested_by="phase-1-fetch-reporting-demo",
            run_label=run_label,
            persist_payload=True,
        )
        diagnostics = get_admin_diagnostics(
            repository_mode=repository_mode,
            seed_demo=False,
            provider_name="covers",
            team_code="LAL",
            season_label="2024-2025",
            run_label=run_label,
            started_from=window_start,
        )

    return {
        "repository_mode": repository_mode,
        "fetch_result": fetch_result,
        "retrieval_trends": diagnostics["retrieval_trends"],
        "quality_trends": diagnostics["quality_trends"],
        "jobs": diagnostics["job_runs"],
        "page_retrievals": diagnostics["page_retrievals"],
    }


def _build_validation_ingestion_source_url(
    *,
    fixture_url: str,
    run_label: str,
    started_at: datetime,
) -> str:
    timestamp = started_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{fixture_url}#validation_run={run_label}:{timestamp}"
