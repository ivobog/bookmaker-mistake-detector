from __future__ import annotations

from pathlib import Path

from bookmaker_detector_api.ingestion.providers import CoversHistoricalTeamPageProvider
from bookmaker_detector_api.repositories import PostgresIngestionRepository
from bookmaker_detector_api.services.features import (
    materialize_baseline_feature_snapshots_for_postgres,
)
from bookmaker_detector_api.services.ingestion_pipeline import (
    HistoricalIngestionRequest,
    ingest_historical_team_page,
)
from bookmaker_detector_api.services.repository_factory import (
    build_bootstrap_postgres_ingestion_repository,
)

_FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def seed_phase_two_feature_postgres(
    connection: object,
) -> tuple[PostgresIngestionRepository, object, dict[str, object]]:
    provider = CoversHistoricalTeamPageProvider()
    fixture_html = provider.load_fixture(_FIXTURE_DIR / "covers_sample_team_page.html")
    repository = build_bootstrap_postgres_ingestion_repository(connection)
    ingest_result = ingest_historical_team_page(
        request=HistoricalIngestionRequest(
            provider_name=provider.provider_name,
            team_code="LAL",
            season_label="2024-2025",
            source_url="https://example.com/covers/lal/2024-2025",
            requested_by="phase-2-feature-demo",
            html=fixture_html,
        ),
        provider=provider,
        repository=repository,
    )
    feature_result = materialize_baseline_feature_snapshots_for_postgres(connection)
    return repository, ingest_result, feature_result
