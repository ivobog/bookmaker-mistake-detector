from __future__ import annotations

from bookmaker_detector_api.services.repository_factory import (
    build_postgres_ingestion_repository,
)


def normalize_data_quality_taxonomy(
    *,
    provider_name: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    dry_run: bool = True,
) -> dict[str, object]:
    repository, repository_context = build_postgres_ingestion_repository()
    try:
        normalization = repository.normalize_data_quality_issue_taxonomy(
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            dry_run=dry_run,
        )
        issue_type_counts = repository.get_data_quality_issue_type_counts(
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
        )
        severity_counts = repository.get_data_quality_issue_severity_counts(
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
        )
        return {
            "dry_run": dry_run,
            "filters": {
                "provider_name": provider_name,
                "team_code": team_code,
                "season_label": season_label,
            },
            "normalization": normalization,
            "stats": {
                "data_quality_issue_type_counts": issue_type_counts,
                "data_quality_issue_severity_counts": severity_counts,
            },
        }
    finally:
        if repository_context is not None:
            repository_context.__exit__(None, None, None)
