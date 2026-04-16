from __future__ import annotations

from dataclasses import dataclass

from bookmaker_detector_api.data_quality_taxonomy import canonical_severity_for_issue_type
from bookmaker_detector_api.ingestion.models import ParseStatus
from bookmaker_detector_api.ingestion.providers.base import HistoricalTeamPageProvider
from bookmaker_detector_api.repositories.ingestion import (
    DataQualityIssueRecord,
    IngestionRepository,
    PageRetrievalRecord,
    PersistedIngestionRun,
)
from bookmaker_detector_api.services.canonical import canonicalize_rows
from bookmaker_detector_api.services.metrics import calculate_game_metric


@dataclass(slots=True)
class HistoricalIngestionRequest:
    provider_name: str
    team_code: str
    season_label: str
    source_url: str
    requested_by: str = "system"
    run_label: str | None = None
    html: str | None = None
    retrieval_status: str = "SUCCESS"
    retrieval_http_status: int | None = 200
    payload_storage_path: str | None = None


def ingest_historical_team_page(
    *,
    request: HistoricalIngestionRequest,
    provider: HistoricalTeamPageProvider,
    repository: IngestionRepository,
) -> PersistedIngestionRun:
    html = request.html
    if html is None:
        raise ValueError("Historical ingestion currently requires HTML content.")

    job_id = repository.create_job_run(
        job_name="historical_team_page_ingestion",
        requested_by=request.requested_by,
        payload={
            "provider": request.provider_name,
            "team_code": request.team_code,
            "season_label": request.season_label,
            "source_url": request.source_url,
            "run_label": request.run_label,
        },
    )

    page_retrieval_id = repository.create_page_retrieval(
        job_id=job_id,
        record=PageRetrievalRecord(
            provider_name=request.provider_name,
            team_code=request.team_code,
            season_label=request.season_label,
            source_url=request.source_url,
            status=request.retrieval_status,
            http_status=request.retrieval_http_status,
            payload_storage_path=request.payload_storage_path,
        ),
    )

    raw_rows = provider.parse_team_page(
        html=html,
        team_code=request.team_code,
        season_label=request.season_label,
        source_url=request.source_url,
    )
    canonical_source_rows = [row for row in raw_rows if row.parse_status != ParseStatus.INVALID]
    canonical_games = canonicalize_rows(canonical_source_rows)

    persisted_raw_rows = repository.save_raw_rows(
        page_retrieval_id=page_retrieval_id,
        rows=raw_rows,
    )
    persisted_canonical_games = repository.save_canonical_games(canonical_games)
    metrics_saved = repository.save_game_metrics(
        [
            (persisted_game.canonical_game_id, calculate_game_metric(persisted_game.game))
            for persisted_game in persisted_canonical_games
        ]
    )
    data_quality_issues = _build_data_quality_issues(
        persisted_raw_rows=persisted_raw_rows,
        persisted_canonical_games=persisted_canonical_games,
    )
    quality_issues_saved = repository.save_data_quality_issues(data_quality_issues)
    raw_rows_saved = len(persisted_raw_rows)
    canonical_games_saved = len(persisted_canonical_games)

    warnings = sorted(
        {
            warning
            for row in raw_rows
            for warning in row.warnings
        }
        | {
            warning
            for game in canonical_games
            for warning in game.warnings
        }
    )

    summary = {
        "raw_rows_saved": raw_rows_saved,
        "canonical_games_saved": canonical_games_saved,
        "metrics_saved": metrics_saved,
        "quality_issues_saved": quality_issues_saved,
        "warning_count": len(warnings),
        "warnings": warnings,
        "parse_status_counts": _count_parse_statuses(raw_rows),
        "reconciliation_status_counts": _count_reconciliation_statuses(canonical_games),
        "data_quality_issue_type_counts": _count_issue_types(data_quality_issues),
        "data_quality_issue_severity_counts": _count_issue_severities(data_quality_issues),
    }
    repository.complete_job_run(job_id=job_id, summary=summary, status="COMPLETED")

    return PersistedIngestionRun(
        job_id=job_id,
        page_retrieval_id=page_retrieval_id,
        raw_rows_saved=raw_rows_saved,
        canonical_games_saved=canonical_games_saved,
        metrics_saved=metrics_saved,
        warnings=warnings,
    )


def _build_data_quality_issues(
    *,
    persisted_raw_rows,
    persisted_canonical_games,
) -> list[DataQualityIssueRecord]:
    issues: list[DataQualityIssueRecord] = []
    for persisted_row in persisted_raw_rows:
        for warning in persisted_row.row.warnings:
            issues.append(
                DataQualityIssueRecord(
                    id=0,
                    issue_type=warning,
                    severity=_severity_for_issue_type(warning),
                    raw_team_game_row_id=persisted_row.raw_row_id,
                    canonical_game_id=None,
                    details={
                        "team_code": persisted_row.row.team_code,
                        "season_label": persisted_row.row.season_label,
                        "source_row_index": persisted_row.row.source_row_index,
                    },
                )
            )
    for persisted_game in persisted_canonical_games:
        for warning in persisted_game.game.warnings:
            issues.append(
                DataQualityIssueRecord(
                    id=0,
                    issue_type=warning,
                    severity=_severity_for_issue_type(warning),
                    raw_team_game_row_id=None,
                    canonical_game_id=persisted_game.canonical_game_id,
                    details={
                        "home_team_code": persisted_game.game.home_team_code,
                        "away_team_code": persisted_game.game.away_team_code,
                        "season_label": persisted_game.game.season_label,
                    },
                )
            )
    return issues


def _severity_for_issue_type(issue_type: str) -> str:
    return canonical_severity_for_issue_type(issue_type)


def _count_parse_statuses(raw_rows) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in raw_rows:
        parse_status = row.parse_status.value
        counts[parse_status] = counts.get(parse_status, 0) + 1
    return counts


def _count_reconciliation_statuses(canonical_games) -> dict[str, int]:
    counts: dict[str, int] = {}
    for game in canonical_games:
        reconciliation_status = game.reconciliation_status.value
        counts[reconciliation_status] = counts.get(reconciliation_status, 0) + 1
    return counts


def _count_issue_types(issues: list[DataQualityIssueRecord]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for issue in issues:
        counts[issue.issue_type] = counts.get(issue.issue_type, 0) + 1
    return counts


def _count_issue_severities(issues: list[DataQualityIssueRecord]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for issue in issues:
        counts[issue.severity] = counts.get(issue.severity, 0) + 1
    return counts
