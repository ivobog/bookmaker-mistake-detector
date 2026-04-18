from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path

from bookmaker_detector_api.data_quality_taxonomy import canonical_severity_for_issue_type
from bookmaker_detector_api.fetching import store_parser_snapshot
from bookmaker_detector_api.ingestion.models import ParseStatus, RawGameRow
from bookmaker_detector_api.ingestion.providers.base import HistoricalTeamPageProvider
from bookmaker_detector_api.repositories import IngestionRepository
from bookmaker_detector_api.repositories.ingestion_types import (
    DataQualityIssueRecord,
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
    diagnostics: list[str] | None = None
    source_page_url: str | None = None
    persist_parser_snapshot: bool = False
    parser_snapshot_root_dir: Path | None = None


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
            "diagnostics": request.diagnostics or [],
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

    raw_rows, diagnostics = _parse_raw_rows(
        provider=provider,
        html=html,
        team_code=request.team_code,
        season_label=request.season_label,
        source_url=request.source_url,
        initial_diagnostics=request.diagnostics or [],
    )
    raw_rows = _attach_source_coordinates(
        raw_rows,
        source_page_url=request.source_page_url or request.source_url,
        source_page_season_label=request.season_label,
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
        {warning for row in raw_rows for warning in row.warnings}
        | {warning for game in canonical_games for warning in game.warnings}
    )

    parser_snapshot_path = _store_parser_snapshot_if_requested(
        request=request,
        raw_rows=raw_rows,
        diagnostics=diagnostics,
        raw_rows_saved=raw_rows_saved,
        canonical_games_saved=canonical_games_saved,
        metrics_saved=metrics_saved,
        quality_issues_saved=quality_issues_saved,
    )

    summary = {
        "raw_rows_saved": raw_rows_saved,
        "canonical_games_saved": canonical_games_saved,
        "metrics_saved": metrics_saved,
        "quality_issues_saved": quality_issues_saved,
        "payload_storage_path": request.payload_storage_path,
        "parser_snapshot_path": parser_snapshot_path,
        "warning_count": len(warnings),
        "warnings": warnings,
        "diagnostic_count": len(diagnostics),
        "diagnostics": diagnostics,
        "parser_provenance_counts": _count_parser_provenance(raw_rows),
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
        diagnostics=diagnostics,
        parser_snapshot_path=parser_snapshot_path,
    )


def _parse_raw_rows(
    *,
    provider: HistoricalTeamPageProvider,
    html: str,
    team_code: str,
    season_label: str,
    source_url: str,
    initial_diagnostics: list[str],
) -> tuple[list[RawGameRow], list[str]]:
    diagnostics = list(dict.fromkeys(initial_diagnostics))
    if not _supports_structured_provider_pipeline(provider):
        raw_rows = provider.parse_team_page(
            html=html,
            team_code=team_code,
            season_label=season_label,
            source_url=source_url,
        )
        return raw_rows, diagnostics

    season_block = provider.extract_season_block(
        page_content=html,
        season_label=season_label,
        team_main_page_url=source_url,
    )
    if season_block is None:
        diagnostics.append("season_block_missing")
        return [], _dedupe_preserve_order(diagnostics)

    selector_match = season_block.metadata.get("selector_match")
    if isinstance(selector_match, str) and selector_match.strip():
        diagnostics.append(f"season_block_selector_match:{selector_match.strip()}")

    provider_rows = provider.extract_regular_season_rows(season_block=season_block)
    if not provider_rows:
        diagnostics.append("regular_season_rows_missing")

    raw_rows = [
        provider.normalize_row(
            raw_row=provider.parse_row(
                raw_row=provider_row,
                row_index=row_index,
                team_code=team_code,
                season_label=season_label,
                source_url=source_url,
                source_section=str(provider_row.metadata.get("source_section", "Regular Season")),
            )
        )
        for row_index, provider_row in enumerate(provider_rows, start=1)
    ]
    diagnostics.extend(_build_parser_provenance_diagnostics(raw_rows))
    return raw_rows, _dedupe_preserve_order(diagnostics)


def _supports_structured_provider_pipeline(provider: HistoricalTeamPageProvider) -> bool:
    return all(
        callable(getattr(provider, attribute_name, None))
        for attribute_name in (
            "extract_season_block",
            "extract_regular_season_rows",
            "parse_row",
            "normalize_row",
        )
    )


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _attach_source_coordinates(
    raw_rows: list[RawGameRow],
    *,
    source_page_url: str,
    source_page_season_label: str,
) -> list[RawGameRow]:
    return [
        replace(
            row,
            source_page_url=row.source_page_url or source_page_url,
            source_page_season_label=row.source_page_season_label or source_page_season_label,
        )
        for row in raw_rows
    ]


def _store_parser_snapshot_if_requested(
    *,
    request: HistoricalIngestionRequest,
    raw_rows: list[RawGameRow],
    diagnostics: list[str],
    raw_rows_saved: int,
    canonical_games_saved: int,
    metrics_saved: int,
    quality_issues_saved: int,
) -> str | None:
    if not request.persist_parser_snapshot or request.parser_snapshot_root_dir is None:
        return None
    payload = {
        "provider_name": request.provider_name,
        "team_code": request.team_code,
        "season_label": request.season_label,
        "source_url": request.source_url,
        "source_page_url": request.source_page_url or request.source_url,
        "diagnostics": diagnostics,
        "summary": {
            "raw_rows_saved": raw_rows_saved,
            "canonical_games_saved": canonical_games_saved,
            "metrics_saved": metrics_saved,
            "quality_issues_saved": quality_issues_saved,
            "parser_provenance_counts": _count_parser_provenance(raw_rows),
            "parse_status_counts": _count_parse_statuses(raw_rows),
        },
        "raw_rows": [row.as_dict() for row in raw_rows],
    }
    snapshot_path = store_parser_snapshot(
        root_dir=request.parser_snapshot_root_dir,
        provider_name=request.provider_name,
        team_code=request.team_code,
        season_label=request.season_label,
        source_url=request.source_page_url or request.source_url,
        payload=payload,
    )
    return str(snapshot_path)


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


def _count_parser_provenance(raw_rows: list[RawGameRow]) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = {}
    for row in raw_rows:
        opponent_resolution = row.parser_provenance.get("opponent_resolution", {})
        if isinstance(opponent_resolution, dict):
            _increment_nested_count(
                counts,
                "opponent_resolution_mode",
                str(opponent_resolution.get("mode", "unknown")),
            )
        _increment_nested_count(
            counts,
            "ats_parse_mode",
            str(row.parser_provenance.get("ats_parse_mode", "unknown")),
        )
        _increment_nested_count(
            counts,
            "ou_parse_mode",
            str(row.parser_provenance.get("ou_parse_mode", "unknown")),
        )
    return {
        category: {
            mode: count
            for mode, count in category_counts.items()
            if mode not in {"unknown", "missing"} or count > 0
        }
        for category, category_counts in counts.items()
    }


def _increment_nested_count(
    counts: dict[str, dict[str, int]],
    category: str,
    mode: str,
) -> None:
    category_counts = counts.setdefault(category, {})
    category_counts[mode] = category_counts.get(mode, 0) + 1


def _build_parser_provenance_diagnostics(raw_rows: list[RawGameRow]) -> list[str]:
    provenance_counts = _count_parser_provenance(raw_rows)
    diagnostics: list[str] = []
    for category, category_counts in provenance_counts.items():
        for mode, count in sorted(category_counts.items()):
            if mode in {"direct_code", "full", "missing"}:
                continue
            diagnostics.append(f"{category}:{mode}={count}")
    return diagnostics


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
