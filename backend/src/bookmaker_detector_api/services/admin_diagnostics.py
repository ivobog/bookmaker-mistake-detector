from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from bookmaker_detector_api.ingestion.models import ParseStatus, RawGameRow
from bookmaker_detector_api.repositories import InMemoryIngestionRepository
from bookmaker_detector_api.repositories.ingestion import IngestionRepository
from bookmaker_detector_api.services.fetch_ingestion_runner import run_fetch_and_ingest
from bookmaker_detector_api.services.ingestion_pipeline import (
    HistoricalIngestionRequest,
    ingest_historical_team_page,
)
from bookmaker_detector_api.services.repository_factory import build_ingestion_repository


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def get_admin_diagnostics(
    *,
    repository_mode: str,
    seed_demo: bool,
    repository_override: IngestionRepository | None = None,
    job_limit: int = 20,
    retrieval_limit: int = 20,
    job_offset: int = 0,
    retrieval_offset: int = 0,
    retrieval_status: str | None = None,
    quality_issue_limit: int = 20,
    quality_issue_offset: int = 0,
    quality_issue_severity: str | None = None,
    quality_issue_type: str | None = None,
    provider_name: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    run_label: str | None = None,
    job_status: str | None = None,
    trend_limit: int = 20,
    validation_compare_limit: int = 10,
    started_from: datetime | None = None,
    started_to: datetime | None = None,
) -> dict[str, object]:
    if repository_override is not None:
        repository = repository_override
        repository_context = None
    else:
        repository, repository_context = build_ingestion_repository(repository_mode)
    try:
        if seed_demo:
            _seed_in_memory_demo_data(repository)

        job_runs = repository.list_job_runs(
            limit=job_limit,
            offset=job_offset,
            status=job_status,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
            started_from=started_from,
            started_to=started_to,
        )
        retrievals = repository.list_page_retrievals(
            limit=retrieval_limit,
            offset=retrieval_offset,
            status=retrieval_status,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
        )
        quality_issues = repository.list_data_quality_issues(
            limit=quality_issue_limit,
            offset=quality_issue_offset,
            severity=quality_issue_severity,
            issue_type=quality_issue_type,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
        )
        trend_job_runs = repository.list_job_runs(
            limit=trend_limit,
            offset=0,
            status=job_status,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
            started_from=started_from,
            started_to=started_to,
        )
        validation_compare_runs = repository.list_job_runs(
            limit=validation_compare_limit,
            offset=0,
            status=job_status,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
            started_from=started_from,
            started_to=started_to,
        )
        trend_daily_summaries = repository.list_job_run_daily_summaries(
            status=job_status,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
            started_from=started_from,
            started_to=started_to,
        )
        retrieval_trend_daily_summaries = repository.list_page_retrieval_daily_summaries(
            status=retrieval_status,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
            retrieved_from=started_from,
            retrieved_to=started_to,
        )
        quality_trend_daily_summaries = repository.list_job_run_quality_daily_summaries(
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
            started_from=started_from,
            started_to=started_to,
        )

        return {
            "repository_mode": repository_mode,
            "filters": {
                "provider_name": provider_name,
                "team_code": team_code,
                "season_label": season_label,
                "run_label": run_label,
                "job_status": job_status,
                "started_from": started_from,
                "started_to": started_to,
                "retrieval_status": retrieval_status,
                "quality_issue_severity": quality_issue_severity,
                "quality_issue_type": quality_issue_type,
            },
            "job_runs": [asdict(job_run) for job_run in job_runs],
            "page_retrievals": [asdict(retrieval) for retrieval in retrievals],
            "data_quality_issues": [asdict(issue) for issue in quality_issues],
            "stats": {
                "parse_status_counts": repository.get_parse_status_counts(
                    provider_name=provider_name,
                    team_code=team_code,
                    season_label=season_label,
                    run_label=run_label,
                ),
                "reconciliation_status_counts": repository.get_reconciliation_status_counts(
                    provider_name=provider_name,
                    team_code=team_code,
                    season_label=season_label,
                    run_label=run_label,
                ),
                "data_quality_issue_type_counts": repository.get_data_quality_issue_type_counts(
                    provider_name=provider_name,
                    team_code=team_code,
                    season_label=season_label,
                    run_label=run_label,
                ),
                "data_quality_issue_severity_counts": (
                    repository.get_data_quality_issue_severity_counts(
                        provider_name=provider_name,
                        team_code=team_code,
                        season_label=season_label,
                        run_label=run_label,
                    )
                ),
            },
            "trends": _build_run_trends(
                trend_job_runs=trend_job_runs,
                trend_daily_summaries=trend_daily_summaries,
            ),
            "validation_run_comparison": _build_validation_run_comparison(
                validation_compare_runs,
                run_label=run_label,
            ),
            "retrieval_trends": _build_retrieval_trends(retrieval_trend_daily_summaries),
            "quality_trends": _build_quality_trends(quality_trend_daily_summaries),
        }
    finally:
        if repository_context is not None:
            repository_context.__exit__(None, None, None)


def _seed_in_memory_demo_data(repository: IngestionRepository) -> None:
    if not isinstance(repository, InMemoryIngestionRepository):
        return

    fixture_path = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "covers_sample_team_page.html"
    ).resolve()
    run_fetch_and_ingest(
        repository_mode="in_memory",
        team_code="LAL",
        season_label="2024-2025",
        source_url=fixture_path.as_uri(),
        requested_by="admin-diagnostics-seed-success",
        persist_payload=False,
        repository_override=repository,
    )
    _shift_latest_in_memory_run(repository, days_ago=3)
    run_fetch_and_ingest(
        repository_mode="in_memory",
        team_code="NYK",
        season_label="2023-2024",
        source_url=fixture_path.as_uri(),
        requested_by="admin-diagnostics-seed-alt-scope",
        persist_payload=False,
        repository_override=repository,
    )
    _shift_latest_in_memory_run(repository, days_ago=2)
    _seed_invalid_parse_demo(repository)
    _seed_canonical_conflict_demo(repository)


def _seed_invalid_parse_demo(repository: IngestionRepository) -> None:
    class InvalidParseProvider:
        provider_name = "covers"

        def parse_team_page(self, **kwargs) -> list[RawGameRow]:
            return [
                RawGameRow(
                    provider_name="covers",
                    team_code="LAL",
                    season_label="2024-2025",
                    source_url="https://example.com/covers/lal/invalid-parse",
                    source_section="Regular Season",
                    source_row_index=1,
                    game_date=date(2024, 11, 7),
                    opponent_code="BOS",
                    is_away=False,
                    result_flag="W",
                    team_score=0,
                    opponent_score=0,
                    ats_result=None,
                    ats_line=None,
                    ou_result=None,
                    total_line=None,
                    parse_status=ParseStatus.INVALID,
                    warnings=["parse.invalid_score_format"],
                )
            ]

    ingest_historical_team_page(
        request=HistoricalIngestionRequest(
            provider_name="covers",
            team_code="LAL",
            season_label="2024-2025",
            source_url="https://example.com/covers/lal/invalid-parse",
            requested_by="admin-diagnostics-seed-invalid-parse",
            html="<html></html>",
        ),
        provider=InvalidParseProvider(),
        repository=repository,
    )
    _shift_latest_in_memory_run(repository, days_ago=1)


def _seed_canonical_conflict_demo(repository: IngestionRepository) -> None:
    class ConflictProvider:
        provider_name = "covers"

        def parse_team_page(self, **kwargs) -> list[RawGameRow]:
            return [
                RawGameRow(
                    provider_name="covers",
                    team_code="LAL",
                    season_label="2024-2025",
                    source_url="https://example.com/covers/lal/conflict",
                    source_section="Regular Season",
                    source_row_index=1,
                    game_date=date(2024, 11, 8),
                    opponent_code="BOS",
                    is_away=False,
                    result_flag="W",
                    team_score=112,
                    opponent_score=104,
                    ats_result="W",
                    ats_line=-3.5,
                    ou_result="O",
                    total_line=214.5,
                    parse_status=ParseStatus.VALID,
                ),
                RawGameRow(
                    provider_name="covers",
                    team_code="BOS",
                    season_label="2024-2025",
                    source_url="https://example.com/covers/bos/conflict",
                    source_section="Regular Season",
                    source_row_index=2,
                    game_date=date(2024, 11, 8),
                    opponent_code="@LAL",
                    is_away=True,
                    result_flag="L",
                    team_score=103,
                    opponent_score=112,
                    ats_result="L",
                    ats_line=3.5,
                    ou_result="O",
                    total_line=214.5,
                    parse_status=ParseStatus.VALID,
                ),
            ]

    ingest_historical_team_page(
        request=HistoricalIngestionRequest(
            provider_name="covers",
            team_code="LAL",
            season_label="2024-2025",
            source_url="https://example.com/covers/lal/conflict",
            requested_by="admin-diagnostics-seed-conflict",
            html="<html></html>",
        ),
        provider=ConflictProvider(),
        repository=repository,
    )
    missing_path = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "missing_team_page.html"
    ).resolve()
    run_fetch_and_ingest(
        repository_mode="in_memory",
        team_code="LAL",
        season_label="2024-2025",
        source_url=missing_path.as_uri(),
        requested_by="admin-diagnostics-seed-failure",
        persist_payload=False,
        repository_override=repository,
    )
    _shift_latest_in_memory_run(repository, days_ago=0)


def _build_run_trends(*, trend_job_runs, trend_daily_summaries) -> dict[str, object]:
    recent_runs = [_build_recent_run_summary(job_run) for job_run in trend_job_runs]
    return {
        "overview": _build_trend_overview(trend_daily_summaries),
        "daily_buckets": [asdict(summary) for summary in trend_daily_summaries],
        "recent_runs": recent_runs,
    }


def _build_validation_run_comparison(
    validation_compare_runs,
    *,
    run_label: str | None,
) -> dict[str, object]:
    recent_runs = [_build_recent_run_summary(job_run) for job_run in validation_compare_runs]
    latest_run = recent_runs[0] if recent_runs else None
    previous_run = recent_runs[1] if len(recent_runs) > 1 else None

    return {
        "run_label": run_label,
        "run_count": len(recent_runs),
        "latest_run": latest_run,
        "previous_run": previous_run,
        "latest_vs_previous": _build_run_delta(latest_run, previous_run),
        "recent_runs": recent_runs,
    }


def _build_retrieval_trends(trend_daily_summaries) -> dict[str, object]:
    retrieval_count = sum(summary.retrieval_count for summary in trend_daily_summaries)
    successful_retrievals = sum(summary.successful_retrievals for summary in trend_daily_summaries)
    failed_retrievals = sum(summary.failed_retrievals for summary in trend_daily_summaries)
    payload_saved_count = sum(summary.payload_saved_count for summary in trend_daily_summaries)
    missing_http_status_count = sum(
        summary.missing_http_status_count for summary in trend_daily_summaries
    )
    return {
        "overview": {
            "retrieval_count": retrieval_count,
            "successful_retrievals": successful_retrievals,
            "failed_retrievals": failed_retrievals,
            "success_rate": round((successful_retrievals / retrieval_count), 4)
            if retrieval_count
            else 0.0,
            "payload_saved_count": payload_saved_count,
            "missing_http_status_count": missing_http_status_count,
        },
        "daily_buckets": [asdict(summary) for summary in trend_daily_summaries],
    }


def _build_quality_trends(trend_daily_summaries) -> dict[str, object]:
    parse_valid_count = sum(summary.parse_valid_count for summary in trend_daily_summaries)
    parse_invalid_count = sum(summary.parse_invalid_count for summary in trend_daily_summaries)
    parse_warning_count = sum(summary.parse_warning_count for summary in trend_daily_summaries)
    reconciliation_full_match_count = sum(
        summary.reconciliation_full_match_count for summary in trend_daily_summaries
    )
    reconciliation_partial_single_row_count = sum(
        summary.reconciliation_partial_single_row_count for summary in trend_daily_summaries
    )
    reconciliation_conflict_score_count = sum(
        summary.reconciliation_conflict_score_count for summary in trend_daily_summaries
    )
    reconciliation_conflict_total_line_count = sum(
        summary.reconciliation_conflict_total_line_count for summary in trend_daily_summaries
    )
    reconciliation_conflict_spread_line_count = sum(
        summary.reconciliation_conflict_spread_line_count for summary in trend_daily_summaries
    )
    quality_issue_warning_count = sum(
        summary.quality_issue_warning_count for summary in trend_daily_summaries
    )
    quality_issue_error_count = sum(
        summary.quality_issue_error_count for summary in trend_daily_summaries
    )
    parse_total = parse_valid_count + parse_invalid_count + parse_warning_count
    reconciliation_total = (
        reconciliation_full_match_count
        + reconciliation_partial_single_row_count
        + reconciliation_conflict_score_count
        + reconciliation_conflict_total_line_count
        + reconciliation_conflict_spread_line_count
    )
    reconciliation_conflict_count = (
        reconciliation_conflict_score_count
        + reconciliation_conflict_total_line_count
        + reconciliation_conflict_spread_line_count
    )
    quality_issue_total = quality_issue_warning_count + quality_issue_error_count

    return {
        "overview": {
            "job_count": sum(summary.job_count for summary in trend_daily_summaries),
            "parse_valid_count": parse_valid_count,
            "parse_invalid_count": parse_invalid_count,
            "parse_warning_count": parse_warning_count,
            "parse_invalid_rate": round((parse_invalid_count / parse_total), 4)
            if parse_total
            else 0.0,
            "reconciliation_full_match_count": reconciliation_full_match_count,
            "reconciliation_partial_single_row_count": reconciliation_partial_single_row_count,
            "reconciliation_conflict_score_count": reconciliation_conflict_score_count,
            "reconciliation_conflict_total_line_count": reconciliation_conflict_total_line_count,
            "reconciliation_conflict_spread_line_count": (
                reconciliation_conflict_spread_line_count
            ),
            "reconciliation_conflict_rate": round(
                (reconciliation_conflict_count / reconciliation_total),
                4,
            )
            if reconciliation_total
            else 0.0,
            "quality_issue_warning_count": quality_issue_warning_count,
            "quality_issue_error_count": quality_issue_error_count,
            "quality_issue_error_rate": round(
                (quality_issue_error_count / quality_issue_total),
                4,
            )
            if quality_issue_total
            else 0.0,
        },
        "daily_buckets": [asdict(summary) for summary in trend_daily_summaries],
    }


def _build_recent_run_summary(job_run) -> dict[str, object]:
    summary = job_run.summary or {}
    payload = job_run.payload or {}
    return {
        "job_id": job_run.id,
        "job_name": job_run.job_name,
        "status": job_run.status,
        "requested_by": job_run.requested_by,
        "provider_name": payload.get("provider"),
        "team_code": payload.get("team_code"),
        "season_label": payload.get("season_label"),
        "source_url": payload.get("source_url"),
        "run_label": payload.get("run_label"),
        "started_at": job_run.started_at,
        "completed_at": job_run.completed_at,
        "raw_rows_saved": summary.get("raw_rows_saved", 0),
        "canonical_games_saved": summary.get("canonical_games_saved", 0),
        "metrics_saved": summary.get("metrics_saved", 0),
        "quality_issues_saved": summary.get("quality_issues_saved", 0),
        "warning_count": summary.get("warning_count", 0),
        "warnings": summary.get("warnings", []),
        "parse_status_counts": summary.get("parse_status_counts", {}),
        "reconciliation_status_counts": summary.get("reconciliation_status_counts", {}),
        "data_quality_issue_type_counts": summary.get("data_quality_issue_type_counts", {}),
        "data_quality_issue_severity_counts": summary.get(
            "data_quality_issue_severity_counts",
            {},
        ),
    }


def _build_run_delta(
    latest_run: dict[str, object] | None,
    previous_run: dict[str, object] | None,
) -> dict[str, object] | None:
    if latest_run is None or previous_run is None:
        return None

    return {
        "latest_job_id": latest_run["job_id"],
        "previous_job_id": previous_run["job_id"],
        "status_changed": latest_run["status"] != previous_run["status"],
        "timing": {
            "latest_started_at": latest_run["started_at"],
            "previous_started_at": previous_run["started_at"],
            "latest_completed_at": latest_run["completed_at"],
            "previous_completed_at": previous_run["completed_at"],
        },
        "metric_deltas": {
            "raw_rows_saved": (
                int(latest_run["raw_rows_saved"])
                - int(previous_run["raw_rows_saved"])
            ),
            "canonical_games_saved": int(latest_run["canonical_games_saved"])
            - int(previous_run["canonical_games_saved"]),
            "metrics_saved": int(latest_run["metrics_saved"]) - int(previous_run["metrics_saved"]),
            "quality_issues_saved": int(latest_run["quality_issues_saved"])
            - int(previous_run["quality_issues_saved"]),
            "warning_count": int(latest_run["warning_count"])
            - int(previous_run["warning_count"]),
        },
        "parse_status_count_deltas": _build_count_delta(
            latest_run["parse_status_counts"],
            previous_run["parse_status_counts"],
        ),
        "reconciliation_status_count_deltas": _build_count_delta(
            latest_run["reconciliation_status_counts"],
            previous_run["reconciliation_status_counts"],
        ),
        "data_quality_issue_type_count_deltas": _build_count_delta(
            latest_run["data_quality_issue_type_counts"],
            previous_run["data_quality_issue_type_counts"],
        ),
        "data_quality_issue_severity_count_deltas": _build_count_delta(
            latest_run["data_quality_issue_severity_counts"],
            previous_run["data_quality_issue_severity_counts"],
        ),
    }


def _build_count_delta(
    latest_counts: dict[str, int],
    previous_counts: dict[str, int],
) -> dict[str, int]:
    delta: dict[str, int] = {}
    for key in sorted(set(latest_counts) | set(previous_counts)):
        change = int(latest_counts.get(key, 0)) - int(previous_counts.get(key, 0))
        if change != 0:
            delta[key] = change
    return delta


def _build_trend_overview(trend_daily_summaries) -> dict[str, object]:
    job_count = sum(summary.job_count for summary in trend_daily_summaries)
    completed_jobs = sum(summary.completed_jobs for summary in trend_daily_summaries)
    failed_jobs = sum(summary.failed_jobs for summary in trend_daily_summaries)
    total_warning_count = sum(summary.warning_count for summary in trend_daily_summaries)
    total_quality_issues = sum(summary.quality_issues_saved for summary in trend_daily_summaries)
    return {
        "job_count": job_count,
        "completed_jobs": completed_jobs,
        "failed_jobs": failed_jobs,
        "completion_rate": round((completed_jobs / job_count), 4) if job_count else 0.0,
        "total_warning_count": total_warning_count,
        "avg_warning_count": round((total_warning_count / job_count), 2) if job_count else 0.0,
        "total_quality_issues_saved": total_quality_issues,
        "avg_quality_issues_saved": round((total_quality_issues / job_count), 2)
        if job_count
        else 0.0,
    }


def _bucket_date_for_run(started_at: object) -> str:
    if isinstance(started_at, datetime):
        return started_at.date().isoformat()
    return "unknown"


def resolve_started_window(
    *,
    started_from: date | None = None,
    started_to: date | None = None,
    days: int | None = None,
) -> tuple[datetime | None, datetime | None]:
    resolved_started_to = started_to or _utc_today()
    resolved_started_from = started_from
    if resolved_started_from is None and days is not None:
        resolved_started_from = resolved_started_to - timedelta(days=max(days - 1, 0))

    start_datetime = None
    end_datetime = None
    if resolved_started_from is not None:
        start_datetime = datetime.combine(
            resolved_started_from,
            datetime.min.time(),
            tzinfo=timezone.utc,
        )
    if resolved_started_to is not None:
        end_datetime = datetime.combine(
            resolved_started_to,
            datetime.max.time(),
            tzinfo=timezone.utc,
        )
    return start_datetime, end_datetime


def _shift_latest_in_memory_run(repository: IngestionRepository, *, days_ago: int) -> None:
    if not isinstance(repository, InMemoryIngestionRepository):
        return
    target_started_at = datetime.now(timezone.utc) - timedelta(days=days_ago)
    if repository.job_runs:
        repository.job_runs[-1]["started_at"] = target_started_at
        repository.job_runs[-1]["completed_at"] = target_started_at + timedelta(seconds=5)
    if repository.job_run_reporting_snapshots:
        repository.job_run_reporting_snapshots[-1]["started_at"] = target_started_at
        repository.job_run_reporting_snapshots[-1]["completed_at"] = (
            target_started_at + timedelta(seconds=5)
        )
    if repository.page_retrievals:
        repository.page_retrievals[-1]["retrieved_at"] = target_started_at + timedelta(seconds=1)
    if repository.page_retrieval_reporting_snapshots:
        repository.page_retrieval_reporting_snapshots[-1]["retrieved_at"] = (
            target_started_at + timedelta(seconds=1)
        )
    if repository.job_run_quality_snapshots:
        repository.job_run_quality_snapshots[-1]["started_at"] = target_started_at
        repository.job_run_quality_snapshots[-1]["completed_at"] = (
            target_started_at + timedelta(seconds=5)
        )
