from __future__ import annotations

from datetime import datetime
from typing import Any

from bookmaker_detector_api.data_quality_taxonomy import normalize_issue_type_and_severity
from bookmaker_detector_api.repositories.ingestion_types import (
    DataQualityIssueRecord,
    JobRunReportingSnapshot,
)


def issue_matches_scope(
    issue: dict[str, Any],
    *,
    raw_rows: list[dict[str, Any]],
    canonical_games: list[dict[str, Any]],
    page_retrievals: list[dict[str, Any]],
    job_runs: list[dict[str, Any]],
    provider_name: str | None,
    team_code: str | None,
    season_label: str | None,
    run_label: str | None,
) -> bool:
    raw_row_id = issue["raw_team_game_row_id"]
    if raw_row_id is not None:
        raw_row = next((row for row in raw_rows if row["id"] == raw_row_id), None)
        return raw_row is not None and raw_row_matches_scope(
            raw_row,
            page_retrievals=page_retrievals,
            job_runs=job_runs,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
        )

    canonical_game_id = issue["canonical_game_id"]
    if canonical_game_id is not None:
        canonical_game = next((game for game in canonical_games if game["id"] == canonical_game_id), None)
        return canonical_game is not None and canonical_game_matches_scope(
            canonical_game,
            raw_rows=raw_rows,
            page_retrievals=page_retrievals,
            job_runs=job_runs,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
        )

    return (
        provider_name is None
        and team_code is None
        and season_label is None
        and run_label is None
    )


def raw_row_matches_scope(
    row: dict[str, Any],
    *,
    page_retrievals: list[dict[str, Any]],
    job_runs: list[dict[str, Any]],
    provider_name: str | None,
    team_code: str | None,
    season_label: str | None,
    run_label: str | None,
) -> bool:
    return (
        (provider_name is None or row["provider_name"] == provider_name)
        and (team_code is None or row["team_code"] == team_code)
        and (season_label is None or row["season_label"] == season_label)
        and (
            run_label is None
            or page_retrieval_has_run_label(
                row.get("page_retrieval_id"),
                page_retrievals=page_retrievals,
                job_runs=job_runs,
                run_label=run_label,
            )
        )
    )


def canonical_game_matches_scope(
    game: dict[str, Any],
    *,
    raw_rows: list[dict[str, Any]],
    page_retrievals: list[dict[str, Any]],
    job_runs: list[dict[str, Any]],
    provider_name: str | None,
    team_code: str | None,
    season_label: str | None,
    run_label: str | None,
) -> bool:
    if season_label is not None and game["season_label"] != season_label:
        return False
    if team_code is not None and team_code not in {game["home_team_code"], game["away_team_code"]}:
        return False
    if provider_name is not None and not canonical_game_has_provider(
        game,
        raw_rows=raw_rows,
        provider_name=provider_name,
    ):
        return False
    if run_label is not None and not canonical_game_has_run_label(
        game,
        raw_rows=raw_rows,
        page_retrievals=page_retrievals,
        job_runs=job_runs,
        run_label=run_label,
    ):
        return False
    return True


def canonical_game_has_provider(
    game: dict[str, Any],
    *,
    raw_rows: list[dict[str, Any]],
    provider_name: str,
) -> bool:
    relevant_teams = {game["home_team_code"], game["away_team_code"]}
    relevant_row_indexes = set(game["source_row_indexes"])
    return any(
        row["provider_name"] == provider_name
        and row["season_label"] == game["season_label"]
        and row["team_code"] in relevant_teams
        and row["source_row_index"] in relevant_row_indexes
        for row in raw_rows
    )


def canonical_game_has_run_label(
    game: dict[str, Any],
    *,
    raw_rows: list[dict[str, Any]],
    page_retrievals: list[dict[str, Any]],
    job_runs: list[dict[str, Any]],
    run_label: str,
) -> bool:
    relevant_teams = {game["home_team_code"], game["away_team_code"]}
    relevant_row_indexes = set(game["source_row_indexes"])
    return any(
        row["season_label"] == game["season_label"]
        and row["team_code"] in relevant_teams
        and row["source_row_index"] in relevant_row_indexes
        and page_retrieval_has_run_label(
            row.get("page_retrieval_id"),
            page_retrievals=page_retrievals,
            job_runs=job_runs,
            run_label=run_label,
        )
        for row in raw_rows
    )


def page_retrieval_has_run_label(
    page_retrieval_id: int | None,
    *,
    page_retrievals: list[dict[str, Any]],
    job_runs: list[dict[str, Any]],
    run_label: str,
) -> bool:
    if page_retrieval_id is None:
        return False
    page_retrieval = next((entry for entry in page_retrievals if entry["id"] == page_retrieval_id), None)
    if page_retrieval is None:
        return False
    job_run = next((entry for entry in job_runs if entry["id"] == page_retrieval["job_id"]), None)
    return job_run is not None and job_run.get("payload", {}).get("run_label") == run_label


def normalize_issue_record(issue: DataQualityIssueRecord) -> DataQualityIssueRecord:
    normalized_issue_type, normalized_severity = normalize_issue_type_and_severity(
        issue.issue_type,
        issue.severity,
    )
    return DataQualityIssueRecord(
        id=issue.id,
        issue_type=normalized_issue_type,
        severity=normalized_severity,
        raw_team_game_row_id=issue.raw_team_game_row_id,
        canonical_game_id=issue.canonical_game_id,
        details=issue.details,
    )


def build_reporting_snapshot(job: dict[str, Any]) -> dict[str, Any]:
    payload = job.get("payload", {})
    summary = job.get("summary", {})
    return {
        "job_run_id": job["id"],
        "job_name": job["job_name"],
        "status": job["status"],
        "run_label": payload.get("run_label"),
        "provider_name": payload.get("provider"),
        "team_code": payload.get("team_code"),
        "season_label": payload.get("season_label"),
        "started_at": job.get("started_at"),
        "completed_at": job.get("completed_at"),
        "raw_rows_saved": int(summary.get("raw_rows_saved", 0)),
        "canonical_games_saved": int(summary.get("canonical_games_saved", 0)),
        "metrics_saved": int(summary.get("metrics_saved", 0)),
        "quality_issues_saved": int(summary.get("quality_issues_saved", 0)),
        "warning_count": int(summary.get("warning_count", 0)),
    }


def upsert_quality_snapshot(
    quality_snapshots: list[dict[str, Any]],
    *,
    job: dict[str, Any],
) -> None:
    payload = job.get("payload", {})
    summary = job.get("summary", {})
    parse_status_counts = summary.get("parse_status_counts", {})
    reconciliation_status_counts = summary.get("reconciliation_status_counts", {})
    quality_issue_severity_counts = summary.get("data_quality_issue_severity_counts", {})
    snapshot = {
        "job_run_id": job["id"],
        "job_name": job["job_name"],
        "run_label": payload.get("run_label"),
        "provider_name": payload.get("provider"),
        "team_code": payload.get("team_code"),
        "season_label": payload.get("season_label"),
        "started_at": job.get("started_at"),
        "completed_at": job.get("completed_at"),
        "parse_valid_count": int(parse_status_counts.get("VALID", 0)),
        "parse_invalid_count": int(parse_status_counts.get("INVALID", 0)),
        "parse_warning_count": int(parse_status_counts.get("VALID_WITH_WARNINGS", 0)),
        "reconciliation_full_match_count": int(reconciliation_status_counts.get("FULL_MATCH", 0)),
        "reconciliation_partial_single_row_count": int(
            reconciliation_status_counts.get("PARTIAL_SINGLE_ROW", 0)
        ),
        "reconciliation_conflict_score_count": int(
            reconciliation_status_counts.get("CONFLICT_SCORE", 0)
        ),
        "reconciliation_conflict_total_line_count": int(
            reconciliation_status_counts.get("CONFLICT_TOTAL_LINE", 0)
        ),
        "reconciliation_conflict_spread_line_count": int(
            reconciliation_status_counts.get("CONFLICT_SPREAD_LINE", 0)
        ),
        "quality_issue_warning_count": int(quality_issue_severity_counts.get("warning", 0)),
        "quality_issue_error_count": int(quality_issue_severity_counts.get("error", 0)),
    }
    existing_snapshot = next(
        (entry for entry in quality_snapshots if entry["job_run_id"] == job["id"]),
        None,
    )
    if existing_snapshot is None:
        quality_snapshots.append(snapshot)
        return
    existing_snapshot.update(snapshot)


def upsert_reporting_snapshot(
    reporting_snapshots: list[dict[str, Any]],
    quality_snapshots: list[dict[str, Any]],
    *,
    job: dict[str, Any],
) -> None:
    snapshot = build_reporting_snapshot(job)
    existing_snapshot = next(
        (entry for entry in reporting_snapshots if entry["job_run_id"] == job["id"]),
        None,
    )
    if existing_snapshot is None:
        reporting_snapshots.append(snapshot)
    else:
        existing_snapshot.update(snapshot)
    upsert_quality_snapshot(quality_snapshots, job=job)


def list_reporting_snapshots(
    reporting_snapshots: list[dict[str, Any]],
    *,
    status: str | None = None,
    provider_name: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    run_label: str | None = None,
    started_from: datetime | None = None,
    started_to: datetime | None = None,
) -> list[JobRunReportingSnapshot]:
    selected = [
        snapshot
        for snapshot in reporting_snapshots
        if (status is None or snapshot["status"] == status)
        and (run_label is None or snapshot["run_label"] == run_label)
        and (provider_name is None or snapshot["provider_name"] == provider_name)
        and (team_code is None or snapshot["team_code"] == team_code)
        and (season_label is None or snapshot["season_label"] == season_label)
        and (
            started_from is None
            or (snapshot["started_at"] is not None and snapshot["started_at"] >= started_from)
        )
        and (
            started_to is None
            or (snapshot["started_at"] is not None and snapshot["started_at"] <= started_to)
        )
    ]
    return [JobRunReportingSnapshot(**entry) for entry in selected]


def list_page_retrieval_reporting_snapshots(
    reporting_snapshots: list[dict[str, Any]],
    *,
    status: str | None = None,
    provider_name: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    run_label: str | None = None,
    retrieved_from: datetime | None = None,
    retrieved_to: datetime | None = None,
) -> list[dict[str, Any]]:
    return [
        snapshot
        for snapshot in reporting_snapshots
        if (status is None or snapshot["status"] == status)
        and (run_label is None or snapshot["run_label"] == run_label)
        and (provider_name is None or snapshot["provider_name"] == provider_name)
        and (team_code is None or snapshot["team_code"] == team_code)
        and (season_label is None or snapshot["season_label"] == season_label)
        and (
            retrieved_from is None
            or (snapshot["retrieved_at"] is not None and snapshot["retrieved_at"] >= retrieved_from)
        )
        and (
            retrieved_to is None
            or (snapshot["retrieved_at"] is not None and snapshot["retrieved_at"] <= retrieved_to)
        )
    ]


def list_quality_snapshots(
    quality_snapshots: list[dict[str, Any]],
    *,
    provider_name: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    run_label: str | None = None,
    started_from: datetime | None = None,
    started_to: datetime | None = None,
) -> list[dict[str, Any]]:
    return [
        snapshot
        for snapshot in quality_snapshots
        if quality_snapshot_has_activity(snapshot)
        and (run_label is None or snapshot["run_label"] == run_label)
        and (provider_name is None or snapshot["provider_name"] == provider_name)
        and (team_code is None or snapshot["team_code"] == team_code)
        and (season_label is None or snapshot["season_label"] == season_label)
        and (
            started_from is None
            or (snapshot["started_at"] is not None and snapshot["started_at"] >= started_from)
        )
        and (
            started_to is None
            or (snapshot["started_at"] is not None and snapshot["started_at"] <= started_to)
        )
    ]


def bucket_date_from_datetime(value: datetime | None) -> str:
    if value is None:
        return "unknown"
    return value.date().isoformat()


def quality_snapshot_has_activity(snapshot: dict[str, Any]) -> bool:
    return any(
        int(snapshot.get(field_name, 0)) > 0
        for field_name in (
            "parse_valid_count",
            "parse_invalid_count",
            "parse_warning_count",
            "reconciliation_full_match_count",
            "reconciliation_partial_single_row_count",
            "reconciliation_conflict_score_count",
            "reconciliation_conflict_total_line_count",
            "reconciliation_conflict_spread_line_count",
            "quality_issue_warning_count",
            "quality_issue_error_count",
        )
    )
