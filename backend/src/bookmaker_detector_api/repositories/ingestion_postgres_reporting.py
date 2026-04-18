from __future__ import annotations

from datetime import datetime
from typing import Any

from bookmaker_detector_api.data_quality_taxonomy import (
    merge_issue_type_counts,
    normalize_issue_type_and_severity,
    severity_counts_from_issue_type_counts,
)
from bookmaker_detector_api.repositories.ingestion_postgres_support import (
    build_canonical_scope_where_clauses,
    build_issue_scope_where_clauses,
    build_raw_row_scope_where_clauses,
    count_by_column,
    select_data_quality_issue_rows_for_normalization,
)
from bookmaker_detector_api.repositories.ingestion_postgres_support import (
    normalize_issue_record as normalize_postgres_issue_record,
)
from bookmaker_detector_api.repositories.ingestion_types import (
    DailyJobRunQualitySummary,
    DailyJobRunSummary,
    DailyPageRetrievalSummary,
    DataQualityIssueRecord,
    JobRunRecord,
    PageRetrievalSnapshot,
)


def list_job_runs(
    connection: Any,
    *,
    limit: int = 20,
    offset: int = 0,
    status: str | None = None,
    provider_name: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    run_label: str | None = None,
    started_from: datetime | None = None,
    started_to: datetime | None = None,
) -> list[JobRunRecord]:
    query = """
        SELECT
            id,
            job_name,
            status,
            requested_by,
            payload_json,
            summary_json,
            started_at,
            completed_at
        FROM job_run
    """
    params: list[Any] = []
    where_clauses: list[str] = []
    if status is not None:
        where_clauses.append("status = %s")
        params.append(status)
    if provider_name is not None:
        where_clauses.append("payload_json ->> 'provider' = %s")
        params.append(provider_name)
    if team_code is not None:
        where_clauses.append("payload_json ->> 'team_code' = %s")
        params.append(team_code)
    if season_label is not None:
        where_clauses.append("payload_json ->> 'season_label' = %s")
        params.append(season_label)
    if run_label is not None:
        where_clauses.append("payload_json ->> 'run_label' = %s")
        params.append(run_label)
    if started_from is not None:
        where_clauses.append("started_at >= %s")
        params.append(started_from)
    if started_to is not None:
        where_clauses.append("started_at <= %s")
        params.append(started_to)
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY id DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        JobRunRecord(
            id=row[0],
            job_name=row[1],
            status=row[2],
            requested_by=row[3],
            payload=row[4],
            summary=row[5],
            started_at=row[6],
            completed_at=row[7],
        )
        for row in rows
    ]


def list_job_run_daily_summaries(
    connection: Any,
    *,
    status: str | None = None,
    provider_name: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    run_label: str | None = None,
    started_from: datetime | None = None,
    started_to: datetime | None = None,
) -> list[DailyJobRunSummary]:
    query = """
        SELECT
            started_at::date::text AS bucket_date,
            COUNT(*) AS job_count,
            SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_jobs,
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_jobs,
            SUM(raw_rows_saved) AS raw_rows_saved,
            SUM(canonical_games_saved) AS canonical_games_saved,
            SUM(metrics_saved) AS metrics_saved,
            SUM(quality_issues_saved) AS quality_issues_saved,
            SUM(warning_count) AS warning_count
        FROM job_run_reporting_snapshot
    """
    params: list[Any] = []
    where_clauses: list[str] = []
    if status is not None:
        where_clauses.append("status = %s")
        params.append(status)
    if provider_name is not None:
        where_clauses.append("provider_name = %s")
        params.append(provider_name)
    if team_code is not None:
        where_clauses.append("team_code = %s")
        params.append(team_code)
    if season_label is not None:
        where_clauses.append("season_label = %s")
        params.append(season_label)
    if run_label is not None:
        where_clauses.append("run_label = %s")
        params.append(run_label)
    if started_from is not None:
        where_clauses.append("started_at >= %s")
        params.append(started_from)
    if started_to is not None:
        where_clauses.append("started_at <= %s")
        params.append(started_to)
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " GROUP BY started_at::date ORDER BY started_at::date ASC"

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        DailyJobRunSummary(
            date=row[0],
            job_count=row[1],
            completed_jobs=row[2],
            failed_jobs=row[3],
            raw_rows_saved=row[4],
            canonical_games_saved=row[5],
            metrics_saved=row[6],
            quality_issues_saved=row[7],
            warning_count=row[8],
        )
        for row in rows
    ]


def list_page_retrieval_daily_summaries(
    connection: Any,
    *,
    status: str | None = None,
    provider_name: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    run_label: str | None = None,
    retrieved_from: datetime | None = None,
    retrieved_to: datetime | None = None,
) -> list[DailyPageRetrievalSummary]:
    query = """
        SELECT
            retrieved_at::date::text AS bucket_date,
            COUNT(*) AS retrieval_count,
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_retrievals,
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_retrievals,
            SUM(
                CASE WHEN payload_storage_path IS NOT NULL THEN 1 ELSE 0 END
            ) AS payload_saved_count,
            SUM(CASE WHEN http_status IS NULL THEN 1 ELSE 0 END) AS missing_http_status_count
        FROM page_retrieval_reporting_snapshot
    """
    params: list[Any] = []
    where_clauses: list[str] = []
    if status is not None:
        where_clauses.append("status = %s")
        params.append(status)
    if provider_name is not None:
        where_clauses.append("provider_name = %s")
        params.append(provider_name)
    if team_code is not None:
        where_clauses.append("team_code = %s")
        params.append(team_code)
    if season_label is not None:
        where_clauses.append("season_label = %s")
        params.append(season_label)
    if run_label is not None:
        where_clauses.append("run_label = %s")
        params.append(run_label)
    if retrieved_from is not None:
        where_clauses.append("retrieved_at >= %s")
        params.append(retrieved_from)
    if retrieved_to is not None:
        where_clauses.append("retrieved_at <= %s")
        params.append(retrieved_to)
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " GROUP BY retrieved_at::date ORDER BY retrieved_at::date ASC"

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        DailyPageRetrievalSummary(
            date=row[0],
            retrieval_count=row[1],
            successful_retrievals=row[2],
            failed_retrievals=row[3],
            payload_saved_count=row[4],
            missing_http_status_count=row[5],
        )
        for row in rows
    ]


def list_job_run_quality_daily_summaries(
    connection: Any,
    *,
    provider_name: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    run_label: str | None = None,
    started_from: datetime | None = None,
    started_to: datetime | None = None,
) -> list[DailyJobRunQualitySummary]:
    query = """
        SELECT
            started_at::date::text AS bucket_date,
            COUNT(*) AS job_count,
            SUM(parse_valid_count) AS parse_valid_count,
            SUM(parse_invalid_count) AS parse_invalid_count,
            SUM(parse_warning_count) AS parse_warning_count,
            SUM(reconciliation_full_match_count) AS reconciliation_full_match_count,
            SUM(
                reconciliation_partial_single_row_count
            ) AS reconciliation_partial_single_row_count,
            SUM(reconciliation_conflict_score_count) AS reconciliation_conflict_score_count,
            SUM(
                reconciliation_conflict_total_line_count
            ) AS reconciliation_conflict_total_line_count,
            SUM(
                reconciliation_conflict_spread_line_count
            ) AS reconciliation_conflict_spread_line_count,
            SUM(quality_issue_warning_count) AS quality_issue_warning_count,
            SUM(quality_issue_error_count) AS quality_issue_error_count
        FROM job_run_quality_snapshot
    """
    params: list[Any] = []
    where_clauses: list[str] = [
        "("
        "parse_valid_count > 0 OR parse_invalid_count > 0 OR parse_warning_count > 0 OR "
        "reconciliation_full_match_count > 0 OR reconciliation_partial_single_row_count > 0 OR "
        "reconciliation_conflict_score_count > 0 OR "
        "reconciliation_conflict_total_line_count > 0 OR "
        "reconciliation_conflict_spread_line_count > 0 OR quality_issue_warning_count > 0 OR "
        "quality_issue_error_count > 0"
        ")"
    ]
    if provider_name is not None:
        where_clauses.append("provider_name = %s")
        params.append(provider_name)
    if team_code is not None:
        where_clauses.append("team_code = %s")
        params.append(team_code)
    if season_label is not None:
        where_clauses.append("season_label = %s")
        params.append(season_label)
    if run_label is not None:
        where_clauses.append("run_label = %s")
        params.append(run_label)
    if started_from is not None:
        where_clauses.append("started_at >= %s")
        params.append(started_from)
    if started_to is not None:
        where_clauses.append("started_at <= %s")
        params.append(started_to)
    query += " WHERE " + " AND ".join(where_clauses)
    query += " GROUP BY started_at::date ORDER BY started_at::date ASC"

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        DailyJobRunQualitySummary(
            date=row[0],
            job_count=row[1],
            parse_valid_count=row[2],
            parse_invalid_count=row[3],
            parse_warning_count=row[4],
            reconciliation_full_match_count=row[5],
            reconciliation_partial_single_row_count=row[6],
            reconciliation_conflict_score_count=row[7],
            reconciliation_conflict_total_line_count=row[8],
            reconciliation_conflict_spread_line_count=row[9],
            quality_issue_warning_count=row[10],
            quality_issue_error_count=row[11],
        )
        for row in rows
    ]


def list_page_retrievals(
    connection: Any,
    *,
    limit: int = 20,
    offset: int = 0,
    status: str | None = None,
    provider_name: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    run_label: str | None = None,
) -> list[PageRetrievalSnapshot]:
    query = """
        SELECT
            pr.id,
            pr.job_run_id,
            jr.payload_json ->> 'run_label',
            p.name,
            t.code,
            s.label,
            pr.source_url,
            pr.status,
            pr.http_status,
            pr.payload_storage_path,
            pr.error_message,
            pr.retrieved_at
        FROM page_retrieval pr
        LEFT JOIN job_run jr ON jr.id = pr.job_run_id
        JOIN provider p ON p.id = pr.provider_id
        JOIN team t ON t.id = pr.team_id
        JOIN season s ON s.id = pr.season_id
    """
    params: list[Any] = []
    where_clauses: list[str] = []
    if status is not None:
        where_clauses.append("pr.status = %s")
        params.append(status)
    if provider_name is not None:
        where_clauses.append("p.name = %s")
        params.append(provider_name)
    if team_code is not None:
        where_clauses.append("t.code = %s")
        params.append(team_code)
    if season_label is not None:
        where_clauses.append("s.label = %s")
        params.append(season_label)
    if run_label is not None:
        where_clauses.append("jr.payload_json ->> 'run_label' = %s")
        params.append(run_label)
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY pr.id DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        PageRetrievalSnapshot(
            id=row[0],
            job_id=row[1],
            run_label=row[2],
            provider_name=row[3],
            team_code=row[4],
            season_label=row[5],
            source_url=row[6],
            status=row[7],
            http_status=row[8],
            payload_storage_path=row[9],
            error_message=row[10],
            retrieved_at=row[11],
        )
        for row in rows
    ]


def list_data_quality_issues(
    connection: Any,
    *,
    limit: int = 20,
    offset: int = 0,
    severity: str | None = None,
    issue_type: str | None = None,
    provider_name: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    run_label: str | None = None,
) -> list[DataQualityIssueRecord]:
    query = """
        SELECT
            dqi.id,
            dqi.issue_type,
            dqi.severity,
            dqi.raw_team_game_row_id,
            dqi.canonical_game_id,
            dqi.details_json
        FROM data_quality_issue dqi
        LEFT JOIN raw_team_game_row rr ON rr.id = dqi.raw_team_game_row_id
        LEFT JOIN provider pr ON pr.id = rr.provider_id
        LEFT JOIN team tr ON tr.id = rr.team_id
        LEFT JOIN season sr ON sr.id = rr.season_id
        LEFT JOIN canonical_game cg ON cg.id = dqi.canonical_game_id
        LEFT JOIN team th ON th.id = cg.home_team_id
        LEFT JOIN team ta ON ta.id = cg.away_team_id
        LEFT JOIN season sc ON sc.id = cg.season_id
    """
    where_clauses, params = build_issue_scope_where_clauses(
        severity=severity,
        issue_type=issue_type,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY dqi.id DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        normalize_postgres_issue_record(
            DataQualityIssueRecord(
                id=row[0],
                issue_type=row[1],
                severity=row[2],
                raw_team_game_row_id=row[3],
                canonical_game_id=row[4],
                details=row[5],
            )
        )
        for row in rows
    ]


def get_parse_status_counts(
    connection: Any,
    *,
    provider_name: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    run_label: str | None = None,
) -> dict[str, int]:
    query = """
        SELECT rr.parse_status, COUNT(*)
        FROM raw_team_game_row rr
        LEFT JOIN page_retrieval prr ON prr.id = rr.page_retrieval_id
        LEFT JOIN job_run jr ON jr.id = prr.job_run_id
        JOIN provider p ON p.id = rr.provider_id
        JOIN team t ON t.id = rr.team_id
        JOIN season s ON s.id = rr.season_id
    """
    where_clauses, params = build_raw_row_scope_where_clauses(
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " GROUP BY rr.parse_status"
    return count_by_column(connection, query=query, params=params)


def get_reconciliation_status_counts(
    connection: Any,
    *,
    provider_name: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    run_label: str | None = None,
) -> dict[str, int]:
    query = """
        SELECT cg.reconciliation_status, COUNT(*)
        FROM canonical_game cg
        JOIN team th ON th.id = cg.home_team_id
        JOIN team ta ON ta.id = cg.away_team_id
        JOIN season s ON s.id = cg.season_id
    """
    where_clauses, params = build_canonical_scope_where_clauses(
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " GROUP BY cg.reconciliation_status"
    return count_by_column(connection, query=query, params=params)


def get_data_quality_issue_type_counts(
    connection: Any,
    *,
    provider_name: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    run_label: str | None = None,
) -> dict[str, int]:
    query = """
        SELECT dqi.issue_type, COUNT(*)
        FROM data_quality_issue dqi
        LEFT JOIN raw_team_game_row rr ON rr.id = dqi.raw_team_game_row_id
        LEFT JOIN provider pr ON pr.id = rr.provider_id
        LEFT JOIN team tr ON tr.id = rr.team_id
        LEFT JOIN season sr ON sr.id = rr.season_id
        LEFT JOIN canonical_game cg ON cg.id = dqi.canonical_game_id
        LEFT JOIN team th ON th.id = cg.home_team_id
        LEFT JOIN team ta ON ta.id = cg.away_team_id
        LEFT JOIN season sc ON sc.id = cg.season_id
    """
    where_clauses, params = build_issue_scope_where_clauses(
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " GROUP BY dqi.issue_type"
    return merge_issue_type_counts(count_by_column(connection, query=query, params=params))


def get_data_quality_issue_severity_counts(
    connection: Any,
    *,
    provider_name: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    run_label: str | None = None,
) -> dict[str, int]:
    issue_type_counts = get_data_quality_issue_type_counts(
        connection,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
    )
    return severity_counts_from_issue_type_counts(issue_type_counts)


def normalize_data_quality_issue_taxonomy(
    connection: Any,
    *,
    provider_name: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    dry_run: bool = True,
) -> dict[str, int]:
    selected_issues = select_data_quality_issue_rows_for_normalization(
        connection,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
    )
    updates: list[tuple[str, str, int]] = []
    issue_type_updates = 0
    severity_updates = 0

    for issue_id, issue_type, severity in selected_issues:
        normalized_issue_type, normalized_severity = normalize_issue_type_and_severity(
            issue_type,
            severity,
        )
        issue_type_changed = issue_type != normalized_issue_type
        severity_changed = severity != normalized_severity
        if issue_type_changed:
            issue_type_updates += 1
        if severity_changed:
            severity_updates += 1
        if issue_type_changed or severity_changed:
            updates.append((normalized_issue_type, normalized_severity, issue_id))

    if updates and not dry_run:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                UPDATE data_quality_issue
                SET issue_type = %s, severity = %s
                WHERE id = %s
                """,
                updates,
            )
        connection.commit()

    return {
        "matched_rows": len(selected_issues),
        "updated_rows": len(updates),
        "issue_type_updates": issue_type_updates,
        "severity_updates": severity_updates,
    }
