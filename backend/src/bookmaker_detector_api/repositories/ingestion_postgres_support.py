from __future__ import annotations

from typing import Any

from bookmaker_detector_api.data_quality_taxonomy import (
    issue_type_filter_variants,
    normalize_issue_type_and_severity,
)
from bookmaker_detector_api.repositories.ingestion_types import DataQualityIssueRecord


def build_raw_row_scope_where_clauses(
    *,
    provider_name: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    run_label: str | None = None,
) -> tuple[list[str], list[Any]]:
    where_clauses: list[str] = []
    params: list[Any] = []
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
    return where_clauses, params


def build_canonical_scope_where_clauses(
    *,
    provider_name: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    run_label: str | None = None,
) -> tuple[list[str], list[Any]]:
    where_clauses: list[str] = []
    params: list[Any] = []
    if team_code is not None:
        where_clauses.append("(th.code = %s OR ta.code = %s)")
        params.extend([team_code, team_code])
    if season_label is not None:
        where_clauses.append("s.label = %s")
        params.append(season_label)
    if provider_name is not None:
        where_clauses.append(
            """
            EXISTS (
                SELECT 1
                FROM raw_team_game_row rr_scope
                JOIN provider p_scope ON p_scope.id = rr_scope.provider_id
                WHERE rr_scope.season_id = cg.season_id
                  AND rr_scope.team_id IN (cg.home_team_id, cg.away_team_id)
                  AND rr_scope.source_row_index IN (
                      SELECT value::integer
                      FROM jsonb_array_elements_text(cg.source_row_indexes_json)
                  )
                  AND p_scope.name = %s
            )
            """.strip()
        )
        params.append(provider_name)
    if run_label is not None:
        where_clauses.append(
            """
            EXISTS (
                SELECT 1
                FROM raw_team_game_row rr_scope
                JOIN page_retrieval pr_scope ON pr_scope.id = rr_scope.page_retrieval_id
                JOIN job_run jr_scope ON jr_scope.id = pr_scope.job_run_id
                WHERE rr_scope.season_id = cg.season_id
                  AND rr_scope.team_id IN (cg.home_team_id, cg.away_team_id)
                  AND rr_scope.source_row_index IN (
                      SELECT value::integer
                      FROM jsonb_array_elements_text(cg.source_row_indexes_json)
                  )
                  AND jr_scope.payload_json ->> 'run_label' = %s
            )
            """.strip()
        )
        params.append(run_label)
    return where_clauses, params


def build_issue_scope_where_clauses(
    *,
    severity: str | None = None,
    issue_type: str | None = None,
    provider_name: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    run_label: str | None = None,
) -> tuple[list[str], list[Any]]:
    where_clauses: list[str] = []
    params: list[Any] = []
    if severity is not None:
        where_clauses.append(f"{canonical_issue_severity_sql('dqi.issue_type')} = %s")
        params.append(severity)
    if issue_type is not None:
        issue_type_variants = sorted(issue_type_filter_variants(issue_type))
        placeholders = ", ".join(["%s"] * len(issue_type_variants))
        where_clauses.append(f"dqi.issue_type IN ({placeholders})")
        params.extend(issue_type_variants)
    if provider_name is not None:
        where_clauses.append(
            """
            (
                (rr.id IS NOT NULL AND pr.name = %s)
                OR (
                    cg.id IS NOT NULL
                    AND EXISTS (
                        SELECT 1
                        FROM raw_team_game_row rr_scope
                        JOIN provider p_scope ON p_scope.id = rr_scope.provider_id
                        WHERE rr_scope.season_id = cg.season_id
                          AND rr_scope.team_id IN (cg.home_team_id, cg.away_team_id)
                          AND rr_scope.source_row_index IN (
                              SELECT value::integer
                              FROM jsonb_array_elements_text(cg.source_row_indexes_json)
                          )
                          AND p_scope.name = %s
                    )
                )
            )
            """.strip()
        )
        params.extend([provider_name, provider_name])
    if team_code is not None:
        where_clauses.append(
            """
            (
                (rr.id IS NOT NULL AND tr.code = %s)
                OR (cg.id IS NOT NULL AND (th.code = %s OR ta.code = %s))
            )
            """.strip()
        )
        params.extend([team_code, team_code, team_code])
    if season_label is not None:
        where_clauses.append(
            "((rr.id IS NOT NULL AND sr.label = %s) OR (cg.id IS NOT NULL AND sc.label = %s))"
        )
        params.extend([season_label, season_label])
    if run_label is not None:
        where_clauses.append(
            """
            (
                (rr.id IS NOT NULL AND EXISTS (
                    SELECT 1
                    FROM page_retrieval pr_run
                    JOIN job_run jr_run ON jr_run.id = pr_run.job_run_id
                    WHERE pr_run.id = rr.page_retrieval_id
                      AND jr_run.payload_json ->> 'run_label' = %s
                ))
                OR (
                    cg.id IS NOT NULL
                    AND EXISTS (
                        SELECT 1
                        FROM raw_team_game_row rr_scope
                        JOIN page_retrieval pr_scope ON pr_scope.id = rr_scope.page_retrieval_id
                        JOIN job_run jr_scope ON jr_scope.id = pr_scope.job_run_id
                        WHERE rr_scope.season_id = cg.season_id
                          AND rr_scope.team_id IN (cg.home_team_id, cg.away_team_id)
                          AND rr_scope.source_row_index IN (
                              SELECT value::integer
                              FROM jsonb_array_elements_text(cg.source_row_indexes_json)
                          )
                          AND jr_scope.payload_json ->> 'run_label' = %s
                    )
                )
            )
            """.strip()
        )
        params.extend([run_label, run_label])
    return where_clauses, params


def count_by_column(
    connection: Any,
    *,
    query: str,
    params: list[Any] | None = None,
) -> dict[str, int]:
    with connection.cursor() as cursor:
        cursor.execute(query, params or [])
        rows = cursor.fetchall()
    return {row[0]: row[1] for row in rows}


def select_data_quality_issue_rows_for_normalization(
    connection: Any,
    *,
    provider_name: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
) -> list[tuple[int, str, str]]:
    query = """
        SELECT dqi.id, dqi.issue_type, dqi.severity
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
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        return cursor.fetchall()


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


def canonical_issue_severity_sql(issue_type_expression: str) -> str:
    error_issue_types = [
        "parse.invalid_game_date_format",
        "parse.invalid_score_format",
        "row_too_short",
        "canonical.score_mismatch",
        "score_mismatch",
        "canonical.total_line_mismatch",
        "total_line_mismatch",
        "canonical.spread_line_mismatch",
        "spread_line_mismatch",
    ]
    placeholders = ", ".join(f"'{issue_type}'" for issue_type in error_issue_types)
    return f"CASE WHEN {issue_type_expression} IN ({placeholders}) THEN 'error' ELSE 'warning' END"
