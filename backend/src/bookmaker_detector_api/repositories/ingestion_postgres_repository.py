from __future__ import annotations

from datetime import datetime
from typing import Any

from bookmaker_detector_api.repositories.ingestion_json import _json_dumps
from bookmaker_detector_api.repositories.ingestion_postgres_reporting import (
    get_data_quality_issue_severity_counts as get_data_quality_issue_severity_counts_query,
    get_data_quality_issue_type_counts as get_data_quality_issue_type_counts_query,
    get_parse_status_counts as get_parse_status_counts_query,
    get_reconciliation_status_counts as get_reconciliation_status_counts_query,
    list_data_quality_issues as list_data_quality_issues_query,
    list_job_run_daily_summaries as list_job_run_daily_summaries_query,
    list_job_run_quality_daily_summaries as list_job_run_quality_daily_summaries_query,
    list_job_runs as list_job_runs_query,
    list_page_retrieval_daily_summaries as list_page_retrieval_daily_summaries_query,
    list_page_retrievals as list_page_retrievals_query,
    normalize_data_quality_issue_taxonomy as normalize_data_quality_issue_taxonomy_query,
)
from bookmaker_detector_api.repositories.ingestion_postgres_schema import (
    ensure_data_quality_issue_identity_schema,
    ensure_raw_row_source_identity_schema,
    verify_data_quality_issue_identity_schema,
    verify_raw_row_source_identity_schema,
)
from bookmaker_detector_api.repositories.ingestion_types import (
    DataQualityIssueRecord,
    PageRetrievalRecord,
    PersistedCanonicalGame,
    PersistedRawRow,
)
from bookmaker_detector_api.repositories.ingestion_types import (
    IngestionRepository as _IngestionRepository,
)
from bookmaker_detector_api.repositories.ingestion_types import (
    PersistedIngestionRun as _PersistedIngestionRun,
)

IngestionRepository = _IngestionRepository
PersistedIngestionRun = _PersistedIngestionRun


class PostgresIngestionRepository:
    def __init__(self, connection: Any, *, allow_runtime_schema_mutation: bool = True) -> None:
        self.connection = connection
        self.allow_runtime_schema_mutation = allow_runtime_schema_mutation
        self._raw_row_source_identity_ready = False
        self._data_quality_issue_identity_ready = False

    def create_job_run(self, *, job_name: str, requested_by: str, payload: dict[str, Any]) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO job_run (job_name, status, requested_by, payload_json)
                VALUES (%s, 'RUNNING', %s, %s::jsonb)
                RETURNING id
                """,
                (job_name, requested_by, _json_dumps(payload)),
            )
            job_id = cursor.fetchone()[0]
        self.connection.commit()
        return int(job_id)

    def create_page_retrieval(self, *, job_id: int, record: PageRetrievalRecord) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO page_retrieval (
                    job_run_id,
                    provider_id,
                    team_id,
                    season_id,
                    source_url,
                    http_status,
                    payload_storage_path,
                    status,
                    error_message
                )
                VALUES (
                    %s,
                    (SELECT id FROM provider WHERE name = %s),
                    (SELECT id FROM team WHERE code = %s),
                    (SELECT id FROM season WHERE label = %s),
                    %s, %s, %s, %s, %s
                )
                RETURNING id
                """,
                (
                    job_id,
                    record.provider_name,
                    record.team_code,
                    record.season_label,
                    record.source_url,
                    record.http_status,
                    record.payload_storage_path,
                    record.status,
                    record.error_message,
                ),
            )
            retrieval_id = cursor.fetchone()[0]
            cursor.execute(
                """
                INSERT INTO page_retrieval_reporting_snapshot (
                    page_retrieval_id,
                    job_run_id,
                    run_label,
                    provider_name,
                    team_code,
                    season_label,
                    source_url,
                    status,
                    http_status,
                    payload_storage_path,
                    error_message,
                    retrieved_at
                )
                SELECT
                    %s,
                    %s,
                    jr.payload_json ->> 'run_label',
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    NOW()
                FROM job_run jr
                WHERE jr.id = %s
                ON CONFLICT (page_retrieval_id)
                DO UPDATE SET
                    job_run_id = EXCLUDED.job_run_id,
                    run_label = EXCLUDED.run_label,
                    provider_name = EXCLUDED.provider_name,
                    team_code = EXCLUDED.team_code,
                    season_label = EXCLUDED.season_label,
                    source_url = EXCLUDED.source_url,
                    status = EXCLUDED.status,
                    http_status = EXCLUDED.http_status,
                    payload_storage_path = EXCLUDED.payload_storage_path,
                    error_message = EXCLUDED.error_message,
                    retrieved_at = EXCLUDED.retrieved_at,
                    updated_at = NOW()
                """,
                (
                    retrieval_id,
                    job_id,
                    record.provider_name,
                    record.team_code,
                    record.season_label,
                    record.source_url,
                    record.status,
                    record.http_status,
                    record.payload_storage_path,
                    record.error_message,
                    job_id,
                ),
            )
        self.connection.commit()
        return int(retrieval_id)

    def save_raw_rows(
        self,
        *,
        page_retrieval_id: int,
        rows: list[RawGameRow],
    ) -> list[PersistedRawRow]:
        if not rows:
            return []
        self._ensure_raw_row_source_identity_schema()
        persisted_rows: list[PersistedRawRow] = []
        with self.connection.cursor() as cursor:
            for row in rows:
                cursor.execute(
                    """
                    INSERT INTO raw_team_game_row (
                        provider_id,
                        team_id,
                        season_id,
                        page_retrieval_id,
                        source_url,
                        source_page_url,
                        source_page_season_label,
                        source_section,
                        source_row_index,
                        game_date,
                        opponent_team_code,
                        is_away,
                        result_flag,
                        team_score,
                        opponent_score,
                        ats_result_flag,
                        ats_line,
                        ou_result_flag,
                        total_line,
                        parse_status,
                        parse_warning_codes_json
                    )
                    VALUES (
                        (SELECT id FROM provider WHERE name = %s),
                        (SELECT id FROM team WHERE code = %s),
                        (SELECT id FROM season WHERE label = %s),
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s::jsonb
                    )
                    ON CONFLICT (
                        provider_id,
                        team_id,
                        season_id,
                        source_page_url,
                        source_page_season_label,
                        source_section,
                        source_row_index
                    )
                    DO UPDATE SET
                        page_retrieval_id = EXCLUDED.page_retrieval_id,
                        source_url = EXCLUDED.source_url,
                        source_section = EXCLUDED.source_section,
                        game_date = EXCLUDED.game_date,
                        opponent_team_code = EXCLUDED.opponent_team_code,
                        is_away = EXCLUDED.is_away,
                        result_flag = EXCLUDED.result_flag,
                        team_score = EXCLUDED.team_score,
                        opponent_score = EXCLUDED.opponent_score,
                        ats_result_flag = EXCLUDED.ats_result_flag,
                        ats_line = EXCLUDED.ats_line,
                        ou_result_flag = EXCLUDED.ou_result_flag,
                        total_line = EXCLUDED.total_line,
                        parse_status = EXCLUDED.parse_status,
                        parse_warning_codes_json = EXCLUDED.parse_warning_codes_json
                    RETURNING id
                    """,
                    (
                        row.provider_name,
                        row.team_code,
                        row.season_label,
                        page_retrieval_id,
                        row.source_url,
                        row.source_page_url or row.source_url,
                        row.source_page_season_label or row.season_label,
                        row.source_section,
                        row.source_row_index,
                        row.game_date,
                        row.opponent_code,
                        row.is_away,
                        row.result_flag,
                        row.team_score,
                        row.opponent_score,
                        row.ats_result,
                        row.ats_line,
                        row.ou_result,
                        row.total_line,
                        row.parse_status.value,
                        _json_dumps(row.warnings),
                    ),
                )
                raw_row_id = cursor.fetchone()[0]
                persisted_rows.append(PersistedRawRow(raw_row_id=int(raw_row_id), row=row))
        self.connection.commit()
        return persisted_rows

    def save_canonical_games(self, games: list[CanonicalGame]) -> list[PersistedCanonicalGame]:
        if not games:
            return []
        persisted_games: list[PersistedCanonicalGame] = []
        with self.connection.cursor() as cursor:
            for game in games:
                cursor.execute(
                    """
                    INSERT INTO canonical_game (
                        season_id,
                        game_date,
                        home_team_id,
                        away_team_id,
                        home_score,
                        away_score,
                        final_home_margin,
                        final_total_points,
                        total_line,
                        home_spread_line,
                        away_spread_line,
                        reconciliation_status,
                        source_row_indexes_json,
                        warning_codes_json
                    )
                    VALUES (
                        (SELECT id FROM season WHERE label = %s),
                        %s,
                        (SELECT id FROM team WHERE code = %s),
                        (SELECT id FROM team WHERE code = %s),
                        %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb
                    )
                    ON CONFLICT (season_id, game_date, home_team_id, away_team_id)
                    DO UPDATE SET
                        home_score = EXCLUDED.home_score,
                        away_score = EXCLUDED.away_score,
                        final_home_margin = EXCLUDED.final_home_margin,
                        final_total_points = EXCLUDED.final_total_points,
                        total_line = EXCLUDED.total_line,
                        home_spread_line = EXCLUDED.home_spread_line,
                        away_spread_line = EXCLUDED.away_spread_line,
                        reconciliation_status = EXCLUDED.reconciliation_status,
                        source_row_indexes_json = EXCLUDED.source_row_indexes_json,
                        warning_codes_json = EXCLUDED.warning_codes_json,
                        updated_at = NOW()
                    RETURNING id
                    """,
                    (
                        game.season_label,
                        game.game_date,
                        game.home_team_code,
                        game.away_team_code,
                        game.home_score,
                        game.away_score,
                        game.final_home_margin,
                        game.final_total_points,
                        game.total_line,
                        game.home_spread_line,
                        game.away_spread_line,
                        game.reconciliation_status.value,
                        _json_dumps(game.source_row_indexes),
                        _json_dumps(game.warnings),
                    ),
                )
                canonical_game_id = cursor.fetchone()[0]
                persisted_games.append(
                    PersistedCanonicalGame(canonical_game_id=int(canonical_game_id), game=game)
                )
        self.connection.commit()
        return persisted_games

    def save_game_metrics(self, metrics_by_game_id: list[tuple[int, GameMetric]]) -> int:
        if not metrics_by_game_id:
            return 0
        with self.connection.cursor() as cursor:
            for canonical_game_id, metric in metrics_by_game_id:
                cursor.execute(
                    """
                    INSERT INTO game_metric (
                        canonical_game_id,
                        spread_error_home,
                        spread_error_away,
                        total_error,
                        home_covered,
                        away_covered,
                        went_over,
                        went_under
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (canonical_game_id)
                    DO UPDATE SET
                        spread_error_home = EXCLUDED.spread_error_home,
                        spread_error_away = EXCLUDED.spread_error_away,
                        total_error = EXCLUDED.total_error,
                        home_covered = EXCLUDED.home_covered,
                        away_covered = EXCLUDED.away_covered,
                        went_over = EXCLUDED.went_over,
                        went_under = EXCLUDED.went_under
                    """,
                    (
                        canonical_game_id,
                        metric.spread_error_home,
                        metric.spread_error_away,
                        metric.total_error,
                        metric.home_covered,
                        metric.away_covered,
                        metric.went_over,
                        metric.went_under,
                    ),
                )
        self.connection.commit()
        return len(metrics_by_game_id)

    def complete_job_run(self, *, job_id: int, summary: dict[str, Any], status: str) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE job_run
                SET status = %s, summary_json = %s::jsonb, completed_at = NOW()
                WHERE id = %s
                """,
                (status, _json_dumps(summary), job_id),
            )
            cursor.execute(
                """
                INSERT INTO job_run_reporting_snapshot (
                    job_run_id,
                    job_name,
                    status,
                    run_label,
                    provider_name,
                    team_code,
                    season_label,
                    started_at,
                    completed_at,
                    raw_rows_saved,
                    canonical_games_saved,
                    metrics_saved,
                    quality_issues_saved,
                    warning_count,
                    updated_at
                )
                SELECT
                    jr.id,
                    jr.job_name,
                    jr.status,
                    jr.payload_json ->> 'run_label',
                    jr.payload_json ->> 'provider',
                    jr.payload_json ->> 'team_code',
                    jr.payload_json ->> 'season_label',
                    jr.started_at,
                    jr.completed_at,
                    COALESCE((jr.summary_json ->> 'raw_rows_saved')::int, 0),
                    COALESCE((jr.summary_json ->> 'canonical_games_saved')::int, 0),
                    COALESCE((jr.summary_json ->> 'metrics_saved')::int, 0),
                    COALESCE((jr.summary_json ->> 'quality_issues_saved')::int, 0),
                    COALESCE((jr.summary_json ->> 'warning_count')::int, 0),
                    NOW()
                FROM job_run jr
                WHERE jr.id = %s
                ON CONFLICT (job_run_id)
                DO UPDATE SET
                    job_name = EXCLUDED.job_name,
                    status = EXCLUDED.status,
                    run_label = EXCLUDED.run_label,
                    provider_name = EXCLUDED.provider_name,
                    team_code = EXCLUDED.team_code,
                    season_label = EXCLUDED.season_label,
                    started_at = EXCLUDED.started_at,
                    completed_at = EXCLUDED.completed_at,
                    raw_rows_saved = EXCLUDED.raw_rows_saved,
                    canonical_games_saved = EXCLUDED.canonical_games_saved,
                    metrics_saved = EXCLUDED.metrics_saved,
                    quality_issues_saved = EXCLUDED.quality_issues_saved,
                    warning_count = EXCLUDED.warning_count,
                    updated_at = NOW()
                """,
                (job_id,),
            )
            cursor.execute(
                """
                INSERT INTO job_run_quality_snapshot (
                    job_run_id,
                    job_name,
                    run_label,
                    provider_name,
                    team_code,
                    season_label,
                    started_at,
                    completed_at,
                    parse_valid_count,
                    parse_invalid_count,
                    parse_warning_count,
                    reconciliation_full_match_count,
                    reconciliation_partial_single_row_count,
                    reconciliation_conflict_score_count,
                    reconciliation_conflict_total_line_count,
                    reconciliation_conflict_spread_line_count,
                    quality_issue_warning_count,
                    quality_issue_error_count,
                    updated_at
                )
                SELECT
                    jr.id,
                    jr.job_name,
                    jr.payload_json ->> 'run_label',
                    jr.payload_json ->> 'provider',
                    jr.payload_json ->> 'team_code',
                    jr.payload_json ->> 'season_label',
                    jr.started_at,
                    jr.completed_at,
                    COALESCE((jr.summary_json -> 'parse_status_counts' ->> 'VALID')::int, 0),
                    COALESCE((jr.summary_json -> 'parse_status_counts' ->> 'INVALID')::int, 0),
                    COALESCE(
                        (jr.summary_json -> 'parse_status_counts' ->> 'VALID_WITH_WARNINGS')::int,
                        0
                    ),
                    COALESCE(
                        (jr.summary_json -> 'reconciliation_status_counts' ->> 'FULL_MATCH')::int,
                        0
                    ),
                    COALESCE(
                        (
                            jr.summary_json
                            -> 'reconciliation_status_counts'
                            ->> 'PARTIAL_SINGLE_ROW'
                        )::int,
                        0
                    ),
                    COALESCE(
                        (
                            jr.summary_json
                            -> 'reconciliation_status_counts'
                            ->> 'CONFLICT_SCORE'
                        )::int,
                        0
                    ),
                    COALESCE(
                        (
                            jr.summary_json
                            -> 'reconciliation_status_counts'
                            ->> 'CONFLICT_TOTAL_LINE'
                        )::int,
                        0
                    ),
                    COALESCE(
                        (
                            jr.summary_json
                            -> 'reconciliation_status_counts'
                            ->> 'CONFLICT_SPREAD_LINE'
                        )::int,
                        0
                    ),
                    COALESCE(
                        (
                            jr.summary_json
                            -> 'data_quality_issue_severity_counts'
                            ->> 'warning'
                        )::int,
                        0
                    ),
                    COALESCE(
                        (
                            jr.summary_json
                            -> 'data_quality_issue_severity_counts'
                            ->> 'error'
                        )::int,
                        0
                    ),
                    NOW()
                FROM job_run jr
                WHERE jr.id = %s
                ON CONFLICT (job_run_id)
                DO UPDATE SET
                    job_name = EXCLUDED.job_name,
                    run_label = EXCLUDED.run_label,
                    provider_name = EXCLUDED.provider_name,
                    team_code = EXCLUDED.team_code,
                    season_label = EXCLUDED.season_label,
                    started_at = EXCLUDED.started_at,
                    completed_at = EXCLUDED.completed_at,
                    parse_valid_count = EXCLUDED.parse_valid_count,
                    parse_invalid_count = EXCLUDED.parse_invalid_count,
                    parse_warning_count = EXCLUDED.parse_warning_count,
                    reconciliation_full_match_count = (
                        EXCLUDED.reconciliation_full_match_count
                    ),
                    reconciliation_partial_single_row_count = (
                        EXCLUDED.reconciliation_partial_single_row_count
                    ),
                    reconciliation_conflict_score_count = (
                        EXCLUDED.reconciliation_conflict_score_count
                    ),
                    reconciliation_conflict_total_line_count = (
                        EXCLUDED.reconciliation_conflict_total_line_count
                    ),
                    reconciliation_conflict_spread_line_count = (
                        EXCLUDED.reconciliation_conflict_spread_line_count
                    ),
                    quality_issue_warning_count = EXCLUDED.quality_issue_warning_count,
                    quality_issue_error_count = EXCLUDED.quality_issue_error_count,
                    updated_at = NOW()
                """,
                (job_id,),
            )
        self.connection.commit()

    def list_job_runs(
        self,
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
        return list_job_runs_query(
            self.connection,
            limit=limit,
            offset=offset,
            status=status,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
            started_from=started_from,
            started_to=started_to,
        )

    def list_job_run_daily_summaries(
        self,
        *,
        status: str | None = None,
        provider_name: str | None = None,
        team_code: str | None = None,
        season_label: str | None = None,
        run_label: str | None = None,
        started_from: datetime | None = None,
        started_to: datetime | None = None,
    ) -> list[DailyJobRunSummary]:
        return list_job_run_daily_summaries_query(
            self.connection,
            status=status,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
            started_from=started_from,
            started_to=started_to,
        )

    def list_page_retrieval_daily_summaries(
        self,
        *,
        status: str | None = None,
        provider_name: str | None = None,
        team_code: str | None = None,
        season_label: str | None = None,
        run_label: str | None = None,
        retrieved_from: datetime | None = None,
        retrieved_to: datetime | None = None,
    ) -> list[DailyPageRetrievalSummary]:
        return list_page_retrieval_daily_summaries_query(
            self.connection,
            status=status,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
            retrieved_from=retrieved_from,
            retrieved_to=retrieved_to,
        )

    def list_job_run_quality_daily_summaries(
        self,
        *,
        provider_name: str | None = None,
        team_code: str | None = None,
        season_label: str | None = None,
        run_label: str | None = None,
        started_from: datetime | None = None,
        started_to: datetime | None = None,
    ) -> list[DailyJobRunQualitySummary]:
        return list_job_run_quality_daily_summaries_query(
            self.connection,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
            started_from=started_from,
            started_to=started_to,
        )

    def list_page_retrievals(
        self,
        *,
        limit: int = 20,
        offset: int = 0,
        status: str | None = None,
        provider_name: str | None = None,
        team_code: str | None = None,
        season_label: str | None = None,
        run_label: str | None = None,
    ) -> list[PageRetrievalSnapshot]:
        return list_page_retrievals_query(
            self.connection,
            limit=limit,
            offset=offset,
            status=status,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
        )

    def save_data_quality_issues(self, issues: list[DataQualityIssueRecord]) -> int:
        if not issues:
            return 0
        self._ensure_data_quality_issue_identity_schema()
        with self.connection.cursor() as cursor:
            for issue in issues:
                cursor.execute(
                    """
                    INSERT INTO data_quality_issue (
                        issue_type,
                        severity,
                        raw_team_game_row_id,
                        canonical_game_id,
                        details_json
                    )
                    VALUES (%s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (
                        issue_type,
                        COALESCE(raw_team_game_row_id, 0),
                        COALESCE(canonical_game_id, 0)
                    )
                    DO UPDATE SET
                        severity = EXCLUDED.severity,
                        details_json = EXCLUDED.details_json
                    """,
                    (
                        issue.issue_type,
                        issue.severity,
                        issue.raw_team_game_row_id,
                        issue.canonical_game_id,
                        _json_dumps(issue.details),
                    ),
                )
        self.connection.commit()
        return len(issues)

    def list_data_quality_issues(
        self,
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
        return list_data_quality_issues_query(
            self.connection,
            limit=limit,
            offset=offset,
            severity=severity,
            issue_type=issue_type,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
        )

    def get_parse_status_counts(
        self,
        *,
        provider_name: str | None = None,
        team_code: str | None = None,
        season_label: str | None = None,
        run_label: str | None = None,
    ) -> dict[str, int]:
        return get_parse_status_counts_query(
            self.connection,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
        )

    def get_reconciliation_status_counts(
        self,
        *,
        provider_name: str | None = None,
        team_code: str | None = None,
        season_label: str | None = None,
        run_label: str | None = None,
    ) -> dict[str, int]:
        return get_reconciliation_status_counts_query(
            self.connection,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
        )

    def get_data_quality_issue_type_counts(
        self,
        *,
        provider_name: str | None = None,
        team_code: str | None = None,
        season_label: str | None = None,
        run_label: str | None = None,
    ) -> dict[str, int]:
        return get_data_quality_issue_type_counts_query(
            self.connection,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
        )

    def get_data_quality_issue_severity_counts(
        self,
        *,
        provider_name: str | None = None,
        team_code: str | None = None,
        season_label: str | None = None,
        run_label: str | None = None,
    ) -> dict[str, int]:
        return get_data_quality_issue_severity_counts_query(
            self.connection,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
        )

    def normalize_data_quality_issue_taxonomy(
        self,
        *,
        provider_name: str | None = None,
        team_code: str | None = None,
        season_label: str | None = None,
        dry_run: bool = True,
    ) -> dict[str, int]:
        return normalize_data_quality_issue_taxonomy_query(
            self.connection,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            dry_run=dry_run,
        )

    def _ensure_raw_row_source_identity_schema(self) -> None:
        if self._raw_row_source_identity_ready:
            return
        if self.allow_runtime_schema_mutation:
            ensure_raw_row_source_identity_schema(self.connection)
        elif not verify_raw_row_source_identity_schema(self.connection):
            raise RuntimeError(
                "PostgreSQL raw row source identity schema is not ready. "
                "Apply the bootstrap or run an explicit maintenance/bootstrap flow "
                "before postgres-backed ingestion writes."
            )
        self._raw_row_source_identity_ready = True

    def _ensure_data_quality_issue_identity_schema(self) -> None:
        if self._data_quality_issue_identity_ready:
            return
        if self.allow_runtime_schema_mutation:
            ensure_data_quality_issue_identity_schema(self.connection)
        elif not verify_data_quality_issue_identity_schema(self.connection):
            raise RuntimeError(
                "PostgreSQL data quality issue identity schema is not ready. "
                "Apply the bootstrap or run an explicit maintenance/bootstrap flow "
                "before postgres-backed quality issue writes."
            )
        self._data_quality_issue_identity_ready = True

