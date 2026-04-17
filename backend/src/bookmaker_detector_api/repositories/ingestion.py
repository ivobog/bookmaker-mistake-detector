from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from bookmaker_detector_api.data_quality_taxonomy import (
    canonical_issue_type,
    issue_type_filter_variants,
    merge_issue_type_counts,
    normalize_issue_type_and_severity,
    severity_counts_from_issue_type_counts,
)
from bookmaker_detector_api.ingestion.models import CanonicalGame, GameMetric, RawGameRow
from bookmaker_detector_api.repositories.ingestion_in_memory_support import (
    bucket_date_from_datetime,
    canonical_game_matches_scope,
    issue_matches_scope,
    list_page_retrieval_reporting_snapshots,
    list_quality_snapshots,
    list_reporting_snapshots,
    normalize_issue_record,
    raw_row_matches_scope,
    upsert_quality_snapshot,
    upsert_reporting_snapshot,
)
from bookmaker_detector_api.repositories.ingestion_postgres_support import (
    build_canonical_scope_where_clauses,
    build_issue_scope_where_clauses,
    build_raw_row_scope_where_clauses,
    count_by_column,
    normalize_issue_record as normalize_postgres_issue_record,
    select_data_quality_issue_rows_for_normalization,
)
from bookmaker_detector_api.repositories.ingestion_types import (
    DailyJobRunQualitySummary,
    DailyJobRunSummary,
    DailyPageRetrievalSummary,
    DataQualityIssueRecord,
    IngestionRepository,
    JobRunRecord,
    PageRetrievalRecord,
    PageRetrievalSnapshot,
    PersistedCanonicalGame,
    PersistedIngestionRun,
    PersistedRawRow,
)


class InMemoryIngestionRepository:
    def __init__(self) -> None:
        self.job_runs: list[dict[str, Any]] = []
        self.job_run_reporting_snapshots: list[dict[str, Any]] = []
        self.job_run_quality_snapshots: list[dict[str, Any]] = []
        self.page_retrievals: list[dict[str, Any]] = []
        self.page_retrieval_reporting_snapshots: list[dict[str, Any]] = []
        self.raw_rows: list[dict[str, Any]] = []
        self.canonical_games: list[dict[str, Any]] = []
        self.metrics: list[dict[str, Any]] = []
        self.data_quality_issues: list[dict[str, Any]] = []
        self.feature_versions: list[dict[str, Any]] = []
        self.feature_snapshots: list[dict[str, Any]] = []
        self.feature_analysis_artifacts: list[dict[str, Any]] = []
        self.model_registries: list[dict[str, Any]] = []
        self.model_training_runs: list[dict[str, Any]] = []
        self.model_evaluation_snapshots: list[dict[str, Any]] = []
        self.model_selection_snapshots: list[dict[str, Any]] = []
        self.model_market_boards: list[dict[str, Any]] = []
        self.model_market_board_source_runs: list[dict[str, Any]] = []
        self.model_market_board_refresh_events: list[dict[str, Any]] = []
        self.model_market_board_refresh_batches: list[dict[str, Any]] = []
        self.model_market_board_scoring_batches: list[dict[str, Any]] = []
        self.model_market_board_cadence_batches: list[dict[str, Any]] = []
        self.model_scoring_runs: list[dict[str, Any]] = []
        self.model_opportunities: list[dict[str, Any]] = []
        self.model_backtest_runs: list[dict[str, Any]] = []

    def create_job_run(self, *, job_name: str, requested_by: str, payload: dict[str, Any]) -> int:
        job_id = len(self.job_runs) + 1
        self.job_runs.append(
            {
                "id": job_id,
                "job_name": job_name,
                "requested_by": requested_by,
                "payload": payload,
                "status": "RUNNING",
                "started_at": datetime.now(timezone.utc),
                "completed_at": None,
            }
        )
        return job_id

    def create_page_retrieval(self, *, job_id: int, record: PageRetrievalRecord) -> int:
        retrieval_id = len(self.page_retrievals) + 1
        retrieved_at = datetime.now(timezone.utc)
        job_payload = next(
            (entry.get("payload", {}) for entry in self.job_runs if entry["id"] == job_id),
            {},
        )
        self.page_retrievals.append(
            {
                "id": retrieval_id,
                "job_id": job_id,
                "record": record,
                "retrieved_at": retrieved_at,
            }
        )
        self.page_retrieval_reporting_snapshots.append(
            {
                "page_retrieval_id": retrieval_id,
                "job_run_id": job_id,
                "run_label": job_payload.get("run_label"),
                "provider_name": record.provider_name,
                "team_code": record.team_code,
                "season_label": record.season_label,
                "source_url": record.source_url,
                "status": record.status,
                "http_status": record.http_status,
                "payload_storage_path": record.payload_storage_path,
                "error_message": record.error_message,
                "retrieved_at": retrieved_at,
            }
        )
        return retrieval_id

    def save_raw_rows(
        self,
        *,
        page_retrieval_id: int,
        rows: list[RawGameRow],
    ) -> list[PersistedRawRow]:
        persisted_rows: list[PersistedRawRow] = []
        for row in rows:
            raw_row_id = len(self.raw_rows) + 1
            self.raw_rows.append(
                {"id": raw_row_id, "page_retrieval_id": page_retrieval_id, **row.as_dict()}
            )
            persisted_rows.append(PersistedRawRow(raw_row_id=raw_row_id, row=row))
        return persisted_rows

    def save_canonical_games(self, games: list[CanonicalGame]) -> list[PersistedCanonicalGame]:
        persisted_games: list[PersistedCanonicalGame] = []
        for game in games:
            canonical_game_id = len(self.canonical_games) + 1
            self.canonical_games.append({"id": canonical_game_id, **game.as_dict()})
            persisted_games.append(
                PersistedCanonicalGame(canonical_game_id=canonical_game_id, game=game)
            )
        return persisted_games

    def save_game_metrics(self, metrics_by_game_id: list[tuple[int, GameMetric]]) -> int:
        self.metrics.extend(
            {"canonical_game_id": canonical_game_id, **metric.as_dict()}
            for canonical_game_id, metric in metrics_by_game_id
        )
        return len(metrics_by_game_id)

    def complete_job_run(self, *, job_id: int, summary: dict[str, Any], status: str) -> None:
        job = next(run for run in self.job_runs if run["id"] == job_id)
        job["summary"] = summary
        job["status"] = status
        job["completed_at"] = datetime.now(timezone.utc)
        upsert_reporting_snapshot(
            self.job_run_reporting_snapshots,
            self.job_run_quality_snapshots,
            job=job,
        )

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
        selected = [
            entry
            for entry in self.job_runs
            if (status is None or entry["status"] == status)
            and (
                provider_name is None
                or entry.get("payload", {}).get("provider") == provider_name
            )
            and (
                team_code is None
                or entry.get("payload", {}).get("team_code") == team_code
            )
            and (
                season_label is None
                or entry.get("payload", {}).get("season_label") == season_label
            )
            and (
                run_label is None
                or entry.get("payload", {}).get("run_label") == run_label
            )
            and (
                started_from is None
                or (
                    entry.get("started_at") is not None
                    and entry["started_at"] >= started_from
                )
            )
            and (
                started_to is None
                or (
                    entry.get("started_at") is not None
                    and entry["started_at"] <= started_to
                )
            )
        ]
        selected = list(reversed(selected))[offset : offset + limit]
        return [
            JobRunRecord(
                id=entry["id"],
                job_name=entry["job_name"],
                status=entry["status"],
                requested_by=entry["requested_by"],
                payload=entry.get("payload", {}),
                summary=entry.get("summary", {}),
                started_at=entry.get("started_at"),
                completed_at=entry.get("completed_at"),
            )
            for entry in selected
        ]

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
        selected_entries = self.page_retrievals
        if status is not None:
            selected_entries = [
                entry for entry in selected_entries if entry["record"].status == status
            ]
        if provider_name is not None:
            selected_entries = [
                entry
                for entry in selected_entries
                if entry["record"].provider_name == provider_name
            ]
        if team_code is not None:
            selected_entries = [
                entry
                for entry in selected_entries
                if entry["record"].team_code == team_code
            ]
        if season_label is not None:
            selected_entries = [
                entry
                for entry in selected_entries
                if entry["record"].season_label == season_label
            ]
        if run_label is not None:
            selected_entries = [
                entry
                for entry in selected_entries
                if next(
                    (
                        job.get("payload", {}).get("run_label")
                        for job in self.job_runs
                        if job["id"] == entry["job_id"]
                    ),
                    None,
                )
                == run_label
            ]
        selected_entries = list(reversed(selected_entries))[offset : offset + limit]
        return [
            PageRetrievalSnapshot(
                id=entry["id"],
                job_id=entry["job_id"],
                run_label=next(
                    (
                        job.get("payload", {}).get("run_label")
                        for job in self.job_runs
                        if job["id"] == entry["job_id"]
                    ),
                    None,
                ),
                provider_name=entry["record"].provider_name,
                team_code=entry["record"].team_code,
                season_label=entry["record"].season_label,
                source_url=entry["record"].source_url,
                status=entry["record"].status,
                http_status=entry["record"].http_status,
                payload_storage_path=entry["record"].payload_storage_path,
                error_message=entry["record"].error_message,
                retrieved_at=entry.get("retrieved_at"),
            )
            for entry in selected_entries
        ]

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
        buckets: dict[str, dict[str, int | str]] = {}
        for snapshot in list_reporting_snapshots(
            self.job_run_reporting_snapshots,
            status=status,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
            started_from=started_from,
            started_to=started_to,
        ):
            bucket_date = bucket_date_from_datetime(snapshot.started_at)
            if bucket_date not in buckets:
                buckets[bucket_date] = {
                    "date": bucket_date,
                    "job_count": 0,
                    "completed_jobs": 0,
                    "failed_jobs": 0,
                    "raw_rows_saved": 0,
                    "canonical_games_saved": 0,
                    "metrics_saved": 0,
                    "quality_issues_saved": 0,
                    "warning_count": 0,
                }
            bucket = buckets[bucket_date]
            bucket["job_count"] = int(bucket["job_count"]) + 1
            if snapshot.status == "COMPLETED":
                bucket["completed_jobs"] = int(bucket["completed_jobs"]) + 1
            if snapshot.status == "FAILED":
                bucket["failed_jobs"] = int(bucket["failed_jobs"]) + 1
            bucket["raw_rows_saved"] = int(bucket["raw_rows_saved"]) + snapshot.raw_rows_saved
            bucket["canonical_games_saved"] = (
                int(bucket["canonical_games_saved"]) + snapshot.canonical_games_saved
            )
            bucket["metrics_saved"] = int(bucket["metrics_saved"]) + snapshot.metrics_saved
            bucket["quality_issues_saved"] = (
                int(bucket["quality_issues_saved"]) + snapshot.quality_issues_saved
            )
            bucket["warning_count"] = int(bucket["warning_count"]) + snapshot.warning_count
        return [
            DailyJobRunSummary(**bucket)
            for bucket in sorted(buckets.values(), key=lambda item: item["date"])
        ]

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
        buckets: dict[str, dict[str, int | str]] = {}
        for snapshot in list_page_retrieval_reporting_snapshots(
            self.page_retrieval_reporting_snapshots,
            status=status,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
            retrieved_from=retrieved_from,
            retrieved_to=retrieved_to,
        ):
            bucket_date = bucket_date_from_datetime(snapshot["retrieved_at"])
            if bucket_date not in buckets:
                buckets[bucket_date] = {
                    "date": bucket_date,
                    "retrieval_count": 0,
                    "successful_retrievals": 0,
                    "failed_retrievals": 0,
                    "payload_saved_count": 0,
                    "missing_http_status_count": 0,
                }
            bucket = buckets[bucket_date]
            bucket["retrieval_count"] = int(bucket["retrieval_count"]) + 1
            if snapshot["status"] == "SUCCESS":
                bucket["successful_retrievals"] = int(bucket["successful_retrievals"]) + 1
            if snapshot["status"] == "FAILED":
                bucket["failed_retrievals"] = int(bucket["failed_retrievals"]) + 1
            if snapshot["payload_storage_path"] is not None:
                bucket["payload_saved_count"] = int(bucket["payload_saved_count"]) + 1
            if snapshot["http_status"] is None:
                bucket["missing_http_status_count"] = int(bucket["missing_http_status_count"]) + 1
        return [
            DailyPageRetrievalSummary(**bucket)
            for bucket in sorted(buckets.values(), key=lambda item: item["date"])
        ]

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
        buckets: dict[str, dict[str, int | str]] = {}
        for snapshot in list_quality_snapshots(
            self.job_run_quality_snapshots,
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
            started_from=started_from,
            started_to=started_to,
        ):
            bucket_date = bucket_date_from_datetime(snapshot["started_at"])
            if bucket_date not in buckets:
                buckets[bucket_date] = {
                    "date": bucket_date,
                    "job_count": 0,
                    "parse_valid_count": 0,
                    "parse_invalid_count": 0,
                    "parse_warning_count": 0,
                    "reconciliation_full_match_count": 0,
                    "reconciliation_partial_single_row_count": 0,
                    "reconciliation_conflict_score_count": 0,
                    "reconciliation_conflict_total_line_count": 0,
                    "reconciliation_conflict_spread_line_count": 0,
                    "quality_issue_warning_count": 0,
                    "quality_issue_error_count": 0,
                }
            bucket = buckets[bucket_date]
            bucket["job_count"] = int(bucket["job_count"]) + 1
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
            ):
                bucket[field_name] = int(bucket[field_name]) + int(snapshot[field_name])
        return [
            DailyJobRunQualitySummary(**bucket)
            for bucket in sorted(buckets.values(), key=lambda item: item["date"])
        ]

    def save_data_quality_issues(self, issues: list[DataQualityIssueRecord]) -> int:
        for issue in issues:
            issue_id = len(self.data_quality_issues) + 1
            self.data_quality_issues.append(
                {
                    "id": issue_id,
                    "issue_type": issue.issue_type,
                    "severity": issue.severity,
                    "raw_team_game_row_id": issue.raw_team_game_row_id,
                    "canonical_game_id": issue.canonical_game_id,
                    "details": issue.details,
                }
            )
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
        selected_issues = [
            issue
            for issue in self.data_quality_issues
            if (
                severity is None
                or normalize_issue_type_and_severity(
                    issue["issue_type"],
                    issue["severity"],
                )[1]
                == severity
            )
            and (
                issue_type is None
                or canonical_issue_type(issue["issue_type"]) == canonical_issue_type(issue_type)
            )
            and issue_matches_scope(
                issue,
                raw_rows=self.raw_rows,
                canonical_games=self.canonical_games,
                page_retrievals=self.page_retrievals,
                job_runs=self.job_runs,
                provider_name=provider_name,
                team_code=team_code,
                season_label=season_label,
                run_label=run_label,
            )
        ]
        selected_issues = list(reversed(selected_issues))[offset : offset + limit]
        return [
            normalize_issue_record(
                DataQualityIssueRecord(
                    id=issue["id"],
                    issue_type=issue["issue_type"],
                    severity=issue["severity"],
                    raw_team_game_row_id=issue["raw_team_game_row_id"],
                    canonical_game_id=issue["canonical_game_id"],
                    details=issue["details"],
                )
            )
            for issue in selected_issues
        ]

    def get_parse_status_counts(
        self,
        *,
        provider_name: str | None = None,
        team_code: str | None = None,
        season_label: str | None = None,
        run_label: str | None = None,
    ) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in self.raw_rows:
            if not raw_row_matches_scope(
                row,
                page_retrievals=self.page_retrievals,
                job_runs=self.job_runs,
                provider_name=provider_name,
                team_code=team_code,
                season_label=season_label,
                run_label=run_label,
            ):
                continue
            parse_status = row["parse_status"]
            counts[parse_status] = counts.get(parse_status, 0) + 1
        return counts

    def get_reconciliation_status_counts(
        self,
        *,
        provider_name: str | None = None,
        team_code: str | None = None,
        season_label: str | None = None,
        run_label: str | None = None,
    ) -> dict[str, int]:
        counts: dict[str, int] = {}
        for game in self.canonical_games:
            if not canonical_game_matches_scope(
                game,
                raw_rows=self.raw_rows,
                page_retrievals=self.page_retrievals,
                job_runs=self.job_runs,
                provider_name=provider_name,
                team_code=team_code,
                season_label=season_label,
                run_label=run_label,
            ):
                continue
            reconciliation_status = game["reconciliation_status"]
            counts[reconciliation_status] = counts.get(reconciliation_status, 0) + 1
        return counts

    def get_data_quality_issue_type_counts(
        self,
        *,
        provider_name: str | None = None,
        team_code: str | None = None,
        season_label: str | None = None,
        run_label: str | None = None,
    ) -> dict[str, int]:
        counts: dict[str, int] = {}
        for issue in self.data_quality_issues:
            if not issue_matches_scope(
                issue,
                raw_rows=self.raw_rows,
                canonical_games=self.canonical_games,
                page_retrievals=self.page_retrievals,
                job_runs=self.job_runs,
                provider_name=provider_name,
                team_code=team_code,
                season_label=season_label,
                run_label=run_label,
            ):
                continue
            issue_type = issue["issue_type"]
            counts[issue_type] = counts.get(issue_type, 0) + 1
        return merge_issue_type_counts(counts)

    def get_data_quality_issue_severity_counts(
        self,
        *,
        provider_name: str | None = None,
        team_code: str | None = None,
        season_label: str | None = None,
        run_label: str | None = None,
    ) -> dict[str, int]:
        issue_type_counts = self.get_data_quality_issue_type_counts(
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
        )
        return severity_counts_from_issue_type_counts(issue_type_counts)

    def normalize_data_quality_issue_taxonomy(
        self,
        *,
        provider_name: str | None = None,
        team_code: str | None = None,
        season_label: str | None = None,
        dry_run: bool = True,
    ) -> dict[str, int]:
        matched_rows = 0
        issue_type_updates = 0
        severity_updates = 0
        updated_rows = 0

        for issue in self.data_quality_issues:
            if not issue_matches_scope(
                issue,
                raw_rows=self.raw_rows,
                canonical_games=self.canonical_games,
                page_retrievals=self.page_retrievals,
                job_runs=self.job_runs,
                provider_name=provider_name,
                team_code=team_code,
                season_label=season_label,
                run_label=None,
            ):
                continue
            matched_rows += 1
            normalized_issue_type, normalized_severity = normalize_issue_type_and_severity(
                issue["issue_type"],
                issue["severity"],
            )
            issue_type_changed = issue["issue_type"] != normalized_issue_type
            severity_changed = issue["severity"] != normalized_severity
            if issue_type_changed:
                issue_type_updates += 1
            if severity_changed:
                severity_updates += 1
            if issue_type_changed or severity_changed:
                updated_rows += 1
                if not dry_run:
                    issue["issue_type"] = normalized_issue_type
                    issue["severity"] = normalized_severity

        return {
            "matched_rows": matched_rows,
            "updated_rows": updated_rows,
            "issue_type_updates": issue_type_updates,
            "severity_updates": severity_updates,
        }

class PostgresIngestionRepository:
    def __init__(self, connection: Any) -> None:
        self.connection = connection

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
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
                    )
                    ON CONFLICT (provider_id, team_id, season_id, source_url, source_row_index)
                    DO UPDATE SET
                        page_retrieval_id = EXCLUDED.page_retrieval_id,
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

        with self.connection.cursor() as cursor:
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

        with self.connection.cursor() as cursor:
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

        with self.connection.cursor() as cursor:
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
        self,
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

        with self.connection.cursor() as cursor:
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

        with self.connection.cursor() as cursor:
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

    def save_data_quality_issues(self, issues: list[DataQualityIssueRecord]) -> int:
        if not issues:
            return 0
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

        with self.connection.cursor() as cursor:
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
        self,
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
        return count_by_column(self.connection, query=query, params=params)

    def get_reconciliation_status_counts(
        self,
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
        return count_by_column(self.connection, query=query, params=params)

    def get_data_quality_issue_type_counts(
        self,
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
        return merge_issue_type_counts(count_by_column(self.connection, query=query, params=params))

    def get_data_quality_issue_severity_counts(
        self,
        *,
        provider_name: str | None = None,
        team_code: str | None = None,
        season_label: str | None = None,
        run_label: str | None = None,
    ) -> dict[str, int]:
        issue_type_counts = self.get_data_quality_issue_type_counts(
            provider_name=provider_name,
            team_code=team_code,
            season_label=season_label,
            run_label=run_label,
        )
        return severity_counts_from_issue_type_counts(issue_type_counts)

    def normalize_data_quality_issue_taxonomy(
        self,
        *,
        provider_name: str | None = None,
        team_code: str | None = None,
        season_label: str | None = None,
        dry_run: bool = True,
    ) -> dict[str, int]:
        selected_issues = select_data_quality_issue_rows_for_normalization(
            self.connection,
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
            with self.connection.cursor() as cursor:
                cursor.executemany(
                    """
                    UPDATE data_quality_issue
                    SET issue_type = %s, severity = %s
                    WHERE id = %s
                    """,
                    updates,
                )
            self.connection.commit()

        return {
            "matched_rows": len(selected_issues),
            "updated_rows": len(updates),
            "issue_type_updates": issue_type_updates,
            "severity_updates": severity_updates,
        }

def _json_dumps(payload: Any) -> str:
    import json

    return json.dumps(payload, default=str)
