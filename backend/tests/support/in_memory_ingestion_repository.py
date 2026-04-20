from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from bookmaker_detector_api.data_quality_taxonomy import (
    canonical_issue_type,
    merge_issue_type_counts,
    normalize_issue_type_and_severity,
    severity_counts_from_issue_type_counts,
)
from bookmaker_detector_api.ingestion.models import CanonicalGame, GameMetric, RawGameRow
from tests.support.in_memory_ingestion_support import (
    bucket_date_from_datetime,
    canonical_game_matches_scope,
    issue_matches_scope,
    list_page_retrieval_reporting_snapshots,
    list_quality_snapshots,
    list_reporting_snapshots,
    normalize_issue_record,
    raw_row_matches_scope,
    upsert_reporting_snapshot,
)
from bookmaker_detector_api.repositories.ingestion_types import (
    DailyJobRunQualitySummary,
    DailyJobRunSummary,
    DailyPageRetrievalSummary,
    DataQualityIssueRecord,
    JobRunRecord,
    PageRetrievalRecord,
    PageRetrievalSnapshot,
    PersistedCanonicalGame,
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
            raw_row_key = (
                row.provider_name,
                row.team_code,
                row.season_label,
                row.source_page_url or row.source_url,
                row.source_page_season_label or row.season_label,
                row.source_section,
                row.source_row_index,
            )
            existing_row = next(
                (
                    entry
                    for entry in self.raw_rows
                    if (
                        entry["provider_name"],
                        entry["team_code"],
                        entry["season_label"],
                        entry.get("source_page_url") or entry["source_url"],
                        entry.get("source_page_season_label") or entry["season_label"],
                        entry["source_section"],
                        entry["source_row_index"],
                    )
                    == raw_row_key
                ),
                None,
            )
            if existing_row is None:
                raw_row_id = len(self.raw_rows) + 1
                self.raw_rows.append(
                    {"id": raw_row_id, "page_retrieval_id": page_retrieval_id, **row.as_dict()}
                )
            else:
                raw_row_id = int(existing_row["id"])
                existing_row.update(
                    {
                        "page_retrieval_id": page_retrieval_id,
                        **row.as_dict(),
                    }
                )
            persisted_rows.append(PersistedRawRow(raw_row_id=raw_row_id, row=row))
        return persisted_rows

    def save_canonical_games(self, games: list[CanonicalGame]) -> list[PersistedCanonicalGame]:
        persisted_games: list[PersistedCanonicalGame] = []
        for game in games:
            canonical_game_key = (
                game.season_label,
                game.game_date,
                game.home_team_code,
                game.away_team_code,
            )
            existing_game = next(
                (
                    entry
                    for entry in self.canonical_games
                    if (
                        entry["season_label"],
                        entry["game_date"],
                        entry["home_team_code"],
                        entry["away_team_code"],
                    )
                    == canonical_game_key
                ),
                None,
            )
            if existing_game is None:
                canonical_game_id = len(self.canonical_games) + 1
                self.canonical_games.append({"id": canonical_game_id, **game.as_dict()})
            else:
                canonical_game_id = int(existing_game["id"])
                existing_game.update({"id": canonical_game_id, **game.as_dict()})
            persisted_games.append(
                PersistedCanonicalGame(canonical_game_id=canonical_game_id, game=game)
            )
        return persisted_games

    def save_game_metrics(self, metrics_by_game_id: list[tuple[int, GameMetric]]) -> int:
        for canonical_game_id, metric in metrics_by_game_id:
            existing_metric = next(
                (
                    entry
                    for entry in self.metrics
                    if entry["canonical_game_id"] == canonical_game_id
                ),
                None,
            )
            if existing_metric is None:
                self.metrics.append({"canonical_game_id": canonical_game_id, **metric.as_dict()})
            else:
                existing_metric.update({"canonical_game_id": canonical_game_id, **metric.as_dict()})
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
            and (provider_name is None or entry.get("payload", {}).get("provider") == provider_name)
            and (team_code is None or entry.get("payload", {}).get("team_code") == team_code)
            and (
                season_label is None or entry.get("payload", {}).get("season_label") == season_label
            )
            and (run_label is None or entry.get("payload", {}).get("run_label") == run_label)
            and (
                started_from is None
                or (entry.get("started_at") is not None and entry["started_at"] >= started_from)
            )
            and (
                started_to is None
                or (entry.get("started_at") is not None and entry["started_at"] <= started_to)
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
                entry for entry in selected_entries if entry["record"].team_code == team_code
            ]
        if season_label is not None:
            selected_entries = [
                entry for entry in selected_entries if entry["record"].season_label == season_label
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
            issue_key = (
                issue.issue_type,
                issue.raw_team_game_row_id,
                issue.canonical_game_id,
            )
            existing_issue = next(
                (
                    entry
                    for entry in self.data_quality_issues
                    if (
                        entry["issue_type"],
                        entry["raw_team_game_row_id"],
                        entry["canonical_game_id"],
                    )
                    == issue_key
                ),
                None,
            )
            if existing_issue is None:
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
            else:
                existing_issue.update(
                    {
                        "severity": issue.severity,
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
