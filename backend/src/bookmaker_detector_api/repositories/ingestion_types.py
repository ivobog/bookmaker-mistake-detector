from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from bookmaker_detector_api.ingestion.models import CanonicalGame, GameMetric, RawGameRow


@dataclass(slots=True)
class PageRetrievalRecord:
    provider_name: str
    team_code: str
    season_label: str
    source_url: str
    status: str
    http_status: int | None = None
    payload_storage_path: str | None = None
    error_message: str | None = None


@dataclass(slots=True)
class PersistedIngestionRun:
    job_id: int
    page_retrieval_id: int
    raw_rows_saved: int
    canonical_games_saved: int
    metrics_saved: int
    warnings: list[str] = field(default_factory=list)


@dataclass(slots=True)
class PersistedCanonicalGame:
    canonical_game_id: int
    game: CanonicalGame


@dataclass(slots=True)
class PersistedRawRow:
    raw_row_id: int
    row: RawGameRow


@dataclass(slots=True)
class JobRunRecord:
    id: int
    job_name: str
    status: str
    requested_by: str | None
    payload: dict[str, Any]
    summary: dict[str, Any]
    started_at: datetime | None = None
    completed_at: datetime | None = None


@dataclass(slots=True)
class PageRetrievalSnapshot:
    id: int
    job_id: int
    run_label: str | None
    provider_name: str
    team_code: str
    season_label: str
    source_url: str
    status: str
    http_status: int | None
    payload_storage_path: str | None
    error_message: str | None
    retrieved_at: datetime | None = None


@dataclass(slots=True)
class DailyJobRunSummary:
    date: str
    job_count: int
    completed_jobs: int
    failed_jobs: int
    raw_rows_saved: int
    canonical_games_saved: int
    metrics_saved: int
    quality_issues_saved: int
    warning_count: int


@dataclass(slots=True)
class JobRunReportingSnapshot:
    job_run_id: int
    job_name: str
    status: str
    run_label: str | None
    provider_name: str | None
    team_code: str | None
    season_label: str | None
    started_at: datetime | None
    completed_at: datetime | None
    raw_rows_saved: int
    canonical_games_saved: int
    metrics_saved: int
    quality_issues_saved: int
    warning_count: int


@dataclass(slots=True)
class DailyPageRetrievalSummary:
    date: str
    retrieval_count: int
    successful_retrievals: int
    failed_retrievals: int
    payload_saved_count: int
    missing_http_status_count: int


@dataclass(slots=True)
class DailyJobRunQualitySummary:
    date: str
    job_count: int
    parse_valid_count: int
    parse_invalid_count: int
    parse_warning_count: int
    reconciliation_full_match_count: int
    reconciliation_partial_single_row_count: int
    reconciliation_conflict_score_count: int
    reconciliation_conflict_total_line_count: int
    reconciliation_conflict_spread_line_count: int
    quality_issue_warning_count: int
    quality_issue_error_count: int


@dataclass(slots=True)
class DataQualityIssueRecord:
    id: int
    issue_type: str
    severity: str
    raw_team_game_row_id: int | None
    canonical_game_id: int | None
    details: dict[str, Any]


class IngestionRepository(Protocol):
    def create_job_run(self, *, job_name: str, requested_by: str, payload: dict[str, Any]) -> int:
        ...

    def create_page_retrieval(self, *, job_id: int, record: PageRetrievalRecord) -> int:
        ...

    def save_raw_rows(
        self,
        *,
        page_retrieval_id: int,
        rows: list[RawGameRow],
    ) -> list[PersistedRawRow]:
        ...

    def save_canonical_games(self, games: list[CanonicalGame]) -> list[PersistedCanonicalGame]:
        ...

    def save_game_metrics(self, metrics_by_game_id: list[tuple[int, GameMetric]]) -> int:
        ...

    def complete_job_run(self, *, job_id: int, summary: dict[str, Any], status: str) -> None:
        ...

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
        ...

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
        ...

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
        ...

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
        ...

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
        ...

    def save_data_quality_issues(self, issues: list[DataQualityIssueRecord]) -> int:
        ...

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
        ...

    def get_parse_status_counts(
        self,
        *,
        provider_name: str | None = None,
        team_code: str | None = None,
        season_label: str | None = None,
        run_label: str | None = None,
    ) -> dict[str, int]:
        ...

    def get_reconciliation_status_counts(
        self,
        *,
        provider_name: str | None = None,
        team_code: str | None = None,
        season_label: str | None = None,
        run_label: str | None = None,
    ) -> dict[str, int]:
        ...

    def get_data_quality_issue_type_counts(
        self,
        *,
        provider_name: str | None = None,
        team_code: str | None = None,
        season_label: str | None = None,
        run_label: str | None = None,
    ) -> dict[str, int]:
        ...

    def get_data_quality_issue_severity_counts(
        self,
        *,
        provider_name: str | None = None,
        team_code: str | None = None,
        season_label: str | None = None,
        run_label: str | None = None,
    ) -> dict[str, int]:
        ...

    def normalize_data_quality_issue_taxonomy(
        self,
        *,
        provider_name: str | None = None,
        team_code: str | None = None,
        season_label: str | None = None,
        dry_run: bool = True,
    ) -> dict[str, int]:
        ...
