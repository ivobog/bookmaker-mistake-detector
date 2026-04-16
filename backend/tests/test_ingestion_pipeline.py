from datetime import date
from pathlib import Path

from bookmaker_detector_api.ingestion.models import ParseStatus, RawGameRow
from bookmaker_detector_api.ingestion.providers import CoversHistoricalTeamPageProvider
from bookmaker_detector_api.repositories import InMemoryIngestionRepository
from bookmaker_detector_api.services.ingestion_pipeline import (
    HistoricalIngestionRequest,
    ingest_historical_team_page,
)


def test_ingestion_pipeline_persists_fixture_run_in_memory() -> None:
    provider = CoversHistoricalTeamPageProvider()
    repository = InMemoryIngestionRepository()
    fixture_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "bookmaker_detector_api"
        / "fixtures"
        / "covers_sample_team_page.html"
    )
    fixture_html = provider.load_fixture(fixture_path)

    result = ingest_historical_team_page(
        request=HistoricalIngestionRequest(
            provider_name="covers",
            team_code="LAL",
            season_label="2024-2025",
            source_url="https://example.com/covers/lal/2024-2025",
            requested_by="test-suite",
            html=fixture_html,
        ),
        provider=provider,
        repository=repository,
    )

    assert result.job_id == 1
    assert result.page_retrieval_id == 1
    assert result.raw_rows_saved == 3
    assert result.canonical_games_saved == 3
    assert result.metrics_saved == 3
    assert repository.job_runs[0]["status"] == "COMPLETED"
    assert repository.job_runs[0]["summary"]["raw_rows_saved"] == 3
    assert repository.job_runs[0]["summary"]["quality_issues_saved"] == 3
    assert repository.job_runs[0]["summary"]["parse_status_counts"] == {"VALID": 3}
    assert repository.job_runs[0]["summary"]["reconciliation_status_counts"] == {
        "PARTIAL_SINGLE_ROW": 3
    }
    assert repository.job_runs[0]["summary"]["data_quality_issue_type_counts"] == {
        "canonical.single_team_perspective_only": 3
    }
    assert repository.job_runs[0]["summary"]["data_quality_issue_severity_counts"] == {
        "warning": 3
    }
    assert repository.job_run_reporting_snapshots[0]["job_run_id"] == 1
    assert repository.job_run_reporting_snapshots[0]["team_code"] == "LAL"
    assert repository.job_run_reporting_snapshots[0]["season_label"] == "2024-2025"
    assert repository.job_run_reporting_snapshots[0]["raw_rows_saved"] == 3
    assert repository.job_run_reporting_snapshots[0]["warning_count"] == 1
    assert repository.job_run_quality_snapshots[0]["job_run_id"] == 1
    assert repository.job_run_quality_snapshots[0]["parse_valid_count"] == 3
    assert repository.job_run_quality_snapshots[0]["parse_invalid_count"] == 0
    assert repository.job_run_quality_snapshots[0]["reconciliation_partial_single_row_count"] == 3
    assert repository.job_run_quality_snapshots[0]["quality_issue_warning_count"] == 3
    assert repository.job_run_quality_snapshots[0]["quality_issue_error_count"] == 0
    assert repository.page_retrieval_reporting_snapshots[0]["team_code"] == "LAL"
    assert repository.page_retrieval_reporting_snapshots[0]["status"] == "SUCCESS"
    assert repository.page_retrieval_reporting_snapshots[0]["http_status"] == 200
    assert len(repository.raw_rows) == 3
    assert len(repository.canonical_games) == 3
    assert len(repository.metrics) == 3
    assert len(repository.data_quality_issues) == 3
    assert repository.canonical_games[0]["id"] == 1
    assert repository.metrics[0]["canonical_game_id"] == 1


def test_ingestion_pipeline_skips_invalid_rows_from_canonicalization() -> None:
    class FakeProvider:
        provider_name = "covers"

        def parse_team_page(self, **kwargs) -> list[RawGameRow]:
            return [
                RawGameRow(
                    provider_name="covers",
                    team_code="LAL",
                    season_label="2024-2025",
                    source_url="https://example.com/covers/lal/2024-2025",
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

    repository = InMemoryIngestionRepository()
    result = ingest_historical_team_page(
        request=HistoricalIngestionRequest(
            provider_name="covers",
            team_code="LAL",
            season_label="2024-2025",
            source_url="https://example.com/covers/lal/2024-2025",
            requested_by="test-suite",
            html="<html></html>",
        ),
        provider=FakeProvider(),
        repository=repository,
    )

    assert result.raw_rows_saved == 1
    assert result.canonical_games_saved == 0
    assert result.metrics_saved == 0
    assert repository.job_runs[0]["summary"]["quality_issues_saved"] == 1
    assert repository.job_runs[0]["summary"]["parse_status_counts"] == {"INVALID": 1}
    assert repository.job_runs[0]["summary"]["reconciliation_status_counts"] == {}
    assert repository.job_runs[0]["summary"]["data_quality_issue_type_counts"] == {
        "parse.invalid_score_format": 1
    }
    assert repository.job_runs[0]["summary"]["data_quality_issue_severity_counts"] == {
        "error": 1
    }
    assert repository.job_run_reporting_snapshots[0]["quality_issues_saved"] == 1
    assert repository.job_run_reporting_snapshots[0]["warning_count"] == 1
    assert repository.job_run_quality_snapshots[0]["parse_valid_count"] == 0
    assert repository.job_run_quality_snapshots[0]["parse_invalid_count"] == 1
    assert repository.job_run_quality_snapshots[0]["quality_issue_warning_count"] == 0
    assert repository.job_run_quality_snapshots[0]["quality_issue_error_count"] == 1
    assert repository.page_retrieval_reporting_snapshots[0]["status"] == "SUCCESS"
    assert repository.data_quality_issues[0]["issue_type"] == "parse.invalid_score_format"
    assert repository.data_quality_issues[0]["severity"] == "error"
