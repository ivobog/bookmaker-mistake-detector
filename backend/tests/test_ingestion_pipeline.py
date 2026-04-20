import json
from datetime import date
from pathlib import Path

import pytest

from bookmaker_detector_api.fetching import FetchedPage
from bookmaker_detector_api.ingestion.models import ParseStatus, RawGameRow
from bookmaker_detector_api.ingestion.providers import CoversHistoricalTeamPageProvider
from bookmaker_detector_api.services.ingestion_pipeline import (
    HistoricalIngestionRequest,
    ingest_historical_team_page,
)
from tests.support.in_memory_ingestion_repository import InMemoryIngestionRepository
from tests.support.covers_fixtures import load_covers_fixture


def _load_fixture(provider: CoversHistoricalTeamPageProvider, fixture_name: str) -> str:
    return load_covers_fixture(fixture_name)


def test_ingestion_pipeline_persists_fixture_run() -> None:
    provider = CoversHistoricalTeamPageProvider()
    repository = InMemoryIngestionRepository()
    fixture_html = _load_fixture(provider, "covers_sample_team_page.html")

    result = ingest_historical_team_page(
        request=HistoricalIngestionRequest(
            provider_name="covers",
            team_code="LAL",
            season_label="2024-2025",
            source_url="https://example.com/covers/lal/2024-2025",
            requested_by="test-suite",
            html=fixture_html,
            payload_storage_path="payload.html",
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
    assert repository.job_runs[0]["summary"]["payload_storage_path"] == "payload.html"
    assert repository.job_runs[0]["summary"]["diagnostic_count"] == 1
    assert repository.job_runs[0]["summary"]["diagnostics"] == [
        "season_block_selector_match:page-fallback"
    ]
    assert repository.job_runs[0]["summary"]["parser_provenance_counts"] == {
        "opponent_resolution_mode": {"direct_code": 3},
        "ats_parse_mode": {"full": 3},
        "ou_parse_mode": {"full": 3},
    }
    assert repository.job_runs[0]["summary"]["parse_status_counts"] == {"VALID": 3}
    assert repository.job_runs[0]["summary"]["reconciliation_status_counts"] == {
        "PARTIAL_SINGLE_ROW": 3
    }
    assert repository.job_runs[0]["summary"]["data_quality_issue_type_counts"] == {
        "canonical.single_team_perspective_only": 3
    }
    assert repository.job_runs[0]["summary"]["data_quality_issue_severity_counts"] == {"warning": 3}
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


def test_ingestion_pipeline_rejects_team_identity_mismatch() -> None:
    provider = CoversHistoricalTeamPageProvider()
    repository = InMemoryIngestionRepository()
    fixture_html = _load_fixture(provider, "covers_sample_team_page.html")

    with pytest.raises(ValueError, match="identity mismatch"):
        ingest_historical_team_page(
            request=HistoricalIngestionRequest(
                provider_name="covers",
                team_code="PHX",
                season_label="2024-2025",
                source_url="https://example.com/covers/phx/2024-2025",
                requested_by="test-suite",
                html=fixture_html,
            ),
            provider=provider,
            repository=repository,
        )


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
    assert result.diagnostics == []
    assert repository.job_runs[0]["summary"]["quality_issues_saved"] == 1
    assert repository.job_runs[0]["summary"]["diagnostic_count"] == 0
    assert repository.job_runs[0]["summary"]["parse_status_counts"] == {"INVALID": 1}
    assert repository.job_runs[0]["summary"]["reconciliation_status_counts"] == {}
    assert repository.job_runs[0]["summary"]["data_quality_issue_type_counts"] == {
        "parse.invalid_score_format": 1
    }
    assert repository.job_runs[0]["summary"]["data_quality_issue_severity_counts"] == {"error": 1}
    assert repository.job_run_reporting_snapshots[0]["quality_issues_saved"] == 1
    assert repository.job_run_reporting_snapshots[0]["warning_count"] == 1
    assert repository.job_run_quality_snapshots[0]["parse_valid_count"] == 0
    assert repository.job_run_quality_snapshots[0]["parse_invalid_count"] == 1
    assert repository.job_run_quality_snapshots[0]["quality_issue_warning_count"] == 0
    assert repository.job_run_quality_snapshots[0]["quality_issue_error_count"] == 1
    assert repository.page_retrieval_reporting_snapshots[0]["status"] == "SUCCESS"
    assert repository.data_quality_issues[0]["issue_type"] == "parse.invalid_score_format"
    assert repository.data_quality_issues[0]["severity"] == "error"


def test_ingestion_pipeline_persists_live_historical_fragment_snapshot(
    monkeypatch,
    tmp_path,
) -> None:
    provider = CoversHistoricalTeamPageProvider()
    repository = InMemoryIngestionRepository()
    static_html = _load_fixture(provider, "covers_live_team_page.html")
    fragment_html = _load_fixture(provider, "covers_live_schedule_fragment_2023_2024.html")

    def fake_fetch_page(*, url: str):
        content = fragment_html if "getschedule/schedule/175/47854" in url else static_html
        return FetchedPage(
            source_url=url,
            content=content,
            status="SUCCESS",
            http_status=200,
            content_type="text/html",
        )

    monkeypatch.setattr(provider, "fetch_page", fake_fetch_page)

    fetch_result = provider.fetch_team_main_page(
        url="https://example.com/covers/lal",
        requested_season_labels=("2023-2024",),
        browser_fallback=True,
    )

    result = ingest_historical_team_page(
        request=HistoricalIngestionRequest(
            provider_name="covers",
            team_code="LAL",
            season_label="2023-2024",
            source_url="https://example.com/covers/lal",
            source_page_url="https://example.com/covers/lal",
            requested_by="test-suite",
            html=fetch_result.fetched_page.content,
            diagnostics=list(fetch_result.diagnostics),
            persist_parser_snapshot=True,
            parser_snapshot_root_dir=tmp_path / "parser-output",
        ),
        provider=provider,
        repository=repository,
    )

    assert fetch_result.diagnostics == ("season_content_fetch_used",)
    assert result.raw_rows_saved == 1
    assert result.canonical_games_saved == 1
    assert result.metrics_saved == 1
    assert result.diagnostics == [
        "season_content_fetch_used",
        "season_block_selector_match:season-fragment",
        "opponent_resolution_mode:alias_name=1",
    ]
    assert result.parser_snapshot_path is not None

    parser_snapshot = json.loads(Path(result.parser_snapshot_path).read_text(encoding="utf-8"))
    assert parser_snapshot["team_code"] == "LAL"
    assert parser_snapshot["season_label"] == "2023-2024"
    assert parser_snapshot["source_page_url"] == "https://example.com/covers/lal"
    assert parser_snapshot["diagnostics"] == [
        "season_content_fetch_used",
        "season_block_selector_match:season-fragment",
        "opponent_resolution_mode:alias_name=1",
    ]
    assert parser_snapshot["summary"]["raw_rows_saved"] == 1
    assert parser_snapshot["summary"]["parse_status_counts"] == {"VALID": 1}
    assert parser_snapshot["summary"]["parser_provenance_counts"] == {
        "opponent_resolution_mode": {"alias_name": 1},
        "ats_parse_mode": {"full": 1},
        "ou_parse_mode": {"full": 1},
    }
    assert parser_snapshot["raw_rows"][0]["opponent_code"] == "NOP"
    assert parser_snapshot["raw_rows"][0]["parser_provenance"]["opponent_resolution"]["mode"] == (
        "alias_name"
    )

    assert repository.job_runs[0]["summary"]["raw_rows_saved"] == 1
    assert repository.job_runs[0]["summary"]["payload_storage_path"] is None
    assert repository.job_runs[0]["summary"]["parser_snapshot_path"] == result.parser_snapshot_path
    assert repository.job_runs[0]["summary"]["diagnostic_count"] == 3
    assert repository.job_runs[0]["summary"]["diagnostics"] == [
        "season_content_fetch_used",
        "season_block_selector_match:season-fragment",
        "opponent_resolution_mode:alias_name=1",
    ]
    assert repository.job_runs[0]["summary"]["parser_provenance_counts"] == {
        "opponent_resolution_mode": {"alias_name": 1},
        "ats_parse_mode": {"full": 1},
        "ou_parse_mode": {"full": 1},
    }
    assert repository.job_runs[0]["summary"]["parse_status_counts"] == {"VALID": 1}


def test_ingestion_pipeline_persists_missing_season_block_diagnostics() -> None:
    provider = CoversHistoricalTeamPageProvider()
    repository = InMemoryIngestionRepository()

    result = ingest_historical_team_page(
        request=HistoricalIngestionRequest(
            provider_name="covers",
            team_code="LAL",
            season_label="2099-2100",
            source_url="https://example.com/covers/lal",
            requested_by="test-suite",
            html="""
<!doctype html>
<html>
  <body>
    <section id="2024-2025">
      <section data-section="Regular Season">
        <table>
          <tbody>
            <tr>
              <td>2024-11-01</td>
              <td>BOS</td>
              <td>W</td>
              <td>112-104</td>
              <td>W -3.5</td>
              <td>O 214.5</td>
            </tr>
          </tbody>
        </table>
      </section>
    </section>
  </body>
</html>
""",
        ),
        provider=provider,
        repository=repository,
    )

    assert result.raw_rows_saved == 0
    assert result.canonical_games_saved == 0
    assert result.metrics_saved == 0
    assert result.diagnostics == ["season_block_missing"]
    assert repository.job_runs[0]["status"] == "COMPLETED"
    assert repository.job_runs[0]["summary"]["diagnostic_count"] == 1
    assert repository.job_runs[0]["summary"]["diagnostics"] == ["season_block_missing"]
    assert repository.job_runs[0]["summary"]["parse_status_counts"] == {}
    assert repository.job_runs[0]["summary"]["reconciliation_status_counts"] == {}


def test_ingestion_pipeline_tracks_parse_warnings_in_quality_snapshots() -> None:
    provider = CoversHistoricalTeamPageProvider()
    repository = InMemoryIngestionRepository()

    result = ingest_historical_team_page(
        request=HistoricalIngestionRequest(
            provider_name="covers",
            team_code="LAL",
            season_label="2024-2025",
            source_url="https://example.com/covers/lal",
            requested_by="test-suite",
            html="""
<!doctype html>
<html>
  <body>
    <section data-section="Regular Season">
      <table>
        <tbody>
          <tr>
            <td>2024-11-07</td>
            <td>@New York Knicks</td>
            <td>W</td>
            <td>103-99</td>
            <td>-2.5</td>
            <td>212</td>
          </tr>
        </tbody>
      </table>
    </section>
  </body>
</html>
""",
        ),
        provider=provider,
        repository=repository,
    )

    assert result.raw_rows_saved == 1
    assert result.canonical_games_saved == 1
    assert result.metrics_saved == 1
    assert result.diagnostics == [
        "season_block_selector_match:page-fallback",
        "opponent_resolution_mode:alias_name=1",
        "ats_parse_mode:line_only=1",
        "ou_parse_mode:line_only=1",
    ]
    assert repository.job_runs[0]["summary"]["diagnostic_count"] == 4
    assert repository.job_runs[0]["summary"]["diagnostics"] == [
        "season_block_selector_match:page-fallback",
        "opponent_resolution_mode:alias_name=1",
        "ats_parse_mode:line_only=1",
        "ou_parse_mode:line_only=1",
    ]
    assert repository.job_runs[0]["summary"]["parser_provenance_counts"] == {
        "opponent_resolution_mode": {"alias_name": 1},
        "ats_parse_mode": {"line_only": 1},
        "ou_parse_mode": {"line_only": 1},
    }
    assert repository.job_runs[0]["summary"]["parse_status_counts"] == {"VALID_WITH_WARNINGS": 1}
    assert repository.job_runs[0]["summary"]["data_quality_issue_type_counts"] == {
        "parse.missing_ats_result": 1,
        "parse.missing_ou_result": 1,
        "canonical.single_team_perspective_only": 1,
    }
    assert repository.job_runs[0]["summary"]["data_quality_issue_severity_counts"] == {"warning": 3}
    assert repository.job_run_quality_snapshots[0]["parse_warning_count"] == 1
    assert repository.job_run_quality_snapshots[0]["quality_issue_warning_count"] == 3
