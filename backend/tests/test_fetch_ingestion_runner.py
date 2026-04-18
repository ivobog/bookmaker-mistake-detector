import json
import logging
from pathlib import Path

from bookmaker_detector_api.config import settings
from bookmaker_detector_api.fetching import fetch_page
from bookmaker_detector_api.repositories import InMemoryIngestionRepository
from bookmaker_detector_api.services import fetch_ingestion_runner
from bookmaker_detector_api.services.fetch_ingestion_runner import run_fetch_and_ingest
from bookmaker_detector_api.services.workflow_logging import WORKFLOW_LOGGER_NAME
from tests.support.covers_fixtures import (
    DEFAULT_TEAM_PAGE_URL,
    build_fixture_backed_covers_provider,
    load_covers_fixture,
)


def _workflow_events(caplog) -> list[dict[str, object]]:
    return [
        json.loads(record.getMessage())
        for record in caplog.records
        if record.name == WORKFLOW_LOGGER_NAME
    ]


def test_fetch_page_reads_fixture_file_uri() -> None:
    fixture_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "bookmaker_detector_api"
        / "fixtures"
        / "covers_sample_team_page.html"
    )

    fetched_page = fetch_page(fixture_path.resolve().as_uri())

    assert fetched_page.status == "SUCCESS"
    assert fetched_page.http_status == 200
    assert "Regular Season" in fetched_page.content


def test_fetch_and_ingest_stores_payload_snapshot(tmp_path) -> None:
    fixture_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "bookmaker_detector_api"
        / "fixtures"
        / "covers_sample_team_page.html"
    )
    original_payload_dir = settings.raw_payload_dir
    original_parser_snapshot_dir = settings.parser_snapshot_dir
    settings.raw_payload_dir = str(tmp_path)
    settings.parser_snapshot_dir = str(tmp_path / "parser-output")

    try:
        result = run_fetch_and_ingest(
            repository_mode="in_memory",
            team_code="LAL",
            season_label="2024-2025",
            source_url=fixture_path.resolve().as_uri(),
            requested_by="test-suite",
            persist_payload=True,
        )
    finally:
        settings.raw_payload_dir = original_payload_dir
        settings.parser_snapshot_dir = original_parser_snapshot_dir

    payload_path = Path(result["payload_storage_path"])
    parser_snapshot_path = Path(result["parser_snapshot_path"])
    assert payload_path.exists()
    assert parser_snapshot_path.exists()
    assert payload_path.read_text(encoding="utf-8").startswith("<!doctype html>")
    parser_snapshot = json.loads(parser_snapshot_path.read_text(encoding="utf-8"))
    assert parser_snapshot["team_code"] == "LAL"
    assert parser_snapshot["season_label"] == "2024-2025"
    assert parser_snapshot["summary"]["raw_rows_saved"] == 3
    assert parser_snapshot["summary"]["parser_provenance_counts"] == {
        "opponent_resolution_mode": {"direct_code": 3},
        "ats_parse_mode": {"full": 3},
        "ou_parse_mode": {"full": 3},
    }
    assert parser_snapshot["summary"]["parse_status_counts"] == {"VALID": 3}
    assert parser_snapshot["raw_rows"][0]["parser_provenance"]["opponent_resolution"]["mode"] == (
        "direct_code"
    )
    assert len(parser_snapshot["raw_rows"]) == 3
    assert result["result"]["raw_rows_saved"] == 3
    assert result["result"]["metrics_saved"] == 3


def test_fetch_and_ingest_stores_live_shape_payload_snapshot(monkeypatch, tmp_path) -> None:
    original_payload_dir = settings.raw_payload_dir
    original_parser_snapshot_dir = settings.parser_snapshot_dir
    settings.raw_payload_dir = str(tmp_path)
    settings.parser_snapshot_dir = str(tmp_path / "parser-output")
    monkeypatch.setattr(
        fetch_ingestion_runner,
        "CoversHistoricalTeamPageProvider",
        build_fixture_backed_covers_provider(),
    )

    try:
        result = run_fetch_and_ingest(
            repository_mode="in_memory",
            team_code="LAL",
            season_label="2024-2025",
            source_url=DEFAULT_TEAM_PAGE_URL,
            requested_by="test-suite",
            persist_payload=True,
        )
    finally:
        settings.raw_payload_dir = original_payload_dir
        settings.parser_snapshot_dir = original_parser_snapshot_dir

    payload_path = Path(result["payload_storage_path"])
    parser_snapshot_path = Path(result["parser_snapshot_path"])
    assert payload_path.exists()
    assert parser_snapshot_path.exists()
    parser_snapshot = json.loads(parser_snapshot_path.read_text(encoding="utf-8"))
    assert parser_snapshot["team_code"] == "LAL"
    assert parser_snapshot["season_label"] == "2024-2025"
    assert parser_snapshot["source_page_url"] == DEFAULT_TEAM_PAGE_URL
    assert parser_snapshot["summary"]["raw_rows_saved"] == 1
    assert parser_snapshot["summary"]["parse_status_counts"] == {"VALID": 1}
    assert parser_snapshot["summary"]["parser_provenance_counts"] == {
        "opponent_resolution_mode": {"alias_name": 1},
        "ats_parse_mode": {"full": 1},
        "ou_parse_mode": {"full": 1},
    }
    assert parser_snapshot["diagnostics"] == [
        "season_block_selector_match:season-content",
        "opponent_resolution_mode:alias_name=1",
    ]
    assert parser_snapshot["raw_rows"][0]["opponent_code"] == "GSW"
    assert result["result"]["raw_rows_saved"] == 1
    assert result["result"]["metrics_saved"] == 1


def test_fetch_and_ingest_emits_structured_workflow_logs(caplog, tmp_path) -> None:
    fixture_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "bookmaker_detector_api"
        / "fixtures"
        / "covers_sample_team_page.html"
    )
    original_payload_dir = settings.raw_payload_dir
    original_parser_snapshot_dir = settings.parser_snapshot_dir
    settings.raw_payload_dir = str(tmp_path)
    settings.parser_snapshot_dir = str(tmp_path / "parser-output")

    try:
        with caplog.at_level(logging.INFO, logger=WORKFLOW_LOGGER_NAME):
            result = run_fetch_and_ingest(
                repository_mode="in_memory",
                team_code="LAL",
                season_label="2024-2025",
                source_url=fixture_path.resolve().as_uri(),
                requested_by="test-suite",
                persist_payload=True,
            )
    finally:
        settings.raw_payload_dir = original_payload_dir
        settings.parser_snapshot_dir = original_parser_snapshot_dir

    events = _workflow_events(caplog)
    assert [entry["event"] for entry in events] == [
        "workflow_started",
        "workflow_succeeded",
    ]
    assert events[0]["workflow_name"] == "ingestion.fetch_and_ingest"
    assert events[1]["job_id"] == result["result"]["job_id"]
    assert events[1]["raw_rows_saved"] == result["result"]["raw_rows_saved"]


def test_fetch_and_ingest_records_failed_fetch_in_memory() -> None:
    missing_fixture_url = (
        (
            Path(__file__).resolve().parents[1]
            / "src"
            / "bookmaker_detector_api"
            / "fixtures"
            / "missing_team_page.html"
        )
        .resolve()
        .as_uri()
    )

    result = run_fetch_and_ingest(
        repository_mode="in_memory",
        team_code="LAL",
        season_label="2024-2025",
        source_url=missing_fixture_url,
        requested_by="test-suite",
        persist_payload=True,
    )

    assert result["status"] == "FAILED"
    assert result["job_id"] == 1
    assert result["page_retrieval_id"] == 1
    assert "does not exist" in result["error_message"]
    assert result["job_runs"][0]["status"] == "FAILED"
    assert result["page_retrievals"][0]["record"]["status"] == "FAILED"
    assert result["page_retrievals"][0]["record"]["error_message"] == result["error_message"]


def test_fetch_and_ingest_logs_failure_workflow_event(caplog) -> None:
    missing_fixture_url = (
        (
            Path(__file__).resolve().parents[1]
            / "src"
            / "bookmaker_detector_api"
            / "fixtures"
            / "missing_team_page.html"
        )
        .resolve()
        .as_uri()
    )

    with caplog.at_level(logging.INFO, logger=WORKFLOW_LOGGER_NAME):
        result = run_fetch_and_ingest(
            repository_mode="in_memory",
            team_code="LAL",
            season_label="2024-2025",
            source_url=missing_fixture_url,
            requested_by="test-suite",
            persist_payload=True,
        )

    events = _workflow_events(caplog)
    assert [entry["event"] for entry in events] == [
        "workflow_started",
        "workflow_failed",
    ]
    assert events[1]["workflow_name"] == "ingestion.fetch_and_ingest"
    assert events[1]["job_id"] == result["job_id"]
    assert events[1]["page_retrieval_id"] == result["page_retrieval_id"]


def test_fetch_and_ingest_uses_ingestion_source_url_for_persistence() -> None:
    fixture_url = (
        (
            Path(__file__).resolve().parents[1]
            / "src"
            / "bookmaker_detector_api"
            / "fixtures"
            / "covers_sample_team_page.html"
        )
        .resolve()
        .as_uri()
    )
    repository = InMemoryIngestionRepository()
    persisted_source_url = f"{fixture_url}#validation_run=phase-1-fetch-reporting-demo"

    result = run_fetch_and_ingest(
        repository_mode="in_memory",
        team_code="LAL",
        season_label="2024-2025",
        source_url=fixture_url,
        ingestion_source_url=persisted_source_url,
        requested_by="test-suite",
        run_label="phase-1-fetch-reporting-demo",
        persist_payload=False,
        repository_override=repository,
    )

    assert result["status"] == "COMPLETED"
    assert result["parser_snapshot_path"] is None
    assert all(row["source_url"] == persisted_source_url for row in repository.raw_rows)
    assert all(row["source_page_url"] == fixture_url for row in repository.raw_rows)
    assert all(row["source_page_season_label"] == "2024-2025" for row in repository.raw_rows)
    assert repository.page_retrievals[0]["record"].source_url == persisted_source_url


def test_fetch_and_ingest_deduplicates_raw_rows_by_source_coordinates() -> None:
    fixture_url = (
        (
            Path(__file__).resolve().parents[1]
            / "src"
            / "bookmaker_detector_api"
            / "fixtures"
            / "covers_sample_team_page.html"
        )
        .resolve()
        .as_uri()
    )
    repository = InMemoryIngestionRepository()

    first_result = run_fetch_and_ingest(
        repository_mode="in_memory",
        team_code="LAL",
        season_label="2024-2025",
        source_url=fixture_url,
        ingestion_source_url=f"{fixture_url}#validation_run=first",
        requested_by="test-suite",
        persist_payload=False,
        repository_override=repository,
    )
    second_result = run_fetch_and_ingest(
        repository_mode="in_memory",
        team_code="LAL",
        season_label="2024-2025",
        source_url=fixture_url,
        ingestion_source_url=f"{fixture_url}#validation_run=second",
        requested_by="test-suite",
        persist_payload=False,
        repository_override=repository,
    )

    assert first_result["status"] == "COMPLETED"
    assert second_result["status"] == "COMPLETED"
    assert len(repository.raw_rows) == 3
    assert len(repository.canonical_games) == 3
    assert len(repository.metrics) == 3
    assert len(repository.data_quality_issues) == 3
    assert all(row["source_page_url"] == fixture_url for row in repository.raw_rows)
    assert {row["source_url"] for row in repository.raw_rows} == {
        f"{fixture_url}#validation_run=second"
    }


def test_ingestion_pipeline_deduplicates_canonical_games_and_metrics_on_rerun() -> None:
    provider = build_fixture_backed_covers_provider()()
    repository = InMemoryIngestionRepository()
    fixture_html = load_covers_fixture("covers_live_team_page.html")

    first_result = fetch_ingestion_runner.ingest_historical_team_page(
        request=fetch_ingestion_runner.HistoricalIngestionRequest(
            provider_name="covers",
            team_code="LAL",
            season_label="2024-2025",
            source_url=DEFAULT_TEAM_PAGE_URL,
            source_page_url=DEFAULT_TEAM_PAGE_URL,
            requested_by="test-suite",
            html=fixture_html,
        ),
        provider=provider,
        repository=repository,
    )
    second_result = fetch_ingestion_runner.ingest_historical_team_page(
        request=fetch_ingestion_runner.HistoricalIngestionRequest(
            provider_name="covers",
            team_code="LAL",
            season_label="2024-2025",
            source_url=DEFAULT_TEAM_PAGE_URL,
            source_page_url=DEFAULT_TEAM_PAGE_URL,
            requested_by="test-suite",
            html=fixture_html,
        ),
        provider=provider,
        repository=repository,
    )

    assert first_result.raw_rows_saved == 1
    assert second_result.raw_rows_saved == 1
    assert len(repository.raw_rows) == 1
    assert len(repository.canonical_games) == 1
    assert len(repository.metrics) == 1
    assert len(repository.data_quality_issues) == 1
    assert repository.canonical_games[0]["id"] == 1
    assert repository.metrics[0]["canonical_game_id"] == 1
