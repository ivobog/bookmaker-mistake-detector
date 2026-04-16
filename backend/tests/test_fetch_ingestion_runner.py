from pathlib import Path

from bookmaker_detector_api.config import settings
from bookmaker_detector_api.fetching import fetch_page
from bookmaker_detector_api.repositories import InMemoryIngestionRepository
from bookmaker_detector_api.services.fetch_ingestion_runner import run_fetch_and_ingest


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
    settings.raw_payload_dir = str(tmp_path)

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

    payload_path = Path(result["payload_storage_path"])
    assert payload_path.exists()
    assert payload_path.read_text(encoding="utf-8").startswith("<!doctype html>")
    assert result["result"]["raw_rows_saved"] == 3
    assert result["result"]["metrics_saved"] == 3


def test_fetch_and_ingest_records_failed_fetch_in_memory() -> None:
    missing_fixture_url = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "bookmaker_detector_api"
        / "fixtures"
        / "missing_team_page.html"
    ).resolve().as_uri()

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


def test_fetch_and_ingest_uses_ingestion_source_url_for_persistence() -> None:
    fixture_url = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "bookmaker_detector_api"
        / "fixtures"
        / "covers_sample_team_page.html"
    ).resolve().as_uri()
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
    assert all(row["source_url"] == persisted_source_url for row in repository.raw_rows)
    assert repository.page_retrievals[0]["record"].source_url == persisted_source_url
