import json
import logging

import pytest

from bookmaker_detector_api.services.fixture_ingestion_runner import run_fixture_ingestion
from bookmaker_detector_api.services.workflow_logging import WORKFLOW_LOGGER_NAME


def _workflow_events(caplog: pytest.LogCaptureFixture) -> list[dict[str, object]]:
    return [
        json.loads(record.getMessage())
        for record in caplog.records
        if record.name == WORKFLOW_LOGGER_NAME
    ]


def test_run_fixture_ingestion_emits_structured_workflow_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.INFO, logger=WORKFLOW_LOGGER_NAME):
        result = run_fixture_ingestion(
            repository_mode="in_memory",
            team_code="LAL",
            season_label="2024-2025",
            source_url="fixture://covers_sample_team_page.html",
            requested_by="test-suite",
        )

    events = _workflow_events(caplog)
    assert [entry["event"] for entry in events] == [
        "workflow_started",
        "workflow_succeeded",
    ]
    assert events[0]["workflow_name"] == "ingestion.fixture_ingestion"
    assert events[1]["job_id"] == result["result"]["job_id"]
    assert events[1]["raw_rows_saved"] == result["result"]["raw_rows_saved"]


def test_run_fixture_ingestion_logs_failure_for_invalid_mode(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.INFO, logger=WORKFLOW_LOGGER_NAME):
        with pytest.raises(ValueError):
            run_fixture_ingestion(
                repository_mode="unsupported",
                team_code="LAL",
                season_label="2024-2025",
                source_url="fixture://covers_sample_team_page.html",
                requested_by="test-suite",
            )

    events = _workflow_events(caplog)
    assert [entry["event"] for entry in events] == [
        "workflow_started",
        "workflow_failed",
    ]
    assert events[1]["workflow_name"] == "ingestion.fixture_ingestion"
    assert events[1]["error_type"] == "ValueError"
