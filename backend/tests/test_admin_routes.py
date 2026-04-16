from pathlib import Path

from fastapi.testclient import TestClient

from bookmaker_detector_api.main import app
from bookmaker_detector_api.repositories import InMemoryIngestionRepository
from bookmaker_detector_api import demo as demo_module
from bookmaker_detector_api.services.admin_diagnostics import get_admin_diagnostics
from bookmaker_detector_api.services.fetch_ingestion_runner import run_fetch_and_ingest


client = TestClient(app)


def test_admin_provider_listing() -> None:
    response = client.get("/api/v1/admin/providers")

    assert response.status_code == 200
    payload = response.json()
    assert payload["providers"][0]["name"] == "covers"


def test_phase_one_demo_endpoint_exposes_ingestion_summary() -> None:
    response = client.get("/api/v1/admin/phase-1-demo")

    assert response.status_code == 200
    payload = response.json()
    assert payload["provider"] == "covers"
    assert payload["raw_row_count"] == 3
    assert payload["canonical_game_count"] == 3


def test_phase_one_persistence_demo_endpoint_exposes_job_summary() -> None:
    response = client.get("/api/v1/admin/phase-1-persistence-demo")

    assert response.status_code == 200
    payload = response.json()
    assert payload["job_id"] == 1
    assert payload["page_retrieval_id"] == 1
    assert payload["raw_rows_saved"] == 3
    assert payload["canonical_games_saved"] == 3
    assert payload["metrics_saved"] == 3


def test_phase_one_worker_demo_endpoint_exposes_worker_summary() -> None:
    response = client.get("/api/v1/admin/phase-1-worker-demo")

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["result"]["job_id"] == 1
    assert payload["result"]["metrics_saved"] == 3


def test_phase_one_fetch_demo_endpoint_exposes_fetch_summary() -> None:
    response = client.get("/api/v1/admin/phase-1-fetch-demo")

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["fetch_http_status"] == 200
    assert payload["result"]["raw_rows_saved"] == 3
    assert payload["payload_storage_path"] is not None


def test_phase_one_fetch_failure_demo_endpoint_exposes_failure_summary() -> None:
    response = client.get("/api/v1/admin/phase-1-fetch-failure-demo")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "FAILED"
    assert payload["job_id"] == 1
    assert payload["page_retrieval_id"] == 1
    assert "does not exist" in payload["error_message"]


def test_phase_one_fetch_reporting_demo_endpoint_exposes_reporting_summary(monkeypatch) -> None:
    def fake_fetch_reporting_demo(*, repository_mode: str) -> dict[str, object]:
        return {
            "repository_mode": repository_mode,
            "fetch_result": {
                "status": "COMPLETED",
                "result": {"raw_rows_saved": 3},
            },
            "retrieval_trends": {
                "overview": {"retrieval_count": 1, "successful_retrievals": 1},
                "daily_buckets": [{"date": "2026-04-16", "retrieval_count": 1}],
            },
            "quality_trends": {
                "overview": {"job_count": 1, "parse_valid_count": 3},
                "daily_buckets": [{"date": "2026-04-16", "job_count": 1}],
            },
            "jobs": [{"id": 1, "status": "COMPLETED"}],
            "page_retrievals": [{"id": 1, "status": "SUCCESS"}],
        }

    monkeypatch.setattr(
        "bookmaker_detector_api.api.admin_routes.run_phase_one_fetch_reporting_demo_job",
        fake_fetch_reporting_demo,
    )

    response = client.get(
        "/api/v1/admin/phase-1-fetch-reporting-demo?repository_mode=postgres"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "postgres"
    assert payload["fetch_result"]["status"] == "COMPLETED"
    assert payload["retrieval_trends"]["overview"]["retrieval_count"] == 1
    assert payload["quality_trends"]["overview"]["parse_valid_count"] == 3


def test_phase_one_fetch_reporting_demo_endpoint_isolates_labeled_run() -> None:
    response = client.get(
        "/api/v1/admin/phase-1-fetch-reporting-demo?repository_mode=in_memory"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["fetch_result"]["status"] == "COMPLETED"
    assert payload["retrieval_trends"]["overview"]["retrieval_count"] == 1
    assert payload["quality_trends"]["overview"]["job_count"] == 1
    assert len(payload["jobs"]) == 1
    assert len(payload["page_retrievals"]) == 1
    assert payload["jobs"][0]["payload"]["run_label"] == "phase-1-fetch-reporting-demo"
    assert payload["jobs"][0]["payload"]["run_label"] == payload["page_retrievals"][0]["run_label"]


def test_phase_one_fetch_reporting_demo_uses_distinct_ingestion_source_url(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run_fetch_and_ingest(**kwargs):
        captured.update(kwargs)
        return {"status": "COMPLETED", "result": {"raw_rows_saved": 3}}

    def fake_get_admin_diagnostics(**kwargs):
        return {
            "retrieval_trends": {"overview": {"retrieval_count": 1}, "daily_buckets": []},
            "quality_trends": {"overview": {"job_count": 1}, "daily_buckets": []},
            "job_runs": [],
            "page_retrievals": [],
        }

    monkeypatch.setattr(demo_module, "run_fetch_and_ingest", fake_run_fetch_and_ingest)
    monkeypatch.setattr(demo_module, "get_admin_diagnostics", fake_get_admin_diagnostics)

    payload = demo_module.run_phase_one_fetch_reporting_demo(repository_mode="in_memory")

    assert payload["fetch_result"]["status"] == "COMPLETED"
    assert captured["source_url"] != captured["ingestion_source_url"]
    assert str(captured["ingestion_source_url"]).startswith(str(captured["source_url"]))
    assert "#validation_run=phase-1-fetch-reporting-demo:" in str(
        captured["ingestion_source_url"]
    )


def test_recent_job_runs_endpoint_returns_repository_backed_rows() -> None:
    response = client.get("/api/v1/admin/jobs/recent?repository_mode=in_memory&seed_demo=true")

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert len(payload["job_runs"]) == 5
    assert payload["job_runs"][0]["status"] == "FAILED"
    assert payload["job_runs"][1]["status"] == "COMPLETED"
    assert payload["filters"]["team_code"] is None


def test_recent_ingestion_issues_endpoint_returns_failed_retrievals() -> None:
    response = client.get("/api/v1/admin/ingestion/issues?repository_mode=in_memory&seed_demo=true")

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert len(payload["page_retrievals"]) == 1
    assert payload["page_retrievals"][0]["status"] == "FAILED"
    assert "does not exist" in payload["page_retrievals"][0]["error_message"]


def test_recent_job_runs_endpoint_supports_offset() -> None:
    response = client.get(
        "/api/v1/admin/jobs/recent?repository_mode=in_memory&seed_demo=true&limit=1&offset=1"
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["job_runs"]) == 1
    assert payload["job_runs"][0]["status"] == "COMPLETED"


def test_recent_job_runs_endpoint_supports_team_and_season_filters() -> None:
    response = client.get(
        "/api/v1/admin/jobs/recent"
        "?repository_mode=in_memory&seed_demo=true&team_code=NYK&season_label=2023-2024"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["filters"]["team_code"] == "NYK"
    assert payload["filters"]["season_label"] == "2023-2024"
    assert len(payload["job_runs"]) == 1
    assert payload["job_runs"][0]["payload"]["team_code"] == "NYK"
    assert payload["job_runs"][0]["payload"]["season_label"] == "2023-2024"


def test_data_quality_issues_endpoint_returns_seeded_quality_issues() -> None:
    response = client.get(
        "/api/v1/admin/data-quality/issues?repository_mode=in_memory&seed_demo=true"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert len(payload["data_quality_issues"]) == 8
    assert payload["data_quality_issues"][0]["issue_type"] == "canonical.score_mismatch"


def test_data_quality_issues_endpoint_supports_issue_type_filter() -> None:
    response = client.get(
        "/api/v1/admin/data-quality/issues"
        "?repository_mode=in_memory&seed_demo=true&issue_type=canonical.single_team_perspective_only"
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["data_quality_issues"]) == 5


def test_data_quality_issues_endpoint_supports_severity_filter() -> None:
    response = client.get(
        "/api/v1/admin/data-quality/issues"
        "?repository_mode=in_memory&seed_demo=true&severity=error"
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["data_quality_issues"]) == 3
    assert {issue["issue_type"] for issue in payload["data_quality_issues"]} == {
        "canonical.score_mismatch",
        "parse.invalid_score_format",
    }


def test_data_quality_issues_endpoint_supports_scope_filters() -> None:
    response = client.get(
        "/api/v1/admin/data-quality/issues"
        "?repository_mode=in_memory&seed_demo=true&team_code=NYK&season_label=2023-2024"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["filters"]["team_code"] == "NYK"
    assert payload["filters"]["season_label"] == "2023-2024"
    assert len(payload["data_quality_issues"]) == 3
    assert all(
        issue["details"].get("season_label") == "2023-2024"
        and "NYK"
        in {
            issue["details"].get("team_code"),
            issue["details"].get("home_team_code"),
            issue["details"].get("away_team_code"),
        }
        for issue in payload["data_quality_issues"]
    )


def test_data_quality_issues_endpoint_supports_run_label_filter() -> None:
    repository = InMemoryIngestionRepository()
    fixture_result = run_fetch_and_ingest(
        repository_mode="in_memory",
        team_code="LAL",
        season_label="2024-2025",
        source_url=(
            Path(__file__).resolve().parents[1]
            / "src"
            / "bookmaker_detector_api"
            / "fixtures"
            / "covers_sample_team_page.html"
        ).resolve().as_uri(),
        requested_by="test-suite",
        run_label="phase-1-fetch-reporting-demo",
        persist_payload=False,
        repository_override=repository,
    )

    assert fixture_result["status"] == "COMPLETED"
    diagnostics = get_admin_diagnostics(
        repository_mode="in_memory",
        seed_demo=False,
        repository_override=repository,
        run_label="phase-1-fetch-reporting-demo",
    )
    assert len(diagnostics["data_quality_issues"]) == 3
    assert {
        issue["issue_type"] for issue in diagnostics["data_quality_issues"]
    } == {"canonical.single_team_perspective_only"}


def test_ingestion_stats_endpoint_returns_breakdowns() -> None:
    response = client.get("/api/v1/admin/ingestion/stats?repository_mode=in_memory&seed_demo=true")

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    stats = payload["stats"]
    assert stats["parse_status_counts"] == {"VALID": 8, "INVALID": 1}
    assert stats["reconciliation_status_counts"] == {
        "PARTIAL_SINGLE_ROW": 5,
        "CONFLICT_SCORE": 2,
    }
    assert stats["data_quality_issue_type_counts"]["canonical.single_team_perspective_only"] == 5
    assert stats["data_quality_issue_type_counts"]["parse.invalid_score_format"] == 1
    assert stats["data_quality_issue_type_counts"]["canonical.score_mismatch"] == 2
    assert stats["data_quality_issue_severity_counts"] == {"warning": 5, "error": 3}


def test_ingestion_issues_endpoint_supports_scope_filters() -> None:
    response = client.get(
        "/api/v1/admin/ingestion/issues"
        "?repository_mode=in_memory&seed_demo=true&status=SUCCESS&team_code=NYK&season_label=2023-2024"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["filters"]["retrieval_status"] == "SUCCESS"
    assert len(payload["page_retrievals"]) == 1
    assert payload["page_retrievals"][0]["team_code"] == "NYK"
    assert payload["page_retrievals"][0]["season_label"] == "2023-2024"


def test_recent_job_runs_endpoint_forwards_run_label_filter(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_get_admin_diagnostics(**kwargs):
        captured.update(kwargs)
        return {"repository_mode": "in_memory", "filters": {"run_label": kwargs["run_label"]}, "job_runs": []}

    monkeypatch.setattr(
        "bookmaker_detector_api.api.admin_routes.get_admin_diagnostics",
        fake_get_admin_diagnostics,
    )

    response = client.get(
        "/api/v1/admin/jobs/recent?repository_mode=in_memory&seed_demo=false&run_label=validation-demo"
    )

    assert response.status_code == 200
    assert captured["run_label"] == "validation-demo"
    assert response.json()["filters"]["run_label"] == "validation-demo"


def test_ingestion_stats_endpoint_supports_scope_filters() -> None:
    response = client.get(
        "/api/v1/admin/ingestion/stats"
        "?repository_mode=in_memory&seed_demo=true&team_code=NYK&season_label=2023-2024"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["filters"]["team_code"] == "NYK"
    assert payload["filters"]["season_label"] == "2023-2024"
    assert payload["stats"]["parse_status_counts"] == {"VALID": 3}
    assert payload["stats"]["reconciliation_status_counts"] == {
        "PARTIAL_SINGLE_ROW": 2,
        "CONFLICT_SCORE": 1,
    }
    assert payload["stats"]["data_quality_issue_type_counts"] == {
        "canonical.single_team_perspective_only": 2,
        "canonical.score_mismatch": 1,
    }
    assert payload["stats"]["data_quality_issue_severity_counts"] == {"warning": 2, "error": 1}


def test_ingestion_stats_supports_run_label_filter() -> None:
    repository = InMemoryIngestionRepository()
    fixture_url = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "bookmaker_detector_api"
        / "fixtures"
        / "covers_sample_team_page.html"
    ).resolve().as_uri()
    run_fetch_and_ingest(
        repository_mode="in_memory",
        team_code="LAL",
        season_label="2024-2025",
        source_url=fixture_url,
        requested_by="test-suite",
        run_label="phase-1-fetch-reporting-demo",
        persist_payload=False,
        repository_override=repository,
    )

    diagnostics = get_admin_diagnostics(
        repository_mode="in_memory",
        seed_demo=False,
        repository_override=repository,
        run_label="phase-1-fetch-reporting-demo",
    )
    assert diagnostics["stats"]["parse_status_counts"] == {"VALID": 3}
    assert diagnostics["stats"]["reconciliation_status_counts"] == {
        "PARTIAL_SINGLE_ROW": 3
    }
    assert diagnostics["stats"]["data_quality_issue_type_counts"] == {
        "canonical.single_team_perspective_only": 3
    }
    assert diagnostics["stats"]["data_quality_issue_severity_counts"] == {"warning": 3}


def test_ingestion_stats_endpoint_forwards_run_label_filter(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_get_admin_diagnostics(**kwargs):
        captured.update(kwargs)
        return {
            "repository_mode": "in_memory",
            "filters": {"run_label": kwargs["run_label"]},
            "stats": {},
        }

    monkeypatch.setattr(
        "bookmaker_detector_api.api.admin_routes.get_admin_diagnostics",
        fake_get_admin_diagnostics,
    )

    response = client.get(
        "/api/v1/admin/ingestion/stats?repository_mode=in_memory&seed_demo=false&run_label=validation-demo"
    )

    assert response.status_code == 200
    assert captured["run_label"] == "validation-demo"
    assert response.json()["filters"]["run_label"] == "validation-demo"


def test_validation_run_comparison_builds_latest_vs_previous_delta() -> None:
    repository = InMemoryIngestionRepository()
    first_job_id = repository.create_job_run(
        job_name="historical_team_page_ingestion",
        requested_by="test-suite",
        payload={
            "provider": "covers",
            "team_code": "LAL",
            "season_label": "2024-2025",
            "run_label": "phase-1-fetch-reporting-demo",
        },
    )
    repository.complete_job_run(
        job_id=first_job_id,
        status="COMPLETED",
        summary={
            "raw_rows_saved": 3,
            "canonical_games_saved": 3,
            "metrics_saved": 3,
            "quality_issues_saved": 3,
            "warning_count": 3,
            "parse_status_counts": {"VALID": 3},
            "reconciliation_status_counts": {"PARTIAL_SINGLE_ROW": 3},
            "data_quality_issue_type_counts": {"canonical.single_team_perspective_only": 3},
            "data_quality_issue_severity_counts": {"warning": 3},
        },
    )
    second_job_id = repository.create_job_run(
        job_name="historical_team_page_ingestion",
        requested_by="test-suite",
        payload={
            "provider": "covers",
            "team_code": "LAL",
            "season_label": "2024-2025",
            "run_label": "phase-1-fetch-reporting-demo",
        },
    )
    repository.complete_job_run(
        job_id=second_job_id,
        status="COMPLETED",
        summary={
            "raw_rows_saved": 3,
            "canonical_games_saved": 2,
            "metrics_saved": 2,
            "quality_issues_saved": 1,
            "warning_count": 1,
            "parse_status_counts": {"VALID": 2, "INVALID": 1},
            "reconciliation_status_counts": {"PARTIAL_SINGLE_ROW": 2},
            "data_quality_issue_type_counts": {"parse.invalid_score_format": 1},
            "data_quality_issue_severity_counts": {"error": 1},
        },
    )

    diagnostics = get_admin_diagnostics(
        repository_mode="in_memory",
        seed_demo=False,
        repository_override=repository,
        run_label="phase-1-fetch-reporting-demo",
        validation_compare_limit=5,
    )

    comparison = diagnostics["validation_run_comparison"]
    assert comparison["run_label"] == "phase-1-fetch-reporting-demo"
    assert comparison["run_count"] == 2
    assert comparison["latest_run"]["job_id"] == second_job_id
    assert comparison["previous_run"]["job_id"] == first_job_id
    assert comparison["latest_vs_previous"]["status_changed"] is False
    assert comparison["latest_vs_previous"]["metric_deltas"] == {
        "raw_rows_saved": 0,
        "canonical_games_saved": -1,
        "metrics_saved": -1,
        "quality_issues_saved": -2,
        "warning_count": -2,
    }
    assert comparison["latest_vs_previous"]["parse_status_count_deltas"] == {
        "INVALID": 1,
        "VALID": -1,
    }
    assert comparison["latest_vs_previous"]["reconciliation_status_count_deltas"] == {
        "PARTIAL_SINGLE_ROW": -1
    }
    assert comparison["latest_vs_previous"]["data_quality_issue_type_count_deltas"] == {
        "canonical.single_team_perspective_only": -3,
        "parse.invalid_score_format": 1,
    }
    assert comparison["latest_vs_previous"]["data_quality_issue_severity_count_deltas"] == {
        "error": 1,
        "warning": -3,
    }


def test_validation_run_comparison_endpoint_forwards_filters(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_get_admin_diagnostics(**kwargs):
        captured.update(kwargs)
        return {
            "repository_mode": "in_memory",
            "filters": {"run_label": kwargs["run_label"]},
            "validation_run_comparison": {
                "run_label": kwargs["run_label"],
                "run_count": 2,
                "latest_run": {"job_id": 2},
                "previous_run": {"job_id": 1},
                "latest_vs_previous": {"metric_deltas": {"warning_count": -1}},
                "recent_runs": [],
            },
        }

    monkeypatch.setattr(
        "bookmaker_detector_api.api.admin_routes.get_admin_diagnostics",
        fake_get_admin_diagnostics,
    )

    response = client.get(
        "/api/v1/admin/validation-runs/compare"
        "?repository_mode=in_memory&seed_demo=false&run_label=validation-demo&limit=6"
    )

    assert response.status_code == 200
    assert captured["run_label"] == "validation-demo"
    assert captured["validation_compare_limit"] == 6
    payload = response.json()
    assert payload["filters"]["run_label"] == "validation-demo"
    assert payload["validation_run_comparison"]["run_count"] == 2


def test_ingestion_stats_endpoint_supports_provider_filter() -> None:
    response = client.get(
        "/api/v1/admin/ingestion/stats"
        "?repository_mode=in_memory&seed_demo=true&provider_name=missing-provider"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["filters"]["provider_name"] == "missing-provider"
    assert payload["stats"]["parse_status_counts"] == {}
    assert payload["stats"]["reconciliation_status_counts"] == {}
    assert payload["stats"]["data_quality_issue_type_counts"] == {}
    assert payload["stats"]["data_quality_issue_severity_counts"] == {}


def test_ingestion_trends_endpoint_returns_recent_run_rollup() -> None:
    response = client.get(
        "/api/v1/admin/ingestion/trends?repository_mode=in_memory&seed_demo=true&limit=10"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    overview = payload["trends"]["overview"]
    assert overview["job_count"] == 5
    assert overview["completed_jobs"] == 4
    assert overview["failed_jobs"] == 1
    assert overview["total_warning_count"] == 5
    assert overview["total_quality_issues_saved"] == 8
    assert len(payload["trends"]["recent_runs"]) == 5
    assert payload["trends"]["recent_runs"][0]["status"] == "FAILED"
    assert payload["trends"]["recent_runs"][1]["reconciliation_status_counts"] == {
        "CONFLICT_SCORE": 1
    }
    assert len(payload["trends"]["daily_buckets"]) == 4
    assert sum(bucket["job_count"] for bucket in payload["trends"]["daily_buckets"]) == 5


def test_ingestion_trends_endpoint_supports_scope_filters() -> None:
    response = client.get(
        "/api/v1/admin/ingestion/trends"
        "?repository_mode=in_memory&seed_demo=true&team_code=NYK&season_label=2023-2024"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["filters"]["team_code"] == "NYK"
    assert payload["filters"]["season_label"] == "2023-2024"
    overview = payload["trends"]["overview"]
    assert overview["job_count"] == 1
    assert overview["completed_jobs"] == 1
    assert overview["failed_jobs"] == 0
    recent_run = payload["trends"]["recent_runs"][0]
    assert recent_run["team_code"] == "NYK"
    assert recent_run["parse_status_counts"] == {"VALID": 3}
    assert recent_run["reconciliation_status_counts"] == {
        "PARTIAL_SINGLE_ROW": 2,
        "CONFLICT_SCORE": 1,
    }
    assert recent_run["data_quality_issue_type_counts"] == {
        "canonical.single_team_perspective_only": 2,
        "canonical.score_mismatch": 1,
    }


def test_ingestion_trends_endpoint_supports_day_window() -> None:
    response = client.get(
        "/api/v1/admin/ingestion/trends"
        "?repository_mode=in_memory&seed_demo=true&days=2"
    )

    assert response.status_code == 200
    payload = response.json()
    overview = payload["trends"]["overview"]
    assert overview["job_count"] == 3
    assert overview["completed_jobs"] == 2
    assert overview["failed_jobs"] == 1
    assert len(payload["trends"]["recent_runs"]) == 3
    assert len(payload["trends"]["daily_buckets"]) == 2


def test_ingestion_trends_overview_uses_full_window_not_recent_run_limit() -> None:
    response = client.get(
        "/api/v1/admin/ingestion/trends"
        "?repository_mode=in_memory&seed_demo=true&limit=2&days=7"
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["trends"]["recent_runs"]) == 2
    assert payload["trends"]["overview"]["job_count"] == 5
    assert len(payload["trends"]["daily_buckets"]) == 4


def test_retrieval_trends_endpoint_returns_rollup() -> None:
    response = client.get(
        "/api/v1/admin/retrieval/trends?repository_mode=in_memory&seed_demo=true&days=7"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    overview = payload["retrieval_trends"]["overview"]
    assert overview["retrieval_count"] == 5
    assert overview["successful_retrievals"] == 4
    assert overview["failed_retrievals"] == 1
    assert overview["payload_saved_count"] == 0
    assert overview["missing_http_status_count"] == 1
    assert len(payload["retrieval_trends"]["daily_buckets"]) == 4


def test_retrieval_trends_endpoint_supports_status_filter() -> None:
    response = client.get(
        "/api/v1/admin/retrieval/trends"
        "?repository_mode=in_memory&seed_demo=true&status=FAILED&days=7"
    )

    assert response.status_code == 200
    payload = response.json()
    overview = payload["retrieval_trends"]["overview"]
    assert overview["retrieval_count"] == 1
    assert overview["successful_retrievals"] == 0
    assert overview["failed_retrievals"] == 1
    assert len(payload["retrieval_trends"]["daily_buckets"]) == 1


def test_ingestion_quality_trends_endpoint_returns_rollup() -> None:
    response = client.get(
        "/api/v1/admin/ingestion/quality-trends?repository_mode=in_memory&seed_demo=true&days=7"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    overview = payload["quality_trends"]["overview"]
    assert overview["job_count"] == 4
    assert overview["parse_valid_count"] == 8
    assert overview["parse_invalid_count"] == 1
    assert overview["parse_warning_count"] == 0
    assert overview["reconciliation_partial_single_row_count"] == 5
    assert overview["reconciliation_conflict_score_count"] == 2
    assert overview["reconciliation_conflict_total_line_count"] == 0
    assert overview["reconciliation_conflict_spread_line_count"] == 0
    assert overview["quality_issue_warning_count"] == 5
    assert overview["quality_issue_error_count"] == 3
    assert len(payload["quality_trends"]["daily_buckets"]) == 4


def test_ingestion_quality_trends_endpoint_supports_scope_filters() -> None:
    response = client.get(
        "/api/v1/admin/ingestion/quality-trends"
        "?repository_mode=in_memory&seed_demo=true&team_code=NYK&season_label=2023-2024"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["filters"]["team_code"] == "NYK"
    assert payload["filters"]["season_label"] == "2023-2024"
    overview = payload["quality_trends"]["overview"]
    assert overview["job_count"] == 1
    assert overview["parse_valid_count"] == 3
    assert overview["parse_invalid_count"] == 0
    assert overview["reconciliation_partial_single_row_count"] == 2
    assert overview["reconciliation_conflict_score_count"] == 1
    assert overview["quality_issue_warning_count"] == 2
    assert overview["quality_issue_error_count"] == 1
    assert len(payload["quality_trends"]["daily_buckets"]) == 1


def test_recent_job_runs_endpoint_supports_started_window() -> None:
    response = client.get(
        "/api/v1/admin/jobs/recent"
        "?repository_mode=in_memory&seed_demo=true&started_from=2026-04-15&started_to=2026-04-16"
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["job_runs"]) == 3
    assert payload["filters"]["started_from"].startswith("2026-04-15T00:00:00")
    assert payload["filters"]["started_to"].startswith("2026-04-16T23:59:59")


def test_normalize_data_quality_issue_taxonomy_endpoint_returns_summary(monkeypatch) -> None:
    def fake_normalize_data_quality_taxonomy(**kwargs) -> dict[str, object]:
        return {
            "repository_mode": kwargs["repository_mode"],
            "dry_run": kwargs["dry_run"],
            "filters": {
                "provider_name": kwargs["provider_name"],
                "team_code": kwargs["team_code"],
                "season_label": kwargs["season_label"],
            },
            "normalization": {
                "matched_rows": 4,
                "updated_rows": 3,
                "issue_type_updates": 2,
                "severity_updates": 1,
            },
            "stats": {
                "data_quality_issue_type_counts": {
                    "canonical.single_team_perspective_only": 4,
                },
                "data_quality_issue_severity_counts": {"warning": 4},
            },
        }

    monkeypatch.setattr(
        "bookmaker_detector_api.api.admin_routes.normalize_data_quality_taxonomy",
        fake_normalize_data_quality_taxonomy,
    )

    response = client.post(
        "/api/v1/admin/data-quality/normalize-taxonomy"
        "?repository_mode=postgres&team_code=LAL&season_label=2024-2025&dry_run=false"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "postgres"
    assert payload["dry_run"] is False
    assert payload["filters"]["team_code"] == "LAL"
    assert payload["filters"]["season_label"] == "2024-2025"
    assert payload["normalization"]["updated_rows"] == 3
