from pathlib import Path

from fastapi.testclient import TestClient

from bookmaker_detector_api import demo as demo_module
from bookmaker_detector_api.main import app
from bookmaker_detector_api.repositories import InMemoryIngestionRepository
from bookmaker_detector_api.services import models as models_module
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


def test_phase_two_feature_demo_endpoint_exposes_feature_snapshot_summary() -> None:
    response = client.get("/api/v1/admin/phase-2-feature-demo?repository_mode=in_memory")

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["ingest_result"]["canonical_games_saved"] == 3
    assert payload["feature_result"]["canonical_game_count"] == 3
    assert payload["feature_result"]["snapshots_saved"] == 3
    assert (
        payload["feature_result"]["feature_version"]["feature_key"]
        == "baseline_team_features_v1"
    )
    assert len(payload["feature_result"]["feature_snapshots"]) == 3


def test_phase_three_model_train_endpoint_returns_ranked_baseline_runs() -> None:
    response = client.post(
        "/api/v1/admin/models/train"
        "?repository_mode=in_memory&seed_demo=true"
        "&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["filters"]["target_task"] == "spread_error_regression"
    assert payload["dataset_row_count"] > 0
    assert len(payload["model_runs"]) == 2
    assert payload["best_model"] is not None
    assert payload["best_model"]["artifact"]["model_family"] == "linear_feature"
    assert "selection_metrics" in payload["model_runs"][0]["artifact"]
    assert "split_target_summary" in payload["model_runs"][0]["artifact"]


def test_phase_three_model_registry_endpoint_returns_seeded_registry_rows() -> None:
    response = client.get(
        "/api/v1/admin/models/registry"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true"
        "&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["filters"]["target_task"] == "spread_error_regression"
    assert payload["model_registry_count"] == 2
    assert {entry["model_family"] for entry in payload["model_registry"]} == {
        "linear_feature",
        "tree_stump",
    }


def test_phase_three_model_runs_endpoint_returns_persisted_training_runs() -> None:
    response = client.get(
        "/api/v1/admin/models/runs"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true"
        "&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["filters"]["target_task"] == "spread_error_regression"
    assert payload["model_run_count"] == 2
    assert {run["artifact"]["model_family"] for run in payload["model_runs"]} == {
        "linear_feature",
        "tree_stump",
    }
    assert all("train" in run["metrics"] for run in payload["model_runs"])
    assert all(
        run["metrics"]["validation"]["prediction_count"] > 0
        for run in payload["model_runs"]
    )


def test_phase_three_model_run_detail_endpoint_returns_payload() -> None:
    response = client.get(
        "/api/v1/admin/models/runs/1"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true"
        "&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["model_run"] is not None
    assert payload["model_run"]["id"] == 1
    assert payload["model_run"]["artifact"]["model_family"] in {
        "linear_feature",
        "tree_stump",
    }
    assert payload["model_run"]["metrics"]["validation"]["prediction_count"] > 0


def test_phase_three_model_summary_endpoint_returns_best_and_latest_runs() -> None:
    response = client.get(
        "/api/v1/admin/models/summary"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true"
        "&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["filters"]["target_task"] == "spread_error_regression"
    assert payload["model_summary"]["run_count"] == 2
    assert payload["model_summary"]["status_counts"] == {"COMPLETED": 2}
    assert payload["model_summary"]["usable_run_count"] == 2
    assert payload["model_summary"]["fallback_run_count"] == 1
    assert payload["model_summary"]["best_overall"] is not None
    assert payload["model_summary"]["best_overall"]["artifact"]["model_family"] == "linear_feature"
    assert payload["model_summary"]["latest_run"] is not None
    assert "linear_feature" in payload["model_summary"]["best_by_family"]
    assert "tree_stump" in payload["model_summary"]["best_by_family"]


def test_phase_three_model_history_endpoint_returns_rollup() -> None:
    response = client.get(
        "/api/v1/admin/models/history"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true"
        "&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25"
        "&recent_limit=5"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["filters"]["target_task"] == "spread_error_regression"
    assert payload["model_history"]["overview"]["run_count"] == 2
    assert payload["model_history"]["overview"]["best_overall"]["artifact"]["model_family"] == (
        "linear_feature"
    )
    assert payload["model_history"]["overview"]["fallback_run_count"] == 1
    assert len(payload["model_history"]["daily_buckets"]) == 1
    assert payload["model_history"]["daily_buckets"][0]["run_count"] == 2
    assert len(payload["model_history"]["recent_runs"]) == 2


def test_phase_three_model_evaluations_endpoint_returns_snapshots() -> None:
    response = client.get(
        "/api/v1/admin/models/evaluations"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true"
        "&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["evaluation_snapshot_count"] == 2
    assert {snapshot["model_family"] for snapshot in payload["evaluation_snapshots"]} == {
        "linear_feature",
        "tree_stump",
    }


def test_phase_three_model_evaluation_detail_endpoint_returns_payload() -> None:
    response = client.get(
        "/api/v1/admin/models/evaluations/1"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true"
        "&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["evaluation_snapshot"] is not None
    assert payload["evaluation_snapshot"]["id"] == 1
    assert payload["evaluation_snapshot"]["model_family"] in {
        "linear_feature",
        "tree_stump",
    }
    assert payload["evaluation_snapshot"]["validation_prediction_count"] > 0


def test_phase_three_model_evaluation_history_endpoint_returns_rollup() -> None:
    response = client.get(
        "/api/v1/admin/models/evaluations/history"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true"
        "&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25"
        "&recent_limit=5"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["model_evaluation_history"]["overview"]["snapshot_count"] == 2
    assert payload["model_evaluation_history"]["overview"]["fallback_strategy_counts"] == {
        "constant_mean": 1
    }
    assert len(payload["model_evaluation_history"]["daily_buckets"]) == 1
    assert len(payload["model_evaluation_history"]["recent_snapshots"]) == 2


def test_phase_three_model_select_endpoint_promotes_linear_candidate() -> None:
    response = client.post(
        "/api/v1/admin/models/select"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true"
        "&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["selection_policy_name"] == "validation_mae_candidate_v1"
    assert payload["selected_snapshot"]["model_family"] == "linear_feature"
    assert payload["active_selection"]["model_family"] == "linear_feature"
    assert payload["selection_count"] == 1


def test_phase_three_model_selections_endpoint_returns_active_selection() -> None:
    response = client.get(
        "/api/v1/admin/models/selections"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true"
        "&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25"
        "&active_only=true"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["selection_count"] == 1
    assert payload["selections"][0]["model_family"] == "linear_feature"
    assert payload["selections"][0]["is_active"] is True


def test_phase_three_model_selection_detail_endpoint_returns_payload() -> None:
    response = client.get(
        "/api/v1/admin/models/selections/1"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true"
        "&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["selection"] is not None
    assert payload["selection"]["id"] == 1
    assert payload["selection"]["model_family"] == "linear_feature"
    assert payload["selection"]["is_active"] is True


def test_phase_three_model_selection_history_endpoint_returns_rollup() -> None:
    response = client.get(
        "/api/v1/admin/models/selections/history"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true"
        "&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25"
        "&recent_limit=5"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["model_selection_history"]["overview"]["selection_count"] == 1
    assert payload["model_selection_history"]["overview"]["active_selection_count"] == 1
    assert payload["model_selection_history"]["overview"]["latest_selection"]["model_family"] == (
        "linear_feature"
    )
    assert len(payload["model_selection_history"]["recent_selections"]) == 1


def test_phase_three_model_score_preview_endpoint_returns_scored_predictions() -> None:
    response = client.get(
        "/api/v1/admin/models/score-preview"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&auto_select_demo=true"
        "&target_task=spread_error_regression&team_code=LAL&season_label=2024-2025"
        "&canonical_game_id=3&train_ratio=0.5&validation_ratio=0.25&limit=5"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["filters"]["target_task"] == "spread_error_regression"
    assert payload["filters"]["canonical_game_id"] == 3
    assert payload["active_selection"] is not None
    assert payload["active_selection"]["model_family"] == "linear_feature"
    assert payload["active_evaluation_snapshot"] is not None
    assert payload["scored_prediction_count"] == 1
    assert payload["prediction_summary"]["prediction_count"] == 1
    prediction = payload["predictions"][0]
    assert prediction["team_code"] == "LAL"
    assert prediction["model"]["model_family"] == "linear_feature"
    assert prediction["evidence"]["summary"] is not None
    assert prediction["evidence"]["recommendation"] is not None


def test_phase_three_model_future_game_preview_endpoint_returns_scenario_predictions() -> None:
    response = client.get(
        "/api/v1/admin/models/future-game-preview"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&auto_select_demo=true"
        "&target_task=spread_error_regression&season_label=2025-2026&game_date=2026-04-20"
        "&home_team_code=LAL&away_team_code=BOS&home_spread_line=-3.5&total_line=228.5"
        "&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["scenario"]["home_team_code"] == "LAL"
    assert payload["scenario"]["away_team_code"] == "BOS"
    assert payload["scored_prediction_count"] == 2
    assert len(payload["predictions"]) == 2
    assert payload["predictions"][0]["market_context"]["total_line"] == 228.5
    assert len(payload["opportunity_preview"]) >= 1


def test_phase_three_model_future_game_preview_materialize_endpoint_returns_scoring_run() -> None:
    response = client.post(
        "/api/v1/admin/models/future-game-preview/materialize"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&auto_select_demo=true"
        "&target_task=spread_error_regression&season_label=2025-2026&game_date=2026-04-20"
        "&home_team_code=LAL&away_team_code=BOS&home_spread_line=-3.5&total_line=228.5"
        "&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["materialized_count"] == 1
    assert payload["scoring_run"] is not None
    assert payload["scoring_run"]["home_team_code"] == "LAL"
    assert payload["scoring_run"]["away_team_code"] == "BOS"


def test_phase_three_model_future_game_preview_runs_endpoint_returns_materialized_runs() -> None:
    response = client.get(
        "/api/v1/admin/models/future-game-preview/runs"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true"
        "&auto_select_demo=true&auto_materialize_demo=true"
        "&target_task=spread_error_regression&season_label=2025-2026&game_date=2026-04-20"
        "&team_code=LAL&home_team_code=LAL&away_team_code=BOS&home_spread_line=-3.5"
        "&total_line=228.5&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["scoring_run_count"] == 1
    assert payload["scoring_runs"][0]["home_team_code"] == "LAL"


def test_phase_three_model_future_game_preview_run_detail_endpoint_returns_payload() -> None:
    response = client.get(
        "/api/v1/admin/models/future-game-preview/runs/1"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true"
        "&auto_select_demo=true&auto_materialize_demo=true"
        "&target_task=spread_error_regression&season_label=2025-2026&game_date=2026-04-20"
        "&home_team_code=LAL&away_team_code=BOS&home_spread_line=-3.5&total_line=228.5"
        "&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["scoring_run"] is not None
    assert payload["scoring_run"]["id"] == 1
    assert payload["scoring_run"]["payload"]["scenario"]["away_team_code"] == "BOS"


def test_phase_three_model_future_game_preview_history_endpoint_returns_rollup() -> None:
    response = client.get(
        "/api/v1/admin/models/future-game-preview/history"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true"
        "&auto_select_demo=true&auto_materialize_demo=true"
        "&target_task=spread_error_regression&season_label=2025-2026&game_date=2026-04-20"
        "&team_code=LAL&home_team_code=LAL&away_team_code=BOS&home_spread_line=-3.5"
        "&total_line=228.5&train_ratio=0.5&validation_ratio=0.25&recent_limit=5"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    overview = payload["model_scoring_history"]["overview"]
    assert overview["scoring_run_count"] == 1
    assert overview["prediction_count"] == 2
    assert overview["latest_scoring_run"]["home_team_code"] == "LAL"
    assert len(payload["model_scoring_history"]["recent_scoring_runs"]) == 1


def test_phase_three_model_future_opportunity_materialize_endpoint_returns_future_rows() -> None:
    response = client.post(
        "/api/v1/admin/models/future-game-preview/opportunities/materialize"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&auto_select_demo=true"
        "&target_task=spread_error_regression&season_label=2025-2026&game_date=2026-04-20"
        "&home_team_code=LAL&away_team_code=BOS&home_spread_line=-3.5&total_line=228.5"
        "&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["scoring_run"] is not None
    assert payload["opportunity_count"] >= 1
    assert payload["opportunities"][0]["source_kind"] == "future_scenario"
    assert payload["opportunities"][0]["scenario_key"] == "2025-2026:2026-04-20:LAL:BOS"
    assert payload["opportunities"][0]["model_scoring_run_id"] == payload["scoring_run"]["id"]


def test_phase_three_model_future_slate_preview_endpoint_returns_batch_summary() -> None:
    response = client.post(
        "/api/v1/admin/models/future-slate/preview"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&auto_select_demo=true"
        "&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25",
        json={
            "slate_label": "demo-slate",
            "games": [
                {
                    "season_label": "2025-2026",
                    "game_date": "2026-04-20",
                    "home_team_code": "LAL",
                    "away_team_code": "BOS",
                    "home_spread_line": -3.5,
                    "total_line": 228.5,
                },
                {
                    "season_label": "2025-2026",
                    "game_date": "2026-04-21",
                    "home_team_code": "NYK",
                    "away_team_code": "MIA",
                    "home_spread_line": -1.5,
                    "total_line": 219.5,
                },
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["slate"]["slate_key"] == "spread_error_regression:demo-slate"
    assert payload["slate"]["game_count"] == 2
    assert payload["game_preview_count"] == 2
    assert payload["scored_prediction_count"] == 4
    assert len(payload["games"]) == 2


def test_phase_three_model_future_slate_materialize_endpoint_persists_batch_results() -> None:
    response = client.post(
        "/api/v1/admin/models/future-slate/materialize"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&auto_select_demo=true"
        "&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25",
        json={
            "slate_label": "demo-slate",
            "games": [
                {
                    "season_label": "2025-2026",
                    "game_date": "2026-04-20",
                    "home_team_code": "LAL",
                    "away_team_code": "BOS",
                    "home_spread_line": -3.5,
                    "total_line": 228.5,
                },
                {
                    "season_label": "2025-2026",
                    "game_date": "2026-04-21",
                    "home_team_code": "NYK",
                    "away_team_code": "MIA",
                    "home_spread_line": -1.5,
                    "total_line": 219.5,
                },
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["slate"]["slate_key"] == "spread_error_regression:demo-slate"
    assert payload["materialized_scoring_run_count"] == 2
    assert len(payload["scoring_runs"]) == 2
    assert payload["materialized_opportunity_count"] >= 2
    assert len(payload["opportunities"]) >= 2


def test_phase_three_model_market_board_materialize_endpoint_persists_board() -> None:
    response = client.post(
        "/api/v1/admin/models/market-board/materialize"
        "?repository_mode=in_memory&target_task=spread_error_regression",
        json={
            "slate_label": "demo-market-board",
            "games": [
                {
                    "season_label": "2025-2026",
                    "game_date": "2026-04-20",
                    "home_team_code": "LAL",
                    "away_team_code": "BOS",
                    "home_spread_line": -3.5,
                    "total_line": 228.5,
                }
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["board"]["board_key"] == "spread_error_regression:demo-market-board"
    assert payload["board"]["game_count"] == 1


def test_phase_three_model_market_board_sources_endpoint_lists_builtin_sources() -> None:
    response = client.get("/api/v1/admin/models/market-board/sources")

    assert response.status_code == 200
    payload = response.json()
    assert payload["sources"]
    assert payload["sources"][0]["source_name"] == "demo_daily_lines_v1"
    assert any(
        source["source_name"] == "demo_source_failure_v1" for source in payload["sources"]
    )
    assert any(source["source_name"] == "file_market_board_v1" for source in payload["sources"])


def test_phase_three_model_market_board_refresh_endpoint_persists_source_board() -> None:
    response = client.post(
        "/api/v1/admin/models/market-board/refresh"
        "?repository_mode=in_memory&target_task=spread_error_regression"
        "&source_name=demo_daily_lines_v1&season_label=2025-2026"
        "&game_date=2026-04-20&slate_label=demo-refresh-board&game_count=2"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["source_name"] == "demo_daily_lines_v1"
    assert payload["generated_game_count"] == 2
    assert payload["board"]["board_key"] == "spread_error_regression:demo-refresh-board"
    assert payload["board"]["payload"]["source"]["source_name"] == "demo_daily_lines_v1"
    assert payload["board"]["freshness"]["freshness_status"] == "fresh"
    assert payload["change_summary"]["added_game_count"] == 2
    assert payload["source_payload_fingerprints"]["raw_game_count"] == 2


def test_phase_three_model_market_board_refresh_endpoint_returns_source_failure() -> None:
    response = client.post(
        "/api/v1/admin/models/market-board/refresh"
        "?repository_mode=in_memory&target_task=spread_error_regression"
        "&source_name=demo_source_failure_v1&season_label=2025-2026"
        "&game_date=2026-04-20&slate_label=demo-failing-refresh-board&game_count=2"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["status"] == "FAILED"
    assert payload["error_message"]
    assert payload["generated_game_count"] == 0
    assert payload["board"] is None
    assert payload["change_summary"] is None
    assert payload["source_run"]["status"] == "FAILED"


def test_phase_three_model_market_board_refresh_endpoint_normalizes_partial_source() -> None:
    response = client.post(
        "/api/v1/admin/models/market-board/refresh"
        "?repository_mode=in_memory&target_task=spread_error_regression"
        "&source_name=demo_partial_lines_v1&season_label=2025-2026"
        "&game_date=2026-04-20&slate_label=demo-partial-refresh-board&game_count=3"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "SUCCESS_WITH_WARNINGS"
    assert payload["generated_game_count"] == 1
    assert payload["board"]["game_count"] == 1
    assert payload["validation_summary"]["raw_row_count"] == 3
    assert payload["validation_summary"]["invalid_row_count"] == 2
    assert payload["source_run"]["payload"]["validation_summary"]["warning_count"] == 1
    assert payload["source_payload_fingerprints"]["normalized_game_count"] == 1


def test_phase_three_model_market_board_refresh_endpoint_supports_file_source() -> None:
    response = client.post(
        "/api/v1/admin/models/market-board/refresh"
        "?repository_mode=in_memory&target_task=spread_error_regression"
        "&source_name=file_market_board_v1&season_label=2025-2026"
        "&game_date=2026-04-20&slate_label=demo-file-refresh-board"
        "&source_path=fixture://demo_market_board_file_source.json"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "SUCCESS"
    assert payload["source_path"] == "fixture://demo_market_board_file_source.json"
    assert payload["generated_game_count"] == 2
    assert payload["board"]["game_count"] == 2
    assert payload["board"]["payload"]["source"]["source_path"] == (
        "fixture://demo_market_board_file_source.json"
    )


def test_phase_three_model_market_board_refresh_endpoint_supports_external_odds_source(
    monkeypatch,
) -> None:
    monkeypatch.setattr(models_module.settings, "the_odds_api_key", "test-key")
    monkeypatch.setattr(
        models_module,
        "_fetch_the_odds_api_games",
        lambda: [
            {
                "home_team": "PHX",
                "away_team": "DAL",
                "commence_time": "2026-04-20T23:00:00Z",
                "bookmakers": [
                    {
                        "markets": [
                            {
                                "key": "spreads",
                                "outcomes": [
                                    {"name": "PHX", "point": 1.5},
                                    {"name": "DAL", "point": -1.5},
                                ],
                            },
                            {
                                "key": "totals",
                                "outcomes": [
                                    {"name": "Over", "point": 226.5},
                                    {"name": "Under", "point": 226.5},
                                ],
                            },
                        ]
                    }
                ],
            }
        ],
    )

    response = client.post(
        "/api/v1/admin/models/market-board/refresh"
        "?repository_mode=in_memory&target_task=spread_error_regression"
        "&source_name=the_odds_api_v4_nba&season_label=2025-2026"
        "&game_date=2026-04-20&slate_label=demo-odds-api-refresh-board"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "SUCCESS"
    assert payload["generated_game_count"] == 1
    assert payload["source_request_context"]["sport_key"] == "basketball_nba"
    assert payload["generated_games"][0]["home_team_code"] == "PHX"


def test_phase_three_model_market_board_history_endpoint_returns_refresh_rollup() -> None:
    response = client.get(
        "/api/v1/admin/models/market-board/history"
        "?repository_mode=in_memory&auto_refresh_demo=true"
        "&target_task=spread_error_regression&source_name=demo_daily_lines_v1"
        "&season_label=2025-2026&game_date=2026-04-20&slate_label=demo-refresh-board"
        "&game_count=2&recent_limit=5"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    overview = payload["market_board_refresh_history"]["overview"]
    assert overview["refresh_event_count"] == 1
    assert overview["status_counts"]["created"] == 1
    assert overview["source_counts"]["demo_daily_lines_v1"] == 1
    assert (
        payload["market_board_refresh_history"]["recent_refresh_events"][0]["payload"][
            "change_summary"
        ]["added_game_count"]
        == 2
    )
    assert payload["market_board_refresh_history"]["recent_refresh_events"]


def test_phase_three_model_market_board_source_runs_endpoint_returns_source_history() -> None:
    response = client.get(
        "/api/v1/admin/models/market-board/source-runs"
        "?repository_mode=in_memory&auto_refresh_demo=true"
        "&target_task=spread_error_regression&source_name=demo_daily_lines_v1"
        "&season_label=2025-2026&game_date=2026-04-20&slate_label=demo-refresh-board"
        "&game_count=2&recent_limit=5"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    history = payload["market_board_source_run_history"]
    assert history["overview"]["source_run_count"] == 1
    assert history["overview"]["generated_game_count"] == 2
    assert history["recent_source_runs"][0]["payload"]["request"]["requested_game_count"] == 2


def test_phase_three_model_market_board_source_runs_endpoint_returns_failed_history() -> None:
    response = client.get(
        "/api/v1/admin/models/market-board/source-runs"
        "?repository_mode=in_memory&auto_refresh_demo=true"
        "&target_task=spread_error_regression&source_name=demo_source_failure_v1"
        "&season_label=2025-2026&game_date=2026-04-20&slate_label=demo-failing-refresh-board"
        "&game_count=2&recent_limit=5"
    )

    assert response.status_code == 200
    payload = response.json()
    history = payload["market_board_source_run_history"]
    assert history["overview"]["source_run_count"] == 1
    assert history["overview"]["generated_game_count"] == 0
    assert history["overview"]["status_counts"]["FAILED"] == 1
    assert history["recent_source_runs"][0]["payload"]["error_message"]


def test_phase_three_model_market_board_source_runs_endpoint_returns_validation_history() -> None:
    response = client.get(
        "/api/v1/admin/models/market-board/source-runs"
        "?repository_mode=in_memory&auto_refresh_demo=true"
        "&target_task=spread_error_regression&source_name=demo_partial_lines_v1"
        "&season_label=2025-2026&game_date=2026-04-20&slate_label=demo-partial-refresh-board"
        "&game_count=3&recent_limit=5"
    )

    assert response.status_code == 200
    payload = response.json()
    history = payload["market_board_source_run_history"]
    assert history["overview"]["source_run_count"] == 1
    assert history["overview"]["generated_game_count"] == 1
    assert history["overview"]["invalid_row_count"] == 2
    assert history["overview"]["warning_count"] == 1
    assert history["overview"]["status_counts"]["SUCCESS_WITH_WARNINGS"] == 1
    assert (
        history["recent_source_runs"][0]["payload"]["validation_summary"]["invalid_row_count"]
        == 2
    )


def test_phase_three_model_market_board_refresh_queue_endpoint_returns_refreshable_board() -> None:
    response = client.get(
        "/api/v1/admin/models/market-board/refresh-queue"
        "?repository_mode=in_memory&auto_refresh_demo=true"
        "&target_task=spread_error_regression&source_name=demo_daily_lines_v1"
        "&season_label=2025-2026&game_date=2026-04-20&slate_label=demo-refresh-board"
        "&game_count=2&pending_only=false"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    queue = payload["market_board_refresh_queue"]
    assert queue["overview"]["refreshable_board_count"] == 1
    assert queue["queue_entries"][0]["queue_status"] == "up_to_date"
    assert queue["queue_entries"][0]["refreshable"] is True
    assert queue["queue_entries"][0]["freshness_status"] == "fresh"


def test_phase_three_model_market_board_queue_endpoint_returns_pending_board() -> None:
    response = client.get(
        "/api/v1/admin/models/market-board/queue"
        "?repository_mode=in_memory&auto_refresh_demo=true"
        "&target_task=spread_error_regression&source_name=demo_daily_lines_v1"
        "&season_label=2025-2026&game_date=2026-04-20&slate_label=demo-refresh-board"
        "&game_count=2&freshness_status=fresh&pending_only=true"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    queue = payload["market_board_scoring_queue"]
    assert queue["overview"]["pending_board_count"] == 1
    assert queue["queue_entries"][0]["queue_status"] == "pending_score"
    assert queue["queue_entries"][0]["freshness_status"] == "fresh"


def test_phase_three_model_market_board_refresh_orchestrate_endpoint_refreshes_board() -> None:
    response = client.post(
        "/api/v1/admin/models/market-board/orchestrate-refresh"
        "?repository_mode=in_memory&auto_refresh_demo=true"
        "&target_task=spread_error_regression&source_name=demo_daily_lines_v1"
        "&season_label=2025-2026&game_date=2026-04-20&slate_label=demo-refresh-board"
        "&game_count=2&pending_only=false&recent_limit=5"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["candidate_board_count"] == 1
    assert payload["refreshed_board_count"] == 1
    assert payload["unchanged_board_count"] == 1
    assert payload["refresh_batch"]["candidate_board_count"] == 1
    assert payload["refresh_runs"][0]["refresh_result_status"] == "unchanged"


def test_phase_three_model_market_board_orchestrate_endpoint_scores_pending_board() -> None:
    response = client.post(
        "/api/v1/admin/models/market-board/orchestrate-score"
        "?repository_mode=in_memory&seed_demo=true&auto_refresh_demo=true"
        "&auto_train_demo=true&auto_select_demo=true"
        "&target_task=spread_error_regression&source_name=demo_daily_lines_v1"
        "&season_label=2025-2026&game_date=2026-04-20&slate_label=demo-refresh-board"
        "&game_count=2&freshness_status=fresh&pending_only=true"
        "&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["candidate_board_count"] == 1
    assert payload["scored_board_count"] == 1
    assert payload["materialized_scoring_run_count"] == 2
    assert payload["queue_before"]["overview"]["pending_board_count"] == 1
    assert payload["queue_after"]["overview"]["pending_board_count"] == 0
    assert payload["orchestration_batch"]["candidate_board_count"] == 1


def test_phase_three_model_market_board_orchestrate_cadence_endpoint_runs_full_cycle() -> None:
    response = client.post(
        "/api/v1/admin/models/market-board/orchestrate-cadence"
        "?repository_mode=in_memory&seed_demo=true&auto_refresh_demo=true"
        "&auto_train_demo=true&auto_select_demo=true"
        "&target_task=spread_error_regression&source_name=demo_daily_lines_v1"
        "&season_label=2025-2026&game_date=2026-04-20&game_count=2"
        "&refresh_pending_only=false"
        "&scoring_freshness_status=fresh&scoring_pending_only=true"
        "&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["refreshed_board_count"] == 1
    assert payload["scored_board_count"] == 1
    assert payload["materialized_scoring_run_count"] == 2
    assert payload["cadence_batch"]["refreshed_board_count"] == 1


def test_phase_three_model_market_board_refresh_orchestration_history_endpoint_returns_batches(
) -> None:
    response = client.get(
        "/api/v1/admin/models/market-board/refresh-orchestration-history"
        "?repository_mode=in_memory&auto_refresh_demo=true&auto_orchestrate_demo=true"
        "&target_task=spread_error_regression&source_name=demo_daily_lines_v1"
        "&season_label=2025-2026&game_date=2026-04-20&game_count=2"
        "&pending_only=false&recent_limit=5"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    history = payload["market_board_refresh_orchestration_history"]
    assert history["overview"]["batch_count"] == 1
    assert history["overview"]["candidate_board_count"] == 1
    assert history["overview"]["refreshed_board_count"] == 1
    assert history["recent_batches"][0]["target_task"] == "spread_error_regression"


def test_phase_three_model_market_board_cadence_history_endpoint_returns_batches() -> None:
    response = client.get(
        "/api/v1/admin/models/market-board/cadence-history"
        "?repository_mode=in_memory&seed_demo=true&auto_refresh_demo=true"
        "&auto_train_demo=true&auto_select_demo=true&auto_orchestrate_demo=true"
        "&target_task=spread_error_regression&source_name=demo_daily_lines_v1"
        "&season_label=2025-2026&game_date=2026-04-20&game_count=2"
        "&refresh_pending_only=false"
        "&scoring_freshness_status=fresh&scoring_pending_only=true"
        "&train_ratio=0.5&validation_ratio=0.25&recent_limit=5"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    history = payload["market_board_cadence_history"]
    assert history["overview"]["batch_count"] == 1
    assert history["overview"]["refreshed_board_count"] == 1
    assert history["overview"]["scored_board_count"] == 1
    assert history["recent_batches"][0]["target_task"] == "spread_error_regression"


def test_phase_three_model_market_board_orchestration_history_endpoint_returns_batches() -> None:
    response = client.get(
        "/api/v1/admin/models/market-board/orchestration-history"
        "?repository_mode=in_memory&seed_demo=true&auto_refresh_demo=true"
        "&auto_train_demo=true&auto_select_demo=true&auto_orchestrate_demo=true"
        "&target_task=spread_error_regression&source_name=demo_daily_lines_v1"
        "&season_label=2025-2026&game_date=2026-04-20&game_count=2"
        "&freshness_status=fresh&pending_only=true&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    history = payload["market_board_orchestration_history"]
    assert history["overview"]["batch_count"] == 1
    assert history["overview"]["candidate_board_count"] == 1
    assert history["overview"]["materialized_scoring_run_count"] == 2
    assert history["recent_batches"][0]["target_task"] == "spread_error_regression"


def test_phase_three_model_market_board_operations_endpoint_returns_summary() -> None:
    response = client.get(
        "/api/v1/admin/models/market-board/1/operations"
        "?repository_mode=in_memory&seed_demo=true&auto_refresh_demo=true"
        "&auto_train_demo=true&auto_select_demo=true&auto_orchestrate_demo=true"
        "&target_task=spread_error_regression&source_name=demo_daily_lines_v1"
        "&season_label=2025-2026&game_date=2026-04-20&game_count=2"
        "&freshness_status=fresh&pending_only=true&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    operations = payload["operations"]
    assert operations["board"]["id"] == 1
    assert operations["queue_entry"]["scoring_status"] == "current"
    assert operations["source_runs"]["source_run_count"] == 1
    assert operations["scoring"]["scoring_run_count"] == 2
    assert operations["opportunities"]["opportunity_count"] == 2
    assert operations["refresh_orchestration"]["batch_count"] == 0
    assert operations["cadence"]["batch_count"] == 0
    assert operations["orchestration"]["batch_count"] == 1
    assert operations["refresh"]["latest_refresh_event"]["payload"]["change_summary"] is not None


def test_phase_three_model_market_board_cadence_endpoint_returns_dashboard() -> None:
    response = client.get(
        "/api/v1/admin/models/market-board/cadence"
        "?repository_mode=in_memory&seed_demo=true&auto_refresh_demo=true"
        "&auto_train_demo=true&auto_select_demo=true&auto_orchestrate_demo=true"
        "&target_task=spread_error_regression&source_name=demo_daily_lines_v1"
        "&season_label=2025-2026&game_date=2026-04-20&game_count=2"
        "&freshness_status=fresh&pending_only=true&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    dashboard = payload["market_board_cadence"]
    assert dashboard["overview"]["board_count"] == 1
    assert dashboard["overview"]["cadence_status_counts"]["recently_scored"] == 1
    assert dashboard["cadence_entries"][0]["priority"] == "low"


def test_phase_three_model_market_board_list_and_detail_endpoints_return_board() -> None:
    list_response = client.get(
        "/api/v1/admin/models/market-board"
        "?repository_mode=in_memory&target_task=spread_error_regression"
        "&season_label=2025-2026&auto_materialize_demo=true&slate_label=demo-market-board"
        "&game_date=2026-04-20&home_team_code=LAL&away_team_code=BOS"
        "&home_spread_line=-3.5&total_line=228.5"
    )

    assert list_response.status_code == 200
    list_payload = list_response.json()
    assert list_payload["repository_mode"] == "in_memory"
    assert list_payload["board_count"] == 1
    assert list_payload["boards"][0]["board_key"] == "spread_error_regression:demo-market-board"

    detail_response = client.get(
        "/api/v1/admin/models/market-board/1"
        "?repository_mode=in_memory&auto_materialize_demo=true"
        "&target_task=spread_error_regression&season_label=2025-2026"
        "&slate_label=demo-market-board&game_date=2026-04-20"
        "&home_team_code=LAL&away_team_code=BOS&home_spread_line=-3.5&total_line=228.5"
    )

    assert detail_response.status_code == 200
    detail_payload = detail_response.json()
    assert detail_payload["repository_mode"] == "in_memory"
    assert detail_payload["board"]["id"] == 1
    assert detail_payload["board"]["payload"]["games"][0]["away_team_code"] == "BOS"


def test_phase_three_model_market_board_score_endpoint_materializes_slate() -> None:
    response = client.post(
        "/api/v1/admin/models/market-board/1/score"
        "?repository_mode=in_memory&seed_demo=true&auto_materialize_demo=true"
        "&auto_train_demo=true&auto_select_demo=true"
        "&target_task=spread_error_regression&season_label=2025-2026"
        "&slate_label=demo-market-board&game_date=2026-04-20"
        "&home_team_code=LAL&away_team_code=BOS&home_spread_line=-3.5&total_line=228.5"
        "&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["board"]["id"] == 1
    assert payload["slate_result"]["materialized_scoring_run_count"] == 1
    assert payload["slate_result"]["materialized_opportunity_count"] >= 1


def test_phase_three_model_opportunity_materialize_endpoint_returns_opportunities() -> None:
    response = client.post(
        "/api/v1/admin/models/opportunities/materialize"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true&auto_select_demo=true"
        "&target_task=spread_error_regression&team_code=LAL&season_label=2024-2025"
        "&canonical_game_id=3&train_ratio=0.5&validation_ratio=0.25&limit=5"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["materialized_count"] == 1
    assert payload["opportunity_count"] == 1
    assert payload["opportunities"][0]["team_code"] == "LAL"
    assert payload["opportunities"][0]["status"] == "review_manually"


def test_phase_three_model_opportunities_endpoint_returns_materialized_rows() -> None:
    response = client.get(
        "/api/v1/admin/models/opportunities"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true"
        "&auto_select_demo=true&auto_materialize_demo=true"
        "&target_task=spread_error_regression&team_code=LAL&season_label=2024-2025"
        "&canonical_game_id=3&train_ratio=0.5&validation_ratio=0.25&status=review_manually"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["opportunity_count"] == 1
    assert payload["opportunities"][0]["status"] == "review_manually"
    assert payload["opportunities"][0]["team_code"] == "LAL"


def test_phase_three_model_opportunities_endpoint_returns_future_materialized_rows() -> None:
    response = client.get(
        "/api/v1/admin/models/opportunities"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true"
        "&auto_select_demo=true&auto_materialize_demo=true"
        "&target_task=spread_error_regression&season_label=2025-2026"
        "&source_kind=future_scenario&team_code=LAL&game_date=2026-04-20"
        "&home_team_code=LAL&away_team_code=BOS&home_spread_line=-3.5&total_line=228.5"
        "&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["opportunity_count"] >= 1
    assert payload["opportunities"][0]["source_kind"] == "future_scenario"
    assert payload["opportunities"][0]["canonical_game_id"] is None


def test_phase_three_model_opportunity_detail_endpoint_returns_payload() -> None:
    response = client.get(
        "/api/v1/admin/models/opportunities/1"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true"
        "&auto_select_demo=true&auto_materialize_demo=true"
        "&target_task=spread_error_regression&team_code=LAL&season_label=2024-2025"
        "&canonical_game_id=3&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["opportunity"] is not None
    assert payload["opportunity"]["id"] == 1
    assert payload["opportunity"]["payload"]["prediction"]["team_code"] == "LAL"


def test_phase_three_model_opportunity_detail_endpoint_returns_future_payload() -> None:
    response = client.get(
        "/api/v1/admin/models/opportunities/1"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true"
        "&auto_select_demo=true&auto_materialize_demo=true"
        "&target_task=spread_error_regression&season_label=2025-2026"
        "&source_kind=future_scenario&game_date=2026-04-20"
        "&home_team_code=LAL&away_team_code=BOS&home_spread_line=-3.5&total_line=228.5"
        "&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["opportunity"] is not None
    assert payload["opportunity"]["source_kind"] == "future_scenario"
    assert payload["opportunity"]["payload"]["scenario"]["home_team_code"] == "LAL"


def test_phase_three_model_opportunity_history_endpoint_returns_rollup() -> None:
    response = client.get(
        "/api/v1/admin/models/opportunities/history"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true"
        "&auto_select_demo=true&auto_materialize_demo=true"
        "&target_task=spread_error_regression&team_code=LAL&season_label=2024-2025"
        "&canonical_game_id=3&train_ratio=0.5&validation_ratio=0.25&recent_limit=5"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    overview = payload["model_opportunity_history"]["overview"]
    assert overview["opportunity_count"] == 1
    assert overview["status_counts"] == {"review_manually": 1}
    assert overview["latest_opportunity"]["team_code"] == "LAL"
    assert len(payload["model_opportunity_history"]["recent_opportunities"]) == 1


def test_phase_three_model_opportunity_history_endpoint_returns_future_rollup() -> None:
    response = client.get(
        "/api/v1/admin/models/opportunities/history"
        "?repository_mode=in_memory&seed_demo=true&auto_train_demo=true"
        "&auto_select_demo=true&auto_materialize_demo=true"
        "&target_task=spread_error_regression&season_label=2025-2026"
        "&source_kind=future_scenario&team_code=LAL&game_date=2026-04-20"
        "&home_team_code=LAL&away_team_code=BOS&home_spread_line=-3.5&total_line=228.5"
        "&train_ratio=0.5&validation_ratio=0.25&recent_limit=5"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    overview = payload["model_opportunity_history"]["overview"]
    assert overview["opportunity_count"] >= 1
    assert overview["source_kind_counts"]["future_scenario"] >= 1
    assert overview["latest_opportunity"]["scenario_key"] == "2025-2026:2026-04-20:LAL:BOS"


def test_feature_snapshots_endpoint_returns_filtered_phase_two_snapshots() -> None:
    response = client.get(
        "/api/v1/admin/features/snapshots"
        "?repository_mode=in_memory&seed_demo=true&team_code=MIA&season_label=2024-2025"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["filters"]["team_code"] == "MIA"
    assert payload["filters"]["season_label"] == "2024-2025"
    assert payload["feature_version"]["feature_key"] == "baseline_team_features_v1"
    assert payload["snapshot_count"] == 1
    assert len(payload["feature_snapshots"]) == 1
    snapshot = payload["feature_snapshots"][0]
    assert snapshot["away_team_code"] == "MIA"
    assert snapshot["feature_payload"]["home_team"]["rolling_home_windows"]["3"]["sample_size"] == 1
    assert "volatility" in snapshot["feature_payload"]["home_team"]
    assert "trend_signals" in snapshot["feature_payload"]["home_team"]
    assert (
        snapshot["feature_payload"]["home_team"]["trend_signals"][
            "recent_point_margin_delta_3_vs_10"
        ]
        == 0.0
    )


def test_feature_summary_endpoint_returns_team_rollup() -> None:
    response = client.get(
        "/api/v1/admin/features/summary"
        "?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["filters"]["team_code"] == "LAL"
    assert payload["filters"]["season_label"] == "2024-2025"
    assert payload["feature_version"]["feature_key"] == "baseline_team_features_v1"
    assert payload["snapshot_count"] == 3
    assert payload["perspective_count"] == 3
    assert payload["summary"]["team_count"] == 1
    assert (
        payload["summary"]["home_perspective_count"]
        + payload["summary"]["away_perspective_count"]
        == 3
    )
    assert "rolling_window_averages" in payload["summary"]
    assert payload["latest_perspective"]["team_code"] == "LAL"


def test_feature_dataset_endpoint_returns_flattened_rows() -> None:
    response = client.get(
        "/api/v1/admin/features/dataset"
        "?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["filters"]["team_code"] == "LAL"
    assert payload["filters"]["season_label"] == "2024-2025"
    assert payload["feature_version"]["feature_key"] == "baseline_team_features_v1"
    assert payload["row_count"] == 3
    assert len(payload["feature_rows"]) == 3
    first_row = payload["feature_rows"][0]
    assert first_row["team_code"] == "LAL"
    assert first_row["games_played_prior"] == 0
    assert "rolling_3_avg_point_margin" in first_row
    assert "point_margin_actual" in first_row
    assert "covered_actual" in first_row


def test_feature_dataset_profile_endpoint_returns_dataset_health_summary() -> None:
    response = client.get(
        "/api/v1/admin/features/dataset/profile"
        "?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["filters"]["team_code"] == "LAL"
    assert payload["filters"]["season_label"] == "2024-2025"
    assert payload["feature_version"]["feature_key"] == "baseline_team_features_v1"
    assert payload["row_count"] == 3
    assert payload["profile"]["season_count"] == 1
    assert payload["profile"]["team_count"] == 1
    assert (
        payload["profile"]["venue_counts"]["home"]
        + payload["profile"]["venue_counts"]["away"]
        == 3
    )
    assert "covered_actual" in payload["profile"]["label_balance"]
    assert "days_rest" in payload["profile"]["feature_coverage"]


def test_feature_patterns_endpoint_returns_bucketed_pattern_summaries() -> None:
    response = client.get(
        "/api/v1/admin/features/patterns"
        "?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025"
        "&target_task=spread_error_regression&dimensions=venue,days_rest_bucket"
        "&min_sample_size=1&limit=10"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["filters"]["target_task"] == "spread_error_regression"
    assert payload["filters"]["dimensions"] == ["venue", "days_rest_bucket"]
    assert payload["task"]["target_column"] == "spread_error_actual"
    assert payload["row_count"] == 3
    assert payload["pattern_count"] >= 2
    assert "pattern_key" in payload["patterns"][0]
    assert "comparable_lookup" in payload["patterns"][0]
    assert "conditions" in payload["patterns"][0]
    assert "sample_size" in payload["patterns"][0]


def test_feature_comparables_endpoint_returns_matching_cases() -> None:
    response = client.get(
        "/api/v1/admin/features/comparables"
        "?repository_mode=in_memory&seed_demo=true&season_label=2024-2025"
        "&target_task=spread_error_regression&dimensions=venue,days_rest_bucket"
        "&condition_values=home,unknown_rest&limit=10"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["filters"]["target_task"] == "spread_error_regression"
    assert payload["filters"]["dimensions"] == ["venue", "days_rest_bucket"]
    assert payload["filters"]["condition_values"] == ["home", "unknown_rest"]
    assert payload["task"]["target_column"] == "spread_error_actual"
    assert payload["comparable_count"] >= 1
    assert payload["comparables"]
    assert payload["comparables"][0]["matched_conditions"]["venue"] == "home"
    assert "similarity_score" in payload["comparables"][0]


def test_feature_comparables_endpoint_accepts_pattern_key() -> None:
    response = client.get(
        "/api/v1/admin/features/comparables"
        "?repository_mode=in_memory&seed_demo=true&season_label=2024-2025"
        "&target_task=spread_error_regression&pattern_key=venue=home|days_rest_bucket=unknown_rest"
        "&limit=10"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["filters"]["pattern_key"] == "venue=home|days_rest_bucket=unknown_rest"
    assert payload["pattern_key"] == "venue=home|days_rest_bucket=unknown_rest"
    assert payload["comparable_count"] >= 1


def test_feature_evidence_endpoint_returns_unified_analysis_payload() -> None:
    response = client.get(
        "/api/v1/admin/features/evidence"
        "?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025"
        "&target_task=spread_error_regression&canonical_game_id=3"
        "&dimensions=venue,days_rest_bucket&comparable_limit=5"
        "&min_pattern_sample_size=1&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["filters"]["canonical_game_id"] == 3
    assert payload["task"]["target_column"] == "spread_error_actual"
    assert payload["evidence"]["summary"]["pattern_key"] is not None
    assert payload["evidence"]["strength"]["rating"] in {
        "weak",
        "moderate",
        "strong",
    }
    assert payload["evidence"]["recommendation"]["status"] in {
        "monitor_only",
        "review_manually",
        "candidate_signal",
    }
    assert (
        payload["evidence"]["recommendation"]["policy_profile"]["target_task"]
        == "spread_error_regression"
    )
    assert payload["evidence"]["pattern"]["selected_pattern"] is not None
    assert payload["evidence"]["comparables"]["anchor_case"]["canonical_game_id"] == 3
    assert "benchmark_rankings" in payload["evidence"]["benchmark_context"]


def test_feature_analysis_materialize_endpoint_persists_pattern_and_evidence_artifacts() -> None:
    response = client.post(
        "/api/v1/admin/features/analysis/materialize"
        "?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025"
        "&target_task=spread_error_regression&canonical_game_id=3"
        "&dimensions=venue,days_rest_bucket&min_sample_size=1"
        "&comparable_limit=5&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["materialized_count"] >= 2
    assert payload["artifact_counts"]["pattern_summary"] >= 1
    assert payload["artifact_counts"]["evidence_bundle"] == 1
    assert any(
        artifact["artifact_type"] == "pattern_summary"
        for artifact in payload["artifacts"]
    )
    assert any(
        artifact["artifact_type"] == "evidence_bundle"
        for artifact in payload["artifacts"]
    )


def test_feature_analysis_artifacts_endpoint_lists_materialized_artifacts() -> None:
    response = client.get(
        "/api/v1/admin/features/analysis/artifacts"
        "?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025"
        "&target_task=spread_error_regression&canonical_game_id=3"
        "&dimensions=venue,days_rest_bucket&min_sample_size=1"
        "&comparable_limit=5&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["artifact_count"] >= 2
    assert payload["filters"]["target_task"] == "spread_error_regression"
    assert any(
        artifact["artifact_type"] == "pattern_summary"
        for artifact in payload["artifacts"]
    )
    assert any(
        artifact["artifact_type"] == "evidence_bundle"
        for artifact in payload["artifacts"]
    )


def test_feature_analysis_history_endpoint_returns_artifact_rollup() -> None:
    response = client.get(
        "/api/v1/admin/features/analysis/history"
        "?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025"
        "&target_task=spread_error_regression&canonical_game_id=3"
        "&dimensions=venue,days_rest_bucket&min_sample_size=1"
        "&comparable_limit=5&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["overview"]["artifact_count"] >= 2
    assert payload["overview"]["artifact_type_counts"]["evidence_bundle"] == 1
    assert payload["overview"]["evidence_status_counts"]["monitor_only"] == 1
    assert payload["daily_buckets"]
    assert payload["latest_evidence_artifacts"]


def test_feature_dataset_splits_endpoint_returns_chronological_split_summary() -> None:
    response = client.get(
        "/api/v1/admin/features/dataset/splits"
        "?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025"
        "&train_ratio=0.5&validation_ratio=0.25&preview_limit=2"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["filters"]["team_code"] == "LAL"
    assert payload["filters"]["season_label"] == "2024-2025"
    assert payload["filters"]["preview_limit"] == 2
    assert payload["row_count"] == 3
    assert payload["split_summary"]["train"]["game_count"] == 1
    assert payload["split_summary"]["validation"]["game_count"] == 1
    assert payload["split_summary"]["test"]["game_count"] == 1
    assert payload["split_summary"]["train"]["row_count"] == 1
    assert payload["split_summary"]["validation"]["row_count"] == 1
    assert payload["split_summary"]["test"]["row_count"] == 1
    assert len(payload["split_previews"]["train"]) == 1


def test_feature_dataset_training_view_endpoint_returns_target_projection() -> None:
    response = client.get(
        "/api/v1/admin/features/dataset/training-view"
        "?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025"
        "&target_task=spread_error_regression"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["filters"]["team_code"] == "LAL"
    assert payload["filters"]["target_task"] == "spread_error_regression"
    assert payload["task"]["target_column"] == "spread_error_actual"
    assert payload["row_count"] == 3
    first_row = payload["training_rows"][0]
    assert first_row["team_code"] == "LAL"
    assert "target_value" in first_row
    assert "games_played_prior" in first_row["features"]
    assert "spread_error_actual" not in first_row["features"]


def test_feature_dataset_training_manifest_endpoint_returns_schema_summary() -> None:
    response = client.get(
        "/api/v1/admin/features/dataset/training-manifest"
        "?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025"
        "&target_task=spread_error_regression"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["filters"]["target_task"] == "spread_error_regression"
    assert payload["task"]["target_column"] == "spread_error_actual"
    assert payload["training_manifest"]["feature_column_count"] > 0
    assert "games_played_prior" in payload["training_manifest"]["feature_columns"]
    assert "games_played_prior" in payload["training_manifest"]["feature_coverage"]


def test_feature_dataset_training_bundle_endpoint_returns_split_task_package() -> None:
    response = client.get(
        "/api/v1/admin/features/dataset/training-bundle"
        "?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025"
        "&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25"
        "&preview_limit=1"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["filters"]["target_task"] == "spread_error_regression"
    assert payload["task"]["target_column"] == "spread_error_actual"
    assert payload["row_count"] == 3
    assert payload["bundle_summary"]["train"]["game_count"] == 1
    assert payload["bundle_summary"]["validation"]["game_count"] == 1
    assert payload["bundle_summary"]["test"]["game_count"] == 1
    assert payload["bundle_summary"]["train"]["target_summary"]["row_count"] == 1
    assert len(payload["split_previews"]["train"]) == 1


def test_feature_dataset_training_benchmark_endpoint_returns_baseline_scores() -> None:
    response = client.get(
        "/api/v1/admin/features/dataset/training-benchmark"
        "?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025"
        "&target_task=spread_error_regression&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["filters"]["target_task"] == "spread_error_regression"
    assert payload["task"]["target_column"] == "spread_error_actual"
    assert payload["row_count"] == 3
    assert "train_mean_baseline" in payload["benchmark_summary"]["validation"]["benchmarks"]
    assert "rolling_3_feature_baseline" in payload["benchmark_summary"]["test"]["benchmarks"]
    assert payload["benchmark_rankings"][0]["primary_metric"] == "mae"


def test_feature_dataset_training_task_matrix_endpoint_returns_task_comparison() -> None:
    response = client.get(
        "/api/v1/admin/features/dataset/training-task-matrix"
        "?repository_mode=in_memory&seed_demo=true&team_code=LAL&season_label=2024-2025"
        "&train_ratio=0.5&validation_ratio=0.25"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["repository_mode"] == "in_memory"
    assert payload["dataset_row_count"] == 3
    assert "spread_error_regression" in payload["task_matrix"]
    assert "cover_classification" in payload["task_matrix"]
    assert (
        payload["task_matrix"]["spread_error_regression"]["task"]["target_column"]
        == "spread_error_actual"
    )
    assert (
        payload["task_matrix"]["spread_error_regression"]["bundle_summary"]["train"][
            "game_count"
        ]
        == 1
    )


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
        return {
            "repository_mode": "in_memory",
            "filters": {"run_label": kwargs["run_label"]},
            "job_runs": [],
        }

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


def test_phase_four_model_backtest_run_endpoint_returns_walk_forward_summary() -> None:
    response = client.post(
        "/api/v1/admin/models/backtests/run",
        params={
            "repository_mode": "in_memory",
            "seed_demo": True,
            "target_task": "spread_error_regression",
            "minimum_train_games": 1,
            "test_window_games": 1,
            "train_ratio": 0.5,
            "validation_ratio": 0.25,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["backtest_run"] is not None
    assert payload["summary"]["fold_count"] >= 1
    assert (
        payload["summary"]["strategy_results"]["candidate_threshold"]["strategy_name"]
        == "candidate_threshold"
    )


def test_phase_four_model_backtest_history_endpoint_returns_recent_runs() -> None:
    response = client.get(
        "/api/v1/admin/models/backtests/history",
        params={
            "repository_mode": "in_memory",
            "seed_demo": True,
            "auto_run_demo": True,
            "target_task": "spread_error_regression",
            "minimum_train_games": 1,
            "test_window_games": 1,
            "train_ratio": 0.5,
            "validation_ratio": 0.25,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["model_backtest_history"]["overview"]["run_count"] >= 1
    assert payload["model_backtest_history"]["overview"]["latest_run"]["target_task"] == (
        "spread_error_regression"
    )
