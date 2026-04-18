import json
import logging
from datetime import date, datetime, timedelta, timezone

import pytest

from bookmaker_detector_api.demo import seed_phase_two_feature_in_memory
from bookmaker_detector_api.services import (
    model_market_board_sources as market_board_sources_module,
)
from bookmaker_detector_api.services.models import (
    get_model_backtest_detail_in_memory,
    get_model_backtest_history_in_memory,
    get_model_evaluation_history_in_memory,
    get_model_future_game_preview_in_memory,
    get_model_future_slate_preview_in_memory,
    get_model_market_board_cadence_batch_history_in_memory,
    get_model_market_board_cadence_dashboard_in_memory,
    get_model_market_board_detail_in_memory,
    get_model_market_board_operations_in_memory,
    get_model_market_board_refresh_batch_history_in_memory,
    get_model_market_board_refresh_history_in_memory,
    get_model_market_board_refresh_queue_in_memory,
    get_model_market_board_scoring_batch_history_in_memory,
    get_model_market_board_scoring_queue_in_memory,
    get_model_market_board_source_run_history_in_memory,
    get_model_opportunity_detail_in_memory,
    get_model_opportunity_history_in_memory,
    get_model_scoring_history_in_memory,
    get_model_scoring_preview_in_memory,
    get_model_scoring_run_detail_in_memory,
    get_model_selection_history_in_memory,
    get_model_training_history_in_memory,
    get_model_training_summary_in_memory,
    list_model_backtest_runs_in_memory,
    list_model_evaluation_snapshots_in_memory,
    list_model_market_board_sources,
    list_model_market_boards_in_memory,
    list_model_opportunities_in_memory,
    list_model_registry_in_memory,
    list_model_scoring_runs_in_memory,
    list_model_selection_snapshots_in_memory,
    list_model_training_runs_in_memory,
    materialize_model_future_game_preview_in_memory,
    materialize_model_future_opportunities_in_memory,
    materialize_model_future_slate_in_memory,
    materialize_model_market_board_in_memory,
    materialize_model_opportunities_in_memory,
    orchestrate_model_market_board_cadence_in_memory,
    orchestrate_model_market_board_refresh_in_memory,
    orchestrate_model_market_board_scoring_in_memory,
    promote_best_model_in_memory,
    refresh_model_market_board_in_memory,
    run_model_backtest_in_memory,
    score_model_market_board_in_memory,
    train_phase_three_models_in_memory,
)
from bookmaker_detector_api.services.workflow_logging import WORKFLOW_LOGGER_NAME


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def _workflow_events(caplog: pytest.LogCaptureFixture) -> list[dict[str, object]]:
    return [
        json.loads(record.getMessage())
        for record in caplog.records
        if record.name == WORKFLOW_LOGGER_NAME
    ]


def test_train_phase_three_models_in_memory_persists_baseline_runs() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()

    result = train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    assert result["feature_version"]["feature_key"] == "baseline_team_features_v1"
    assert result["dataset_row_count"] > 0
    assert len(result["model_runs"]) == 2
    assert result["best_model"] is not None
    assert result["persisted_run_count"] == 2

    registries = list_model_registry_in_memory(
        repository,
        target_task="spread_error_regression",
    )
    runs = list_model_training_runs_in_memory(
        repository,
        target_task="spread_error_regression",
    )

    assert len(registries) == 2
    assert {entry.model_family for entry in registries} == {
        "linear_feature",
        "tree_stump",
    }
    assert len(runs) == 2
    assert {run.artifact["model_family"] for run in runs} == {
        "linear_feature",
        "tree_stump",
    }
    assert any(run.artifact["selected_feature"] is not None for run in runs)
    assert all(run.metrics["validation"]["prediction_count"] > 0 for run in runs)
    assert all("train" in run.metrics for run in runs)
    assert all("validation" in run.metrics for run in runs)
    assert all("test" in run.metrics for run in runs)
    assert all("split_summary" in run.artifact for run in runs)
    assert all("split_target_summary" in run.artifact for run in runs)
    assert all("selection_metrics" in run.artifact for run in runs)
    tree_stump_run = next(run for run in runs if run.artifact["model_family"] == "tree_stump")
    assert tree_stump_run.artifact["fallback_strategy"] == "constant_mean"
    assert tree_stump_run.artifact["fallback_reason"] == "no_valid_split"
    evaluation_snapshots = list_model_evaluation_snapshots_in_memory(
        repository,
        target_task="spread_error_regression",
    )
    assert len(evaluation_snapshots) == 2
    assert {snapshot.model_family for snapshot in evaluation_snapshots} == {
        "linear_feature",
        "tree_stump",
    }


def test_run_model_backtest_in_memory_persists_walk_forward_summary() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()

    result = run_model_backtest_in_memory(
        repository,
        target_task="spread_error_regression",
        minimum_train_games=1,
        test_window_games=1,
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    assert result["feature_version"]["feature_key"] == "baseline_team_features_v1"
    assert result["backtest_run"] is not None
    assert result["summary"]["fold_count"] >= 1
    assert result["summary"]["strategy_results"]["candidate_threshold"]["bet_count"] >= 0
    assert result["summary"]["folds"][0]["selected_model"]["evaluation_snapshot_id"] >= 1
    assert "model_training_run_id" in result["summary"]["folds"][0]["selected_model"]

    runs = list_model_backtest_runs_in_memory(
        repository,
        target_task="spread_error_regression",
    )
    assert len(runs) == 1
    assert runs[0].fold_count == result["summary"]["fold_count"]


def test_run_model_backtest_in_memory_emits_structured_workflow_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()

    with caplog.at_level(logging.INFO, logger=WORKFLOW_LOGGER_NAME):
        result = run_model_backtest_in_memory(
            repository,
            target_task="spread_error_regression",
            minimum_train_games=1,
            test_window_games=1,
            train_ratio=0.5,
            validation_ratio=0.25,
        )

    events = _workflow_events(caplog)
    assert [entry["event"] for entry in events] == [
        "workflow_started",
        "workflow_succeeded",
    ]
    assert events[0]["workflow_name"] == "model_backtest.run"
    assert events[0]["storage_mode"] == "in_memory"
    assert events[1]["backtest_run_id"] == result["backtest_run"]["id"]
    assert events[1]["fold_count"] == result["summary"]["fold_count"]
    assert events[1]["dataset_game_count"] == result["summary"]["dataset_game_count"]
    assert isinstance(events[1]["duration_ms"], float)


def test_model_backtest_history_and_detail_in_memory_return_recent_runs() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()

    run_model_backtest_in_memory(
        repository,
        target_task="spread_error_regression",
        minimum_train_games=1,
        test_window_games=1,
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    history = get_model_backtest_history_in_memory(
        repository,
        target_task="spread_error_regression",
        recent_limit=5,
    )
    detail = get_model_backtest_detail_in_memory(repository, backtest_run_id=1)

    assert history["overview"]["run_count"] == 1
    assert history["overview"]["latest_run"]["id"] == 1
    assert detail is not None
    assert detail["id"] == 1
    assert detail["payload"]["target_task"] == "spread_error_regression"


def test_get_model_training_summary_in_memory_returns_best_and_latest_views() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    summary = get_model_training_summary_in_memory(
        repository,
        target_task="spread_error_regression",
    )

    assert summary["run_count"] == 2
    assert summary["status_counts"] == {"COMPLETED": 2}
    assert summary["model_family_counts"] == {
        "linear_feature": 1,
        "tree_stump": 1,
    }
    assert summary["usable_run_count"] == 2
    assert summary["fallback_run_count"] == 1
    assert summary["latest_run"] is not None
    assert summary["best_overall"] is not None
    assert summary["best_overall"]["artifact"]["model_family"] == "linear_feature"
    assert "linear_feature" in summary["best_by_family"]
    assert "tree_stump" in summary["best_by_family"]


def test_get_model_training_history_in_memory_returns_rollup_and_recent_runs() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    history = get_model_training_history_in_memory(
        repository,
        target_task="spread_error_regression",
        recent_limit=5,
    )

    assert history["overview"]["run_count"] == 2
    assert history["overview"]["fallback_run_count"] == 1
    assert history["overview"]["best_overall"]["artifact"]["model_family"] == "linear_feature"
    assert len(history["daily_buckets"]) == 1
    assert history["daily_buckets"][0]["run_count"] == 2
    assert history["daily_buckets"][0]["usable_run_count"] == 2
    assert history["daily_buckets"][0]["fallback_run_count"] == 1
    assert len(history["recent_runs"]) == 2


def test_get_model_evaluation_history_in_memory_returns_snapshot_rollup() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    history = get_model_evaluation_history_in_memory(
        repository,
        target_task="spread_error_regression",
        recent_limit=5,
    )

    assert history["overview"]["snapshot_count"] == 2
    assert history["overview"]["fallback_strategy_counts"] == {"constant_mean": 1}
    assert history["overview"]["latest_snapshot"] is not None
    assert len(history["daily_buckets"]) == 1
    assert history["daily_buckets"][0]["snapshot_count"] == 2
    assert history["daily_buckets"][0]["fallback_count"] == 1
    assert len(history["recent_snapshots"]) == 2


def test_promote_best_model_in_memory_selects_non_fallback_candidate() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    promotion = promote_best_model_in_memory(
        repository,
        target_task="spread_error_regression",
    )

    assert promotion["selected_snapshot"] is not None
    assert promotion["selected_snapshot"]["model_family"] == "linear_feature"
    assert promotion["active_selection"] is not None
    assert promotion["active_selection"]["model_family"] == "linear_feature"
    selections = list_model_selection_snapshots_in_memory(
        repository,
        target_task="spread_error_regression",
    )
    assert len(selections) == 1
    assert selections[0].is_active is True


def test_get_model_selection_history_in_memory_returns_active_selection_summary() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
    )
    promote_best_model_in_memory(
        repository,
        target_task="spread_error_regression",
    )

    history = get_model_selection_history_in_memory(
        repository,
        target_task="spread_error_regression",
        recent_limit=5,
    )

    assert history["overview"]["selection_count"] == 1
    assert history["overview"]["active_selection_count"] == 1
    assert history["overview"]["model_family_counts"] == {"linear_feature": 1}
    assert history["overview"]["latest_selection"]["model_family"] == "linear_feature"
    assert len(history["recent_selections"]) == 1


def test_get_model_scoring_preview_in_memory_uses_active_selection() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        team_code="LAL",
        season_label="2024-2025",
        train_ratio=0.5,
        validation_ratio=0.25,
    )
    promote_best_model_in_memory(
        repository,
        target_task="spread_error_regression",
    )

    scoring_preview = get_model_scoring_preview_in_memory(
        repository,
        target_task="spread_error_regression",
        team_code="LAL",
        season_label="2024-2025",
        canonical_game_id=3,
        limit=5,
        include_evidence=True,
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    assert scoring_preview["active_selection"] is not None
    assert scoring_preview["active_selection"]["model_family"] == "linear_feature"
    assert scoring_preview["active_evaluation_snapshot"] is not None
    assert scoring_preview["row_count"] == 1
    assert scoring_preview["scored_prediction_count"] == 1
    assert scoring_preview["prediction_summary"]["prediction_count"] == 1
    prediction = scoring_preview["predictions"][0]
    assert prediction["team_code"] == "LAL"
    assert prediction["model"]["model_family"] == "linear_feature"
    assert prediction["prediction_context"]["target_type"] == "spread_error"
    assert prediction["evidence"] is not None
    assert prediction["evidence"]["summary"] is not None
    assert prediction["evidence"]["recommendation"] is not None


def test_materialize_model_opportunities_in_memory_persists_reviewable_signal() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        team_code="LAL",
        season_label="2024-2025",
        train_ratio=0.5,
        validation_ratio=0.25,
    )
    promote_best_model_in_memory(
        repository,
        target_task="spread_error_regression",
    )

    materialized = materialize_model_opportunities_in_memory(
        repository,
        target_task="spread_error_regression",
        team_code="LAL",
        season_label="2024-2025",
        canonical_game_id=3,
        limit=5,
        include_evidence=True,
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    assert materialized["materialized_count"] == 1
    assert materialized["opportunity_count"] == 1
    opportunity = materialized["opportunities"][0]
    assert opportunity["team_code"] == "LAL"
    assert opportunity["status"] == "review_manually"
    assert opportunity["policy_name"] == "spread_edge_policy_v1"
    stored = list_model_opportunities_in_memory(
        repository,
        target_task="spread_error_regression",
    )
    assert len(stored) == 1
    assert stored[0].status == "review_manually"


def test_materialize_model_opportunities_in_memory_emits_structured_workflow_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        team_code="LAL",
        season_label="2024-2025",
        train_ratio=0.5,
        validation_ratio=0.25,
    )
    promote_best_model_in_memory(
        repository,
        target_task="spread_error_regression",
    )

    with caplog.at_level(logging.INFO, logger=WORKFLOW_LOGGER_NAME):
        materialized = materialize_model_opportunities_in_memory(
            repository,
            target_task="spread_error_regression",
            team_code="LAL",
            season_label="2024-2025",
            canonical_game_id=3,
            limit=5,
            include_evidence=True,
            train_ratio=0.5,
            validation_ratio=0.25,
        )

    events = _workflow_events(caplog)
    assert [entry["event"] for entry in events] == [
        "workflow_started",
        "workflow_succeeded",
    ]
    assert events[0]["workflow_name"] == "model_opportunities.materialize"
    assert events[1]["opportunity_count"] == materialized["opportunity_count"]
    assert events[1]["materialized_count"] == materialized["materialized_count"]
    assert events[1]["scoring_preview_count"] == materialized["scored_prediction_count"]


def test_get_model_future_game_preview_in_memory_scores_both_perspectives() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
    )
    promote_best_model_in_memory(
        repository,
        target_task="spread_error_regression",
    )

    preview = get_model_future_game_preview_in_memory(
        repository,
        target_task="spread_error_regression",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        home_team_code="LAL",
        away_team_code="BOS",
        home_spread_line=-3.5,
        total_line=228.5,
        include_evidence=True,
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    assert preview["active_selection"] is not None
    assert preview["scenario"] is not None
    assert preview["scenario"]["home_team_code"] == "LAL"
    assert preview["scenario"]["away_team_code"] == "BOS"
    assert preview["scored_prediction_count"] == 2
    assert len(preview["predictions"]) == 2
    assert {entry["team_code"] for entry in preview["predictions"]} == {"LAL", "BOS"}
    assert preview["predictions"][0]["market_context"]["total_line"] == 228.5
    assert len(preview["opportunity_preview"]) >= 1


def test_materialize_model_future_game_preview_in_memory_persists_scoring_run() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
    )
    promote_best_model_in_memory(
        repository,
        target_task="spread_error_regression",
    )

    materialized = materialize_model_future_game_preview_in_memory(
        repository,
        target_task="spread_error_regression",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        home_team_code="LAL",
        away_team_code="BOS",
        home_spread_line=-3.5,
        total_line=228.5,
        include_evidence=True,
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    assert materialized["materialized_count"] == 1
    assert materialized["scoring_run"] is not None
    assert materialized["scoring_run"]["scenario_key"] == "2025-2026:2026-04-20:LAL:BOS"
    stored = list_model_scoring_runs_in_memory(
        repository,
        target_task="spread_error_regression",
    )
    assert len(stored) == 1
    assert stored[0].prediction_count == 2


def test_get_model_scoring_run_detail_and_history_in_memory_return_persisted_preview() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
    )
    promote_best_model_in_memory(
        repository,
        target_task="spread_error_regression",
    )
    materialize_model_future_game_preview_in_memory(
        repository,
        target_task="spread_error_regression",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        home_team_code="LAL",
        away_team_code="BOS",
        home_spread_line=-3.5,
        total_line=228.5,
        include_evidence=True,
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    detail = get_model_scoring_run_detail_in_memory(repository, scoring_run_id=1)
    history = get_model_scoring_history_in_memory(
        repository,
        target_task="spread_error_regression",
        recent_limit=5,
    )

    assert detail is not None
    assert detail["id"] == 1
    assert detail["home_team_code"] == "LAL"
    assert detail["payload"]["scenario"]["away_team_code"] == "BOS"
    assert history["overview"]["scoring_run_count"] == 1
    assert history["overview"]["prediction_count"] == 2
    assert len(history["daily_buckets"]) == 1
    assert len(history["recent_scoring_runs"]) == 1


def test_materialize_model_future_opportunities_in_memory_persists_future_review_rows() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
    )
    promote_best_model_in_memory(
        repository,
        target_task="spread_error_regression",
    )

    materialized = materialize_model_future_opportunities_in_memory(
        repository,
        target_task="spread_error_regression",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        home_team_code="LAL",
        away_team_code="BOS",
        home_spread_line=-3.5,
        total_line=228.5,
        include_evidence=True,
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    assert materialized["materialized_count"] == 1
    assert materialized["scoring_run"] is not None
    stored = list_model_opportunities_in_memory(
        repository,
        target_task="spread_error_regression",
        source_kind="future_scenario",
        season_label="2025-2026",
    )
    assert len(stored) >= 1
    assert stored[0].source_kind == "future_scenario"
    assert stored[0].scenario_key == "2025-2026:2026-04-20:LAL:BOS"
    assert stored[0].model_scoring_run_id == materialized["scoring_run"]["id"]
    assert stored[0].canonical_game_id is None


def test_get_model_future_slate_preview_in_memory_returns_batch_summary() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
    )
    promote_best_model_in_memory(
        repository,
        target_task="spread_error_regression",
    )

    preview = get_model_future_slate_preview_in_memory(
        repository,
        target_task="spread_error_regression",
        slate_label="demo-slate",
        games=[
            {
                "season_label": "2025-2026",
                "game_date": date(2026, 4, 20),
                "home_team_code": "LAL",
                "away_team_code": "BOS",
                "home_spread_line": -3.5,
                "total_line": 228.5,
            },
            {
                "season_label": "2025-2026",
                "game_date": date(2026, 4, 21),
                "home_team_code": "NYK",
                "away_team_code": "MIA",
                "home_spread_line": -1.5,
                "total_line": 219.5,
            },
        ],
        include_evidence=True,
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    assert preview["slate"]["slate_key"] == "spread_error_regression:demo-slate"
    assert preview["slate"]["game_count"] == 2
    assert preview["game_preview_count"] == 2
    assert preview["scored_prediction_count"] == 4
    assert len(preview["games"]) == 2
    assert preview["games"][0]["scenario"] is not None
    assert preview["games"][1]["scenario"]["home_team_code"] == "NYK"


def test_materialize_model_future_slate_in_memory_persists_batch_runs_and_opportunities() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
    )
    promote_best_model_in_memory(
        repository,
        target_task="spread_error_regression",
    )

    materialized = materialize_model_future_slate_in_memory(
        repository,
        target_task="spread_error_regression",
        slate_label="demo-slate",
        games=[
            {
                "season_label": "2025-2026",
                "game_date": date(2026, 4, 20),
                "home_team_code": "LAL",
                "away_team_code": "BOS",
                "home_spread_line": -3.5,
                "total_line": 228.5,
            },
            {
                "season_label": "2025-2026",
                "game_date": date(2026, 4, 21),
                "home_team_code": "NYK",
                "away_team_code": "MIA",
                "home_spread_line": -1.5,
                "total_line": 219.5,
            },
        ],
        include_evidence=True,
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    assert materialized["slate"]["slate_key"] == "spread_error_regression:demo-slate"
    assert materialized["materialized_scoring_run_count"] == 2
    assert len(materialized["scoring_runs"]) == 2
    assert materialized["materialized_opportunity_count"] >= 2
    stored_runs = list_model_scoring_runs_in_memory(
        repository,
        target_task="spread_error_regression",
        season_label="2025-2026",
    )
    stored_opportunities = list_model_opportunities_in_memory(
        repository,
        target_task="spread_error_regression",
        source_kind="future_scenario",
        season_label="2025-2026",
    )
    assert len(stored_runs) == 2
    assert len(stored_opportunities) >= 2


def test_materialize_model_market_board_in_memory_persists_board() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()

    materialized = materialize_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        slate_label="demo-market-board",
        games=[
            {
                "season_label": "2025-2026",
                "game_date": date(2026, 4, 20),
                "home_team_code": "LAL",
                "away_team_code": "BOS",
                "home_spread_line": -3.5,
                "total_line": 228.5,
            }
        ],
    )

    assert materialized["board"] is not None
    assert materialized["board"]["board_key"] == "spread_error_regression:demo-market-board"
    boards = list_model_market_boards_in_memory(
        repository,
        target_task="spread_error_regression",
        season_label="2025-2026",
    )
    assert len(boards) == 1
    assert boards[0].game_count == 1


def test_refresh_model_market_board_in_memory_uses_builtin_source() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()

    result = refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_daily_lines_v1",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-refresh-board",
        game_count=2,
    )

    assert result["source_name"] == "demo_daily_lines_v1"
    assert result["generated_game_count"] == 2
    assert result["board"] is not None
    assert result["board"]["board_key"] == "spread_error_regression:demo-refresh-board"
    assert result["board"]["payload"]["source"]["source_name"] == "demo_daily_lines_v1"
    assert result["board"]["freshness"]["freshness_status"] == "fresh"
    assert result["change_summary"]["added_game_count"] == 2
    assert result["change_summary"]["changed_game_count"] == 0
    assert result["source_payload_fingerprints"]["raw_game_count"] == 2
    assert (
        result["change_summary"]["source_fingerprint_comparison"]["previous_fingerprints_available"]
        is False
    )


def test_refresh_model_market_board_in_memory_tracks_updated_change_summary() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    materialize_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        slate_label="demo-refresh-board",
        games=[
            {
                "season_label": "2025-2026",
                "game_date": date(2026, 4, 20),
                "home_team_code": "LAL",
                "away_team_code": "BOS",
                "home_spread_line": -3.5,
                "total_line": 228.5,
            },
            {
                "season_label": "2025-2026",
                "game_date": date(2026, 4, 20),
                "home_team_code": "NYK",
                "away_team_code": "MIA",
                "home_spread_line": -1.5,
                "total_line": 219.5,
            },
        ],
    )

    result = refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_daily_lines_v1",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-refresh-board",
        game_count=2,
    )

    assert result["board"] is not None
    assert result["board"]["payload"]["source"]["last_refresh_status"] == "updated"
    assert result["change_summary"]["removed_game_count"] == 2
    assert result["change_summary"]["added_game_count"] == 2
    assert result["change_summary"]["generated_game_count"] == 2


def test_list_model_market_board_sources_returns_demo_source() -> None:
    catalog = list_model_market_board_sources()

    assert catalog["sources"]
    assert catalog["sources"][0]["source_name"] == "demo_daily_lines_v1"
    assert any(entry["source_name"] == "demo_source_failure_v1" for entry in catalog["sources"])
    assert any(entry["source_name"] == "file_market_board_v1" for entry in catalog["sources"])


def test_refresh_model_market_board_in_memory_supports_file_source() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()

    result = refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="file_market_board_v1",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-file-refresh-board",
        source_path="fixture://demo_market_board_file_source.json",
    )

    assert result["status"] == "SUCCESS"
    assert result["source_path"] == "fixture://demo_market_board_file_source.json"
    assert result["generated_game_count"] == 2
    assert result["board"] is not None
    assert result["board"]["game_count"] == 2
    assert result["board"]["payload"]["source"]["source_path"] == (
        "fixture://demo_market_board_file_source.json"
    )
    assert result["generated_games"][0]["home_team_code"] == "MIL"


def test_orchestrate_model_market_board_refresh_in_memory_reuses_file_source_path() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="file_market_board_v1",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-file-refresh-board",
        source_path="fixture://demo_market_board_file_source.json",
    )
    repository.model_market_boards[0]["payload"]["source"]["refreshed_at"] = (
        _utc_today() - timedelta(days=2)
    ).isoformat()

    result = orchestrate_model_market_board_refresh_in_memory(
        repository,
        target_task="spread_error_regression",
        season_label="2025-2026",
        source_name="file_market_board_v1",
        pending_only=True,
        recent_limit=5,
    )

    assert result["candidate_board_count"] == 1
    assert result["refresh_runs"][0]["board"]["payload"]["source"]["source_path"] == (
        "fixture://demo_market_board_file_source.json"
    )


def test_refresh_model_market_board_in_memory_supports_external_odds_source(
    monkeypatch,
) -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()

    monkeypatch.setattr(market_board_sources_module.settings, "the_odds_api_key", "test-key")
    monkeypatch.setattr(
        market_board_sources_module,
        "_fetch_the_odds_api_games",
        lambda: [
            {
                "home_team": "LAL",
                "away_team": "BOS",
                "commence_time": "2026-04-20T23:00:00Z",
                "bookmakers": [
                    {
                        "markets": [
                            {
                                "key": "spreads",
                                "outcomes": [
                                    {"name": "LAL", "point": -3.5},
                                    {"name": "BOS", "point": 3.5},
                                ],
                            },
                            {
                                "key": "totals",
                                "outcomes": [
                                    {"name": "Over", "point": 228.5},
                                    {"name": "Under", "point": 228.5},
                                ],
                            },
                        ]
                    }
                ],
            }
        ],
    )

    result = refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="the_odds_api_v4_nba",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-odds-api-refresh-board",
    )

    assert result["status"] == "SUCCESS"
    assert result["generated_game_count"] == 1
    assert result["source_request_context"]["sport_key"] == "basketball_nba"
    assert result["board"] is not None
    assert result["generated_games"][0]["home_team_code"] == "LAL"
    assert result["generated_games"][0]["total_line"] == 228.5


def test_market_board_refresh_in_memory_persists_failed_source_run() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()

    result = refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_source_failure_v1",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-failing-refresh-board",
        game_count=2,
    )
    history = get_model_market_board_source_run_history_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_source_failure_v1",
        season_label="2025-2026",
        recent_limit=5,
    )

    assert result["status"] == "FAILED"
    assert result["error_message"]
    assert result["board"] is None
    assert result["change_summary"] is None
    assert result["generated_game_count"] == 0
    assert result["source_run"] is not None
    assert result["source_run"]["status"] == "FAILED"
    assert result["source_run"]["payload"]["error_message"] == result["error_message"]
    assert history["overview"]["source_run_count"] == 1
    assert history["overview"]["generated_game_count"] == 0
    assert history["overview"]["status_counts"]["FAILED"] == 1
    assert history["recent_source_runs"][0]["payload"]["error_message"] == result["error_message"]


def test_market_board_refresh_in_memory_normalizes_partial_source_rows() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()

    result = refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_partial_lines_v1",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-partial-refresh-board",
        game_count=3,
    )
    history = get_model_market_board_source_run_history_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_partial_lines_v1",
        season_label="2025-2026",
        recent_limit=5,
    )

    assert result["status"] == "SUCCESS_WITH_WARNINGS"
    assert result["error_message"] is None
    assert result["generated_game_count"] == 1
    assert result["board"] is not None
    assert result["board"]["game_count"] == 1
    assert result["generated_games"][0]["home_team_code"] == "LAL"
    assert result["generated_games"][0]["away_team_code"] == "BOS"
    assert result["validation_summary"]["raw_row_count"] == 3
    assert result["validation_summary"]["valid_row_count"] == 1
    assert result["validation_summary"]["invalid_row_count"] == 2
    assert result["validation_summary"]["warning_count"] == 1
    assert result["source_payload_fingerprints"]["raw_game_count"] == 3
    assert result["source_payload_fingerprints"]["normalized_game_count"] == 1
    assert history["overview"]["generated_game_count"] == 1
    assert history["overview"]["invalid_row_count"] == 2
    assert history["overview"]["warning_count"] == 1
    assert history["overview"]["status_counts"]["SUCCESS_WITH_WARNINGS"] == 1
    assert (
        history["recent_source_runs"][0]["payload"]["validation_summary"]["invalid_row_count"] == 2
    )


def test_market_board_refresh_in_memory_tracks_raw_payload_drift_when_normalized_board_is_same(
) -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    first_result = refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_partial_lines_v1",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-partial-refresh-board",
        game_count=3,
    )

    repository.model_market_boards[0]["payload"]["source"]["source_payload_fingerprints"][
        "raw_payload_sha256"
    ] = "previous-raw-hash"

    second_result = refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_partial_lines_v1",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-partial-refresh-board",
        game_count=3,
    )

    fingerprint_comparison = second_result["change_summary"]["source_fingerprint_comparison"]
    assert first_result["board"] is not None
    assert second_result["board"] is not None
    assert second_result["board"]["payload"]["source"]["last_refresh_status"] == "unchanged"
    assert second_result["change_summary"]["unchanged_game_count"] == 1
    assert fingerprint_comparison["previous_fingerprints_available"] is True
    assert fingerprint_comparison["raw_payload_changed"] is True
    assert fingerprint_comparison["normalized_payload_changed"] is False
    assert fingerprint_comparison["raw_changed_but_normalized_same"] is True


def test_market_board_refresh_history_in_memory_tracks_created_and_unchanged() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_daily_lines_v1",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-refresh-board",
        game_count=2,
    )
    refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_daily_lines_v1",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-refresh-board",
        game_count=2,
    )

    history = get_model_market_board_refresh_history_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_daily_lines_v1",
        recent_limit=5,
    )
    board_detail = get_model_market_board_detail_in_memory(repository, board_id=1)

    assert history["overview"]["refresh_event_count"] == 2
    assert history["overview"]["status_counts"]["created"] == 1
    assert history["overview"]["status_counts"]["unchanged"] == 1
    assert board_detail is not None
    assert board_detail["freshness"]["refresh_count"] == 2
    assert board_detail["freshness"]["last_refresh_status"] == "unchanged"
    assert (
        history["recent_refresh_events"][0]["payload"]["change_summary"]["unchanged_game_count"]
        == 2
    )


def test_market_board_source_run_history_in_memory_tracks_refresh_inputs() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    result = refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_daily_lines_v1",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-refresh-board",
        game_count=2,
    )

    history = get_model_market_board_source_run_history_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_daily_lines_v1",
        season_label="2025-2026",
        recent_limit=5,
    )

    assert result["source_run"] is not None
    assert history["overview"]["source_run_count"] == 1
    assert history["overview"]["generated_game_count"] == 2
    assert history["overview"]["status_counts"]["SUCCESS"] == 1
    assert history["recent_source_runs"][0]["payload"]["request"]["requested_game_count"] == 2


def test_market_board_refresh_queue_in_memory_tracks_pending_refresh_and_current() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_daily_lines_v1",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-refresh-board",
        game_count=2,
    )

    repository.model_market_boards[0]["payload"]["source"]["refreshed_at"] = (
        _utc_today() - timedelta(days=2)
    ).isoformat()

    pending_queue = get_model_market_board_refresh_queue_in_memory(
        repository,
        target_task="spread_error_regression",
        season_label="2025-2026",
        source_name="demo_daily_lines_v1",
        pending_only=True,
    )
    full_queue = get_model_market_board_refresh_queue_in_memory(
        repository,
        target_task="spread_error_regression",
        season_label="2025-2026",
        source_name="demo_daily_lines_v1",
        pending_only=False,
    )

    assert pending_queue["overview"]["pending_refresh_count"] == 1
    assert pending_queue["queue_entries"][0]["queue_status"] == "pending_refresh"
    assert pending_queue["queue_entries"][0]["refreshable"] is True
    assert full_queue["queue_entries"][0]["freshness_status"] == "stale"


def test_orchestrate_model_market_board_refresh_in_memory_refreshes_stale_boards() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_daily_lines_v1",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-refresh-board",
        game_count=2,
    )
    repository.model_market_boards[0]["payload"]["source"]["refreshed_at"] = (
        _utc_today() - timedelta(days=2)
    ).isoformat()

    result = orchestrate_model_market_board_refresh_in_memory(
        repository,
        target_task="spread_error_regression",
        season_label="2025-2026",
        source_name="demo_daily_lines_v1",
        pending_only=True,
        recent_limit=5,
    )

    assert result["candidate_board_count"] == 1
    assert result["refreshed_board_count"] == 1
    assert result["queue_before"]["overview"]["pending_refresh_count"] == 1
    assert result["queue_after"]["overview"]["pending_refresh_count"] == 0
    assert result["refresh_batch"]["candidate_board_count"] == 1
    assert result["refresh_runs"][0]["refresh_result_status"] == "unchanged"


def test_market_board_refresh_batch_history_in_memory_rolls_up_batches() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_daily_lines_v1",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-refresh-board",
        game_count=2,
    )
    repository.model_market_boards[0]["payload"]["source"]["refreshed_at"] = (
        _utc_today() - timedelta(days=2)
    ).isoformat()
    orchestrate_model_market_board_refresh_in_memory(
        repository,
        target_task="spread_error_regression",
        season_label="2025-2026",
        source_name="demo_daily_lines_v1",
        pending_only=True,
        recent_limit=5,
    )

    history = get_model_market_board_refresh_batch_history_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_daily_lines_v1",
        recent_limit=5,
    )

    assert history["overview"]["batch_count"] == 1
    assert history["overview"]["candidate_board_count"] == 1
    assert history["overview"]["refreshed_board_count"] == 1
    assert history["overview"]["unchanged_board_count"] == 1
    assert history["recent_batches"][0]["target_task"] == "spread_error_regression"


def test_score_model_market_board_in_memory_materializes_slate_results() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
    )
    promote_best_model_in_memory(
        repository,
        target_task="spread_error_regression",
    )
    materialize_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        slate_label="demo-market-board",
        games=[
            {
                "season_label": "2025-2026",
                "game_date": date(2026, 4, 20),
                "home_team_code": "LAL",
                "away_team_code": "BOS",
                "home_spread_line": -3.5,
                "total_line": 228.5,
            }
        ],
    )

    result = score_model_market_board_in_memory(
        repository,
        board_id=1,
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    assert result["board"] is not None
    assert result["board"]["id"] == 1
    assert result["slate_result"] is not None
    assert result["slate_result"]["materialized_scoring_run_count"] == 1
    detail = get_model_market_board_detail_in_memory(repository, board_id=1)
    assert detail is not None
    assert detail["payload"]["games"][0]["home_team_code"] == "LAL"


def test_market_board_scoring_queue_in_memory_tracks_pending_and_current() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
    )
    promote_best_model_in_memory(repository, target_task="spread_error_regression")
    refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_daily_lines_v1",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-refresh-board",
        game_count=2,
    )

    queue_before = get_model_market_board_scoring_queue_in_memory(
        repository,
        target_task="spread_error_regression",
        season_label="2025-2026",
        source_name="demo_daily_lines_v1",
        freshness_status="fresh",
        pending_only=True,
    )
    score_model_market_board_in_memory(
        repository,
        board_id=1,
        train_ratio=0.5,
        validation_ratio=0.25,
    )
    queue_after = get_model_market_board_scoring_queue_in_memory(
        repository,
        target_task="spread_error_regression",
        season_label="2025-2026",
        source_name="demo_daily_lines_v1",
        freshness_status="fresh",
        pending_only=False,
    )

    assert queue_before["overview"]["pending_board_count"] == 1
    assert queue_before["queue_entries"][0]["scoring_status"] == "unscored"
    assert queue_after["overview"]["pending_board_count"] == 0
    assert queue_after["queue_entries"][0]["scoring_status"] == "current"
    assert queue_after["queue_entries"][0]["latest_scoring_run"]["model_market_board_id"] == 1


def test_orchestrate_model_market_board_scoring_in_memory_scores_pending_boards() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
    )
    promote_best_model_in_memory(repository, target_task="spread_error_regression")
    refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_daily_lines_v1",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-refresh-board",
        game_count=2,
    )

    result = orchestrate_model_market_board_scoring_in_memory(
        repository,
        target_task="spread_error_regression",
        season_label="2025-2026",
        source_name="demo_daily_lines_v1",
        freshness_status="fresh",
        pending_only=True,
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    assert result["candidate_board_count"] == 1
    assert result["scored_board_count"] == 1
    assert result["materialized_scoring_run_count"] == 2
    assert result["queue_before"]["overview"]["pending_board_count"] == 1
    assert result["queue_after"]["overview"]["pending_board_count"] == 0
    assert result["orchestration_batch"]["candidate_board_count"] == 1


def test_market_board_scoring_batch_history_in_memory_rolls_up_batches() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
    )
    promote_best_model_in_memory(repository, target_task="spread_error_regression")
    refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_daily_lines_v1",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-refresh-board",
        game_count=2,
    )
    orchestrate_model_market_board_scoring_in_memory(
        repository,
        target_task="spread_error_regression",
        season_label="2025-2026",
        source_name="demo_daily_lines_v1",
        freshness_status="fresh",
        pending_only=True,
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    history = get_model_market_board_scoring_batch_history_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_daily_lines_v1",
        recent_limit=5,
    )

    assert history["overview"]["batch_count"] == 1
    assert history["overview"]["candidate_board_count"] == 1
    assert history["overview"]["materialized_scoring_run_count"] == 2
    assert history["overview"]["freshness_status_counts"]["fresh"] == 1
    assert history["recent_batches"][0]["target_task"] == "spread_error_regression"


def test_orchestrate_model_market_board_cadence_in_memory_runs_refresh_then_scoring() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
    )
    promote_best_model_in_memory(repository, target_task="spread_error_regression")
    refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_daily_lines_v1",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-refresh-board",
        game_count=2,
    )
    repository.model_market_boards[0]["payload"]["source"]["refreshed_at"] = (
        _utc_today() - timedelta(days=2)
    ).isoformat()

    result = orchestrate_model_market_board_cadence_in_memory(
        repository,
        target_task="spread_error_regression",
        season_label="2025-2026",
        source_name="demo_daily_lines_v1",
        refresh_freshness_status="stale",
        refresh_pending_only=True,
        scoring_freshness_status="fresh",
        scoring_pending_only=True,
        train_ratio=0.5,
        validation_ratio=0.25,
        recent_limit=5,
    )

    assert result["refreshed_board_count"] == 1
    assert result["scored_board_count"] == 1
    assert result["materialized_scoring_run_count"] == 2
    assert result["materialized_opportunity_count"] == 2
    assert result["refresh_result"]["queue_before"]["overview"]["pending_refresh_count"] == 1
    assert result["scoring_result"]["queue_after"]["overview"]["pending_board_count"] == 0
    assert result["cadence_batch"]["refreshed_board_count"] == 1


def test_orchestrate_model_market_board_cadence_in_memory_emits_structured_workflow_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
    )
    promote_best_model_in_memory(repository, target_task="spread_error_regression")
    refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_daily_lines_v1",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-refresh-board",
        game_count=2,
    )
    repository.model_market_boards[0]["payload"]["source"]["refreshed_at"] = (
        _utc_today() - timedelta(days=2)
    ).isoformat()

    with caplog.at_level(logging.INFO, logger=WORKFLOW_LOGGER_NAME):
        result = orchestrate_model_market_board_cadence_in_memory(
            repository,
            target_task="spread_error_regression",
            season_label="2025-2026",
            source_name="demo_daily_lines_v1",
            refresh_freshness_status="stale",
            refresh_pending_only=True,
            scoring_freshness_status="fresh",
            scoring_pending_only=True,
            train_ratio=0.5,
            validation_ratio=0.25,
            recent_limit=5,
        )

    events = _workflow_events(caplog)
    workflow_names = [
        entry["workflow_name"] for entry in events if entry["event"] == "workflow_succeeded"
    ]
    assert "model_market_board.refresh" in workflow_names
    assert "model_market_board.refresh_orchestration" in workflow_names
    assert "model_market_board.scoring_orchestration" in workflow_names
    assert "model_market_board.cadence_orchestration" in workflow_names
    cadence_success = next(
        entry
        for entry in events
        if entry["event"] == "workflow_succeeded"
        and entry["workflow_name"] == "model_market_board.cadence_orchestration"
    )
    assert cadence_success["refreshed_board_count"] == result["refreshed_board_count"]
    assert cadence_success["scored_board_count"] == result["scored_board_count"]
    assert (
        cadence_success["materialized_opportunity_count"]
        == result["materialized_opportunity_count"]
    )


def test_market_board_cadence_batch_history_in_memory_rolls_up_batches() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
    )
    promote_best_model_in_memory(repository, target_task="spread_error_regression")
    refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_daily_lines_v1",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-refresh-board",
        game_count=2,
    )
    repository.model_market_boards[0]["payload"]["source"]["refreshed_at"] = (
        _utc_today() - timedelta(days=2)
    ).isoformat()
    orchestrate_model_market_board_cadence_in_memory(
        repository,
        target_task="spread_error_regression",
        season_label="2025-2026",
        source_name="demo_daily_lines_v1",
        refresh_freshness_status="stale",
        refresh_pending_only=True,
        scoring_freshness_status="fresh",
        scoring_pending_only=True,
        train_ratio=0.5,
        validation_ratio=0.25,
        recent_limit=5,
    )

    history = get_model_market_board_cadence_batch_history_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_daily_lines_v1",
        recent_limit=5,
    )

    assert history["overview"]["batch_count"] == 1
    assert history["overview"]["refreshed_board_count"] == 1
    assert history["overview"]["scored_board_count"] == 1
    assert history["overview"]["materialized_scoring_run_count"] == 2
    assert history["recent_batches"][0]["target_task"] == "spread_error_regression"


def test_market_board_operations_in_memory_returns_operational_summary() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
    )
    promote_best_model_in_memory(repository, target_task="spread_error_regression")
    refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_daily_lines_v1",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-refresh-board",
        game_count=2,
    )
    repository.model_market_boards[0]["payload"]["source"]["refreshed_at"] = (
        _utc_today() - timedelta(days=2)
    ).isoformat()
    orchestrate_model_market_board_refresh_in_memory(
        repository,
        target_task="spread_error_regression",
        season_label="2025-2026",
        source_name="demo_daily_lines_v1",
        pending_only=True,
        recent_limit=5,
    )
    orchestrate_model_market_board_scoring_in_memory(
        repository,
        target_task="spread_error_regression",
        season_label="2025-2026",
        source_name="demo_daily_lines_v1",
        freshness_status="fresh",
        pending_only=True,
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    operations = get_model_market_board_operations_in_memory(
        repository,
        board_id=1,
        recent_limit=5,
    )

    assert operations is not None
    assert operations["board"]["id"] == 1
    assert operations["queue_entry"]["scoring_status"] == "current"
    assert operations["source_runs"]["source_run_count"] == 2
    assert operations["refresh"]["refresh_event_count"] == 2
    assert operations["refresh_orchestration"]["batch_count"] == 1
    assert operations["cadence"]["batch_count"] == 0
    assert operations["scoring"]["scoring_run_count"] == 2
    assert operations["opportunities"]["opportunity_count"] == 2
    assert operations["orchestration"]["batch_count"] == 1


def test_market_board_cadence_dashboard_in_memory_returns_actionable_status() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
    )
    promote_best_model_in_memory(repository, target_task="spread_error_regression")
    refresh_model_market_board_in_memory(
        repository,
        target_task="spread_error_regression",
        source_name="demo_daily_lines_v1",
        season_label="2025-2026",
        game_date=date(2026, 4, 20),
        slate_label="demo-refresh-board",
        game_count=2,
    )
    orchestrate_model_market_board_scoring_in_memory(
        repository,
        target_task="spread_error_regression",
        season_label="2025-2026",
        source_name="demo_daily_lines_v1",
        freshness_status="fresh",
        pending_only=True,
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    dashboard = get_model_market_board_cadence_dashboard_in_memory(
        repository,
        target_task="spread_error_regression",
        season_label="2025-2026",
        source_name="demo_daily_lines_v1",
        recent_limit=5,
    )

    assert dashboard["overview"]["board_count"] == 1
    assert dashboard["overview"]["cadence_status_counts"]["recently_scored"] == 1
    assert dashboard["overview"]["priority_counts"]["low"] == 1
    assert dashboard["cadence_entries"][0]["cadence_status"] == "recently_scored"
    assert dashboard["cadence_entries"][0]["priority"] == "low"


def test_get_model_opportunity_history_in_memory_returns_rollup() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    train_phase_three_models_in_memory(
        repository,
        target_task="spread_error_regression",
        team_code="LAL",
        season_label="2024-2025",
        train_ratio=0.5,
        validation_ratio=0.25,
    )
    promote_best_model_in_memory(
        repository,
        target_task="spread_error_regression",
    )
    materialize_model_opportunities_in_memory(
        repository,
        target_task="spread_error_regression",
        team_code="LAL",
        season_label="2024-2025",
        canonical_game_id=3,
        limit=5,
        include_evidence=True,
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    history = get_model_opportunity_history_in_memory(
        repository,
        target_task="spread_error_regression",
        recent_limit=5,
    )
    detail = get_model_opportunity_detail_in_memory(repository, opportunity_id=1)

    assert history["overview"]["opportunity_count"] == 1
    assert history["overview"]["status_counts"] == {"review_manually": 1}
    assert history["overview"]["latest_opportunity"]["team_code"] == "LAL"
    assert len(history["daily_buckets"]) == 1
    assert history["daily_buckets"][0]["opportunity_count"] == 1
    assert detail is not None
    assert detail["id"] == 1
    assert detail["payload"]["prediction"]["team_code"] == "LAL"


def test_train_phase_three_models_rejects_unsupported_targets() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()

    with pytest.raises(ValueError):
        train_phase_three_models_in_memory(
            repository,
            target_task="cover_classification",
        )
