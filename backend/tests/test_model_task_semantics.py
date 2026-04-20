from __future__ import annotations

import pytest

from bookmaker_detector_api.services import model_backtest_workflows, model_scoring_previews
from bookmaker_detector_api.services.feature_records import FeatureVersionRecord
from bookmaker_detector_api.services.model_records import ModelEvaluationSnapshotRecord


@pytest.mark.parametrize(
    ("target_task", "prediction_value", "row", "expected_signal_strength", "expected_direction", "expected_market_edge"),
    [
        (
            "point_margin_regression",
            5.0,
            {"team_spread_line": -3.5, "total_line": 221.0},
            1.5,
            "team_cover_edge",
            1.5,
        ),
        (
            "total_points_regression",
            218.0,
            {"team_spread_line": -3.5, "total_line": 221.0},
            3.0,
            "under_edge",
            -3.0,
        ),
    ],
)
def test_serialize_scored_prediction_uses_market_edge_signal_strength_for_raw_targets(
    target_task: str,
    prediction_value: float,
    row: dict[str, float],
    expected_signal_strength: float,
    expected_direction: str,
    expected_market_edge: float,
) -> None:
    snapshot = ModelEvaluationSnapshotRecord(
        id=1,
        model_training_run_id=2,
        model_registry_id=3,
        feature_version_id=4,
        target_task=target_task,
        model_family="linear_feature",
        selected_feature="days_rest",
        fallback_strategy=None,
        primary_metric_name="mae",
        validation_metric_value=1.2,
        test_metric_value=1.4,
        validation_prediction_count=10,
        test_prediction_count=5,
        snapshot={"artifact": {"model_family": "linear_feature", "selected_feature": "days_rest"}},
    )
    prediction = model_scoring_previews.serialize_scored_prediction(
        {
            "canonical_game_id": 12,
            "season_label": "2024-2025",
            "game_date": "2024-11-05",
            "team_code": "LAL",
            "opponent_code": "BOS",
            "venue": "home",
            "target_value": 6.0,
            "days_rest": 2,
            "games_played_prior": 10,
            "prior_matchup_count": 1,
            **row,
        },
        target_task=target_task,
        prediction_value=prediction_value,
        active_snapshot=snapshot,
        evidence_payload=None,
        get_row_feature_value=lambda payload, key: payload.get(key),
    )

    assert prediction["signal_strength"] == pytest.approx(expected_signal_strength)
    assert prediction["prediction_context"]["signal_direction"] == expected_direction
    assert prediction["prediction_context"]["market_edge_points"] == pytest.approx(expected_market_edge)


def test_build_backtest_bet_supports_phase_a_raw_regression_targets() -> None:
    margin_bet = model_backtest_workflows.build_backtest_bet(
        prediction={
            "canonical_game_id": 12,
            "game_date": "2024-11-05",
            "team_code": "LAL",
            "opponent_code": "BOS",
            "prediction_value": 5.0,
            "actual_target_value": 6.0,
            "market_context": {"team_spread_line": -3.5, "total_line": 221.0},
        },
        target_task="point_margin_regression",
        strategy_name="candidate_threshold",
        threshold=1.0,
        float_or_none=model_backtest_workflows._float_or_none,
    )
    total_bet = model_backtest_workflows.build_backtest_bet(
        prediction={
            "canonical_game_id": 12,
            "game_date": "2024-11-05",
            "team_code": "LAL",
            "opponent_code": "BOS",
            "prediction_value": 218.0,
            "actual_target_value": 217.0,
            "market_context": {"team_spread_line": -3.5, "total_line": 221.0},
        },
        target_task="total_points_regression",
        strategy_name="candidate_threshold",
        threshold=1.0,
        float_or_none=model_backtest_workflows._float_or_none,
    )

    assert margin_bet is not None
    assert margin_bet["edge_direction"] == "team_cover_edge"
    assert margin_bet["signal_strength"] == pytest.approx(1.5)
    assert margin_bet["market_edge_points"] == pytest.approx(1.5)
    assert margin_bet["result"] == "win"

    assert total_bet is not None
    assert total_bet["edge_direction"] == "under_edge"
    assert total_bet["signal_strength"] == pytest.approx(3.0)
    assert total_bet["market_edge_points"] == pytest.approx(-3.0)
    assert total_bet["result"] == "win"


def test_run_walk_forward_backtest_accepts_all_phase_a_regression_targets() -> None:
    feature_version = FeatureVersionRecord(
        id=1,
        feature_key="baseline_team_features_v1",
        version_label="Baseline Team Features v1",
        description="baseline",
        config={},
    )

    result = model_backtest_workflows.run_walk_forward_backtest(
        dataset_rows=[],
        feature_version=feature_version,
        target_task="point_margin_regression",
        team_code=None,
        season_label=None,
        selection_policy_name="validation_regression_candidate_v1",
        minimum_train_games=1,
        test_window_games=1,
        train_ratio=0.7,
        validation_ratio=0.15,
        opportunity_policy_configs={
            "point_margin_regression": {
                "candidate_min_signal_strength": 4.0,
                "review_min_signal_strength": 2.5,
            }
        },
        partition_feature_dataset_rows=lambda *args, **kwargs: {},
        build_feature_training_view=lambda *args, **kwargs: {"training_rows": []},
        train_linear_feature_model=lambda *args, **kwargs: {},
        train_tree_stump_model=lambda *args, **kwargs: {},
        select_best_evaluation_snapshot=lambda *args, **kwargs: None,
        score_dataset_rows_with_active_selection=lambda *args, **kwargs: [],
    )

    assert result["record"].target_task == "point_margin_regression"
    assert result["summary"]["strategy_name"] == "point_margin_regression_walk_forward_v1"
