from __future__ import annotations

from datetime import datetime, timezone

from bookmaker_detector_api.services import model_backtest_runs, model_backtest_workflows
from bookmaker_detector_api.services.feature_records import FeatureVersionRecord
from bookmaker_detector_api.services.model_records import ModelBacktestRunRecord
from bookmaker_detector_api.services.task_registry import (
    DEFAULT_REGRESSION_SELECTION_POLICY_NAME,
    normalize_selection_policy_name,
)


def test_normalize_selection_policy_name_canonicalizes_legacy_alias() -> None:
    assert normalize_selection_policy_name("validation_mae_candidate_v1") == (
        DEFAULT_REGRESSION_SELECTION_POLICY_NAME
    )


def test_run_walk_forward_backtest_normalizes_legacy_policy_name_in_summary() -> None:
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
        target_task="spread_error_regression",
        team_code=None,
        season_label=None,
        selection_policy_name="validation_mae_candidate_v1",
        minimum_train_games=1,
        test_window_games=1,
        train_ratio=0.7,
        validation_ratio=0.15,
        opportunity_policy_configs={
            "spread_error_regression": {
                "candidate_min_signal_strength": 2.0,
                "review_min_signal_strength": 1.0,
            }
        },
        partition_feature_dataset_rows=lambda *args, **kwargs: {},
        build_feature_training_view=lambda *args, **kwargs: {"training_rows": []},
        train_linear_feature_model=lambda *args, **kwargs: {},
        train_tree_stump_model=lambda *args, **kwargs: {},
        select_best_evaluation_snapshot=lambda *args, **kwargs: None,
        score_dataset_rows_with_active_selection=lambda *args, **kwargs: [],
    )

    assert result["record"].selection_policy_name == DEFAULT_REGRESSION_SELECTION_POLICY_NAME
    assert result["summary"]["selection_policy_name"] == DEFAULT_REGRESSION_SELECTION_POLICY_NAME


def test_serialize_model_backtest_run_canonicalizes_legacy_policy_name() -> None:
    payload = model_backtest_runs.serialize_model_backtest_run(
        ModelBacktestRunRecord(
            id=1,
            feature_version_id=2,
            target_task="spread_error_regression",
            team_code=None,
            season_label=None,
            status="COMPLETED",
            selection_policy_name="validation_mae_candidate_v1",
            strategy_name="spread_error_regression_walk_forward_v1",
            minimum_train_games=1,
            test_window_games=1,
            train_ratio=0.7,
            validation_ratio=0.15,
            fold_count=0,
            payload={},
            created_at=datetime.now(timezone.utc),
            completed_at=datetime.now(timezone.utc),
        )
    )

    assert payload is not None
    assert payload["selection_policy_name"] == DEFAULT_REGRESSION_SELECTION_POLICY_NAME
