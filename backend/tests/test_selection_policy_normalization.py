from __future__ import annotations

from bookmaker_detector_api.repositories.ingestion_in_memory_repository import (
    InMemoryIngestionRepository,
)
from bookmaker_detector_api.services import (
    model_backtest_runs,
    model_backtest_workflows,
    model_training_views,
)
from bookmaker_detector_api.services.feature_records import FeatureVersionRecord
from bookmaker_detector_api.services.model_records import (
    ModelBacktestRunRecord,
    ModelEvaluationSnapshotRecord,
)
from bookmaker_detector_api.services.model_training_lifecycle import (
    save_model_selection_snapshot_in_memory,
)


def test_save_model_selection_snapshot_in_memory_normalizes_legacy_policy_name() -> None:
    repository = InMemoryIngestionRepository()
    snapshot = ModelEvaluationSnapshotRecord(
        id=1,
        model_training_run_id=2,
        model_registry_id=3,
        feature_version_id=4,
        target_task="spread_error_regression",
        model_family="linear_feature",
        selected_feature="days_rest",
        fallback_strategy=None,
        primary_metric_name="mae",
        validation_metric_value=1.2,
        test_metric_value=1.4,
        validation_prediction_count=10,
        test_prediction_count=5,
        selection_score=1.2,
        selection_score_name="validation_regression_candidate_v1",
        snapshot={},
    )

    selection = save_model_selection_snapshot_in_memory(
        repository,
        snapshot,
        selection_policy_name="validation_mae_candidate_v1",
    )

    assert selection.selection_policy_name == "validation_regression_candidate_v1"
    assert repository.model_selection_snapshots[0]["selection_policy_name"] == (
        "validation_regression_candidate_v1"
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

    assert result["record"].selection_policy_name == "validation_regression_candidate_v1"
    assert result["summary"]["selection_policy_name"] == "validation_regression_candidate_v1"


def test_save_model_backtest_run_in_memory_normalizes_legacy_policy_name() -> None:
    repository = InMemoryIngestionRepository()
    saved_run = model_backtest_runs.save_model_backtest_run_in_memory(
        repository,
        ModelBacktestRunRecord(
            id=0,
            feature_version_id=1,
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
            payload={"selection_policy_name": "validation_mae_candidate_v1"},
        ),
    )

    assert saved_run.selection_policy_name == "validation_regression_candidate_v1"
    assert repository.model_backtest_runs[0]["selection_policy_name"] == (
        "validation_regression_candidate_v1"
    )


def test_readers_canonicalize_legacy_selection_policy_aliases_from_existing_rows() -> None:
    repository = InMemoryIngestionRepository()
    repository.model_selection_snapshots.append(
        {
            "id": 1,
            "model_evaluation_snapshot_id": 2,
            "model_training_run_id": 3,
            "model_registry_id": 4,
            "feature_version_id": 5,
            "target_task": "spread_error_regression",
            "model_family": "linear_feature",
            "selection_policy_name": "validation_mae_candidate_v1",
            "rationale": {},
            "is_active": True,
            "created_at": None,
        }
    )
    repository.model_backtest_runs.append(
        {
            "id": 1,
            "feature_version_id": 1,
            "target_task": "spread_error_regression",
            "team_code": None,
            "season_label": None,
            "status": "COMPLETED",
            "selection_policy_name": "validation_mae_candidate_v1",
            "strategy_name": "spread_error_regression_walk_forward_v1",
            "minimum_train_games": 1,
            "test_window_games": 1,
            "train_ratio": 0.7,
            "validation_ratio": 0.15,
            "fold_count": 0,
            "payload": {},
            "created_at": None,
            "completed_at": None,
        }
    )

    selections = model_training_views.list_model_selection_snapshots_in_memory(repository)
    backtests = model_backtest_runs.list_model_backtest_runs_in_memory(repository)

    assert selections[0].selection_policy_name == "validation_regression_candidate_v1"
    assert backtests[0].selection_policy_name == "validation_regression_candidate_v1"
