from __future__ import annotations

from bookmaker_detector_api.services.model_records import (
    ModelFamilyCapabilityRecord,
    TargetTaskDefinitionRecord,
)
from bookmaker_detector_api.services.task_registry import (
    build_model_capabilities_payload,
    is_workflow_supported,
)


def test_build_model_capabilities_payload_keeps_phase_a_backtest_support() -> None:
    payload = build_model_capabilities_payload(
        task_definitions=[
            TargetTaskDefinitionRecord(
                task_key="point_margin_regression",
                task_kind="regression",
                label="Point Margin Regression",
                description="Predicts team margin against the market spread.",
                market_type="spread",
                primary_metric_name="mae",
                metric_direction="lower_is_better",
                opportunity_policy_name="margin_signal_policy_v1",
                is_enabled=True,
                config={
                    "default_selection_policy_name": "validation_regression_candidate_v1",
                    "selection_policy_names": [
                        "validation_regression_candidate_v1",
                        "validation_mae_candidate_v1",
                    ],
                    "scoring_output_semantics": "market_edge_regression",
                    "signal_strength_interpretation": "predicted_margin_vs_market_line",
                    "workflow_support": {
                        "training": True,
                        "selection": True,
                        "scoring": True,
                        "opportunity_materialization": True,
                        "market_board": True,
                        "backtesting": True,
                    },
                },
            ),
            TargetTaskDefinitionRecord(
                task_key="total_points_regression",
                task_kind="regression",
                label="Total Points Regression",
                description="Predicts total points against the market total.",
                market_type="total",
                primary_metric_name="mae",
                metric_direction="lower_is_better",
                opportunity_policy_name="totals_signal_policy_v1",
                is_enabled=True,
                config={
                    "default_selection_policy_name": "validation_regression_candidate_v1",
                    "selection_policy_names": [
                        "validation_regression_candidate_v1",
                        "validation_mae_candidate_v1",
                    ],
                    "scoring_output_semantics": "market_edge_regression",
                    "signal_strength_interpretation": "predicted_total_points_vs_market_line",
                    "workflow_support": {
                        "training": True,
                        "selection": True,
                        "scoring": True,
                        "opportunity_materialization": True,
                        "market_board": True,
                        "backtesting": True,
                    },
                },
            ),
        ],
        model_family_capabilities=[
            ModelFamilyCapabilityRecord(
                id=1,
                model_family="linear_feature",
                target_task="point_margin_regression",
                is_enabled=True,
                config={},
            ),
            ModelFamilyCapabilityRecord(
                id=2,
                model_family="tree_stump",
                target_task="point_margin_regression",
                is_enabled=True,
                config={},
            ),
            ModelFamilyCapabilityRecord(
                id=3,
                model_family="linear_feature",
                target_task="total_points_regression",
                is_enabled=True,
                config={},
            ),
        ],
    )

    assert payload["task_count"] == 2
    assert is_workflow_supported(
        payload,
        target_task="point_margin_regression",
        workflow_name="backtesting",
    )
    assert is_workflow_supported(
        payload,
        target_task="total_points_regression",
        workflow_name="backtesting",
    )
    point_margin_task = next(
        task for task in payload["target_tasks"] if task["task_key"] == "point_margin_regression"
    )
    total_points_task = next(
        task for task in payload["target_tasks"] if task["task_key"] == "total_points_regression"
    )
    assert point_margin_task["default_opportunity_policy_name"] == "margin_signal_policy_v1"
    assert total_points_task["default_opportunity_policy_name"] == "totals_signal_policy_v1"
    assert point_margin_task["supported_model_families"] == ["linear_feature", "tree_stump"]
    assert total_points_task["supported_model_families"] == ["linear_feature"]
