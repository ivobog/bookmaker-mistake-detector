from __future__ import annotations

import pytest

from bookmaker_detector_api.services.feature_evidence_scoring import (
    build_evidence_recommendation,
)
from bookmaker_detector_api.services.features import FEATURE_EVIDENCE_RECOMMENDATION_POLICIES


def test_build_evidence_recommendation_uses_task_specific_policy_profile() -> None:
    recommendation = build_evidence_recommendation(
        target_task="point_margin_regression",
        task_type="regression",
        evidence_strength={
            "overall_score": 0.8,
            "warnings": [],
            "components": {
                "benchmark_support": {"stability_score": 0.8},
            },
        },
        selected_pattern={"sample_size": 6},
        comparables=[{"similarity_score": 0.9}, {"similarity_score": 0.85}, {"similarity_score": 0.8}],
        benchmark_rankings=[
            {
                "baseline_name": "rolling_3_feature_baseline",
            }
        ],
        evidence_recommendation_policies=FEATURE_EVIDENCE_RECOMMENDATION_POLICIES,
    )

    assert recommendation["status"] == "candidate_signal"
    assert recommendation["policy_profile"]["target_task"] == "point_margin_regression"
    assert recommendation["policy_profile"]["policy_name"] == "regression_margin_policy_v1"
    assert (
        recommendation["policy_profile"]["thresholds"]["candidate_min_overall_score"] == 0.72
    )


def test_build_evidence_recommendation_rejects_unsupported_target_task() -> None:
    with pytest.raises(ValueError, match="Unsupported evidence recommendation target_task"):
        build_evidence_recommendation(
            target_task="unknown_task",
            task_type="regression",
            evidence_strength={
                "overall_score": 0.5,
                "warnings": [],
                "components": {
                    "benchmark_support": {"stability_score": 0.8},
                },
            },
            selected_pattern=None,
            comparables=[],
            benchmark_rankings=[],
            evidence_recommendation_policies=FEATURE_EVIDENCE_RECOMMENDATION_POLICIES,
        )
