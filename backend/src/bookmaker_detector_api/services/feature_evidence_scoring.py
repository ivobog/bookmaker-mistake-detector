from __future__ import annotations

from typing import Any


def build_evidence_strength_summary(
    *,
    task_type: str,
    selected_pattern: dict[str, Any] | None,
    comparables: list[dict[str, Any]],
    benchmark_rankings: list[dict[str, Any]],
) -> dict[str, Any]:
    pattern_component = _build_pattern_strength_component(
        task_type=task_type,
        selected_pattern=selected_pattern,
    )
    comparable_component = _build_comparable_strength_component(comparables)
    benchmark_component = _build_benchmark_strength_component(
        benchmark_rankings=benchmark_rankings,
    )
    overall_score = round(
        (
            pattern_component["score"]
            + comparable_component["score"]
            + benchmark_component["score"]
        )
        / 3,
        4,
    )
    warnings = []
    if selected_pattern is None:
        warnings.append("pattern_not_found")
    elif int(selected_pattern.get("sample_size", 0)) < 3:
        warnings.append("low_pattern_sample")
    if len(comparables) == 0:
        warnings.append("no_comparables_found")
    elif len(comparables) < 3:
        warnings.append("thin_comparable_set")
    if not benchmark_rankings:
        warnings.append("benchmark_context_unavailable")
    elif benchmark_component["stability_score"] < 0.5:
        warnings.append("benchmark_instability")
    return {
        "overall_score": overall_score,
        "rating": _evidence_strength_rating(overall_score),
        "components": {
            "pattern_support": pattern_component,
            "comparable_support": comparable_component,
            "benchmark_support": benchmark_component,
        },
        "warnings": warnings,
    }


def build_evidence_recommendation(
    *,
    target_task: str,
    task_type: str,
    evidence_strength: dict[str, Any],
    selected_pattern: dict[str, Any] | None,
    comparables: list[dict[str, Any]],
    benchmark_rankings: list[dict[str, Any]],
    evidence_recommendation_policies: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    policy = evidence_recommendation_policies.get(
        target_task,
        evidence_recommendation_policies["spread_error_regression"],
    )
    overall_score = float(evidence_strength["overall_score"])
    warnings = list(evidence_strength["warnings"])
    pattern_sample_size = int(selected_pattern.get("sample_size", 0)) if selected_pattern else 0
    comparable_count = len(comparables)
    top_similarity = comparables[0]["similarity_score"] if comparables else None
    benchmark_support = evidence_strength["components"]["benchmark_support"]
    benchmark_stability_score = benchmark_support["stability_score"]
    benchmark_name = benchmark_rankings[0]["baseline_name"] if benchmark_rankings else None

    if (
        overall_score >= policy["candidate_min_overall_score"]
        and pattern_sample_size >= policy["candidate_min_pattern_sample"]
        and comparable_count >= policy["candidate_min_comparables"]
        and benchmark_stability_score >= policy["candidate_min_benchmark_stability"]
        and "benchmark_instability" not in warnings
    ):
        status = "candidate_signal"
        recommended_action = "promote_to_model_review"
        headline = "Signal looks strong enough for deeper model review."
    elif overall_score >= policy["review_min_overall_score"] or (
        pattern_sample_size >= policy["review_min_pattern_sample"]
        and comparable_count >= policy["review_min_comparables"]
    ):
        status = "review_manually"
        recommended_action = "review_manually"
        headline = "Signal has some support, but still needs analyst judgment."
    else:
        status = "monitor_only"
        recommended_action = "monitor_only"
        headline = "Evidence is still thin, so this should stay in monitoring."

    rationale = []
    if pattern_sample_size > 0:
        rationale.append(f"pattern sample size={pattern_sample_size}")
    if comparable_count > 0:
        rationale.append(f"comparables found={comparable_count}")
    if top_similarity is not None:
        rationale.append(f"top comparable similarity={top_similarity}")
    if benchmark_name is not None:
        rationale.append(
            f"best benchmark={benchmark_name} (stability={benchmark_stability_score})"
        )

    next_steps = {
        "candidate_signal": [
            "compare against stronger benchmark or first simple model",
            "review top comparables for face validity",
            "track whether the signal persists on future windows",
        ],
        "review_manually": [
            "inspect the ranked comparables",
            "check whether the pattern holds across nearby buckets",
            "wait for more history before escalating automatically",
        ],
        "monitor_only": [
            "collect more historical examples for this bucket",
            "watch for additional comparable cases",
            "recheck after the next ingestion run",
        ],
    }[status]

    return {
        "status": status,
        "recommended_action": recommended_action,
        "headline": headline,
        "task_type": task_type,
        "policy_profile": {
            "target_task": target_task,
            "policy_name": policy["policy_name"],
            "thresholds": {
                "candidate_min_overall_score": policy["candidate_min_overall_score"],
                "review_min_overall_score": policy["review_min_overall_score"],
                "candidate_min_pattern_sample": policy["candidate_min_pattern_sample"],
                "review_min_pattern_sample": policy["review_min_pattern_sample"],
                "candidate_min_comparables": policy["candidate_min_comparables"],
                "review_min_comparables": policy["review_min_comparables"],
                "candidate_min_benchmark_stability": policy[
                    "candidate_min_benchmark_stability"
                ],
            },
        },
        "rationale": rationale,
        "blocking_factors": warnings,
        "snapshot": {
            "overall_score": overall_score,
            "pattern_sample_size": pattern_sample_size,
            "comparable_count": comparable_count,
            "top_comparable_similarity_score": top_similarity,
            "best_benchmark_name": benchmark_name,
            "benchmark_stability_score": benchmark_stability_score,
        },
        "next_steps": next_steps,
    }


def _build_pattern_strength_component(
    *,
    task_type: str,
    selected_pattern: dict[str, Any] | None,
) -> dict[str, Any]:
    if selected_pattern is None:
        return {
            "score": 0.0,
            "sample_size": 0,
            "signal_strength": None,
            "signal_score": 0.0,
            "sample_score": 0.0,
        }

    sample_size = int(selected_pattern.get("sample_size", 0))
    signal_strength = selected_pattern.get("signal_strength")
    sample_score = _bounded_ratio(sample_size, 8)
    signal_score = _pattern_signal_score(
        task_type=task_type,
        signal_strength=signal_strength,
        target_stddev=selected_pattern.get("target_stddev"),
    )
    return {
        "score": round((sample_score * 0.6) + (signal_score * 0.4), 4),
        "sample_size": sample_size,
        "signal_strength": signal_strength,
        "sample_score": sample_score,
        "signal_score": signal_score,
    }


def _build_comparable_strength_component(
    comparables: list[dict[str, Any]],
) -> dict[str, Any]:
    comparable_count = len(comparables)
    similarity_scores = [
        float(entry["similarity_score"])
        for entry in comparables
        if entry.get("similarity_score") is not None
    ]
    top_similarity_score = similarity_scores[0] if similarity_scores else None
    average_similarity_score = _mean_or_none(similarity_scores)
    count_score = _bounded_ratio(comparable_count, 8)
    similarity_score = (
        round(((top_similarity_score or 0.0) + (average_similarity_score or 0.0)) / 2, 4)
        if similarity_scores
        else 0.0
    )
    return {
        "score": round((count_score * 0.5) + (similarity_score * 0.5), 4),
        "comparable_count": comparable_count,
        "top_similarity_score": top_similarity_score,
        "average_similarity_score": average_similarity_score,
        "count_score": count_score,
        "similarity_score": similarity_score,
    }


def _build_benchmark_strength_component(
    *,
    benchmark_rankings: list[dict[str, Any]],
) -> dict[str, Any]:
    if not benchmark_rankings:
        return {
            "score": 0.0,
            "baseline_name": None,
            "primary_metric": None,
            "validation_primary_metric": None,
            "test_primary_metric": None,
            "stability_gap": None,
            "stability_score": 0.0,
            "separation_score": 0.0,
            "coverage_score": 0.0,
        }

    best_benchmark = benchmark_rankings[0]
    second_benchmark = benchmark_rankings[1] if len(benchmark_rankings) > 1 else None
    validation_primary_metric = best_benchmark.get("validation_primary_metric")
    test_primary_metric = best_benchmark.get("test_primary_metric")
    stability_gap = _metric_gap(validation_primary_metric, test_primary_metric)
    stability_score = _metric_stability_score(
        validation_primary_metric,
        test_primary_metric,
    )
    separation_score = _benchmark_separation_score(best_benchmark, second_benchmark)
    coverage_score = _bounded_ratio(best_benchmark.get("test_prediction_count", 0), 8)
    return {
        "score": round(
            (stability_score * 0.4) + (separation_score * 0.35) + (coverage_score * 0.25),
            4,
        ),
        "baseline_name": best_benchmark.get("baseline_name"),
        "primary_metric": best_benchmark.get("primary_metric"),
        "validation_primary_metric": validation_primary_metric,
        "test_primary_metric": test_primary_metric,
        "stability_gap": stability_gap,
        "stability_score": stability_score,
        "separation_score": separation_score,
        "coverage_score": coverage_score,
    }


def _pattern_signal_score(
    *,
    task_type: str,
    signal_strength: Any,
    target_stddev: Any,
) -> float:
    if signal_strength is None:
        return 0.0
    if task_type == "classification":
        return _bounded_ratio(signal_strength, 0.25)
    if target_stddev not in (None, 0):
        return _bounded_ratio(abs(float(signal_strength)), float(target_stddev))
    return _bounded_ratio(abs(float(signal_strength)), 5.0)


def _metric_gap(first_value: Any, second_value: Any) -> float | None:
    if first_value is None or second_value is None:
        return None
    return round(abs(float(first_value) - float(second_value)), 4)


def _metric_stability_score(first_value: Any, second_value: Any) -> float:
    if first_value is None or second_value is None:
        return 0.0
    gap = abs(float(first_value) - float(second_value))
    denominator = max(abs(float(first_value)), abs(float(second_value)), 1.0)
    return round(max(0.0, 1.0 - min(gap / denominator, 1.0)), 4)


def _benchmark_separation_score(
    best_benchmark: dict[str, Any],
    second_benchmark: dict[str, Any] | None,
) -> float:
    if second_benchmark is None:
        return 0.5
    best_value = best_benchmark.get("test_primary_metric")
    second_value = second_benchmark.get("test_primary_metric")
    if best_value is None or second_value is None:
        return 0.0
    gap = float(second_value) - float(best_value)
    denominator = max(abs(float(second_value)), 1.0)
    return round(max(0.0, min(gap / denominator, 1.0)), 4)


def _evidence_strength_rating(score: float) -> str:
    if score >= 0.75:
        return "strong"
    if score >= 0.45:
        return "moderate"
    return "weak"


def _bounded_ratio(value: Any, upper_bound: float) -> float:
    numeric_value = float(value or 0.0)
    if upper_bound <= 0:
        return 0.0
    return round(max(0.0, min(numeric_value / upper_bound, 1.0)), 4)


def _mean_or_none(values) -> float | None:
    values = [float(value) for value in values]
    if not values:
        return None
    return round(sum(values) / len(values), 4)
