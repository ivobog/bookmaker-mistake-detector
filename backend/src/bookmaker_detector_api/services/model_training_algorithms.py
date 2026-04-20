from __future__ import annotations

from statistics import mean, median
from typing import Any

MAX_TREE_STUMP_THRESHOLDS = 25


def train_linear_feature_model(
    *,
    train_rows: list[dict[str, Any]],
    validation_rows: list[dict[str, Any]],
    test_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    feature_candidates = numeric_feature_candidates(train_rows)
    selection_rows = validation_rows or train_rows
    fallback_prediction = constant_target_mean(train_rows)
    best_model = {
        "feature_name": None,
        "intercept": 0.0,
        "coefficient": 0.0,
        "selection_metrics": {"mae": None, "prediction_count": 0},
    }
    candidate_count = 0
    for feature_name in feature_candidates:
        pairs = training_pairs(train_rows, feature_name)
        if len(pairs) < 2:
            continue
        candidate_count += 1
        slope, intercept = fit_simple_linear_regression(pairs)
        selection_metrics = score_regression_model(
            selection_rows,
            predictor=lambda row, fn=feature_name, b0=intercept, b1=slope: predict_linear(
                row,
                fn,
                b0,
                b1,
            ),
        )
        if is_better_regression_candidate(selection_metrics, best_model["selection_metrics"]):
            best_model = {
                "feature_name": feature_name,
                "intercept": intercept,
                "coefficient": slope,
                "selection_metrics": selection_metrics,
            }
    feature_name = best_model["feature_name"]
    artifact = {
        "model_family": "linear_feature",
        "selected_feature": feature_name,
        "intercept": best_model["intercept"],
        "coefficient": best_model["coefficient"],
        "constant_prediction": fallback_prediction,
        "feature_candidate_count": candidate_count,
        "selection_split": "validation" if validation_rows else "train_fallback",
        "selection_metrics": best_model["selection_metrics"],
        "fallback_strategy": None,
        "fallback_reason": None,
    }
    if feature_name is None:
        artifact["fallback_strategy"] = "constant_mean"
        artifact["fallback_reason"] = "no_usable_feature"

        def predictor(_row: dict[str, Any]) -> float | None:
            return fallback_prediction
    else:

        def predictor(row: dict[str, Any]) -> float | None:
            return predict_linear(
                row,
                feature_name,
                best_model["intercept"],
                best_model["coefficient"],
            )

    metrics = {
        "train": score_regression_model(train_rows, predictor=predictor),
        "validation": score_regression_model(validation_rows, predictor=predictor),
        "test": score_regression_model(test_rows, predictor=predictor),
    }
    return {"artifact": artifact, "metrics": metrics}


def train_tree_stump_model(
    *,
    train_rows: list[dict[str, Any]],
    validation_rows: list[dict[str, Any]],
    test_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    feature_candidates = numeric_feature_candidates(train_rows)
    selection_rows = validation_rows or train_rows
    fallback_prediction = constant_target_mean(train_rows)
    best_model = {
        "feature_name": None,
        "threshold": None,
        "left_prediction": None,
        "right_prediction": None,
        "selection_metrics": {"mae": None, "prediction_count": 0},
    }
    candidate_count = 0
    for feature_name in feature_candidates:
        pairs = training_pairs(train_rows, feature_name)
        if len(pairs) < 2:
            continue
        for threshold in candidate_tree_thresholds(pairs):
            left_targets = [target for value, target in pairs if value <= threshold]
            right_targets = [target for value, target in pairs if value > threshold]
            if not left_targets or not right_targets:
                continue
            candidate_count += 1
            left_prediction = round(float(mean(left_targets)), 4)
            right_prediction = round(float(mean(right_targets)), 4)

            def stump_predictor(
                row: dict[str, Any],
                *,
                fn: str = feature_name,
                split: float = threshold,
                left: float = left_prediction,
                right: float = right_prediction,
            ) -> float | None:
                return predict_tree_stump(row, fn, split, left, right)

            selection_metrics = score_regression_model(
                selection_rows,
                predictor=stump_predictor,
            )
            if is_better_regression_candidate(selection_metrics, best_model["selection_metrics"]):
                best_model = {
                    "feature_name": feature_name,
                    "threshold": threshold,
                    "left_prediction": left_prediction,
                    "right_prediction": right_prediction,
                    "selection_metrics": selection_metrics,
                }
    feature_name = best_model["feature_name"]
    artifact = {
        "model_family": "tree_stump",
        "selected_feature": feature_name,
        "threshold": best_model["threshold"],
        "left_prediction": best_model["left_prediction"],
        "right_prediction": best_model["right_prediction"],
        "constant_prediction": fallback_prediction,
        "feature_candidate_count": candidate_count,
        "selection_split": "validation" if validation_rows else "train_fallback",
        "selection_metrics": best_model["selection_metrics"],
        "fallback_strategy": None,
        "fallback_reason": None,
    }
    if feature_name is None:
        artifact["fallback_strategy"] = "constant_mean"
        artifact["fallback_reason"] = "no_valid_split"

        def predictor(_row: dict[str, Any]) -> float | None:
            return fallback_prediction
    else:

        def predictor(row: dict[str, Any]) -> float | None:
            return predict_tree_stump(
                row,
                feature_name,
                best_model["threshold"],
                best_model["left_prediction"],
                best_model["right_prediction"],
            )

    metrics = {
        "train": score_regression_model(train_rows, predictor=predictor),
        "validation": score_regression_model(validation_rows, predictor=predictor),
        "test": score_regression_model(test_rows, predictor=predictor),
    }
    return {"artifact": artifact, "metrics": metrics}


def numeric_feature_candidates(training_rows: list[dict[str, Any]]) -> list[str]:
    return sorted(
        {
            feature_name
            for row in training_rows
            for feature_name, value in row["features"].items()
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        }
    )


def training_pairs(
    training_rows: list[dict[str, Any]],
    feature_name: str,
) -> list[tuple[float, float]]:
    pairs = []
    for row in training_rows:
        feature_value = row["features"].get(feature_name)
        target_value = row["target_value"]
        if feature_value is None or target_value is None:
            continue
        pairs.append((float(feature_value), float(target_value)))
    return pairs


def fit_simple_linear_regression(
    pairs: list[tuple[float, float]],
) -> tuple[float, float]:
    x_mean = mean(value for value, _ in pairs)
    y_mean = mean(target for _, target in pairs)
    numerator = sum((value - x_mean) * (target - y_mean) for value, target in pairs)
    denominator = sum((value - x_mean) ** 2 for value, _ in pairs)
    if denominator == 0:
        return 0.0, round(float(y_mean), 4)
    slope = numerator / denominator
    intercept = y_mean - (slope * x_mean)
    return round(float(slope), 4), round(float(intercept), 4)


def candidate_tree_thresholds(
    pairs: list[tuple[float, float]],
) -> list[float]:
    unique_values = sorted({value for value, _ in pairs})
    if len(unique_values) < 2:
        return []
    thresholds = [
        round(float((left + right) / 2), 4) for left, right in zip(unique_values, unique_values[1:])
    ]
    if len(thresholds) > MAX_TREE_STUMP_THRESHOLDS:
        step = (len(thresholds) - 1) / (MAX_TREE_STUMP_THRESHOLDS - 1)
        thresholds = [
            thresholds[min(round(index * step), len(thresholds) - 1)]
            for index in range(MAX_TREE_STUMP_THRESHOLDS)
        ]
    median_threshold = round(float(median(unique_values)), 4)
    if median_threshold not in thresholds:
        thresholds.append(median_threshold)
    return sorted(set(thresholds))


def constant_target_mean(training_rows: list[dict[str, Any]]) -> float | None:
    target_values = [
        float(row["target_value"]) for row in training_rows if row.get("target_value") is not None
    ]
    if not target_values:
        return None
    return round(float(mean(target_values)), 4)


def summarize_target_values(training_rows: list[dict[str, Any]]) -> dict[str, Any]:
    target_values = [
        float(row["target_value"]) for row in training_rows if row.get("target_value") is not None
    ]
    if not target_values:
        return {
            "row_count": 0,
            "target_mean": None,
            "target_min": None,
            "target_max": None,
        }
    return {
        "row_count": len(target_values),
        "target_mean": round(float(mean(target_values)), 4),
        "target_min": round(float(min(target_values)), 4),
        "target_max": round(float(max(target_values)), 4),
    }


def predict_linear(
    row: dict[str, Any],
    feature_name: str | None,
    intercept: float,
    coefficient: float,
) -> float | None:
    if feature_name is None:
        return None
    feature_value = get_row_feature_value(row, feature_name)
    if feature_value is None:
        return None
    return round(float(intercept + (coefficient * float(feature_value))), 4)


def predict_tree_stump(
    row: dict[str, Any],
    feature_name: str | None,
    threshold: float | None,
    left_prediction: float | None,
    right_prediction: float | None,
) -> float | None:
    if (
        feature_name is None
        or threshold is None
        or left_prediction is None
        or right_prediction is None
    ):
        return None
    feature_value = get_row_feature_value(row, feature_name)
    if feature_value is None:
        return None
    return left_prediction if float(feature_value) <= threshold else right_prediction


def get_row_feature_value(
    row: dict[str, Any],
    feature_name: str,
) -> Any:
    if "features" in row and isinstance(row["features"], dict):
        return row["features"].get(feature_name)
    return row.get(feature_name)


def score_regression_model(
    training_rows: list[dict[str, Any]],
    *,
    predictor,
) -> dict[str, Any]:
    scored = []
    for row in training_rows:
        target_value = row["target_value"]
        prediction = predictor(row)
        if target_value is None or prediction is None:
            continue
        error = float(prediction) - float(target_value)
        scored.append(
            {
                "prediction": round(float(prediction), 4),
                "absolute_error": round(abs(error), 4),
                "squared_error": round(error * error, 4),
            }
        )
    absolute_errors = [entry["absolute_error"] for entry in scored]
    squared_errors = [entry["squared_error"] for entry in scored]
    return {
        "prediction_count": len(scored),
        "coverage_rate": round(len(scored) / len(training_rows), 4) if training_rows else 0.0,
        "mae": round(float(mean(absolute_errors)), 4) if absolute_errors else None,
        "rmse": round(float(mean(squared_errors) ** 0.5), 4) if squared_errors else None,
    }


def is_better_regression_candidate(
    candidate_metrics: dict[str, Any],
    incumbent_metrics: dict[str, Any],
) -> bool:
    candidate_mae = candidate_metrics.get("mae")
    incumbent_mae = incumbent_metrics.get("mae")
    if candidate_mae is None:
        return False
    if incumbent_mae is None:
        return True
    if float(candidate_mae) != float(incumbent_mae):
        return float(candidate_mae) < float(incumbent_mae)
    return int(candidate_metrics.get("prediction_count", 0)) > int(
        incumbent_metrics.get("prediction_count", 0)
    )
