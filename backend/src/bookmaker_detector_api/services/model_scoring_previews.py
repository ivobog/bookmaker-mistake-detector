from __future__ import annotations

from statistics import mean
from typing import Any, Callable

from bookmaker_detector_api.services.features import (
    build_feature_evidence_bundle,
    resolve_feature_condition_values_for_row,
)
from bookmaker_detector_api.services import model_training_views
from bookmaker_detector_api.services.model_records import (
    ModelEvaluationSnapshotRecord,
    ModelSelectionSnapshotRecord,
)


def resolve_active_model_selection(
    *,
    selections: list[ModelSelectionSnapshotRecord],
) -> ModelSelectionSnapshotRecord | None:
    return selections[0] if selections else None


def resolve_evaluation_snapshot_by_id(
    *,
    snapshots: list[ModelEvaluationSnapshotRecord],
    snapshot_id: int | None,
) -> ModelEvaluationSnapshotRecord | None:
    if snapshot_id is None:
        return None
    return next((entry for entry in snapshots if entry.id == snapshot_id), None)


def build_model_scoring_preview(
    *,
    dataset_rows: list[dict[str, Any]],
    target_task: str,
    active_selection: ModelSelectionSnapshotRecord | None,
    active_snapshot: ModelEvaluationSnapshotRecord | None,
    canonical_game_id: int | None,
    limit: int,
    include_evidence: bool,
    evidence_dimensions: tuple[str, ...],
    comparable_limit: int,
    min_pattern_sample_size: int,
    train_ratio: float,
    validation_ratio: float,
    drop_null_targets: bool,
    predict_linear: Callable[..., float | None],
    predict_tree_stump: Callable[..., float | None],
    get_row_feature_value: Callable[[dict[str, Any], str], Any],
) -> dict[str, Any]:
    filtered_rows = [
        row
        for row in dataset_rows
        if canonical_game_id is None or int(row["canonical_game_id"]) == canonical_game_id
    ]
    scored_predictions = score_dataset_rows_with_active_selection(
        filtered_rows,
        target_task=target_task,
        active_snapshot=active_snapshot,
        full_dataset_rows=dataset_rows,
        include_evidence=include_evidence,
        evidence_dimensions=evidence_dimensions,
        comparable_limit=comparable_limit,
        min_pattern_sample_size=min_pattern_sample_size,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
        predict_linear=predict_linear,
        predict_tree_stump=predict_tree_stump,
        get_row_feature_value=get_row_feature_value,
    )
    ranked_predictions = sorted(
        scored_predictions,
        key=lambda entry: (
            -float(entry["signal_strength"]),
            entry["game_date"],
            entry["canonical_game_id"],
            entry["team_code"],
        ),
    )[:limit]
    return {
        "active_selection": model_training_views._serialize_model_selection_snapshot(
            active_selection
        ),
        "active_evaluation_snapshot": model_training_views._serialize_model_evaluation_snapshot(
            active_snapshot
        ),
        "row_count": len(filtered_rows),
        "scored_prediction_count": len(ranked_predictions),
        "prediction_summary": summarize_scored_predictions(ranked_predictions),
        "predictions": ranked_predictions,
    }


def build_model_future_game_preview(
    *,
    target_task: str,
    active_selection: ModelSelectionSnapshotRecord | None,
    active_snapshot: ModelEvaluationSnapshotRecord | None,
    historical_dataset_rows: list[dict[str, Any]],
    scenario_rows: list[dict[str, Any]],
    include_evidence: bool,
    evidence_dimensions: tuple[str, ...],
    comparable_limit: int,
    min_pattern_sample_size: int,
    train_ratio: float,
    validation_ratio: float,
    drop_null_targets: bool,
    predict_linear: Callable[..., float | None],
    predict_tree_stump: Callable[..., float | None],
    get_row_feature_value: Callable[[dict[str, Any], str], Any],
    evaluate_opportunity_status: Callable[..., str],
    nested_get: Callable[..., Any],
    opportunity_policy: dict[str, Any] | None,
) -> dict[str, Any]:
    scored_predictions = score_dataset_rows_with_active_selection(
        scenario_rows,
        target_task=target_task,
        active_snapshot=active_snapshot,
        full_dataset_rows=historical_dataset_rows,
        include_evidence=include_evidence,
        evidence_dimensions=evidence_dimensions,
        comparable_limit=comparable_limit,
        min_pattern_sample_size=min_pattern_sample_size,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
        predict_linear=predict_linear,
        predict_tree_stump=predict_tree_stump,
        get_row_feature_value=get_row_feature_value,
    )
    ranked_predictions = sorted(
        scored_predictions,
        key=lambda entry: (-float(entry["signal_strength"]), entry["team_code"]),
    )
    opportunity_preview = build_opportunity_preview_entries(
        predictions=ranked_predictions,
        evaluate_opportunity_status=evaluate_opportunity_status,
        nested_get=nested_get,
        opportunity_policy=opportunity_policy,
    )
    return {
        "active_selection": model_training_views._serialize_model_selection_snapshot(
            active_selection
        ),
        "active_evaluation_snapshot": model_training_views._serialize_model_evaluation_snapshot(
            active_snapshot
        ),
        "scenario": serialize_future_scenario(scenario_rows),
        "scored_prediction_count": len(ranked_predictions),
        "prediction_summary": summarize_scored_predictions(ranked_predictions),
        "predictions": ranked_predictions,
        "opportunity_preview": opportunity_preview,
    }


def score_dataset_rows_with_active_selection(
    dataset_rows: list[dict[str, Any]],
    *,
    target_task: str,
    active_snapshot: ModelEvaluationSnapshotRecord | None,
    full_dataset_rows: list[dict[str, Any]],
    include_evidence: bool,
    evidence_dimensions: tuple[str, ...],
    comparable_limit: int,
    min_pattern_sample_size: int,
    train_ratio: float,
    validation_ratio: float,
    drop_null_targets: bool,
    predict_linear: Callable[..., float | None],
    predict_tree_stump: Callable[..., float | None],
    get_row_feature_value: Callable[[dict[str, Any], str], Any],
) -> list[dict[str, Any]]:
    if active_snapshot is None:
        return []
    scored_predictions = []
    for row in dataset_rows:
        prediction_value = predict_row_from_snapshot(
            active_snapshot,
            row,
            predict_linear=predict_linear,
            predict_tree_stump=predict_tree_stump,
        )
        if prediction_value is None:
            continue
        evidence_payload = None
        if include_evidence:
            if row.get("is_future_scenario"):
                evidence_bundle = build_feature_evidence_bundle(
                    full_dataset_rows,
                    target_task=target_task,
                    dimensions=evidence_dimensions,
                    team_code=str(row["team_code"]),
                    condition_values=resolve_feature_condition_values_for_row(
                        row,
                        dimensions=evidence_dimensions,
                    ),
                    comparable_limit=comparable_limit,
                    min_pattern_sample_size=min_pattern_sample_size,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                    drop_null_targets=drop_null_targets,
                )
            else:
                evidence_bundle = build_feature_evidence_bundle(
                    full_dataset_rows,
                    target_task=target_task,
                    dimensions=evidence_dimensions,
                    canonical_game_id=int(row["canonical_game_id"]),
                    team_code=str(row["team_code"]),
                    comparable_limit=comparable_limit,
                    min_pattern_sample_size=min_pattern_sample_size,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                    drop_null_targets=drop_null_targets,
                )
            evidence_payload = evidence_bundle.get("evidence")
        scored_predictions.append(
            serialize_scored_prediction(
                row,
                target_task=target_task,
                prediction_value=prediction_value,
                active_snapshot=active_snapshot,
                evidence_payload=evidence_payload,
                get_row_feature_value=get_row_feature_value,
            )
        )
    return scored_predictions


def serialize_scored_prediction(
    row: dict[str, Any],
    *,
    target_task: str,
    prediction_value: float,
    active_snapshot: ModelEvaluationSnapshotRecord,
    evidence_payload: dict[str, Any] | None,
    get_row_feature_value: Callable[[dict[str, Any], str], Any],
) -> dict[str, Any]:
    actual_target_value = model_training_views._float_or_none(row.get("target_value"))
    realized_residual = None
    if actual_target_value is not None:
        realized_residual = round(float(prediction_value) - float(actual_target_value), 4)
    selected_feature = active_snapshot.selected_feature
    selected_feature_value = (
        get_row_feature_value(row, selected_feature) if selected_feature is not None else None
    )
    return {
        "canonical_game_id": int(row["canonical_game_id"]),
        "season_label": row["season_label"],
        "game_date": row["game_date"],
        "team_code": row["team_code"],
        "opponent_code": row["opponent_code"],
        "venue": row["venue"],
        "prediction_value": round(float(prediction_value), 4),
        "signal_strength": round(abs(float(prediction_value)), 4),
        "prediction_context": build_prediction_context(
            target_task=target_task,
            prediction_value=prediction_value,
        ),
        "actual_target_value": actual_target_value,
        "realized_residual": realized_residual,
        "selected_feature_value": model_training_views._float_or_none(selected_feature_value),
        "feature_context": {
            "days_rest": model_training_views._float_or_none(
                get_row_feature_value(row, "days_rest")
            ),
            "games_played_prior": model_training_views._float_or_none(
                get_row_feature_value(row, "games_played_prior")
            ),
            "prior_matchup_count": model_training_views._float_or_none(
                get_row_feature_value(row, "prior_matchup_count")
            ),
        },
        "market_context": {
            "team_spread_line": model_training_views._float_or_none(row.get("team_spread_line")),
            "opponent_spread_line": model_training_views._float_or_none(
                row.get("opponent_spread_line")
            ),
            "total_line": model_training_views._float_or_none(row.get("total_line")),
        },
        "model": {
            "target_task": target_task,
            "model_family": active_snapshot.model_family,
            "selected_feature": selected_feature,
            "fallback_strategy": active_snapshot.fallback_strategy,
            "primary_metric_name": active_snapshot.primary_metric_name,
            "validation_metric_value": active_snapshot.validation_metric_value,
            "test_metric_value": active_snapshot.test_metric_value,
        },
        "evidence": (
            {
                "summary": evidence_payload.get("summary"),
                "strength": evidence_payload.get("strength"),
                "recommendation": evidence_payload.get("recommendation"),
            }
            if evidence_payload is not None
            else None
        ),
    }


def build_prediction_context(
    *,
    target_task: str,
    prediction_value: float,
) -> dict[str, Any]:
    if target_task == "spread_error_regression":
        signal_direction = "team_cover_edge" if prediction_value > 0 else "opponent_cover_edge"
        if prediction_value == 0:
            signal_direction = "neutral"
        return {
            "target_type": "spread_error",
            "signal_direction": signal_direction,
            "market_edge_points": round(float(prediction_value), 4),
        }
    if target_task == "total_error_regression":
        signal_direction = "over_edge" if prediction_value > 0 else "under_edge"
        if prediction_value == 0:
            signal_direction = "neutral"
        return {
            "target_type": "total_error",
            "signal_direction": signal_direction,
            "market_edge_points": round(float(prediction_value), 4),
        }
    if target_task == "point_margin_regression":
        signal_direction = (
            "team_margin_advantage" if prediction_value > 0 else "opponent_margin_advantage"
        )
        if prediction_value == 0:
            signal_direction = "neutral"
        return {
            "target_type": "point_margin",
            "signal_direction": signal_direction,
            "predicted_margin": round(float(prediction_value), 4),
        }
    signal_direction = "higher_total" if prediction_value > 0 else "lower_total"
    if prediction_value == 0:
        signal_direction = "neutral"
    return {
        "target_type": "total_points",
        "signal_direction": signal_direction,
        "predicted_total_points": round(float(prediction_value), 4),
    }


def predict_row_from_snapshot(
    snapshot: ModelEvaluationSnapshotRecord,
    row: dict[str, Any],
    *,
    predict_linear: Callable[..., float | None],
    predict_tree_stump: Callable[..., float | None],
) -> float | None:
    artifact = snapshot.snapshot.get("artifact", {})
    model_family = artifact.get("model_family")
    if artifact.get("fallback_strategy") == "constant_mean":
        return model_training_views._float_or_none(artifact.get("constant_prediction"))
    if model_family == "linear_feature":
        return predict_linear(
            row,
            artifact.get("selected_feature"),
            float(artifact.get("intercept", 0.0)),
            float(artifact.get("coefficient", 0.0)),
        )
    if model_family == "tree_stump":
        return predict_tree_stump(
            row,
            artifact.get("selected_feature"),
            model_training_views._float_or_none(artifact.get("threshold")),
            model_training_views._float_or_none(artifact.get("left_prediction")),
            model_training_views._float_or_none(artifact.get("right_prediction")),
        )
    return None


def summarize_scored_predictions(predictions: list[dict[str, Any]]) -> dict[str, Any]:
    prediction_values = [float(entry["prediction_value"]) for entry in predictions]
    signal_strengths = [float(entry["signal_strength"]) for entry in predictions]
    positive_count = len([value for value in prediction_values if value > 0])
    negative_count = len([value for value in prediction_values if value < 0])
    return {
        "prediction_count": len(predictions),
        "positive_prediction_count": positive_count,
        "negative_prediction_count": negative_count,
        "average_prediction_value": (
            round(float(mean(prediction_values)), 4) if prediction_values else None
        ),
        "average_signal_strength": (
            round(float(mean(signal_strengths)), 4) if signal_strengths else None
        ),
        "top_prediction": predictions[0] if predictions else None,
    }


def build_opportunity_preview_entries(
    *,
    predictions: list[dict[str, Any]],
    evaluate_opportunity_status: Callable[..., str],
    nested_get: Callable[..., Any],
    opportunity_policy: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if opportunity_policy is None:
        return []
    preview_entries = []
    for prediction in predictions:
        status = evaluate_opportunity_status(
            prediction=prediction,
            policy=opportunity_policy,
        )
        preview_entries.append(
            {
                "team_code": prediction["team_code"],
                "opponent_code": prediction["opponent_code"],
                "game_date": prediction["game_date"],
                "status": status,
                "policy_name": opportunity_policy["policy_name"],
                "signal_strength": prediction["signal_strength"],
                "prediction_value": prediction["prediction_value"],
                "recommendation_status": nested_get(
                    prediction,
                    "evidence",
                    "recommendation",
                    "status",
                ),
                "evidence_rating": nested_get(
                    prediction,
                    "evidence",
                    "strength",
                    "rating",
                ),
            }
        )
    return preview_entries


def serialize_future_scenario(
    scenario_rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not scenario_rows:
        return None
    home_row = next((row for row in scenario_rows if row["venue"] == "home"), None)
    away_row = next((row for row in scenario_rows if row["venue"] == "away"), None)
    representative = home_row or away_row
    if representative is None:
        return None
    return {
        "scenario_key": representative.get("scenario_key"),
        "season_label": representative["season_label"],
        "game_date": representative["game_date"],
        "home_team_code": home_row["team_code"] if home_row is not None else None,
        "away_team_code": away_row["team_code"] if away_row is not None else None,
        "home_spread_line": model_training_views._float_or_none(
            home_row.get("team_spread_line") if home_row is not None else None
        ),
        "away_spread_line": model_training_views._float_or_none(
            away_row.get("team_spread_line") if away_row is not None else None
        ),
        "total_line": model_training_views._float_or_none(representative.get("total_line")),
    }
