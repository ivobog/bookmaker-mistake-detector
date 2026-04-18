from __future__ import annotations

from statistics import mean
from typing import Any, Callable

from bookmaker_detector_api.services.features import FeatureVersionRecord
from bookmaker_detector_api.services.model_records import (
    ModelBacktestRunRecord,
    ModelEvaluationSnapshotRecord,
)


def run_walk_forward_backtest(
    *,
    dataset_rows: list[dict[str, Any]],
    feature_version: FeatureVersionRecord,
    target_task: str,
    team_code: str | None,
    season_label: str | None,
    selection_policy_name: str,
    minimum_train_games: int,
    test_window_games: int,
    train_ratio: float,
    validation_ratio: float,
    opportunity_policy_configs: dict[str, dict[str, Any]],
    partition_feature_dataset_rows: Callable[..., dict[str, list[dict[str, Any]]]],
    build_feature_training_view: Callable[..., dict[str, Any]],
    train_linear_feature_model: Callable[..., dict[str, Any]],
    train_tree_stump_model: Callable[..., dict[str, Any]],
    select_best_evaluation_snapshot: Callable[..., ModelEvaluationSnapshotRecord | None],
    score_dataset_rows_with_active_selection: Callable[..., list[dict[str, Any]]],
) -> dict[str, Any]:
    if target_task not in {"spread_error_regression", "total_error_regression"}:
        raise ValueError(
            "Phase 4 walk-forward backtesting currently supports spread and total regression "
            f"targets only: {target_task}"
        )
    ordered_game_ids = ordered_dataset_game_ids(dataset_rows)
    if len(ordered_game_ids) <= minimum_train_games:
        summary = empty_backtest_summary(
            target_task=target_task,
            selection_policy_name=selection_policy_name,
            strategy_name=backtest_strategy_name(target_task),
            minimum_train_games=minimum_train_games,
            test_window_games=test_window_games,
        )
        summary["dataset_game_count"] = len(ordered_game_ids)
        summary["dataset_row_count"] = len(dataset_rows)
        return {
            "record": ModelBacktestRunRecord(
                id=0,
                feature_version_id=feature_version.id,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                status="COMPLETED",
                selection_policy_name=selection_policy_name,
                strategy_name=backtest_strategy_name(target_task),
                minimum_train_games=minimum_train_games,
                test_window_games=test_window_games,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                fold_count=0,
                payload=summary,
            ),
            "summary": summary,
        }

    rows_by_game: dict[int, list[dict[str, Any]]] = {}
    for row in dataset_rows:
        rows_by_game.setdefault(int(row["canonical_game_id"]), []).append(row)

    fold_summaries: list[dict[str, Any]] = []
    all_predictions: list[dict[str, Any]] = []
    for fold_index, train_end in enumerate(
        range(minimum_train_games, len(ordered_game_ids), test_window_games),
        start=1,
    ):
        train_game_ids = ordered_game_ids[:train_end]
        test_game_ids = ordered_game_ids[train_end : train_end + test_window_games]
        if not test_game_ids:
            continue
        train_dataset_rows = [
            row for game_id in train_game_ids for row in rows_by_game.get(game_id, [])
        ]
        test_dataset_rows = [
            row for game_id in test_game_ids for row in rows_by_game.get(game_id, [])
        ]
        selected_snapshot = train_walk_forward_snapshot(
            dataset_rows=train_dataset_rows,
            feature_version=feature_version,
            target_task=target_task,
            selection_policy_name=selection_policy_name,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            partition_feature_dataset_rows=partition_feature_dataset_rows,
            build_feature_training_view=build_feature_training_view,
            train_linear_feature_model=train_linear_feature_model,
            train_tree_stump_model=train_tree_stump_model,
            select_best_evaluation_snapshot=select_best_evaluation_snapshot,
        )
        if selected_snapshot is None:
            continue
        predictions = score_dataset_rows_with_active_selection(
            test_dataset_rows,
            target_task=target_task,
            active_snapshot=selected_snapshot,
            full_dataset_rows=train_dataset_rows,
            include_evidence=False,
            evidence_dimensions=("venue", "days_rest_bucket"),
            comparable_limit=5,
            min_pattern_sample_size=1,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=True,
        )
        fold_summary = build_backtest_fold_summary(
            fold_index=fold_index,
            target_task=target_task,
            train_game_ids=train_game_ids,
            test_game_ids=test_game_ids,
            selected_snapshot=selected_snapshot,
            predictions=predictions,
            opportunity_policy_configs=opportunity_policy_configs,
        )
        fold_summaries.append(fold_summary)
        all_predictions.extend(predictions)

    summary = summarize_walk_forward_backtest(
        target_task=target_task,
        selection_policy_name=selection_policy_name,
        minimum_train_games=minimum_train_games,
        test_window_games=test_window_games,
        dataset_row_count=len(dataset_rows),
        dataset_game_count=len(ordered_game_ids),
        fold_summaries=fold_summaries,
        predictions=all_predictions,
    )
    record = ModelBacktestRunRecord(
        id=0,
        feature_version_id=feature_version.id,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        status="COMPLETED",
        selection_policy_name=selection_policy_name,
        strategy_name=summary["strategy_name"],
        minimum_train_games=minimum_train_games,
        test_window_games=test_window_games,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        fold_count=len(fold_summaries),
        payload=summary,
    )
    return {"record": record, "summary": summary}


def ordered_dataset_game_ids(dataset_rows: list[dict[str, Any]]) -> list[int]:
    seen: set[int] = set()
    ordered_game_ids: list[int] = []
    for row in sorted(
        dataset_rows,
        key=lambda entry: (entry["game_date"], int(entry["canonical_game_id"]), entry["team_code"]),
    ):
        canonical_game_id = int(row["canonical_game_id"])
        if canonical_game_id in seen:
            continue
        seen.add(canonical_game_id)
        ordered_game_ids.append(canonical_game_id)
    return ordered_game_ids


def train_walk_forward_snapshot(
    *,
    dataset_rows: list[dict[str, Any]],
    feature_version: FeatureVersionRecord,
    target_task: str,
    selection_policy_name: str,
    train_ratio: float,
    validation_ratio: float,
    partition_feature_dataset_rows: Callable[..., dict[str, list[dict[str, Any]]]],
    build_feature_training_view: Callable[..., dict[str, Any]],
    train_linear_feature_model: Callable[..., dict[str, Any]],
    train_tree_stump_model: Callable[..., dict[str, Any]],
    select_best_evaluation_snapshot: Callable[..., ModelEvaluationSnapshotRecord | None],
) -> ModelEvaluationSnapshotRecord | None:
    split_rows = partition_feature_dataset_rows(
        dataset_rows,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
    )
    split_training_rows = {
        split_name: build_feature_training_view(
            rows,
            target_task=target_task,
            drop_null_targets=True,
        )["training_rows"]
        for split_name, rows in split_rows.items()
    }
    candidate_snapshots: list[ModelEvaluationSnapshotRecord] = []
    for index, (model_family, trainer) in enumerate(
        (("linear_feature", train_linear_feature_model), ("tree_stump", train_tree_stump_model)),
        start=1,
    ):
        model_result = trainer(
            train_rows=split_training_rows["train"],
            validation_rows=split_training_rows["validation"],
            test_rows=split_training_rows["test"],
        )
        candidate_snapshots.append(
            ModelEvaluationSnapshotRecord(
                id=index,
                model_training_run_id=0,
                model_registry_id=0,
                feature_version_id=feature_version.id,
                target_task=target_task,
                model_family=model_family,
                selected_feature=model_result["artifact"].get("selected_feature"),
                fallback_strategy=model_result["artifact"].get("fallback_strategy"),
                primary_metric_name="mae",
                validation_metric_value=model_result["metrics"]["validation"].get("mae"),
                test_metric_value=model_result["metrics"]["test"].get("mae"),
                validation_prediction_count=int(
                    model_result["metrics"]["validation"].get("prediction_count", 0)
                ),
                test_prediction_count=int(
                    model_result["metrics"]["test"].get("prediction_count", 0)
                ),
                snapshot=model_result,
            )
        )
    return select_best_evaluation_snapshot(
        candidate_snapshots,
        selection_policy_name=selection_policy_name,
    )


def build_backtest_fold_summary(
    *,
    fold_index: int,
    target_task: str,
    train_game_ids: list[int],
    test_game_ids: list[int],
    selected_snapshot: ModelEvaluationSnapshotRecord,
    predictions: list[dict[str, Any]],
    opportunity_policy_configs: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    candidate_strategy = evaluate_backtest_strategy(
        predictions=predictions,
        target_task=target_task,
        threshold=float(opportunity_policy_configs[target_task]["candidate_min_signal_strength"]),
        strategy_name="candidate_threshold",
    )
    review_strategy = evaluate_backtest_strategy(
        predictions=predictions,
        target_task=target_task,
        threshold=float(opportunity_policy_configs[target_task]["review_min_signal_strength"]),
        strategy_name="review_threshold",
    )
    return {
        "fold_index": fold_index,
        "train_game_count": len(train_game_ids),
        "test_game_count": len(test_game_ids),
        "train_game_ids": train_game_ids,
        "test_game_ids": test_game_ids,
        "selected_model": {
            "evaluation_snapshot_id": selected_snapshot.id,
            "model_training_run_id": selected_snapshot.model_training_run_id,
            "model_family": selected_snapshot.model_family,
            "selected_feature": selected_snapshot.selected_feature,
            "fallback_strategy": selected_snapshot.fallback_strategy,
            "validation_metric_value": selected_snapshot.validation_metric_value,
            "test_metric_value": selected_snapshot.test_metric_value,
        },
        "prediction_metrics": summarize_backtest_prediction_metrics(predictions),
        "strategies": {
            "candidate_threshold": candidate_strategy,
            "review_threshold": review_strategy,
        },
    }


def summarize_walk_forward_backtest(
    *,
    target_task: str,
    selection_policy_name: str,
    minimum_train_games: int,
    test_window_games: int,
    dataset_row_count: int,
    dataset_game_count: int,
    fold_summaries: list[dict[str, Any]],
    predictions: list[dict[str, Any]],
) -> dict[str, Any]:
    candidate_bets = [
        bet for fold in fold_summaries for bet in fold["strategies"]["candidate_threshold"]["bets"]
    ]
    review_bets = [
        bet for fold in fold_summaries for bet in fold["strategies"]["review_threshold"]["bets"]
    ]
    selected_family_counts: dict[str, int] = {}
    for fold in fold_summaries:
        family = fold["selected_model"]["model_family"]
        selected_family_counts[family] = selected_family_counts.get(family, 0) + 1
    return {
        "target_task": target_task,
        "selection_policy_name": selection_policy_name,
        "strategy_name": backtest_strategy_name(target_task),
        "minimum_train_games": minimum_train_games,
        "test_window_games": test_window_games,
        "dataset_row_count": dataset_row_count,
        "dataset_game_count": dataset_game_count,
        "fold_count": len(fold_summaries),
        "selected_model_family_counts": selected_family_counts,
        "prediction_metrics": summarize_backtest_prediction_metrics(predictions),
        "strategy_results": {
            "candidate_threshold": summarize_backtest_bets(
                candidate_bets,
                strategy_name="candidate_threshold",
            ),
            "review_threshold": summarize_backtest_bets(
                review_bets,
                strategy_name="review_threshold",
            ),
        },
        "folds": fold_summaries,
    }


def empty_backtest_summary(
    *,
    target_task: str,
    selection_policy_name: str,
    strategy_name: str,
    minimum_train_games: int,
    test_window_games: int,
) -> dict[str, Any]:
    return {
        "target_task": target_task,
        "selection_policy_name": selection_policy_name,
        "strategy_name": strategy_name,
        "minimum_train_games": minimum_train_games,
        "test_window_games": test_window_games,
        "dataset_row_count": 0,
        "dataset_game_count": 0,
        "fold_count": 0,
        "selected_model_family_counts": {},
        "prediction_metrics": summarize_backtest_prediction_metrics([]),
        "strategy_results": {
            "candidate_threshold": summarize_backtest_bets(
                [],
                strategy_name="candidate_threshold",
            ),
            "review_threshold": summarize_backtest_bets(
                [],
                strategy_name="review_threshold",
            ),
        },
        "folds": [],
    }


def backtest_strategy_name(target_task: str) -> str:
    return f"{target_task}_walk_forward_v1"


def summarize_backtest_prediction_metrics(predictions: list[dict[str, Any]]) -> dict[str, Any]:
    realized_residuals = [
        float(entry["realized_residual"])
        for entry in predictions
        if entry.get("realized_residual") is not None
    ]
    actual_targets = [
        float(entry["actual_target_value"])
        for entry in predictions
        if entry.get("actual_target_value") is not None
    ]
    mae = None
    rmse = None
    if predictions and actual_targets:
        absolute_errors = [
            abs(float(entry["prediction_value"]) - float(entry["actual_target_value"]))
            for entry in predictions
            if entry.get("actual_target_value") is not None
        ]
        squared_errors = [
            (float(entry["prediction_value"]) - float(entry["actual_target_value"])) ** 2
            for entry in predictions
            if entry.get("actual_target_value") is not None
        ]
        mae = round(float(mean(absolute_errors)), 4) if absolute_errors else None
        rmse = round(float(mean(squared_errors) ** 0.5), 4) if squared_errors else None
    return {
        "prediction_count": len(predictions),
        "mae": mae,
        "rmse": rmse,
        "average_prediction_value": (
            round(float(mean(float(entry["prediction_value"]) for entry in predictions)), 4)
            if predictions
            else None
        ),
        "average_realized_residual": (
            round(float(mean(realized_residuals)), 4) if realized_residuals else None
        ),
    }


def evaluate_backtest_strategy(
    *,
    predictions: list[dict[str, Any]],
    target_task: str,
    threshold: float,
    strategy_name: str,
) -> dict[str, Any]:
    bets = []
    for prediction in predictions:
        signal_strength = float(prediction["signal_strength"])
        if signal_strength < threshold:
            continue
        bet = build_backtest_bet(
            prediction=prediction,
            target_task=target_task,
            strategy_name=strategy_name,
            threshold=threshold,
            float_or_none=_float_or_none,
        )
        if bet is not None:
            bets.append(bet)
    summary = summarize_backtest_bets(bets, strategy_name=strategy_name)
    summary["threshold"] = threshold
    summary["bets"] = bets
    return summary


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_backtest_bet(
    *,
    prediction: dict[str, Any],
    target_task: str,
    strategy_name: str,
    threshold: float,
    float_or_none: Callable[[Any], float | None],
) -> dict[str, Any] | None:
    prediction_value = float(prediction["prediction_value"])
    result = None
    edge_direction = None
    actual_target_value = float_or_none(prediction.get("actual_target_value"))
    if actual_target_value is None:
        return None
    if target_task == "spread_error_regression":
        edge_direction = "team_cover_edge" if prediction_value > 0 else "opponent_cover_edge"
        if actual_target_value == 0:
            result = "push"
        elif prediction_value > 0:
            result = "win" if actual_target_value > 0 else "loss"
        elif prediction_value < 0:
            result = "win" if actual_target_value < 0 else "loss"
        else:
            return None
    elif target_task == "total_error_regression":
        if actual_target_value == 0:
            result = "push"
        elif prediction_value > 0:
            edge_direction = "over_edge"
            result = "win" if actual_target_value > 0 else "loss"
        elif prediction_value < 0:
            edge_direction = "under_edge"
            result = "win" if actual_target_value < 0 else "loss"
        else:
            return None
    else:
        return None
    profit_units = 0.0
    if result == "win":
        profit_units = 0.9091
    elif result == "loss":
        profit_units = -1.0
    edge_bucket = backtest_edge_bucket(abs(prediction_value))
    return {
        "canonical_game_id": prediction["canonical_game_id"],
        "game_date": prediction["game_date"],
        "team_code": prediction["team_code"],
        "opponent_code": prediction["opponent_code"],
        "strategy_name": strategy_name,
        "threshold": threshold,
        "edge_direction": edge_direction,
        "signal_strength": round(abs(prediction_value), 4),
        "prediction_value": round(prediction_value, 4),
        "result": result,
        "profit_units": round(profit_units, 4),
        "edge_bucket": edge_bucket,
    }


def backtest_edge_bucket(signal_strength: float) -> str:
    if signal_strength < 1:
        return "0_to_1"
    if signal_strength < 2:
        return "1_to_2"
    if signal_strength < 3:
        return "2_to_3"
    return "3_plus"


def summarize_backtest_bets(
    bets: list[dict[str, Any]],
    *,
    strategy_name: str,
) -> dict[str, Any]:
    win_count = sum(1 for bet in bets if bet["result"] == "win")
    loss_count = sum(1 for bet in bets if bet["result"] == "loss")
    push_count = sum(1 for bet in bets if bet["result"] == "push")
    settled_bet_count = win_count + loss_count
    total_profit_units = round(float(sum(float(bet["profit_units"]) for bet in bets)), 4)
    edge_bucket_performance: dict[str, dict[str, Any]] = {}
    for bet in bets:
        bucket = edge_bucket_performance.setdefault(
            bet["edge_bucket"],
            {
                "bet_count": 0,
                "win_count": 0,
                "loss_count": 0,
                "push_count": 0,
                "profit_units": 0.0,
            },
        )
        bucket["bet_count"] += 1
        bucket[f"{bet['result']}_count"] += 1
        bucket["profit_units"] = round(
            float(bucket["profit_units"]) + float(bet["profit_units"]),
            4,
        )
    for bucket in edge_bucket_performance.values():
        settled = int(bucket["win_count"]) + int(bucket["loss_count"])
        bucket["hit_rate"] = round(int(bucket["win_count"]) / settled, 4) if settled else None
        bucket["push_rate"] = (
            round(int(bucket["push_count"]) / int(bucket["bet_count"]), 4)
            if bucket["bet_count"]
            else None
        )
        bucket["roi"] = (
            round(float(bucket["profit_units"]) / int(bucket["bet_count"]), 4)
            if bucket["bet_count"]
            else None
        )
    return {
        "strategy_name": strategy_name,
        "bet_count": len(bets),
        "win_count": win_count,
        "loss_count": loss_count,
        "push_count": push_count,
        "hit_rate": round(win_count / settled_bet_count, 4) if settled_bet_count else None,
        "push_rate": round(push_count / len(bets), 4) if bets else None,
        "roi": round(total_profit_units / len(bets), 4) if bets else None,
        "profit_units": total_profit_units,
        "edge_bucket_performance": edge_bucket_performance,
    }
