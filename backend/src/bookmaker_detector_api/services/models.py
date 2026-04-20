from __future__ import annotations

from dataclasses import asdict
from datetime import date
from typing import Any

from bookmaker_detector_api.services import (
    model_backtest_runs,
    model_backtest_workflows,
    model_future_scenarios,
    model_market_board_orchestration,
    model_opportunities,
    model_scoring_previews,
    model_scoring_runs,
    model_training_algorithms,
    model_training_lifecycle,
    model_training_views,
)
from bookmaker_detector_api.services.features import (
    DEFAULT_FEATURE_KEY,
    FeatureVersionRecord,
    _partition_feature_dataset_rows,
    build_feature_dataset_rows,
    build_feature_training_view,
    build_future_feature_dataset_rows,
    get_feature_version_postgres,
    list_canonical_game_metric_records_postgres,
    list_feature_snapshots_postgres,
)
from bookmaker_detector_api.services.model_market_board_sources import (
    MARKET_BOARD_SOURCE_CONFIGS,
    _build_market_board_source_fingerprint_comparison,
    _build_market_board_source_payload_fingerprints,
    _build_market_board_source_request_context,
    _normalize_market_board_source_games,
    build_model_market_board_source_games,
)
from bookmaker_detector_api.services.model_market_board_sources import (
    list_model_market_board_sources as _list_model_market_board_sources,
)
from bookmaker_detector_api.services.model_market_board_store import (
    _build_market_board_refresh_change_summary,
    _build_model_market_board,
    _build_model_market_board_cadence_batch,
    _build_model_market_board_refresh_batch,
    _build_model_market_board_scoring_batch,
    _build_model_market_board_source_run,
    _find_model_market_board_postgres,
    _resolve_market_board_refresh_count,
    _resolve_market_board_refresh_status,
    _serialize_model_market_board,
    _serialize_model_market_board_cadence_batch,
    _serialize_model_market_board_refresh_batch,
    _serialize_model_market_board_scoring_batch,
    _serialize_model_market_board_source_run,
    _summarize_market_board_refresh_history,
    _summarize_model_market_board_source_run_history,
    list_model_market_board_cadence_batches_postgres,
    list_model_market_board_refresh_batches_postgres,
    list_model_market_board_refresh_events_postgres,
    list_model_market_board_scoring_batches_postgres,
    list_model_market_board_source_runs_postgres,
    list_model_market_boards_postgres,
    save_model_market_board_cadence_batch_postgres,
    save_model_market_board_postgres,
    save_model_market_board_refresh_batch_postgres,
    save_model_market_board_refresh_event_postgres,
    save_model_market_board_scoring_batch_postgres,
    save_model_market_board_source_run_postgres,
)
from bookmaker_detector_api.services.model_market_board_views import (
    _serialize_model_opportunity,
    _serialize_model_scoring_run,
    _summarize_model_market_board_cadence_batch_history,
    _summarize_model_market_board_refresh_batch_history,
    _summarize_model_market_board_scoring_batch_history,
)
from bookmaker_detector_api.services.model_records import (
    ModelBacktestRunRecord,
    ModelEvaluationSnapshotRecord,
    ModelOpportunityRecord,
    ModelRegistryRecord,
    ModelScoringRunRecord,
    ModelSelectionSnapshotRecord,
    ModelTrainingRunRecord,
)
from bookmaker_detector_api.services.task_registry import DEFAULT_REGRESSION_SELECTION_POLICY_NAME
from bookmaker_detector_api.services.workflow_logging import start_workflow_span

list_model_market_board_sources = _list_model_market_board_sources

SUPPORTED_MODEL_TARGET_TASKS = {
    "point_margin_regression",
    "spread_error_regression",
    "total_error_regression",
    "total_points_regression",
}
MODEL_FAMILY_CONFIGS = model_training_lifecycle.MODEL_FAMILY_CONFIGS
OPPORTUNITY_POLICY_CONFIGS = {
    "spread_error_regression": {
        "policy_name": "spread_edge_policy_v1",
        "candidate_min_signal_strength": 2.0,
        "review_min_signal_strength": 1.0,
        "candidate_evidence_ratings": {"strong"},
        "review_evidence_ratings": {"weak", "moderate", "strong"},
        "candidate_recommendation_statuses": {"candidate_signal"},
        "review_recommendation_statuses": {
            "monitor_only",
            "review_manually",
            "candidate_signal",
        },
    },
    "total_error_regression": {
        "policy_name": "total_edge_policy_v1",
        "candidate_min_signal_strength": 2.0,
        "review_min_signal_strength": 1.0,
        "candidate_evidence_ratings": {"strong"},
        "review_evidence_ratings": {"weak", "moderate", "strong"},
        "candidate_recommendation_statuses": {"candidate_signal"},
        "review_recommendation_statuses": {
            "monitor_only",
            "review_manually",
            "candidate_signal",
        },
    },
    "point_margin_regression": {
        "policy_name": "margin_signal_policy_v1",
        "candidate_min_signal_strength": 4.0,
        "review_min_signal_strength": 2.5,
        "candidate_evidence_ratings": {"strong"},
        "review_evidence_ratings": {"weak", "moderate", "strong"},
        "candidate_recommendation_statuses": {"candidate_signal"},
        "review_recommendation_statuses": {
            "monitor_only",
            "review_manually",
            "candidate_signal",
        },
    },
    "total_points_regression": {
        "policy_name": "totals_signal_policy_v1",
        "candidate_min_signal_strength": 4.0,
        "review_min_signal_strength": 2.5,
        "candidate_evidence_ratings": {"strong"},
        "review_evidence_ratings": {"weak", "moderate", "strong"},
        "candidate_recommendation_statuses": {"candidate_signal"},
        "review_recommendation_statuses": {
            "monitor_only",
            "review_manually",
            "candidate_signal",
        },
    },
}


def train_phase_three_models_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    team_code: str | None = None,
    season_label: str | None = None,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
) -> dict[str, Any]:
    span = start_workflow_span(
        workflow_name="model_training.train",
        storage_mode="postgres",
        feature_key=feature_key,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
    )
    try:
        feature_version = get_feature_version_postgres(connection, feature_key=feature_key)
        if feature_version is None:
            result = {
                "feature_version": None,
                "dataset_row_count": 0,
                "model_runs": [],
                "best_model": None,
            }
        else:
            dataset_rows = _load_training_dataset_rows_postgres(
                connection,
                feature_version_id=feature_version.id,
                team_code=team_code,
                season_label=season_label,
            )
            result = _train_phase_three_models(
                dataset_rows=dataset_rows,
                feature_version=feature_version,
                team_code=team_code,
                season_label=season_label,
                target_task=target_task,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                ensure_registry=lambda model_family: ensure_model_registry_postgres(
                    connection,
                    target_task=target_task,
                    model_family=model_family,
                    team_code=team_code,
                ),
                save_run=lambda run: save_model_training_run_postgres(connection, run),
                list_runs=lambda: list_model_training_runs_postgres(
                    connection,
                    target_task=target_task,
                    team_code=team_code,
                    season_label=season_label,
                ),
            )
    except Exception as exc:
        span.failure(exc)
        raise
    span.success(
        dataset_row_count=int(result.get("dataset_row_count", 0)),
        persisted_run_count=int(result.get("persisted_run_count", 0)),
        best_model_id=(result.get("best_model") or {}).get("id"),
    )
    return result


def ensure_model_registry_postgres(
    connection: Any,
    *,
    target_task: str,
    model_family: str,
    team_code: str | None,
) -> ModelRegistryRecord:
    return model_training_lifecycle.ensure_model_registry_postgres(
        connection,
        target_task=target_task,
        model_family=model_family,
        team_code=team_code,
    )


def save_model_training_run_postgres(
    connection: Any,
    run: ModelTrainingRunRecord,
) -> ModelTrainingRunRecord:
    return model_training_lifecycle.save_model_training_run_postgres(connection, run)


def list_model_registry_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
) -> list[ModelRegistryRecord]:
    return model_training_views.list_model_registry_postgres(
        connection,
        target_task=target_task,
    )


def list_model_training_runs_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
) -> list[ModelTrainingRunRecord]:
    return model_training_views.list_model_training_runs_postgres(
        connection,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
    )


def get_model_training_run_detail_postgres(
    connection: Any,
    *,
    run_id: int,
) -> ModelTrainingRunRecord | None:
    return model_training_views.get_model_training_run_detail_postgres(
        connection,
        run_id=run_id,
    )


def list_model_evaluation_snapshots_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    model_family: str | None = None,
) -> list[ModelEvaluationSnapshotRecord]:
    return model_training_views.list_model_evaluation_snapshots_postgres(
        connection,
        target_task=target_task,
        model_family=model_family,
    )


def get_model_evaluation_snapshot_detail_postgres(
    connection: Any,
    *,
    snapshot_id: int,
) -> ModelEvaluationSnapshotRecord | None:
    return model_training_views.get_model_evaluation_snapshot_detail_postgres(
        connection,
        snapshot_id=snapshot_id,
    )


def get_model_training_summary_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
) -> dict[str, Any]:
    return model_training_views.get_model_training_summary_postgres(
        connection,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
    )


def get_model_training_history_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    return model_training_views.get_model_training_history_postgres(
        connection,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        recent_limit=recent_limit,
    )


def get_model_evaluation_history_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    model_family: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    return model_training_views.get_model_evaluation_history_postgres(
        connection,
        target_task=target_task,
        model_family=model_family,
        recent_limit=recent_limit,
    )


def list_model_selection_snapshots_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    active_only: bool = False,
) -> list[ModelSelectionSnapshotRecord]:
    return model_training_views.list_model_selection_snapshots_postgres(
        connection,
        target_task=target_task,
        active_only=active_only,
    )


def get_model_selection_snapshot_detail_postgres(
    connection: Any,
    *,
    selection_id: int,
) -> ModelSelectionSnapshotRecord | None:
    return model_training_views.get_model_selection_snapshot_detail_postgres(
        connection,
        selection_id=selection_id,
    )


def promote_best_model_postgres(
    connection: Any,
    *,
    target_task: str,
    selection_policy_name: str = DEFAULT_REGRESSION_SELECTION_POLICY_NAME,
) -> dict[str, Any]:
    span = start_workflow_span(
        workflow_name="model_training.promote",
        storage_mode="postgres",
        target_task=target_task,
        selection_policy_name=selection_policy_name,
    )
    try:
        result = model_training_lifecycle.promote_best_model_postgres(
            connection,
            target_task=target_task,
            selection_policy_name=selection_policy_name,
        )
    except Exception as exc:
        span.failure(exc)
        raise
    span.success(
        selection_count=int(result.get("selection_count", 0)),
        active_selection_id=(result.get("active_selection") or {}).get("id"),
        selected_snapshot_id=(result.get("selected_snapshot") or {}).get("id"),
    )
    return result


def get_model_scoring_preview_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    team_code: str | None = None,
    season_label: str | None = None,
    canonical_game_id: int | None = None,
    limit: int = 10,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    span = start_workflow_span(
        workflow_name="model_scoring.preview",
        storage_mode="postgres",
        feature_key=feature_key,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        canonical_game_id=canonical_game_id,
        limit=limit,
    )
    try:
        feature_version = get_feature_version_postgres(connection, feature_key=feature_key)
        if feature_version is None:
            result = {
                "feature_version": None,
                "active_selection": None,
                "active_evaluation_snapshot": None,
                "row_count": 0,
                "scored_prediction_count": 0,
                "prediction_summary": {},
                "predictions": [],
            }
        else:
            active_selection = model_scoring_previews.resolve_active_model_selection(
                selections=list_model_selection_snapshots_postgres(
                    connection,
                    target_task=target_task,
                    active_only=True,
                ),
            )
            active_snapshot = model_scoring_previews.resolve_evaluation_snapshot_by_id(
                snapshots=list_model_evaluation_snapshots_postgres(
                    connection,
                    target_task=target_task,
                ),
                snapshot_id=(
                    active_selection.model_evaluation_snapshot_id
                    if active_selection is not None
                    else None
                ),
            )
            dataset_rows = _load_training_dataset_rows_postgres(
                connection,
                feature_version_id=feature_version.id,
                team_code=team_code,
                season_label=season_label,
            )
            scoring_result = model_scoring_previews.build_model_scoring_preview(
                dataset_rows=dataset_rows,
                target_task=target_task,
                active_selection=active_selection,
                active_snapshot=active_snapshot,
                canonical_game_id=canonical_game_id,
                limit=limit,
                include_evidence=include_evidence,
                evidence_dimensions=evidence_dimensions,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                drop_null_targets=drop_null_targets,
                predict_linear=_predict_linear,
                predict_tree_stump=_predict_tree_stump,
                get_row_feature_value=_get_row_feature_value,
            )
            result = {
                "feature_version": asdict(feature_version),
                **scoring_result,
            }
    except Exception as exc:
        span.failure(exc)
        raise
    span.success(
        row_count=int(result.get("row_count", 0)),
        scored_prediction_count=int(result.get("scored_prediction_count", 0)),
        active_selection_id=(result.get("active_selection") or {}).get("id"),
    )
    return result


def get_model_future_game_preview_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    season_label: str,
    game_date: date,
    home_team_code: str,
    away_team_code: str,
    home_spread_line: float | None = None,
    total_line: float | None = None,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    span = start_workflow_span(
        workflow_name="model_scoring.future_game_preview",
        storage_mode="postgres",
        feature_key=feature_key,
        target_task=target_task,
        season_label=season_label,
        game_date=game_date,
        home_team_code=home_team_code,
        away_team_code=away_team_code,
    )
    try:
        feature_version = get_feature_version_postgres(connection, feature_key=feature_key)
        if feature_version is None:
            result = {
                "feature_version": None,
                "active_selection": None,
                "active_evaluation_snapshot": None,
                "scenario": None,
                "scored_prediction_count": 0,
                "prediction_summary": {},
                "predictions": [],
                "opportunity_preview": [],
            }
        else:
            active_selection = model_scoring_previews.resolve_active_model_selection(
                selections=list_model_selection_snapshots_postgres(
                    connection,
                    target_task=target_task,
                    active_only=True,
                ),
            )
            active_snapshot = model_scoring_previews.resolve_evaluation_snapshot_by_id(
                snapshots=list_model_evaluation_snapshots_postgres(
                    connection,
                    target_task=target_task,
                ),
                snapshot_id=(
                    active_selection.model_evaluation_snapshot_id
                    if active_selection is not None
                    else None
                ),
            )
            canonical_games = list_canonical_game_metric_records_postgres(connection)
            historical_dataset_rows = _load_training_dataset_rows_postgres(
                connection,
                feature_version_id=feature_version.id,
                team_code=None,
                season_label=None,
            )
            scenario_rows = build_future_feature_dataset_rows(
                canonical_games,
                feature_version_id=feature_version.id,
                season_label=season_label,
                game_date=game_date,
                home_team_code=home_team_code,
                away_team_code=away_team_code,
                home_spread_line=home_spread_line,
                total_line=total_line,
            )
            result = {
                "feature_version": asdict(feature_version),
                **model_scoring_previews.build_model_future_game_preview(
                    target_task=target_task,
                    active_selection=active_selection,
                    active_snapshot=active_snapshot,
                    historical_dataset_rows=historical_dataset_rows,
                    scenario_rows=scenario_rows,
                    include_evidence=include_evidence,
                    evidence_dimensions=evidence_dimensions,
                    comparable_limit=comparable_limit,
                    min_pattern_sample_size=min_pattern_sample_size,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                    drop_null_targets=drop_null_targets,
                    predict_linear=_predict_linear,
                    predict_tree_stump=_predict_tree_stump,
                    get_row_feature_value=_get_row_feature_value,
                    evaluate_opportunity_status=model_opportunities.evaluate_opportunity_status,
                    nested_get=model_opportunities.nested_get,
                    opportunity_policy=OPPORTUNITY_POLICY_CONFIGS.get(target_task),
                ),
            }
    except Exception as exc:
        span.failure(exc)
        raise
    span.success(
        scored_prediction_count=int(result.get("scored_prediction_count", 0)),
        scenario_key=(result.get("scenario") or {}).get("scenario_key"),
        opportunity_preview_count=len(result.get("opportunity_preview", [])),
    )
    return result


def materialize_model_future_game_preview_postgres(
    connection: Any,
    *,
    model_market_board_id: int | None = None,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    season_label: str,
    game_date: date,
    home_team_code: str,
    away_team_code: str,
    home_spread_line: float | None = None,
    total_line: float | None = None,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    span = start_workflow_span(
        workflow_name="model_scoring.future_game_materialize",
        storage_mode="postgres",
        feature_key=feature_key,
        target_task=target_task,
        season_label=season_label,
        game_date=game_date,
        home_team_code=home_team_code,
        away_team_code=away_team_code,
        model_market_board_id=model_market_board_id,
    )
    try:
        preview = get_model_future_game_preview_postgres(
            connection,
            feature_key=feature_key,
            target_task=target_task,
            season_label=season_label,
            game_date=game_date,
            home_team_code=home_team_code,
            away_team_code=away_team_code,
            home_spread_line=home_spread_line,
            total_line=total_line,
            include_evidence=include_evidence,
            evidence_dimensions=evidence_dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
        )
        result = model_future_scenarios.materialize_future_game_preview(
            preview=preview,
            target_task=target_task,
            default_policy_name=OPPORTUNITY_POLICY_CONFIGS.get(target_task, {}).get("policy_name"),
            model_market_board_id=model_market_board_id,
            build_scoring_run=model_scoring_runs.build_model_scoring_run,
            save_scoring_run=lambda scoring_run: (
                model_scoring_runs.save_model_scoring_run_postgres(
                    connection,
                    scoring_run,
                )
            ),
            serialize_scoring_run=_serialize_model_scoring_run,
        )
    except Exception as exc:
        span.failure(exc)
        raise
    span.success(
        scored_prediction_count=int(result.get("scored_prediction_count", 0)),
        materialized_count=int(result.get("materialized_count", 0)),
        scoring_run_id=(result.get("scoring_run") or {}).get("id"),
    )
    return result


def list_model_scoring_runs_postgres(
    connection: Any,
    *,
    model_market_board_id: int | None = None,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
) -> list[ModelScoringRunRecord]:
    return model_scoring_runs.list_model_scoring_runs_postgres(
        connection,
        model_market_board_id=model_market_board_id,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
    )


def get_model_scoring_run_detail_postgres(
    connection: Any,
    *,
    scoring_run_id: int,
) -> dict[str, Any] | None:
    return model_scoring_runs.get_model_scoring_run_detail_postgres(
        connection,
        scoring_run_id=scoring_run_id,
    )


def get_model_scoring_history_postgres(
    connection: Any,
    *,
    model_market_board_id: int | None = None,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    return model_scoring_runs.get_model_scoring_history_postgres(
        connection,
        model_market_board_id=model_market_board_id,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        recent_limit=recent_limit,
    )


def materialize_model_opportunities_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    team_code: str | None = None,
    season_label: str | None = None,
    canonical_game_id: int | None = None,
    limit: int = 10,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    span = start_workflow_span(
        workflow_name="model_opportunities.materialize",
        storage_mode="postgres",
        feature_key=feature_key,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        canonical_game_id=canonical_game_id,
        limit=limit,
    )
    try:
        materialization_context = model_opportunities.build_materialization_context(
            team_code=team_code,
            season_label=season_label,
            canonical_game_id=canonical_game_id,
        )
        scoring_preview = get_model_scoring_preview_postgres(
            connection,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            canonical_game_id=canonical_game_id,
            limit=limit,
            include_evidence=include_evidence,
            evidence_dimensions=evidence_dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
        )
        result = model_opportunities.materialize_model_opportunities(
            scoring_preview=scoring_preview,
            target_task=target_task,
            materialization_context=materialization_context,
            build_opportunities=lambda **kwargs: model_opportunities.build_model_opportunities(
                **kwargs,
                policy=OPPORTUNITY_POLICY_CONFIGS.get(target_task),
            ),
            save_opportunities=lambda opportunities: (
                model_opportunities.save_model_opportunities_postgres(
                    connection,
                    opportunities,
                )
            ),
        )
    except Exception as exc:
        span.failure(exc)
        raise
    span.success(
        scoring_preview_count=int(scoring_preview.get("scored_prediction_count", 0)),
        opportunity_count=int(result.get("opportunity_count", 0)),
        materialized_count=int(result.get("materialized_count", 0)),
    )
    return result


def materialize_model_future_opportunities_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    season_label: str,
    game_date: date,
    home_team_code: str,
    away_team_code: str,
    home_spread_line: float | None = None,
    total_line: float | None = None,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    materialization_context = model_opportunities.build_materialization_context(
        season_label=season_label,
        scope_source="worker",
        scope_key=(
            "worker"
            f"|scenario={season_label}:{game_date.isoformat()}:{home_team_code}:{away_team_code}"
        ),
    )
    materialized_preview = materialize_model_future_game_preview_postgres(
        connection,
        feature_key=feature_key,
        target_task=target_task,
        season_label=season_label,
        game_date=game_date,
        home_team_code=home_team_code,
        away_team_code=away_team_code,
        home_spread_line=home_spread_line,
        total_line=total_line,
        include_evidence=include_evidence,
        evidence_dimensions=evidence_dimensions,
        comparable_limit=comparable_limit,
        min_pattern_sample_size=min_pattern_sample_size,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
    )
    return model_future_scenarios.materialize_future_opportunities(
        materialized_preview=materialized_preview,
        target_task=target_task,
        build_opportunities=lambda **kwargs: model_opportunities.build_model_opportunities(
            **kwargs,
            policy=OPPORTUNITY_POLICY_CONFIGS.get(target_task),
            materialization_context=materialization_context,
        ),
        save_opportunities=lambda opportunities: save_model_opportunities_postgres(
            connection,
            opportunities,
        ),
        serialize_opportunity=_serialize_model_opportunity,
    )


def get_model_future_slate_preview_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    games: list[dict[str, Any]],
    slate_label: str | None = None,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    span = start_workflow_span(
        workflow_name="model_scoring.future_slate_preview",
        storage_mode="postgres",
        feature_key=feature_key,
        target_task=target_task,
        slate_label=slate_label,
        game_count=len(games),
    )
    try:
        result = model_future_scenarios.get_future_slate_preview(
            games=games,
            target_task=target_task,
            slate_label=slate_label,
            preview_loader=lambda game: get_model_future_game_preview_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                season_label=str(game["season_label"]),
                game_date=model_future_scenarios.coerce_date(game["game_date"]),
                home_team_code=str(game["home_team_code"]),
                away_team_code=str(game["away_team_code"]),
                home_spread_line=_float_or_none(game.get("home_spread_line")),
                total_line=_float_or_none(game.get("total_line")),
                include_evidence=include_evidence,
                evidence_dimensions=evidence_dimensions,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                drop_null_targets=drop_null_targets,
            ),
            serialize_input=lambda game: model_future_scenarios.serialize_future_game_input(
                game,
                float_or_none=_float_or_none,
            ),
        )
    except Exception as exc:
        span.failure(exc)
        raise
    span.success(
        game_preview_count=int(result.get("game_preview_count", 0)),
        scored_prediction_count=int(result.get("scored_prediction_count", 0)),
        slate_key=(result.get("slate") or {}).get("slate_key"),
    )
    return result


def materialize_model_future_slate_postgres(
    connection: Any,
    *,
    model_market_board_id: int | None = None,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    games: list[dict[str, Any]],
    slate_label: str | None = None,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    span = start_workflow_span(
        workflow_name="model_scoring.future_slate_materialize",
        storage_mode="postgres",
        feature_key=feature_key,
        target_task=target_task,
        slate_label=slate_label,
        game_count=len(games),
        model_market_board_id=model_market_board_id,
    )
    try:
        slate_scope_key = "worker"
        if slate_label is not None:
            slate_scope_key += f"|slate={slate_label}"
        if model_market_board_id is not None:
            slate_scope_key += f"|board={model_market_board_id}"
        materialization_context = model_opportunities.build_materialization_context(
            scope_source="worker",
            scope_key=slate_scope_key,
        )
        result = model_future_scenarios.materialize_future_slate(
            games=games,
            target_task=target_task,
            slate_label=slate_label,
            materialize_preview_loader=lambda game: materialize_model_future_game_preview_postgres(
                connection,
                model_market_board_id=model_market_board_id,
                feature_key=feature_key,
                target_task=target_task,
                season_label=str(game["season_label"]),
                game_date=model_future_scenarios.coerce_date(game["game_date"]),
                home_team_code=str(game["home_team_code"]),
                away_team_code=str(game["away_team_code"]),
                home_spread_line=_float_or_none(game.get("home_spread_line")),
                total_line=_float_or_none(game.get("total_line")),
                include_evidence=include_evidence,
                evidence_dimensions=evidence_dimensions,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                drop_null_targets=drop_null_targets,
            ),
            build_opportunities=lambda **kwargs: model_opportunities.build_model_opportunities(
                **kwargs,
                policy=OPPORTUNITY_POLICY_CONFIGS.get(target_task),
                materialization_context=materialization_context,
            ),
            save_opportunities=lambda opportunities: save_model_opportunities_postgres(
                connection,
                opportunities,
            ),
            serialize_opportunity=_serialize_model_opportunity,
            serialize_input=lambda game: model_future_scenarios.serialize_future_game_input(
                game,
                float_or_none=_float_or_none,
            ),
        )
    except Exception as exc:
        span.failure(exc)
        raise
    span.success(
        materialized_scoring_run_count=int(result.get("materialized_scoring_run_count", 0)),
        materialized_opportunity_count=int(result.get("materialized_opportunity_count", 0)),
        slate_key=(result.get("slate") or {}).get("slate_key"),
    )
    return result


def materialize_model_market_board_postgres(
    connection: Any,
    *,
    target_task: str,
    games: list[dict[str, Any]],
    slate_label: str | None = None,
) -> dict[str, Any]:
    return model_market_board_orchestration.materialize_model_market_board(
        target_task=target_task,
        games=games,
        slate_label=slate_label,
        build_market_board=_build_model_market_board,
        save_market_board=lambda board: save_model_market_board_postgres(connection, board),
        serialize_market_board=_serialize_model_market_board,
    )


def refresh_model_market_board_postgres(
    connection: Any,
    *,
    target_task: str,
    source_name: str,
    season_label: str,
    game_date: date,
    slate_label: str | None = None,
    game_count: int | None = None,
    source_path: str | None = None,
) -> dict[str, Any]:
    span = start_workflow_span(
        workflow_name="model_market_board.refresh",
        storage_mode="postgres",
        target_task=target_task,
        source_name=source_name,
        season_label=season_label,
        game_date=game_date,
        slate_label=slate_label,
        game_count=game_count,
        source_path=source_path,
    )
    try:
        result = model_market_board_orchestration.refresh_model_market_board(
            target_task=target_task,
            source_name=source_name,
            season_label=season_label,
            game_date=game_date,
            slate_label=slate_label,
            game_count=game_count,
            source_path=source_path,
            default_game_count=int(MARKET_BOARD_SOURCE_CONFIGS[source_name]["default_game_count"]),
            build_source_request_context=_build_market_board_source_request_context,
            load_source_games=build_model_market_board_source_games,
            normalize_source_games=_normalize_market_board_source_games,
            build_source_payload_fingerprints=_build_market_board_source_payload_fingerprints,
            build_source_run=_build_model_market_board_source_run,
            save_source_run=lambda source_run: save_model_market_board_source_run_postgres(
                connection,
                source_run,
            ),
            serialize_source_run=_serialize_model_market_board_source_run,
            find_existing_board=lambda **kwargs: _find_model_market_board_postgres(
                connection,
                **kwargs,
            ),
            materialize_board=lambda **kwargs: materialize_model_market_board_postgres(
                connection,
                **kwargs,
            ),
            build_fingerprint_comparison=_build_market_board_source_fingerprint_comparison,
            build_refresh_change_summary=_build_market_board_refresh_change_summary,
            resolve_refresh_status=_resolve_market_board_refresh_status,
            resolve_refresh_count=_resolve_market_board_refresh_count,
            save_market_board=lambda board: save_model_market_board_postgres(connection, board),
            save_refresh_event=lambda event: save_model_market_board_refresh_event_postgres(
                connection,
                event,
            ),
            serialize_market_board=_serialize_model_market_board,
        )
    except Exception as exc:
        span.failure(exc)
        raise
    validation_summary = result.get("validation_summary") or {}
    source_run = result.get("source_run") or {}
    board = result.get("board") or {}
    span.success(
        refresh_status=result.get("status"),
        generated_game_count=int(result.get("generated_game_count", 0)),
        invalid_row_count=int(validation_summary.get("invalid_row_count", 0)),
        source_run_id=source_run.get("id"),
        board_id=board.get("id"),
    )
    return result


def get_model_market_board_detail_postgres(
    connection: Any,
    *,
    board_id: int,
) -> dict[str, Any] | None:
    board = next(
        (entry for entry in list_model_market_boards_postgres(connection) if entry.id == board_id),
        None,
    )
    return _serialize_model_market_board(board)


def get_model_market_board_refresh_history_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    events = list_model_market_board_refresh_events_postgres(
        connection,
        target_task=target_task,
        source_name=source_name,
    )
    return _summarize_market_board_refresh_history(events, recent_limit=recent_limit)


def get_model_market_board_source_run_history_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    season_label: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    runs = list_model_market_board_source_runs_postgres(
        connection,
        target_task=target_task,
        source_name=source_name,
        season_label=season_label,
    )
    return _summarize_model_market_board_source_run_history(runs, recent_limit=recent_limit)


def get_model_market_board_refresh_queue_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    season_label: str | None = None,
    source_name: str | None = None,
    freshness_status: str | None = None,
    pending_only: bool = False,
    recent_limit: int = 10,
) -> dict[str, Any]:
    return model_market_board_orchestration.get_model_market_board_refresh_queue(
        boards=list_model_market_boards_postgres(
            connection,
            target_task=target_task,
            season_label=season_label,
        ),
        refresh_events=list_model_market_board_refresh_events_postgres(
            connection,
            target_task=target_task,
            source_name=source_name,
        ),
        source_name=source_name,
        freshness_status=freshness_status,
        pending_only=pending_only,
        recent_limit=recent_limit,
    )


def get_model_market_board_scoring_queue_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    season_label: str | None = None,
    source_name: str | None = None,
    freshness_status: str | None = None,
    pending_only: bool = False,
    recent_limit: int = 10,
) -> dict[str, Any]:
    return model_market_board_orchestration.get_model_market_board_scoring_queue(
        boards=list_model_market_boards_postgres(
            connection,
            target_task=target_task,
            season_label=season_label,
        ),
        scoring_runs=list_model_scoring_runs_postgres(
            connection,
            target_task=target_task,
            season_label=season_label,
        ),
        source_name=source_name,
        freshness_status=freshness_status,
        pending_only=pending_only,
        recent_limit=recent_limit,
    )


def orchestrate_model_market_board_refresh_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    season_label: str | None = None,
    source_name: str | None = None,
    freshness_status: str | None = None,
    pending_only: bool = True,
    recent_limit: int = 10,
) -> dict[str, Any]:
    span = start_workflow_span(
        workflow_name="model_market_board.refresh_orchestration",
        storage_mode="postgres",
        target_task=target_task,
        season_label=season_label,
        source_name=source_name,
        freshness_status=freshness_status,
        pending_only=pending_only,
        recent_limit=recent_limit,
    )
    try:
        result = model_market_board_orchestration.orchestrate_model_market_board_refresh(
            target_task=target_task,
            season_label=season_label,
            source_name=source_name,
            freshness_status=freshness_status,
            pending_only=pending_only,
            recent_limit=recent_limit,
            get_queue=lambda **kwargs: get_model_market_board_refresh_queue_postgres(
                connection,
                **kwargs,
            ),
            refresh_board=lambda **kwargs: refresh_model_market_board_postgres(
                connection, **kwargs
            ),
            build_batch=_build_model_market_board_refresh_batch,
            save_batch=lambda batch: save_model_market_board_refresh_batch_postgres(
                connection, batch
            ),
            serialize_batch=_serialize_model_market_board_refresh_batch,
        )
    except Exception as exc:
        span.failure(exc)
        raise
    refresh_batch = result.get("refresh_batch") or {}
    span.success(
        candidate_board_count=int(result.get("candidate_board_count", 0)),
        refreshed_board_count=int(result.get("refreshed_board_count", 0)),
        refresh_batch_id=refresh_batch.get("id"),
    )
    return result


def orchestrate_model_market_board_scoring_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str | None = None,
    season_label: str | None = None,
    source_name: str | None = None,
    freshness_status: str | None = "fresh",
    pending_only: bool = True,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
    recent_limit: int = 10,
) -> dict[str, Any]:
    span = start_workflow_span(
        workflow_name="model_market_board.scoring_orchestration",
        storage_mode="postgres",
        feature_key=feature_key,
        target_task=target_task,
        season_label=season_label,
        source_name=source_name,
        freshness_status=freshness_status,
        pending_only=pending_only,
        recent_limit=recent_limit,
    )
    try:
        result = model_market_board_orchestration.orchestrate_model_market_board_scoring(
            feature_key=feature_key,
            target_task=target_task,
            season_label=season_label,
            source_name=source_name,
            freshness_status=freshness_status,
            pending_only=pending_only,
            include_evidence=include_evidence,
            evidence_dimensions=evidence_dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
            recent_limit=recent_limit,
            get_queue=lambda **kwargs: get_model_market_board_scoring_queue_postgres(
                connection,
                **kwargs,
            ),
            score_board=lambda **kwargs: score_model_market_board_postgres(connection, **kwargs),
            build_batch=_build_model_market_board_scoring_batch,
            save_batch=lambda batch: save_model_market_board_scoring_batch_postgres(
                connection, batch
            ),
            serialize_batch=_serialize_model_market_board_scoring_batch,
        )
    except Exception as exc:
        span.failure(exc)
        raise
    orchestration_batch = result.get("orchestration_batch") or {}
    span.success(
        candidate_board_count=int(result.get("candidate_board_count", 0)),
        scored_board_count=int(result.get("scored_board_count", 0)),
        materialized_scoring_run_count=int(result.get("materialized_scoring_run_count", 0)),
        materialized_opportunity_count=int(result.get("materialized_opportunity_count", 0)),
        scoring_batch_id=orchestration_batch.get("id"),
    )
    return result


def orchestrate_model_market_board_cadence_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str | None = None,
    season_label: str | None = None,
    source_name: str | None = None,
    refresh_freshness_status: str | None = None,
    refresh_pending_only: bool = False,
    scoring_freshness_status: str | None = "fresh",
    scoring_pending_only: bool = True,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
    recent_limit: int = 10,
) -> dict[str, Any]:
    span = start_workflow_span(
        workflow_name="model_market_board.cadence_orchestration",
        storage_mode="postgres",
        feature_key=feature_key,
        target_task=target_task,
        season_label=season_label,
        source_name=source_name,
        refresh_freshness_status=refresh_freshness_status,
        refresh_pending_only=refresh_pending_only,
        scoring_freshness_status=scoring_freshness_status,
        scoring_pending_only=scoring_pending_only,
        recent_limit=recent_limit,
    )
    try:
        result = model_market_board_orchestration.orchestrate_model_market_board_cadence(
            feature_key=feature_key,
            target_task=target_task,
            season_label=season_label,
            source_name=source_name,
            refresh_freshness_status=refresh_freshness_status,
            refresh_pending_only=refresh_pending_only,
            scoring_freshness_status=scoring_freshness_status,
            scoring_pending_only=scoring_pending_only,
            include_evidence=include_evidence,
            evidence_dimensions=evidence_dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
            recent_limit=recent_limit,
            refresh_orchestrator=lambda **kwargs: orchestrate_model_market_board_refresh_postgres(
                connection,
                **kwargs,
            ),
            scoring_orchestrator=lambda **kwargs: orchestrate_model_market_board_scoring_postgres(
                connection,
                **kwargs,
            ),
            build_batch=_build_model_market_board_cadence_batch,
            save_batch=lambda batch: save_model_market_board_cadence_batch_postgres(
                connection, batch
            ),
            serialize_batch=_serialize_model_market_board_cadence_batch,
        )
    except Exception as exc:
        span.failure(exc)
        raise
    cadence_batch = result.get("cadence_batch") or {}
    span.success(
        refreshed_board_count=int(result.get("refreshed_board_count", 0)),
        scored_board_count=int(result.get("scored_board_count", 0)),
        materialized_scoring_run_count=int(result.get("materialized_scoring_run_count", 0)),
        materialized_opportunity_count=int(result.get("materialized_opportunity_count", 0)),
        cadence_batch_id=cadence_batch.get("id"),
    )
    return result


def get_model_market_board_refresh_batch_history_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    batches = list_model_market_board_refresh_batches_postgres(
        connection,
        target_task=target_task,
        source_name=source_name,
    )
    return _summarize_model_market_board_refresh_batch_history(
        batches,
        recent_limit=recent_limit,
    )


def get_model_market_board_scoring_batch_history_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    batches = list_model_market_board_scoring_batches_postgres(
        connection,
        target_task=target_task,
        source_name=source_name,
    )
    return _summarize_model_market_board_scoring_batch_history(
        batches,
        recent_limit=recent_limit,
    )


def get_model_market_board_cadence_batch_history_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    batches = list_model_market_board_cadence_batches_postgres(
        connection,
        target_task=target_task,
        source_name=source_name,
    )
    return _summarize_model_market_board_cadence_batch_history(
        batches,
        recent_limit=recent_limit,
    )


def get_model_market_board_operations_postgres(
    connection: Any,
    *,
    board_id: int,
    recent_limit: int = 5,
) -> dict[str, Any] | None:
    return model_market_board_orchestration.get_model_market_board_operations(
        board_id=board_id,
        list_boards=lambda: list_model_market_boards_postgres(connection),
        list_source_runs=lambda **kwargs: list_model_market_board_source_runs_postgres(
            connection,
            **kwargs,
        ),
        list_refresh_events=lambda **kwargs: list_model_market_board_refresh_events_postgres(
            connection,
            **kwargs,
        ),
        list_scoring_runs=lambda **kwargs: list_model_scoring_runs_postgres(connection, **kwargs),
        list_opportunities=lambda **kwargs: list_model_opportunities_postgres(
            connection,
            **kwargs,
        ),
        list_refresh_batches=lambda: list_model_market_board_refresh_batches_postgres(connection),
        list_cadence_batches=lambda: list_model_market_board_cadence_batches_postgres(connection),
        list_scoring_batches=lambda: list_model_market_board_scoring_batches_postgres(connection),
        recent_limit=recent_limit,
    )


def get_model_market_board_cadence_dashboard_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    season_label: str | None = None,
    source_name: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    return model_market_board_orchestration.get_model_market_board_cadence_dashboard(
        target_task=target_task,
        season_label=season_label,
        source_name=source_name,
        recent_limit=recent_limit,
        list_boards=lambda **kwargs: list_model_market_boards_postgres(connection, **kwargs),
        list_scoring_runs=lambda **kwargs: list_model_scoring_runs_postgres(connection, **kwargs),
        list_scoring_batches=lambda **kwargs: list_model_market_board_scoring_batches_postgres(
            connection,
            **kwargs,
        ),
    )


def score_model_market_board_postgres(
    connection: Any,
    *,
    board_id: int,
    feature_key: str = DEFAULT_FEATURE_KEY,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    return model_market_board_orchestration.score_model_market_board(
        board_id=board_id,
        feature_key=feature_key,
        include_evidence=include_evidence,
        evidence_dimensions=evidence_dimensions,
        comparable_limit=comparable_limit,
        min_pattern_sample_size=min_pattern_sample_size,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
        get_board_detail=lambda current_board_id: next(
            (
                entry
                for entry in list_model_market_boards_postgres(connection)
                if entry.id == current_board_id
            ),
            None,
        ),
        materialize_future_slate=lambda **kwargs: materialize_model_future_slate_postgres(
            connection,
            **kwargs,
        ),
        serialize_market_board=_serialize_model_market_board,
    )


def list_model_opportunities_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    status: str | None = None,
    season_label: str | None = None,
    source_kind: str | None = None,
    scenario_key: str | None = None,
    materialization_batch_id: str | None = None,
    latest_batch_only: bool = False,
) -> list[ModelOpportunityRecord]:
    return model_opportunities.list_model_opportunities_postgres(
        connection,
        target_task=target_task,
        team_code=team_code,
        status=status,
        season_label=season_label,
        source_kind=source_kind,
        scenario_key=scenario_key,
        materialization_batch_id=materialization_batch_id,
        latest_batch_only=latest_batch_only,
    )


def get_model_opportunity_queue_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    status: str | None = None,
    season_label: str | None = None,
    source_kind: str | None = None,
    scenario_key: str | None = None,
) -> dict[str, Any]:
    return model_opportunities.get_model_opportunity_queue_postgres(
        connection,
        target_task=target_task,
        team_code=team_code,
        status=status,
        season_label=season_label,
        source_kind=source_kind,
        scenario_key=scenario_key,
    )


def get_model_opportunity_detail_postgres(
    connection: Any,
    *,
    opportunity_id: int,
) -> dict[str, Any] | None:
    return model_opportunities.get_model_opportunity_detail_postgres(
        connection,
        opportunity_id=opportunity_id,
    )


def get_model_opportunity_history_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    source_kind: str | None = None,
    scenario_key: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    return model_opportunities.get_model_opportunity_history_postgres(
        connection,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        source_kind=source_kind,
        scenario_key=scenario_key,
        recent_limit=recent_limit,
    )


def run_model_backtest_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    team_code: str | None = None,
    season_label: str | None = None,
    selection_policy_name: str = DEFAULT_REGRESSION_SELECTION_POLICY_NAME,
    minimum_train_games: int = 1,
    test_window_games: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
) -> dict[str, Any]:
    span = start_workflow_span(
        workflow_name="model_backtest.run",
        storage_mode="postgres",
        feature_key=feature_key,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        selection_policy_name=selection_policy_name,
        minimum_train_games=minimum_train_games,
        test_window_games=test_window_games,
    )
    try:
        feature_version = get_feature_version_postgres(connection, feature_key=feature_key)
        if feature_version is None:
            result = {
                "feature_version": None,
                "backtest_run": None,
                "summary": model_backtest_workflows.empty_backtest_summary(
                    target_task=target_task,
                    selection_policy_name=selection_policy_name,
                    strategy_name=model_backtest_workflows.backtest_strategy_name(target_task),
                    minimum_train_games=minimum_train_games,
                    test_window_games=test_window_games,
                ),
            }
            span.success(
                feature_version_id=None,
                backtest_run_id=None,
                dataset_row_count=0,
                dataset_game_count=0,
                fold_count=int(result["summary"]["fold_count"]),
                candidate_bet_count=int(
                    result["summary"]["strategy_results"]["candidate_threshold"]["bet_count"]
                ),
            )
            return result
        dataset_rows = _load_training_dataset_rows_postgres(
            connection,
            feature_version_id=feature_version.id,
            team_code=team_code,
            season_label=season_label,
        )
        workflow_result = _run_walk_forward_backtest(
            dataset_rows=dataset_rows,
            feature_version=feature_version,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            selection_policy_name=selection_policy_name,
            minimum_train_games=minimum_train_games,
            test_window_games=test_window_games,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
        backtest_run = model_backtest_runs.save_model_backtest_run_postgres(
            connection,
            workflow_result["record"],
        )
        result = {
            "feature_version": asdict(feature_version),
            "backtest_run": _serialize_model_backtest_run(backtest_run),
            "summary": workflow_result["summary"],
        }
    except Exception as exc:
        span.failure(exc)
        raise
    span.success(
        feature_version_id=feature_version.id,
        backtest_run_id=backtest_run.id,
        dataset_row_count=int(result["summary"]["dataset_row_count"]),
        dataset_game_count=int(result["summary"]["dataset_game_count"]),
        fold_count=int(result["summary"]["fold_count"]),
        candidate_bet_count=int(
            result["summary"]["strategy_results"]["candidate_threshold"]["bet_count"]
        ),
    )
    return result


def list_model_backtest_runs_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
) -> list[ModelBacktestRunRecord]:
    return model_backtest_runs.list_model_backtest_runs_postgres(
        connection,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
    )


def get_model_backtest_history_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    return model_backtest_runs.get_model_backtest_history_postgres(
        connection,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        recent_limit=recent_limit,
        nested_get=model_opportunities.nested_get,
    )


def get_model_backtest_detail_postgres(
    connection: Any,
    *,
    backtest_run_id: int,
) -> dict[str, Any] | None:
    return model_backtest_runs.get_model_backtest_detail_postgres(
        connection,
        backtest_run_id=backtest_run_id,
    )


def _train_phase_three_models(
    *,
    dataset_rows: list[dict[str, Any]],
    feature_version: FeatureVersionRecord,
    target_task: str,
    team_code: str | None,
    season_label: str | None,
    train_ratio: float,
    validation_ratio: float,
    ensure_registry,
    save_run,
    list_runs,
) -> dict[str, Any]:
    if target_task not in SUPPORTED_MODEL_TARGET_TASKS:
        raise ValueError(
            f"Phase 3 model training currently supports regression targets only: {target_task}"
        )
    split_rows = _partition_feature_dataset_rows(
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
    split_summary = {
        split_name: {
            "row_count": len(rows),
            "game_count": len({row["canonical_game_id"] for row in rows}),
        }
        for split_name, rows in split_training_rows.items()
    }
    split_target_summary = {
        split_name: _summarize_target_values(rows)
        for split_name, rows in split_training_rows.items()
    }
    candidate_runs: list[ModelTrainingRunRecord] = []
    for model_family, trainer in (
        ("linear_feature", _train_linear_feature_model),
        ("tree_stump", _train_tree_stump_model),
    ):
        registry = ensure_registry(model_family)
        model_artifact = trainer(
            train_rows=split_training_rows["train"],
            validation_rows=split_training_rows["validation"],
            test_rows=split_training_rows["test"],
        )
        model_artifact["artifact"]["split_summary"] = split_summary
        model_artifact["artifact"]["split_target_summary"] = split_target_summary
        run = save_run(
            ModelTrainingRunRecord(
                id=0,
                model_registry_id=registry.id,
                feature_version_id=feature_version.id,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                status="COMPLETED",
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                artifact=model_artifact["artifact"],
                metrics=model_artifact["metrics"],
            )
        )
        candidate_runs.append(run)

    ranked_runs = sorted(
        candidate_runs,
        key=lambda run: (
            _metric_value_or_inf(run.metrics.get("validation", {}).get("mae")),
            -int(run.metrics.get("validation", {}).get("prediction_count", 0)),
        ),
    )
    return {
        "feature_version": asdict(feature_version),
        "dataset_row_count": len(dataset_rows),
        "model_runs": [asdict(run) for run in ranked_runs],
        "best_model": asdict(ranked_runs[0]) if ranked_runs else None,
        "persisted_run_count": len(list_runs()),
    }


def _run_walk_forward_backtest(
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
) -> dict[str, Any]:
    return model_backtest_workflows.run_walk_forward_backtest(
        dataset_rows=dataset_rows,
        feature_version=feature_version,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        selection_policy_name=selection_policy_name,
        minimum_train_games=minimum_train_games,
        test_window_games=test_window_games,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        opportunity_policy_configs=OPPORTUNITY_POLICY_CONFIGS,
        partition_feature_dataset_rows=_partition_feature_dataset_rows,
        build_feature_training_view=build_feature_training_view,
        train_linear_feature_model=_train_linear_feature_model,
        train_tree_stump_model=_train_tree_stump_model,
        select_best_evaluation_snapshot=_select_best_evaluation_snapshot,
        score_dataset_rows_with_active_selection=lambda dataset_rows, **kwargs: (
            model_scoring_previews.score_dataset_rows_with_active_selection(
                dataset_rows,
                **kwargs,
                predict_linear=_predict_linear,
                predict_tree_stump=_predict_tree_stump,
                get_row_feature_value=_get_row_feature_value,
            )
        ),
    )


def _ordered_dataset_game_ids(dataset_rows: list[dict[str, Any]]) -> list[int]:
    return model_backtest_workflows.ordered_dataset_game_ids(dataset_rows)


def _train_walk_forward_snapshot(
    *,
    dataset_rows: list[dict[str, Any]],
    feature_version: FeatureVersionRecord,
    target_task: str,
    selection_policy_name: str,
    train_ratio: float,
    validation_ratio: float,
) -> ModelEvaluationSnapshotRecord | None:
    return model_backtest_workflows.train_walk_forward_snapshot(
        dataset_rows=dataset_rows,
        feature_version=feature_version,
        target_task=target_task,
        selection_policy_name=selection_policy_name,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        partition_feature_dataset_rows=_partition_feature_dataset_rows,
        build_feature_training_view=build_feature_training_view,
        train_linear_feature_model=_train_linear_feature_model,
        train_tree_stump_model=_train_tree_stump_model,
        select_best_evaluation_snapshot=_select_best_evaluation_snapshot,
    )


def _build_backtest_fold_summary(
    *,
    fold_index: int,
    target_task: str,
    train_game_ids: list[int],
    test_game_ids: list[int],
    selected_snapshot: ModelEvaluationSnapshotRecord,
    predictions: list[dict[str, Any]],
) -> dict[str, Any]:
    return model_backtest_workflows.build_backtest_fold_summary(
        fold_index=fold_index,
        target_task=target_task,
        train_game_ids=train_game_ids,
        test_game_ids=test_game_ids,
        selected_snapshot=selected_snapshot,
        predictions=predictions,
        opportunity_policy_configs=OPPORTUNITY_POLICY_CONFIGS,
    )


def _summarize_walk_forward_backtest(
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
    return model_backtest_workflows.summarize_walk_forward_backtest(
        target_task=target_task,
        selection_policy_name=selection_policy_name,
        minimum_train_games=minimum_train_games,
        test_window_games=test_window_games,
        dataset_row_count=dataset_row_count,
        dataset_game_count=dataset_game_count,
        fold_summaries=fold_summaries,
        predictions=predictions,
    )


def _empty_backtest_summary(
    *,
    target_task: str,
    selection_policy_name: str,
    strategy_name: str,
    minimum_train_games: int,
    test_window_games: int,
) -> dict[str, Any]:
    return model_backtest_workflows.empty_backtest_summary(
        target_task=target_task,
        selection_policy_name=selection_policy_name,
        strategy_name=strategy_name,
        minimum_train_games=minimum_train_games,
        test_window_games=test_window_games,
    )


def _backtest_strategy_name(target_task: str) -> str:
    return model_backtest_workflows.backtest_strategy_name(target_task)


def _summarize_backtest_prediction_metrics(predictions: list[dict[str, Any]]) -> dict[str, Any]:
    return model_backtest_workflows.summarize_backtest_prediction_metrics(predictions)


def _evaluate_backtest_strategy(
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
        bet = _build_backtest_bet(
            prediction=prediction,
            target_task=target_task,
            strategy_name=strategy_name,
            threshold=threshold,
        )
        if bet is not None:
            bets.append(bet)
    summary = _summarize_backtest_bets(bets, strategy_name=strategy_name)
    summary["threshold"] = threshold
    summary["bets"] = bets
    return summary


def _build_backtest_bet(
    *,
    prediction: dict[str, Any],
    target_task: str,
    strategy_name: str,
    threshold: float,
) -> dict[str, Any] | None:
    return model_backtest_workflows.build_backtest_bet(
        prediction=prediction,
        target_task=target_task,
        strategy_name=strategy_name,
        threshold=threshold,
        float_or_none=_float_or_none,
    )


def _backtest_edge_bucket(signal_strength: float) -> str:
    return model_backtest_workflows.backtest_edge_bucket(signal_strength)


def _summarize_backtest_bets(
    bets: list[dict[str, Any]],
    *,
    strategy_name: str,
) -> dict[str, Any]:
    return model_backtest_workflows.summarize_backtest_bets(
        bets,
        strategy_name=strategy_name,
    )


def _train_linear_feature_model(
    *,
    train_rows: list[dict[str, Any]],
    validation_rows: list[dict[str, Any]],
    test_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return model_training_algorithms.train_linear_feature_model(
        train_rows=train_rows,
        validation_rows=validation_rows,
        test_rows=test_rows,
    )


def _train_tree_stump_model(
    *,
    train_rows: list[dict[str, Any]],
    validation_rows: list[dict[str, Any]],
    test_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return model_training_algorithms.train_tree_stump_model(
        train_rows=train_rows,
        validation_rows=validation_rows,
        test_rows=test_rows,
    )


def _load_training_dataset_rows_postgres(
    connection: Any,
    *,
    feature_version_id: int,
    team_code: str | None,
    season_label: str | None,
) -> list[dict[str, Any]]:
    snapshots = list_feature_snapshots_postgres(
        connection,
        feature_version_id=feature_version_id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    canonical_games = list_canonical_game_metric_records_postgres(connection)
    return build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code=team_code,
    )


def _numeric_feature_candidates(training_rows: list[dict[str, Any]]) -> list[str]:
    return model_training_algorithms.numeric_feature_candidates(training_rows)


def _training_pairs(
    training_rows: list[dict[str, Any]],
    feature_name: str,
) -> list[tuple[float, float]]:
    return model_training_algorithms.training_pairs(training_rows, feature_name)


def _fit_simple_linear_regression(
    pairs: list[tuple[float, float]],
) -> tuple[float, float]:
    return model_training_algorithms.fit_simple_linear_regression(pairs)


def _candidate_tree_thresholds(
    pairs: list[tuple[float, float]],
) -> list[float]:
    return model_training_algorithms.candidate_tree_thresholds(pairs)


def _constant_target_mean(
    training_rows: list[dict[str, Any]],
) -> float | None:
    return model_training_algorithms.constant_target_mean(training_rows)


def _summarize_target_values(training_rows: list[dict[str, Any]]) -> dict[str, Any]:
    return model_training_algorithms.summarize_target_values(training_rows)


def _predict_linear(
    row: dict[str, Any],
    feature_name: str | None,
    intercept: float,
    coefficient: float,
) -> float | None:
    return model_training_algorithms.predict_linear(
        row,
        feature_name,
        intercept,
        coefficient,
    )


def _predict_tree_stump(
    row: dict[str, Any],
    feature_name: str | None,
    threshold: float | None,
    left_prediction: float | None,
    right_prediction: float | None,
) -> float | None:
    return model_training_algorithms.predict_tree_stump(
        row,
        feature_name,
        threshold,
        left_prediction,
        right_prediction,
    )


def _get_row_feature_value(
    row: dict[str, Any],
    feature_name: str,
) -> Any:
    return model_training_algorithms.get_row_feature_value(row, feature_name)


def _score_regression_model(training_rows: list[dict[str, Any]], *, predictor) -> dict[str, Any]:
    return model_training_algorithms.score_regression_model(
        training_rows,
        predictor=predictor,
    )


def _is_better_regression_candidate(
    candidate_metrics: dict[str, Any],
    incumbent_metrics: dict[str, Any],
) -> bool:
    return model_training_algorithms.is_better_regression_candidate(
        candidate_metrics,
        incumbent_metrics,
    )


def _metric_value_or_inf(value: Any) -> float:
    return model_training_views._metric_value_or_inf(value)


def save_model_opportunities_postgres(
    connection: Any,
    opportunities: list[ModelOpportunityRecord],
) -> list[ModelOpportunityRecord]:
    return model_opportunities.save_model_opportunities_postgres(connection, opportunities)


def save_model_backtest_run_postgres(
    connection: Any,
    backtest_run: ModelBacktestRunRecord,
) -> ModelBacktestRunRecord:
    return model_backtest_runs.save_model_backtest_run_postgres(connection, backtest_run)


def _serialize_model_backtest_run(
    backtest_run: ModelBacktestRunRecord | None,
) -> dict[str, Any] | None:
    return model_backtest_runs.serialize_model_backtest_run(backtest_run)


def _summarize_model_scoring_history(
    scoring_runs: list[ModelScoringRunRecord],
    *,
    recent_limit: int,
) -> dict[str, Any]:
    return model_scoring_runs._summarize_model_scoring_history(
        scoring_runs,
        recent_limit=recent_limit,
    )


def get_model_selection_history_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    return model_training_views.get_model_selection_history_postgres(
        connection,
        target_task=target_task,
        recent_limit=recent_limit,
    )


def save_model_evaluation_snapshot_postgres(
    connection: Any,
    run: ModelTrainingRunRecord,
) -> ModelEvaluationSnapshotRecord:
    return model_training_lifecycle.save_model_evaluation_snapshot_postgres(connection, run)


def save_model_selection_snapshot_postgres(
    connection: Any,
    snapshot: ModelEvaluationSnapshotRecord,
    *,
    selection_policy_name: str,
) -> ModelSelectionSnapshotRecord:
    return model_training_lifecycle.save_model_selection_snapshot_postgres(
        connection,
        snapshot,
        selection_policy_name=selection_policy_name,
    )


def _float_or_none(value: Any) -> float | None:
    return model_training_views._float_or_none(value)


def _select_best_evaluation_snapshot(
    snapshots: list[ModelEvaluationSnapshotRecord],
    *,
    selection_policy_name: str,
) -> ModelEvaluationSnapshotRecord | None:
    return model_training_views._select_best_evaluation_snapshot(
        snapshots,
        selection_policy_name=selection_policy_name,
    )
