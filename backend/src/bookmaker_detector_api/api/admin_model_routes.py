from typing import Annotated

from fastapi import APIRouter, Depends, Query

from bookmaker_detector_api.api.schemas import (
    AdminBacktestHistoryFilters,
    AdminBacktestHistoryResponse,
    AdminEvaluationDetailResponse,
    AdminEvaluationHistoryFilters,
    AdminEvaluationHistoryResponse,
    AdminEvaluationSnapshot,
    AdminModelEvaluationsFilters,
    AdminModelEvaluationsResponse,
    AdminModelHistoryFilters,
    AdminModelHistoryResponse,
    AdminModelRegistryEntry,
    AdminModelRegistryFilters,
    AdminModelRegistryResponse,
    AdminModelRun,
    AdminModelRunDetailResponse,
    AdminModelRunsFilters,
    AdminModelRunsResponse,
    AdminModelSelectionsFilters,
    AdminModelSelectionsResponse,
    AdminModelSummaryFilters,
    AdminModelSummaryResponse,
    AdminSelectionDetailResponse,
    AdminSelectionHistoryFilters,
    AdminSelectionHistoryResponse,
    AdminSelectionSnapshot,
)
from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.services.models import (
    get_model_backtest_history_in_memory,
    get_model_backtest_history_postgres,
    get_model_evaluation_history_in_memory,
    get_model_evaluation_history_postgres,
    get_model_evaluation_snapshot_detail_in_memory,
    get_model_evaluation_snapshot_detail_postgres,
    get_model_selection_history_in_memory,
    get_model_selection_history_postgres,
    get_model_selection_snapshot_detail_in_memory,
    get_model_selection_snapshot_detail_postgres,
    get_model_training_history_in_memory,
    get_model_training_history_postgres,
    get_model_training_run_detail_in_memory,
    get_model_training_run_detail_postgres,
    get_model_training_summary_in_memory,
    get_model_training_summary_postgres,
    list_model_evaluation_snapshots_in_memory,
    list_model_evaluation_snapshots_postgres,
    list_model_registry_in_memory,
    list_model_registry_postgres,
    list_model_selection_snapshots_in_memory,
    list_model_selection_snapshots_postgres,
    list_model_training_runs_in_memory,
    list_model_training_runs_postgres,
    promote_best_model_in_memory,
    promote_best_model_postgres,
    run_model_backtest_in_memory,
    run_model_backtest_postgres,
    train_phase_three_models_in_memory,
    train_phase_three_models_postgres,
)
from bookmaker_detector_api.services.repository_factory import (
    build_in_memory_phase_three_modeling_store,
)

from .admin_model_support import (
    _prepare_in_memory_phase_three_model_repository,
    _use_postgres_stable_read_mode,
)

router = APIRouter(prefix="/admin", tags=["admin"])


def _serialize_model_registry_entry(entry) -> AdminModelRegistryEntry:
    return AdminModelRegistryEntry(
        id=entry.id,
        model_key=entry.model_key,
        target_task=entry.target_task,
        model_family=entry.model_family,
        version_label=entry.version_label,
        description=entry.description,
        config=entry.config,
        created_at=entry.created_at.isoformat() if entry.created_at else None,
    )


def _serialize_model_run(run) -> AdminModelRun:
    return AdminModelRun(
        id=run.id,
        model_registry_id=run.model_registry_id,
        feature_version_id=run.feature_version_id,
        target_task=run.target_task,
        team_code=run.team_code,
        season_label=run.season_label,
        status=run.status,
        train_ratio=run.train_ratio,
        validation_ratio=run.validation_ratio,
        artifact=run.artifact,
        metrics=run.metrics,
        created_at=run.created_at.isoformat() if run.created_at else None,
        completed_at=run.completed_at.isoformat() if run.completed_at else None,
    )


def _serialize_evaluation_snapshot(snapshot) -> AdminEvaluationSnapshot:
    return AdminEvaluationSnapshot(
        id=snapshot.id,
        model_training_run_id=snapshot.model_training_run_id,
        model_registry_id=snapshot.model_registry_id,
        feature_version_id=snapshot.feature_version_id,
        target_task=snapshot.target_task,
        model_family=snapshot.model_family,
        selected_feature=snapshot.selected_feature,
        fallback_strategy=snapshot.fallback_strategy,
        primary_metric_name=snapshot.primary_metric_name,
        validation_metric_value=snapshot.validation_metric_value,
        test_metric_value=snapshot.test_metric_value,
        validation_prediction_count=snapshot.validation_prediction_count,
        test_prediction_count=snapshot.test_prediction_count,
        snapshot=snapshot.snapshot,
        created_at=snapshot.created_at.isoformat() if snapshot.created_at else None,
    )


def _serialize_selection_snapshot(selection) -> AdminSelectionSnapshot:
    return AdminSelectionSnapshot(
        id=selection.id,
        model_evaluation_snapshot_id=selection.model_evaluation_snapshot_id,
        model_training_run_id=selection.model_training_run_id,
        model_registry_id=selection.model_registry_id,
        feature_version_id=selection.feature_version_id,
        target_task=selection.target_task,
        model_family=selection.model_family,
        selection_policy_name=selection.selection_policy_name,
        rationale=selection.rationale,
        is_active=selection.is_active,
        created_at=selection.created_at.isoformat() if selection.created_at else None,
    )


@router.post("/models/train")
def phase_three_model_train(
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            training_result = train_phase_three_models_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_phase_three_model_repository(
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
        training_result = train_phase_three_models_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
        },
        **training_result,
    }


@router.post("/models/backtests/run")
def phase_four_model_backtest_run(
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    selection_policy_name: str = Query(default="validation_mae_candidate_v1"),
    minimum_train_games: int = Query(default=1, ge=1),
    test_window_games: int = Query(default=1, ge=1),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            result = run_model_backtest_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                selection_policy_name=selection_policy_name,
                minimum_train_games=minimum_train_games,
                test_window_games=test_window_games,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_phase_three_model_repository(
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
        result = run_model_backtest_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            selection_policy_name=selection_policy_name,
            minimum_train_games=minimum_train_games,
            test_window_games=test_window_games,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "selection_policy_name": selection_policy_name,
            "minimum_train_games": minimum_train_games,
            "test_window_games": test_window_games,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
        },
        **result,
    }


@router.get("/models/backtests/history")
def phase_four_model_backtest_history(
    filters: Annotated[AdminBacktestHistoryFilters, Depends()],
) -> AdminBacktestHistoryResponse:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            history = get_model_backtest_history_postgres(
                connection,
                target_task=filters.target_task,
                team_code=filters.team_code,
                season_label=filters.season_label,
                recent_limit=filters.recent_limit,
            )
        repository_mode = "postgres"
    else:
        repository = build_in_memory_phase_three_modeling_store()
        history = get_model_backtest_history_in_memory(
            repository,
            target_task=filters.target_task,
            team_code=filters.team_code,
            season_label=filters.season_label,
            recent_limit=filters.recent_limit,
        )
        repository_mode = "in_memory"

    return AdminBacktestHistoryResponse(
        repository_mode=repository_mode,
        filters=filters,
        model_backtest_history=history,
    )


@router.get("/models/registry")
def phase_three_model_registry(
    filters: Annotated[AdminModelRegistryFilters, Depends()],
) -> AdminModelRegistryResponse:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            registries = list_model_registry_postgres(
                connection,
                target_task=filters.target_task,
            )
        repository_mode = "postgres"
    else:
        repository = build_in_memory_phase_three_modeling_store()
        registries = list_model_registry_in_memory(
            repository,
            target_task=filters.target_task,
        )
        repository_mode = "in_memory"

    return AdminModelRegistryResponse(
        repository_mode=repository_mode,
        filters=filters,
        model_registry_count=len(registries),
        model_registry=[_serialize_model_registry_entry(entry) for entry in registries],
    )


@router.get("/models/runs")
def phase_three_model_runs(
    filters: Annotated[AdminModelRunsFilters, Depends()],
) -> AdminModelRunsResponse:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            runs = list_model_training_runs_postgres(
                connection,
                target_task=filters.target_task,
                team_code=filters.team_code,
                season_label=filters.season_label,
            )
        repository_mode = "postgres"
    else:
        repository = build_in_memory_phase_three_modeling_store()
        runs = list_model_training_runs_in_memory(
            repository,
            target_task=filters.target_task,
            team_code=filters.team_code,
            season_label=filters.season_label,
        )
        repository_mode = "in_memory"

    return AdminModelRunsResponse(
        repository_mode=repository_mode,
        filters=filters,
        model_run_count=len(runs),
        model_runs=[_serialize_model_run(run) for run in runs],
    )


@router.get("/models/runs/{run_id}")
def phase_three_model_run_detail(
    run_id: int,
) -> AdminModelRunDetailResponse:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            run = get_model_training_run_detail_postgres(connection, run_id=run_id)
        repository_mode = "postgres"
    else:
        repository = build_in_memory_phase_three_modeling_store()
        run = get_model_training_run_detail_in_memory(repository, run_id=run_id)
        repository_mode = "in_memory"

    return AdminModelRunDetailResponse(
        repository_mode=repository_mode,
        model_run=_serialize_model_run(run) if run is not None else None,
    )


@router.get("/models/summary")
def phase_three_model_summary(
    filters: Annotated[AdminModelSummaryFilters, Depends()],
) -> AdminModelSummaryResponse:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            summary = get_model_training_summary_postgres(
                connection,
                target_task=filters.target_task,
                team_code=filters.team_code,
                season_label=filters.season_label,
            )
        repository_mode = "postgres"
    else:
        repository = build_in_memory_phase_three_modeling_store()
        summary = get_model_training_summary_in_memory(
            repository,
            target_task=filters.target_task,
            team_code=filters.team_code,
            season_label=filters.season_label,
        )
        repository_mode = "in_memory"

    return AdminModelSummaryResponse(
        repository_mode=repository_mode,
        filters=filters,
        model_summary=summary,
    )


@router.get("/models/history")
def phase_three_model_history(
    filters: Annotated[AdminModelHistoryFilters, Depends()],
) -> AdminModelHistoryResponse:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            history = get_model_training_history_postgres(
                connection,
                target_task=filters.target_task,
                team_code=filters.team_code,
                season_label=filters.season_label,
                recent_limit=filters.recent_limit,
            )
        repository_mode = "postgres"
    else:
        repository = build_in_memory_phase_three_modeling_store()
        history = get_model_training_history_in_memory(
            repository,
            target_task=filters.target_task,
            team_code=filters.team_code,
            season_label=filters.season_label,
            recent_limit=filters.recent_limit,
        )
        repository_mode = "in_memory"

    return AdminModelHistoryResponse(
        repository_mode=repository_mode,
        filters=filters,
        model_history=history,
    )


@router.get("/models/evaluations")
def phase_three_model_evaluations(
    filters: Annotated[AdminModelEvaluationsFilters, Depends()],
) -> AdminModelEvaluationsResponse:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            snapshots = list_model_evaluation_snapshots_postgres(
                connection,
                target_task=filters.target_task,
                model_family=filters.model_family,
            )
        repository_mode = "postgres"
    else:
        repository = build_in_memory_phase_three_modeling_store()
        snapshots = list_model_evaluation_snapshots_in_memory(
            repository,
            target_task=filters.target_task,
            model_family=filters.model_family,
        )
        repository_mode = "in_memory"

    return AdminModelEvaluationsResponse(
        repository_mode=repository_mode,
        filters=filters,
        evaluation_snapshot_count=len(snapshots),
        evaluation_snapshots=[_serialize_evaluation_snapshot(snapshot) for snapshot in snapshots],
    )


@router.get("/models/evaluations/history")
def phase_three_model_evaluation_history(
    filters: Annotated[AdminEvaluationHistoryFilters, Depends()],
) -> AdminEvaluationHistoryResponse:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            history = get_model_evaluation_history_postgres(
                connection,
                target_task=filters.target_task,
                model_family=filters.model_family,
                recent_limit=filters.recent_limit,
            )
        repository_mode = "postgres"
    else:
        repository = build_in_memory_phase_three_modeling_store()
        history = get_model_evaluation_history_in_memory(
            repository,
            target_task=filters.target_task,
            model_family=filters.model_family,
            recent_limit=filters.recent_limit,
        )
        repository_mode = "in_memory"

    return AdminEvaluationHistoryResponse(
        repository_mode=repository_mode,
        filters=filters,
        model_evaluation_history=history,
    )


@router.get("/models/evaluations/{snapshot_id}")
def phase_three_model_evaluation_detail(
    snapshot_id: int,
) -> AdminEvaluationDetailResponse:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            snapshot = get_model_evaluation_snapshot_detail_postgres(
                connection,
                snapshot_id=snapshot_id,
            )
        repository_mode = "postgres"
    else:
        repository = build_in_memory_phase_three_modeling_store()
        snapshot = get_model_evaluation_snapshot_detail_in_memory(
            repository,
            snapshot_id=snapshot_id,
        )
        repository_mode = "in_memory"

    return AdminEvaluationDetailResponse(
        repository_mode=repository_mode,
        evaluation_snapshot=(
            _serialize_evaluation_snapshot(snapshot) if snapshot is not None else None
        ),
    )


@router.post("/models/select")
def phase_three_model_select(
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    selection_policy_name: str = Query(default="validation_mae_candidate_v1"),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            selection_result = promote_best_model_postgres(
                connection,
                target_task=target_task,
                selection_policy_name=selection_policy_name,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_phase_three_model_repository(
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
        selection_result = promote_best_model_in_memory(
            repository,
            target_task=target_task,
            selection_policy_name=selection_policy_name,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "selection_policy_name": selection_policy_name,
        },
        **selection_result,
    }


@router.get("/models/selections")
def phase_three_model_selections(
    filters: Annotated[AdminModelSelectionsFilters, Depends()],
) -> AdminModelSelectionsResponse:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            selections = list_model_selection_snapshots_postgres(
                connection,
                target_task=filters.target_task,
                active_only=filters.active_only,
            )
        repository_mode = "postgres"
    else:
        repository = build_in_memory_phase_three_modeling_store()
        selections = list_model_selection_snapshots_in_memory(
            repository,
            target_task=filters.target_task,
            active_only=filters.active_only,
        )
        repository_mode = "in_memory"

    return AdminModelSelectionsResponse(
        repository_mode=repository_mode,
        filters=filters,
        selection_count=len(selections),
        selections=[_serialize_selection_snapshot(selection) for selection in selections],
    )


@router.get("/models/selections/history")
def phase_three_model_selection_history(
    filters: Annotated[AdminSelectionHistoryFilters, Depends()],
) -> AdminSelectionHistoryResponse:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            history = get_model_selection_history_postgres(
                connection,
                target_task=filters.target_task,
                recent_limit=filters.recent_limit,
            )
        repository_mode = "postgres"
    else:
        repository = build_in_memory_phase_three_modeling_store()
        history = get_model_selection_history_in_memory(
            repository,
            target_task=filters.target_task,
            recent_limit=filters.recent_limit,
        )
        repository_mode = "in_memory"

    return AdminSelectionHistoryResponse(
        repository_mode=repository_mode,
        filters=filters,
        model_selection_history=history,
    )


@router.get("/models/selections/{selection_id}")
def phase_three_model_selection_detail(
    selection_id: int,
) -> AdminSelectionDetailResponse:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            selection = get_model_selection_snapshot_detail_postgres(
                connection,
                selection_id=selection_id,
            )
        repository_mode = "postgres"
    else:
        repository = build_in_memory_phase_three_modeling_store()
        selection = get_model_selection_snapshot_detail_in_memory(
            repository,
            selection_id=selection_id,
        )
        repository_mode = "in_memory"

    return AdminSelectionDetailResponse(
        repository_mode=repository_mode,
        selection=(_serialize_selection_snapshot(selection) if selection is not None else None),
    )
