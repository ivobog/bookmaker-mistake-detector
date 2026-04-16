from datetime import date

from fastapi import APIRouter, Body, Query
from pydantic import BaseModel, Field

from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.demo import (
    run_phase_one_demo,
    run_phase_one_fetch_demo,
    run_phase_one_fetch_failure_demo,
    run_phase_one_persistence_demo,
    run_phase_one_worker_demo,
    seed_phase_two_feature_in_memory,
    seed_phase_two_feature_postgres,
)
from bookmaker_detector_api.demo import (
    run_phase_one_fetch_reporting_demo as run_phase_one_fetch_reporting_demo_job,
)
from bookmaker_detector_api.demo import (
    run_phase_two_feature_demo as run_phase_two_feature_demo_job,
)
from bookmaker_detector_api.repositories import InMemoryIngestionRepository
from bookmaker_detector_api.services.admin_diagnostics import (
    get_admin_diagnostics,
    resolve_started_window,
)
from bookmaker_detector_api.services.data_quality_maintenance import (
    normalize_data_quality_taxonomy,
)
from bookmaker_detector_api.services.features import (
    get_feature_analysis_artifact_catalog_in_memory,
    get_feature_analysis_artifact_catalog_postgres,
    get_feature_analysis_artifact_history_in_memory,
    get_feature_analysis_artifact_history_postgres,
    get_feature_comparable_cases_in_memory,
    get_feature_comparable_cases_postgres,
    get_feature_dataset_in_memory,
    get_feature_dataset_postgres,
    get_feature_dataset_profile_in_memory,
    get_feature_dataset_profile_postgres,
    get_feature_dataset_splits_in_memory,
    get_feature_dataset_splits_postgres,
    get_feature_evidence_bundle_in_memory,
    get_feature_evidence_bundle_postgres,
    get_feature_pattern_catalog_in_memory,
    get_feature_pattern_catalog_postgres,
    get_feature_snapshot_catalog_in_memory,
    get_feature_snapshot_catalog_postgres,
    get_feature_snapshot_summary_in_memory,
    get_feature_snapshot_summary_postgres,
    get_feature_training_benchmark_in_memory,
    get_feature_training_benchmark_postgres,
    get_feature_training_bundle_in_memory,
    get_feature_training_bundle_postgres,
    get_feature_training_manifest_in_memory,
    get_feature_training_manifest_postgres,
    get_feature_training_task_matrix_in_memory,
    get_feature_training_task_matrix_postgres,
    get_feature_training_view_in_memory,
    get_feature_training_view_postgres,
    materialize_feature_analysis_artifacts_in_memory,
    materialize_feature_analysis_artifacts_postgres,
)
from bookmaker_detector_api.services.models import (
    get_model_evaluation_history_in_memory,
    get_model_evaluation_history_postgres,
    get_model_future_game_preview_in_memory,
    get_model_future_game_preview_postgres,
    get_model_future_slate_preview_in_memory,
    get_model_future_slate_preview_postgres,
    get_model_market_board_cadence_batch_history_in_memory,
    get_model_market_board_cadence_batch_history_postgres,
    get_model_market_board_cadence_dashboard_in_memory,
    get_model_market_board_cadence_dashboard_postgres,
    get_model_market_board_detail_in_memory,
    get_model_market_board_detail_postgres,
    get_model_market_board_operations_in_memory,
    get_model_market_board_operations_postgres,
    get_model_market_board_refresh_batch_history_in_memory,
    get_model_market_board_refresh_batch_history_postgres,
    get_model_market_board_refresh_history_in_memory,
    get_model_market_board_refresh_history_postgres,
    get_model_market_board_refresh_queue_in_memory,
    get_model_market_board_refresh_queue_postgres,
    get_model_market_board_scoring_batch_history_in_memory,
    get_model_market_board_scoring_batch_history_postgres,
    get_model_market_board_scoring_queue_in_memory,
    get_model_market_board_scoring_queue_postgres,
    get_model_market_board_source_run_history_in_memory,
    get_model_market_board_source_run_history_postgres,
    get_model_opportunity_detail_in_memory,
    get_model_opportunity_detail_postgres,
    get_model_opportunity_history_in_memory,
    get_model_opportunity_history_postgres,
    get_model_scoring_history_in_memory,
    get_model_scoring_history_postgres,
    get_model_scoring_preview_in_memory,
    get_model_scoring_preview_postgres,
    get_model_scoring_run_detail_in_memory,
    get_model_scoring_run_detail_postgres,
    get_model_selection_history_in_memory,
    get_model_selection_history_postgres,
    get_model_training_history_in_memory,
    get_model_training_history_postgres,
    get_model_training_summary_in_memory,
    get_model_training_summary_postgres,
    list_model_evaluation_snapshots_in_memory,
    list_model_evaluation_snapshots_postgres,
    list_model_market_board_sources,
    list_model_market_boards_in_memory,
    list_model_market_boards_postgres,
    list_model_opportunities_in_memory,
    list_model_opportunities_postgres,
    list_model_registry_in_memory,
    list_model_registry_postgres,
    list_model_scoring_runs_in_memory,
    list_model_scoring_runs_postgres,
    list_model_selection_snapshots_in_memory,
    list_model_selection_snapshots_postgres,
    list_model_training_runs_in_memory,
    list_model_training_runs_postgres,
    materialize_model_future_game_preview_in_memory,
    materialize_model_future_game_preview_postgres,
    materialize_model_future_opportunities_in_memory,
    materialize_model_future_opportunities_postgres,
    materialize_model_future_slate_in_memory,
    materialize_model_future_slate_postgres,
    materialize_model_market_board_in_memory,
    materialize_model_market_board_postgres,
    materialize_model_opportunities_in_memory,
    materialize_model_opportunities_postgres,
    orchestrate_model_market_board_cadence_in_memory,
    orchestrate_model_market_board_cadence_postgres,
    orchestrate_model_market_board_refresh_in_memory,
    orchestrate_model_market_board_refresh_postgres,
    orchestrate_model_market_board_scoring_in_memory,
    orchestrate_model_market_board_scoring_postgres,
    promote_best_model_in_memory,
    promote_best_model_postgres,
    refresh_model_market_board_in_memory,
    refresh_model_market_board_postgres,
    score_model_market_board_in_memory,
    score_model_market_board_postgres,
    train_phase_three_models_in_memory,
    train_phase_three_models_postgres,
)

router = APIRouter(prefix="/admin", tags=["admin"])


class FutureSlateGameRequest(BaseModel):
    season_label: str = Field(default="2025-2026")
    game_date: date
    home_team_code: str
    away_team_code: str
    home_spread_line: float | None = None
    total_line: float | None = None


class FutureSlateRequest(BaseModel):
    slate_label: str | None = None
    games: list[FutureSlateGameRequest] = Field(min_length=1, max_length=20)


@router.get("/providers")
def list_supported_providers() -> dict[str, list[dict[str, str]]]:
    return {
        "providers": [
            {
                "name": "covers",
                "type": "historical_team_page",
                "status": "fixture_backed",
            }
        ]
    }


@router.get("/phase-1-demo")
def phase_one_demo() -> dict[str, object]:
    return run_phase_one_demo()


@router.get("/phase-1-persistence-demo")
def phase_one_persistence_demo() -> dict[str, object]:
    return run_phase_one_persistence_demo()


@router.get("/phase-1-worker-demo")
def phase_one_worker_demo() -> dict[str, object]:
    return run_phase_one_worker_demo()


@router.get("/phase-1-fetch-demo")
def phase_one_fetch_demo() -> dict[str, object]:
    return run_phase_one_fetch_demo()


@router.get("/phase-1-fetch-failure-demo")
def phase_one_fetch_failure_demo() -> dict[str, object]:
    return run_phase_one_fetch_failure_demo()


@router.get("/phase-1-fetch-reporting-demo")
def phase_one_fetch_reporting_demo(
    repository_mode: str = Query(default="in_memory"),
) -> dict[str, object]:
    return run_phase_one_fetch_reporting_demo_job(repository_mode=repository_mode)


@router.get("/phase-2-feature-demo")
def phase_two_feature_demo(
    repository_mode: str = Query(default="in_memory"),
) -> dict[str, object]:
    return run_phase_two_feature_demo_job(repository_mode=repository_mode)


@router.post("/models/train")
def phase_three_model_train(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        training_result = train_phase_three_models_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            training_result = train_phase_three_models_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

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


@router.get("/models/registry")
def phase_three_model_registry(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_train_demo and target_task is not None:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        registries = list_model_registry_in_memory(
            repository,
            target_task=target_task,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_train_demo and target_task is not None:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=team_code,
                    season_label=season_label,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            registries = list_model_registry_postgres(
                connection,
                target_task=target_task,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "auto_train_demo": auto_train_demo,
        },
        "model_registry_count": len(registries),
        "model_registry": [
            {
                "id": entry.id,
                "model_key": entry.model_key,
                "target_task": entry.target_task,
                "model_family": entry.model_family,
                "version_label": entry.version_label,
                "description": entry.description,
                "config": entry.config,
                "created_at": entry.created_at.isoformat() if entry.created_at else None,
            }
            for entry in registries
        ],
    }


@router.get("/models/runs")
def phase_three_model_runs(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_train_demo and target_task is not None:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        runs = list_model_training_runs_in_memory(
            repository,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_train_demo and target_task is not None:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=team_code,
                    season_label=season_label,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            runs = list_model_training_runs_postgres(
                connection,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "auto_train_demo": auto_train_demo,
        },
        "model_run_count": len(runs),
        "model_runs": [
            {
                "id": run.id,
                "model_registry_id": run.model_registry_id,
                "feature_version_id": run.feature_version_id,
                "target_task": run.target_task,
                "team_code": run.team_code,
                "season_label": run.season_label,
                "status": run.status,
                "train_ratio": run.train_ratio,
                "validation_ratio": run.validation_ratio,
                "artifact": run.artifact,
                "metrics": run.metrics,
                "created_at": run.created_at.isoformat() if run.created_at else None,
                "completed_at": run.completed_at.isoformat()
                if run.completed_at
                else None,
            }
            for run in runs
        ],
    }


@router.get("/models/summary")
def phase_three_model_summary(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_train_demo and target_task is not None:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        summary = get_model_training_summary_in_memory(
            repository,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_train_demo and target_task is not None:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=team_code,
                    season_label=season_label,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            summary = get_model_training_summary_postgres(
                connection,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "auto_train_demo": auto_train_demo,
        },
        "model_summary": summary,
    }


@router.get("/models/history")
def phase_three_model_history(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_train_demo and target_task is not None:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        history = get_model_training_history_in_memory(
            repository,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            recent_limit=recent_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_train_demo and target_task is not None:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=team_code,
                    season_label=season_label,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            history = get_model_training_history_postgres(
                connection,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                recent_limit=recent_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "auto_train_demo": auto_train_demo,
            "recent_limit": recent_limit,
        },
        "model_history": history,
    }


@router.get("/models/evaluations")
def phase_three_model_evaluations(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    model_family: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_train_demo and target_task is not None:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        snapshots = list_model_evaluation_snapshots_in_memory(
            repository,
            target_task=target_task,
            model_family=model_family,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_train_demo and target_task is not None:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=team_code,
                    season_label=season_label,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            snapshots = list_model_evaluation_snapshots_postgres(
                connection,
                target_task=target_task,
                model_family=model_family,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "model_family": model_family,
            "team_code": team_code,
            "season_label": season_label,
            "auto_train_demo": auto_train_demo,
        },
        "evaluation_snapshot_count": len(snapshots),
        "evaluation_snapshots": [
            {
                "id": snapshot.id,
                "model_training_run_id": snapshot.model_training_run_id,
                "model_registry_id": snapshot.model_registry_id,
                "feature_version_id": snapshot.feature_version_id,
                "target_task": snapshot.target_task,
                "model_family": snapshot.model_family,
                "selected_feature": snapshot.selected_feature,
                "fallback_strategy": snapshot.fallback_strategy,
                "primary_metric_name": snapshot.primary_metric_name,
                "validation_metric_value": snapshot.validation_metric_value,
                "test_metric_value": snapshot.test_metric_value,
                "validation_prediction_count": snapshot.validation_prediction_count,
                "test_prediction_count": snapshot.test_prediction_count,
                "snapshot": snapshot.snapshot,
                "created_at": snapshot.created_at.isoformat() if snapshot.created_at else None,
            }
            for snapshot in snapshots
        ],
    }


@router.get("/models/evaluations/history")
def phase_three_model_evaluation_history(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    model_family: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_train_demo and target_task is not None:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        history = get_model_evaluation_history_in_memory(
            repository,
            target_task=target_task,
            model_family=model_family,
            recent_limit=recent_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_train_demo and target_task is not None:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=team_code,
                    season_label=season_label,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            history = get_model_evaluation_history_postgres(
                connection,
                target_task=target_task,
                model_family=model_family,
                recent_limit=recent_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "model_family": model_family,
            "team_code": team_code,
            "season_label": season_label,
            "auto_train_demo": auto_train_demo,
            "recent_limit": recent_limit,
        },
        "model_evaluation_history": history,
    }


@router.post("/models/select")
def phase_three_model_select(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    selection_policy_name: str = Query(default="validation_mae_candidate_v1"),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_train_demo:
            train_phase_three_models_in_memory(
                repository,
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
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_train_demo:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=team_code,
                    season_label=season_label,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            selection_result = promote_best_model_postgres(
                connection,
                target_task=target_task,
                selection_policy_name=selection_policy_name,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "auto_train_demo": auto_train_demo,
            "selection_policy_name": selection_policy_name,
        },
        **selection_result,
    }


@router.get("/models/selections")
def phase_three_model_selections(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    active_only: bool = Query(default=False),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_train_demo and target_task is not None:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
            promote_best_model_in_memory(repository, target_task=target_task)
        selections = list_model_selection_snapshots_in_memory(
            repository,
            target_task=target_task,
            active_only=active_only,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_train_demo and target_task is not None:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=team_code,
                    season_label=season_label,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
                promote_best_model_postgres(connection, target_task=target_task)
            selections = list_model_selection_snapshots_postgres(
                connection,
                target_task=target_task,
                active_only=active_only,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "auto_train_demo": auto_train_demo,
            "active_only": active_only,
        },
        "selection_count": len(selections),
        "selections": [
            {
                "id": selection.id,
                "model_evaluation_snapshot_id": selection.model_evaluation_snapshot_id,
                "model_training_run_id": selection.model_training_run_id,
                "model_registry_id": selection.model_registry_id,
                "feature_version_id": selection.feature_version_id,
                "target_task": selection.target_task,
                "model_family": selection.model_family,
                "selection_policy_name": selection.selection_policy_name,
                "rationale": selection.rationale,
                "is_active": selection.is_active,
                "created_at": selection.created_at.isoformat()
                if selection.created_at
                else None,
            }
            for selection in selections
        ],
    }


@router.get("/models/selections/history")
def phase_three_model_selection_history(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_train_demo and target_task is not None:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
            promote_best_model_in_memory(repository, target_task=target_task)
        history = get_model_selection_history_in_memory(
            repository,
            target_task=target_task,
            recent_limit=recent_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_train_demo and target_task is not None:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=team_code,
                    season_label=season_label,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
                promote_best_model_postgres(connection, target_task=target_task)
            history = get_model_selection_history_postgres(
                connection,
                target_task=target_task,
                recent_limit=recent_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "auto_train_demo": auto_train_demo,
            "recent_limit": recent_limit,
        },
        "model_selection_history": history,
    }


@router.get("/models/score-preview")
def phase_three_model_score_preview(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    auto_select_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    canonical_game_id: int | None = Query(default=None, ge=1),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    limit: int = Query(default=10, ge=1, le=100),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_train_demo:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        if auto_select_demo:
            promote_best_model_in_memory(repository, target_task=target_task)
        scoring_preview = get_model_scoring_preview_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            canonical_game_id=canonical_game_id,
            limit=limit,
            include_evidence=include_evidence,
            evidence_dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_train_demo:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=team_code,
                    season_label=season_label,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            if auto_select_demo:
                promote_best_model_postgres(connection, target_task=target_task)
            scoring_preview = get_model_scoring_preview_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                canonical_game_id=canonical_game_id,
                limit=limit,
                include_evidence=include_evidence,
                evidence_dimensions=dimensions,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "canonical_game_id": canonical_game_id,
            "auto_train_demo": auto_train_demo,
            "auto_select_demo": auto_select_demo,
            "limit": limit,
            "include_evidence": include_evidence,
            "dimensions": list(dimensions),
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
        },
        **scoring_preview,
    }


@router.get("/models/future-game-preview")
def phase_three_model_future_game_preview(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    auto_select_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    home_team_code: str = Query(default="LAL"),
    away_team_code: str = Query(default="BOS"),
    home_spread_line: float | None = Query(default=None),
    total_line: float | None = Query(default=None),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_train_demo:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=None,
                season_label=None,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        if auto_select_demo:
            promote_best_model_in_memory(repository, target_task=target_task)
        preview = get_model_future_game_preview_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            season_label=season_label,
            game_date=game_date,
            home_team_code=home_team_code,
            away_team_code=away_team_code,
            home_spread_line=home_spread_line,
            total_line=total_line,
            include_evidence=include_evidence,
            evidence_dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_train_demo:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=None,
                    season_label=None,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            if auto_select_demo:
                promote_best_model_postgres(connection, target_task=target_task)
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
                evidence_dimensions=dimensions,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "season_label": season_label,
            "game_date": game_date,
            "home_team_code": home_team_code,
            "away_team_code": away_team_code,
            "home_spread_line": home_spread_line,
            "total_line": total_line,
            "auto_train_demo": auto_train_demo,
            "auto_select_demo": auto_select_demo,
            "include_evidence": include_evidence,
            "dimensions": list(dimensions),
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
        },
        **preview,
    }


@router.post("/models/future-game-preview/materialize")
def phase_three_model_future_game_preview_materialize(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    auto_select_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    home_team_code: str = Query(default="LAL"),
    away_team_code: str = Query(default="BOS"),
    home_spread_line: float | None = Query(default=None),
    total_line: float | None = Query(default=None),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_train_demo:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=None,
                season_label=None,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        if auto_select_demo:
            promote_best_model_in_memory(repository, target_task=target_task)
        materialized = materialize_model_future_game_preview_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            season_label=season_label,
            game_date=game_date,
            home_team_code=home_team_code,
            away_team_code=away_team_code,
            home_spread_line=home_spread_line,
            total_line=total_line,
            include_evidence=include_evidence,
            evidence_dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_train_demo:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=None,
                    season_label=None,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            if auto_select_demo:
                promote_best_model_postgres(connection, target_task=target_task)
            materialized = materialize_model_future_game_preview_postgres(
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
                evidence_dimensions=dimensions,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "season_label": season_label,
            "game_date": game_date,
            "home_team_code": home_team_code,
            "away_team_code": away_team_code,
            "home_spread_line": home_spread_line,
            "total_line": total_line,
            "auto_train_demo": auto_train_demo,
            "auto_select_demo": auto_select_demo,
            "include_evidence": include_evidence,
            "dimensions": list(dimensions),
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
        },
        **materialized,
    }


@router.get("/models/future-game-preview/runs")
def phase_three_model_future_game_preview_runs(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    auto_select_demo: bool = Query(default=True),
    auto_materialize_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    team_code: str | None = Query(default=None),
    home_team_code: str = Query(default="LAL"),
    away_team_code: str = Query(default="BOS"),
    home_spread_line: float | None = Query(default=None),
    total_line: float | None = Query(default=None),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    limit: int = Query(default=10, ge=1, le=100),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_train_demo and target_task is not None:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=None,
                season_label=None,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        if auto_select_demo and target_task is not None:
            promote_best_model_in_memory(repository, target_task=target_task)
        if auto_materialize_demo and target_task is not None:
            materialize_model_future_game_preview_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                season_label=season_label,
                game_date=game_date,
                home_team_code=home_team_code,
                away_team_code=away_team_code,
                home_spread_line=home_spread_line,
                total_line=total_line,
                include_evidence=include_evidence,
                evidence_dimensions=dimensions,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        scoring_runs = list_model_scoring_runs_in_memory(
            repository,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_train_demo and target_task is not None:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=None,
                    season_label=None,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            if auto_select_demo and target_task is not None:
                promote_best_model_postgres(connection, target_task=target_task)
            if auto_materialize_demo and target_task is not None:
                materialize_model_future_game_preview_postgres(
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
                    evidence_dimensions=dimensions,
                    comparable_limit=comparable_limit,
                    min_pattern_sample_size=min_pattern_sample_size,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            scoring_runs = list_model_scoring_runs_postgres(
                connection,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "season_label": season_label,
            "game_date": game_date,
            "team_code": team_code,
            "home_team_code": home_team_code,
            "away_team_code": away_team_code,
            "home_spread_line": home_spread_line,
            "total_line": total_line,
            "auto_train_demo": auto_train_demo,
            "auto_select_demo": auto_select_demo,
            "auto_materialize_demo": auto_materialize_demo,
            "limit": limit,
        },
        "scoring_run_count": len(scoring_runs),
        "scoring_runs": [
            {
                "id": scoring_run.id,
                "model_selection_snapshot_id": scoring_run.model_selection_snapshot_id,
                "model_evaluation_snapshot_id": scoring_run.model_evaluation_snapshot_id,
                "feature_version_id": scoring_run.feature_version_id,
                "target_task": scoring_run.target_task,
                "scenario_key": scoring_run.scenario_key,
                "season_label": scoring_run.season_label,
                "game_date": scoring_run.game_date.isoformat(),
                "home_team_code": scoring_run.home_team_code,
                "away_team_code": scoring_run.away_team_code,
                "home_spread_line": scoring_run.home_spread_line,
                "total_line": scoring_run.total_line,
                "policy_name": scoring_run.policy_name,
                "prediction_count": scoring_run.prediction_count,
                "candidate_opportunity_count": scoring_run.candidate_opportunity_count,
                "review_opportunity_count": scoring_run.review_opportunity_count,
                "discarded_opportunity_count": scoring_run.discarded_opportunity_count,
                "payload": scoring_run.payload,
                "created_at": scoring_run.created_at.isoformat()
                if scoring_run.created_at
                else None,
            }
            for scoring_run in scoring_runs[:limit]
        ],
    }


@router.get("/models/future-game-preview/runs/{scoring_run_id}")
def phase_three_model_future_game_preview_run_detail(
    scoring_run_id: int,
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    auto_select_demo: bool = Query(default=True),
    auto_materialize_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    home_team_code: str = Query(default="LAL"),
    away_team_code: str = Query(default="BOS"),
    home_spread_line: float | None = Query(default=None),
    total_line: float | None = Query(default=None),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_train_demo and target_task is not None:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=None,
                season_label=None,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        if auto_select_demo and target_task is not None:
            promote_best_model_in_memory(repository, target_task=target_task)
        if auto_materialize_demo and target_task is not None:
            materialize_model_future_game_preview_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                season_label=season_label,
                game_date=game_date,
                home_team_code=home_team_code,
                away_team_code=away_team_code,
                home_spread_line=home_spread_line,
                total_line=total_line,
                include_evidence=include_evidence,
                evidence_dimensions=dimensions,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        scoring_run = get_model_scoring_run_detail_in_memory(
            repository,
            scoring_run_id=scoring_run_id,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_train_demo and target_task is not None:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=None,
                    season_label=None,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            if auto_select_demo and target_task is not None:
                promote_best_model_postgres(connection, target_task=target_task)
            if auto_materialize_demo and target_task is not None:
                materialize_model_future_game_preview_postgres(
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
                    evidence_dimensions=dimensions,
                    comparable_limit=comparable_limit,
                    min_pattern_sample_size=min_pattern_sample_size,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            scoring_run = get_model_scoring_run_detail_postgres(
                connection,
                scoring_run_id=scoring_run_id,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "season_label": season_label,
            "game_date": game_date,
            "home_team_code": home_team_code,
            "away_team_code": away_team_code,
            "home_spread_line": home_spread_line,
            "total_line": total_line,
            "auto_train_demo": auto_train_demo,
            "auto_select_demo": auto_select_demo,
            "auto_materialize_demo": auto_materialize_demo,
        },
        "scoring_run": scoring_run,
    }


@router.get("/models/future-game-preview/history")
def phase_three_model_future_game_preview_history(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    auto_select_demo: bool = Query(default=True),
    auto_materialize_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    team_code: str | None = Query(default=None),
    home_team_code: str = Query(default="LAL"),
    away_team_code: str = Query(default="BOS"),
    home_spread_line: float | None = Query(default=None),
    total_line: float | None = Query(default=None),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_train_demo and target_task is not None:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=None,
                season_label=None,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        if auto_select_demo and target_task is not None:
            promote_best_model_in_memory(repository, target_task=target_task)
        if auto_materialize_demo and target_task is not None:
            materialize_model_future_game_preview_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                season_label=season_label,
                game_date=game_date,
                home_team_code=home_team_code,
                away_team_code=away_team_code,
                home_spread_line=home_spread_line,
                total_line=total_line,
                include_evidence=include_evidence,
                evidence_dimensions=dimensions,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        history = get_model_scoring_history_in_memory(
            repository,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            recent_limit=recent_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_train_demo and target_task is not None:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=None,
                    season_label=None,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            if auto_select_demo and target_task is not None:
                promote_best_model_postgres(connection, target_task=target_task)
            if auto_materialize_demo and target_task is not None:
                materialize_model_future_game_preview_postgres(
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
                    evidence_dimensions=dimensions,
                    comparable_limit=comparable_limit,
                    min_pattern_sample_size=min_pattern_sample_size,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            history = get_model_scoring_history_postgres(
                connection,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                recent_limit=recent_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "season_label": season_label,
            "game_date": game_date,
            "team_code": team_code,
            "home_team_code": home_team_code,
            "away_team_code": away_team_code,
            "home_spread_line": home_spread_line,
            "total_line": total_line,
            "auto_train_demo": auto_train_demo,
            "auto_select_demo": auto_select_demo,
            "auto_materialize_demo": auto_materialize_demo,
            "recent_limit": recent_limit,
        },
        "model_scoring_history": history,
    }


@router.get("/models/market-board/sources")
def phase_three_model_market_board_sources() -> dict[str, object]:
    return list_model_market_board_sources()


@router.post("/models/market-board/refresh")
def phase_three_model_market_board_refresh(
    repository_mode: str = Query(default="in_memory"),
    target_task: str = Query(default="spread_error_regression"),
    source_name: str = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default=None),
    game_count: int | None = Query(default=None, ge=1, le=20),
    source_path: str | None = Query(default=None),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        result = refresh_model_market_board_in_memory(
            repository,
            target_task=target_task,
            source_name=source_name,
            season_label=season_label,
            game_date=game_date,
            slate_label=slate_label,
            game_count=game_count,
            source_path=source_path,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            result = refresh_model_market_board_postgres(
                connection,
                target_task=target_task,
                source_name=source_name,
                season_label=season_label,
                game_date=game_date,
                slate_label=slate_label,
                game_count=game_count,
                source_path=source_path,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "source_path": source_path,
        },
        **result,
    }


@router.get("/models/market-board/history")
def phase_three_model_market_board_history(
    repository_mode: str = Query(default="in_memory"),
    auto_refresh_demo: bool = Query(default=True),
    target_task: str = Query(default="spread_error_regression"),
    source_name: str = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    source_path: str | None = Query(default=None),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if auto_refresh_demo:
            refresh_model_market_board_in_memory(
                repository,
                target_task=target_task,
                source_name=source_name,
                season_label=season_label,
                game_date=game_date,
                slate_label=slate_label,
                game_count=game_count,
                source_path=source_path,
            )
        history = get_model_market_board_refresh_history_in_memory(
            repository,
            target_task=target_task,
            source_name=source_name,
            recent_limit=recent_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if auto_refresh_demo:
                refresh_model_market_board_postgres(
                    connection,
                    target_task=target_task,
                    source_name=source_name,
                    season_label=season_label,
                    game_date=game_date,
                    slate_label=slate_label,
                    game_count=game_count,
                    source_path=source_path,
                )
            history = get_model_market_board_refresh_history_postgres(
                connection,
                target_task=target_task,
                source_name=source_name,
                recent_limit=recent_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "auto_refresh_demo": auto_refresh_demo,
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "source_path": source_path,
            "recent_limit": recent_limit,
        },
        "market_board_refresh_history": history,
    }


@router.get("/models/market-board/source-runs")
def phase_three_model_market_board_source_runs(
    repository_mode: str = Query(default="in_memory"),
    auto_refresh_demo: bool = Query(default=True),
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    source_path: str | None = Query(default=None),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if auto_refresh_demo:
            refresh_model_market_board_in_memory(
                repository,
                target_task=target_task,
                source_name=source_name or "demo_daily_lines_v1",
                season_label=season_label,
                game_date=game_date,
                slate_label=slate_label,
                game_count=game_count,
                source_path=source_path,
            )
        history = get_model_market_board_source_run_history_in_memory(
            repository,
            target_task=target_task,
            source_name=source_name,
            season_label=season_label,
            recent_limit=recent_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if auto_refresh_demo:
                refresh_model_market_board_postgres(
                    connection,
                    target_task=target_task,
                    source_name=source_name or "demo_daily_lines_v1",
                    season_label=season_label,
                    game_date=game_date,
                    slate_label=slate_label,
                    game_count=game_count,
                    source_path=source_path,
                )
            history = get_model_market_board_source_run_history_postgres(
                connection,
                target_task=target_task,
                source_name=source_name,
                season_label=season_label,
                recent_limit=recent_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "auto_refresh_demo": auto_refresh_demo,
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "source_path": source_path,
            "recent_limit": recent_limit,
        },
        "market_board_source_run_history": history,
    }


@router.get("/models/market-board/refresh-queue")
def phase_three_model_market_board_refresh_queue(
    repository_mode: str = Query(default="in_memory"),
    auto_refresh_demo: bool = Query(default=True),
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    freshness_status: str | None = Query(default=None),
    pending_only: bool = Query(default=False),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if auto_refresh_demo:
            refresh_model_market_board_in_memory(
                repository,
                target_task=target_task,
                source_name=source_name or "demo_daily_lines_v1",
                season_label=season_label,
                game_date=game_date,
                slate_label=slate_label,
                game_count=game_count,
            )
        queue = get_model_market_board_refresh_queue_in_memory(
            repository,
            target_task=target_task,
            season_label=season_label,
            source_name=source_name,
            freshness_status=freshness_status,
            pending_only=pending_only,
            recent_limit=recent_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if auto_refresh_demo:
                refresh_model_market_board_postgres(
                    connection,
                    target_task=target_task,
                    source_name=source_name or "demo_daily_lines_v1",
                    season_label=season_label,
                    game_date=game_date,
                    slate_label=slate_label,
                    game_count=game_count,
                )
            queue = get_model_market_board_refresh_queue_postgres(
                connection,
                target_task=target_task,
                season_label=season_label,
                source_name=source_name,
                freshness_status=freshness_status,
                pending_only=pending_only,
                recent_limit=recent_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "auto_refresh_demo": auto_refresh_demo,
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "freshness_status": freshness_status,
            "pending_only": pending_only,
            "recent_limit": recent_limit,
        },
        "market_board_refresh_queue": queue,
    }


@router.get("/models/market-board/queue")
def phase_three_model_market_board_queue(
    repository_mode: str = Query(default="in_memory"),
    auto_refresh_demo: bool = Query(default=True),
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    freshness_status: str | None = Query(default="fresh"),
    pending_only: bool = Query(default=False),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if auto_refresh_demo:
            refresh_model_market_board_in_memory(
                repository,
                target_task=target_task,
                source_name=source_name or "demo_daily_lines_v1",
                season_label=season_label,
                game_date=game_date,
                slate_label=slate_label,
                game_count=game_count,
            )
        queue = get_model_market_board_scoring_queue_in_memory(
            repository,
            target_task=target_task,
            season_label=season_label,
            source_name=source_name,
            freshness_status=freshness_status,
            pending_only=pending_only,
            recent_limit=recent_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if auto_refresh_demo:
                refresh_model_market_board_postgres(
                    connection,
                    target_task=target_task,
                    source_name=source_name or "demo_daily_lines_v1",
                    season_label=season_label,
                    game_date=game_date,
                    slate_label=slate_label,
                    game_count=game_count,
                )
            queue = get_model_market_board_scoring_queue_postgres(
                connection,
                target_task=target_task,
                season_label=season_label,
                source_name=source_name,
                freshness_status=freshness_status,
                pending_only=pending_only,
                recent_limit=recent_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "auto_refresh_demo": auto_refresh_demo,
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "freshness_status": freshness_status,
            "pending_only": pending_only,
            "recent_limit": recent_limit,
        },
        "market_board_scoring_queue": queue,
    }


@router.post("/models/market-board/orchestrate-refresh")
def phase_three_model_market_board_orchestrate_refresh(
    repository_mode: str = Query(default="in_memory"),
    auto_refresh_demo: bool = Query(default=True),
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    freshness_status: str | None = Query(default=None),
    pending_only: bool = Query(default=True),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if auto_refresh_demo:
            refresh_model_market_board_in_memory(
                repository,
                target_task=target_task,
                source_name=source_name or "demo_daily_lines_v1",
                season_label=season_label,
                game_date=game_date,
                slate_label=slate_label,
                game_count=game_count,
            )
        result = orchestrate_model_market_board_refresh_in_memory(
            repository,
            target_task=target_task,
            season_label=season_label,
            source_name=source_name,
            freshness_status=freshness_status,
            pending_only=pending_only,
            recent_limit=recent_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if auto_refresh_demo:
                refresh_model_market_board_postgres(
                    connection,
                    target_task=target_task,
                    source_name=source_name or "demo_daily_lines_v1",
                    season_label=season_label,
                    game_date=game_date,
                    slate_label=slate_label,
                    game_count=game_count,
                )
            result = orchestrate_model_market_board_refresh_postgres(
                connection,
                target_task=target_task,
                season_label=season_label,
                source_name=source_name,
                freshness_status=freshness_status,
                pending_only=pending_only,
                recent_limit=recent_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "auto_refresh_demo": auto_refresh_demo,
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "freshness_status": freshness_status,
            "pending_only": pending_only,
            "recent_limit": recent_limit,
        },
        **result,
    }


@router.post("/models/market-board/orchestrate-score")
def phase_three_model_market_board_orchestrate_score(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_refresh_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    auto_select_demo: bool = Query(default=True),
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    feature_key: str = Query(default="baseline_team_features_v1"),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    freshness_status: str | None = Query(default="fresh"),
    pending_only: bool = Query(default=True),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_refresh_demo:
            refresh_model_market_board_in_memory(
                repository,
                target_task=target_task,
                source_name=source_name or "demo_daily_lines_v1",
                season_label=season_label,
                game_date=game_date,
                slate_label=slate_label,
                game_count=game_count,
            )
        if auto_train_demo:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=None,
                season_label=None,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        if auto_select_demo:
            promote_best_model_in_memory(repository, target_task=target_task)
        result = orchestrate_model_market_board_scoring_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            season_label=season_label,
            source_name=source_name,
            freshness_status=freshness_status,
            pending_only=pending_only,
            include_evidence=include_evidence,
            evidence_dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            recent_limit=recent_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_refresh_demo:
                refresh_model_market_board_postgres(
                    connection,
                    target_task=target_task,
                    source_name=source_name or "demo_daily_lines_v1",
                    season_label=season_label,
                    game_date=game_date,
                    slate_label=slate_label,
                    game_count=game_count,
                )
            if auto_train_demo:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=None,
                    season_label=None,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            if auto_select_demo:
                promote_best_model_postgres(connection, target_task=target_task)
            result = orchestrate_model_market_board_scoring_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                season_label=season_label,
                source_name=source_name,
                freshness_status=freshness_status,
                pending_only=pending_only,
                include_evidence=include_evidence,
                evidence_dimensions=dimensions,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                recent_limit=recent_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "seed_demo": seed_demo,
            "auto_refresh_demo": auto_refresh_demo,
            "auto_train_demo": auto_train_demo,
            "auto_select_demo": auto_select_demo,
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "feature_key": feature_key,
            "include_evidence": include_evidence,
            "dimensions": dimensions,
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "freshness_status": freshness_status,
            "pending_only": pending_only,
            "recent_limit": recent_limit,
        },
        **result,
    }


@router.post("/models/market-board/orchestrate-cadence")
def phase_three_model_market_board_orchestrate_cadence(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_refresh_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    auto_select_demo: bool = Query(default=True),
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    feature_key: str = Query(default="baseline_team_features_v1"),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    refresh_freshness_status: str | None = Query(default=None),
    refresh_pending_only: bool = Query(default=False),
    scoring_freshness_status: str | None = Query(default="fresh"),
    scoring_pending_only: bool = Query(default=True),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_refresh_demo:
            refresh_model_market_board_in_memory(
                repository,
                target_task=target_task,
                source_name=source_name or "demo_daily_lines_v1",
                season_label=season_label,
                game_date=game_date,
                slate_label=slate_label,
                game_count=game_count,
            )
        if auto_train_demo:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=None,
                season_label=None,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        if auto_select_demo:
            promote_best_model_in_memory(repository, target_task=target_task)
        result = orchestrate_model_market_board_cadence_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            season_label=season_label,
            source_name=source_name,
            refresh_freshness_status=refresh_freshness_status,
            refresh_pending_only=refresh_pending_only,
            scoring_freshness_status=scoring_freshness_status,
            scoring_pending_only=scoring_pending_only,
            include_evidence=include_evidence,
            evidence_dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            recent_limit=recent_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_refresh_demo:
                refresh_model_market_board_postgres(
                    connection,
                    target_task=target_task,
                    source_name=source_name or "demo_daily_lines_v1",
                    season_label=season_label,
                    game_date=game_date,
                    slate_label=slate_label,
                    game_count=game_count,
                )
            if auto_train_demo:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=None,
                    season_label=None,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            if auto_select_demo:
                promote_best_model_postgres(connection, target_task=target_task)
            result = orchestrate_model_market_board_cadence_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                season_label=season_label,
                source_name=source_name,
                refresh_freshness_status=refresh_freshness_status,
                refresh_pending_only=refresh_pending_only,
                scoring_freshness_status=scoring_freshness_status,
                scoring_pending_only=scoring_pending_only,
                include_evidence=include_evidence,
                evidence_dimensions=dimensions,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                recent_limit=recent_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "seed_demo": seed_demo,
            "auto_refresh_demo": auto_refresh_demo,
            "auto_train_demo": auto_train_demo,
            "auto_select_demo": auto_select_demo,
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "feature_key": feature_key,
            "include_evidence": include_evidence,
            "dimensions": dimensions,
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "refresh_freshness_status": refresh_freshness_status,
            "refresh_pending_only": refresh_pending_only,
            "scoring_freshness_status": scoring_freshness_status,
            "scoring_pending_only": scoring_pending_only,
            "recent_limit": recent_limit,
        },
        **result,
    }


@router.get("/models/market-board/refresh-orchestration-history")
def phase_three_model_market_board_refresh_orchestration_history(
    repository_mode: str = Query(default="in_memory"),
    auto_refresh_demo: bool = Query(default=True),
    auto_orchestrate_demo: bool = Query(default=True),
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    freshness_status: str | None = Query(default=None),
    pending_only: bool = Query(default=False),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if auto_refresh_demo:
            refresh_model_market_board_in_memory(
                repository,
                target_task=target_task,
                source_name=source_name or "demo_daily_lines_v1",
                season_label=season_label,
                game_date=game_date,
                slate_label=slate_label,
                game_count=game_count,
            )
        if auto_orchestrate_demo:
            orchestrate_model_market_board_refresh_in_memory(
                repository,
                target_task=target_task,
                season_label=season_label,
                source_name=source_name,
                freshness_status=freshness_status,
                pending_only=pending_only,
                recent_limit=recent_limit,
            )
        history = get_model_market_board_refresh_batch_history_in_memory(
            repository,
            target_task=target_task,
            source_name=source_name,
            recent_limit=recent_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if auto_refresh_demo:
                refresh_model_market_board_postgres(
                    connection,
                    target_task=target_task,
                    source_name=source_name or "demo_daily_lines_v1",
                    season_label=season_label,
                    game_date=game_date,
                    slate_label=slate_label,
                    game_count=game_count,
                )
            if auto_orchestrate_demo:
                orchestrate_model_market_board_refresh_postgres(
                    connection,
                    target_task=target_task,
                    season_label=season_label,
                    source_name=source_name,
                    freshness_status=freshness_status,
                    pending_only=pending_only,
                    recent_limit=recent_limit,
                )
            history = get_model_market_board_refresh_batch_history_postgres(
                connection,
                target_task=target_task,
                source_name=source_name,
                recent_limit=recent_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "auto_refresh_demo": auto_refresh_demo,
            "auto_orchestrate_demo": auto_orchestrate_demo,
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "freshness_status": freshness_status,
            "pending_only": pending_only,
            "recent_limit": recent_limit,
        },
        "market_board_refresh_orchestration_history": history,
    }


@router.get("/models/market-board/cadence-history")
def phase_three_model_market_board_cadence_history(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_refresh_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    auto_select_demo: bool = Query(default=True),
    auto_orchestrate_demo: bool = Query(default=True),
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    feature_key: str = Query(default="baseline_team_features_v1"),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    refresh_freshness_status: str | None = Query(default=None),
    refresh_pending_only: bool = Query(default=False),
    scoring_freshness_status: str | None = Query(default="fresh"),
    scoring_pending_only: bool = Query(default=True),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_refresh_demo:
            refresh_model_market_board_in_memory(
                repository,
                target_task=target_task,
                source_name=source_name or "demo_daily_lines_v1",
                season_label=season_label,
                game_date=game_date,
                slate_label=slate_label,
                game_count=game_count,
            )
        if auto_train_demo:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=None,
                season_label=None,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        if auto_select_demo:
            promote_best_model_in_memory(repository, target_task=target_task)
        if auto_orchestrate_demo:
            orchestrate_model_market_board_cadence_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                season_label=season_label,
                source_name=source_name,
                refresh_freshness_status=refresh_freshness_status,
                refresh_pending_only=refresh_pending_only,
                scoring_freshness_status=scoring_freshness_status,
                scoring_pending_only=scoring_pending_only,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                recent_limit=recent_limit,
            )
        history = get_model_market_board_cadence_batch_history_in_memory(
            repository,
            target_task=target_task,
            source_name=source_name,
            recent_limit=recent_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_refresh_demo:
                refresh_model_market_board_postgres(
                    connection,
                    target_task=target_task,
                    source_name=source_name or "demo_daily_lines_v1",
                    season_label=season_label,
                    game_date=game_date,
                    slate_label=slate_label,
                    game_count=game_count,
                )
            if auto_train_demo:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=None,
                    season_label=None,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            if auto_select_demo:
                promote_best_model_postgres(connection, target_task=target_task)
            if auto_orchestrate_demo:
                orchestrate_model_market_board_cadence_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    season_label=season_label,
                    source_name=source_name,
                    refresh_freshness_status=refresh_freshness_status,
                    refresh_pending_only=refresh_pending_only,
                    scoring_freshness_status=scoring_freshness_status,
                    scoring_pending_only=scoring_pending_only,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                    recent_limit=recent_limit,
                )
            history = get_model_market_board_cadence_batch_history_postgres(
                connection,
                target_task=target_task,
                source_name=source_name,
                recent_limit=recent_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "seed_demo": seed_demo,
            "auto_refresh_demo": auto_refresh_demo,
            "auto_train_demo": auto_train_demo,
            "auto_select_demo": auto_select_demo,
            "auto_orchestrate_demo": auto_orchestrate_demo,
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "feature_key": feature_key,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "refresh_freshness_status": refresh_freshness_status,
            "refresh_pending_only": refresh_pending_only,
            "scoring_freshness_status": scoring_freshness_status,
            "scoring_pending_only": scoring_pending_only,
            "recent_limit": recent_limit,
        },
        "market_board_cadence_history": history,
    }


@router.get("/models/market-board/orchestration-history")
def phase_three_model_market_board_orchestration_history(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_refresh_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    auto_select_demo: bool = Query(default=True),
    auto_orchestrate_demo: bool = Query(default=True),
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    feature_key: str = Query(default="baseline_team_features_v1"),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    freshness_status: str | None = Query(default="fresh"),
    pending_only: bool = Query(default=True),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_refresh_demo:
            refresh_model_market_board_in_memory(
                repository,
                target_task=target_task,
                source_name=source_name or "demo_daily_lines_v1",
                season_label=season_label,
                game_date=game_date,
                slate_label=slate_label,
                game_count=game_count,
            )
        if auto_train_demo:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=None,
                season_label=None,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        if auto_select_demo:
            promote_best_model_in_memory(repository, target_task=target_task)
        if auto_orchestrate_demo:
            orchestrate_model_market_board_scoring_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                season_label=season_label,
                source_name=source_name,
                freshness_status=freshness_status,
                pending_only=pending_only,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        history = get_model_market_board_scoring_batch_history_in_memory(
            repository,
            target_task=target_task,
            source_name=source_name,
            recent_limit=recent_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_refresh_demo:
                refresh_model_market_board_postgres(
                    connection,
                    target_task=target_task,
                    source_name=source_name or "demo_daily_lines_v1",
                    season_label=season_label,
                    game_date=game_date,
                    slate_label=slate_label,
                    game_count=game_count,
                )
            if auto_train_demo:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=None,
                    season_label=None,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            if auto_select_demo:
                promote_best_model_postgres(connection, target_task=target_task)
            if auto_orchestrate_demo:
                orchestrate_model_market_board_scoring_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    season_label=season_label,
                    source_name=source_name,
                    freshness_status=freshness_status,
                    pending_only=pending_only,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            history = get_model_market_board_scoring_batch_history_postgres(
                connection,
                target_task=target_task,
                source_name=source_name,
                recent_limit=recent_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "seed_demo": seed_demo,
            "auto_refresh_demo": auto_refresh_demo,
            "auto_train_demo": auto_train_demo,
            "auto_select_demo": auto_select_demo,
            "auto_orchestrate_demo": auto_orchestrate_demo,
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "feature_key": feature_key,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "freshness_status": freshness_status,
            "pending_only": pending_only,
            "recent_limit": recent_limit,
        },
        "market_board_orchestration_history": history,
    }


@router.get("/models/market-board/cadence")
def phase_three_model_market_board_cadence(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_refresh_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    auto_select_demo: bool = Query(default=True),
    auto_orchestrate_demo: bool = Query(default=True),
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    feature_key: str = Query(default="baseline_team_features_v1"),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    freshness_status: str | None = Query(default="fresh"),
    pending_only: bool = Query(default=True),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_refresh_demo:
            refresh_model_market_board_in_memory(
                repository,
                target_task=target_task,
                source_name=source_name or "demo_daily_lines_v1",
                season_label=season_label,
                game_date=game_date,
                slate_label=slate_label,
                game_count=game_count,
            )
        if auto_train_demo:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=None,
                season_label=None,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        if auto_select_demo:
            promote_best_model_in_memory(repository, target_task=target_task)
        if auto_orchestrate_demo:
            orchestrate_model_market_board_scoring_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                season_label=season_label,
                source_name=source_name,
                freshness_status=freshness_status,
                pending_only=pending_only,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        dashboard = get_model_market_board_cadence_dashboard_in_memory(
            repository,
            target_task=target_task,
            season_label=season_label,
            source_name=source_name,
            recent_limit=recent_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_refresh_demo:
                refresh_model_market_board_postgres(
                    connection,
                    target_task=target_task,
                    source_name=source_name or "demo_daily_lines_v1",
                    season_label=season_label,
                    game_date=game_date,
                    slate_label=slate_label,
                    game_count=game_count,
                )
            if auto_train_demo:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=None,
                    season_label=None,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            if auto_select_demo:
                promote_best_model_postgres(connection, target_task=target_task)
            if auto_orchestrate_demo:
                orchestrate_model_market_board_scoring_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    season_label=season_label,
                    source_name=source_name,
                    freshness_status=freshness_status,
                    pending_only=pending_only,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            dashboard = get_model_market_board_cadence_dashboard_postgres(
                connection,
                target_task=target_task,
                season_label=season_label,
                source_name=source_name,
                recent_limit=recent_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "seed_demo": seed_demo,
            "auto_refresh_demo": auto_refresh_demo,
            "auto_train_demo": auto_train_demo,
            "auto_select_demo": auto_select_demo,
            "auto_orchestrate_demo": auto_orchestrate_demo,
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "feature_key": feature_key,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "freshness_status": freshness_status,
            "pending_only": pending_only,
            "recent_limit": recent_limit,
        },
        "market_board_cadence": dashboard,
    }


@router.post("/models/market-board/materialize")
def phase_three_model_market_board_materialize(
    request: FutureSlateRequest = Body(...),
    repository_mode: str = Query(default="in_memory"),
    target_task: str = Query(default="spread_error_regression"),
) -> dict[str, object]:
    games = [game.model_dump() for game in request.games]
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        board_result = materialize_model_market_board_in_memory(
            repository,
            target_task=target_task,
            games=games,
            slate_label=request.slate_label,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            board_result = materialize_model_market_board_postgres(
                connection,
                target_task=target_task,
                games=games,
                slate_label=request.slate_label,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "target_task": target_task,
            "slate_label": request.slate_label,
        },
        **board_result,
    }


@router.get("/models/market-board")
def phase_three_model_market_boards(
    repository_mode: str = Query(default="in_memory"),
    auto_materialize_demo: bool = Query(default=True),
    target_task: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    slate_label: str | None = Query(default="demo-market-board"),
    game_date: date = Query(default=date(2026, 4, 20)),
    home_team_code: str = Query(default="LAL"),
    away_team_code: str = Query(default="BOS"),
    home_spread_line: float | None = Query(default=None),
    total_line: float | None = Query(default=None),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if auto_materialize_demo and target_task is not None:
            materialize_model_market_board_in_memory(
                repository,
                target_task=target_task,
                slate_label=slate_label,
                games=[
                    {
                        "season_label": season_label or "2025-2026",
                        "game_date": game_date,
                        "home_team_code": home_team_code,
                        "away_team_code": away_team_code,
                        "home_spread_line": home_spread_line,
                        "total_line": total_line,
                    }
                ],
            )
        boards = list_model_market_boards_in_memory(
            repository,
            target_task=target_task,
            season_label=season_label,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if auto_materialize_demo and target_task is not None:
                materialize_model_market_board_postgres(
                    connection,
                    target_task=target_task,
                    slate_label=slate_label,
                    games=[
                        {
                            "season_label": season_label or "2025-2026",
                            "game_date": game_date,
                            "home_team_code": home_team_code,
                            "away_team_code": away_team_code,
                            "home_spread_line": home_spread_line,
                            "total_line": total_line,
                        }
                    ],
                )
            boards = list_model_market_boards_postgres(
                connection,
                target_task=target_task,
                season_label=season_label,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "auto_materialize_demo": auto_materialize_demo,
            "target_task": target_task,
            "season_label": season_label,
        },
        "board_count": len(boards),
        "boards": boards,
    }


@router.get("/models/market-board/{board_id}")
def phase_three_model_market_board_detail(
    board_id: int,
    repository_mode: str = Query(default="in_memory"),
    auto_materialize_demo: bool = Query(default=True),
    target_task: str = Query(default="spread_error_regression"),
    season_label: str = Query(default="2025-2026"),
    slate_label: str | None = Query(default="demo-market-board"),
    game_date: date = Query(default=date(2026, 4, 20)),
    home_team_code: str = Query(default="LAL"),
    away_team_code: str = Query(default="BOS"),
    home_spread_line: float | None = Query(default=None),
    total_line: float | None = Query(default=None),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if auto_materialize_demo:
            materialize_model_market_board_in_memory(
                repository,
                target_task=target_task,
                slate_label=slate_label,
                games=[
                    {
                        "season_label": season_label,
                        "game_date": game_date,
                        "home_team_code": home_team_code,
                        "away_team_code": away_team_code,
                        "home_spread_line": home_spread_line,
                        "total_line": total_line,
                    }
                ],
            )
        board = get_model_market_board_detail_in_memory(repository, board_id=board_id)
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if auto_materialize_demo:
                materialize_model_market_board_postgres(
                    connection,
                    target_task=target_task,
                    slate_label=slate_label,
                    games=[
                        {
                            "season_label": season_label,
                            "game_date": game_date,
                            "home_team_code": home_team_code,
                            "away_team_code": away_team_code,
                            "home_spread_line": home_spread_line,
                            "total_line": total_line,
                        }
                    ],
                )
            board = get_model_market_board_detail_postgres(connection, board_id=board_id)
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "auto_materialize_demo": auto_materialize_demo,
            "target_task": target_task,
            "season_label": season_label,
        },
        "board": board,
    }


@router.get("/models/market-board/{board_id}/operations")
def phase_three_model_market_board_operations(
    board_id: int,
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_refresh_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    auto_select_demo: bool = Query(default=True),
    auto_orchestrate_demo: bool = Query(default=True),
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    feature_key: str = Query(default="baseline_team_features_v1"),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    freshness_status: str | None = Query(default="fresh"),
    pending_only: bool = Query(default=True),
    recent_limit: int = Query(default=5, ge=1, le=20),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_refresh_demo:
            refresh_model_market_board_in_memory(
                repository,
                target_task=target_task,
                source_name=source_name or "demo_daily_lines_v1",
                season_label=season_label,
                game_date=game_date,
                slate_label=slate_label,
                game_count=game_count,
            )
        if auto_train_demo:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=None,
                season_label=None,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        if auto_select_demo:
            promote_best_model_in_memory(repository, target_task=target_task)
        if auto_orchestrate_demo:
            orchestrate_model_market_board_scoring_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                season_label=season_label,
                source_name=source_name,
                freshness_status=freshness_status,
                pending_only=pending_only,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        operations = get_model_market_board_operations_in_memory(
            repository,
            board_id=board_id,
            recent_limit=recent_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_refresh_demo:
                refresh_model_market_board_postgres(
                    connection,
                    target_task=target_task,
                    source_name=source_name or "demo_daily_lines_v1",
                    season_label=season_label,
                    game_date=game_date,
                    slate_label=slate_label,
                    game_count=game_count,
                )
            if auto_train_demo:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=None,
                    season_label=None,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            if auto_select_demo:
                promote_best_model_postgres(connection, target_task=target_task)
            if auto_orchestrate_demo:
                orchestrate_model_market_board_scoring_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    season_label=season_label,
                    source_name=source_name,
                    freshness_status=freshness_status,
                    pending_only=pending_only,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            operations = get_model_market_board_operations_postgres(
                connection,
                board_id=board_id,
                recent_limit=recent_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "seed_demo": seed_demo,
            "auto_refresh_demo": auto_refresh_demo,
            "auto_train_demo": auto_train_demo,
            "auto_select_demo": auto_select_demo,
            "auto_orchestrate_demo": auto_orchestrate_demo,
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "feature_key": feature_key,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "freshness_status": freshness_status,
            "pending_only": pending_only,
            "recent_limit": recent_limit,
        },
        "operations": operations,
    }


@router.post("/models/market-board/{board_id}/score")
def phase_three_model_market_board_score(
    board_id: int,
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_materialize_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    auto_select_demo: bool = Query(default=True),
    target_task: str = Query(default="spread_error_regression"),
    season_label: str = Query(default="2025-2026"),
    slate_label: str | None = Query(default="demo-market-board"),
    game_date: date = Query(default=date(2026, 4, 20)),
    home_team_code: str = Query(default="LAL"),
    away_team_code: str = Query(default="BOS"),
    home_spread_line: float | None = Query(default=None),
    total_line: float | None = Query(default=None),
    feature_key: str = Query(default="baseline_team_features_v1"),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_materialize_demo:
            materialize_model_market_board_in_memory(
                repository,
                target_task=target_task,
                slate_label=slate_label,
                games=[
                    {
                        "season_label": season_label,
                        "game_date": game_date,
                        "home_team_code": home_team_code,
                        "away_team_code": away_team_code,
                        "home_spread_line": home_spread_line,
                        "total_line": total_line,
                    }
                ],
            )
        board = get_model_market_board_detail_in_memory(repository, board_id=board_id)
        resolved_target_task = (
            str(board["target_task"]) if board is not None else target_task
        )
        if auto_train_demo:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=resolved_target_task,
                team_code=None,
                season_label=None,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        if auto_select_demo:
            promote_best_model_in_memory(repository, target_task=resolved_target_task)
        result = score_model_market_board_in_memory(
            repository,
            board_id=board_id,
            feature_key=feature_key,
            include_evidence=include_evidence,
            evidence_dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_materialize_demo:
                materialize_model_market_board_postgres(
                    connection,
                    target_task=target_task,
                    slate_label=slate_label,
                    games=[
                        {
                            "season_label": season_label,
                            "game_date": game_date,
                            "home_team_code": home_team_code,
                            "away_team_code": away_team_code,
                            "home_spread_line": home_spread_line,
                            "total_line": total_line,
                        }
                    ],
                )
            board = get_model_market_board_detail_postgres(connection, board_id=board_id)
            resolved_target_task = str(board["target_task"]) if board is not None else target_task
            if auto_train_demo:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=resolved_target_task,
                    team_code=None,
                    season_label=None,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            if auto_select_demo:
                promote_best_model_postgres(connection, target_task=resolved_target_task)
            result = score_model_market_board_postgres(
                connection,
                board_id=board_id,
                feature_key=feature_key,
                include_evidence=include_evidence,
                evidence_dimensions=dimensions,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "board_id": board_id,
            "feature_key": feature_key,
            "auto_materialize_demo": auto_materialize_demo,
            "auto_train_demo": auto_train_demo,
            "auto_select_demo": auto_select_demo,
            "include_evidence": include_evidence,
            "dimensions": list(dimensions),
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
        },
        **result,
    }


@router.post("/models/future-slate/preview")
def phase_three_model_future_slate_preview(
    request: FutureSlateRequest = Body(...),
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    auto_select_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    games = [game.model_dump() for game in request.games]
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_train_demo:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=None,
                season_label=None,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        if auto_select_demo:
            promote_best_model_in_memory(repository, target_task=target_task)
        preview = get_model_future_slate_preview_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            games=games,
            slate_label=request.slate_label,
            include_evidence=include_evidence,
            evidence_dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_train_demo:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=None,
                    season_label=None,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            if auto_select_demo:
                promote_best_model_postgres(connection, target_task=target_task)
            preview = get_model_future_slate_preview_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                games=games,
                slate_label=request.slate_label,
                include_evidence=include_evidence,
                evidence_dimensions=dimensions,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "slate_label": request.slate_label,
            "auto_train_demo": auto_train_demo,
            "auto_select_demo": auto_select_demo,
            "include_evidence": include_evidence,
            "dimensions": list(dimensions),
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
        },
        **preview,
    }


@router.post("/models/future-slate/materialize")
def phase_three_model_future_slate_materialize(
    request: FutureSlateRequest = Body(...),
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    auto_select_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    games = [game.model_dump() for game in request.games]
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_train_demo:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=None,
                season_label=None,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        if auto_select_demo:
            promote_best_model_in_memory(repository, target_task=target_task)
        materialized = materialize_model_future_slate_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            games=games,
            slate_label=request.slate_label,
            include_evidence=include_evidence,
            evidence_dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_train_demo:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=None,
                    season_label=None,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            if auto_select_demo:
                promote_best_model_postgres(connection, target_task=target_task)
            materialized = materialize_model_future_slate_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                games=games,
                slate_label=request.slate_label,
                include_evidence=include_evidence,
                evidence_dimensions=dimensions,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "slate_label": request.slate_label,
            "auto_train_demo": auto_train_demo,
            "auto_select_demo": auto_select_demo,
            "include_evidence": include_evidence,
            "dimensions": list(dimensions),
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
        },
        **materialized,
    }


@router.post("/models/future-game-preview/opportunities/materialize")
def phase_three_model_future_opportunity_materialize(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    auto_select_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    home_team_code: str = Query(default="LAL"),
    away_team_code: str = Query(default="BOS"),
    home_spread_line: float | None = Query(default=None),
    total_line: float | None = Query(default=None),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_train_demo:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=None,
                season_label=None,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        if auto_select_demo:
            promote_best_model_in_memory(repository, target_task=target_task)
        materialized = materialize_model_future_opportunities_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            season_label=season_label,
            game_date=game_date,
            home_team_code=home_team_code,
            away_team_code=away_team_code,
            home_spread_line=home_spread_line,
            total_line=total_line,
            include_evidence=include_evidence,
            evidence_dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_train_demo:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=None,
                    season_label=None,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            if auto_select_demo:
                promote_best_model_postgres(connection, target_task=target_task)
            materialized = materialize_model_future_opportunities_postgres(
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
                evidence_dimensions=dimensions,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "season_label": season_label,
            "game_date": game_date,
            "home_team_code": home_team_code,
            "away_team_code": away_team_code,
            "home_spread_line": home_spread_line,
            "total_line": total_line,
            "auto_train_demo": auto_train_demo,
            "auto_select_demo": auto_select_demo,
            "include_evidence": include_evidence,
            "dimensions": list(dimensions),
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
        },
        **materialized,
    }


@router.post("/models/opportunities/materialize")
def phase_three_model_opportunity_materialize(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    auto_select_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    canonical_game_id: int | None = Query(default=None, ge=1),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    limit: int = Query(default=10, ge=1, le=100),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_train_demo:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        if auto_select_demo:
            promote_best_model_in_memory(repository, target_task=target_task)
        materialized = materialize_model_opportunities_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            canonical_game_id=canonical_game_id,
            limit=limit,
            include_evidence=include_evidence,
            evidence_dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_train_demo:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=team_code,
                    season_label=season_label,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            if auto_select_demo:
                promote_best_model_postgres(connection, target_task=target_task)
            materialized = materialize_model_opportunities_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                canonical_game_id=canonical_game_id,
                limit=limit,
                include_evidence=include_evidence,
                evidence_dimensions=dimensions,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "canonical_game_id": canonical_game_id,
            "auto_train_demo": auto_train_demo,
            "auto_select_demo": auto_select_demo,
            "limit": limit,
            "include_evidence": include_evidence,
            "dimensions": list(dimensions),
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
        },
        **materialized,
    }


@router.get("/models/opportunities")
def phase_three_model_opportunities(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    auto_select_demo: bool = Query(default=True),
    auto_materialize_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    canonical_game_id: int | None = Query(default=None, ge=1),
    source_kind: str | None = Query(default=None),
    scenario_key: str | None = Query(default=None),
    game_date: date = Query(default=date(2026, 4, 20)),
    home_team_code: str = Query(default="LAL"),
    away_team_code: str = Query(default="BOS"),
    home_spread_line: float | None = Query(default=None),
    total_line: float | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    status: str | None = Query(default=None),
    limit: int = Query(default=10, ge=1, le=100),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_train_demo and target_task is not None:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=None if source_kind == "future_scenario" else team_code,
                season_label=None if source_kind == "future_scenario" else season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        if auto_select_demo and target_task is not None:
            promote_best_model_in_memory(repository, target_task=target_task)
        if auto_materialize_demo and target_task is not None:
            if source_kind == "future_scenario":
                materialize_model_future_opportunities_in_memory(
                    repository,
                    feature_key=feature_key,
                    target_task=target_task,
                    season_label=season_label or "2025-2026",
                    game_date=game_date,
                    home_team_code=home_team_code,
                    away_team_code=away_team_code,
                    home_spread_line=home_spread_line,
                    total_line=total_line,
                    include_evidence=include_evidence,
                    evidence_dimensions=dimensions,
                    comparable_limit=comparable_limit,
                    min_pattern_sample_size=min_pattern_sample_size,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            else:
                materialize_model_opportunities_in_memory(
                    repository,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=team_code,
                    season_label=season_label,
                    canonical_game_id=canonical_game_id,
                    limit=limit,
                    include_evidence=include_evidence,
                    evidence_dimensions=dimensions,
                    comparable_limit=comparable_limit,
                    min_pattern_sample_size=min_pattern_sample_size,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
        opportunities = list_model_opportunities_in_memory(
            repository,
            target_task=target_task,
            team_code=team_code,
            status=status,
            season_label=season_label,
            source_kind=source_kind,
            scenario_key=scenario_key,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_train_demo and target_task is not None:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=None if source_kind == "future_scenario" else team_code,
                    season_label=None if source_kind == "future_scenario" else season_label,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            if auto_select_demo and target_task is not None:
                promote_best_model_postgres(connection, target_task=target_task)
            if auto_materialize_demo and target_task is not None:
                if source_kind == "future_scenario":
                    materialize_model_future_opportunities_postgres(
                        connection,
                        feature_key=feature_key,
                        target_task=target_task,
                        season_label=season_label or "2025-2026",
                        game_date=game_date,
                        home_team_code=home_team_code,
                        away_team_code=away_team_code,
                        home_spread_line=home_spread_line,
                        total_line=total_line,
                        include_evidence=include_evidence,
                        evidence_dimensions=dimensions,
                        comparable_limit=comparable_limit,
                        min_pattern_sample_size=min_pattern_sample_size,
                        train_ratio=train_ratio,
                        validation_ratio=validation_ratio,
                    )
                else:
                    materialize_model_opportunities_postgres(
                        connection,
                        feature_key=feature_key,
                        target_task=target_task,
                        team_code=team_code,
                        season_label=season_label,
                        canonical_game_id=canonical_game_id,
                        limit=limit,
                        include_evidence=include_evidence,
                        evidence_dimensions=dimensions,
                        comparable_limit=comparable_limit,
                        min_pattern_sample_size=min_pattern_sample_size,
                        train_ratio=train_ratio,
                        validation_ratio=validation_ratio,
                    )
            opportunities = list_model_opportunities_postgres(
                connection,
                target_task=target_task,
                team_code=team_code,
                status=status,
                season_label=season_label,
                source_kind=source_kind,
                scenario_key=scenario_key,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "canonical_game_id": canonical_game_id,
            "source_kind": source_kind,
            "scenario_key": scenario_key,
            "game_date": game_date,
            "home_team_code": home_team_code,
            "away_team_code": away_team_code,
            "home_spread_line": home_spread_line,
            "total_line": total_line,
            "auto_train_demo": auto_train_demo,
            "auto_select_demo": auto_select_demo,
            "auto_materialize_demo": auto_materialize_demo,
            "status": status,
            "limit": limit,
        },
        "opportunity_count": len(opportunities),
        "opportunities": [
            {
                "id": opportunity.id,
                "model_scoring_run_id": opportunity.model_scoring_run_id,
                "model_selection_snapshot_id": opportunity.model_selection_snapshot_id,
                "model_evaluation_snapshot_id": opportunity.model_evaluation_snapshot_id,
                "feature_version_id": opportunity.feature_version_id,
                "target_task": opportunity.target_task,
                "source_kind": opportunity.source_kind,
                "scenario_key": opportunity.scenario_key,
                "opportunity_key": opportunity.opportunity_key,
                "team_code": opportunity.team_code,
                "opponent_code": opportunity.opponent_code,
                "season_label": opportunity.season_label,
                "canonical_game_id": opportunity.canonical_game_id,
                "game_date": opportunity.game_date.isoformat(),
                "policy_name": opportunity.policy_name,
                "status": opportunity.status,
                "prediction_value": opportunity.prediction_value,
                "signal_strength": opportunity.signal_strength,
                "evidence_rating": opportunity.evidence_rating,
                "recommendation_status": opportunity.recommendation_status,
                "payload": opportunity.payload,
                "created_at": opportunity.created_at.isoformat()
                if opportunity.created_at
                else None,
                "updated_at": opportunity.updated_at.isoformat()
                if opportunity.updated_at
                else None,
            }
            for opportunity in opportunities[:limit]
        ],
    }


@router.get("/models/opportunities/history")
def phase_three_model_opportunity_history(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    auto_select_demo: bool = Query(default=True),
    auto_materialize_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    canonical_game_id: int | None = Query(default=None, ge=1),
    source_kind: str | None = Query(default=None),
    scenario_key: str | None = Query(default=None),
    game_date: date = Query(default=date(2026, 4, 20)),
    home_team_code: str = Query(default="LAL"),
    away_team_code: str = Query(default="BOS"),
    home_spread_line: float | None = Query(default=None),
    total_line: float | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    limit: int = Query(default=10, ge=1, le=100),
    recent_limit: int = Query(default=10, ge=1, le=50),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_train_demo and target_task is not None:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=None if source_kind == "future_scenario" else team_code,
                season_label=None if source_kind == "future_scenario" else season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        if auto_select_demo and target_task is not None:
            promote_best_model_in_memory(repository, target_task=target_task)
        if auto_materialize_demo and target_task is not None:
            if source_kind == "future_scenario":
                materialize_model_future_opportunities_in_memory(
                    repository,
                    feature_key=feature_key,
                    target_task=target_task,
                    season_label=season_label or "2025-2026",
                    game_date=game_date,
                    home_team_code=home_team_code,
                    away_team_code=away_team_code,
                    home_spread_line=home_spread_line,
                    total_line=total_line,
                    include_evidence=include_evidence,
                    evidence_dimensions=dimensions,
                    comparable_limit=comparable_limit,
                    min_pattern_sample_size=min_pattern_sample_size,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            else:
                materialize_model_opportunities_in_memory(
                    repository,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=team_code,
                    season_label=season_label,
                    canonical_game_id=canonical_game_id,
                    limit=limit,
                    include_evidence=include_evidence,
                    evidence_dimensions=dimensions,
                    comparable_limit=comparable_limit,
                    min_pattern_sample_size=min_pattern_sample_size,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
        history = get_model_opportunity_history_in_memory(
            repository,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            source_kind=source_kind,
            scenario_key=scenario_key,
            recent_limit=recent_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_train_demo and target_task is not None:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=None if source_kind == "future_scenario" else team_code,
                    season_label=None if source_kind == "future_scenario" else season_label,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            if auto_select_demo and target_task is not None:
                promote_best_model_postgres(connection, target_task=target_task)
            if auto_materialize_demo and target_task is not None:
                if source_kind == "future_scenario":
                    materialize_model_future_opportunities_postgres(
                        connection,
                        feature_key=feature_key,
                        target_task=target_task,
                        season_label=season_label or "2025-2026",
                        game_date=game_date,
                        home_team_code=home_team_code,
                        away_team_code=away_team_code,
                        home_spread_line=home_spread_line,
                        total_line=total_line,
                        include_evidence=include_evidence,
                        evidence_dimensions=dimensions,
                        comparable_limit=comparable_limit,
                        min_pattern_sample_size=min_pattern_sample_size,
                        train_ratio=train_ratio,
                        validation_ratio=validation_ratio,
                    )
                else:
                    materialize_model_opportunities_postgres(
                        connection,
                        feature_key=feature_key,
                        target_task=target_task,
                        team_code=team_code,
                        season_label=season_label,
                        canonical_game_id=canonical_game_id,
                        limit=limit,
                        include_evidence=include_evidence,
                        evidence_dimensions=dimensions,
                        comparable_limit=comparable_limit,
                        min_pattern_sample_size=min_pattern_sample_size,
                        train_ratio=train_ratio,
                        validation_ratio=validation_ratio,
                    )
            history = get_model_opportunity_history_postgres(
                connection,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                source_kind=source_kind,
                scenario_key=scenario_key,
                recent_limit=recent_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "canonical_game_id": canonical_game_id,
            "source_kind": source_kind,
            "scenario_key": scenario_key,
            "game_date": game_date,
            "home_team_code": home_team_code,
            "away_team_code": away_team_code,
            "home_spread_line": home_spread_line,
            "total_line": total_line,
            "auto_train_demo": auto_train_demo,
            "auto_select_demo": auto_select_demo,
            "auto_materialize_demo": auto_materialize_demo,
            "recent_limit": recent_limit,
        },
        "model_opportunity_history": history,
    }


@router.get("/models/opportunities/{opportunity_id}")
def phase_three_model_opportunity_detail(
    opportunity_id: int,
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    auto_train_demo: bool = Query(default=True),
    auto_select_demo: bool = Query(default=True),
    auto_materialize_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    canonical_game_id: int | None = Query(default=None, ge=1),
    source_kind: str | None = Query(default=None),
    scenario_key: str | None = Query(default=None),
    game_date: date = Query(default=date(2026, 4, 20)),
    home_team_code: str = Query(default="LAL"),
    away_team_code: str = Query(default="BOS"),
    home_spread_line: float | None = Query(default=None),
    total_line: float | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    limit: int = Query(default=10, ge=1, le=100),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        if auto_train_demo:
            train_phase_three_models_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=None if source_kind == "future_scenario" else team_code,
                season_label=None if source_kind == "future_scenario" else season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        if auto_select_demo:
            promote_best_model_in_memory(repository, target_task=target_task)
        if auto_materialize_demo:
            if source_kind == "future_scenario":
                materialize_model_future_opportunities_in_memory(
                    repository,
                    feature_key=feature_key,
                    target_task=target_task,
                    season_label=season_label or "2025-2026",
                    game_date=game_date,
                    home_team_code=home_team_code,
                    away_team_code=away_team_code,
                    home_spread_line=home_spread_line,
                    total_line=total_line,
                    include_evidence=include_evidence,
                    evidence_dimensions=dimensions,
                    comparable_limit=comparable_limit,
                    min_pattern_sample_size=min_pattern_sample_size,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            else:
                materialize_model_opportunities_in_memory(
                    repository,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=team_code,
                    season_label=season_label,
                    canonical_game_id=canonical_game_id,
                    limit=limit,
                    include_evidence=include_evidence,
                    evidence_dimensions=dimensions,
                    comparable_limit=comparable_limit,
                    min_pattern_sample_size=min_pattern_sample_size,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
        opportunity = get_model_opportunity_detail_in_memory(
            repository,
            opportunity_id=opportunity_id,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            if auto_train_demo:
                train_phase_three_models_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=None if source_kind == "future_scenario" else team_code,
                    season_label=None if source_kind == "future_scenario" else season_label,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                )
            if auto_select_demo:
                promote_best_model_postgres(connection, target_task=target_task)
            if auto_materialize_demo:
                if source_kind == "future_scenario":
                    materialize_model_future_opportunities_postgres(
                        connection,
                        feature_key=feature_key,
                        target_task=target_task,
                        season_label=season_label or "2025-2026",
                        game_date=game_date,
                        home_team_code=home_team_code,
                        away_team_code=away_team_code,
                        home_spread_line=home_spread_line,
                        total_line=total_line,
                        include_evidence=include_evidence,
                        evidence_dimensions=dimensions,
                        comparable_limit=comparable_limit,
                        min_pattern_sample_size=min_pattern_sample_size,
                        train_ratio=train_ratio,
                        validation_ratio=validation_ratio,
                    )
                else:
                    materialize_model_opportunities_postgres(
                        connection,
                        feature_key=feature_key,
                        target_task=target_task,
                        team_code=team_code,
                        season_label=season_label,
                        canonical_game_id=canonical_game_id,
                        limit=limit,
                        include_evidence=include_evidence,
                        evidence_dimensions=dimensions,
                        comparable_limit=comparable_limit,
                        min_pattern_sample_size=min_pattern_sample_size,
                        train_ratio=train_ratio,
                        validation_ratio=validation_ratio,
                    )
            opportunity = get_model_opportunity_detail_postgres(
                connection,
                opportunity_id=opportunity_id,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "canonical_game_id": canonical_game_id,
            "source_kind": source_kind,
            "scenario_key": scenario_key,
            "game_date": game_date,
            "home_team_code": home_team_code,
            "away_team_code": away_team_code,
            "home_spread_line": home_spread_line,
            "total_line": total_line,
            "auto_train_demo": auto_train_demo,
            "auto_select_demo": auto_select_demo,
            "auto_materialize_demo": auto_materialize_demo,
        },
        "opportunity": opportunity,
    }


@router.get("/features/snapshots")
def feature_snapshots(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        snapshot_result = get_feature_snapshot_catalog_in_memory(
            repository,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
            limit=limit,
            offset=offset,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            snapshot_result = get_feature_snapshot_catalog_postgres(
                connection,
                feature_key=feature_key,
                team_code=team_code,
                season_label=season_label,
                limit=limit,
                offset=offset,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "team_code": team_code,
            "season_label": season_label,
            "limit": limit,
            "offset": offset,
        },
        **snapshot_result,
    }


@router.get("/features/summary")
def feature_summary(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        summary_result = get_feature_snapshot_summary_in_memory(
            repository,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            summary_result = get_feature_snapshot_summary_postgres(
                connection,
                feature_key=feature_key,
                team_code=team_code,
                season_label=season_label,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "team_code": team_code,
            "season_label": season_label,
        },
        **summary_result,
    }


@router.get("/features/dataset")
def feature_dataset(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        dataset_result = get_feature_dataset_in_memory(
            repository,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
            limit=limit,
            offset=offset,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            dataset_result = get_feature_dataset_postgres(
                connection,
                feature_key=feature_key,
                team_code=team_code,
                season_label=season_label,
                limit=limit,
                offset=offset,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "team_code": team_code,
            "season_label": season_label,
            "limit": limit,
            "offset": offset,
        },
        **dataset_result,
    }


@router.get("/features/dataset/profile")
def feature_dataset_profile(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        profile_result = get_feature_dataset_profile_in_memory(
            repository,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            profile_result = get_feature_dataset_profile_postgres(
                connection,
                feature_key=feature_key,
                team_code=team_code,
                season_label=season_label,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "team_code": team_code,
            "season_label": season_label,
        },
        **profile_result,
    }


@router.get("/features/patterns")
def feature_patterns(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    dimensions: str = Query(default="venue,days_rest_bucket"),
    min_sample_size: int = Query(default=2, ge=1, le=100),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, object]:
    parsed_dimensions = tuple(
        dimension.strip() for dimension in dimensions.split(",") if dimension.strip()
    )
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        pattern_result = get_feature_pattern_catalog_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            dimensions=parsed_dimensions,
            min_sample_size=min_sample_size,
            limit=limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            pattern_result = get_feature_pattern_catalog_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                dimensions=parsed_dimensions,
                min_sample_size=min_sample_size,
                limit=limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "dimensions": list(parsed_dimensions),
            "min_sample_size": min_sample_size,
            "limit": limit,
        },
        **pattern_result,
    }


@router.get("/features/comparables")
def feature_comparables(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    dimensions: str = Query(default="venue,days_rest_bucket"),
    canonical_game_id: int | None = Query(default=None, ge=1),
    condition_values: str | None = Query(default=None),
    pattern_key: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, object]:
    parsed_dimensions = tuple(
        dimension.strip() for dimension in dimensions.split(",") if dimension.strip()
    )
    parsed_condition_values = (
        tuple(value.strip() for value in condition_values.split(","))
        if condition_values is not None
        else None
    )
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        comparable_result = get_feature_comparable_cases_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            dimensions=parsed_dimensions,
            canonical_game_id=canonical_game_id,
            condition_values=parsed_condition_values,
            pattern_key=pattern_key,
            limit=limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            comparable_result = get_feature_comparable_cases_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                dimensions=parsed_dimensions,
                canonical_game_id=canonical_game_id,
                condition_values=parsed_condition_values,
                pattern_key=pattern_key,
                limit=limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "dimensions": list(parsed_dimensions),
            "canonical_game_id": canonical_game_id,
            "condition_values": list(parsed_condition_values)
            if parsed_condition_values is not None
            else None,
            "pattern_key": pattern_key,
            "limit": limit,
        },
        **comparable_result,
    }


@router.get("/features/evidence")
def feature_evidence(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    dimensions: str = Query(default="venue,days_rest_bucket"),
    canonical_game_id: int | None = Query(default=None, ge=1),
    condition_values: str | None = Query(default=None),
    pattern_key: str | None = Query(default=None),
    comparable_limit: int = Query(default=10, ge=1, le=100),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    drop_null_targets: bool = Query(default=True),
) -> dict[str, object]:
    parsed_dimensions = tuple(
        dimension.strip() for dimension in dimensions.split(",") if dimension.strip()
    )
    parsed_condition_values = (
        tuple(value.strip() for value in condition_values.split(","))
        if condition_values is not None
        else None
    )
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        evidence_result = get_feature_evidence_bundle_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            dimensions=parsed_dimensions,
            canonical_game_id=canonical_game_id,
            condition_values=parsed_condition_values,
            pattern_key=pattern_key,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            evidence_result = get_feature_evidence_bundle_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                dimensions=parsed_dimensions,
                canonical_game_id=canonical_game_id,
                condition_values=parsed_condition_values,
                pattern_key=pattern_key,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                drop_null_targets=drop_null_targets,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "dimensions": list(parsed_dimensions),
            "canonical_game_id": canonical_game_id,
            "condition_values": list(parsed_condition_values)
            if parsed_condition_values is not None
            else None,
            "pattern_key": pattern_key,
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "drop_null_targets": drop_null_targets,
        },
        **evidence_result,
    }


@router.post("/features/analysis/materialize")
def materialize_feature_analysis(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    dimensions: str = Query(default="venue,days_rest_bucket"),
    min_sample_size: int = Query(default=2, ge=1, le=100),
    canonical_game_id: int | None = Query(default=None, ge=1),
    condition_values: str | None = Query(default=None),
    pattern_key: str | None = Query(default=None),
    comparable_limit: int = Query(default=10, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    drop_null_targets: bool = Query(default=True),
) -> dict[str, object]:
    parsed_dimensions = tuple(
        dimension.strip() for dimension in dimensions.split(",") if dimension.strip()
    )
    parsed_condition_values = (
        tuple(value.strip() for value in condition_values.split(","))
        if condition_values is not None
        else None
    )
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        materialize_result = materialize_feature_analysis_artifacts_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            dimensions=parsed_dimensions,
            min_sample_size=min_sample_size,
            canonical_game_id=canonical_game_id,
            condition_values=parsed_condition_values,
            pattern_key=pattern_key,
            comparable_limit=comparable_limit,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            materialize_result = materialize_feature_analysis_artifacts_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                dimensions=parsed_dimensions,
                min_sample_size=min_sample_size,
                canonical_game_id=canonical_game_id,
                condition_values=parsed_condition_values,
                pattern_key=pattern_key,
                comparable_limit=comparable_limit,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                drop_null_targets=drop_null_targets,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "dimensions": list(parsed_dimensions),
            "min_sample_size": min_sample_size,
            "canonical_game_id": canonical_game_id,
            "condition_values": list(parsed_condition_values)
            if parsed_condition_values is not None
            else None,
            "pattern_key": pattern_key,
            "comparable_limit": comparable_limit,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "drop_null_targets": drop_null_targets,
        },
        **materialize_result,
    }


@router.get("/features/analysis/artifacts")
def feature_analysis_artifacts(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    artifact_type: str | None = Query(default=None),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    dimensions: str = Query(default="venue,days_rest_bucket"),
    min_sample_size: int = Query(default=2, ge=1, le=100),
    canonical_game_id: int | None = Query(default=None, ge=1),
    condition_values: str | None = Query(default=None),
    pattern_key: str | None = Query(default=None),
    comparable_limit: int = Query(default=10, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    drop_null_targets: bool = Query(default=True),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, object]:
    parsed_dimensions = tuple(
        dimension.strip() for dimension in dimensions.split(",") if dimension.strip()
    )
    parsed_condition_values = (
        tuple(value.strip() for value in condition_values.split(","))
        if condition_values is not None
        else None
    )
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
            materialize_feature_analysis_artifacts_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                dimensions=parsed_dimensions,
                min_sample_size=min_sample_size,
                canonical_game_id=canonical_game_id,
                condition_values=parsed_condition_values,
                pattern_key=pattern_key,
                comparable_limit=comparable_limit,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                drop_null_targets=drop_null_targets,
            )
        artifact_result = get_feature_analysis_artifact_catalog_in_memory(
            repository,
            feature_key=feature_key,
            artifact_type=artifact_type,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            limit=limit,
            offset=offset,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
                materialize_feature_analysis_artifacts_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=team_code,
                    season_label=season_label,
                    dimensions=parsed_dimensions,
                    min_sample_size=min_sample_size,
                    canonical_game_id=canonical_game_id,
                    condition_values=parsed_condition_values,
                    pattern_key=pattern_key,
                    comparable_limit=comparable_limit,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                    drop_null_targets=drop_null_targets,
                )
            artifact_result = get_feature_analysis_artifact_catalog_postgres(
                connection,
                feature_key=feature_key,
                artifact_type=artifact_type,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                limit=limit,
                offset=offset,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "artifact_type": artifact_type,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "dimensions": list(parsed_dimensions),
            "min_sample_size": min_sample_size,
            "canonical_game_id": canonical_game_id,
            "condition_values": list(parsed_condition_values)
            if parsed_condition_values is not None
            else None,
            "pattern_key": pattern_key,
            "comparable_limit": comparable_limit,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "drop_null_targets": drop_null_targets,
            "limit": limit,
            "offset": offset,
        },
        **artifact_result,
    }


@router.get("/features/analysis/history")
def feature_analysis_history(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    artifact_type: str | None = Query(default=None),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    dimensions: str = Query(default="venue,days_rest_bucket"),
    min_sample_size: int = Query(default=2, ge=1, le=100),
    canonical_game_id: int | None = Query(default=None, ge=1),
    condition_values: str | None = Query(default=None),
    pattern_key: str | None = Query(default=None),
    comparable_limit: int = Query(default=10, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    drop_null_targets: bool = Query(default=True),
    latest_limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, object]:
    parsed_dimensions = tuple(
        dimension.strip() for dimension in dimensions.split(",") if dimension.strip()
    )
    parsed_condition_values = (
        tuple(value.strip() for value in condition_values.split(","))
        if condition_values is not None
        else None
    )
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
            materialize_feature_analysis_artifacts_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                dimensions=parsed_dimensions,
                min_sample_size=min_sample_size,
                canonical_game_id=canonical_game_id,
                condition_values=parsed_condition_values,
                pattern_key=pattern_key,
                comparable_limit=comparable_limit,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                drop_null_targets=drop_null_targets,
            )
        history_result = get_feature_analysis_artifact_history_in_memory(
            repository,
            feature_key=feature_key,
            artifact_type=artifact_type,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            latest_limit=latest_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
                materialize_feature_analysis_artifacts_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
                    team_code=team_code,
                    season_label=season_label,
                    dimensions=parsed_dimensions,
                    min_sample_size=min_sample_size,
                    canonical_game_id=canonical_game_id,
                    condition_values=parsed_condition_values,
                    pattern_key=pattern_key,
                    comparable_limit=comparable_limit,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                    drop_null_targets=drop_null_targets,
                )
            history_result = get_feature_analysis_artifact_history_postgres(
                connection,
                feature_key=feature_key,
                artifact_type=artifact_type,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                latest_limit=latest_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "artifact_type": artifact_type,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "dimensions": list(parsed_dimensions),
            "min_sample_size": min_sample_size,
            "canonical_game_id": canonical_game_id,
            "condition_values": list(parsed_condition_values)
            if parsed_condition_values is not None
            else None,
            "pattern_key": pattern_key,
            "comparable_limit": comparable_limit,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "drop_null_targets": drop_null_targets,
            "latest_limit": latest_limit,
        },
        **history_result,
    }


@router.get("/features/dataset/splits")
def feature_dataset_splits(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    preview_limit: int = Query(default=5, ge=1, le=20),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        split_result = get_feature_dataset_splits_in_memory(
            repository,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            preview_limit=preview_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            split_result = get_feature_dataset_splits_postgres(
                connection,
                feature_key=feature_key,
                team_code=team_code,
                season_label=season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                preview_limit=preview_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "team_code": team_code,
            "season_label": season_label,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "preview_limit": preview_limit,
        },
        **split_result,
    }


@router.get("/features/dataset/training-view")
def feature_dataset_training_view(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    drop_null_targets: bool = Query(default=True),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        training_view = get_feature_training_view_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            drop_null_targets=drop_null_targets,
            limit=limit,
            offset=offset,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            training_view = get_feature_training_view_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                drop_null_targets=drop_null_targets,
                limit=limit,
                offset=offset,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "drop_null_targets": drop_null_targets,
            "limit": limit,
            "offset": offset,
        },
        **training_view,
    }


@router.get("/features/dataset/training-manifest")
def feature_dataset_training_manifest(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    drop_null_targets: bool = Query(default=True),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        training_manifest = get_feature_training_manifest_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            drop_null_targets=drop_null_targets,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            training_manifest = get_feature_training_manifest_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                drop_null_targets=drop_null_targets,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "drop_null_targets": drop_null_targets,
        },
        **training_manifest,
    }


@router.get("/features/dataset/training-bundle")
def feature_dataset_training_bundle(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    drop_null_targets: bool = Query(default=True),
    preview_limit: int = Query(default=5, ge=1, le=20),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        training_bundle = get_feature_training_bundle_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
            preview_limit=preview_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            training_bundle = get_feature_training_bundle_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                drop_null_targets=drop_null_targets,
                preview_limit=preview_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "drop_null_targets": drop_null_targets,
            "preview_limit": preview_limit,
        },
        **training_bundle,
    }


@router.get("/features/dataset/training-benchmark")
def feature_dataset_training_benchmark(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    drop_null_targets: bool = Query(default=True),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        training_benchmark = get_feature_training_benchmark_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            training_benchmark = get_feature_training_benchmark_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                drop_null_targets=drop_null_targets,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "drop_null_targets": drop_null_targets,
        },
        **training_benchmark,
    }


@router.get("/features/dataset/training-task-matrix")
def feature_dataset_training_task_matrix(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    drop_null_targets: bool = Query(default=True),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        training_task_matrix = get_feature_training_task_matrix_in_memory(
            repository,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            training_task_matrix = get_feature_training_task_matrix_postgres(
                connection,
                feature_key=feature_key,
                team_code=team_code,
                season_label=season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                drop_null_targets=drop_null_targets,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "team_code": team_code,
            "season_label": season_label,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "drop_null_targets": drop_null_targets,
        },
        **training_task_matrix,
    }


@router.get("/jobs/recent")
def recent_job_runs(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    status: str | None = Query(default=None),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
    started_from: date | None = Query(default=None),
    started_to: date | None = Query(default=None),
) -> dict[str, object]:
    resolved_started_from, resolved_started_to = resolve_started_window(
        started_from=started_from,
        started_to=started_to,
    )
    diagnostics = get_admin_diagnostics(
        repository_mode=repository_mode,
        seed_demo=seed_demo,
        job_limit=limit,
        job_offset=offset,
        retrieval_limit=20,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
        job_status=status,
        started_from=resolved_started_from,
        started_to=resolved_started_to,
    )
    return {
        "repository_mode": diagnostics["repository_mode"],
        "filters": diagnostics["filters"],
        "job_runs": diagnostics["job_runs"],
    }


@router.get("/ingestion/issues")
def recent_ingestion_issues(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    status: str = Query(default="FAILED"),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
) -> dict[str, object]:
    diagnostics = get_admin_diagnostics(
        repository_mode=repository_mode,
        seed_demo=seed_demo,
        job_limit=20,
        retrieval_limit=limit,
        retrieval_offset=offset,
        retrieval_status=status,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
    )
    return {
        "repository_mode": diagnostics["repository_mode"],
        "filters": diagnostics["filters"],
        "page_retrievals": diagnostics["page_retrievals"],
    }


@router.get("/data-quality/issues")
def recent_data_quality_issues(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    severity: str | None = Query(default=None),
    issue_type: str | None = Query(default=None),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
) -> dict[str, object]:
    diagnostics = get_admin_diagnostics(
        repository_mode=repository_mode,
        seed_demo=seed_demo,
        quality_issue_limit=limit,
        quality_issue_offset=offset,
        quality_issue_severity=severity,
        quality_issue_type=issue_type,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
    )
    return {
        "repository_mode": diagnostics["repository_mode"],
        "filters": diagnostics["filters"],
        "data_quality_issues": diagnostics["data_quality_issues"],
    }


@router.get("/ingestion/stats")
def ingestion_stats(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
) -> dict[str, object]:
    diagnostics = get_admin_diagnostics(
        repository_mode=repository_mode,
        seed_demo=seed_demo,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
    )
    return {
        "repository_mode": diagnostics["repository_mode"],
        "filters": diagnostics["filters"],
        "stats": diagnostics["stats"],
    }


@router.get("/validation-runs/compare")
def compare_validation_runs(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    run_label: str = Query(..., min_length=1),
    limit: int = Query(default=10, ge=2, le=50),
    status: str | None = Query(default=None),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    started_from: date | None = Query(default=None),
    started_to: date | None = Query(default=None),
) -> dict[str, object]:
    resolved_started_from, resolved_started_to = resolve_started_window(
        started_from=started_from,
        started_to=started_to,
    )
    diagnostics = get_admin_diagnostics(
        repository_mode=repository_mode,
        seed_demo=seed_demo,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
        job_status=status,
        validation_compare_limit=limit,
        started_from=resolved_started_from,
        started_to=resolved_started_to,
    )
    return {
        "repository_mode": diagnostics["repository_mode"],
        "filters": diagnostics["filters"],
        "validation_run_comparison": diagnostics["validation_run_comparison"],
    }


@router.get("/ingestion/trends")
def ingestion_trends(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    limit: int = Query(default=20, ge=1, le=100),
    days: int | None = Query(default=7, ge=1, le=365),
    started_from: date | None = Query(default=None),
    started_to: date | None = Query(default=None),
    status: str | None = Query(default=None),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
) -> dict[str, object]:
    resolved_started_from, resolved_started_to = resolve_started_window(
        started_from=started_from,
        started_to=started_to,
        days=days,
    )
    diagnostics = get_admin_diagnostics(
        repository_mode=repository_mode,
        seed_demo=seed_demo,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
        job_status=status,
        trend_limit=limit,
        started_from=resolved_started_from,
        started_to=resolved_started_to,
    )
    return {
        "repository_mode": diagnostics["repository_mode"],
        "filters": diagnostics["filters"],
        "trends": diagnostics["trends"],
    }


@router.get("/retrieval/trends")
def retrieval_trends(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    days: int | None = Query(default=7, ge=1, le=365),
    started_from: date | None = Query(default=None),
    started_to: date | None = Query(default=None),
    status: str | None = Query(default=None),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
) -> dict[str, object]:
    resolved_started_from, resolved_started_to = resolve_started_window(
        started_from=started_from,
        started_to=started_to,
        days=days,
    )
    diagnostics = get_admin_diagnostics(
        repository_mode=repository_mode,
        seed_demo=seed_demo,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
        retrieval_status=status,
        started_from=resolved_started_from,
        started_to=resolved_started_to,
    )
    return {
        "repository_mode": diagnostics["repository_mode"],
        "filters": diagnostics["filters"],
        "retrieval_trends": diagnostics["retrieval_trends"],
    }


@router.get("/ingestion/quality-trends")
def ingestion_quality_trends(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    days: int | None = Query(default=7, ge=1, le=365),
    started_from: date | None = Query(default=None),
    started_to: date | None = Query(default=None),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
) -> dict[str, object]:
    resolved_started_from, resolved_started_to = resolve_started_window(
        started_from=started_from,
        started_to=started_to,
        days=days,
    )
    diagnostics = get_admin_diagnostics(
        repository_mode=repository_mode,
        seed_demo=seed_demo,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
        started_from=resolved_started_from,
        started_to=resolved_started_to,
    )
    return {
        "repository_mode": diagnostics["repository_mode"],
        "filters": diagnostics["filters"],
        "quality_trends": diagnostics["quality_trends"],
    }


@router.post("/data-quality/normalize-taxonomy")
def normalize_data_quality_issue_taxonomy(
    repository_mode: str = Query(default="in_memory"),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    dry_run: bool = Query(default=True),
) -> dict[str, object]:
    return normalize_data_quality_taxonomy(
        repository_mode=repository_mode,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        dry_run=dry_run,
    )
