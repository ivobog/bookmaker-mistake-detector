from datetime import date

from fastapi import APIRouter, Body, Query
from pydantic import BaseModel, Field

from bookmaker_detector_api.config import settings
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
    get_feature_dataset_in_memory,
    get_feature_dataset_postgres,
    get_feature_dataset_profile_in_memory,
    get_feature_dataset_profile_postgres,
    get_feature_dataset_splits_in_memory,
    get_feature_dataset_splits_postgres,
    get_feature_snapshot_catalog_in_memory,
    get_feature_snapshot_catalog_postgres,
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
    get_model_backtest_history_in_memory,
    get_model_backtest_history_postgres,
    get_model_evaluation_history_in_memory,
    get_model_evaluation_history_postgres,
    get_model_evaluation_snapshot_detail_in_memory,
    get_model_evaluation_snapshot_detail_postgres,
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
    list_model_market_board_sources,
    list_model_market_boards_in_memory,
    list_model_market_boards_postgres,
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
    run_model_backtest_in_memory,
    run_model_backtest_postgres,
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


def _use_postgres_stable_read_mode() -> bool:
    return settings.api_env.lower() == "production"


def _prepare_in_memory_backtest_history_repository(
    *,
    feature_key: str,
    target_task: str,
    team_code: str | None,
    season_label: str | None,
    selection_policy_name: str,
    minimum_train_games: int,
    test_window_games: int,
    train_ratio: float,
    validation_ratio: float,
) -> InMemoryIngestionRepository:
    repository, _, _ = seed_phase_two_feature_in_memory()
    run_model_backtest_in_memory(
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
    return repository


def _prepare_in_memory_opportunity_history_repository(
    *,
    feature_key: str,
    target_task: str | None,
    team_code: str | None,
    season_label: str | None,
    canonical_game_id: int | None,
    source_kind: str | None,
    game_date: date,
    home_team_code: str,
    away_team_code: str,
    home_spread_line: float | None,
    total_line: float | None,
    train_ratio: float,
    validation_ratio: float,
    limit: int,
    include_evidence: bool,
    dimensions: tuple[str, ...],
    comparable_limit: int,
    min_pattern_sample_size: int,
) -> InMemoryIngestionRepository:
    repository, _, _ = seed_phase_two_feature_in_memory()
    if target_task is not None:
        train_phase_three_models_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=None if source_kind == "future_scenario" else team_code,
            season_label=None if source_kind == "future_scenario" else season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
        promote_best_model_in_memory(repository, target_task=target_task)
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
    return repository


def _prepare_in_memory_phase_three_model_repository(
    *,
    feature_key: str,
    target_task: str | None,
    team_code: str | None,
    season_label: str | None,
    train_ratio: float,
    validation_ratio: float,
    promote_best: bool = False,
) -> InMemoryIngestionRepository:
    repository, _, _ = seed_phase_two_feature_in_memory()
    if target_task is not None:
        train_phase_three_models_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
        if promote_best:
            promote_best_model_in_memory(repository, target_task=target_task)
    return repository


def _prepare_in_memory_future_game_scoring_repository(
    *,
    feature_key: str,
    target_task: str | None,
    season_label: str,
    game_date: date,
    home_team_code: str,
    away_team_code: str,
    home_spread_line: float | None,
    total_line: float | None,
    include_evidence: bool,
    dimensions: tuple[str, ...],
    comparable_limit: int,
    min_pattern_sample_size: int,
    train_ratio: float,
    validation_ratio: float,
    materialize_preview: bool = False,
) -> InMemoryIngestionRepository:
    repository = _prepare_in_memory_phase_three_model_repository(
        feature_key=feature_key,
        target_task=target_task,
        team_code=None,
        season_label=None,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        promote_best=target_task is not None,
    )
    if materialize_preview and target_task is not None:
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
    return repository


def _prepare_in_memory_future_slate_repository(
    *,
    feature_key: str,
    target_task: str,
    games: list[dict[str, object]],
    slate_label: str | None,
    include_evidence: bool,
    dimensions: tuple[str, ...],
    comparable_limit: int,
    min_pattern_sample_size: int,
    train_ratio: float,
    validation_ratio: float,
    materialize_slate: bool = False,
) -> InMemoryIngestionRepository:
    repository = _prepare_in_memory_phase_three_model_repository(
        feature_key=feature_key,
        target_task=target_task,
        team_code=None,
        season_label=None,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        promote_best=True,
    )
    if materialize_slate:
        materialize_model_future_slate_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            games=games,
            slate_label=slate_label,
            include_evidence=include_evidence,
            evidence_dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
    return repository


def _prepare_in_memory_market_board_refresh_repository(
    *,
    target_task: str,
    source_name: str,
    season_label: str,
    game_date: date,
    slate_label: str | None,
    game_count: int | None,
    source_path: str | None = None,
) -> InMemoryIngestionRepository:
    repository = InMemoryIngestionRepository()
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
    return repository


def _prepare_in_memory_market_board_materialized_repository(
    *,
    target_task: str | None,
    season_label: str | None,
    slate_label: str | None,
    game_date: date,
    home_team_code: str,
    away_team_code: str,
    home_spread_line: float | None,
    total_line: float | None,
) -> InMemoryIngestionRepository:
    repository = InMemoryIngestionRepository()
    if target_task is not None:
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
    return repository


def _prepare_in_memory_market_board_score_repository(
    *,
    board_id: int,
    feature_key: str,
    target_task: str,
    season_label: str,
    slate_label: str | None,
    game_date: date,
    home_team_code: str,
    away_team_code: str,
    home_spread_line: float | None,
    total_line: float | None,
    train_ratio: float,
    validation_ratio: float,
) -> tuple[InMemoryIngestionRepository, str]:
    repository, _, _ = seed_phase_two_feature_in_memory()
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
    resolved_target_task = str(board["target_task"]) if board is not None else target_task
    train_phase_three_models_in_memory(
        repository,
        feature_key=feature_key,
        target_task=resolved_target_task,
        team_code=None,
        season_label=None,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
    )
    promote_best_model_in_memory(repository, target_task=resolved_target_task)
    return repository, resolved_target_task


def _prepare_in_memory_market_board_orchestration_repository(
    *,
    target_task: str,
    source_name: str | None,
    season_label: str,
    game_date: date,
    slate_label: str | None,
    game_count: int | None,
    feature_key: str,
    train_ratio: float,
    validation_ratio: float,
    refresh_freshness_status: str | None = None,
    refresh_pending_only: bool = False,
    scoring_freshness_status: str | None = "fresh",
    scoring_pending_only: bool = True,
    recent_limit: int = 10,
    run_refresh_orchestration: bool = False,
    run_scoring_orchestration: bool = False,
    run_cadence_orchestration: bool = False,
) -> InMemoryIngestionRepository:
    repository, _, _ = seed_phase_two_feature_in_memory()
    resolved_source_name = source_name or "demo_daily_lines_v1"
    refresh_model_market_board_in_memory(
        repository,
        target_task=target_task,
        source_name=resolved_source_name,
        season_label=season_label,
        game_date=game_date,
        slate_label=slate_label,
        game_count=game_count,
    )
    train_phase_three_models_in_memory(
        repository,
        feature_key=feature_key,
        target_task=target_task,
        team_code=None,
        season_label=None,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
    )
    promote_best_model_in_memory(repository, target_task=target_task)
    if run_refresh_orchestration:
        orchestrate_model_market_board_refresh_in_memory(
            repository,
            target_task=target_task,
            season_label=season_label,
            source_name=source_name,
            freshness_status=refresh_freshness_status,
            pending_only=refresh_pending_only,
            recent_limit=recent_limit,
        )
    if run_scoring_orchestration:
        orchestrate_model_market_board_scoring_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            season_label=season_label,
            source_name=source_name,
            freshness_status=scoring_freshness_status,
            pending_only=scoring_pending_only,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
    if run_cadence_orchestration:
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
    return repository


def _prepare_in_memory_feature_repository() -> InMemoryIngestionRepository:
    repository, _, _ = seed_phase_two_feature_in_memory()
    return repository


def _prepare_in_memory_feature_analysis_repository(
    *,
    feature_key: str,
    target_task: str,
    team_code: str | None,
    season_label: str | None,
    dimensions: tuple[str, ...],
    min_sample_size: int,
    canonical_game_id: int | None,
    condition_values: tuple[str, ...] | None,
    pattern_key: str | None,
    comparable_limit: int,
    train_ratio: float,
    validation_ratio: float,
    drop_null_targets: bool,
) -> InMemoryIngestionRepository:
    repository = _prepare_in_memory_feature_repository()
    materialize_feature_analysis_artifacts_in_memory(
        repository,
        feature_key=feature_key,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        dimensions=dimensions,
        min_sample_size=min_sample_size,
        canonical_game_id=canonical_game_id,
        condition_values=condition_values,
        pattern_key=pattern_key,
        comparable_limit=comparable_limit,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
    )
    return repository


def _run_admin_diagnostics_stable_read(**kwargs) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        return get_admin_diagnostics(repository_mode="postgres", seed_demo=False, **kwargs)
    return get_admin_diagnostics(repository_mode="in_memory", seed_demo=True, **kwargs)


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
def phase_one_fetch_reporting_demo() -> dict[str, object]:
    repository_mode = "postgres" if _use_postgres_stable_read_mode() else "in_memory"
    return run_phase_one_fetch_reporting_demo_job(repository_mode=repository_mode)


@router.get("/phase-2-feature-demo")
def phase_two_feature_demo() -> dict[str, object]:
    repository_mode = "postgres" if _use_postgres_stable_read_mode() else "in_memory"
    return run_phase_two_feature_demo_job(repository_mode=repository_mode)


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
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    selection_policy_name: str = Query(default="validation_mae_candidate_v1"),
    minimum_train_games: int = Query(default=1, ge=1),
    test_window_games: int = Query(default=1, ge=1),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            history = get_model_backtest_history_postgres(
                connection,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                recent_limit=recent_limit,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_backtest_history_repository(
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
        history = get_model_backtest_history_in_memory(
            repository,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            recent_limit=recent_limit,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "recent_limit": recent_limit,
        },
        "model_backtest_history": history,
    }


@router.get("/models/registry")
def phase_three_model_registry(
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            registries = list_model_registry_postgres(
                connection,
                target_task=target_task,
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
        registries = list_model_registry_in_memory(
            repository,
            target_task=target_task,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
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
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            runs = list_model_training_runs_postgres(
                connection,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
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
        runs = list_model_training_runs_in_memory(
            repository,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
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


@router.get("/models/runs/{run_id}")
def phase_three_model_run_detail(
    run_id: int,
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            run = get_model_training_run_detail_postgres(connection, run_id=run_id)
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
        run = get_model_training_run_detail_in_memory(repository, run_id=run_id)
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "run_id": run_id,
        },
        "model_run": (
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
                "completed_at": run.completed_at.isoformat() if run.completed_at else None,
            }
            if run is not None
            else None
        ),
    }


@router.get("/models/summary")
def phase_three_model_summary(
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            summary = get_model_training_summary_postgres(
                connection,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
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
        summary = get_model_training_summary_in_memory(
            repository,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
        },
        "model_summary": summary,
    }


@router.get("/models/history")
def phase_three_model_history(
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            history = get_model_training_history_postgres(
                connection,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                recent_limit=recent_limit,
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
        history = get_model_training_history_in_memory(
            repository,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            recent_limit=recent_limit,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "recent_limit": recent_limit,
        },
        "model_history": history,
    }


@router.get("/models/evaluations")
def phase_three_model_evaluations(
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    model_family: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            snapshots = list_model_evaluation_snapshots_postgres(
                connection,
                target_task=target_task,
                model_family=model_family,
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
        snapshots = list_model_evaluation_snapshots_in_memory(
            repository,
            target_task=target_task,
            model_family=model_family,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "model_family": model_family,
            "team_code": team_code,
            "season_label": season_label,
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
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    model_family: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            history = get_model_evaluation_history_postgres(
                connection,
                target_task=target_task,
                model_family=model_family,
                recent_limit=recent_limit,
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
        history = get_model_evaluation_history_in_memory(
            repository,
            target_task=target_task,
            model_family=model_family,
            recent_limit=recent_limit,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "model_family": model_family,
            "team_code": team_code,
            "season_label": season_label,
            "recent_limit": recent_limit,
        },
        "model_evaluation_history": history,
    }


@router.get("/models/evaluations/{snapshot_id}")
def phase_three_model_evaluation_detail(
    snapshot_id: int,
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    model_family: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            snapshot = get_model_evaluation_snapshot_detail_postgres(
                connection,
                snapshot_id=snapshot_id,
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
        snapshot = get_model_evaluation_snapshot_detail_in_memory(
            repository,
            snapshot_id=snapshot_id,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "model_family": model_family,
            "team_code": team_code,
            "season_label": season_label,
            "snapshot_id": snapshot_id,
        },
        "evaluation_snapshot": (
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
                "created_at": snapshot.created_at.isoformat()
                if snapshot.created_at
                else None,
            }
            if snapshot is not None
            else None
        ),
    }


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
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    active_only: bool = Query(default=False),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            selections = list_model_selection_snapshots_postgres(
                connection,
                target_task=target_task,
                active_only=active_only,
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
            promote_best=True,
        )
        selections = list_model_selection_snapshots_in_memory(
            repository,
            target_task=target_task,
            active_only=active_only,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
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
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            history = get_model_selection_history_postgres(
                connection,
                target_task=target_task,
                recent_limit=recent_limit,
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
            promote_best=True,
        )
        history = get_model_selection_history_in_memory(
            repository,
            target_task=target_task,
            recent_limit=recent_limit,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "recent_limit": recent_limit,
        },
        "model_selection_history": history,
    }


@router.get("/models/selections/{selection_id}")
def phase_three_model_selection_detail(
    selection_id: int,
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            selection = get_model_selection_snapshot_detail_postgres(
                connection,
                selection_id=selection_id,
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
            promote_best=True,
        )
        selection = get_model_selection_snapshot_detail_in_memory(
            repository,
            selection_id=selection_id,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "selection_id": selection_id,
        },
        "selection": (
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
            if selection is not None
            else None
        ),
    }


@router.get("/models/score-preview")
def phase_three_model_score_preview(
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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
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
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_phase_three_model_repository(
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            promote_best=True,
        )
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
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "canonical_game_id": canonical_game_id,
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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
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
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_future_game_scoring_repository(
            feature_key=feature_key,
            target_task=target_task,
            season_label=season_label,
            game_date=game_date,
            home_team_code=home_team_code,
            away_team_code=away_team_code,
            home_spread_line=home_spread_line,
            total_line=total_line,
            include_evidence=include_evidence,
            dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
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
        repository_mode = "in_memory"

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
            "include_evidence": include_evidence,
            "dimensions": list(dimensions),
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
        },
        **preview,
    }


@router.post("/models/future-game-preview/materialize")
def phase_three_model_future_game_preview_materialize(
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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
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
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_future_game_scoring_repository(
            feature_key=feature_key,
            target_task=target_task,
            season_label=season_label,
            game_date=game_date,
            home_team_code=home_team_code,
            away_team_code=away_team_code,
            home_spread_line=home_spread_line,
            total_line=total_line,
            include_evidence=include_evidence,
            dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
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
        repository_mode = "in_memory"

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
            "include_evidence": include_evidence,
            "dimensions": list(dimensions),
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
        },
        **materialized,
    }


@router.get("/models/future-game-preview/runs")
def phase_three_model_future_game_preview_runs(
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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            scoring_runs = list_model_scoring_runs_postgres(
                connection,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_future_game_scoring_repository(
            feature_key=feature_key,
            target_task=target_task,
            season_label=season_label,
            game_date=game_date,
            home_team_code=home_team_code,
            away_team_code=away_team_code,
            home_spread_line=home_spread_line,
            total_line=total_line,
            include_evidence=include_evidence,
            dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            materialize_preview=True,
        )
        scoring_runs = list_model_scoring_runs_in_memory(
            repository,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
        )
        repository_mode = "in_memory"

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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            scoring_run = get_model_scoring_run_detail_postgres(
                connection,
                scoring_run_id=scoring_run_id,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_future_game_scoring_repository(
            feature_key=feature_key,
            target_task=target_task,
            season_label=season_label,
            game_date=game_date,
            home_team_code=home_team_code,
            away_team_code=away_team_code,
            home_spread_line=home_spread_line,
            total_line=total_line,
            include_evidence=include_evidence,
            dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            materialize_preview=True,
        )
        scoring_run = get_model_scoring_run_detail_in_memory(
            repository,
            scoring_run_id=scoring_run_id,
        )
        repository_mode = "in_memory"

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
        },
        "scoring_run": scoring_run,
    }


@router.get("/models/future-game-preview/history")
def phase_three_model_future_game_preview_history(
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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            history = get_model_scoring_history_postgres(
                connection,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                recent_limit=recent_limit,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_future_game_scoring_repository(
            feature_key=feature_key,
            target_task=target_task,
            season_label=season_label,
            game_date=game_date,
            home_team_code=home_team_code,
            away_team_code=away_team_code,
            home_spread_line=home_spread_line,
            total_line=total_line,
            include_evidence=include_evidence,
            dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            materialize_preview=True,
        )
        history = get_model_scoring_history_in_memory(
            repository,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            recent_limit=recent_limit,
        )
        repository_mode = "in_memory"

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
            "recent_limit": recent_limit,
        },
        "model_scoring_history": history,
    }


@router.get("/models/market-board/sources")
def phase_three_model_market_board_sources() -> dict[str, object]:
    return list_model_market_board_sources()


@router.post("/models/market-board/refresh")
def phase_three_model_market_board_refresh(
    target_task: str = Query(default="spread_error_regression"),
    source_name: str = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default=None),
    game_count: int | None = Query(default=None, ge=1, le=20),
    source_path: str | None = Query(default=None),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
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
        repository_mode = "postgres"
    else:
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
        repository_mode = "in_memory"

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
    target_task: str = Query(default="spread_error_regression"),
    source_name: str = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    source_path: str | None = Query(default=None),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            history = get_model_market_board_refresh_history_postgres(
                connection,
                target_task=target_task,
                source_name=source_name,
                recent_limit=recent_limit,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_market_board_refresh_repository(
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
        repository_mode = "in_memory"

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
            "recent_limit": recent_limit,
        },
        "market_board_refresh_history": history,
    }


@router.get("/models/market-board/source-runs")
def phase_three_model_market_board_source_runs(
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    source_path: str | None = Query(default=None),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    resolved_source_name = source_name or "demo_daily_lines_v1"
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            history = get_model_market_board_source_run_history_postgres(
                connection,
                target_task=target_task,
                source_name=source_name,
                season_label=season_label,
                recent_limit=recent_limit,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_market_board_refresh_repository(
            target_task=target_task,
            source_name=resolved_source_name,
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
        repository_mode = "in_memory"

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
            "recent_limit": recent_limit,
        },
        "market_board_source_run_history": history,
    }


@router.get("/models/market-board/refresh-queue")
def phase_three_model_market_board_refresh_queue(
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
    resolved_source_name = source_name or "demo_daily_lines_v1"
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            queue = get_model_market_board_refresh_queue_postgres(
                connection,
                target_task=target_task,
                season_label=season_label,
                source_name=source_name,
                freshness_status=freshness_status,
                pending_only=pending_only,
                recent_limit=recent_limit,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_market_board_refresh_repository(
            target_task=target_task,
            source_name=resolved_source_name,
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
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
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
    resolved_source_name = source_name or "demo_daily_lines_v1"
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            queue = get_model_market_board_scoring_queue_postgres(
                connection,
                target_task=target_task,
                season_label=season_label,
                source_name=source_name,
                freshness_status=freshness_status,
                pending_only=pending_only,
                recent_limit=recent_limit,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_market_board_refresh_repository(
            target_task=target_task,
            source_name=resolved_source_name,
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
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            result = orchestrate_model_market_board_refresh_postgres(
                connection,
                target_task=target_task,
                season_label=season_label,
                source_name=source_name,
                freshness_status=freshness_status,
                pending_only=pending_only,
                recent_limit=recent_limit,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_market_board_refresh_repository(
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
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
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
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_market_board_orchestration_repository(
            target_task=target_task,
            source_name=source_name,
            season_label=season_label,
            game_date=game_date,
            slate_label=slate_label,
            game_count=game_count,
            feature_key=feature_key,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
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
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
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
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_market_board_orchestration_repository(
            target_task=target_task,
            source_name=source_name,
            season_label=season_label,
            game_date=game_date,
            slate_label=slate_label,
            game_count=game_count,
            feature_key=feature_key,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
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
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            history = get_model_market_board_refresh_batch_history_postgres(
                connection,
                target_task=target_task,
                source_name=source_name,
                recent_limit=recent_limit,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_market_board_orchestration_repository(
            target_task=target_task,
            source_name=source_name,
            season_label=season_label,
            game_date=game_date,
            slate_label=slate_label,
            game_count=game_count,
            feature_key="baseline_team_features_v1",
            train_ratio=0.7,
            validation_ratio=0.15,
            refresh_freshness_status=freshness_status,
            refresh_pending_only=pending_only,
            recent_limit=recent_limit,
            run_refresh_orchestration=True,
        )
        history = get_model_market_board_refresh_batch_history_in_memory(
            repository,
            target_task=target_task,
            source_name=source_name,
            recent_limit=recent_limit,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            history = get_model_market_board_cadence_batch_history_postgres(
                connection,
                target_task=target_task,
                source_name=source_name,
                recent_limit=recent_limit,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_market_board_orchestration_repository(
            target_task=target_task,
            source_name=source_name,
            season_label=season_label,
            game_date=game_date,
            slate_label=slate_label,
            game_count=game_count,
            feature_key=feature_key,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            refresh_freshness_status=refresh_freshness_status,
            refresh_pending_only=refresh_pending_only,
            scoring_freshness_status=scoring_freshness_status,
            scoring_pending_only=scoring_pending_only,
            recent_limit=recent_limit,
            run_cadence_orchestration=True,
        )
        history = get_model_market_board_cadence_batch_history_in_memory(
            repository,
            target_task=target_task,
            source_name=source_name,
            recent_limit=recent_limit,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            history = get_model_market_board_scoring_batch_history_postgres(
                connection,
                target_task=target_task,
                source_name=source_name,
                recent_limit=recent_limit,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_market_board_orchestration_repository(
            target_task=target_task,
            source_name=source_name,
            season_label=season_label,
            game_date=game_date,
            slate_label=slate_label,
            game_count=game_count,
            feature_key=feature_key,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            scoring_freshness_status=freshness_status,
            scoring_pending_only=pending_only,
            recent_limit=recent_limit,
            run_scoring_orchestration=True,
        )
        history = get_model_market_board_scoring_batch_history_in_memory(
            repository,
            target_task=target_task,
            source_name=source_name,
            recent_limit=recent_limit,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            dashboard = get_model_market_board_cadence_dashboard_postgres(
                connection,
                target_task=target_task,
                season_label=season_label,
                source_name=source_name,
                recent_limit=recent_limit,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_market_board_orchestration_repository(
            target_task=target_task,
            source_name=source_name,
            season_label=season_label,
            game_date=game_date,
            slate_label=slate_label,
            game_count=game_count,
            feature_key=feature_key,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            scoring_freshness_status=freshness_status,
            scoring_pending_only=pending_only,
            recent_limit=recent_limit,
            run_scoring_orchestration=True,
        )
        dashboard = get_model_market_board_cadence_dashboard_in_memory(
            repository,
            target_task=target_task,
            season_label=season_label,
            source_name=source_name,
            recent_limit=recent_limit,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
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
    target_task: str = Query(default="spread_error_regression"),
) -> dict[str, object]:
    games = [game.model_dump() for game in request.games]
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            board_result = materialize_model_market_board_postgres(
                connection,
                target_task=target_task,
                games=games,
                slate_label=request.slate_label,
            )
        repository_mode = "postgres"
    else:
        repository = InMemoryIngestionRepository()
        board_result = materialize_model_market_board_in_memory(
            repository,
            target_task=target_task,
            games=games,
            slate_label=request.slate_label,
        )
        repository_mode = "in_memory"

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
    target_task: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    slate_label: str | None = Query(default="demo-market-board"),
    game_date: date = Query(default=date(2026, 4, 20)),
    home_team_code: str = Query(default="LAL"),
    away_team_code: str = Query(default="BOS"),
    home_spread_line: float | None = Query(default=None),
    total_line: float | None = Query(default=None),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            boards = list_model_market_boards_postgres(
                connection,
                target_task=target_task,
                season_label=season_label,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_market_board_materialized_repository(
            target_task=target_task,
            season_label=season_label,
            slate_label=slate_label,
            game_date=game_date,
            home_team_code=home_team_code,
            away_team_code=away_team_code,
            home_spread_line=home_spread_line,
            total_line=total_line,
        )
        boards = list_model_market_boards_in_memory(
            repository,
            target_task=target_task,
            season_label=season_label,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "target_task": target_task,
            "season_label": season_label,
        },
        "board_count": len(boards),
        "boards": boards,
    }


@router.get("/models/market-board/{board_id}")
def phase_three_model_market_board_detail(
    board_id: int,
    target_task: str = Query(default="spread_error_regression"),
    season_label: str = Query(default="2025-2026"),
    slate_label: str | None = Query(default="demo-market-board"),
    game_date: date = Query(default=date(2026, 4, 20)),
    home_team_code: str = Query(default="LAL"),
    away_team_code: str = Query(default="BOS"),
    home_spread_line: float | None = Query(default=None),
    total_line: float | None = Query(default=None),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            board = get_model_market_board_detail_postgres(connection, board_id=board_id)
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_market_board_materialized_repository(
            target_task=target_task,
            season_label=season_label,
            slate_label=slate_label,
            game_date=game_date,
            home_team_code=home_team_code,
            away_team_code=away_team_code,
            home_spread_line=home_spread_line,
            total_line=total_line,
        )
        board = get_model_market_board_detail_in_memory(repository, board_id=board_id)
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "target_task": target_task,
            "season_label": season_label,
        },
        "board": board,
    }


@router.get("/models/market-board/{board_id}/operations")
def phase_three_model_market_board_operations(
    board_id: int,
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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            operations = get_model_market_board_operations_postgres(
                connection,
                board_id=board_id,
                recent_limit=recent_limit,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_market_board_orchestration_repository(
            target_task=target_task,
            source_name=source_name,
            season_label=season_label,
            game_date=game_date,
            slate_label=slate_label,
            game_count=game_count,
            feature_key=feature_key,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            scoring_freshness_status=freshness_status,
            scoring_pending_only=pending_only,
            recent_limit=recent_limit,
            run_scoring_orchestration=True,
        )
        operations = get_model_market_board_operations_in_memory(
            repository,
            board_id=board_id,
            recent_limit=recent_limit,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
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
        repository_mode = "postgres"
    else:
        repository, _ = _prepare_in_memory_market_board_score_repository(
            board_id=board_id,
            feature_key=feature_key,
            target_task=target_task,
            season_label=season_label,
            slate_label=slate_label,
            game_date=game_date,
            home_team_code=home_team_code,
            away_team_code=away_team_code,
            home_spread_line=home_spread_line,
            total_line=total_line,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
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
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "board_id": board_id,
            "target_task": target_task,
            "season_label": season_label,
            "slate_label": slate_label,
            "feature_key": feature_key,
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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
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
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_future_slate_repository(
            feature_key=feature_key,
            target_task=target_task,
            games=games,
            slate_label=request.slate_label,
            include_evidence=include_evidence,
            dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
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
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "slate_label": request.slate_label,
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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
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
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_future_slate_repository(
            feature_key=feature_key,
            target_task=target_task,
            games=games,
            slate_label=request.slate_label,
            include_evidence=include_evidence,
            dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
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
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "slate_label": request.slate_label,
            "include_evidence": include_evidence,
            "dimensions": list(dimensions),
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
        },
        **materialized,
    }


@router.post("/models/future-game-preview/opportunities/materialize")
def phase_three_model_future_opportunity_materialize(
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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
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
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_phase_three_model_repository(
            feature_key=feature_key,
            target_task=target_task,
            team_code=None,
            season_label=None,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            promote_best=True,
        )
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
        repository_mode = "in_memory"

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
            "include_evidence": include_evidence,
            "dimensions": list(dimensions),
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
        },
        **materialized,
    }


@router.post("/models/opportunities/materialize")
def phase_three_model_opportunity_materialize(
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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
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
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_phase_three_model_repository(
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            promote_best=True,
        )
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
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "canonical_game_id": canonical_game_id,
            "limit": limit,
            "include_evidence": include_evidence,
            "dimensions": list(dimensions),
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
        },
        **materialized,
    }


@router.get("/models/opportunities/history")
def phase_three_model_opportunity_history(
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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            history = get_model_opportunity_history_postgres(
                connection,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                source_kind=source_kind,
                scenario_key=scenario_key,
                recent_limit=recent_limit,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_opportunity_history_repository(
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            canonical_game_id=canonical_game_id,
            source_kind=source_kind,
            game_date=game_date,
            home_team_code=home_team_code,
            away_team_code=away_team_code,
            home_spread_line=home_spread_line,
            total_line=total_line,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            limit=limit,
            include_evidence=include_evidence,
            dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
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
        repository_mode = "in_memory"

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
            "recent_limit": recent_limit,
        },
        "model_opportunity_history": history,
    }


@router.get("/features/snapshots")
def feature_snapshots(
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            snapshot_result = get_feature_snapshot_catalog_postgres(
                connection,
                feature_key=feature_key,
                team_code=team_code,
                season_label=season_label,
                limit=limit,
                offset=offset,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_feature_repository()
        snapshot_result = get_feature_snapshot_catalog_in_memory(
            repository,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
            limit=limit,
            offset=offset,
        )
        repository_mode = "in_memory"

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


@router.get("/features/dataset")
def feature_dataset(
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            dataset_result = get_feature_dataset_postgres(
                connection,
                feature_key=feature_key,
                team_code=team_code,
                season_label=season_label,
                limit=limit,
                offset=offset,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_feature_repository()
        dataset_result = get_feature_dataset_in_memory(
            repository,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
            limit=limit,
            offset=offset,
        )
        repository_mode = "in_memory"

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
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            profile_result = get_feature_dataset_profile_postgres(
                connection,
                feature_key=feature_key,
                team_code=team_code,
                season_label=season_label,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_feature_repository()
        profile_result = get_feature_dataset_profile_in_memory(
            repository,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "team_code": team_code,
            "season_label": season_label,
        },
        **profile_result,
    }


@router.post("/features/analysis/materialize")
def materialize_feature_analysis(
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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
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
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_feature_repository()
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
        repository_mode = "in_memory"

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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
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
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_feature_analysis_repository(
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
        repository_mode = "in_memory"

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
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            history_result = get_feature_analysis_artifact_history_postgres(
                connection,
                feature_key=feature_key,
                artifact_type=artifact_type,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                latest_limit=latest_limit,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_feature_analysis_repository(
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
        repository_mode = "in_memory"

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
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    preview_limit: int = Query(default=5, ge=1, le=20),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            split_result = get_feature_dataset_splits_postgres(
                connection,
                feature_key=feature_key,
                team_code=team_code,
                season_label=season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                preview_limit=preview_limit,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_feature_repository()
        split_result = get_feature_dataset_splits_in_memory(
            repository,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            preview_limit=preview_limit,
        )
        repository_mode = "in_memory"

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
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    drop_null_targets: bool = Query(default=True),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
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
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_feature_repository()
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
        repository_mode = "in_memory"

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
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    drop_null_targets: bool = Query(default=True),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            training_manifest = get_feature_training_manifest_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                drop_null_targets=drop_null_targets,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_feature_repository()
        training_manifest = get_feature_training_manifest_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            drop_null_targets=drop_null_targets,
        )
        repository_mode = "in_memory"

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
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    drop_null_targets: bool = Query(default=True),
    preview_limit: int = Query(default=5, ge=1, le=20),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
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
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_feature_repository()
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
            "drop_null_targets": drop_null_targets,
            "preview_limit": preview_limit,
        },
        **training_bundle,
    }


@router.get("/features/dataset/training-benchmark")
def feature_dataset_training_benchmark(
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    drop_null_targets: bool = Query(default=True),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
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
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_feature_repository()
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
            "drop_null_targets": drop_null_targets,
        },
        **training_benchmark,
    }


@router.get("/features/dataset/training-task-matrix")
def feature_dataset_training_task_matrix(
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    drop_null_targets: bool = Query(default=True),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            training_task_matrix = get_feature_training_task_matrix_postgres(
                connection,
                feature_key=feature_key,
                team_code=team_code,
                season_label=season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                drop_null_targets=drop_null_targets,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_feature_repository()
        training_task_matrix = get_feature_training_task_matrix_in_memory(
            repository,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
        )
        repository_mode = "in_memory"

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
    diagnostics = _run_admin_diagnostics_stable_read(
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
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    status: str = Query(default="FAILED"),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
) -> dict[str, object]:
    diagnostics = _run_admin_diagnostics_stable_read(
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
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    severity: str | None = Query(default=None),
    issue_type: str | None = Query(default=None),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
) -> dict[str, object]:
    diagnostics = _run_admin_diagnostics_stable_read(
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
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
) -> dict[str, object]:
    diagnostics = _run_admin_diagnostics_stable_read(
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
    diagnostics = _run_admin_diagnostics_stable_read(
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
    diagnostics = _run_admin_diagnostics_stable_read(
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
    diagnostics = _run_admin_diagnostics_stable_read(
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
    diagnostics = _run_admin_diagnostics_stable_read(
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
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    dry_run: bool = Query(default=True),
) -> dict[str, object]:
    repository_mode = "postgres" if _use_postgres_stable_read_mode() else "in_memory"
    return normalize_data_quality_taxonomy(
        repository_mode=repository_mode,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        dry_run=dry_run,
    )
