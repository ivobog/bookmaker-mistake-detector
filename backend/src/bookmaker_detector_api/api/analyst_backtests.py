from fastapi import APIRouter, Query

from bookmaker_detector_api.config import settings
from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.demo import (
    seed_phase_two_feature_in_memory,
)
from bookmaker_detector_api.repositories import InMemoryIngestionRepository
from bookmaker_detector_api.services.models import (
    get_model_backtest_detail_in_memory,
    get_model_backtest_detail_postgres,
    list_model_backtest_runs_in_memory,
    list_model_backtest_runs_postgres,
    run_model_backtest_in_memory,
)

router = APIRouter(prefix="/analyst", tags=["analyst"])


def _use_postgres_analyst_mode() -> bool:
    return settings.api_env.lower() == "production"


@router.get("/backtests")
def phase_four_model_backtests(
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
    if _use_postgres_analyst_mode():
        with postgres_connection() as connection:
            runs = list_model_backtest_runs_postgres(
                connection,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
            )
        repository_mode = "postgres"
    else:
        repository = InMemoryIngestionRepository()
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
        runs = list_model_backtest_runs_in_memory(
            repository,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "backtest_run_count": len(runs),
        "backtest_runs": [run.payload | {"id": run.id} for run in runs],
    }


@router.get("/backtests/{backtest_run_id}")
def phase_four_model_backtest_detail(
    backtest_run_id: int,
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
    if _use_postgres_analyst_mode():
        with postgres_connection() as connection:
            backtest_run = get_model_backtest_detail_postgres(
                connection,
                backtest_run_id=backtest_run_id,
            )
        repository_mode = "postgres"
    else:
        repository = InMemoryIngestionRepository()
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
        backtest_run = get_model_backtest_detail_in_memory(
            repository,
            backtest_run_id=backtest_run_id,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "backtest_run": backtest_run,
    }
