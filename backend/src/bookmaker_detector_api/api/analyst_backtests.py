from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from bookmaker_detector_api.api.schemas import (
    AnalystBacktestDetailResponse,
    AnalystBacktestListFilters,
    AnalystBacktestListResponse,
    AnalystBacktestRun,
)
from bookmaker_detector_api.config import settings
from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.repositories import InMemoryIngestionRepository
from bookmaker_detector_api.services.model_records import ModelBacktestRunRecord
from bookmaker_detector_api.services.models import (
    get_model_backtest_detail_in_memory,
    get_model_backtest_detail_postgres,
    list_model_backtest_runs_in_memory,
    list_model_backtest_runs_postgres,
)

router = APIRouter(prefix="/analyst", tags=["analyst"])


def _use_postgres_analyst_mode() -> bool:
    return settings.api_env.lower() == "production"


def _serialize_backtest_run(run: ModelBacktestRunRecord) -> AnalystBacktestRun:
    return AnalystBacktestRun(
        id=run.id,
        feature_version_id=run.feature_version_id,
        target_task=run.target_task,
        team_code=run.team_code,
        season_label=run.season_label,
        status=run.status,
        selection_policy_name=run.selection_policy_name,
        strategy_name=run.strategy_name,
        minimum_train_games=run.minimum_train_games,
        test_window_games=run.test_window_games,
        train_ratio=run.train_ratio,
        validation_ratio=run.validation_ratio,
        fold_count=run.fold_count,
        payload=run.payload,
        created_at=run.created_at.isoformat() if run.created_at else None,
        completed_at=run.completed_at.isoformat() if run.completed_at else None,
    )


def _load_backtest_runs(filters: AnalystBacktestListFilters) -> tuple[str, list[ModelBacktestRunRecord]]:
    if _use_postgres_analyst_mode():
        with postgres_connection() as connection:
            runs = list_model_backtest_runs_postgres(
                connection,
                target_task=filters.target_task,
                team_code=filters.team_code,
                season_label=filters.season_label,
            )
        return "postgres", runs

    repository = InMemoryIngestionRepository()
    runs = list_model_backtest_runs_in_memory(
        repository,
        target_task=filters.target_task,
        team_code=filters.team_code,
        season_label=filters.season_label,
    )
    return "in_memory", runs


def _load_backtest_detail(backtest_run_id: int) -> tuple[str, dict[str, object] | None]:
    if _use_postgres_analyst_mode():
        with postgres_connection() as connection:
            backtest_run = get_model_backtest_detail_postgres(
                connection,
                backtest_run_id=backtest_run_id,
            )
        return "postgres", backtest_run

    repository = InMemoryIngestionRepository()
    backtest_run = get_model_backtest_detail_in_memory(
        repository,
        backtest_run_id=backtest_run_id,
    )
    return "in_memory", backtest_run


@router.get("/backtests", response_model=AnalystBacktestListResponse)
def phase_four_model_backtests(
    filters: Annotated[AnalystBacktestListFilters, Depends()],
) -> AnalystBacktestListResponse:
    repository_mode, runs = _load_backtest_runs(filters)
    return AnalystBacktestListResponse(
        repository_mode=repository_mode,
        backtest_run_count=len(runs),
        backtest_runs=[_serialize_backtest_run(run) for run in runs],
    )


@router.get("/backtests/{backtest_run_id}", response_model=AnalystBacktestDetailResponse)
def phase_four_model_backtest_detail(
    backtest_run_id: int,
) -> AnalystBacktestDetailResponse:
    repository_mode, backtest_run = _load_backtest_detail(backtest_run_id)
    return AnalystBacktestDetailResponse(
        repository_mode=repository_mode,
        backtest_run=(
            AnalystBacktestRun.model_validate(backtest_run) if backtest_run is not None else None
        ),
    )
