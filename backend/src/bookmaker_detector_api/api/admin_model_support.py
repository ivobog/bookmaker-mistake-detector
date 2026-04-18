from datetime import date

from pydantic import BaseModel, Field

from bookmaker_detector_api.config import settings
from bookmaker_detector_api.demo import seed_phase_two_feature_in_memory
from bookmaker_detector_api.repositories import PhaseThreeModelingStore
from bookmaker_detector_api.services.models import (
    materialize_model_future_game_preview_in_memory,
    materialize_model_future_slate_in_memory,
    promote_best_model_in_memory,
    train_phase_three_models_in_memory,
)


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


def _prepare_in_memory_phase_three_model_repository(
    *,
    feature_key: str,
    target_task: str | None,
    team_code: str | None,
    season_label: str | None,
    train_ratio: float,
    validation_ratio: float,
    promote_best: bool = False,
) -> PhaseThreeModelingStore:
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
) -> PhaseThreeModelingStore:
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
) -> PhaseThreeModelingStore:
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

