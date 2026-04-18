from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from bookmaker_detector_api.api.schemas import (
    AdminOpportunityHistoryFilters,
    AdminOpportunityHistoryResponse,
)
from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.services.models import (
    get_model_opportunity_history_in_memory,
    get_model_opportunity_history_postgres,
    materialize_model_future_opportunities_in_memory,
    materialize_model_future_opportunities_postgres,
    materialize_model_opportunities_in_memory,
    materialize_model_opportunities_postgres,
)
from bookmaker_detector_api.services.repository_factory import (
    build_in_memory_phase_three_modeling_store,
)

from .admin_model_support import (
    _prepare_in_memory_phase_three_model_repository,
    _use_postgres_stable_read_mode,
)

router = APIRouter(prefix="/admin", tags=["admin"])


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
    filters: Annotated[AdminOpportunityHistoryFilters, Depends()],
) -> AdminOpportunityHistoryResponse:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            history = get_model_opportunity_history_postgres(
                connection,
                target_task=filters.target_task,
                team_code=filters.team_code,
                season_label=filters.season_label,
                source_kind=filters.source_kind,
                scenario_key=filters.scenario_key,
                recent_limit=filters.recent_limit,
            )
        repository_mode = "postgres"
    else:
        repository = build_in_memory_phase_three_modeling_store()
        history = get_model_opportunity_history_in_memory(
            repository,
            target_task=filters.target_task,
            team_code=filters.team_code,
            season_label=filters.season_label,
            source_kind=filters.source_kind,
            scenario_key=filters.scenario_key,
            recent_limit=filters.recent_limit,
        )
        repository_mode = "in_memory"

    return AdminOpportunityHistoryResponse(
        repository_mode=repository_mode,
        filters=filters,
        model_opportunity_history=history,
    )
