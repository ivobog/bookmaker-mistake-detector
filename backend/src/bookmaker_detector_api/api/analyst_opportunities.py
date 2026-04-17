from datetime import date

from fastapi import APIRouter, Query

from bookmaker_detector_api.config import settings
from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.demo import (
    seed_phase_two_feature_in_memory,
)
from bookmaker_detector_api.repositories import InMemoryIngestionRepository
from bookmaker_detector_api.services.models import (
    get_model_opportunity_detail_in_memory,
    get_model_opportunity_detail_postgres,
    list_model_opportunities_in_memory,
    list_model_opportunities_postgres,
    materialize_model_future_opportunities_in_memory,
    materialize_model_opportunities_in_memory,
    promote_best_model_in_memory,
    train_phase_three_models_in_memory,
)

router = APIRouter(prefix="/analyst", tags=["analyst"])


def _use_postgres_analyst_mode() -> bool:
    return settings.api_env.lower() == "production"


def _prepare_in_memory_opportunity_repository(
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


@router.get("/opportunities")
def phase_three_model_opportunities(
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
    if _use_postgres_analyst_mode():
        with postgres_connection() as connection:
            opportunities = list_model_opportunities_postgres(
                connection,
                target_task=target_task,
                team_code=team_code,
                status=status,
                season_label=season_label,
                source_kind=source_kind,
                scenario_key=scenario_key,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_opportunity_repository(
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
        opportunities = list_model_opportunities_in_memory(
            repository,
            target_task=target_task,
            team_code=team_code,
            status=status,
            season_label=season_label,
            source_kind=source_kind,
            scenario_key=scenario_key,
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


@router.get("/opportunities/{opportunity_id}")
def phase_three_model_opportunity_detail(
    opportunity_id: int,
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
    if _use_postgres_analyst_mode():
        with postgres_connection() as connection:
            opportunity = get_model_opportunity_detail_postgres(
                connection,
                opportunity_id=opportunity_id,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_opportunity_repository(
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
        opportunity = get_model_opportunity_detail_in_memory(
            repository,
            opportunity_id=opportunity_id,
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
        },
        "opportunity": opportunity,
    }
