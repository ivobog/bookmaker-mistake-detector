from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from bookmaker_detector_api.api.schemas import (
    AnalystOpportunity,
    AnalystOpportunityDetailResponse,
    AnalystOpportunityListFilters,
    AnalystOpportunityListResponse,
)
from bookmaker_detector_api.config import settings
from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.repositories import InMemoryIngestionRepository
from bookmaker_detector_api.services.model_records import ModelOpportunityRecord
from bookmaker_detector_api.services.models import (
    get_model_opportunity_detail_in_memory,
    get_model_opportunity_detail_postgres,
    list_model_opportunities_in_memory,
    list_model_opportunities_postgres,
)

router = APIRouter(prefix="/analyst", tags=["analyst"])


def _use_postgres_analyst_mode() -> bool:
    return settings.api_env.lower() == "production"


def _serialize_opportunity(opportunity: ModelOpportunityRecord) -> AnalystOpportunity:
    return AnalystOpportunity(
        id=opportunity.id,
        model_scoring_run_id=opportunity.model_scoring_run_id,
        model_selection_snapshot_id=opportunity.model_selection_snapshot_id,
        model_evaluation_snapshot_id=opportunity.model_evaluation_snapshot_id,
        feature_version_id=opportunity.feature_version_id,
        target_task=opportunity.target_task,
        source_kind=opportunity.source_kind,
        scenario_key=opportunity.scenario_key,
        opportunity_key=opportunity.opportunity_key,
        team_code=opportunity.team_code,
        opponent_code=opportunity.opponent_code,
        season_label=opportunity.season_label,
        canonical_game_id=opportunity.canonical_game_id,
        game_date=opportunity.game_date.isoformat() if opportunity.game_date else None,
        policy_name=opportunity.policy_name,
        status=opportunity.status,
        prediction_value=opportunity.prediction_value,
        signal_strength=opportunity.signal_strength,
        evidence_rating=opportunity.evidence_rating,
        recommendation_status=opportunity.recommendation_status,
        payload=opportunity.payload,
        created_at=opportunity.created_at.isoformat() if opportunity.created_at else None,
        updated_at=opportunity.updated_at.isoformat() if opportunity.updated_at else None,
    )


def _load_opportunities(
    filters: AnalystOpportunityListFilters,
) -> tuple[str, list[ModelOpportunityRecord]]:
    if _use_postgres_analyst_mode():
        with postgres_connection() as connection:
            opportunities = list_model_opportunities_postgres(
                connection,
                target_task=filters.target_task,
                team_code=filters.team_code,
                status=filters.status,
                season_label=filters.season_label,
                source_kind=filters.source_kind,
                scenario_key=filters.scenario_key,
            )
        return "postgres", opportunities

    repository = InMemoryIngestionRepository()
    opportunities = list_model_opportunities_in_memory(
        repository,
        target_task=filters.target_task,
        team_code=filters.team_code,
        status=filters.status,
        season_label=filters.season_label,
        source_kind=filters.source_kind,
        scenario_key=filters.scenario_key,
    )
    return "in_memory", opportunities


def _load_opportunity_detail(opportunity_id: int) -> tuple[str, dict[str, object] | None]:
    if _use_postgres_analyst_mode():
        with postgres_connection() as connection:
            opportunity = get_model_opportunity_detail_postgres(
                connection,
                opportunity_id=opportunity_id,
            )
        return "postgres", opportunity

    repository = InMemoryIngestionRepository()
    opportunity = get_model_opportunity_detail_in_memory(
        repository,
        opportunity_id=opportunity_id,
    )
    return "in_memory", opportunity


@router.get("/opportunities", response_model=AnalystOpportunityListResponse)
def phase_three_model_opportunities(
    filters: Annotated[AnalystOpportunityListFilters, Depends()],
) -> AnalystOpportunityListResponse:
    repository_mode, opportunities = _load_opportunities(filters)
    selected = opportunities[: filters.limit]
    return AnalystOpportunityListResponse(
        repository_mode=repository_mode,
        opportunity_count=len(opportunities),
        opportunities=[_serialize_opportunity(entry) for entry in selected],
    )


@router.get("/opportunities/{opportunity_id}", response_model=AnalystOpportunityDetailResponse)
def phase_three_model_opportunity_detail(
    opportunity_id: int,
) -> AnalystOpportunityDetailResponse:
    repository_mode, opportunity = _load_opportunity_detail(opportunity_id)
    return AnalystOpportunityDetailResponse(
        repository_mode=repository_mode,
        opportunity=(
            AnalystOpportunity.model_validate(opportunity) if opportunity is not None else None
        ),
    )
