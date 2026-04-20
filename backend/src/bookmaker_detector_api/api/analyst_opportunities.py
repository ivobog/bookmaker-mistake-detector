from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from bookmaker_detector_api.api.schemas import (
    AnalystOpportunity,
    AnalystOpportunityDetailResponse,
    AnalystOpportunityListFilters,
    AnalystOpportunityListResponse,
)
from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.services.model_market_board_views import (
    _serialize_model_opportunity,
)
from bookmaker_detector_api.services.model_records import ModelOpportunityRecord
from bookmaker_detector_api.services.models import (
    get_model_opportunity_detail_postgres,
    get_model_opportunity_queue_postgres,
)

router = APIRouter(prefix="/analyst", tags=["analyst"])


def _serialize_opportunity(opportunity: ModelOpportunityRecord) -> AnalystOpportunity:
    return AnalystOpportunity.model_validate(_serialize_model_opportunity(opportunity))


def _load_opportunities(
    filters: AnalystOpportunityListFilters,
) -> tuple[str, dict[str, object]]:
    with postgres_connection() as connection:
        queue = get_model_opportunity_queue_postgres(
            connection,
            target_task=filters.target_task,
            team_code=filters.team_code,
            status=filters.status,
            season_label=filters.season_label,
            source_kind=filters.source_kind,
            scenario_key=filters.scenario_key,
        )
    return "postgres", queue


def _load_opportunity_detail(opportunity_id: int) -> tuple[str, dict[str, object] | None]:
    with postgres_connection() as connection:
        opportunity = get_model_opportunity_detail_postgres(
            connection,
            opportunity_id=opportunity_id,
        )
    return "postgres", opportunity


@router.get("/opportunities", response_model=AnalystOpportunityListResponse)
def phase_three_model_opportunities(
    filters: Annotated[AnalystOpportunityListFilters, Depends()],
) -> AnalystOpportunityListResponse:
    repository_mode, queue = _load_opportunities(filters)
    opportunities = list(queue.get("opportunities", []))
    selected = opportunities[: filters.limit]
    return AnalystOpportunityListResponse(
        repository_mode=repository_mode,
        queue_batch_id=queue.get("queue_batch_id"),
        queue_materialized_at=queue.get("queue_materialized_at"),
        queue_scope=dict(queue.get("queue_scope", {})),
        queue_scope_label=queue.get("queue_scope_label"),
        queue_scope_is_scoped=bool(queue.get("queue_scope_is_scoped", False)),
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
