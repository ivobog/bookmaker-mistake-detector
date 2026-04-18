from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from bookmaker_detector_api.api.schemas import AnalystTrendFilters, AnalystTrendResponse
from bookmaker_detector_api.config import settings
from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.repositories import InMemoryIngestionRepository
from bookmaker_detector_api.services.features import (
    get_feature_snapshot_summary_in_memory,
    get_feature_snapshot_summary_postgres,
)

router = APIRouter(prefix="/analyst", tags=["analyst"])


def _use_postgres_analyst_mode() -> bool:
    return settings.api_env.lower() == "production"


@router.get("/trends/summary", response_model=AnalystTrendResponse)
def feature_summary(
    filters: Annotated[AnalystTrendFilters, Depends()],
) -> AnalystTrendResponse:
    if _use_postgres_analyst_mode():
        with postgres_connection() as connection:
            summary_result = get_feature_snapshot_summary_postgres(
                connection,
                feature_key=filters.feature_key,
                team_code=filters.team_code,
                season_label=filters.season_label,
            )
        repository_mode = "postgres"
    else:
        repository = InMemoryIngestionRepository()
        summary_result = get_feature_snapshot_summary_in_memory(
            repository,
            feature_key=filters.feature_key,
            team_code=filters.team_code,
            season_label=filters.season_label,
        )
        repository_mode = "in_memory"

    return AnalystTrendResponse(
        repository_mode=repository_mode,
        filters=filters,
        feature_version=summary_result.get("feature_version"),
        snapshot_count=int(summary_result.get("snapshot_count", 0)),
        perspective_count=int(summary_result.get("perspective_count", 0)),
        summary=summary_result.get("summary", {}),
        latest_perspective=summary_result.get("latest_perspective"),
    )
