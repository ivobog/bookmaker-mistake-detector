from fastapi import APIRouter, Query

from bookmaker_detector_api.config import settings
from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.demo import (
    seed_phase_two_feature_in_memory,
)
from bookmaker_detector_api.repositories import InMemoryIngestionRepository
from bookmaker_detector_api.services.features import (
    get_feature_snapshot_summary_in_memory,
    get_feature_snapshot_summary_postgres,
)

router = APIRouter(prefix="/analyst", tags=["analyst"])


def _use_postgres_analyst_mode() -> bool:
    return settings.api_env.lower() == "production"


@router.get("/trends/summary")
def feature_summary(
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
) -> dict[str, object]:
    if _use_postgres_analyst_mode():
        with postgres_connection() as connection:
            summary_result = get_feature_snapshot_summary_postgres(
                connection,
                feature_key=feature_key,
                team_code=team_code,
                season_label=season_label,
            )
        repository_mode = "postgres"
    else:
        repository = InMemoryIngestionRepository()
        repository, _, _ = seed_phase_two_feature_in_memory()
        summary_result = get_feature_snapshot_summary_in_memory(
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
        **summary_result,
    }
