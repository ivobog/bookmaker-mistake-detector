from fastapi import APIRouter, Query

from bookmaker_detector_api.config import settings
from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.demo import (
    seed_phase_two_feature_in_memory,
)
from bookmaker_detector_api.repositories import InMemoryIngestionRepository
from bookmaker_detector_api.services.features import (
    get_feature_comparable_cases_in_memory,
    get_feature_comparable_cases_postgres,
    get_feature_evidence_bundle_in_memory,
    get_feature_evidence_bundle_postgres,
    get_feature_pattern_catalog_in_memory,
    get_feature_pattern_catalog_postgres,
)

router = APIRouter(prefix="/analyst", tags=["analyst"])


def _use_postgres_analyst_mode() -> bool:
    return settings.api_env.lower() == "production"


@router.get("/patterns")
def feature_patterns(
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    dimensions: str = Query(default="venue,days_rest_bucket"),
    min_sample_size: int = Query(default=2, ge=1, le=100),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, object]:
    parsed_dimensions = tuple(
        dimension.strip() for dimension in dimensions.split(",") if dimension.strip()
    )
    if _use_postgres_analyst_mode():
        with postgres_connection() as connection:
            pattern_result = get_feature_pattern_catalog_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                dimensions=parsed_dimensions,
                min_sample_size=min_sample_size,
                limit=limit,
            )
        repository_mode = "postgres"
    else:
        repository = InMemoryIngestionRepository()
        repository, _, _ = seed_phase_two_feature_in_memory()
        pattern_result = get_feature_pattern_catalog_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            dimensions=parsed_dimensions,
            min_sample_size=min_sample_size,
            limit=limit,
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
            "limit": limit,
        },
        **pattern_result,
    }


@router.get("/comparables")
def feature_comparables(
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    dimensions: str = Query(default="venue,days_rest_bucket"),
    canonical_game_id: int | None = Query(default=None, ge=1),
    condition_values: str | None = Query(default=None),
    pattern_key: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, object]:
    parsed_dimensions = tuple(
        dimension.strip() for dimension in dimensions.split(",") if dimension.strip()
    )
    parsed_condition_values = (
        tuple(value.strip() for value in condition_values.split(","))
        if condition_values is not None
        else None
    )
    if _use_postgres_analyst_mode():
        with postgres_connection() as connection:
            comparable_result = get_feature_comparable_cases_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                dimensions=parsed_dimensions,
                canonical_game_id=canonical_game_id,
                condition_values=parsed_condition_values,
                pattern_key=pattern_key,
                limit=limit,
            )
        repository_mode = "postgres"
    else:
        repository = InMemoryIngestionRepository()
        repository, _, _ = seed_phase_two_feature_in_memory()
        comparable_result = get_feature_comparable_cases_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            dimensions=parsed_dimensions,
            canonical_game_id=canonical_game_id,
            condition_values=parsed_condition_values,
            pattern_key=pattern_key,
            limit=limit,
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
            "canonical_game_id": canonical_game_id,
            "condition_values": list(parsed_condition_values)
            if parsed_condition_values is not None
            else None,
            "pattern_key": pattern_key,
            "limit": limit,
        },
        **comparable_result,
    }


@router.get("/evidence")
def feature_evidence(
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    dimensions: str = Query(default="venue,days_rest_bucket"),
    canonical_game_id: int | None = Query(default=None, ge=1),
    condition_values: str | None = Query(default=None),
    pattern_key: str | None = Query(default=None),
    comparable_limit: int = Query(default=10, ge=1, le=100),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
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
    if _use_postgres_analyst_mode():
        with postgres_connection() as connection:
            evidence_result = get_feature_evidence_bundle_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                dimensions=parsed_dimensions,
                canonical_game_id=canonical_game_id,
                condition_values=parsed_condition_values,
                pattern_key=pattern_key,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                drop_null_targets=drop_null_targets,
            )
        repository_mode = "postgres"
    else:
        repository = InMemoryIngestionRepository()
        repository, _, _ = seed_phase_two_feature_in_memory()
        evidence_result = get_feature_evidence_bundle_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            dimensions=parsed_dimensions,
            canonical_game_id=canonical_game_id,
            condition_values=parsed_condition_values,
            pattern_key=pattern_key,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
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
            "canonical_game_id": canonical_game_id,
            "condition_values": list(parsed_condition_values)
            if parsed_condition_values is not None
            else None,
            "pattern_key": pattern_key,
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "drop_null_targets": drop_null_targets,
        },
        **evidence_result,
    }
