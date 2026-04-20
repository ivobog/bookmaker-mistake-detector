from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from bookmaker_detector_api.api.schemas import (
    AnalystComparableFilters,
    AnalystComparableResponse,
    AnalystEvidenceFilters,
    AnalystEvidenceResponse,
    AnalystPatternFilters,
    AnalystPatternResponse,
)
from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.services.features import (
    get_feature_comparable_cases_postgres,
    get_feature_evidence_bundle_postgres,
    get_feature_pattern_catalog_postgres,
)

router = APIRouter(prefix="/analyst", tags=["analyst"])


def _parse_csv_values(value: str | None) -> list[str] | None:
    if value is None:
        return None
    parsed = [item.strip() for item in value.split(",") if item.strip()]
    return parsed or None


@router.get("/patterns", response_model=AnalystPatternResponse)
def feature_patterns(
    filters: Annotated[AnalystPatternFilters, Depends()],
    dimensions: str = Query(default="venue,days_rest_bucket"),
) -> AnalystPatternResponse:
    parsed_dimensions = _parse_csv_values(dimensions) or []
    with postgres_connection() as connection:
        pattern_result = get_feature_pattern_catalog_postgres(
            connection,
            feature_key=filters.feature_key,
            target_task=filters.target_task,
            team_code=filters.team_code,
            season_label=filters.season_label,
            dimensions=tuple(parsed_dimensions),
            min_sample_size=filters.min_sample_size,
            limit=filters.limit,
        )

    response_filters = filters.model_copy(update={"dimensions": parsed_dimensions})
    return AnalystPatternResponse(
        filters=response_filters,
        feature_version=pattern_result.get("feature_version"),
        row_count=int(pattern_result.get("row_count", 0)),
        task=pattern_result.get("task"),
        pattern_count=int(pattern_result.get("pattern_count", 0)),
        patterns=pattern_result.get("patterns", []),
    )


@router.get("/comparables", response_model=AnalystComparableResponse)
def feature_comparables(
    filters: Annotated[AnalystComparableFilters, Depends()],
    dimensions: str = Query(default="venue,days_rest_bucket"),
    condition_values: str | None = Query(default=None),
) -> AnalystComparableResponse:
    parsed_dimensions = _parse_csv_values(dimensions) or []
    parsed_condition_values = _parse_csv_values(condition_values)
    with postgres_connection() as connection:
        comparable_result = get_feature_comparable_cases_postgres(
            connection,
            feature_key=filters.feature_key,
            target_task=filters.target_task,
            team_code=filters.team_code,
            season_label=filters.season_label,
            dimensions=tuple(parsed_dimensions),
            canonical_game_id=filters.canonical_game_id,
            condition_values=tuple(parsed_condition_values) if parsed_condition_values else None,
            pattern_key=filters.pattern_key,
            limit=filters.limit,
        )

    response_filters = filters.model_copy(
        update={
            "dimensions": parsed_dimensions,
            "condition_values": parsed_condition_values,
        }
    )
    return AnalystComparableResponse(
        filters=response_filters,
        feature_version=comparable_result.get("feature_version"),
        row_count=int(comparable_result.get("row_count", 0)),
        task=comparable_result.get("task"),
        anchor_case=comparable_result.get("anchor_case"),
        comparable_count=int(comparable_result.get("comparable_count", 0)),
        comparables=comparable_result.get("comparables", []),
        pattern_key=comparable_result.get("pattern_key"),
    )


@router.get("/evidence", response_model=AnalystEvidenceResponse)
def feature_evidence(
    filters: Annotated[AnalystEvidenceFilters, Depends()],
    dimensions: str = Query(default="venue,days_rest_bucket"),
    condition_values: str | None = Query(default=None),
) -> AnalystEvidenceResponse:
    parsed_dimensions = _parse_csv_values(dimensions) or []
    parsed_condition_values = _parse_csv_values(condition_values)
    with postgres_connection() as connection:
        evidence_result = get_feature_evidence_bundle_postgres(
            connection,
            feature_key=filters.feature_key,
            target_task=filters.target_task,
            team_code=filters.team_code,
            season_label=filters.season_label,
            dimensions=tuple(parsed_dimensions),
            canonical_game_id=filters.canonical_game_id,
            condition_values=tuple(parsed_condition_values) if parsed_condition_values else None,
            pattern_key=filters.pattern_key,
            comparable_limit=filters.comparable_limit,
            min_pattern_sample_size=filters.min_pattern_sample_size,
            train_ratio=filters.train_ratio,
            validation_ratio=filters.validation_ratio,
            drop_null_targets=filters.drop_null_targets,
        )

    response_filters = filters.model_copy(
        update={
            "dimensions": parsed_dimensions,
            "condition_values": parsed_condition_values,
        }
    )
    return AnalystEvidenceResponse(
        filters=response_filters,
        feature_version=evidence_result.get("feature_version"),
        row_count=int(evidence_result.get("row_count", 0)),
        task=evidence_result.get("task"),
        evidence=evidence_result.get("evidence", {}),
    )
