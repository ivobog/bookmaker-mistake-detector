from fastapi import APIRouter, Query

from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.services.features import (
    get_feature_analysis_artifact_catalog_postgres,
    get_feature_analysis_artifact_history_postgres,
    get_feature_dataset_postgres,
    get_feature_dataset_profile_postgres,
    get_feature_dataset_splits_postgres,
    get_feature_snapshot_catalog_postgres,
    get_feature_training_benchmark_postgres,
    get_feature_training_bundle_postgres,
    get_feature_training_manifest_postgres,
    get_feature_training_task_matrix_postgres,
    get_feature_training_view_postgres,
    materialize_feature_analysis_artifacts_postgres,
)

from .admin_model_support import _resolve_target_task, _validate_model_admin_inputs

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/features/snapshots")
def feature_snapshots(
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> dict[str, object]:
    with postgres_connection() as connection:
        snapshot_result = get_feature_snapshot_catalog_postgres(
            connection,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
            limit=limit,
            offset=offset,
        )

    return {
        "filters": {
            "feature_key": feature_key,
            "team_code": team_code,
            "season_label": season_label,
            "limit": limit,
            "offset": offset,
        },
        **snapshot_result,
    }


@router.get("/features/dataset")
def feature_dataset(
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, object]:
    with postgres_connection() as connection:
        dataset_result = get_feature_dataset_postgres(
            connection,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
            limit=limit,
            offset=offset,
        )

    return {
        "filters": {
            "feature_key": feature_key,
            "team_code": team_code,
            "season_label": season_label,
            "limit": limit,
            "offset": offset,
        },
        **dataset_result,
    }


@router.get("/features/dataset/profile")
def feature_dataset_profile(
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
) -> dict[str, object]:
    with postgres_connection() as connection:
        profile_result = get_feature_dataset_profile_postgres(
            connection,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
        )

    return {
        "filters": {
            "feature_key": feature_key,
            "team_code": team_code,
            "season_label": season_label,
        },
        **profile_result,
    }


@router.post("/features/analysis/materialize")
def materialize_feature_analysis(
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    dimensions: str = Query(default="venue,days_rest_bucket"),
    min_sample_size: int = Query(default=2, ge=1, le=100),
    canonical_game_id: int | None = Query(default=None, ge=1),
    condition_values: str | None = Query(default=None),
    pattern_key: str | None = Query(default=None),
    comparable_limit: int = Query(default=10, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    drop_null_targets: bool = Query(default=True),
) -> dict[str, object]:
    resolved_target_task, capabilities_payload = _resolve_target_task(target_task)
    _validate_model_admin_inputs(
        capabilities_payload=capabilities_payload,
        target_task=resolved_target_task,
    )
    parsed_dimensions = tuple(
        dimension.strip() for dimension in dimensions.split(",") if dimension.strip()
    )
    parsed_condition_values = (
        tuple(value.strip() for value in condition_values.split(","))
        if condition_values is not None
        else None
    )
    with postgres_connection() as connection:
        materialize_result = materialize_feature_analysis_artifacts_postgres(
            connection,
            feature_key=feature_key,
            target_task=resolved_target_task,
            team_code=team_code,
            season_label=season_label,
            dimensions=parsed_dimensions,
            min_sample_size=min_sample_size,
            canonical_game_id=canonical_game_id,
            condition_values=parsed_condition_values,
            pattern_key=pattern_key,
            comparable_limit=comparable_limit,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
        )

    return {
        "filters": {
            "feature_key": feature_key,
            "target_task": resolved_target_task,
            "team_code": team_code,
            "season_label": season_label,
            "dimensions": list(parsed_dimensions),
            "min_sample_size": min_sample_size,
            "canonical_game_id": canonical_game_id,
            "condition_values": list(parsed_condition_values)
            if parsed_condition_values is not None
            else None,
            "pattern_key": pattern_key,
            "comparable_limit": comparable_limit,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "drop_null_targets": drop_null_targets,
        },
        **materialize_result,
    }


@router.get("/features/analysis/artifacts")
def feature_analysis_artifacts(
    feature_key: str = Query(default="baseline_team_features_v1"),
    artifact_type: str | None = Query(default=None),
    target_task: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    dimensions: str = Query(default="venue,days_rest_bucket"),
    min_sample_size: int = Query(default=2, ge=1, le=100),
    canonical_game_id: int | None = Query(default=None, ge=1),
    condition_values: str | None = Query(default=None),
    pattern_key: str | None = Query(default=None),
    comparable_limit: int = Query(default=10, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    drop_null_targets: bool = Query(default=True),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, object]:
    resolved_target_task, capabilities_payload = _resolve_target_task(target_task)
    _validate_model_admin_inputs(
        capabilities_payload=capabilities_payload,
        target_task=resolved_target_task,
    )
    parsed_dimensions = tuple(
        dimension.strip() for dimension in dimensions.split(",") if dimension.strip()
    )
    parsed_condition_values = (
        tuple(value.strip() for value in condition_values.split(","))
        if condition_values is not None
        else None
    )
    with postgres_connection() as connection:
        artifact_result = get_feature_analysis_artifact_catalog_postgres(
            connection,
            feature_key=feature_key,
            artifact_type=artifact_type,
            target_task=resolved_target_task,
            team_code=team_code,
            season_label=season_label,
            limit=limit,
            offset=offset,
        )

    return {
        "filters": {
            "feature_key": feature_key,
            "artifact_type": artifact_type,
            "target_task": resolved_target_task,
            "team_code": team_code,
            "season_label": season_label,
            "dimensions": list(parsed_dimensions),
            "min_sample_size": min_sample_size,
            "canonical_game_id": canonical_game_id,
            "condition_values": list(parsed_condition_values)
            if parsed_condition_values is not None
            else None,
            "pattern_key": pattern_key,
            "comparable_limit": comparable_limit,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "drop_null_targets": drop_null_targets,
            "limit": limit,
            "offset": offset,
        },
        **artifact_result,
    }


@router.get("/features/analysis/history")
def feature_analysis_history(
    feature_key: str = Query(default="baseline_team_features_v1"),
    artifact_type: str | None = Query(default=None),
    target_task: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    dimensions: str = Query(default="venue,days_rest_bucket"),
    min_sample_size: int = Query(default=2, ge=1, le=100),
    canonical_game_id: int | None = Query(default=None, ge=1),
    condition_values: str | None = Query(default=None),
    pattern_key: str | None = Query(default=None),
    comparable_limit: int = Query(default=10, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    drop_null_targets: bool = Query(default=True),
    latest_limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, object]:
    resolved_target_task, capabilities_payload = _resolve_target_task(target_task)
    _validate_model_admin_inputs(
        capabilities_payload=capabilities_payload,
        target_task=resolved_target_task,
    )
    parsed_dimensions = tuple(
        dimension.strip() for dimension in dimensions.split(",") if dimension.strip()
    )
    parsed_condition_values = (
        tuple(value.strip() for value in condition_values.split(","))
        if condition_values is not None
        else None
    )
    with postgres_connection() as connection:
        history_result = get_feature_analysis_artifact_history_postgres(
            connection,
            feature_key=feature_key,
            artifact_type=artifact_type,
            target_task=resolved_target_task,
            team_code=team_code,
            season_label=season_label,
            latest_limit=latest_limit,
        )

    return {
        "filters": {
            "feature_key": feature_key,
            "artifact_type": artifact_type,
            "target_task": resolved_target_task,
            "team_code": team_code,
            "season_label": season_label,
            "dimensions": list(parsed_dimensions),
            "min_sample_size": min_sample_size,
            "canonical_game_id": canonical_game_id,
            "condition_values": list(parsed_condition_values)
            if parsed_condition_values is not None
            else None,
            "pattern_key": pattern_key,
            "comparable_limit": comparable_limit,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "drop_null_targets": drop_null_targets,
            "latest_limit": latest_limit,
        },
        **history_result,
    }


@router.get("/features/dataset/splits")
def feature_dataset_splits(
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    preview_limit: int = Query(default=5, ge=1, le=20),
) -> dict[str, object]:
    with postgres_connection() as connection:
        split_result = get_feature_dataset_splits_postgres(
            connection,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            preview_limit=preview_limit,
        )

    return {
        "filters": {
            "feature_key": feature_key,
            "team_code": team_code,
            "season_label": season_label,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "preview_limit": preview_limit,
        },
        **split_result,
    }


@router.get("/features/dataset/training-view")
def feature_dataset_training_view(
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    drop_null_targets: bool = Query(default=True),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, object]:
    resolved_target_task, capabilities_payload = _resolve_target_task(target_task)
    _validate_model_admin_inputs(
        capabilities_payload=capabilities_payload,
        target_task=resolved_target_task,
    )
    with postgres_connection() as connection:
        training_view = get_feature_training_view_postgres(
            connection,
            feature_key=feature_key,
            target_task=resolved_target_task,
            team_code=team_code,
            season_label=season_label,
            drop_null_targets=drop_null_targets,
            limit=limit,
            offset=offset,
        )

    return {
        "filters": {
            "feature_key": feature_key,
            "target_task": resolved_target_task,
            "team_code": team_code,
            "season_label": season_label,
            "drop_null_targets": drop_null_targets,
            "limit": limit,
            "offset": offset,
        },
        **training_view,
    }


@router.get("/features/dataset/training-manifest")
def feature_dataset_training_manifest(
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    drop_null_targets: bool = Query(default=True),
) -> dict[str, object]:
    resolved_target_task, capabilities_payload = _resolve_target_task(target_task)
    _validate_model_admin_inputs(
        capabilities_payload=capabilities_payload,
        target_task=resolved_target_task,
    )
    with postgres_connection() as connection:
        training_manifest = get_feature_training_manifest_postgres(
            connection,
            feature_key=feature_key,
            target_task=resolved_target_task,
            team_code=team_code,
            season_label=season_label,
            drop_null_targets=drop_null_targets,
        )

    return {
        "filters": {
            "feature_key": feature_key,
            "target_task": resolved_target_task,
            "team_code": team_code,
            "season_label": season_label,
            "drop_null_targets": drop_null_targets,
        },
        **training_manifest,
    }


@router.get("/features/dataset/training-bundle")
def feature_dataset_training_bundle(
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    drop_null_targets: bool = Query(default=True),
    preview_limit: int = Query(default=5, ge=1, le=20),
) -> dict[str, object]:
    resolved_target_task, capabilities_payload = _resolve_target_task(target_task)
    _validate_model_admin_inputs(
        capabilities_payload=capabilities_payload,
        target_task=resolved_target_task,
    )
    with postgres_connection() as connection:
        training_bundle = get_feature_training_bundle_postgres(
            connection,
            feature_key=feature_key,
            target_task=resolved_target_task,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
            preview_limit=preview_limit,
        )

    return {
        "filters": {
            "feature_key": feature_key,
            "target_task": resolved_target_task,
            "team_code": team_code,
            "season_label": season_label,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "drop_null_targets": drop_null_targets,
            "preview_limit": preview_limit,
        },
        **training_bundle,
    }


@router.get("/features/dataset/training-benchmark")
def feature_dataset_training_benchmark(
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    drop_null_targets: bool = Query(default=True),
) -> dict[str, object]:
    resolved_target_task, capabilities_payload = _resolve_target_task(target_task)
    _validate_model_admin_inputs(
        capabilities_payload=capabilities_payload,
        target_task=resolved_target_task,
    )
    with postgres_connection() as connection:
        training_benchmark = get_feature_training_benchmark_postgres(
            connection,
            feature_key=feature_key,
            target_task=resolved_target_task,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
        )

    return {
        "filters": {
            "feature_key": feature_key,
            "target_task": resolved_target_task,
            "team_code": team_code,
            "season_label": season_label,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "drop_null_targets": drop_null_targets,
        },
        **training_benchmark,
    }


@router.get("/features/dataset/training-task-matrix")
def feature_dataset_training_task_matrix(
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    drop_null_targets: bool = Query(default=True),
) -> dict[str, object]:
    with postgres_connection() as connection:
        training_task_matrix = get_feature_training_task_matrix_postgres(
            connection,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
        )

    return {
        "filters": {
            "feature_key": feature_key,
            "team_code": team_code,
            "season_label": season_label,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "drop_null_targets": drop_null_targets,
        },
        **training_task_matrix,
    }
