from datetime import date

from fastapi import APIRouter, Query

from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.demo import (
    run_phase_one_demo,
    run_phase_one_fetch_demo,
    run_phase_one_fetch_failure_demo,
    run_phase_one_persistence_demo,
    run_phase_one_worker_demo,
    seed_phase_two_feature_in_memory,
    seed_phase_two_feature_postgres,
)
from bookmaker_detector_api.demo import (
    run_phase_one_fetch_reporting_demo as run_phase_one_fetch_reporting_demo_job,
)
from bookmaker_detector_api.demo import (
    run_phase_two_feature_demo as run_phase_two_feature_demo_job,
)
from bookmaker_detector_api.repositories import InMemoryIngestionRepository
from bookmaker_detector_api.services.admin_diagnostics import (
    get_admin_diagnostics,
    resolve_started_window,
)
from bookmaker_detector_api.services.data_quality_maintenance import (
    normalize_data_quality_taxonomy,
)
from bookmaker_detector_api.services.features import (
    get_feature_analysis_artifact_catalog_in_memory,
    get_feature_analysis_artifact_catalog_postgres,
    get_feature_analysis_artifact_history_in_memory,
    get_feature_analysis_artifact_history_postgres,
    get_feature_comparable_cases_in_memory,
    get_feature_comparable_cases_postgres,
    get_feature_dataset_in_memory,
    get_feature_dataset_postgres,
    get_feature_dataset_profile_in_memory,
    get_feature_dataset_profile_postgres,
    get_feature_dataset_splits_in_memory,
    get_feature_dataset_splits_postgres,
    get_feature_evidence_bundle_in_memory,
    get_feature_evidence_bundle_postgres,
    get_feature_pattern_catalog_in_memory,
    get_feature_pattern_catalog_postgres,
    get_feature_snapshot_catalog_in_memory,
    get_feature_snapshot_catalog_postgres,
    get_feature_snapshot_summary_in_memory,
    get_feature_snapshot_summary_postgres,
    get_feature_training_benchmark_in_memory,
    get_feature_training_benchmark_postgres,
    get_feature_training_bundle_in_memory,
    get_feature_training_bundle_postgres,
    get_feature_training_manifest_in_memory,
    get_feature_training_manifest_postgres,
    get_feature_training_task_matrix_in_memory,
    get_feature_training_task_matrix_postgres,
    get_feature_training_view_in_memory,
    get_feature_training_view_postgres,
    materialize_feature_analysis_artifacts_in_memory,
    materialize_feature_analysis_artifacts_postgres,
)

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/providers")
def list_supported_providers() -> dict[str, list[dict[str, str]]]:
    return {
        "providers": [
            {
                "name": "covers",
                "type": "historical_team_page",
                "status": "fixture_backed",
            }
        ]
    }


@router.get("/phase-1-demo")
def phase_one_demo() -> dict[str, object]:
    return run_phase_one_demo()


@router.get("/phase-1-persistence-demo")
def phase_one_persistence_demo() -> dict[str, object]:
    return run_phase_one_persistence_demo()


@router.get("/phase-1-worker-demo")
def phase_one_worker_demo() -> dict[str, object]:
    return run_phase_one_worker_demo()


@router.get("/phase-1-fetch-demo")
def phase_one_fetch_demo() -> dict[str, object]:
    return run_phase_one_fetch_demo()


@router.get("/phase-1-fetch-failure-demo")
def phase_one_fetch_failure_demo() -> dict[str, object]:
    return run_phase_one_fetch_failure_demo()


@router.get("/phase-1-fetch-reporting-demo")
def phase_one_fetch_reporting_demo(
    repository_mode: str = Query(default="in_memory"),
) -> dict[str, object]:
    return run_phase_one_fetch_reporting_demo_job(repository_mode=repository_mode)


@router.get("/phase-2-feature-demo")
def phase_two_feature_demo(
    repository_mode: str = Query(default="in_memory"),
) -> dict[str, object]:
    return run_phase_two_feature_demo_job(repository_mode=repository_mode)


@router.get("/features/snapshots")
def feature_snapshots(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        snapshot_result = get_feature_snapshot_catalog_in_memory(
            repository,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
            limit=limit,
            offset=offset,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            snapshot_result = get_feature_snapshot_catalog_postgres(
                connection,
                feature_key=feature_key,
                team_code=team_code,
                season_label=season_label,
                limit=limit,
                offset=offset,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "team_code": team_code,
            "season_label": season_label,
            "limit": limit,
            "offset": offset,
        },
        **snapshot_result,
    }


@router.get("/features/summary")
def feature_summary(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        summary_result = get_feature_snapshot_summary_in_memory(
            repository,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            summary_result = get_feature_snapshot_summary_postgres(
                connection,
                feature_key=feature_key,
                team_code=team_code,
                season_label=season_label,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "team_code": team_code,
            "season_label": season_label,
        },
        **summary_result,
    }


@router.get("/features/dataset")
def feature_dataset(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        dataset_result = get_feature_dataset_in_memory(
            repository,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
            limit=limit,
            offset=offset,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            dataset_result = get_feature_dataset_postgres(
                connection,
                feature_key=feature_key,
                team_code=team_code,
                season_label=season_label,
                limit=limit,
                offset=offset,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
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
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        profile_result = get_feature_dataset_profile_in_memory(
            repository,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            profile_result = get_feature_dataset_profile_postgres(
                connection,
                feature_key=feature_key,
                team_code=team_code,
                season_label=season_label,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "team_code": team_code,
            "season_label": season_label,
        },
        **profile_result,
    }


@router.get("/features/patterns")
def feature_patterns(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
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
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
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
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
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
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

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


@router.get("/features/comparables")
def feature_comparables(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
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
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
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
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
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
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

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


@router.get("/features/evidence")
def feature_evidence(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
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
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
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
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
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
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

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


@router.post("/features/analysis/materialize")
def materialize_feature_analysis(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
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
    parsed_dimensions = tuple(
        dimension.strip() for dimension in dimensions.split(",") if dimension.strip()
    )
    parsed_condition_values = (
        tuple(value.strip() for value in condition_values.split(","))
        if condition_values is not None
        else None
    )
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        materialize_result = materialize_feature_analysis_artifacts_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
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
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            materialize_result = materialize_feature_analysis_artifacts_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
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
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
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
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    artifact_type: str | None = Query(default=None),
    target_task: str = Query(default="spread_error_regression"),
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
    parsed_dimensions = tuple(
        dimension.strip() for dimension in dimensions.split(",") if dimension.strip()
    )
    parsed_condition_values = (
        tuple(value.strip() for value in condition_values.split(","))
        if condition_values is not None
        else None
    )
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
            materialize_feature_analysis_artifacts_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
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
        artifact_result = get_feature_analysis_artifact_catalog_in_memory(
            repository,
            feature_key=feature_key,
            artifact_type=artifact_type,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            limit=limit,
            offset=offset,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
                materialize_feature_analysis_artifacts_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
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
            artifact_result = get_feature_analysis_artifact_catalog_postgres(
                connection,
                feature_key=feature_key,
                artifact_type=artifact_type,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                limit=limit,
                offset=offset,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "artifact_type": artifact_type,
            "target_task": target_task,
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
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    artifact_type: str | None = Query(default=None),
    target_task: str = Query(default="spread_error_regression"),
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
    parsed_dimensions = tuple(
        dimension.strip() for dimension in dimensions.split(",") if dimension.strip()
    )
    parsed_condition_values = (
        tuple(value.strip() for value in condition_values.split(","))
        if condition_values is not None
        else None
    )
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
            materialize_feature_analysis_artifacts_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
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
        history_result = get_feature_analysis_artifact_history_in_memory(
            repository,
            feature_key=feature_key,
            artifact_type=artifact_type,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            latest_limit=latest_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
                materialize_feature_analysis_artifacts_postgres(
                    connection,
                    feature_key=feature_key,
                    target_task=target_task,
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
            history_result = get_feature_analysis_artifact_history_postgres(
                connection,
                feature_key=feature_key,
                artifact_type=artifact_type,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                latest_limit=latest_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "artifact_type": artifact_type,
            "target_task": target_task,
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
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    preview_limit: int = Query(default=5, ge=1, le=20),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        split_result = get_feature_dataset_splits_in_memory(
            repository,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            preview_limit=preview_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            split_result = get_feature_dataset_splits_postgres(
                connection,
                feature_key=feature_key,
                team_code=team_code,
                season_label=season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                preview_limit=preview_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
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
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    drop_null_targets: bool = Query(default=True),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        training_view = get_feature_training_view_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            drop_null_targets=drop_null_targets,
            limit=limit,
            offset=offset,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            training_view = get_feature_training_view_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                drop_null_targets=drop_null_targets,
                limit=limit,
                offset=offset,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
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
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    drop_null_targets: bool = Query(default=True),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        training_manifest = get_feature_training_manifest_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            drop_null_targets=drop_null_targets,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            training_manifest = get_feature_training_manifest_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                drop_null_targets=drop_null_targets,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "team_code": team_code,
            "season_label": season_label,
            "drop_null_targets": drop_null_targets,
        },
        **training_manifest,
    }


@router.get("/features/dataset/training-bundle")
def feature_dataset_training_bundle(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    drop_null_targets: bool = Query(default=True),
    preview_limit: int = Query(default=5, ge=1, le=20),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        training_bundle = get_feature_training_bundle_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
            preview_limit=preview_limit,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            training_bundle = get_feature_training_bundle_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                drop_null_targets=drop_null_targets,
                preview_limit=preview_limit,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
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
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    drop_null_targets: bool = Query(default=True),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        training_benchmark = get_feature_training_benchmark_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            training_benchmark = get_feature_training_benchmark_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                drop_null_targets=drop_null_targets,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
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
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    feature_key: str = Query(default="baseline_team_features_v1"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    drop_null_targets: bool = Query(default=True),
) -> dict[str, object]:
    if repository_mode == "in_memory":
        repository = InMemoryIngestionRepository()
        if seed_demo:
            repository, _, _ = seed_phase_two_feature_in_memory()
        training_task_matrix = get_feature_training_task_matrix_in_memory(
            repository,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
        )
    elif repository_mode == "postgres":
        with postgres_connection() as connection:
            if seed_demo:
                seed_phase_two_feature_postgres(connection)
            training_task_matrix = get_feature_training_task_matrix_postgres(
                connection,
                feature_key=feature_key,
                team_code=team_code,
                season_label=season_label,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                drop_null_targets=drop_null_targets,
            )
    else:
        raise ValueError(f"Unsupported repository mode: {repository_mode}")

    return {
        "repository_mode": repository_mode,
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


@router.get("/jobs/recent")
def recent_job_runs(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    status: str | None = Query(default=None),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
    started_from: date | None = Query(default=None),
    started_to: date | None = Query(default=None),
) -> dict[str, object]:
    resolved_started_from, resolved_started_to = resolve_started_window(
        started_from=started_from,
        started_to=started_to,
    )
    diagnostics = get_admin_diagnostics(
        repository_mode=repository_mode,
        seed_demo=seed_demo,
        job_limit=limit,
        job_offset=offset,
        retrieval_limit=20,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
        job_status=status,
        started_from=resolved_started_from,
        started_to=resolved_started_to,
    )
    return {
        "repository_mode": diagnostics["repository_mode"],
        "filters": diagnostics["filters"],
        "job_runs": diagnostics["job_runs"],
    }


@router.get("/ingestion/issues")
def recent_ingestion_issues(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    status: str = Query(default="FAILED"),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
) -> dict[str, object]:
    diagnostics = get_admin_diagnostics(
        repository_mode=repository_mode,
        seed_demo=seed_demo,
        job_limit=20,
        retrieval_limit=limit,
        retrieval_offset=offset,
        retrieval_status=status,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
    )
    return {
        "repository_mode": diagnostics["repository_mode"],
        "filters": diagnostics["filters"],
        "page_retrievals": diagnostics["page_retrievals"],
    }


@router.get("/data-quality/issues")
def recent_data_quality_issues(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    severity: str | None = Query(default=None),
    issue_type: str | None = Query(default=None),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
) -> dict[str, object]:
    diagnostics = get_admin_diagnostics(
        repository_mode=repository_mode,
        seed_demo=seed_demo,
        quality_issue_limit=limit,
        quality_issue_offset=offset,
        quality_issue_severity=severity,
        quality_issue_type=issue_type,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
    )
    return {
        "repository_mode": diagnostics["repository_mode"],
        "filters": diagnostics["filters"],
        "data_quality_issues": diagnostics["data_quality_issues"],
    }


@router.get("/ingestion/stats")
def ingestion_stats(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
) -> dict[str, object]:
    diagnostics = get_admin_diagnostics(
        repository_mode=repository_mode,
        seed_demo=seed_demo,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
    )
    return {
        "repository_mode": diagnostics["repository_mode"],
        "filters": diagnostics["filters"],
        "stats": diagnostics["stats"],
    }


@router.get("/validation-runs/compare")
def compare_validation_runs(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    run_label: str = Query(..., min_length=1),
    limit: int = Query(default=10, ge=2, le=50),
    status: str | None = Query(default=None),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    started_from: date | None = Query(default=None),
    started_to: date | None = Query(default=None),
) -> dict[str, object]:
    resolved_started_from, resolved_started_to = resolve_started_window(
        started_from=started_from,
        started_to=started_to,
    )
    diagnostics = get_admin_diagnostics(
        repository_mode=repository_mode,
        seed_demo=seed_demo,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
        job_status=status,
        validation_compare_limit=limit,
        started_from=resolved_started_from,
        started_to=resolved_started_to,
    )
    return {
        "repository_mode": diagnostics["repository_mode"],
        "filters": diagnostics["filters"],
        "validation_run_comparison": diagnostics["validation_run_comparison"],
    }


@router.get("/ingestion/trends")
def ingestion_trends(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    limit: int = Query(default=20, ge=1, le=100),
    days: int | None = Query(default=7, ge=1, le=365),
    started_from: date | None = Query(default=None),
    started_to: date | None = Query(default=None),
    status: str | None = Query(default=None),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
) -> dict[str, object]:
    resolved_started_from, resolved_started_to = resolve_started_window(
        started_from=started_from,
        started_to=started_to,
        days=days,
    )
    diagnostics = get_admin_diagnostics(
        repository_mode=repository_mode,
        seed_demo=seed_demo,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
        job_status=status,
        trend_limit=limit,
        started_from=resolved_started_from,
        started_to=resolved_started_to,
    )
    return {
        "repository_mode": diagnostics["repository_mode"],
        "filters": diagnostics["filters"],
        "trends": diagnostics["trends"],
    }


@router.get("/retrieval/trends")
def retrieval_trends(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    days: int | None = Query(default=7, ge=1, le=365),
    started_from: date | None = Query(default=None),
    started_to: date | None = Query(default=None),
    status: str | None = Query(default=None),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
) -> dict[str, object]:
    resolved_started_from, resolved_started_to = resolve_started_window(
        started_from=started_from,
        started_to=started_to,
        days=days,
    )
    diagnostics = get_admin_diagnostics(
        repository_mode=repository_mode,
        seed_demo=seed_demo,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
        retrieval_status=status,
        started_from=resolved_started_from,
        started_to=resolved_started_to,
    )
    return {
        "repository_mode": diagnostics["repository_mode"],
        "filters": diagnostics["filters"],
        "retrieval_trends": diagnostics["retrieval_trends"],
    }


@router.get("/ingestion/quality-trends")
def ingestion_quality_trends(
    repository_mode: str = Query(default="in_memory"),
    seed_demo: bool = Query(default=True),
    days: int | None = Query(default=7, ge=1, le=365),
    started_from: date | None = Query(default=None),
    started_to: date | None = Query(default=None),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    run_label: str | None = Query(default=None),
) -> dict[str, object]:
    resolved_started_from, resolved_started_to = resolve_started_window(
        started_from=started_from,
        started_to=started_to,
        days=days,
    )
    diagnostics = get_admin_diagnostics(
        repository_mode=repository_mode,
        seed_demo=seed_demo,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        run_label=run_label,
        started_from=resolved_started_from,
        started_to=resolved_started_to,
    )
    return {
        "repository_mode": diagnostics["repository_mode"],
        "filters": diagnostics["filters"],
        "quality_trends": diagnostics["quality_trends"],
    }


@router.post("/data-quality/normalize-taxonomy")
def normalize_data_quality_issue_taxonomy(
    repository_mode: str = Query(default="in_memory"),
    provider_name: str | None = Query(default=None),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    dry_run: bool = Query(default=True),
) -> dict[str, object]:
    return normalize_data_quality_taxonomy(
        repository_mode=repository_mode,
        provider_name=provider_name,
        team_code=team_code,
        season_label=season_label,
        dry_run=dry_run,
    )
