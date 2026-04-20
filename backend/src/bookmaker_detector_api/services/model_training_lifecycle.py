from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from bookmaker_detector_api.repositories import ModelTrainingArtifactStore
from bookmaker_detector_api.repositories.ingestion_json import _json_dumps
from bookmaker_detector_api.services import model_training_views
from bookmaker_detector_api.services.model_records import (
    ModelEvaluationSnapshotRecord,
    ModelRegistryRecord,
    ModelSelectionSnapshotRecord,
    ModelTrainingRunRecord,
)
from bookmaker_detector_api.services.task_registry import (
    DEFAULT_REGRESSION_SELECTION_POLICY_NAME,
    normalize_selection_policy_name,
)

MODEL_FAMILY_CONFIGS = {
    "linear_feature": {
        "version_label": "Linear Feature Baseline v1",
        "description": "Single-feature ordinary least squares baseline selected by validation MAE.",
    },
    "tree_stump": {
        "version_label": "Tree Stump Baseline v1",
        "description": "Single-split regression stump selected by validation MAE.",
    },
}


def ensure_model_registry_in_memory(
    repository: ModelTrainingArtifactStore,
    *,
    target_task: str,
    model_family: str,
    team_code: str | None,
) -> ModelRegistryRecord:
    model_key = _build_model_key(
        target_task=target_task,
        model_family=model_family,
        team_code=team_code,
    )
    existing = next(
        (entry for entry in repository.model_registries if entry["model_key"] == model_key),
        None,
    )
    if existing is not None:
        return ModelRegistryRecord(**existing)
    config = MODEL_FAMILY_CONFIGS[model_family]
    payload = {
        "id": len(repository.model_registries) + 1,
        "model_key": model_key,
        "target_task": target_task,
        "model_family": model_family,
        "version_label": config["version_label"],
        "description": config["description"],
        "config": {"team_code_scope": team_code},
        "created_at": datetime.now(timezone.utc),
    }
    repository.model_registries.append(payload)
    return ModelRegistryRecord(**payload)


def save_model_training_run_in_memory(
    repository: ModelTrainingArtifactStore,
    run: ModelTrainingRunRecord,
) -> ModelTrainingRunRecord:
    payload = asdict(run)
    payload["id"] = len(repository.model_training_runs) + 1
    payload["created_at"] = datetime.now(timezone.utc)
    payload["completed_at"] = payload["created_at"]
    repository.model_training_runs.append(payload)
    saved_run = ModelTrainingRunRecord(**payload)
    save_model_evaluation_snapshot_in_memory(repository, saved_run)
    return saved_run


def ensure_model_registry_postgres(
    connection: Any,
    *,
    target_task: str,
    model_family: str,
    team_code: str | None,
) -> ModelRegistryRecord:
    config = MODEL_FAMILY_CONFIGS[model_family]
    model_key = _build_model_key(
        target_task=target_task,
        model_family=model_family,
        team_code=team_code,
    )
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO model_registry (
                model_key,
                target_task,
                model_family,
                version_label,
                description,
                config_json
            )
            VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (model_key)
            DO UPDATE SET
                version_label = EXCLUDED.version_label,
                description = EXCLUDED.description,
                config_json = EXCLUDED.config_json
            RETURNING
                id,
                model_key,
                target_task,
                model_family,
                version_label,
                description,
                config_json,
                created_at
            """,
            (
                model_key,
                target_task,
                model_family,
                config["version_label"],
                config["description"],
                _json_dumps({"team_code_scope": team_code}),
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return ModelRegistryRecord(
        id=int(row[0]),
        model_key=row[1],
        target_task=row[2],
        model_family=row[3],
        version_label=row[4],
        description=row[5] or "",
        config=row[6],
        created_at=row[7],
    )


def save_model_training_run_postgres(
    connection: Any,
    run: ModelTrainingRunRecord,
) -> ModelTrainingRunRecord:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO model_training_run (
                model_registry_id,
                feature_version_id,
                target_task,
                scope_team_code,
                scope_season_label,
                status,
                train_ratio,
                validation_ratio,
                artifact_json,
                metrics_json,
                completed_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, NOW())
            RETURNING id, created_at, completed_at
            """,
            (
                run.model_registry_id,
                run.feature_version_id,
                run.target_task,
                run.team_code or "",
                run.season_label or "",
                run.status,
                run.train_ratio,
                run.validation_ratio,
                _json_dumps(run.artifact),
                _json_dumps(run.metrics),
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    saved_run = ModelTrainingRunRecord(
        id=int(row[0]),
        model_registry_id=run.model_registry_id,
        feature_version_id=run.feature_version_id,
        target_task=run.target_task,
        team_code=run.team_code,
        season_label=run.season_label,
        status=run.status,
        train_ratio=run.train_ratio,
        validation_ratio=run.validation_ratio,
        artifact=run.artifact,
        metrics=run.metrics,
        created_at=row[1],
        completed_at=row[2],
    )
    save_model_evaluation_snapshot_postgres(connection, saved_run)
    return saved_run


def promote_best_model_in_memory(
    repository: ModelTrainingArtifactStore,
    *,
    target_task: str,
    selection_policy_name: str = DEFAULT_REGRESSION_SELECTION_POLICY_NAME,
) -> dict[str, Any]:
    normalized_selection_policy_name = normalize_selection_policy_name(selection_policy_name)
    snapshots = model_training_views.list_model_evaluation_snapshots_in_memory(
        repository,
        target_task=target_task,
    )
    selected_snapshot = model_training_views._select_best_evaluation_snapshot(
        snapshots,
        selection_policy_name=normalized_selection_policy_name,
    )
    if selected_snapshot is None:
        return {
            "selection_policy_name": normalized_selection_policy_name,
            "selected_snapshot": None,
            "active_selection": None,
            "selection_count": 0,
        }
    selection = save_model_selection_snapshot_in_memory(
        repository,
        selected_snapshot,
        selection_policy_name=normalized_selection_policy_name,
    )
    selections = model_training_views.list_model_selection_snapshots_in_memory(
        repository,
        target_task=target_task,
        active_only=True,
    )
    return {
        "selection_policy_name": normalized_selection_policy_name,
        "selected_snapshot": model_training_views._serialize_model_evaluation_snapshot(
            selected_snapshot
        ),
        "active_selection": model_training_views._serialize_model_selection_snapshot(selection),
        "selection_count": len(selections),
    }


def promote_best_model_postgres(
    connection: Any,
    *,
    target_task: str,
    selection_policy_name: str = DEFAULT_REGRESSION_SELECTION_POLICY_NAME,
) -> dict[str, Any]:
    normalized_selection_policy_name = normalize_selection_policy_name(selection_policy_name)
    snapshots = model_training_views.list_model_evaluation_snapshots_postgres(
        connection,
        target_task=target_task,
    )
    selected_snapshot = model_training_views._select_best_evaluation_snapshot(
        snapshots,
        selection_policy_name=normalized_selection_policy_name,
    )
    if selected_snapshot is None:
        return {
            "selection_policy_name": normalized_selection_policy_name,
            "selected_snapshot": None,
            "active_selection": None,
            "selection_count": 0,
        }
    selection = save_model_selection_snapshot_postgres(
        connection,
        selected_snapshot,
        selection_policy_name=normalized_selection_policy_name,
    )
    selections = model_training_views.list_model_selection_snapshots_postgres(
        connection,
        target_task=target_task,
        active_only=True,
    )
    return {
        "selection_policy_name": normalized_selection_policy_name,
        "selected_snapshot": model_training_views._serialize_model_evaluation_snapshot(
            selected_snapshot
        ),
        "active_selection": model_training_views._serialize_model_selection_snapshot(selection),
        "selection_count": len(selections),
    }


def save_model_evaluation_snapshot_in_memory(
    repository: ModelTrainingArtifactStore,
    run: ModelTrainingRunRecord,
) -> ModelEvaluationSnapshotRecord:
    payload = _build_model_evaluation_snapshot_payload(run)
    payload["id"] = len(repository.model_evaluation_snapshots) + 1
    payload["created_at"] = datetime.now(timezone.utc)
    repository.model_evaluation_snapshots.append(payload)
    return ModelEvaluationSnapshotRecord(**payload)


def save_model_evaluation_snapshot_postgres(
    connection: Any,
    run: ModelTrainingRunRecord,
) -> ModelEvaluationSnapshotRecord:
    payload = _build_model_evaluation_snapshot_payload(run)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO model_evaluation_snapshot (
                model_training_run_id,
                model_registry_id,
                feature_version_id,
                target_task,
                model_family,
                selected_feature,
                fallback_strategy,
                primary_metric_name,
                primary_metric_direction,
                validation_metric_value,
                test_metric_value,
                validation_prediction_count,
                test_prediction_count,
                selection_score,
                selection_score_name,
                snapshot_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (model_training_run_id)
            DO UPDATE SET
                model_registry_id = EXCLUDED.model_registry_id,
                feature_version_id = EXCLUDED.feature_version_id,
                target_task = EXCLUDED.target_task,
                model_family = EXCLUDED.model_family,
                selected_feature = EXCLUDED.selected_feature,
                fallback_strategy = EXCLUDED.fallback_strategy,
                primary_metric_name = EXCLUDED.primary_metric_name,
                primary_metric_direction = EXCLUDED.primary_metric_direction,
                validation_metric_value = EXCLUDED.validation_metric_value,
                test_metric_value = EXCLUDED.test_metric_value,
                validation_prediction_count = EXCLUDED.validation_prediction_count,
                test_prediction_count = EXCLUDED.test_prediction_count,
                selection_score = EXCLUDED.selection_score,
                selection_score_name = EXCLUDED.selection_score_name,
                snapshot_json = EXCLUDED.snapshot_json
            RETURNING id, created_at
            """,
            (
                payload["model_training_run_id"],
                payload["model_registry_id"],
                payload["feature_version_id"],
                payload["target_task"],
                payload["model_family"],
                payload["selected_feature"],
                payload["fallback_strategy"],
                payload["primary_metric_name"],
                payload["primary_metric_direction"],
                payload["validation_metric_value"],
                payload["test_metric_value"],
                payload["validation_prediction_count"],
                payload["test_prediction_count"],
                payload["selection_score"],
                payload["selection_score_name"],
                _json_dumps(payload["snapshot"]),
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return ModelEvaluationSnapshotRecord(
        id=int(row[0]),
        created_at=row[1],
        **payload,
    )


def save_model_selection_snapshot_in_memory(
    repository: ModelTrainingArtifactStore,
    snapshot: ModelEvaluationSnapshotRecord,
    *,
    selection_policy_name: str,
) -> ModelSelectionSnapshotRecord:
    normalized_selection_policy_name = normalize_selection_policy_name(selection_policy_name)
    for entry in repository.model_selection_snapshots:
        if entry["target_task"] == snapshot.target_task:
            entry["is_active"] = False
    payload = {
        "id": len(repository.model_selection_snapshots) + 1,
        "model_evaluation_snapshot_id": snapshot.id,
        "model_training_run_id": snapshot.model_training_run_id,
        "model_registry_id": snapshot.model_registry_id,
        "feature_version_id": snapshot.feature_version_id,
        "target_task": snapshot.target_task,
        "model_family": snapshot.model_family,
        "selection_policy_name": normalized_selection_policy_name,
        "rationale": {
            "primary_metric_name": snapshot.primary_metric_name,
            "primary_metric_direction": snapshot.primary_metric_direction,
            "validation_metric_value": snapshot.validation_metric_value,
            "fallback_strategy": snapshot.fallback_strategy,
            "selection_score": snapshot.selection_score,
            "selection_score_name": snapshot.selection_score_name,
        },
        "is_active": True,
        "created_at": datetime.now(timezone.utc),
    }
    repository.model_selection_snapshots.append(payload)
    return ModelSelectionSnapshotRecord(**payload)


def save_model_selection_snapshot_postgres(
    connection: Any,
    snapshot: ModelEvaluationSnapshotRecord,
    *,
    selection_policy_name: str,
) -> ModelSelectionSnapshotRecord:
    normalized_selection_policy_name = normalize_selection_policy_name(selection_policy_name)
    rationale = {
        "primary_metric_name": snapshot.primary_metric_name,
        "primary_metric_direction": snapshot.primary_metric_direction,
        "validation_metric_value": snapshot.validation_metric_value,
        "fallback_strategy": snapshot.fallback_strategy,
        "selection_score": snapshot.selection_score,
        "selection_score_name": snapshot.selection_score_name,
    }
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE model_selection_snapshot SET is_active = FALSE WHERE target_task = %s",
            (snapshot.target_task,),
        )
        cursor.execute(
            """
            INSERT INTO model_selection_snapshot (
                model_evaluation_snapshot_id,
                model_training_run_id,
                model_registry_id,
                feature_version_id,
                target_task,
                model_family,
                selection_policy_name,
                rationale_json,
                is_active
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, TRUE)
            RETURNING id, created_at
            """,
            (
                snapshot.id,
                snapshot.model_training_run_id,
                snapshot.model_registry_id,
                snapshot.feature_version_id,
                snapshot.target_task,
                snapshot.model_family,
                normalized_selection_policy_name,
                _json_dumps(rationale),
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return ModelSelectionSnapshotRecord(
        id=int(row[0]),
        model_evaluation_snapshot_id=snapshot.id,
        model_training_run_id=snapshot.model_training_run_id,
        model_registry_id=snapshot.model_registry_id,
        feature_version_id=snapshot.feature_version_id,
        target_task=snapshot.target_task,
        model_family=snapshot.model_family,
        selection_policy_name=normalized_selection_policy_name,
        rationale=rationale,
        is_active=True,
        created_at=row[1],
    )


def _build_model_evaluation_snapshot_payload(run: ModelTrainingRunRecord) -> dict[str, Any]:
    return {
        "model_training_run_id": run.id,
        "model_registry_id": run.model_registry_id,
        "feature_version_id": run.feature_version_id,
        "target_task": run.target_task,
        "model_family": str(run.artifact.get("model_family", "unknown")),
        "selected_feature": run.artifact.get("selected_feature"),
        "fallback_strategy": run.artifact.get("fallback_strategy"),
        "primary_metric_name": "mae",
        "primary_metric_direction": "lower_is_better",
        "validation_metric_value": model_training_views._float_or_none(
            run.metrics.get("validation", {}).get("mae")
        ),
        "test_metric_value": model_training_views._float_or_none(
            run.metrics.get("test", {}).get("mae")
        ),
        "validation_prediction_count": int(
            run.metrics.get("validation", {}).get("prediction_count", 0)
        ),
        "test_prediction_count": int(run.metrics.get("test", {}).get("prediction_count", 0)),
        "selection_score": _build_selection_score(run),
        "selection_score_name": DEFAULT_REGRESSION_SELECTION_POLICY_NAME,
        "snapshot": {
            "artifact": run.artifact,
            "metrics": run.metrics,
            "target_task": run.target_task,
            "scope": {
                "team_code": run.team_code,
                "season_label": run.season_label,
            },
        },
    }


def _build_model_key(
    *,
    target_task: str,
    model_family: str,
    team_code: str | None,
) -> str:
    scope = team_code.lower() if team_code is not None else "global"
    return f"{target_task}_{model_family}_{scope}_v1"


def _build_selection_score(run: ModelTrainingRunRecord) -> float | None:
    validation_mae = model_training_views._float_or_none(run.metrics.get("validation", {}).get("mae"))
    if validation_mae is None:
        return None
    fallback_penalty = 1000.0 if run.artifact.get("fallback_strategy") is not None else 0.0
    return float(validation_mae) + fallback_penalty
