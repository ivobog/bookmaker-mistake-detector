from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from bookmaker_detector_api.repositories import ModelTrainingArtifactStore
from bookmaker_detector_api.services.model_records import (
    ModelEvaluationSnapshotRecord,
    ModelRegistryRecord,
    ModelSelectionSnapshotRecord,
    ModelTrainingRunRecord,
)


def list_model_registry_in_memory(
    repository: ModelTrainingArtifactStore,
    *,
    target_task: str | None = None,
) -> list[ModelRegistryRecord]:
    return [
        ModelRegistryRecord(**entry)
        for entry in repository.model_registries
        if target_task is None or entry["target_task"] == target_task
    ]


def list_model_training_runs_in_memory(
    repository: ModelTrainingArtifactStore,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
) -> list[ModelTrainingRunRecord]:
    selected = [
        ModelTrainingRunRecord(**entry)
        for entry in repository.model_training_runs
        if (target_task is None or entry["target_task"] == target_task)
        and (team_code is None or entry.get("team_code") == team_code)
        and (season_label is None or entry.get("season_label") == season_label)
    ]
    return sorted(
        selected,
        key=lambda entry: (
            entry.completed_at or entry.created_at or datetime.min.replace(tzinfo=timezone.utc),
            entry.id,
        ),
        reverse=True,
    )


def get_model_training_run_detail_in_memory(
    repository: ModelTrainingArtifactStore,
    *,
    run_id: int,
) -> ModelTrainingRunRecord | None:
    return next(
        (
            run
            for run in list_model_training_runs_in_memory(repository)
            if int(run.id) == int(run_id)
        ),
        None,
    )


def list_model_registry_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
) -> list[ModelRegistryRecord]:
    query = """
        SELECT
            id,
            model_key,
            target_task,
            model_family,
            version_label,
            description,
            config_json,
            created_at
        FROM model_registry
    """
    params: list[Any] = []
    if target_task is not None:
        query += " WHERE target_task = %s"
        params.append(target_task)
    query += " ORDER BY target_task ASC, model_family ASC"
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelRegistryRecord(
            id=int(row[0]),
            model_key=row[1],
            target_task=row[2],
            model_family=row[3],
            version_label=row[4],
            description=row[5] or "",
            config=row[6],
            created_at=row[7],
        )
        for row in rows
    ]


def list_model_training_runs_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
) -> list[ModelTrainingRunRecord]:
    query = """
        SELECT
            id,
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
            created_at,
            completed_at
        FROM model_training_run
        WHERE 1=1
    """
    params: list[Any] = []
    if target_task is not None:
        query += " AND target_task = %s"
        params.append(target_task)
    if team_code is not None:
        query += " AND scope_team_code = %s"
        params.append(team_code)
    if season_label is not None:
        query += " AND scope_season_label = %s"
        params.append(season_label)
    query += " ORDER BY completed_at DESC NULLS LAST, id DESC"
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelTrainingRunRecord(
            id=int(row[0]),
            model_registry_id=int(row[1]),
            feature_version_id=int(row[2]),
            target_task=row[3],
            team_code=row[4] or None,
            season_label=row[5] or None,
            status=row[6],
            train_ratio=float(row[7]),
            validation_ratio=float(row[8]),
            artifact=row[9],
            metrics=row[10],
            created_at=row[11],
            completed_at=row[12],
        )
        for row in rows
    ]


def get_model_training_run_detail_postgres(
    connection: Any,
    *,
    run_id: int,
) -> ModelTrainingRunRecord | None:
    return next(
        (
            run
            for run in list_model_training_runs_postgres(connection)
            if int(run.id) == int(run_id)
        ),
        None,
    )


def list_model_evaluation_snapshots_in_memory(
    repository: ModelTrainingArtifactStore,
    *,
    target_task: str | None = None,
    model_family: str | None = None,
) -> list[ModelEvaluationSnapshotRecord]:
    selected = [
        ModelEvaluationSnapshotRecord(**entry)
        for entry in repository.model_evaluation_snapshots
        if (target_task is None or entry["target_task"] == target_task)
        and (model_family is None or entry["model_family"] == model_family)
    ]
    return sorted(
        selected,
        key=lambda entry: (
            entry.created_at or datetime.min.replace(tzinfo=timezone.utc),
            entry.id,
        ),
        reverse=True,
    )


def get_model_evaluation_snapshot_detail_in_memory(
    repository: ModelTrainingArtifactStore,
    *,
    snapshot_id: int,
) -> ModelEvaluationSnapshotRecord | None:
    return next(
        (
            snapshot
            for snapshot in list_model_evaluation_snapshots_in_memory(repository)
            if int(snapshot.id) == int(snapshot_id)
        ),
        None,
    )


def list_model_evaluation_snapshots_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    model_family: str | None = None,
) -> list[ModelEvaluationSnapshotRecord]:
    query = """
        SELECT
            id,
            model_training_run_id,
            model_registry_id,
            feature_version_id,
            target_task,
            model_family,
            selected_feature,
            fallback_strategy,
            primary_metric_name,
            validation_metric_value,
            test_metric_value,
            validation_prediction_count,
            test_prediction_count,
            snapshot_json,
            created_at
        FROM model_evaluation_snapshot
        WHERE 1=1
    """
    params: list[Any] = []
    if target_task is not None:
        query += " AND target_task = %s"
        params.append(target_task)
    if model_family is not None:
        query += " AND model_family = %s"
        params.append(model_family)
    query += " ORDER BY created_at DESC, id DESC"
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelEvaluationSnapshotRecord(
            id=int(row[0]),
            model_training_run_id=int(row[1]),
            model_registry_id=int(row[2]),
            feature_version_id=int(row[3]),
            target_task=row[4],
            model_family=row[5],
            selected_feature=row[6],
            fallback_strategy=row[7],
            primary_metric_name=row[8],
            validation_metric_value=_float_or_none(row[9]),
            test_metric_value=_float_or_none(row[10]),
            validation_prediction_count=int(row[11]),
            test_prediction_count=int(row[12]),
            snapshot=row[13],
            created_at=row[14],
        )
        for row in rows
    ]


def get_model_evaluation_snapshot_detail_postgres(
    connection: Any,
    *,
    snapshot_id: int,
) -> ModelEvaluationSnapshotRecord | None:
    return next(
        (
            snapshot
            for snapshot in list_model_evaluation_snapshots_postgres(connection)
            if int(snapshot.id) == int(snapshot_id)
        ),
        None,
    )


def get_model_training_summary_in_memory(
    repository: ModelTrainingArtifactStore,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
) -> dict[str, Any]:
    runs = list_model_training_runs_in_memory(
        repository,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
    )
    return _summarize_model_training_runs(runs)


def get_model_training_summary_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
) -> dict[str, Any]:
    runs = list_model_training_runs_postgres(
        connection,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
    )
    return _summarize_model_training_runs(runs)


def get_model_training_history_in_memory(
    repository: ModelTrainingArtifactStore,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    runs = list_model_training_runs_in_memory(
        repository,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
    )
    return _summarize_model_training_history(runs, recent_limit=recent_limit)


def get_model_training_history_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    runs = list_model_training_runs_postgres(
        connection,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
    )
    return _summarize_model_training_history(runs, recent_limit=recent_limit)


def get_model_evaluation_history_in_memory(
    repository: ModelTrainingArtifactStore,
    *,
    target_task: str | None = None,
    model_family: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    snapshots = list_model_evaluation_snapshots_in_memory(
        repository,
        target_task=target_task,
        model_family=model_family,
    )
    return _summarize_model_evaluation_history(snapshots, recent_limit=recent_limit)


def get_model_evaluation_history_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    model_family: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    snapshots = list_model_evaluation_snapshots_postgres(
        connection,
        target_task=target_task,
        model_family=model_family,
    )
    return _summarize_model_evaluation_history(snapshots, recent_limit=recent_limit)


def list_model_selection_snapshots_in_memory(
    repository: ModelTrainingArtifactStore,
    *,
    target_task: str | None = None,
    active_only: bool = False,
) -> list[ModelSelectionSnapshotRecord]:
    selected = [
        ModelSelectionSnapshotRecord(**entry)
        for entry in repository.model_selection_snapshots
        if (target_task is None or entry["target_task"] == target_task)
        and (not active_only or bool(entry["is_active"]))
    ]
    return sorted(
        selected,
        key=lambda entry: (
            entry.created_at or datetime.min.replace(tzinfo=timezone.utc),
            entry.id,
        ),
        reverse=True,
    )


def get_model_selection_snapshot_detail_in_memory(
    repository: ModelTrainingArtifactStore,
    *,
    selection_id: int,
) -> ModelSelectionSnapshotRecord | None:
    return next(
        (
            selection
            for selection in list_model_selection_snapshots_in_memory(repository)
            if int(selection.id) == int(selection_id)
        ),
        None,
    )


def list_model_selection_snapshots_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    active_only: bool = False,
) -> list[ModelSelectionSnapshotRecord]:
    query = """
        SELECT
            id,
            model_evaluation_snapshot_id,
            model_training_run_id,
            model_registry_id,
            feature_version_id,
            target_task,
            model_family,
            selection_policy_name,
            rationale_json,
            is_active,
            created_at
        FROM model_selection_snapshot
        WHERE 1=1
    """
    params: list[Any] = []
    if target_task is not None:
        query += " AND target_task = %s"
        params.append(target_task)
    if active_only:
        query += " AND is_active = TRUE"
    query += " ORDER BY created_at DESC, id DESC"
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelSelectionSnapshotRecord(
            id=int(row[0]),
            model_evaluation_snapshot_id=int(row[1]),
            model_training_run_id=int(row[2]),
            model_registry_id=int(row[3]),
            feature_version_id=int(row[4]),
            target_task=row[5],
            model_family=row[6],
            selection_policy_name=row[7],
            rationale=row[8],
            is_active=bool(row[9]),
            created_at=row[10],
        )
        for row in rows
    ]


def get_model_selection_snapshot_detail_postgres(
    connection: Any,
    *,
    selection_id: int,
) -> ModelSelectionSnapshotRecord | None:
    return next(
        (
            selection
            for selection in list_model_selection_snapshots_postgres(connection)
            if int(selection.id) == int(selection_id)
        ),
        None,
    )


def get_model_selection_history_in_memory(
    repository: ModelTrainingArtifactStore,
    *,
    target_task: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    selections = list_model_selection_snapshots_in_memory(
        repository,
        target_task=target_task,
    )
    return _summarize_model_selection_history(selections, recent_limit=recent_limit)


def get_model_selection_history_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    selections = list_model_selection_snapshots_postgres(
        connection,
        target_task=target_task,
    )
    return _summarize_model_selection_history(selections, recent_limit=recent_limit)


def _summarize_model_training_runs(
    runs: list[ModelTrainingRunRecord],
) -> dict[str, Any]:
    latest_run = runs[0] if runs else None
    best_overall = min(runs, key=_model_run_rank_key) if runs else None
    model_family_counts: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    best_by_family: dict[str, dict[str, Any]] = {}
    feature_selected_count = 0
    usable_run_count = 0
    fallback_run_count = 0

    for run in runs:
        model_family = str(run.artifact.get("model_family", "unknown"))
        model_family_counts[model_family] = model_family_counts.get(model_family, 0) + 1
        status_counts[run.status] = status_counts.get(run.status, 0) + 1
        if run.artifact.get("selected_feature") is not None:
            feature_selected_count += 1
        if int(run.metrics.get("validation", {}).get("prediction_count", 0)) > 0:
            usable_run_count += 1
        if run.artifact.get("fallback_strategy") is not None:
            fallback_run_count += 1
        incumbent = best_by_family.get(model_family)
        if incumbent is None or _model_run_rank_key(run) < _model_run_rank_key_object(incumbent):
            best_by_family[model_family] = _serialize_model_training_run(run)

    return {
        "run_count": len(runs),
        "status_counts": status_counts,
        "model_family_counts": model_family_counts,
        "feature_selected_count": feature_selected_count,
        "usable_run_count": usable_run_count,
        "fallback_run_count": fallback_run_count,
        "latest_run": _serialize_model_training_run(latest_run),
        "best_overall": _serialize_model_training_run(best_overall),
        "best_by_family": best_by_family,
    }


def _summarize_model_training_history(
    runs: list[ModelTrainingRunRecord],
    *,
    recent_limit: int,
) -> dict[str, Any]:
    summary = _summarize_model_training_runs(runs)
    daily_rollups: dict[str, dict[str, Any]] = {}

    for run in runs:
        completed_at = run.completed_at or run.created_at
        if completed_at is None:
            continue
        day_key = completed_at.date().isoformat()
        bucket = daily_rollups.setdefault(
            day_key,
            {
                "date": day_key,
                "run_count": 0,
                "usable_run_count": 0,
                "fallback_run_count": 0,
                "model_family_counts": {},
                "best_validation_mae": None,
            },
        )
        bucket["run_count"] += 1
        if int(run.metrics.get("validation", {}).get("prediction_count", 0)) > 0:
            bucket["usable_run_count"] += 1
        if run.artifact.get("fallback_strategy") is not None:
            bucket["fallback_run_count"] += 1
        family = str(run.artifact.get("model_family", "unknown"))
        family_counts = bucket["model_family_counts"]
        family_counts[family] = family_counts.get(family, 0) + 1
        validation_mae = run.metrics.get("validation", {}).get("mae")
        if validation_mae is not None:
            current_best = bucket["best_validation_mae"]
            if current_best is None or float(validation_mae) < float(current_best):
                bucket["best_validation_mae"] = float(validation_mae)

    daily_buckets = [daily_rollups[key] for key in sorted(daily_rollups.keys())]
    return {
        "overview": summary,
        "daily_buckets": daily_buckets,
        "recent_runs": [_serialize_model_training_run(run) for run in runs[:recent_limit]],
    }


def _summarize_model_evaluation_history(
    snapshots: list[ModelEvaluationSnapshotRecord],
    *,
    recent_limit: int,
) -> dict[str, Any]:
    model_family_counts: dict[str, int] = {}
    fallback_strategy_counts: dict[str, int] = {}
    daily_rollups: dict[str, dict[str, Any]] = {}

    for snapshot in snapshots:
        model_family_counts[snapshot.model_family] = (
            model_family_counts.get(snapshot.model_family, 0) + 1
        )
        if snapshot.fallback_strategy is not None:
            fallback_strategy_counts[snapshot.fallback_strategy] = (
                fallback_strategy_counts.get(snapshot.fallback_strategy, 0) + 1
            )
        created_at = snapshot.created_at
        if created_at is None:
            continue
        day_key = created_at.date().isoformat()
        bucket = daily_rollups.setdefault(
            day_key,
            {
                "date": day_key,
                "snapshot_count": 0,
                "fallback_count": 0,
                "model_family_counts": {},
                "best_validation_metric": None,
            },
        )
        bucket["snapshot_count"] += 1
        if snapshot.fallback_strategy is not None:
            bucket["fallback_count"] += 1
        bucket_family_counts = bucket["model_family_counts"]
        bucket_family_counts[snapshot.model_family] = (
            bucket_family_counts.get(snapshot.model_family, 0) + 1
        )
        if snapshot.validation_metric_value is not None:
            current_best = bucket["best_validation_metric"]
            if current_best is None or snapshot.validation_metric_value < current_best:
                bucket["best_validation_metric"] = snapshot.validation_metric_value

    return {
        "overview": {
            "snapshot_count": len(snapshots),
            "model_family_counts": model_family_counts,
            "fallback_strategy_counts": fallback_strategy_counts,
            "latest_snapshot": _serialize_model_evaluation_snapshot(
                snapshots[0] if snapshots else None
            ),
        },
        "daily_buckets": [daily_rollups[key] for key in sorted(daily_rollups.keys())],
        "recent_snapshots": [
            _serialize_model_evaluation_snapshot(snapshot)
            for snapshot in snapshots[:recent_limit]
        ],
    }


def _summarize_model_selection_history(
    selections: list[ModelSelectionSnapshotRecord],
    *,
    recent_limit: int,
) -> dict[str, Any]:
    policy_counts: dict[str, int] = {}
    family_counts: dict[str, int] = {}
    active_count = 0
    for selection in selections:
        policy_counts[selection.selection_policy_name] = (
            policy_counts.get(selection.selection_policy_name, 0) + 1
        )
        family_counts[selection.model_family] = (
            family_counts.get(selection.model_family, 0) + 1
        )
        if selection.is_active:
            active_count += 1
    return {
        "overview": {
            "selection_count": len(selections),
            "active_selection_count": active_count,
            "policy_counts": policy_counts,
            "model_family_counts": family_counts,
            "latest_selection": _serialize_model_selection_snapshot(
                selections[0] if selections else None
            ),
        },
        "recent_selections": [
            _serialize_model_selection_snapshot(selection)
            for selection in selections[:recent_limit]
        ],
    }


def _model_run_rank_key(run: ModelTrainingRunRecord) -> tuple[int, float, int]:
    validation_metrics = run.metrics.get("validation", {})
    return (
        1 if run.artifact.get("fallback_strategy") is not None else 0,
        _metric_value_or_inf(validation_metrics.get("mae")),
        -int(validation_metrics.get("prediction_count", 0)),
    )


def _model_run_rank_key_object(run: dict[str, Any]) -> tuple[int, float, int]:
    validation_metrics = run.get("metrics", {}).get("validation", {})
    return (
        1 if run.get("artifact", {}).get("fallback_strategy") is not None else 0,
        _metric_value_or_inf(validation_metrics.get("mae")),
        -int(validation_metrics.get("prediction_count", 0)),
    )


def _serialize_model_training_run(
    run: ModelTrainingRunRecord | None,
) -> dict[str, Any] | None:
    if run is None:
        return None
    return {
        "id": run.id,
        "model_registry_id": run.model_registry_id,
        "feature_version_id": run.feature_version_id,
        "target_task": run.target_task,
        "team_code": run.team_code,
        "season_label": run.season_label,
        "status": run.status,
        "train_ratio": run.train_ratio,
        "validation_ratio": run.validation_ratio,
        "artifact": run.artifact,
        "metrics": run.metrics,
        "created_at": run.created_at.isoformat() if run.created_at else None,
        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
    }


def _serialize_model_evaluation_snapshot(
    snapshot: ModelEvaluationSnapshotRecord | None,
) -> dict[str, Any] | None:
    if snapshot is None:
        return None
    return {
        "id": snapshot.id,
        "model_training_run_id": snapshot.model_training_run_id,
        "model_registry_id": snapshot.model_registry_id,
        "feature_version_id": snapshot.feature_version_id,
        "target_task": snapshot.target_task,
        "model_family": snapshot.model_family,
        "selected_feature": snapshot.selected_feature,
        "fallback_strategy": snapshot.fallback_strategy,
        "primary_metric_name": snapshot.primary_metric_name,
        "validation_metric_value": snapshot.validation_metric_value,
        "test_metric_value": snapshot.test_metric_value,
        "validation_prediction_count": snapshot.validation_prediction_count,
        "test_prediction_count": snapshot.test_prediction_count,
        "snapshot": snapshot.snapshot,
        "created_at": snapshot.created_at.isoformat() if snapshot.created_at else None,
    }


def _serialize_model_selection_snapshot(
    selection: ModelSelectionSnapshotRecord | None,
) -> dict[str, Any] | None:
    if selection is None:
        return None
    return {
        "id": selection.id,
        "model_evaluation_snapshot_id": selection.model_evaluation_snapshot_id,
        "model_training_run_id": selection.model_training_run_id,
        "model_registry_id": selection.model_registry_id,
        "feature_version_id": selection.feature_version_id,
        "target_task": selection.target_task,
        "model_family": selection.model_family,
        "selection_policy_name": selection.selection_policy_name,
        "rationale": selection.rationale,
        "is_active": selection.is_active,
        "created_at": selection.created_at.isoformat() if selection.created_at else None,
    }


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _metric_value_or_inf(value: Any) -> float:
    if value is None:
        return float("inf")
    return float(value)


def _select_best_evaluation_snapshot(
    snapshots: list[ModelEvaluationSnapshotRecord],
    *,
    selection_policy_name: str,
) -> ModelEvaluationSnapshotRecord | None:
    if selection_policy_name != "validation_mae_candidate_v1":
        raise ValueError(f"Unsupported selection policy: {selection_policy_name}")
    if not snapshots:
        return None
    return min(
        snapshots,
        key=lambda snapshot: (
            1 if snapshot.fallback_strategy is not None else 0,
            _metric_value_or_inf(snapshot.validation_metric_value),
            _metric_value_or_inf(snapshot.test_metric_value),
            -snapshot.validation_prediction_count,
            -snapshot.test_prediction_count,
            snapshot.id,
        ),
    )
