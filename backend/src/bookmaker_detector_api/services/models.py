from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timezone
from statistics import mean, median
from typing import Any

from bookmaker_detector_api.repositories import InMemoryIngestionRepository
from bookmaker_detector_api.repositories.ingestion import _json_dumps
from bookmaker_detector_api.services.features import (
    DEFAULT_FEATURE_KEY,
    FeatureVersionRecord,
    _partition_feature_dataset_rows,
    build_feature_dataset_rows,
    build_feature_evidence_bundle,
    build_feature_training_view,
    build_future_feature_dataset_rows,
    get_feature_version_in_memory,
    get_feature_version_postgres,
    list_canonical_game_metric_records_in_memory,
    list_canonical_game_metric_records_postgres,
    list_feature_snapshots_in_memory,
    list_feature_snapshots_postgres,
    resolve_feature_condition_values_for_row,
)
from bookmaker_detector_api.services.model_market_board_sources import (
    MARKET_BOARD_SOURCE_CONFIGS,
    _build_market_board_source_fingerprint_comparison,
    _build_market_board_source_payload_fingerprints,
    _build_market_board_source_request_context,
    _normalize_market_board_source_games,
    build_model_market_board_source_games,
)
from bookmaker_detector_api.services.model_market_board_store import (
    _build_market_board_refresh_change_summary,
    _build_model_market_board,
    _build_model_market_board_cadence_batch,
    _build_model_market_board_refresh_batch,
    _build_model_market_board_scoring_batch,
    _build_model_market_board_source_run,
    _find_model_market_board_in_memory,
    _find_model_market_board_postgres,
    _resolve_market_board_refresh_count,
    _resolve_market_board_refresh_status,
    _serialize_model_market_board,
    _serialize_model_market_board_cadence_batch,
    _serialize_model_market_board_refresh_batch,
    _serialize_model_market_board_scoring_batch,
    _serialize_model_market_board_source_run,
    _summarize_market_board_refresh_history,
    _summarize_model_market_board_source_run_history,
    list_model_market_board_cadence_batches_in_memory,
    list_model_market_board_cadence_batches_postgres,
    list_model_market_board_refresh_batches_in_memory,
    list_model_market_board_refresh_batches_postgres,
    list_model_market_board_refresh_events_in_memory,
    list_model_market_board_refresh_events_postgres,
    list_model_market_board_scoring_batches_in_memory,
    list_model_market_board_scoring_batches_postgres,
    list_model_market_board_source_runs_in_memory,
    list_model_market_board_source_runs_postgres,
    list_model_market_boards_in_memory,
    list_model_market_boards_postgres,
    save_model_market_board_cadence_batch_in_memory,
    save_model_market_board_cadence_batch_postgres,
    save_model_market_board_in_memory,
    save_model_market_board_postgres,
    save_model_market_board_refresh_batch_in_memory,
    save_model_market_board_refresh_batch_postgres,
    save_model_market_board_refresh_event_in_memory,
    save_model_market_board_refresh_event_postgres,
    save_model_market_board_scoring_batch_in_memory,
    save_model_market_board_scoring_batch_postgres,
    save_model_market_board_source_run_in_memory,
    save_model_market_board_source_run_postgres,
)
from bookmaker_detector_api.services.model_market_board_views import (
    _build_market_board_cadence_dashboard,
    _build_market_board_cadence_result,
    _build_market_board_operations_summary,
    _build_market_board_refresh_orchestration_result,
    _build_market_board_refresh_queue,
    _build_market_board_scoring_queue,
    _resolve_market_board_refresh_game_date,
    _serialize_model_opportunity,
    _serialize_model_scoring_run,
    _summarize_model_market_board_cadence_batch_history,
    _summarize_model_market_board_refresh_batch_history,
    _summarize_model_market_board_scoring_batch_history,
)
from bookmaker_detector_api.services.model_records import (
    ModelBacktestRunRecord,
    ModelEvaluationSnapshotRecord,
    ModelMarketBoardRecord,
    ModelMarketBoardRefreshRecord,
    ModelOpportunityRecord,
    ModelRegistryRecord,
    ModelScoringRunRecord,
    ModelSelectionSnapshotRecord,
    ModelTrainingRunRecord,
)

SUPPORTED_MODEL_TARGET_TASKS = {
    "point_margin_regression",
    "spread_error_regression",
    "total_error_regression",
    "total_points_regression",
}
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
OPPORTUNITY_POLICY_CONFIGS = {
    "spread_error_regression": {
        "policy_name": "spread_edge_policy_v1",
        "candidate_min_signal_strength": 2.0,
        "review_min_signal_strength": 1.0,
        "candidate_evidence_ratings": {"strong"},
        "review_evidence_ratings": {"weak", "moderate", "strong"},
        "candidate_recommendation_statuses": {"candidate_signal"},
        "review_recommendation_statuses": {
            "monitor_only",
            "review_manually",
            "candidate_signal",
        },
    },
    "total_error_regression": {
        "policy_name": "total_edge_policy_v1",
        "candidate_min_signal_strength": 2.0,
        "review_min_signal_strength": 1.0,
        "candidate_evidence_ratings": {"strong"},
        "review_evidence_ratings": {"weak", "moderate", "strong"},
        "candidate_recommendation_statuses": {"candidate_signal"},
        "review_recommendation_statuses": {
            "monitor_only",
            "review_manually",
            "candidate_signal",
        },
    },
    "point_margin_regression": {
        "policy_name": "margin_signal_policy_v1",
        "candidate_min_signal_strength": 4.0,
        "review_min_signal_strength": 2.5,
        "candidate_evidence_ratings": {"strong"},
        "review_evidence_ratings": {"weak", "moderate", "strong"},
        "candidate_recommendation_statuses": {"candidate_signal"},
        "review_recommendation_statuses": {
            "monitor_only",
            "review_manually",
            "candidate_signal",
        },
    },
    "total_points_regression": {
        "policy_name": "totals_signal_policy_v1",
        "candidate_min_signal_strength": 4.0,
        "review_min_signal_strength": 2.5,
        "candidate_evidence_ratings": {"strong"},
        "review_evidence_ratings": {"weak", "moderate", "strong"},
        "candidate_recommendation_statuses": {"candidate_signal"},
        "review_recommendation_statuses": {
            "monitor_only",
            "review_manually",
            "candidate_signal",
        },
    },
}

def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def train_phase_three_models_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    team_code: str | None = None,
    season_label: str | None = None,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
) -> dict[str, Any]:
    feature_version = get_feature_version_in_memory(repository, feature_key=feature_key)
    if feature_version is None:
        return {
            "feature_version": None,
            "dataset_row_count": 0,
            "model_runs": [],
            "best_model": None,
        }
    dataset_rows = _load_training_dataset_rows_in_memory(
        repository,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
    )
    return _train_phase_three_models(
        dataset_rows=dataset_rows,
        feature_version=feature_version,
        team_code=team_code,
        season_label=season_label,
        target_task=target_task,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        ensure_registry=lambda model_family: ensure_model_registry_in_memory(
            repository,
            target_task=target_task,
            model_family=model_family,
            team_code=team_code,
        ),
        save_run=lambda run: save_model_training_run_in_memory(repository, run),
        list_runs=lambda: list_model_training_runs_in_memory(
            repository,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
        ),
    )


def train_phase_three_models_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    team_code: str | None = None,
    season_label: str | None = None,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
) -> dict[str, Any]:
    feature_version = get_feature_version_postgres(connection, feature_key=feature_key)
    if feature_version is None:
        return {
            "feature_version": None,
            "dataset_row_count": 0,
            "model_runs": [],
            "best_model": None,
        }
    dataset_rows = _load_training_dataset_rows_postgres(
        connection,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
    )
    return _train_phase_three_models(
        dataset_rows=dataset_rows,
        feature_version=feature_version,
        team_code=team_code,
        season_label=season_label,
        target_task=target_task,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        ensure_registry=lambda model_family: ensure_model_registry_postgres(
            connection,
            target_task=target_task,
            model_family=model_family,
            team_code=team_code,
        ),
        save_run=lambda run: save_model_training_run_postgres(connection, run),
        list_runs=lambda: list_model_training_runs_postgres(
            connection,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
        ),
    )


def list_model_registry_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str | None = None,
) -> list[ModelRegistryRecord]:
    return [
        ModelRegistryRecord(**entry)
        for entry in repository.model_registries
        if target_task is None or entry["target_task"] == target_task
    ]


def list_model_training_runs_in_memory(
    repository: InMemoryIngestionRepository,
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
    repository: InMemoryIngestionRepository,
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


def ensure_model_registry_in_memory(
    repository: InMemoryIngestionRepository,
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
    repository: InMemoryIngestionRepository,
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
    repository: InMemoryIngestionRepository,
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
    repository: InMemoryIngestionRepository,
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
    repository: InMemoryIngestionRepository,
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
    repository: InMemoryIngestionRepository,
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
    repository: InMemoryIngestionRepository,
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
    repository: InMemoryIngestionRepository,
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
    repository: InMemoryIngestionRepository,
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


def promote_best_model_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str,
    selection_policy_name: str = "validation_mae_candidate_v1",
) -> dict[str, Any]:
    snapshots = list_model_evaluation_snapshots_in_memory(
        repository,
        target_task=target_task,
    )
    selected_snapshot = _select_best_evaluation_snapshot(
        snapshots,
        selection_policy_name=selection_policy_name,
    )
    if selected_snapshot is None:
        return {
            "selection_policy_name": selection_policy_name,
            "selected_snapshot": None,
            "active_selection": None,
            "selection_count": 0,
        }
    selection = save_model_selection_snapshot_in_memory(
        repository,
        selected_snapshot,
        selection_policy_name=selection_policy_name,
    )
    selections = list_model_selection_snapshots_in_memory(
        repository,
        target_task=target_task,
        active_only=True,
    )
    return {
        "selection_policy_name": selection_policy_name,
        "selected_snapshot": _serialize_model_evaluation_snapshot(selected_snapshot),
        "active_selection": _serialize_model_selection_snapshot(selection),
        "selection_count": len(selections),
    }


def promote_best_model_postgres(
    connection: Any,
    *,
    target_task: str,
    selection_policy_name: str = "validation_mae_candidate_v1",
) -> dict[str, Any]:
    snapshots = list_model_evaluation_snapshots_postgres(
        connection,
        target_task=target_task,
    )
    selected_snapshot = _select_best_evaluation_snapshot(
        snapshots,
        selection_policy_name=selection_policy_name,
    )
    if selected_snapshot is None:
        return {
            "selection_policy_name": selection_policy_name,
            "selected_snapshot": None,
            "active_selection": None,
            "selection_count": 0,
        }
    selection = save_model_selection_snapshot_postgres(
        connection,
        selected_snapshot,
        selection_policy_name=selection_policy_name,
    )
    selections = list_model_selection_snapshots_postgres(
        connection,
        target_task=target_task,
        active_only=True,
    )
    return {
        "selection_policy_name": selection_policy_name,
        "selected_snapshot": _serialize_model_evaluation_snapshot(selected_snapshot),
        "active_selection": _serialize_model_selection_snapshot(selection),
        "selection_count": len(selections),
    }


def get_model_scoring_preview_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    team_code: str | None = None,
    season_label: str | None = None,
    canonical_game_id: int | None = None,
    limit: int = 10,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    feature_version = get_feature_version_in_memory(repository, feature_key=feature_key)
    if feature_version is None:
        return {
            "feature_version": None,
            "active_selection": None,
            "active_evaluation_snapshot": None,
            "row_count": 0,
            "scored_prediction_count": 0,
            "prediction_summary": {},
            "predictions": [],
        }
    active_selection = _resolve_active_model_selection(
        selections=list_model_selection_snapshots_in_memory(
            repository,
            target_task=target_task,
            active_only=True,
        ),
    )
    active_snapshot = _resolve_evaluation_snapshot_by_id(
        snapshots=list_model_evaluation_snapshots_in_memory(
            repository,
            target_task=target_task,
        ),
        snapshot_id=(
            active_selection.model_evaluation_snapshot_id if active_selection is not None else None
        ),
    )
    dataset_rows = _load_training_dataset_rows_in_memory(
        repository,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
    )
    scoring_result = _build_model_scoring_preview(
        dataset_rows=dataset_rows,
        target_task=target_task,
        active_selection=active_selection,
        active_snapshot=active_snapshot,
        team_code=team_code,
        canonical_game_id=canonical_game_id,
        limit=limit,
        include_evidence=include_evidence,
        evidence_dimensions=evidence_dimensions,
        comparable_limit=comparable_limit,
        min_pattern_sample_size=min_pattern_sample_size,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
    )
    return {
        "feature_version": asdict(feature_version),
        **scoring_result,
    }


def get_model_scoring_preview_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    team_code: str | None = None,
    season_label: str | None = None,
    canonical_game_id: int | None = None,
    limit: int = 10,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    feature_version = get_feature_version_postgres(connection, feature_key=feature_key)
    if feature_version is None:
        return {
            "feature_version": None,
            "active_selection": None,
            "active_evaluation_snapshot": None,
            "row_count": 0,
            "scored_prediction_count": 0,
            "prediction_summary": {},
            "predictions": [],
        }
    active_selection = _resolve_active_model_selection(
        selections=list_model_selection_snapshots_postgres(
            connection,
            target_task=target_task,
            active_only=True,
        ),
    )
    active_snapshot = _resolve_evaluation_snapshot_by_id(
        snapshots=list_model_evaluation_snapshots_postgres(
            connection,
            target_task=target_task,
        ),
        snapshot_id=(
            active_selection.model_evaluation_snapshot_id if active_selection is not None else None
        ),
    )
    dataset_rows = _load_training_dataset_rows_postgres(
        connection,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
    )
    scoring_result = _build_model_scoring_preview(
        dataset_rows=dataset_rows,
        target_task=target_task,
        active_selection=active_selection,
        active_snapshot=active_snapshot,
        team_code=team_code,
        canonical_game_id=canonical_game_id,
        limit=limit,
        include_evidence=include_evidence,
        evidence_dimensions=evidence_dimensions,
        comparable_limit=comparable_limit,
        min_pattern_sample_size=min_pattern_sample_size,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
    )
    return {
        "feature_version": asdict(feature_version),
        **scoring_result,
    }


def get_model_future_game_preview_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    season_label: str,
    game_date: date,
    home_team_code: str,
    away_team_code: str,
    home_spread_line: float | None = None,
    total_line: float | None = None,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    feature_version = get_feature_version_in_memory(repository, feature_key=feature_key)
    if feature_version is None:
        return {
            "feature_version": None,
            "active_selection": None,
            "active_evaluation_snapshot": None,
            "scenario": None,
            "scored_prediction_count": 0,
            "prediction_summary": {},
            "predictions": [],
            "opportunity_preview": [],
        }
    active_selection = _resolve_active_model_selection(
        selections=list_model_selection_snapshots_in_memory(
            repository,
            target_task=target_task,
            active_only=True,
        ),
    )
    active_snapshot = _resolve_evaluation_snapshot_by_id(
        snapshots=list_model_evaluation_snapshots_in_memory(
            repository,
            target_task=target_task,
        ),
        snapshot_id=(
            active_selection.model_evaluation_snapshot_id if active_selection is not None else None
        ),
    )
    canonical_games = list_canonical_game_metric_records_in_memory(repository)
    historical_dataset_rows = _load_training_dataset_rows_in_memory(
        repository,
        feature_version_id=feature_version.id,
        team_code=None,
        season_label=None,
    )
    scenario_rows = build_future_feature_dataset_rows(
        canonical_games,
        feature_version_id=feature_version.id,
        season_label=season_label,
        game_date=game_date,
        home_team_code=home_team_code,
        away_team_code=away_team_code,
        home_spread_line=home_spread_line,
        total_line=total_line,
    )
    return {
        "feature_version": asdict(feature_version),
        **_build_model_future_game_preview(
            target_task=target_task,
            active_selection=active_selection,
            active_snapshot=active_snapshot,
            historical_dataset_rows=historical_dataset_rows,
            scenario_rows=scenario_rows,
            include_evidence=include_evidence,
            evidence_dimensions=evidence_dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
        ),
    }


def get_model_future_game_preview_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    season_label: str,
    game_date: date,
    home_team_code: str,
    away_team_code: str,
    home_spread_line: float | None = None,
    total_line: float | None = None,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    feature_version = get_feature_version_postgres(connection, feature_key=feature_key)
    if feature_version is None:
        return {
            "feature_version": None,
            "active_selection": None,
            "active_evaluation_snapshot": None,
            "scenario": None,
            "scored_prediction_count": 0,
            "prediction_summary": {},
            "predictions": [],
            "opportunity_preview": [],
        }
    active_selection = _resolve_active_model_selection(
        selections=list_model_selection_snapshots_postgres(
            connection,
            target_task=target_task,
            active_only=True,
        ),
    )
    active_snapshot = _resolve_evaluation_snapshot_by_id(
        snapshots=list_model_evaluation_snapshots_postgres(
            connection,
            target_task=target_task,
        ),
        snapshot_id=(
            active_selection.model_evaluation_snapshot_id if active_selection is not None else None
        ),
    )
    canonical_games = list_canonical_game_metric_records_postgres(connection)
    historical_dataset_rows = _load_training_dataset_rows_postgres(
        connection,
        feature_version_id=feature_version.id,
        team_code=None,
        season_label=None,
    )
    scenario_rows = build_future_feature_dataset_rows(
        canonical_games,
        feature_version_id=feature_version.id,
        season_label=season_label,
        game_date=game_date,
        home_team_code=home_team_code,
        away_team_code=away_team_code,
        home_spread_line=home_spread_line,
        total_line=total_line,
    )
    return {
        "feature_version": asdict(feature_version),
        **_build_model_future_game_preview(
            target_task=target_task,
            active_selection=active_selection,
            active_snapshot=active_snapshot,
            historical_dataset_rows=historical_dataset_rows,
            scenario_rows=scenario_rows,
            include_evidence=include_evidence,
            evidence_dimensions=evidence_dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
        ),
    }


def materialize_model_future_game_preview_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    model_market_board_id: int | None = None,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    season_label: str,
    game_date: date,
    home_team_code: str,
    away_team_code: str,
    home_spread_line: float | None = None,
    total_line: float | None = None,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    preview = get_model_future_game_preview_in_memory(
        repository,
        feature_key=feature_key,
        target_task=target_task,
        season_label=season_label,
        game_date=game_date,
        home_team_code=home_team_code,
        away_team_code=away_team_code,
        home_spread_line=home_spread_line,
        total_line=total_line,
        include_evidence=include_evidence,
        evidence_dimensions=evidence_dimensions,
        comparable_limit=comparable_limit,
        min_pattern_sample_size=min_pattern_sample_size,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
    )
    scoring_run = _build_model_scoring_run(
        preview=preview,
        target_task=target_task,
        model_market_board_id=model_market_board_id,
    )
    persisted = save_model_scoring_run_in_memory(repository, scoring_run)
    return {
        **preview,
        "materialized_count": 1 if persisted is not None else 0,
        "scoring_run": _serialize_model_scoring_run(persisted),
    }


def materialize_model_future_game_preview_postgres(
    connection: Any,
    *,
    model_market_board_id: int | None = None,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    season_label: str,
    game_date: date,
    home_team_code: str,
    away_team_code: str,
    home_spread_line: float | None = None,
    total_line: float | None = None,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    preview = get_model_future_game_preview_postgres(
        connection,
        feature_key=feature_key,
        target_task=target_task,
        season_label=season_label,
        game_date=game_date,
        home_team_code=home_team_code,
        away_team_code=away_team_code,
        home_spread_line=home_spread_line,
        total_line=total_line,
        include_evidence=include_evidence,
        evidence_dimensions=evidence_dimensions,
        comparable_limit=comparable_limit,
        min_pattern_sample_size=min_pattern_sample_size,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
    )
    scoring_run = _build_model_scoring_run(
        preview=preview,
        target_task=target_task,
        model_market_board_id=model_market_board_id,
    )
    persisted = save_model_scoring_run_postgres(connection, scoring_run)
    return {
        **preview,
        "materialized_count": 1 if persisted is not None else 0,
        "scoring_run": _serialize_model_scoring_run(persisted),
    }


def list_model_scoring_runs_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    model_market_board_id: int | None = None,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
) -> list[ModelScoringRunRecord]:
    selected = [
        ModelScoringRunRecord(**entry)
        for entry in repository.model_scoring_runs
        if (
            model_market_board_id is None
            or entry.get("model_market_board_id") == model_market_board_id
        )
        if (target_task is None or entry["target_task"] == target_task)
        and (
            team_code is None
            or entry["home_team_code"] == team_code
            or entry["away_team_code"] == team_code
        )
        and (season_label is None or entry["season_label"] == season_label)
    ]
    return sorted(
        selected,
        key=lambda entry: (
            entry.created_at or datetime.min.replace(tzinfo=timezone.utc),
            entry.id,
        ),
        reverse=True,
    )


def list_model_scoring_runs_postgres(
    connection: Any,
    *,
    model_market_board_id: int | None = None,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
) -> list[ModelScoringRunRecord]:
    query = """
        SELECT
            id,
            model_market_board_id,
            model_selection_snapshot_id,
            model_evaluation_snapshot_id,
            feature_version_id,
            target_task,
            scenario_key,
            season_label,
            game_date,
            home_team_code,
            away_team_code,
            home_spread_line,
            total_line,
            policy_name,
            prediction_count,
            candidate_opportunity_count,
            review_opportunity_count,
            discarded_opportunity_count,
            payload_json,
            created_at
        FROM model_scoring_run
        WHERE 1=1
    """
    params: list[Any] = []
    if model_market_board_id is not None:
        query += " AND model_market_board_id = %s"
        params.append(model_market_board_id)
    if target_task is not None:
        query += " AND target_task = %s"
        params.append(target_task)
    if team_code is not None:
        query += " AND (home_team_code = %s OR away_team_code = %s)"
        params.extend([team_code, team_code])
    if season_label is not None:
        query += " AND season_label = %s"
        params.append(season_label)
    query += " ORDER BY created_at DESC, id DESC"
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelScoringRunRecord(
            id=int(row[0]),
            model_market_board_id=int(row[1]) if row[1] is not None else None,
            model_selection_snapshot_id=int(row[2]) if row[2] is not None else None,
            model_evaluation_snapshot_id=int(row[3]) if row[3] is not None else None,
            feature_version_id=int(row[4]),
            target_task=row[5],
            scenario_key=row[6],
            season_label=row[7],
            game_date=row[8],
            home_team_code=row[9],
            away_team_code=row[10],
            home_spread_line=_float_or_none(row[11]),
            total_line=_float_or_none(row[12]),
            policy_name=row[13],
            prediction_count=int(row[14]),
            candidate_opportunity_count=int(row[15]),
            review_opportunity_count=int(row[16]),
            discarded_opportunity_count=int(row[17]),
            payload=row[18],
            created_at=row[19],
        )
        for row in rows
    ]


def get_model_scoring_run_detail_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    scoring_run_id: int,
) -> dict[str, Any] | None:
    scoring_run = next(
        (
            entry
            for entry in list_model_scoring_runs_in_memory(repository)
            if entry.id == scoring_run_id
        ),
        None,
    )
    return _serialize_model_scoring_run(scoring_run)


def get_model_scoring_run_detail_postgres(
    connection: Any,
    *,
    scoring_run_id: int,
) -> dict[str, Any] | None:
    scoring_run = next(
        (
            entry
            for entry in list_model_scoring_runs_postgres(connection)
            if entry.id == scoring_run_id
        ),
        None,
    )
    return _serialize_model_scoring_run(scoring_run)


def get_model_scoring_history_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    model_market_board_id: int | None = None,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    scoring_runs = list_model_scoring_runs_in_memory(
        repository,
        model_market_board_id=model_market_board_id,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
    )
    return _summarize_model_scoring_history(scoring_runs, recent_limit=recent_limit)


def get_model_scoring_history_postgres(
    connection: Any,
    *,
    model_market_board_id: int | None = None,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    scoring_runs = list_model_scoring_runs_postgres(
        connection,
        model_market_board_id=model_market_board_id,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
    )
    return _summarize_model_scoring_history(scoring_runs, recent_limit=recent_limit)


def materialize_model_opportunities_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    team_code: str | None = None,
    season_label: str | None = None,
    canonical_game_id: int | None = None,
    limit: int = 10,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    scoring_preview = get_model_scoring_preview_in_memory(
        repository,
        feature_key=feature_key,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        canonical_game_id=canonical_game_id,
        limit=limit,
        include_evidence=include_evidence,
        evidence_dimensions=evidence_dimensions,
        comparable_limit=comparable_limit,
        min_pattern_sample_size=min_pattern_sample_size,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
    )
    opportunities = _build_model_opportunities(
        scoring_preview=scoring_preview,
        target_task=target_task,
    )
    persisted = save_model_opportunities_in_memory(repository, opportunities)
    return {
        **scoring_preview,
        "materialized_count": len(persisted),
        "opportunity_count": len(persisted),
        "opportunities": [_serialize_model_opportunity(entry) for entry in persisted],
    }


def materialize_model_future_opportunities_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    season_label: str,
    game_date: date,
    home_team_code: str,
    away_team_code: str,
    home_spread_line: float | None = None,
    total_line: float | None = None,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    materialized_preview = materialize_model_future_game_preview_in_memory(
        repository,
        feature_key=feature_key,
        target_task=target_task,
        season_label=season_label,
        game_date=game_date,
        home_team_code=home_team_code,
        away_team_code=away_team_code,
        home_spread_line=home_spread_line,
        total_line=total_line,
        include_evidence=include_evidence,
        evidence_dimensions=evidence_dimensions,
        comparable_limit=comparable_limit,
        min_pattern_sample_size=min_pattern_sample_size,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
    )
    scoring_run = materialized_preview.get("scoring_run")
    opportunities = _build_model_opportunities(
        scoring_preview=materialized_preview,
        target_task=target_task,
        model_scoring_run_id=(int(scoring_run["id"]) if scoring_run is not None else None),
        allow_best_effort_review=True,
    )
    persisted = save_model_opportunities_in_memory(repository, opportunities)
    return {
        **materialized_preview,
        "opportunity_count": len(persisted),
        "opportunities": [_serialize_model_opportunity(entry) for entry in persisted],
    }


def materialize_model_opportunities_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    team_code: str | None = None,
    season_label: str | None = None,
    canonical_game_id: int | None = None,
    limit: int = 10,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    scoring_preview = get_model_scoring_preview_postgres(
        connection,
        feature_key=feature_key,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        canonical_game_id=canonical_game_id,
        limit=limit,
        include_evidence=include_evidence,
        evidence_dimensions=evidence_dimensions,
        comparable_limit=comparable_limit,
        min_pattern_sample_size=min_pattern_sample_size,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
    )
    opportunities = _build_model_opportunities(
        scoring_preview=scoring_preview,
        target_task=target_task,
    )
    persisted = save_model_opportunities_postgres(connection, opportunities)
    return {
        **scoring_preview,
        "materialized_count": len(persisted),
        "opportunity_count": len(persisted),
        "opportunities": [_serialize_model_opportunity(entry) for entry in persisted],
    }


def materialize_model_future_opportunities_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    season_label: str,
    game_date: date,
    home_team_code: str,
    away_team_code: str,
    home_spread_line: float | None = None,
    total_line: float | None = None,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    materialized_preview = materialize_model_future_game_preview_postgres(
        connection,
        feature_key=feature_key,
        target_task=target_task,
        season_label=season_label,
        game_date=game_date,
        home_team_code=home_team_code,
        away_team_code=away_team_code,
        home_spread_line=home_spread_line,
        total_line=total_line,
        include_evidence=include_evidence,
        evidence_dimensions=evidence_dimensions,
        comparable_limit=comparable_limit,
        min_pattern_sample_size=min_pattern_sample_size,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
    )
    scoring_run = materialized_preview.get("scoring_run")
    opportunities = _build_model_opportunities(
        scoring_preview=materialized_preview,
        target_task=target_task,
        model_scoring_run_id=(int(scoring_run["id"]) if scoring_run is not None else None),
        allow_best_effort_review=True,
    )
    persisted = save_model_opportunities_postgres(connection, opportunities)
    return {
        **materialized_preview,
        "opportunity_count": len(persisted),
        "opportunities": [_serialize_model_opportunity(entry) for entry in persisted],
    }


def get_model_future_slate_preview_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    games: list[dict[str, Any]],
    slate_label: str | None = None,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    previews = [
        {
            "input": _serialize_future_game_input(game),
            **get_model_future_game_preview_in_memory(
                repository,
                feature_key=feature_key,
                target_task=target_task,
                season_label=str(game["season_label"]),
                game_date=_coerce_date(game["game_date"]),
                home_team_code=str(game["home_team_code"]),
                away_team_code=str(game["away_team_code"]),
                home_spread_line=_float_or_none(game.get("home_spread_line")),
                total_line=_float_or_none(game.get("total_line")),
                include_evidence=include_evidence,
                evidence_dimensions=evidence_dimensions,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                drop_null_targets=drop_null_targets,
            ),
        }
        for game in games
    ]
    return _build_future_slate_response(
        target_task=target_task,
        slate_label=slate_label,
        game_inputs=games,
        games=previews,
    )


def get_model_future_slate_preview_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    games: list[dict[str, Any]],
    slate_label: str | None = None,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    previews = [
        {
            "input": _serialize_future_game_input(game),
            **get_model_future_game_preview_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                season_label=str(game["season_label"]),
                game_date=_coerce_date(game["game_date"]),
                home_team_code=str(game["home_team_code"]),
                away_team_code=str(game["away_team_code"]),
                home_spread_line=_float_or_none(game.get("home_spread_line")),
                total_line=_float_or_none(game.get("total_line")),
                include_evidence=include_evidence,
                evidence_dimensions=evidence_dimensions,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                drop_null_targets=drop_null_targets,
            ),
        }
        for game in games
    ]
    return _build_future_slate_response(
        target_task=target_task,
        slate_label=slate_label,
        game_inputs=games,
        games=previews,
    )


def materialize_model_future_slate_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    model_market_board_id: int | None = None,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    games: list[dict[str, Any]],
    slate_label: str | None = None,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    materialized_games = []
    materialized_scoring_runs = []
    materialized_opportunities = []
    for game in games:
        materialized_preview = materialize_model_future_game_preview_in_memory(
            repository,
            model_market_board_id=model_market_board_id,
            feature_key=feature_key,
            target_task=target_task,
            season_label=str(game["season_label"]),
            game_date=_coerce_date(game["game_date"]),
            home_team_code=str(game["home_team_code"]),
            away_team_code=str(game["away_team_code"]),
            home_spread_line=_float_or_none(game.get("home_spread_line")),
            total_line=_float_or_none(game.get("total_line")),
            include_evidence=include_evidence,
            evidence_dimensions=evidence_dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
        )
        scoring_run = materialized_preview.get("scoring_run")
        opportunities = _build_model_opportunities(
            scoring_preview=materialized_preview,
            target_task=target_task,
            model_scoring_run_id=(int(scoring_run["id"]) if scoring_run is not None else None),
            allow_best_effort_review=True,
        )
        persisted_opportunities = save_model_opportunities_in_memory(
            repository,
            opportunities,
        )
        serialized_opportunities = [
            _serialize_model_opportunity(entry) for entry in persisted_opportunities
        ]
        materialized_games.append(
            {
                "input": _serialize_future_game_input(game),
                **materialized_preview,
                "opportunity_count": len(serialized_opportunities),
                "opportunities": serialized_opportunities,
            }
        )
        if scoring_run is not None:
            materialized_scoring_runs.append(scoring_run)
        materialized_opportunities.extend(serialized_opportunities)
    return {
        **_build_future_slate_response(
            target_task=target_task,
            slate_label=slate_label,
            game_inputs=games,
            games=materialized_games,
        ),
        "materialized_scoring_run_count": len(materialized_scoring_runs),
        "materialized_opportunity_count": len(materialized_opportunities),
        "scoring_runs": materialized_scoring_runs,
        "opportunities": materialized_opportunities,
    }


def materialize_model_future_slate_postgres(
    connection: Any,
    *,
    model_market_board_id: int | None = None,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    games: list[dict[str, Any]],
    slate_label: str | None = None,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    materialized_games = []
    materialized_scoring_runs = []
    materialized_opportunities = []
    for game in games:
        materialized_preview = materialize_model_future_game_preview_postgres(
            connection,
            model_market_board_id=model_market_board_id,
            feature_key=feature_key,
            target_task=target_task,
            season_label=str(game["season_label"]),
            game_date=_coerce_date(game["game_date"]),
            home_team_code=str(game["home_team_code"]),
            away_team_code=str(game["away_team_code"]),
            home_spread_line=_float_or_none(game.get("home_spread_line")),
            total_line=_float_or_none(game.get("total_line")),
            include_evidence=include_evidence,
            evidence_dimensions=evidence_dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
        )
        scoring_run = materialized_preview.get("scoring_run")
        opportunities = _build_model_opportunities(
            scoring_preview=materialized_preview,
            target_task=target_task,
            model_scoring_run_id=(int(scoring_run["id"]) if scoring_run is not None else None),
            allow_best_effort_review=True,
        )
        persisted_opportunities = save_model_opportunities_postgres(
            connection,
            opportunities,
        )
        serialized_opportunities = [
            _serialize_model_opportunity(entry) for entry in persisted_opportunities
        ]
        materialized_games.append(
            {
                "input": _serialize_future_game_input(game),
                **materialized_preview,
                "opportunity_count": len(serialized_opportunities),
                "opportunities": serialized_opportunities,
            }
        )
        if scoring_run is not None:
            materialized_scoring_runs.append(scoring_run)
        materialized_opportunities.extend(serialized_opportunities)
    return {
        **_build_future_slate_response(
            target_task=target_task,
            slate_label=slate_label,
            game_inputs=games,
            games=materialized_games,
        ),
        "materialized_scoring_run_count": len(materialized_scoring_runs),
        "materialized_opportunity_count": len(materialized_opportunities),
        "scoring_runs": materialized_scoring_runs,
        "opportunities": materialized_opportunities,
    }


def materialize_model_market_board_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str,
    games: list[dict[str, Any]],
    slate_label: str | None = None,
) -> dict[str, Any]:
    board = _build_model_market_board(
        target_task=target_task,
        games=games,
        slate_label=slate_label,
    )
    persisted = save_model_market_board_in_memory(repository, board)
    return {
        "board": _serialize_model_market_board(persisted),
    }


def materialize_model_market_board_postgres(
    connection: Any,
    *,
    target_task: str,
    games: list[dict[str, Any]],
    slate_label: str | None = None,
) -> dict[str, Any]:
    board = _build_model_market_board(
        target_task=target_task,
        games=games,
        slate_label=slate_label,
    )
    persisted = save_model_market_board_postgres(connection, board)
    return {
        "board": _serialize_model_market_board(persisted),
    }

def refresh_model_market_board_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str,
    source_name: str,
    season_label: str,
    game_date: date,
    slate_label: str | None = None,
    game_count: int | None = None,
    source_path: str | None = None,
) -> dict[str, Any]:
    resolved_slate_label = slate_label or f"{source_name}:{game_date.isoformat()}"
    source_request_context = _build_market_board_source_request_context(
        source_name=source_name,
        source_path=source_path,
    )
    requested_game_count = game_count or int(
        MARKET_BOARD_SOURCE_CONFIGS[source_name]["default_game_count"]
    )
    try:
        games = build_model_market_board_source_games(
            source_name=source_name,
            season_label=season_label,
            game_date=game_date,
            game_count=game_count,
            source_path=source_path,
        )
    except Exception as exc:
        source_run = save_model_market_board_source_run_in_memory(
            repository,
            _build_model_market_board_source_run(
                source_name=source_name,
                target_task=target_task,
                season_label=season_label,
                game_date=game_date,
                slate_label=resolved_slate_label,
                requested_game_count=requested_game_count,
                generated_games=[],
                source_path=source_path,
                source_request_context=source_request_context,
                status="FAILED",
                error_message=str(exc),
            ),
        )
        return {
            "source_name": source_name,
            "status": "FAILED",
            "error_message": str(exc),
            "validation_summary": None,
            "generated_game_count": 0,
            "generated_games": [],
            "source_run": _serialize_model_market_board_source_run(source_run),
            "change_summary": None,
            "board": None,
        }

    normalization_result = _normalize_market_board_source_games(
        source_name=source_name,
        season_label=season_label,
        game_date=game_date,
        raw_games=games,
    )
    normalized_games = normalization_result["normalized_games"]
    validation_summary = normalization_result["validation_summary"]
    source_payload_fingerprints = _build_market_board_source_payload_fingerprints(
        raw_games=games,
        normalized_games=normalized_games,
    )
    validation_invalid_count = int(validation_summary["invalid_row_count"])
    validation_error_message = None
    validation_status = "SUCCESS"
    if validation_invalid_count > 0 and normalized_games:
        validation_status = "SUCCESS_WITH_WARNINGS"
    elif validation_invalid_count > 0 and not normalized_games:
        validation_status = "FAILED_VALIDATION"
        validation_error_message = "Source provider returned no valid games after normalization."

    source_run = save_model_market_board_source_run_in_memory(
        repository,
        _build_model_market_board_source_run(
            source_name=source_name,
            target_task=target_task,
            season_label=season_label,
            game_date=game_date,
            slate_label=resolved_slate_label,
            requested_game_count=requested_game_count,
            generated_games=normalized_games,
            raw_generated_games=games,
            source_path=source_path,
            source_request_context=source_request_context,
            status=validation_status,
            error_message=validation_error_message,
            validation_summary=validation_summary,
            source_payload_fingerprints=source_payload_fingerprints,
        ),
    )
    if not normalized_games:
        return {
            "source_name": source_name,
            "status": validation_status,
            "error_message": validation_error_message,
            "validation_summary": validation_summary,
            "source_payload_fingerprints": source_payload_fingerprints,
            "generated_game_count": 0,
            "generated_games": [],
            "source_run": _serialize_model_market_board_source_run(source_run),
            "change_summary": None,
            "board": None,
        }

    board_key = _build_future_slate_key(
        target_task=target_task,
        slate_label=resolved_slate_label,
        serialized_inputs=normalized_games,
    )
    existing_board = _find_model_market_board_in_memory(repository, board_key=board_key)
    result = materialize_model_market_board_in_memory(
        repository,
        target_task=target_task,
        games=normalized_games,
        slate_label=resolved_slate_label,
    )
    board = result["board"]
    change_summary = None
    fingerprint_comparison = _build_market_board_source_fingerprint_comparison(
        existing_board=existing_board,
        current_fingerprints=source_payload_fingerprints,
    )
    if board is not None:
        change_summary = _build_market_board_refresh_change_summary(
            existing_board=existing_board,
            generated_games=normalized_games,
        )
        change_summary["source_payload_fingerprints"] = source_payload_fingerprints
        change_summary["source_fingerprint_comparison"] = fingerprint_comparison
        refresh_status = _resolve_market_board_refresh_status(
            existing_board=existing_board,
            generated_games=normalized_games,
        )
        refresh_count = _resolve_market_board_refresh_count(existing_board) + 1
        board["payload"]["source"] = {
            "source_name": source_name,
            "refresh_target_date": game_date.isoformat(),
            "refreshed_at": _utc_today().isoformat(),
            "refresh_count": refresh_count,
            "last_refresh_status": refresh_status,
            "source_run_id": source_run.id,
            "source_path": source_path,
            "source_request_context": source_request_context,
            "change_summary": change_summary,
            "source_payload_fingerprints": source_payload_fingerprints,
            "source_fingerprint_comparison": fingerprint_comparison,
        }
        refreshed = save_model_market_board_in_memory(
            repository,
            ModelMarketBoardRecord(
                id=int(board["id"]),
                board_key=str(board["board_key"]),
                slate_label=board.get("slate_label"),
                target_task=str(board["target_task"]),
                season_label=board.get("season_label"),
                game_count=int(board["game_count"]),
                game_date_start=_coerce_date(board["game_date_start"])
                if board.get("game_date_start")
                else None,
                game_date_end=_coerce_date(board["game_date_end"])
                if board.get("game_date_end")
                else None,
                payload=board["payload"],
                created_at=None,
                updated_at=None,
            ),
        )
        save_model_market_board_refresh_event_in_memory(
            repository,
            ModelMarketBoardRefreshRecord(
                id=0,
                model_market_board_id=refreshed.id,
                board_key=refreshed.board_key,
                target_task=refreshed.target_task,
                source_name=source_name,
                refresh_status=refresh_status,
                game_count=len(normalized_games),
                payload={
                    "games": normalized_games,
                    "refresh_target_date": game_date.isoformat(),
                    "refreshed_at": _utc_today().isoformat(),
                    "source_run_id": source_run.id,
                    "source_path": source_path,
                    "source_request_context": source_request_context,
                    "change_summary": change_summary,
                    "validation_summary": validation_summary,
                    "source_payload_fingerprints": source_payload_fingerprints,
                    "source_fingerprint_comparison": fingerprint_comparison,
                },
            ),
        )
        board = _serialize_model_market_board(refreshed)
    return {
        "source_name": source_name,
        "status": validation_status,
        "error_message": validation_error_message,
        "validation_summary": validation_summary,
        "source_payload_fingerprints": source_payload_fingerprints,
        "source_path": source_path,
        "source_request_context": source_request_context,
        "generated_game_count": len(normalized_games),
        "generated_games": normalized_games,
        "source_run": _serialize_model_market_board_source_run(source_run),
        "change_summary": change_summary,
        "board": board,
    }


def refresh_model_market_board_postgres(
    connection: Any,
    *,
    target_task: str,
    source_name: str,
    season_label: str,
    game_date: date,
    slate_label: str | None = None,
    game_count: int | None = None,
    source_path: str | None = None,
) -> dict[str, Any]:
    resolved_slate_label = slate_label or f"{source_name}:{game_date.isoformat()}"
    source_request_context = _build_market_board_source_request_context(
        source_name=source_name,
        source_path=source_path,
    )
    requested_game_count = game_count or int(
        MARKET_BOARD_SOURCE_CONFIGS[source_name]["default_game_count"]
    )
    try:
        games = build_model_market_board_source_games(
            source_name=source_name,
            season_label=season_label,
            game_date=game_date,
            game_count=game_count,
            source_path=source_path,
        )
    except Exception as exc:
        source_run = save_model_market_board_source_run_postgres(
            connection,
            _build_model_market_board_source_run(
                source_name=source_name,
                target_task=target_task,
                season_label=season_label,
                game_date=game_date,
                slate_label=resolved_slate_label,
                requested_game_count=requested_game_count,
                generated_games=[],
                source_path=source_path,
                source_request_context=source_request_context,
                status="FAILED",
                error_message=str(exc),
            ),
        )
        return {
            "source_name": source_name,
            "status": "FAILED",
            "error_message": str(exc),
            "validation_summary": None,
            "generated_game_count": 0,
            "generated_games": [],
            "source_run": _serialize_model_market_board_source_run(source_run),
            "change_summary": None,
            "board": None,
        }

    normalization_result = _normalize_market_board_source_games(
        source_name=source_name,
        season_label=season_label,
        game_date=game_date,
        raw_games=games,
    )
    normalized_games = normalization_result["normalized_games"]
    validation_summary = normalization_result["validation_summary"]
    source_payload_fingerprints = _build_market_board_source_payload_fingerprints(
        raw_games=games,
        normalized_games=normalized_games,
    )
    validation_invalid_count = int(validation_summary["invalid_row_count"])
    validation_error_message = None
    validation_status = "SUCCESS"
    if validation_invalid_count > 0 and normalized_games:
        validation_status = "SUCCESS_WITH_WARNINGS"
    elif validation_invalid_count > 0 and not normalized_games:
        validation_status = "FAILED_VALIDATION"
        validation_error_message = "Source provider returned no valid games after normalization."

    source_run = save_model_market_board_source_run_postgres(
        connection,
        _build_model_market_board_source_run(
            source_name=source_name,
            target_task=target_task,
            season_label=season_label,
            game_date=game_date,
            slate_label=resolved_slate_label,
            requested_game_count=requested_game_count,
            generated_games=normalized_games,
            raw_generated_games=games,
            source_path=source_path,
            source_request_context=source_request_context,
            status=validation_status,
            error_message=validation_error_message,
            validation_summary=validation_summary,
            source_payload_fingerprints=source_payload_fingerprints,
        ),
    )
    if not normalized_games:
        return {
            "source_name": source_name,
            "status": validation_status,
            "error_message": validation_error_message,
            "validation_summary": validation_summary,
            "source_payload_fingerprints": source_payload_fingerprints,
            "generated_game_count": 0,
            "generated_games": [],
            "source_run": _serialize_model_market_board_source_run(source_run),
            "change_summary": None,
            "board": None,
        }

    board_key = _build_future_slate_key(
        target_task=target_task,
        slate_label=resolved_slate_label,
        serialized_inputs=normalized_games,
    )
    existing_board = _find_model_market_board_postgres(connection, board_key=board_key)
    result = materialize_model_market_board_postgres(
        connection,
        target_task=target_task,
        games=normalized_games,
        slate_label=resolved_slate_label,
    )
    board = result["board"]
    change_summary = None
    fingerprint_comparison = _build_market_board_source_fingerprint_comparison(
        existing_board=existing_board,
        current_fingerprints=source_payload_fingerprints,
    )
    if board is not None:
        change_summary = _build_market_board_refresh_change_summary(
            existing_board=existing_board,
            generated_games=normalized_games,
        )
        change_summary["source_payload_fingerprints"] = source_payload_fingerprints
        change_summary["source_fingerprint_comparison"] = fingerprint_comparison
        refresh_status = _resolve_market_board_refresh_status(
            existing_board=existing_board,
            generated_games=normalized_games,
        )
        refresh_count = _resolve_market_board_refresh_count(existing_board) + 1
        board["payload"]["source"] = {
            "source_name": source_name,
            "refresh_target_date": game_date.isoformat(),
            "refreshed_at": _utc_today().isoformat(),
            "refresh_count": refresh_count,
            "last_refresh_status": refresh_status,
            "source_run_id": source_run.id,
            "source_path": source_path,
            "source_request_context": source_request_context,
            "change_summary": change_summary,
            "source_payload_fingerprints": source_payload_fingerprints,
            "source_fingerprint_comparison": fingerprint_comparison,
        }
        refreshed = save_model_market_board_postgres(
            connection,
            ModelMarketBoardRecord(
                id=int(board["id"]),
                board_key=str(board["board_key"]),
                slate_label=board.get("slate_label"),
                target_task=str(board["target_task"]),
                season_label=board.get("season_label"),
                game_count=int(board["game_count"]),
                game_date_start=_coerce_date(board["game_date_start"])
                if board.get("game_date_start")
                else None,
                game_date_end=_coerce_date(board["game_date_end"])
                if board.get("game_date_end")
                else None,
                payload=board["payload"],
                created_at=None,
                updated_at=None,
            ),
        )
        save_model_market_board_refresh_event_postgres(
            connection,
            ModelMarketBoardRefreshRecord(
                id=0,
                model_market_board_id=refreshed.id,
                board_key=refreshed.board_key,
                target_task=refreshed.target_task,
                source_name=source_name,
                refresh_status=refresh_status,
                game_count=len(normalized_games),
                payload={
                    "games": normalized_games,
                    "refresh_target_date": game_date.isoformat(),
                    "refreshed_at": _utc_today().isoformat(),
                    "source_run_id": source_run.id,
                    "source_path": source_path,
                    "source_request_context": source_request_context,
                    "change_summary": change_summary,
                    "validation_summary": validation_summary,
                    "source_payload_fingerprints": source_payload_fingerprints,
                    "source_fingerprint_comparison": fingerprint_comparison,
                },
            ),
        )
        board = _serialize_model_market_board(refreshed)
    return {
        "source_name": source_name,
        "status": validation_status,
        "error_message": validation_error_message,
        "validation_summary": validation_summary,
        "source_payload_fingerprints": source_payload_fingerprints,
        "source_path": source_path,
        "source_request_context": source_request_context,
        "generated_game_count": len(normalized_games),
        "generated_games": normalized_games,
        "source_run": _serialize_model_market_board_source_run(source_run),
        "change_summary": change_summary,
        "board": board,
    }


def get_model_market_board_detail_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    board_id: int,
) -> dict[str, Any] | None:
    board = next(
        (
            entry
            for entry in list_model_market_boards_in_memory(repository)
            if entry.id == board_id
        ),
        None,
    )
    return _serialize_model_market_board(board)


def get_model_market_board_detail_postgres(
    connection: Any,
    *,
    board_id: int,
) -> dict[str, Any] | None:
    board = next(
        (
            entry
            for entry in list_model_market_boards_postgres(connection)
            if entry.id == board_id
        ),
        None,
    )
    return _serialize_model_market_board(board)


def get_model_market_board_refresh_history_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    events = list_model_market_board_refresh_events_in_memory(
        repository,
        target_task=target_task,
        source_name=source_name,
    )
    return _summarize_market_board_refresh_history(events, recent_limit=recent_limit)


def get_model_market_board_refresh_history_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    events = list_model_market_board_refresh_events_postgres(
        connection,
        target_task=target_task,
        source_name=source_name,
    )
    return _summarize_market_board_refresh_history(events, recent_limit=recent_limit)


def get_model_market_board_source_run_history_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    season_label: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    runs = list_model_market_board_source_runs_in_memory(
        repository,
        target_task=target_task,
        source_name=source_name,
        season_label=season_label,
    )
    return _summarize_model_market_board_source_run_history(runs, recent_limit=recent_limit)


def get_model_market_board_source_run_history_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    season_label: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    runs = list_model_market_board_source_runs_postgres(
        connection,
        target_task=target_task,
        source_name=source_name,
        season_label=season_label,
    )
    return _summarize_model_market_board_source_run_history(runs, recent_limit=recent_limit)


def get_model_market_board_refresh_queue_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str | None = None,
    season_label: str | None = None,
    source_name: str | None = None,
    freshness_status: str | None = None,
    pending_only: bool = False,
    recent_limit: int = 10,
) -> dict[str, Any]:
    boards = list_model_market_boards_in_memory(
        repository,
        target_task=target_task,
        season_label=season_label,
    )
    refresh_events = list_model_market_board_refresh_events_in_memory(
        repository,
        target_task=target_task,
        source_name=source_name,
    )
    return _build_market_board_refresh_queue(
        boards,
        refresh_events,
        source_name=source_name,
        freshness_status=freshness_status,
        pending_only=pending_only,
        recent_limit=recent_limit,
    )


def get_model_market_board_refresh_queue_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    season_label: str | None = None,
    source_name: str | None = None,
    freshness_status: str | None = None,
    pending_only: bool = False,
    recent_limit: int = 10,
) -> dict[str, Any]:
    boards = list_model_market_boards_postgres(
        connection,
        target_task=target_task,
        season_label=season_label,
    )
    refresh_events = list_model_market_board_refresh_events_postgres(
        connection,
        target_task=target_task,
        source_name=source_name,
    )
    return _build_market_board_refresh_queue(
        boards,
        refresh_events,
        source_name=source_name,
        freshness_status=freshness_status,
        pending_only=pending_only,
        recent_limit=recent_limit,
    )


def get_model_market_board_scoring_queue_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str | None = None,
    season_label: str | None = None,
    source_name: str | None = None,
    freshness_status: str | None = None,
    pending_only: bool = False,
    recent_limit: int = 10,
) -> dict[str, Any]:
    boards = list_model_market_boards_in_memory(
        repository,
        target_task=target_task,
        season_label=season_label,
    )
    scoring_runs = list_model_scoring_runs_in_memory(
        repository,
        target_task=target_task,
        season_label=season_label,
    )
    return _build_market_board_scoring_queue(
        boards,
        scoring_runs,
        source_name=source_name,
        freshness_status=freshness_status,
        pending_only=pending_only,
        recent_limit=recent_limit,
    )


def get_model_market_board_scoring_queue_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    season_label: str | None = None,
    source_name: str | None = None,
    freshness_status: str | None = None,
    pending_only: bool = False,
    recent_limit: int = 10,
) -> dict[str, Any]:
    boards = list_model_market_boards_postgres(
        connection,
        target_task=target_task,
        season_label=season_label,
    )
    scoring_runs = list_model_scoring_runs_postgres(
        connection,
        target_task=target_task,
        season_label=season_label,
    )
    return _build_market_board_scoring_queue(
        boards,
        scoring_runs,
        source_name=source_name,
        freshness_status=freshness_status,
        pending_only=pending_only,
        recent_limit=recent_limit,
    )


def orchestrate_model_market_board_refresh_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str | None = None,
    season_label: str | None = None,
    source_name: str | None = None,
    freshness_status: str | None = None,
    pending_only: bool = True,
    recent_limit: int = 10,
) -> dict[str, Any]:
    queue_before = get_model_market_board_refresh_queue_in_memory(
        repository,
        target_task=target_task,
        season_label=season_label,
        source_name=source_name,
        freshness_status=freshness_status,
        pending_only=pending_only,
        recent_limit=recent_limit,
    )
    queue_entries = [
        entry for entry in queue_before["queue_entries"] if bool(entry.get("refreshable"))
    ]
    refresh_runs = []
    for entry in queue_entries:
        board_payload = entry.get("board")
        if not isinstance(board_payload, dict):
            continue
        source_payload = (
            board_payload.get("payload", {}).get("source", {})
            if isinstance(board_payload.get("payload"), dict)
            else {}
        )
        result = refresh_model_market_board_in_memory(
            repository,
            target_task=str(board_payload["target_task"]),
            source_name=str(entry["source_name"]),
            season_label=str(board_payload.get("season_label") or ""),
            game_date=_resolve_market_board_refresh_game_date(board_payload),
            slate_label=board_payload.get("slate_label"),
            game_count=int(board_payload.get("game_count", 0)) or None,
            source_path=(
                str(source_payload.get("source_path"))
                if isinstance(source_payload, dict) and source_payload.get("source_path")
                else None
            ),
        )
        refresh_runs.append(
            {
                "board": result.get("board") or board_payload,
                "queue_status_before": entry.get("queue_status"),
                "refresh_status_before": entry.get("refresh_status"),
                "refresh_result_status": (
                    result.get("board", {})
                    .get("payload", {})
                    .get("source", {})
                    .get("last_refresh_status")
                ),
                "change_summary": result.get("change_summary"),
            }
        )
    queue_after = get_model_market_board_refresh_queue_in_memory(
        repository,
        target_task=target_task,
        season_label=season_label,
        source_name=source_name,
        freshness_status=freshness_status,
        pending_only=pending_only,
        recent_limit=recent_limit,
    )
    result = _build_market_board_refresh_orchestration_result(
        queue_before=queue_before,
        queue_after=queue_after,
        refresh_runs=refresh_runs,
    )
    batch = save_model_market_board_refresh_batch_in_memory(
        repository,
        _build_model_market_board_refresh_batch(
            result=result,
            target_task=target_task,
            source_name=source_name,
            season_label=season_label,
            freshness_status=freshness_status,
            pending_only=pending_only,
        ),
    )
    return {
        **result,
        "refresh_batch": _serialize_model_market_board_refresh_batch(batch),
    }


def orchestrate_model_market_board_refresh_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    season_label: str | None = None,
    source_name: str | None = None,
    freshness_status: str | None = None,
    pending_only: bool = True,
    recent_limit: int = 10,
) -> dict[str, Any]:
    queue_before = get_model_market_board_refresh_queue_postgres(
        connection,
        target_task=target_task,
        season_label=season_label,
        source_name=source_name,
        freshness_status=freshness_status,
        pending_only=pending_only,
        recent_limit=recent_limit,
    )
    queue_entries = [
        entry for entry in queue_before["queue_entries"] if bool(entry.get("refreshable"))
    ]
    refresh_runs = []
    for entry in queue_entries:
        board_payload = entry.get("board")
        if not isinstance(board_payload, dict):
            continue
        source_payload = (
            board_payload.get("payload", {}).get("source", {})
            if isinstance(board_payload.get("payload"), dict)
            else {}
        )
        result = refresh_model_market_board_postgres(
            connection,
            target_task=str(board_payload["target_task"]),
            source_name=str(entry["source_name"]),
            season_label=str(board_payload.get("season_label") or ""),
            game_date=_resolve_market_board_refresh_game_date(board_payload),
            slate_label=board_payload.get("slate_label"),
            game_count=int(board_payload.get("game_count", 0)) or None,
            source_path=(
                str(source_payload.get("source_path"))
                if isinstance(source_payload, dict) and source_payload.get("source_path")
                else None
            ),
        )
        refresh_runs.append(
            {
                "board": result.get("board") or board_payload,
                "queue_status_before": entry.get("queue_status"),
                "refresh_status_before": entry.get("refresh_status"),
                "refresh_result_status": (
                    result.get("board", {})
                    .get("payload", {})
                    .get("source", {})
                    .get("last_refresh_status")
                ),
                "change_summary": result.get("change_summary"),
            }
        )
    queue_after = get_model_market_board_refresh_queue_postgres(
        connection,
        target_task=target_task,
        season_label=season_label,
        source_name=source_name,
        freshness_status=freshness_status,
        pending_only=pending_only,
        recent_limit=recent_limit,
    )
    result = _build_market_board_refresh_orchestration_result(
        queue_before=queue_before,
        queue_after=queue_after,
        refresh_runs=refresh_runs,
    )
    batch = save_model_market_board_refresh_batch_postgres(
        connection,
        _build_model_market_board_refresh_batch(
            result=result,
            target_task=target_task,
            source_name=source_name,
            season_label=season_label,
            freshness_status=freshness_status,
            pending_only=pending_only,
        ),
    )
    return {
        **result,
        "refresh_batch": _serialize_model_market_board_refresh_batch(batch),
    }


def orchestrate_model_market_board_scoring_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str | None = None,
    season_label: str | None = None,
    source_name: str | None = None,
    freshness_status: str | None = "fresh",
    pending_only: bool = True,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
    recent_limit: int = 10,
) -> dict[str, Any]:
    queue_before = get_model_market_board_scoring_queue_in_memory(
        repository,
        target_task=target_task,
        season_label=season_label,
        source_name=source_name,
        freshness_status=freshness_status,
        pending_only=pending_only,
        recent_limit=recent_limit,
    )
    queue_entries = list(queue_before["queue_entries"])
    orchestration_runs = []
    for entry in queue_entries:
        board_payload = entry.get("board")
        if not isinstance(board_payload, dict):
            continue
        board_id = int(board_payload["id"])
        result = score_model_market_board_in_memory(
            repository,
            board_id=board_id,
            feature_key=feature_key,
            include_evidence=include_evidence,
            evidence_dimensions=evidence_dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
        )
        slate_result = result.get("slate_result") or {}
        orchestration_runs.append(
            {
                "board": result.get("board") or board_payload,
                "queue_status_before": entry.get("queue_status"),
                "scoring_status_before": entry.get("scoring_status"),
                "materialized_scoring_run_count": int(
                    slate_result.get("materialized_scoring_run_count", 0)
                ),
                "materialized_opportunity_count": int(
                    slate_result.get("materialized_opportunity_count", 0)
                ),
                "slate_result": slate_result,
            }
        )
    queue_after = get_model_market_board_scoring_queue_in_memory(
        repository,
        target_task=target_task,
        season_label=season_label,
        source_name=source_name,
        freshness_status=freshness_status,
        pending_only=pending_only,
        recent_limit=recent_limit,
    )
    result = {
        "queue_before": queue_before,
        "queue_after": queue_after,
        "candidate_board_count": len(queue_entries),
        "scored_board_count": len(orchestration_runs),
        "materialized_scoring_run_count": sum(
            int(entry["materialized_scoring_run_count"]) for entry in orchestration_runs
        ),
        "materialized_opportunity_count": sum(
            int(entry["materialized_opportunity_count"]) for entry in orchestration_runs
        ),
        "orchestration_runs": orchestration_runs,
    }
    batch = save_model_market_board_scoring_batch_in_memory(
        repository,
        _build_model_market_board_scoring_batch(
            result=result,
            target_task=target_task,
            source_name=source_name,
            season_label=season_label,
            freshness_status=freshness_status,
            pending_only=pending_only,
        ),
    )
    return {
        **result,
        "orchestration_batch": _serialize_model_market_board_scoring_batch(batch),
    }


def orchestrate_model_market_board_scoring_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str | None = None,
    season_label: str | None = None,
    source_name: str | None = None,
    freshness_status: str | None = "fresh",
    pending_only: bool = True,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
    recent_limit: int = 10,
) -> dict[str, Any]:
    queue_before = get_model_market_board_scoring_queue_postgres(
        connection,
        target_task=target_task,
        season_label=season_label,
        source_name=source_name,
        freshness_status=freshness_status,
        pending_only=pending_only,
        recent_limit=recent_limit,
    )
    queue_entries = list(queue_before["queue_entries"])
    orchestration_runs = []
    for entry in queue_entries:
        board_payload = entry.get("board")
        if not isinstance(board_payload, dict):
            continue
        board_id = int(board_payload["id"])
        result = score_model_market_board_postgres(
            connection,
            board_id=board_id,
            feature_key=feature_key,
            include_evidence=include_evidence,
            evidence_dimensions=evidence_dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
        )
        slate_result = result.get("slate_result") or {}
        orchestration_runs.append(
            {
                "board": result.get("board") or board_payload,
                "queue_status_before": entry.get("queue_status"),
                "scoring_status_before": entry.get("scoring_status"),
                "materialized_scoring_run_count": int(
                    slate_result.get("materialized_scoring_run_count", 0)
                ),
                "materialized_opportunity_count": int(
                    slate_result.get("materialized_opportunity_count", 0)
                ),
                "slate_result": slate_result,
            }
        )
    queue_after = get_model_market_board_scoring_queue_postgres(
        connection,
        target_task=target_task,
        season_label=season_label,
        source_name=source_name,
        freshness_status=freshness_status,
        pending_only=pending_only,
        recent_limit=recent_limit,
    )
    result = {
        "queue_before": queue_before,
        "queue_after": queue_after,
        "candidate_board_count": len(queue_entries),
        "scored_board_count": len(orchestration_runs),
        "materialized_scoring_run_count": sum(
            int(entry["materialized_scoring_run_count"]) for entry in orchestration_runs
        ),
        "materialized_opportunity_count": sum(
            int(entry["materialized_opportunity_count"]) for entry in orchestration_runs
        ),
        "orchestration_runs": orchestration_runs,
    }
    batch = save_model_market_board_scoring_batch_postgres(
        connection,
        _build_model_market_board_scoring_batch(
            result=result,
            target_task=target_task,
            source_name=source_name,
            season_label=season_label,
            freshness_status=freshness_status,
            pending_only=pending_only,
        ),
    )
    return {
        **result,
        "orchestration_batch": _serialize_model_market_board_scoring_batch(batch),
    }


def orchestrate_model_market_board_cadence_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str | None = None,
    season_label: str | None = None,
    source_name: str | None = None,
    refresh_freshness_status: str | None = None,
    refresh_pending_only: bool = False,
    scoring_freshness_status: str | None = "fresh",
    scoring_pending_only: bool = True,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
    recent_limit: int = 10,
) -> dict[str, Any]:
    refresh_result = orchestrate_model_market_board_refresh_in_memory(
        repository,
        target_task=target_task,
        season_label=season_label,
        source_name=source_name,
        freshness_status=refresh_freshness_status,
        pending_only=refresh_pending_only,
        recent_limit=recent_limit,
    )
    scoring_result = orchestrate_model_market_board_scoring_in_memory(
        repository,
        feature_key=feature_key,
        target_task=target_task,
        season_label=season_label,
        source_name=source_name,
        freshness_status=scoring_freshness_status,
        pending_only=scoring_pending_only,
        include_evidence=include_evidence,
        evidence_dimensions=evidence_dimensions,
        comparable_limit=comparable_limit,
        min_pattern_sample_size=min_pattern_sample_size,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
        recent_limit=recent_limit,
    )
    result = _build_market_board_cadence_result(
        refresh_result=refresh_result,
        scoring_result=scoring_result,
    )
    batch = save_model_market_board_cadence_batch_in_memory(
        repository,
        _build_model_market_board_cadence_batch(
            result=result,
            target_task=target_task,
            source_name=source_name,
            season_label=season_label,
            refresh_freshness_status=refresh_freshness_status,
            scoring_freshness_status=scoring_freshness_status,
        ),
    )
    return {
        **result,
        "cadence_batch": _serialize_model_market_board_cadence_batch(batch),
    }


def orchestrate_model_market_board_cadence_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str | None = None,
    season_label: str | None = None,
    source_name: str | None = None,
    refresh_freshness_status: str | None = None,
    refresh_pending_only: bool = False,
    scoring_freshness_status: str | None = "fresh",
    scoring_pending_only: bool = True,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
    recent_limit: int = 10,
) -> dict[str, Any]:
    refresh_result = orchestrate_model_market_board_refresh_postgres(
        connection,
        target_task=target_task,
        season_label=season_label,
        source_name=source_name,
        freshness_status=refresh_freshness_status,
        pending_only=refresh_pending_only,
        recent_limit=recent_limit,
    )
    scoring_result = orchestrate_model_market_board_scoring_postgres(
        connection,
        feature_key=feature_key,
        target_task=target_task,
        season_label=season_label,
        source_name=source_name,
        freshness_status=scoring_freshness_status,
        pending_only=scoring_pending_only,
        include_evidence=include_evidence,
        evidence_dimensions=evidence_dimensions,
        comparable_limit=comparable_limit,
        min_pattern_sample_size=min_pattern_sample_size,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
        recent_limit=recent_limit,
    )
    result = _build_market_board_cadence_result(
        refresh_result=refresh_result,
        scoring_result=scoring_result,
    )
    batch = save_model_market_board_cadence_batch_postgres(
        connection,
        _build_model_market_board_cadence_batch(
            result=result,
            target_task=target_task,
            source_name=source_name,
            season_label=season_label,
            refresh_freshness_status=refresh_freshness_status,
            scoring_freshness_status=scoring_freshness_status,
        ),
    )
    return {
        **result,
        "cadence_batch": _serialize_model_market_board_cadence_batch(batch),
    }


def get_model_market_board_refresh_batch_history_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    batches = list_model_market_board_refresh_batches_in_memory(
        repository,
        target_task=target_task,
        source_name=source_name,
    )
    return _summarize_model_market_board_refresh_batch_history(
        batches,
        recent_limit=recent_limit,
    )


def get_model_market_board_refresh_batch_history_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    batches = list_model_market_board_refresh_batches_postgres(
        connection,
        target_task=target_task,
        source_name=source_name,
    )
    return _summarize_model_market_board_refresh_batch_history(
        batches,
        recent_limit=recent_limit,
    )


def get_model_market_board_scoring_batch_history_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    batches = list_model_market_board_scoring_batches_in_memory(
        repository,
        target_task=target_task,
        source_name=source_name,
    )
    return _summarize_model_market_board_scoring_batch_history(
        batches,
        recent_limit=recent_limit,
    )


def get_model_market_board_scoring_batch_history_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    batches = list_model_market_board_scoring_batches_postgres(
        connection,
        target_task=target_task,
        source_name=source_name,
    )
    return _summarize_model_market_board_scoring_batch_history(
        batches,
        recent_limit=recent_limit,
    )


def get_model_market_board_cadence_batch_history_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    batches = list_model_market_board_cadence_batches_in_memory(
        repository,
        target_task=target_task,
        source_name=source_name,
    )
    return _summarize_model_market_board_cadence_batch_history(
        batches,
        recent_limit=recent_limit,
    )


def get_model_market_board_cadence_batch_history_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    batches = list_model_market_board_cadence_batches_postgres(
        connection,
        target_task=target_task,
        source_name=source_name,
    )
    return _summarize_model_market_board_cadence_batch_history(
        batches,
        recent_limit=recent_limit,
    )


def get_model_market_board_operations_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    board_id: int,
    recent_limit: int = 5,
) -> dict[str, Any] | None:
    board = next(
        (
            entry
            for entry in list_model_market_boards_in_memory(repository)
            if entry.id == board_id
        ),
        None,
    )
    if board is None:
        return None
    source_runs = list_model_market_board_source_runs_in_memory(
        repository,
        target_task=board.target_task,
        season_label=board.season_label,
    )
    refresh_events = list_model_market_board_refresh_events_in_memory(
        repository,
        target_task=board.target_task,
        source_name=None,
    )
    scoring_runs = list_model_scoring_runs_in_memory(
        repository,
        model_market_board_id=board.id,
    )
    opportunities = [
        opportunity
        for opportunity in list_model_opportunities_in_memory(
            repository,
            target_task=board.target_task,
            season_label=board.season_label,
        )
        if opportunity.model_scoring_run_id is not None
        and any(
            scoring_run.id == opportunity.model_scoring_run_id for scoring_run in scoring_runs
        )
    ]
    refresh_batches = list_model_market_board_refresh_batches_in_memory(repository)
    cadence_batches = list_model_market_board_cadence_batches_in_memory(repository)
    batches = list_model_market_board_scoring_batches_in_memory(repository)
    return _build_market_board_operations_summary(
        board,
        source_runs=source_runs,
        refresh_events=refresh_events,
        refresh_batches=refresh_batches,
        cadence_batches=cadence_batches,
        scoring_runs=scoring_runs,
        opportunities=opportunities,
        batches=batches,
        recent_limit=recent_limit,
    )


def get_model_market_board_operations_postgres(
    connection: Any,
    *,
    board_id: int,
    recent_limit: int = 5,
) -> dict[str, Any] | None:
    board = next(
        (
            entry
            for entry in list_model_market_boards_postgres(connection)
            if entry.id == board_id
        ),
        None,
    )
    if board is None:
        return None
    source_runs = list_model_market_board_source_runs_postgres(
        connection,
        target_task=board.target_task,
        season_label=board.season_label,
    )
    refresh_events = list_model_market_board_refresh_events_postgres(
        connection,
        target_task=board.target_task,
        source_name=None,
    )
    scoring_runs = list_model_scoring_runs_postgres(
        connection,
        model_market_board_id=board.id,
    )
    opportunities = [
        opportunity
        for opportunity in list_model_opportunities_postgres(
            connection,
            target_task=board.target_task,
            season_label=board.season_label,
        )
        if opportunity.model_scoring_run_id is not None
        and any(
            scoring_run.id == opportunity.model_scoring_run_id for scoring_run in scoring_runs
        )
    ]
    refresh_batches = list_model_market_board_refresh_batches_postgres(connection)
    cadence_batches = list_model_market_board_cadence_batches_postgres(connection)
    batches = list_model_market_board_scoring_batches_postgres(connection)
    return _build_market_board_operations_summary(
        board,
        source_runs=source_runs,
        refresh_events=refresh_events,
        refresh_batches=refresh_batches,
        cadence_batches=cadence_batches,
        scoring_runs=scoring_runs,
        opportunities=opportunities,
        batches=batches,
        recent_limit=recent_limit,
    )


def get_model_market_board_cadence_dashboard_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str | None = None,
    season_label: str | None = None,
    source_name: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    boards = list_model_market_boards_in_memory(
        repository,
        target_task=target_task,
        season_label=season_label,
    )
    scoring_runs = list_model_scoring_runs_in_memory(
        repository,
        target_task=target_task,
        season_label=season_label,
    )
    batches = list_model_market_board_scoring_batches_in_memory(
        repository,
        target_task=target_task,
        source_name=source_name,
    )
    return _build_market_board_cadence_dashboard(
        boards,
        scoring_runs=scoring_runs,
        batches=batches,
        source_name=source_name,
        recent_limit=recent_limit,
    )


def get_model_market_board_cadence_dashboard_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    season_label: str | None = None,
    source_name: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    boards = list_model_market_boards_postgres(
        connection,
        target_task=target_task,
        season_label=season_label,
    )
    scoring_runs = list_model_scoring_runs_postgres(
        connection,
        target_task=target_task,
        season_label=season_label,
    )
    batches = list_model_market_board_scoring_batches_postgres(
        connection,
        target_task=target_task,
        source_name=source_name,
    )
    return _build_market_board_cadence_dashboard(
        boards,
        scoring_runs=scoring_runs,
        batches=batches,
        source_name=source_name,
        recent_limit=recent_limit,
    )


def score_model_market_board_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    board_id: int,
    feature_key: str = DEFAULT_FEATURE_KEY,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    board = next(
        (
            entry
            for entry in list_model_market_boards_in_memory(repository)
            if entry.id == board_id
        ),
        None,
    )
    if board is None:
        return {"board": None, "slate_result": None}
    slate_result = materialize_model_future_slate_in_memory(
        repository,
        model_market_board_id=board.id,
        feature_key=feature_key,
        target_task=board.target_task,
        games=list(board.payload.get("games", [])),
        slate_label=board.slate_label,
        include_evidence=include_evidence,
        evidence_dimensions=evidence_dimensions,
        comparable_limit=comparable_limit,
        min_pattern_sample_size=min_pattern_sample_size,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
    )
    return {
        "board": _serialize_model_market_board(board),
        "slate_result": slate_result,
    }


def score_model_market_board_postgres(
    connection: Any,
    *,
    board_id: int,
    feature_key: str = DEFAULT_FEATURE_KEY,
    include_evidence: bool = True,
    evidence_dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    comparable_limit: int = 5,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    board = next(
        (
            entry
            for entry in list_model_market_boards_postgres(connection)
            if entry.id == board_id
        ),
        None,
    )
    if board is None:
        return {"board": None, "slate_result": None}
    slate_result = materialize_model_future_slate_postgres(
        connection,
        model_market_board_id=board.id,
        feature_key=feature_key,
        target_task=board.target_task,
        games=list(board.payload.get("games", [])),
        slate_label=board.slate_label,
        include_evidence=include_evidence,
        evidence_dimensions=evidence_dimensions,
        comparable_limit=comparable_limit,
        min_pattern_sample_size=min_pattern_sample_size,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
    )
    return {
        "board": _serialize_model_market_board(board),
        "slate_result": slate_result,
    }


def list_model_opportunities_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    status: str | None = None,
    season_label: str | None = None,
    source_kind: str | None = None,
    scenario_key: str | None = None,
) -> list[ModelOpportunityRecord]:
    selected = [
        ModelOpportunityRecord(**entry)
        for entry in repository.model_opportunities
        if (target_task is None or entry["target_task"] == target_task)
        and (
            team_code is None
            or entry["team_code"] == team_code
            or entry["opponent_code"] == team_code
        )
        and (status is None or entry["status"] == status)
        and (season_label is None or entry["season_label"] == season_label)
        and (source_kind is None or entry["source_kind"] == source_kind)
        and (scenario_key is None or entry.get("scenario_key") == scenario_key)
    ]
    return sorted(
        selected,
        key=lambda entry: (
            entry.created_at or datetime.min.replace(tzinfo=timezone.utc),
            entry.id,
        ),
        reverse=True,
    )


def list_model_opportunities_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    status: str | None = None,
    season_label: str | None = None,
    source_kind: str | None = None,
    scenario_key: str | None = None,
) -> list[ModelOpportunityRecord]:
    query = """
        SELECT
            id,
            model_scoring_run_id,
            model_selection_snapshot_id,
            model_evaluation_snapshot_id,
            feature_version_id,
            target_task,
            source_kind,
            scenario_key,
            opportunity_key,
            team_code,
            opponent_code,
            season_label,
            canonical_game_id,
            game_date,
            policy_name,
            status,
            prediction_value,
            signal_strength,
            evidence_rating,
            recommendation_status,
            payload_json,
            created_at,
            updated_at
        FROM model_opportunity
        WHERE 1=1
    """
    params: list[Any] = []
    if target_task is not None:
        query += " AND target_task = %s"
        params.append(target_task)
    if team_code is not None:
        query += " AND (team_code = %s OR opponent_code = %s)"
        params.extend([team_code, team_code])
    if status is not None:
        query += " AND status = %s"
        params.append(status)
    if season_label is not None:
        query += " AND season_label = %s"
        params.append(season_label)
    if source_kind is not None:
        query += " AND source_kind = %s"
        params.append(source_kind)
    if scenario_key is not None:
        query += " AND scenario_key = %s"
        params.append(scenario_key)
    query += " ORDER BY created_at DESC, id DESC"
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelOpportunityRecord(
            id=int(row[0]),
            model_scoring_run_id=int(row[1]) if row[1] is not None else None,
            model_selection_snapshot_id=int(row[2]) if row[2] is not None else None,
            model_evaluation_snapshot_id=int(row[3]) if row[3] is not None else None,
            feature_version_id=int(row[4]),
            target_task=row[5],
            source_kind=row[6],
            scenario_key=row[7],
            opportunity_key=row[8],
            team_code=row[9],
            opponent_code=row[10],
            season_label=row[11],
            canonical_game_id=int(row[12]) if row[12] is not None else None,
            game_date=row[13],
            policy_name=row[14],
            status=row[15],
            prediction_value=float(row[16]),
            signal_strength=float(row[17]),
            evidence_rating=row[18],
            recommendation_status=row[19],
            payload=row[20],
            created_at=row[21],
            updated_at=row[22],
        )
        for row in rows
    ]


def get_model_opportunity_detail_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    opportunity_id: int,
) -> dict[str, Any] | None:
    opportunity = next(
        (
            entry
            for entry in list_model_opportunities_in_memory(repository)
            if entry.id == opportunity_id
        ),
        None,
    )
    return _serialize_model_opportunity(opportunity)


def get_model_opportunity_detail_postgres(
    connection: Any,
    *,
    opportunity_id: int,
) -> dict[str, Any] | None:
    opportunity = next(
        (
            entry
            for entry in list_model_opportunities_postgres(connection)
            if entry.id == opportunity_id
        ),
        None,
    )
    return _serialize_model_opportunity(opportunity)


def get_model_opportunity_history_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    source_kind: str | None = None,
    scenario_key: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    opportunities = list_model_opportunities_in_memory(
        repository,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        source_kind=source_kind,
        scenario_key=scenario_key,
    )
    return _summarize_model_opportunity_history(opportunities, recent_limit=recent_limit)


def get_model_opportunity_history_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    source_kind: str | None = None,
    scenario_key: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    opportunities = list_model_opportunities_postgres(
        connection,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        source_kind=source_kind,
        scenario_key=scenario_key,
    )
    return _summarize_model_opportunity_history(opportunities, recent_limit=recent_limit)


def run_model_backtest_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    team_code: str | None = None,
    season_label: str | None = None,
    selection_policy_name: str = "validation_mae_candidate_v1",
    minimum_train_games: int = 1,
    test_window_games: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
) -> dict[str, Any]:
    feature_version = get_feature_version_in_memory(repository, feature_key=feature_key)
    if feature_version is None:
        return {
            "feature_version": None,
            "backtest_run": None,
            "summary": _empty_backtest_summary(
                target_task=target_task,
                selection_policy_name=selection_policy_name,
                strategy_name=_backtest_strategy_name(target_task),
                minimum_train_games=minimum_train_games,
                test_window_games=test_window_games,
            ),
        }
    dataset_rows = _load_training_dataset_rows_in_memory(
        repository,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
    )
    result = _run_walk_forward_backtest(
        dataset_rows=dataset_rows,
        feature_version=feature_version,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        selection_policy_name=selection_policy_name,
        minimum_train_games=minimum_train_games,
        test_window_games=test_window_games,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
    )
    backtest_run = save_model_backtest_run_in_memory(repository, result["record"])
    return {
        "feature_version": asdict(feature_version),
        "backtest_run": _serialize_model_backtest_run(backtest_run),
        "summary": result["summary"],
    }


def run_model_backtest_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    team_code: str | None = None,
    season_label: str | None = None,
    selection_policy_name: str = "validation_mae_candidate_v1",
    minimum_train_games: int = 1,
    test_window_games: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
) -> dict[str, Any]:
    feature_version = get_feature_version_postgres(connection, feature_key=feature_key)
    if feature_version is None:
        return {
            "feature_version": None,
            "backtest_run": None,
            "summary": _empty_backtest_summary(
                target_task=target_task,
                selection_policy_name=selection_policy_name,
                strategy_name=_backtest_strategy_name(target_task),
                minimum_train_games=minimum_train_games,
                test_window_games=test_window_games,
            ),
        }
    dataset_rows = _load_training_dataset_rows_postgres(
        connection,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
    )
    result = _run_walk_forward_backtest(
        dataset_rows=dataset_rows,
        feature_version=feature_version,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        selection_policy_name=selection_policy_name,
        minimum_train_games=minimum_train_games,
        test_window_games=test_window_games,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
    )
    backtest_run = save_model_backtest_run_postgres(connection, result["record"])
    return {
        "feature_version": asdict(feature_version),
        "backtest_run": _serialize_model_backtest_run(backtest_run),
        "summary": result["summary"],
    }


def list_model_backtest_runs_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
) -> list[ModelBacktestRunRecord]:
    selected = [
        ModelBacktestRunRecord(**entry)
        for entry in repository.model_backtest_runs
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


def list_model_backtest_runs_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
) -> list[ModelBacktestRunRecord]:
    query = """
        SELECT
            id,
            feature_version_id,
            target_task,
            scope_team_code,
            scope_season_label,
            status,
            selection_policy_name,
            strategy_name,
            minimum_train_games,
            test_window_games,
            train_ratio,
            validation_ratio,
            fold_count,
            payload_json,
            created_at,
            completed_at
        FROM model_backtest_run
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
    query += " ORDER BY completed_at DESC NULLS LAST, created_at DESC, id DESC"
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelBacktestRunRecord(
            id=int(row[0]),
            feature_version_id=int(row[1]),
            target_task=row[2],
            team_code=row[3] or None,
            season_label=row[4] or None,
            status=row[5],
            selection_policy_name=row[6],
            strategy_name=row[7],
            minimum_train_games=int(row[8]),
            test_window_games=int(row[9]),
            train_ratio=float(row[10]),
            validation_ratio=float(row[11]),
            fold_count=int(row[12]),
            payload=row[13],
            created_at=row[14],
            completed_at=row[15],
        )
        for row in rows
    ]


def get_model_backtest_history_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    runs = list_model_backtest_runs_in_memory(
        repository,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
    )
    return _summarize_model_backtest_history(runs, recent_limit=recent_limit)


def get_model_backtest_history_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    runs = list_model_backtest_runs_postgres(
        connection,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
    )
    return _summarize_model_backtest_history(runs, recent_limit=recent_limit)


def get_model_backtest_detail_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    backtest_run_id: int,
) -> dict[str, Any] | None:
    run = next(
        (
            entry
            for entry in list_model_backtest_runs_in_memory(repository)
            if entry.id == backtest_run_id
        ),
        None,
    )
    return _serialize_model_backtest_run(run)


def get_model_backtest_detail_postgres(
    connection: Any,
    *,
    backtest_run_id: int,
) -> dict[str, Any] | None:
    run = next(
        (
            entry
            for entry in list_model_backtest_runs_postgres(connection)
            if entry.id == backtest_run_id
        ),
        None,
    )
    return _serialize_model_backtest_run(run)


def _train_phase_three_models(
    *,
    dataset_rows: list[dict[str, Any]],
    feature_version: FeatureVersionRecord,
    target_task: str,
    team_code: str | None,
    season_label: str | None,
    train_ratio: float,
    validation_ratio: float,
    ensure_registry,
    save_run,
    list_runs,
) -> dict[str, Any]:
    if target_task not in SUPPORTED_MODEL_TARGET_TASKS:
        raise ValueError(
            f"Phase 3 model training currently supports regression targets only: {target_task}"
        )
    split_rows = _partition_feature_dataset_rows(
        dataset_rows,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
    )
    split_training_rows = {
        split_name: build_feature_training_view(
            rows,
            target_task=target_task,
            drop_null_targets=True,
        )["training_rows"]
        for split_name, rows in split_rows.items()
    }
    split_summary = {
        split_name: {
            "row_count": len(rows),
            "game_count": len({row["canonical_game_id"] for row in rows}),
        }
        for split_name, rows in split_training_rows.items()
    }
    split_target_summary = {
        split_name: _summarize_target_values(rows)
        for split_name, rows in split_training_rows.items()
    }
    candidate_runs: list[ModelTrainingRunRecord] = []
    for model_family, trainer in (
        ("linear_feature", _train_linear_feature_model),
        ("tree_stump", _train_tree_stump_model),
    ):
        registry = ensure_registry(model_family)
        model_artifact = trainer(
            train_rows=split_training_rows["train"],
            validation_rows=split_training_rows["validation"],
            test_rows=split_training_rows["test"],
        )
        model_artifact["artifact"]["split_summary"] = split_summary
        model_artifact["artifact"]["split_target_summary"] = split_target_summary
        run = save_run(
            ModelTrainingRunRecord(
                id=0,
                model_registry_id=registry.id,
                feature_version_id=feature_version.id,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                status="COMPLETED",
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                artifact=model_artifact["artifact"],
                metrics=model_artifact["metrics"],
            )
        )
        candidate_runs.append(run)

    ranked_runs = sorted(
        candidate_runs,
        key=lambda run: (
            _metric_value_or_inf(run.metrics.get("validation", {}).get("mae")),
            -int(run.metrics.get("validation", {}).get("prediction_count", 0)),
        ),
    )
    return {
        "feature_version": asdict(feature_version),
        "dataset_row_count": len(dataset_rows),
        "model_runs": [asdict(run) for run in ranked_runs],
        "best_model": asdict(ranked_runs[0]) if ranked_runs else None,
        "persisted_run_count": len(list_runs()),
    }


def _run_walk_forward_backtest(
    *,
    dataset_rows: list[dict[str, Any]],
    feature_version: FeatureVersionRecord,
    target_task: str,
    team_code: str | None,
    season_label: str | None,
    selection_policy_name: str,
    minimum_train_games: int,
    test_window_games: int,
    train_ratio: float,
    validation_ratio: float,
) -> dict[str, Any]:
    if target_task not in {"spread_error_regression", "total_error_regression"}:
        raise ValueError(
            "Phase 4 walk-forward backtesting currently supports spread and total regression "
            f"targets only: {target_task}"
        )
    ordered_game_ids = _ordered_dataset_game_ids(dataset_rows)
    if len(ordered_game_ids) <= minimum_train_games:
        summary = _empty_backtest_summary(
            target_task=target_task,
            selection_policy_name=selection_policy_name,
            strategy_name=_backtest_strategy_name(target_task),
            minimum_train_games=minimum_train_games,
            test_window_games=test_window_games,
        )
        summary["dataset_game_count"] = len(ordered_game_ids)
        summary["dataset_row_count"] = len(dataset_rows)
        return {
            "record": ModelBacktestRunRecord(
                id=0,
                feature_version_id=feature_version.id,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                status="COMPLETED",
                selection_policy_name=selection_policy_name,
                strategy_name=_backtest_strategy_name(target_task),
                minimum_train_games=minimum_train_games,
                test_window_games=test_window_games,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
                fold_count=0,
                payload=summary,
            ),
            "summary": summary,
        }

    rows_by_game: dict[int, list[dict[str, Any]]] = {}
    for row in dataset_rows:
        rows_by_game.setdefault(int(row["canonical_game_id"]), []).append(row)

    fold_summaries: list[dict[str, Any]] = []
    all_predictions: list[dict[str, Any]] = []
    for fold_index, train_end in enumerate(
        range(minimum_train_games, len(ordered_game_ids), test_window_games),
        start=1,
    ):
        train_game_ids = ordered_game_ids[:train_end]
        test_game_ids = ordered_game_ids[train_end : train_end + test_window_games]
        if not test_game_ids:
            continue
        train_dataset_rows = [
            row for game_id in train_game_ids for row in rows_by_game.get(game_id, [])
        ]
        test_dataset_rows = [
            row for game_id in test_game_ids for row in rows_by_game.get(game_id, [])
        ]
        selected_snapshot = _train_walk_forward_snapshot(
            dataset_rows=train_dataset_rows,
            feature_version=feature_version,
            target_task=target_task,
            selection_policy_name=selection_policy_name,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
        if selected_snapshot is None:
            continue
        predictions = _score_dataset_rows_with_active_selection(
            test_dataset_rows,
            target_task=target_task,
            active_snapshot=selected_snapshot,
            full_dataset_rows=train_dataset_rows,
            include_evidence=False,
            evidence_dimensions=("venue", "days_rest_bucket"),
            comparable_limit=5,
            min_pattern_sample_size=1,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=True,
        )
        fold_summary = _build_backtest_fold_summary(
            fold_index=fold_index,
            target_task=target_task,
            train_game_ids=train_game_ids,
            test_game_ids=test_game_ids,
            selected_snapshot=selected_snapshot,
            predictions=predictions,
        )
        fold_summaries.append(fold_summary)
        all_predictions.extend(predictions)

    summary = _summarize_walk_forward_backtest(
        target_task=target_task,
        selection_policy_name=selection_policy_name,
        minimum_train_games=minimum_train_games,
        test_window_games=test_window_games,
        dataset_row_count=len(dataset_rows),
        dataset_game_count=len(ordered_game_ids),
        fold_summaries=fold_summaries,
        predictions=all_predictions,
    )
    record = ModelBacktestRunRecord(
        id=0,
        feature_version_id=feature_version.id,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        status="COMPLETED",
        selection_policy_name=selection_policy_name,
        strategy_name=summary["strategy_name"],
        minimum_train_games=minimum_train_games,
        test_window_games=test_window_games,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        fold_count=len(fold_summaries),
        payload=summary,
    )
    return {"record": record, "summary": summary}


def _ordered_dataset_game_ids(dataset_rows: list[dict[str, Any]]) -> list[int]:
    seen: set[int] = set()
    ordered_game_ids: list[int] = []
    for row in sorted(
        dataset_rows,
        key=lambda entry: (entry["game_date"], int(entry["canonical_game_id"]), entry["team_code"]),
    ):
        canonical_game_id = int(row["canonical_game_id"])
        if canonical_game_id in seen:
            continue
        seen.add(canonical_game_id)
        ordered_game_ids.append(canonical_game_id)
    return ordered_game_ids


def _train_walk_forward_snapshot(
    *,
    dataset_rows: list[dict[str, Any]],
    feature_version: FeatureVersionRecord,
    target_task: str,
    selection_policy_name: str,
    train_ratio: float,
    validation_ratio: float,
) -> ModelEvaluationSnapshotRecord | None:
    split_rows = _partition_feature_dataset_rows(
        dataset_rows,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
    )
    split_training_rows = {
        split_name: build_feature_training_view(
            rows,
            target_task=target_task,
            drop_null_targets=True,
        )["training_rows"]
        for split_name, rows in split_rows.items()
    }
    candidate_snapshots: list[ModelEvaluationSnapshotRecord] = []
    for index, (model_family, trainer) in enumerate(
        (("linear_feature", _train_linear_feature_model), ("tree_stump", _train_tree_stump_model)),
        start=1,
    ):
        model_result = trainer(
            train_rows=split_training_rows["train"],
            validation_rows=split_training_rows["validation"],
            test_rows=split_training_rows["test"],
        )
        candidate_snapshots.append(
            ModelEvaluationSnapshotRecord(
                id=index,
                model_training_run_id=0,
                model_registry_id=0,
                feature_version_id=feature_version.id,
                target_task=target_task,
                model_family=model_family,
                selected_feature=model_result["artifact"].get("selected_feature"),
                fallback_strategy=model_result["artifact"].get("fallback_strategy"),
                primary_metric_name="mae",
                validation_metric_value=model_result["metrics"]["validation"].get("mae"),
                test_metric_value=model_result["metrics"]["test"].get("mae"),
                validation_prediction_count=int(
                    model_result["metrics"]["validation"].get("prediction_count", 0)
                ),
                test_prediction_count=int(
                    model_result["metrics"]["test"].get("prediction_count", 0)
                ),
                snapshot=model_result,
            )
        )
    return _select_best_evaluation_snapshot(
        candidate_snapshots,
        selection_policy_name=selection_policy_name,
    )


def _build_backtest_fold_summary(
    *,
    fold_index: int,
    target_task: str,
    train_game_ids: list[int],
    test_game_ids: list[int],
    selected_snapshot: ModelEvaluationSnapshotRecord,
    predictions: list[dict[str, Any]],
) -> dict[str, Any]:
    candidate_strategy = _evaluate_backtest_strategy(
        predictions=predictions,
        target_task=target_task,
        threshold=float(
            OPPORTUNITY_POLICY_CONFIGS[target_task]["candidate_min_signal_strength"]
        ),
        strategy_name="candidate_threshold",
    )
    review_strategy = _evaluate_backtest_strategy(
        predictions=predictions,
        target_task=target_task,
        threshold=float(OPPORTUNITY_POLICY_CONFIGS[target_task]["review_min_signal_strength"]),
        strategy_name="review_threshold",
    )
    return {
        "fold_index": fold_index,
        "train_game_count": len(train_game_ids),
        "test_game_count": len(test_game_ids),
        "train_game_ids": train_game_ids,
        "test_game_ids": test_game_ids,
        "selected_model": {
            "evaluation_snapshot_id": selected_snapshot.id,
            "model_training_run_id": selected_snapshot.model_training_run_id,
            "model_family": selected_snapshot.model_family,
            "selected_feature": selected_snapshot.selected_feature,
            "fallback_strategy": selected_snapshot.fallback_strategy,
            "validation_metric_value": selected_snapshot.validation_metric_value,
            "test_metric_value": selected_snapshot.test_metric_value,
        },
        "prediction_metrics": _summarize_backtest_prediction_metrics(predictions),
        "strategies": {
            "candidate_threshold": candidate_strategy,
            "review_threshold": review_strategy,
        },
    }


def _summarize_walk_forward_backtest(
    *,
    target_task: str,
    selection_policy_name: str,
    minimum_train_games: int,
    test_window_games: int,
    dataset_row_count: int,
    dataset_game_count: int,
    fold_summaries: list[dict[str, Any]],
    predictions: list[dict[str, Any]],
) -> dict[str, Any]:
    candidate_bets = [
        bet
        for fold in fold_summaries
        for bet in fold["strategies"]["candidate_threshold"]["bets"]
    ]
    review_bets = [
        bet
        for fold in fold_summaries
        for bet in fold["strategies"]["review_threshold"]["bets"]
    ]
    selected_family_counts: dict[str, int] = {}
    for fold in fold_summaries:
        family = fold["selected_model"]["model_family"]
        selected_family_counts[family] = selected_family_counts.get(family, 0) + 1
    return {
        "target_task": target_task,
        "selection_policy_name": selection_policy_name,
        "strategy_name": _backtest_strategy_name(target_task),
        "minimum_train_games": minimum_train_games,
        "test_window_games": test_window_games,
        "dataset_row_count": dataset_row_count,
        "dataset_game_count": dataset_game_count,
        "fold_count": len(fold_summaries),
        "selected_model_family_counts": selected_family_counts,
        "prediction_metrics": _summarize_backtest_prediction_metrics(predictions),
        "strategy_results": {
            "candidate_threshold": _summarize_backtest_bets(
                candidate_bets,
                strategy_name="candidate_threshold",
            ),
            "review_threshold": _summarize_backtest_bets(
                review_bets,
                strategy_name="review_threshold",
            ),
        },
        "folds": fold_summaries,
    }


def _empty_backtest_summary(
    *,
    target_task: str,
    selection_policy_name: str,
    strategy_name: str,
    minimum_train_games: int,
    test_window_games: int,
) -> dict[str, Any]:
    return {
        "target_task": target_task,
        "selection_policy_name": selection_policy_name,
        "strategy_name": strategy_name,
        "minimum_train_games": minimum_train_games,
        "test_window_games": test_window_games,
        "dataset_row_count": 0,
        "dataset_game_count": 0,
        "fold_count": 0,
        "selected_model_family_counts": {},
        "prediction_metrics": _summarize_backtest_prediction_metrics([]),
        "strategy_results": {
            "candidate_threshold": _summarize_backtest_bets(
                [],
                strategy_name="candidate_threshold",
            ),
            "review_threshold": _summarize_backtest_bets(
                [],
                strategy_name="review_threshold",
            ),
        },
        "folds": [],
    }


def _backtest_strategy_name(target_task: str) -> str:
    return f"{target_task}_walk_forward_v1"


def _summarize_backtest_prediction_metrics(predictions: list[dict[str, Any]]) -> dict[str, Any]:
    realized_residuals = [
        float(entry["realized_residual"])
        for entry in predictions
        if entry.get("realized_residual") is not None
    ]
    actual_targets = [
        float(entry["actual_target_value"])
        for entry in predictions
        if entry.get("actual_target_value") is not None
    ]
    mae = None
    rmse = None
    if predictions and actual_targets:
        absolute_errors = [
            abs(float(entry["prediction_value"]) - float(entry["actual_target_value"]))
            for entry in predictions
            if entry.get("actual_target_value") is not None
        ]
        squared_errors = [
            (float(entry["prediction_value"]) - float(entry["actual_target_value"])) ** 2
            for entry in predictions
            if entry.get("actual_target_value") is not None
        ]
        mae = round(float(mean(absolute_errors)), 4) if absolute_errors else None
        rmse = round(float(mean(squared_errors) ** 0.5), 4) if squared_errors else None
    return {
        "prediction_count": len(predictions),
        "mae": mae,
        "rmse": rmse,
        "average_prediction_value": (
            round(float(mean(float(entry["prediction_value"]) for entry in predictions)), 4)
            if predictions
            else None
        ),
        "average_realized_residual": (
            round(float(mean(realized_residuals)), 4) if realized_residuals else None
        ),
    }


def _evaluate_backtest_strategy(
    *,
    predictions: list[dict[str, Any]],
    target_task: str,
    threshold: float,
    strategy_name: str,
) -> dict[str, Any]:
    bets = []
    for prediction in predictions:
        signal_strength = float(prediction["signal_strength"])
        if signal_strength < threshold:
            continue
        bet = _build_backtest_bet(
            prediction=prediction,
            target_task=target_task,
            strategy_name=strategy_name,
            threshold=threshold,
        )
        if bet is not None:
            bets.append(bet)
    summary = _summarize_backtest_bets(bets, strategy_name=strategy_name)
    summary["threshold"] = threshold
    summary["bets"] = bets
    return summary


def _build_backtest_bet(
    *,
    prediction: dict[str, Any],
    target_task: str,
    strategy_name: str,
    threshold: float,
) -> dict[str, Any] | None:
    prediction_value = float(prediction["prediction_value"])
    result = None
    edge_direction = None
    actual_target_value = _float_or_none(prediction.get("actual_target_value"))
    if actual_target_value is None:
        return None
    if target_task == "spread_error_regression":
        edge_direction = "team_cover_edge" if prediction_value > 0 else "opponent_cover_edge"
        if actual_target_value == 0:
            result = "push"
        elif prediction_value > 0:
            result = "win" if actual_target_value > 0 else "loss"
        elif prediction_value < 0:
            result = "win" if actual_target_value < 0 else "loss"
        else:
            return None
    elif target_task == "total_error_regression":
        if actual_target_value == 0:
            result = "push"
        elif prediction_value > 0:
            edge_direction = "over_edge"
            result = "win" if actual_target_value > 0 else "loss"
        elif prediction_value < 0:
            edge_direction = "under_edge"
            result = "win" if actual_target_value < 0 else "loss"
        else:
            return None
    else:
        return None
    profit_units = 0.0
    if result == "win":
        profit_units = 0.9091
    elif result == "loss":
        profit_units = -1.0
    edge_bucket = _backtest_edge_bucket(abs(prediction_value))
    return {
        "canonical_game_id": prediction["canonical_game_id"],
        "game_date": prediction["game_date"],
        "team_code": prediction["team_code"],
        "opponent_code": prediction["opponent_code"],
        "strategy_name": strategy_name,
        "threshold": threshold,
        "edge_direction": edge_direction,
        "signal_strength": round(abs(prediction_value), 4),
        "prediction_value": round(prediction_value, 4),
        "result": result,
        "profit_units": round(profit_units, 4),
        "edge_bucket": edge_bucket,
    }


def _backtest_edge_bucket(signal_strength: float) -> str:
    if signal_strength < 1:
        return "0_to_1"
    if signal_strength < 2:
        return "1_to_2"
    if signal_strength < 3:
        return "2_to_3"
    return "3_plus"


def _summarize_backtest_bets(
    bets: list[dict[str, Any]],
    *,
    strategy_name: str,
) -> dict[str, Any]:
    win_count = sum(1 for bet in bets if bet["result"] == "win")
    loss_count = sum(1 for bet in bets if bet["result"] == "loss")
    push_count = sum(1 for bet in bets if bet["result"] == "push")
    settled_bet_count = win_count + loss_count
    total_profit_units = round(float(sum(float(bet["profit_units"]) for bet in bets)), 4)
    edge_bucket_performance: dict[str, dict[str, Any]] = {}
    for bet in bets:
        bucket = edge_bucket_performance.setdefault(
            bet["edge_bucket"],
            {
                "bet_count": 0,
                "win_count": 0,
                "loss_count": 0,
                "push_count": 0,
                "profit_units": 0.0,
            },
        )
        bucket["bet_count"] += 1
        bucket[f"{bet['result']}_count"] += 1
        bucket["profit_units"] = round(
            float(bucket["profit_units"]) + float(bet["profit_units"]),
            4,
        )
    for bucket in edge_bucket_performance.values():
        settled = int(bucket["win_count"]) + int(bucket["loss_count"])
        bucket["hit_rate"] = round(int(bucket["win_count"]) / settled, 4) if settled else None
        bucket["push_rate"] = (
            round(int(bucket["push_count"]) / int(bucket["bet_count"]), 4)
            if bucket["bet_count"]
            else None
        )
        bucket["roi"] = (
            round(float(bucket["profit_units"]) / int(bucket["bet_count"]), 4)
            if bucket["bet_count"]
            else None
        )
    return {
        "strategy_name": strategy_name,
        "bet_count": len(bets),
        "win_count": win_count,
        "loss_count": loss_count,
        "push_count": push_count,
        "hit_rate": round(win_count / settled_bet_count, 4) if settled_bet_count else None,
        "push_rate": round(push_count / len(bets), 4) if bets else None,
        "roi": round(total_profit_units / len(bets), 4) if bets else None,
        "profit_units": total_profit_units,
        "edge_bucket_performance": edge_bucket_performance,
    }


def _train_linear_feature_model(
    *,
    train_rows: list[dict[str, Any]],
    validation_rows: list[dict[str, Any]],
    test_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    feature_candidates = _numeric_feature_candidates(train_rows)
    selection_rows = validation_rows or train_rows
    fallback_prediction = _constant_target_mean(train_rows)
    best_model = {
        "feature_name": None,
        "intercept": 0.0,
        "coefficient": 0.0,
        "selection_metrics": {"mae": None, "prediction_count": 0},
    }
    candidate_count = 0
    for feature_name in feature_candidates:
        pairs = _training_pairs(train_rows, feature_name)
        if len(pairs) < 2:
            continue
        candidate_count += 1
        slope, intercept = _fit_simple_linear_regression(pairs)
        selection_metrics = _score_regression_model(
            selection_rows,
            predictor=lambda row, fn=feature_name, b0=intercept, b1=slope: _predict_linear(
                row,
                fn,
                b0,
                b1,
            ),
        )
        if _is_better_regression_candidate(
            selection_metrics,
            best_model["selection_metrics"],
        ):
            best_model = {
                "feature_name": feature_name,
                "intercept": intercept,
                "coefficient": slope,
                "selection_metrics": selection_metrics,
            }
    feature_name = best_model["feature_name"]
    artifact = {
        "model_family": "linear_feature",
        "selected_feature": feature_name,
        "intercept": best_model["intercept"],
        "coefficient": best_model["coefficient"],
        "constant_prediction": fallback_prediction,
        "feature_candidate_count": candidate_count,
        "selection_split": "validation" if validation_rows else "train_fallback",
        "selection_metrics": best_model["selection_metrics"],
        "fallback_strategy": None,
        "fallback_reason": None,
    }
    if feature_name is None:
        artifact["fallback_strategy"] = "constant_mean"
        artifact["fallback_reason"] = "no_usable_feature"

        def predictor(_row: dict[str, Any]) -> float | None:
            return fallback_prediction
    else:
        def predictor(row: dict[str, Any]) -> float | None:
            return _predict_linear(
                row,
                feature_name,
                best_model["intercept"],
                best_model["coefficient"],
            )
    metrics = {
        "train": _score_regression_model(train_rows, predictor=predictor),
        "validation": _score_regression_model(validation_rows, predictor=predictor),
        "test": _score_regression_model(test_rows, predictor=predictor),
    }
    return {"artifact": artifact, "metrics": metrics}


def _train_tree_stump_model(
    *,
    train_rows: list[dict[str, Any]],
    validation_rows: list[dict[str, Any]],
    test_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    feature_candidates = _numeric_feature_candidates(train_rows)
    selection_rows = validation_rows or train_rows
    fallback_prediction = _constant_target_mean(train_rows)
    best_model = {
        "feature_name": None,
        "threshold": None,
        "left_prediction": None,
        "right_prediction": None,
        "selection_metrics": {"mae": None, "prediction_count": 0},
    }
    candidate_count = 0
    for feature_name in feature_candidates:
        pairs = _training_pairs(train_rows, feature_name)
        if len(pairs) < 2:
            continue
        for threshold in _candidate_tree_thresholds(pairs):
            left_targets = [target for value, target in pairs if value <= threshold]
            right_targets = [target for value, target in pairs if value > threshold]
            if not left_targets or not right_targets:
                continue
            candidate_count += 1
            left_prediction = round(float(mean(left_targets)), 4)
            right_prediction = round(float(mean(right_targets)), 4)
            selection_metrics = _score_regression_model(
                selection_rows,
                predictor=lambda row,
                fn=feature_name,
                split=threshold,
                left=left_prediction,
                right=right_prediction: _predict_tree_stump(
                    row,
                    fn,
                    split,
                    left,
                    right,
                ),
            )
            if _is_better_regression_candidate(
                selection_metrics,
                best_model["selection_metrics"],
            ):
                best_model = {
                    "feature_name": feature_name,
                    "threshold": threshold,
                    "left_prediction": left_prediction,
                    "right_prediction": right_prediction,
                    "selection_metrics": selection_metrics,
                }
    feature_name = best_model["feature_name"]
    artifact = {
        "model_family": "tree_stump",
        "selected_feature": feature_name,
        "threshold": best_model["threshold"],
        "left_prediction": best_model["left_prediction"],
        "right_prediction": best_model["right_prediction"],
        "constant_prediction": fallback_prediction,
        "feature_candidate_count": candidate_count,
        "selection_split": "validation" if validation_rows else "train_fallback",
        "selection_metrics": best_model["selection_metrics"],
        "fallback_strategy": None,
        "fallback_reason": None,
    }
    if feature_name is None:
        artifact["fallback_strategy"] = "constant_mean"
        artifact["fallback_reason"] = "no_valid_split"

        def predictor(_row: dict[str, Any]) -> float | None:
            return fallback_prediction
    else:
        def predictor(row: dict[str, Any]) -> float | None:
            return _predict_tree_stump(
                row,
                feature_name,
                best_model["threshold"],
                best_model["left_prediction"],
                best_model["right_prediction"],
            )
    metrics = {
        "train": _score_regression_model(train_rows, predictor=predictor),
        "validation": _score_regression_model(validation_rows, predictor=predictor),
        "test": _score_regression_model(test_rows, predictor=predictor),
    }
    return {"artifact": artifact, "metrics": metrics}


def _load_training_dataset_rows_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    feature_version_id: int,
    team_code: str | None,
    season_label: str | None,
) -> list[dict[str, Any]]:
    snapshots = list_feature_snapshots_in_memory(
        repository,
        feature_version_id=feature_version_id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    canonical_games = list_canonical_game_metric_records_in_memory(repository)
    return build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code=team_code,
    )


def _load_training_dataset_rows_postgres(
    connection: Any,
    *,
    feature_version_id: int,
    team_code: str | None,
    season_label: str | None,
) -> list[dict[str, Any]]:
    snapshots = list_feature_snapshots_postgres(
        connection,
        feature_version_id=feature_version_id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    canonical_games = list_canonical_game_metric_records_postgres(connection)
    return build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code=team_code,
    )


def _numeric_feature_candidates(training_rows: list[dict[str, Any]]) -> list[str]:
    feature_columns = sorted(
        {
            feature_name
            for row in training_rows
            for feature_name, value in row["features"].items()
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        }
    )
    return feature_columns


def _training_pairs(
    training_rows: list[dict[str, Any]],
    feature_name: str,
) -> list[tuple[float, float]]:
    pairs = []
    for row in training_rows:
        feature_value = row["features"].get(feature_name)
        target_value = row["target_value"]
        if feature_value is None or target_value is None:
            continue
        pairs.append((float(feature_value), float(target_value)))
    return pairs


def _fit_simple_linear_regression(
    pairs: list[tuple[float, float]],
) -> tuple[float, float]:
    x_mean = mean(value for value, _ in pairs)
    y_mean = mean(target for _, target in pairs)
    numerator = sum((value - x_mean) * (target - y_mean) for value, target in pairs)
    denominator = sum((value - x_mean) ** 2 for value, _ in pairs)
    if denominator == 0:
        return 0.0, round(float(y_mean), 4)
    slope = numerator / denominator
    intercept = y_mean - (slope * x_mean)
    return round(float(slope), 4), round(float(intercept), 4)


def _candidate_tree_thresholds(
    pairs: list[tuple[float, float]],
) -> list[float]:
    unique_values = sorted({value for value, _ in pairs})
    if len(unique_values) < 2:
        return []
    thresholds = [
        round(float((left + right) / 2), 4)
        for left, right in zip(unique_values, unique_values[1:])
    ]
    median_threshold = round(float(median(unique_values)), 4)
    if median_threshold not in thresholds:
        thresholds.append(median_threshold)
    return thresholds


def _constant_target_mean(
    training_rows: list[dict[str, Any]],
) -> float | None:
    target_values = [
        float(row["target_value"])
        for row in training_rows
        if row.get("target_value") is not None
    ]
    if not target_values:
        return None
    return round(float(mean(target_values)), 4)


def _summarize_target_values(training_rows: list[dict[str, Any]]) -> dict[str, Any]:
    target_values = [
        float(row["target_value"])
        for row in training_rows
        if row.get("target_value") is not None
    ]
    if not target_values:
        return {
            "row_count": 0,
            "target_mean": None,
            "target_min": None,
            "target_max": None,
        }
    return {
        "row_count": len(target_values),
        "target_mean": round(float(mean(target_values)), 4),
        "target_min": round(float(min(target_values)), 4),
        "target_max": round(float(max(target_values)), 4),
    }


def _predict_linear(
    row: dict[str, Any],
    feature_name: str | None,
    intercept: float,
    coefficient: float,
) -> float | None:
    if feature_name is None:
        return None
    feature_value = _get_row_feature_value(row, feature_name)
    if feature_value is None:
        return None
    return round(float(intercept + (coefficient * float(feature_value))), 4)


def _predict_tree_stump(
    row: dict[str, Any],
    feature_name: str | None,
    threshold: float | None,
    left_prediction: float | None,
    right_prediction: float | None,
) -> float | None:
    if (
        feature_name is None
        or threshold is None
        or left_prediction is None
        or right_prediction is None
    ):
        return None
    feature_value = _get_row_feature_value(row, feature_name)
    if feature_value is None:
        return None
    return left_prediction if float(feature_value) <= threshold else right_prediction


def _get_row_feature_value(
    row: dict[str, Any],
    feature_name: str,
) -> Any:
    if "features" in row and isinstance(row["features"], dict):
        return row["features"].get(feature_name)
    return row.get(feature_name)


def _score_regression_model(training_rows: list[dict[str, Any]], *, predictor) -> dict[str, Any]:
    scored = []
    for row in training_rows:
        target_value = row["target_value"]
        prediction = predictor(row)
        if target_value is None or prediction is None:
            continue
        error = float(prediction) - float(target_value)
        scored.append(
            {
                "prediction": round(float(prediction), 4),
                "absolute_error": round(abs(error), 4),
                "squared_error": round(error * error, 4),
            }
        )
    absolute_errors = [entry["absolute_error"] for entry in scored]
    squared_errors = [entry["squared_error"] for entry in scored]
    return {
        "prediction_count": len(scored),
        "coverage_rate": round(len(scored) / len(training_rows), 4) if training_rows else 0.0,
        "mae": round(float(mean(absolute_errors)), 4) if absolute_errors else None,
        "rmse": round(float(mean(squared_errors) ** 0.5), 4) if squared_errors else None,
    }


def _is_better_regression_candidate(
    candidate_metrics: dict[str, Any],
    incumbent_metrics: dict[str, Any],
) -> bool:
    candidate_mae = candidate_metrics.get("mae")
    incumbent_mae = incumbent_metrics.get("mae")
    if candidate_mae is None:
        return False
    if incumbent_mae is None:
        return True
    if float(candidate_mae) != float(incumbent_mae):
        return float(candidate_mae) < float(incumbent_mae)
    return int(candidate_metrics.get("prediction_count", 0)) > int(
        incumbent_metrics.get("prediction_count", 0)
    )


def _metric_value_or_inf(value: Any) -> float:
    if value is None:
        return float("inf")
    return float(value)


def _resolve_active_model_selection(
    *,
    selections: list[ModelSelectionSnapshotRecord],
) -> ModelSelectionSnapshotRecord | None:
    return selections[0] if selections else None


def _resolve_evaluation_snapshot_by_id(
    *,
    snapshots: list[ModelEvaluationSnapshotRecord],
    snapshot_id: int | None,
) -> ModelEvaluationSnapshotRecord | None:
    if snapshot_id is None:
        return None
    return next((entry for entry in snapshots if entry.id == snapshot_id), None)


def _build_model_scoring_preview(
    *,
    dataset_rows: list[dict[str, Any]],
    target_task: str,
    active_selection: ModelSelectionSnapshotRecord | None,
    active_snapshot: ModelEvaluationSnapshotRecord | None,
    team_code: str | None,
    canonical_game_id: int | None,
    limit: int,
    include_evidence: bool,
    evidence_dimensions: tuple[str, ...],
    comparable_limit: int,
    min_pattern_sample_size: int,
    train_ratio: float,
    validation_ratio: float,
    drop_null_targets: bool,
) -> dict[str, Any]:
    filtered_rows = [
        row
        for row in dataset_rows
        if canonical_game_id is None or int(row["canonical_game_id"]) == canonical_game_id
    ]
    scored_predictions = _score_dataset_rows_with_active_selection(
        filtered_rows,
        target_task=target_task,
        active_snapshot=active_snapshot,
        full_dataset_rows=dataset_rows,
        include_evidence=include_evidence,
        evidence_dimensions=evidence_dimensions,
        comparable_limit=comparable_limit,
        min_pattern_sample_size=min_pattern_sample_size,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
    )
    ranked_predictions = sorted(
        scored_predictions,
        key=lambda entry: (
            -float(entry["signal_strength"]),
            entry["game_date"],
            entry["canonical_game_id"],
            entry["team_code"],
        ),
    )[:limit]
    return {
        "active_selection": _serialize_model_selection_snapshot(active_selection),
        "active_evaluation_snapshot": _serialize_model_evaluation_snapshot(active_snapshot),
        "row_count": len(filtered_rows),
        "scored_prediction_count": len(ranked_predictions),
        "prediction_summary": _summarize_scored_predictions(ranked_predictions),
        "predictions": ranked_predictions,
    }


def _build_model_future_game_preview(
    *,
    target_task: str,
    active_selection: ModelSelectionSnapshotRecord | None,
    active_snapshot: ModelEvaluationSnapshotRecord | None,
    historical_dataset_rows: list[dict[str, Any]],
    scenario_rows: list[dict[str, Any]],
    include_evidence: bool,
    evidence_dimensions: tuple[str, ...],
    comparable_limit: int,
    min_pattern_sample_size: int,
    train_ratio: float,
    validation_ratio: float,
    drop_null_targets: bool,
) -> dict[str, Any]:
    scored_predictions = _score_dataset_rows_with_active_selection(
        scenario_rows,
        target_task=target_task,
        active_snapshot=active_snapshot,
        full_dataset_rows=historical_dataset_rows,
        include_evidence=include_evidence,
        evidence_dimensions=evidence_dimensions,
        comparable_limit=comparable_limit,
        min_pattern_sample_size=min_pattern_sample_size,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
    )
    ranked_predictions = sorted(
        scored_predictions,
        key=lambda entry: (
            -float(entry["signal_strength"]),
            entry["team_code"],
        ),
    )
    opportunity_preview = _build_opportunity_preview_entries(
        predictions=ranked_predictions,
        target_task=target_task,
    )
    return {
        "active_selection": _serialize_model_selection_snapshot(active_selection),
        "active_evaluation_snapshot": _serialize_model_evaluation_snapshot(active_snapshot),
        "scenario": _serialize_future_scenario(scenario_rows),
        "scored_prediction_count": len(ranked_predictions),
        "prediction_summary": _summarize_scored_predictions(ranked_predictions),
        "predictions": ranked_predictions,
        "opportunity_preview": opportunity_preview,
    }


def _score_dataset_rows_with_active_selection(
    dataset_rows: list[dict[str, Any]],
    *,
    target_task: str,
    active_snapshot: ModelEvaluationSnapshotRecord | None,
    full_dataset_rows: list[dict[str, Any]],
    include_evidence: bool,
    evidence_dimensions: tuple[str, ...],
    comparable_limit: int,
    min_pattern_sample_size: int,
    train_ratio: float,
    validation_ratio: float,
    drop_null_targets: bool,
) -> list[dict[str, Any]]:
    if active_snapshot is None:
        return []
    scored_predictions = []
    for row in dataset_rows:
        prediction_value = _predict_row_from_snapshot(active_snapshot, row)
        if prediction_value is None:
            continue
        evidence_payload = None
        if include_evidence:
            if row.get("is_future_scenario"):
                evidence_bundle = build_feature_evidence_bundle(
                    full_dataset_rows,
                    target_task=target_task,
                    dimensions=evidence_dimensions,
                    team_code=str(row["team_code"]),
                    condition_values=resolve_feature_condition_values_for_row(
                        row,
                        dimensions=evidence_dimensions,
                    ),
                    comparable_limit=comparable_limit,
                    min_pattern_sample_size=min_pattern_sample_size,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                    drop_null_targets=drop_null_targets,
                )
            else:
                evidence_bundle = build_feature_evidence_bundle(
                    full_dataset_rows,
                    target_task=target_task,
                    dimensions=evidence_dimensions,
                    canonical_game_id=int(row["canonical_game_id"]),
                    team_code=str(row["team_code"]),
                    comparable_limit=comparable_limit,
                    min_pattern_sample_size=min_pattern_sample_size,
                    train_ratio=train_ratio,
                    validation_ratio=validation_ratio,
                    drop_null_targets=drop_null_targets,
                )
            evidence_payload = evidence_bundle.get("evidence")
        scored_predictions.append(
            _serialize_scored_prediction(
                row,
                target_task=target_task,
                prediction_value=prediction_value,
                active_snapshot=active_snapshot,
                evidence_payload=evidence_payload,
            )
        )
    return scored_predictions


def _serialize_scored_prediction(
    row: dict[str, Any],
    *,
    target_task: str,
    prediction_value: float,
    active_snapshot: ModelEvaluationSnapshotRecord,
    evidence_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    actual_target_value = _float_or_none(row.get("target_value"))
    realized_residual = None
    if actual_target_value is not None:
        realized_residual = round(float(prediction_value) - float(actual_target_value), 4)
    selected_feature = active_snapshot.selected_feature
    selected_feature_value = (
        _get_row_feature_value(row, selected_feature) if selected_feature is not None else None
    )
    return {
        "canonical_game_id": int(row["canonical_game_id"]),
        "season_label": row["season_label"],
        "game_date": row["game_date"],
        "team_code": row["team_code"],
        "opponent_code": row["opponent_code"],
        "venue": row["venue"],
        "prediction_value": round(float(prediction_value), 4),
        "signal_strength": round(abs(float(prediction_value)), 4),
        "prediction_context": _build_prediction_context(
            target_task=target_task,
            prediction_value=prediction_value,
        ),
        "actual_target_value": actual_target_value,
        "realized_residual": realized_residual,
        "selected_feature_value": _float_or_none(selected_feature_value),
        "feature_context": {
            "days_rest": _float_or_none(_get_row_feature_value(row, "days_rest")),
            "games_played_prior": _float_or_none(
                _get_row_feature_value(row, "games_played_prior")
            ),
            "prior_matchup_count": _float_or_none(
                _get_row_feature_value(row, "prior_matchup_count")
            ),
        },
        "market_context": {
            "team_spread_line": _float_or_none(row.get("team_spread_line")),
            "opponent_spread_line": _float_or_none(row.get("opponent_spread_line")),
            "total_line": _float_or_none(row.get("total_line")),
        },
        "model": {
            "target_task": target_task,
            "model_family": active_snapshot.model_family,
            "selected_feature": selected_feature,
            "fallback_strategy": active_snapshot.fallback_strategy,
            "primary_metric_name": active_snapshot.primary_metric_name,
            "validation_metric_value": active_snapshot.validation_metric_value,
            "test_metric_value": active_snapshot.test_metric_value,
        },
        "evidence": (
            {
                "summary": evidence_payload.get("summary"),
                "strength": evidence_payload.get("strength"),
                "recommendation": evidence_payload.get("recommendation"),
            }
            if evidence_payload is not None
            else None
        ),
    }


def _build_prediction_context(
    *,
    target_task: str,
    prediction_value: float,
) -> dict[str, Any]:
    if target_task == "spread_error_regression":
        signal_direction = "team_cover_edge" if prediction_value > 0 else "opponent_cover_edge"
        if prediction_value == 0:
            signal_direction = "neutral"
        return {
            "target_type": "spread_error",
            "signal_direction": signal_direction,
            "market_edge_points": round(float(prediction_value), 4),
        }
    if target_task == "total_error_regression":
        signal_direction = "over_edge" if prediction_value > 0 else "under_edge"
        if prediction_value == 0:
            signal_direction = "neutral"
        return {
            "target_type": "total_error",
            "signal_direction": signal_direction,
            "market_edge_points": round(float(prediction_value), 4),
        }
    if target_task == "point_margin_regression":
        signal_direction = (
            "team_margin_advantage"
            if prediction_value > 0
            else "opponent_margin_advantage"
        )
        if prediction_value == 0:
            signal_direction = "neutral"
        return {
            "target_type": "point_margin",
            "signal_direction": signal_direction,
            "predicted_margin": round(float(prediction_value), 4),
        }
    signal_direction = "higher_total" if prediction_value > 0 else "lower_total"
    if prediction_value == 0:
        signal_direction = "neutral"
    return {
        "target_type": "total_points",
        "signal_direction": signal_direction,
        "predicted_total_points": round(float(prediction_value), 4),
    }


def _predict_row_from_snapshot(
    snapshot: ModelEvaluationSnapshotRecord,
    row: dict[str, Any],
) -> float | None:
    artifact = snapshot.snapshot.get("artifact", {})
    model_family = artifact.get("model_family")
    if artifact.get("fallback_strategy") == "constant_mean":
        return _float_or_none(artifact.get("constant_prediction"))
    if model_family == "linear_feature":
        return _predict_linear(
            row,
            artifact.get("selected_feature"),
            float(artifact.get("intercept", 0.0)),
            float(artifact.get("coefficient", 0.0)),
        )
    if model_family == "tree_stump":
        return _predict_tree_stump(
            row,
            artifact.get("selected_feature"),
            _float_or_none(artifact.get("threshold")),
            _float_or_none(artifact.get("left_prediction")),
            _float_or_none(artifact.get("right_prediction")),
        )
    return None


def _summarize_scored_predictions(predictions: list[dict[str, Any]]) -> dict[str, Any]:
    prediction_values = [float(entry["prediction_value"]) for entry in predictions]
    signal_strengths = [float(entry["signal_strength"]) for entry in predictions]
    positive_count = len([value for value in prediction_values if value > 0])
    negative_count = len([value for value in prediction_values if value < 0])
    return {
        "prediction_count": len(predictions),
        "positive_prediction_count": positive_count,
        "negative_prediction_count": negative_count,
        "average_prediction_value": (
            round(float(mean(prediction_values)), 4) if prediction_values else None
        ),
        "average_signal_strength": (
            round(float(mean(signal_strengths)), 4) if signal_strengths else None
        ),
        "top_prediction": predictions[0] if predictions else None,
    }


def _build_opportunity_preview_entries(
    *,
    predictions: list[dict[str, Any]],
    target_task: str,
) -> list[dict[str, Any]]:
    policy = OPPORTUNITY_POLICY_CONFIGS.get(target_task)
    if policy is None:
        return []
    preview_entries = []
    for prediction in predictions:
        status = _evaluate_opportunity_status(prediction=prediction, policy=policy)
        preview_entries.append(
            {
                "team_code": prediction["team_code"],
                "opponent_code": prediction["opponent_code"],
                "game_date": prediction["game_date"],
                "status": status,
                "policy_name": policy["policy_name"],
                "signal_strength": prediction["signal_strength"],
                "prediction_value": prediction["prediction_value"],
                "recommendation_status": _nested_get(
                    prediction,
                    "evidence",
                    "recommendation",
                    "status",
                ),
                "evidence_rating": _nested_get(
                    prediction,
                    "evidence",
                    "strength",
                    "rating",
                ),
            }
        )
    return preview_entries


def _serialize_future_scenario(
    scenario_rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not scenario_rows:
        return None
    home_row = next((row for row in scenario_rows if row["venue"] == "home"), None)
    away_row = next((row for row in scenario_rows if row["venue"] == "away"), None)
    representative = home_row or away_row
    if representative is None:
        return None
    return {
        "scenario_key": representative.get("scenario_key"),
        "season_label": representative["season_label"],
        "game_date": representative["game_date"],
        "home_team_code": home_row["team_code"] if home_row is not None else None,
        "away_team_code": away_row["team_code"] if away_row is not None else None,
        "home_spread_line": _float_or_none(
            home_row.get("team_spread_line") if home_row is not None else None
        ),
        "away_spread_line": _float_or_none(
            away_row.get("team_spread_line") if away_row is not None else None
        ),
        "total_line": _float_or_none(representative.get("total_line")),
    }


def _serialize_future_game_input(game: dict[str, Any]) -> dict[str, Any]:
    game_date = _coerce_date(game["game_date"])
    return {
        "season_label": str(game["season_label"]),
        "game_date": game_date.isoformat(),
        "home_team_code": str(game["home_team_code"]),
        "away_team_code": str(game["away_team_code"]),
        "home_spread_line": _float_or_none(game.get("home_spread_line")),
        "total_line": _float_or_none(game.get("total_line")),
    }


def _coerce_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _build_future_slate_response(
    *,
    target_task: str,
    slate_label: str | None,
    game_inputs: list[dict[str, Any]],
    games: list[dict[str, Any]],
) -> dict[str, Any]:
    serialized_inputs = [_serialize_future_game_input(game) for game in game_inputs]
    game_dates = [entry["game_date"] for entry in serialized_inputs]
    scenario_keys = [
        str(game["scenario"]["scenario_key"])
        for game in games
        if isinstance(game.get("scenario"), dict)
        and game["scenario"].get("scenario_key") is not None
    ]
    prediction_count = sum(int(game.get("scored_prediction_count", 0)) for game in games)
    status_counts: dict[str, int] = {}
    for game in games:
        for preview in game.get("opportunity_preview", []):
            status = str(preview.get("status", "discarded"))
            status_counts[status] = status_counts.get(status, 0) + 1
    slate_key = _build_future_slate_key(
        target_task=target_task,
        slate_label=slate_label,
        serialized_inputs=serialized_inputs,
    )
    return {
        "slate": {
            "slate_key": slate_key,
            "slate_label": slate_label,
            "target_task": target_task,
            "game_count": len(serialized_inputs),
            "game_dates": sorted(set(game_dates)),
            "scenario_keys": scenario_keys,
            "games": serialized_inputs,
        },
        "game_preview_count": len(games),
        "scored_prediction_count": prediction_count,
        "opportunity_preview_status_counts": status_counts,
        "games": games,
    }


def _build_future_slate_key(
    *,
    target_task: str,
    slate_label: str | None,
    serialized_inputs: list[dict[str, Any]],
) -> str:
    if slate_label:
        return f"{target_task}:{slate_label}"
    if not serialized_inputs:
        return f"{target_task}:empty-slate"
    ordered = sorted(
        serialized_inputs,
        key=lambda entry: (
            entry["game_date"],
            entry["home_team_code"],
            entry["away_team_code"],
        ),
    )
    first = ordered[0]
    last = ordered[-1]
    return (
        f"{target_task}:{first['game_date']}:{last['game_date']}:"
        f"{len(serialized_inputs)}-games"
    )

def _build_model_scoring_run(
    *,
    preview: dict[str, Any],
    target_task: str,
    model_market_board_id: int | None = None,
) -> ModelScoringRunRecord | None:
    active_selection = preview.get("active_selection")
    active_snapshot = preview.get("active_evaluation_snapshot")
    feature_version = preview.get("feature_version")
    scenario = preview.get("scenario")
    if feature_version is None or scenario is None:
        return None
    opportunity_preview = list(preview.get("opportunity_preview", []))
    status_counts = {
        "candidate_signal": 0,
        "review_manually": 0,
        "discarded": 0,
    }
    for entry in opportunity_preview:
        status = str(entry.get("status", "discarded"))
        if status not in status_counts:
            status_counts[status] = 0
        status_counts[status] += 1
    payload = {
        "scenario": scenario,
        "feature_version": feature_version,
        "active_selection": active_selection,
        "active_evaluation_snapshot": active_snapshot,
        "prediction_summary": preview.get("prediction_summary", {}),
        "predictions": preview.get("predictions", []),
        "opportunity_preview": opportunity_preview,
    }
    game_date = scenario.get("game_date")
    if isinstance(game_date, str):
        game_date = date.fromisoformat(game_date)
    return ModelScoringRunRecord(
        id=0,
        model_market_board_id=model_market_board_id,
        model_selection_snapshot_id=(
            int(active_selection["id"]) if active_selection is not None else None
        ),
        model_evaluation_snapshot_id=(
            int(active_snapshot["id"]) if active_snapshot is not None else None
        ),
        feature_version_id=int(feature_version["id"]),
        target_task=target_task,
        scenario_key=str(scenario["scenario_key"]),
        season_label=str(scenario["season_label"]),
        game_date=game_date,
        home_team_code=str(scenario["home_team_code"]),
        away_team_code=str(scenario["away_team_code"]),
        home_spread_line=_float_or_none(scenario.get("home_spread_line")),
        total_line=_float_or_none(scenario.get("total_line")),
        policy_name=(
            str(opportunity_preview[0]["policy_name"])
            if opportunity_preview
            else OPPORTUNITY_POLICY_CONFIGS.get(target_task, {}).get("policy_name")
        ),
        prediction_count=int(preview.get("scored_prediction_count", 0)),
        candidate_opportunity_count=int(status_counts.get("candidate_signal", 0)),
        review_opportunity_count=int(status_counts.get("review_manually", 0)),
        discarded_opportunity_count=int(status_counts.get("discarded", 0)),
        payload=payload,
    )


def save_model_scoring_run_in_memory(
    repository: InMemoryIngestionRepository,
    scoring_run: ModelScoringRunRecord | None,
) -> ModelScoringRunRecord | None:
    if scoring_run is None:
        return None
    payload = asdict(scoring_run)
    payload["id"] = len(repository.model_scoring_runs) + 1
    payload["created_at"] = datetime.now(timezone.utc)
    repository.model_scoring_runs.append(payload)
    return ModelScoringRunRecord(**payload)


def save_model_scoring_run_postgres(
    connection: Any,
    scoring_run: ModelScoringRunRecord | None,
) -> ModelScoringRunRecord | None:
    if scoring_run is None:
        return None
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO model_scoring_run (
                model_market_board_id,
                model_selection_snapshot_id,
                model_evaluation_snapshot_id,
                feature_version_id,
                target_task,
                scenario_key,
                season_label,
                game_date,
                home_team_code,
                away_team_code,
                home_spread_line,
                total_line,
                policy_name,
                prediction_count,
                candidate_opportunity_count,
                review_opportunity_count,
                discarded_opportunity_count,
                payload_json
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
            )
            RETURNING id, created_at
            """,
            (
                scoring_run.model_market_board_id,
                scoring_run.model_selection_snapshot_id,
                scoring_run.model_evaluation_snapshot_id,
                scoring_run.feature_version_id,
                scoring_run.target_task,
                scoring_run.scenario_key,
                scoring_run.season_label,
                scoring_run.game_date,
                scoring_run.home_team_code,
                scoring_run.away_team_code,
                scoring_run.home_spread_line,
                scoring_run.total_line,
                scoring_run.policy_name,
                scoring_run.prediction_count,
                scoring_run.candidate_opportunity_count,
                scoring_run.review_opportunity_count,
                scoring_run.discarded_opportunity_count,
                _json_dumps(scoring_run.payload),
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return ModelScoringRunRecord(
        id=int(row[0]),
        model_market_board_id=scoring_run.model_market_board_id,
        model_selection_snapshot_id=scoring_run.model_selection_snapshot_id,
        model_evaluation_snapshot_id=scoring_run.model_evaluation_snapshot_id,
        feature_version_id=scoring_run.feature_version_id,
        target_task=scoring_run.target_task,
        scenario_key=scoring_run.scenario_key,
        season_label=scoring_run.season_label,
        game_date=scoring_run.game_date,
        home_team_code=scoring_run.home_team_code,
        away_team_code=scoring_run.away_team_code,
        home_spread_line=scoring_run.home_spread_line,
        total_line=scoring_run.total_line,
        policy_name=scoring_run.policy_name,
        prediction_count=scoring_run.prediction_count,
        candidate_opportunity_count=scoring_run.candidate_opportunity_count,
        review_opportunity_count=scoring_run.review_opportunity_count,
        discarded_opportunity_count=scoring_run.discarded_opportunity_count,
        payload=scoring_run.payload,
        created_at=row[1],
    )


def _build_model_opportunities(
    *,
    scoring_preview: dict[str, Any],
    target_task: str,
    model_scoring_run_id: int | None = None,
    allow_best_effort_review: bool = False,
) -> list[ModelOpportunityRecord]:
    active_selection = scoring_preview.get("active_selection")
    active_snapshot = scoring_preview.get("active_evaluation_snapshot")
    feature_version = scoring_preview.get("feature_version")
    scenario = scoring_preview.get("scenario")
    if active_snapshot is None or active_selection is None or feature_version is None:
        return []
    policy = OPPORTUNITY_POLICY_CONFIGS.get(target_task)
    if policy is None:
        raise ValueError(f"Unsupported opportunity policy target_task: {target_task}")
    opportunities = []
    predictions = list(scoring_preview.get("predictions", []))
    for prediction in predictions:
        status = _evaluate_opportunity_status(
            prediction=prediction,
            policy=policy,
        )
        if status == "discarded":
            continue
        canonical_game_id = _positive_int_or_none(prediction.get("canonical_game_id"))
        source_kind = "future_scenario" if scenario is not None else "historical_game"
        scenario_key = str(scenario["scenario_key"]) if scenario is not None else None
        payload = {
            "prediction": prediction,
            "policy": policy,
            "active_selection": active_selection,
            "active_evaluation_snapshot": active_snapshot,
            "scenario": scenario,
        }
        opportunities.append(
            ModelOpportunityRecord(
                id=0,
                model_scoring_run_id=model_scoring_run_id,
                model_selection_snapshot_id=active_selection.get("id"),
                model_evaluation_snapshot_id=active_snapshot.get("id"),
                feature_version_id=int(feature_version["id"]),
                target_task=target_task,
                source_kind=source_kind,
                scenario_key=scenario_key,
                opportunity_key=_build_model_opportunity_key(
                    target_task=target_task,
                    canonical_game_id=canonical_game_id,
                    scenario_key=scenario_key,
                    team_code=str(prediction["team_code"]),
                    policy_name=str(policy["policy_name"]),
                ),
                team_code=str(prediction["team_code"]),
                opponent_code=str(prediction["opponent_code"]),
                season_label=str(prediction["season_label"]),
                canonical_game_id=canonical_game_id,
                game_date=(
                    date.fromisoformat(prediction["game_date"])
                    if isinstance(prediction["game_date"], str)
                    else prediction["game_date"]
                ),
                policy_name=str(policy["policy_name"]),
                status=status,
                prediction_value=float(prediction["prediction_value"]),
                signal_strength=float(prediction["signal_strength"]),
                evidence_rating=_nested_get(
                    prediction,
                    "evidence",
                    "strength",
                    "rating",
                ),
                recommendation_status=_nested_get(
                    prediction,
                    "evidence",
                    "recommendation",
                    "status",
                ),
                payload=payload,
            )
        )
    if not opportunities and allow_best_effort_review and predictions:
        strongest_prediction = max(
            predictions,
            key=lambda entry: float(entry.get("signal_strength", 0.0)),
        )
        canonical_game_id = _positive_int_or_none(strongest_prediction.get("canonical_game_id"))
        source_kind = "future_scenario" if scenario is not None else "historical_game"
        scenario_key = str(scenario["scenario_key"]) if scenario is not None else None
        payload = {
            "prediction": strongest_prediction,
            "policy": policy,
            "active_selection": active_selection,
            "active_evaluation_snapshot": active_snapshot,
            "scenario": scenario,
            "policy_override_reason": "future_scenario_best_effort_review",
        }
        opportunities.append(
            ModelOpportunityRecord(
                id=0,
                model_scoring_run_id=model_scoring_run_id,
                model_selection_snapshot_id=active_selection.get("id"),
                model_evaluation_snapshot_id=active_snapshot.get("id"),
                feature_version_id=int(feature_version["id"]),
                target_task=target_task,
                source_kind=source_kind,
                scenario_key=scenario_key,
                opportunity_key=_build_model_opportunity_key(
                    target_task=target_task,
                    canonical_game_id=canonical_game_id,
                    scenario_key=scenario_key,
                    team_code=str(strongest_prediction["team_code"]),
                    policy_name=str(policy["policy_name"]),
                ),
                team_code=str(strongest_prediction["team_code"]),
                opponent_code=str(strongest_prediction["opponent_code"]),
                season_label=str(strongest_prediction["season_label"]),
                canonical_game_id=canonical_game_id,
                game_date=(
                    date.fromisoformat(strongest_prediction["game_date"])
                    if isinstance(strongest_prediction["game_date"], str)
                    else strongest_prediction["game_date"]
                ),
                policy_name=str(policy["policy_name"]),
                status="review_manually",
                prediction_value=float(strongest_prediction["prediction_value"]),
                signal_strength=float(strongest_prediction["signal_strength"]),
                evidence_rating=_nested_get(
                    strongest_prediction,
                    "evidence",
                    "strength",
                    "rating",
                ),
                recommendation_status=_nested_get(
                    strongest_prediction,
                    "evidence",
                    "recommendation",
                    "status",
                ),
                payload=payload,
            )
        )
    return opportunities


def _evaluate_opportunity_status(
    *,
    prediction: dict[str, Any],
    policy: dict[str, Any],
) -> str:
    signal_strength = float(prediction.get("signal_strength", 0.0))
    evidence_rating = _nested_get(prediction, "evidence", "strength", "rating")
    recommendation_status = _nested_get(prediction, "evidence", "recommendation", "status")
    if (
        signal_strength >= float(policy["candidate_min_signal_strength"])
        and evidence_rating in policy["candidate_evidence_ratings"]
        and recommendation_status in policy["candidate_recommendation_statuses"]
    ):
        return "candidate_signal"
    if (
        signal_strength >= float(policy["review_min_signal_strength"])
        and evidence_rating in policy["review_evidence_ratings"]
        and recommendation_status in policy["review_recommendation_statuses"]
    ):
        return "review_manually"
    return "discarded"


def _build_model_opportunity_key(
    *,
    target_task: str,
    canonical_game_id: int | None,
    scenario_key: str | None,
    team_code: str,
    policy_name: str,
) -> str:
    subject_key = scenario_key if scenario_key is not None else f"game:{canonical_game_id}"
    return f"{target_task}:{subject_key}:{team_code}:{policy_name}"


def _nested_get(payload: dict[str, Any] | None, *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def save_model_opportunities_in_memory(
    repository: InMemoryIngestionRepository,
    opportunities: list[ModelOpportunityRecord],
) -> list[ModelOpportunityRecord]:
    persisted: list[ModelOpportunityRecord] = []
    for opportunity in opportunities:
        existing = next(
            (
                entry
                for entry in repository.model_opportunities
                if entry["opportunity_key"] == opportunity.opportunity_key
            ),
            None,
        )
        now = datetime.now(timezone.utc)
        payload = {
            "model_scoring_run_id": opportunity.model_scoring_run_id,
            "model_selection_snapshot_id": opportunity.model_selection_snapshot_id,
            "model_evaluation_snapshot_id": opportunity.model_evaluation_snapshot_id,
            "feature_version_id": opportunity.feature_version_id,
            "target_task": opportunity.target_task,
            "source_kind": opportunity.source_kind,
            "scenario_key": opportunity.scenario_key,
            "opportunity_key": opportunity.opportunity_key,
            "team_code": opportunity.team_code,
            "opponent_code": opportunity.opponent_code,
            "season_label": opportunity.season_label,
            "canonical_game_id": opportunity.canonical_game_id,
            "game_date": opportunity.game_date,
            "policy_name": opportunity.policy_name,
            "status": opportunity.status,
            "prediction_value": opportunity.prediction_value,
            "signal_strength": opportunity.signal_strength,
            "evidence_rating": opportunity.evidence_rating,
            "recommendation_status": opportunity.recommendation_status,
            "payload": opportunity.payload,
            "updated_at": now,
        }
        if existing is None:
            payload["id"] = len(repository.model_opportunities) + 1
            payload["created_at"] = now
            repository.model_opportunities.append(payload)
            persisted.append(ModelOpportunityRecord(**payload))
        else:
            existing.update(payload)
            persisted.append(ModelOpportunityRecord(**existing))
    return persisted


def save_model_opportunities_postgres(
    connection: Any,
    opportunities: list[ModelOpportunityRecord],
) -> list[ModelOpportunityRecord]:
    persisted: list[ModelOpportunityRecord] = []
    with connection.cursor() as cursor:
        for opportunity in opportunities:
            cursor.execute(
                """
                INSERT INTO model_opportunity (
                    model_scoring_run_id,
                    model_selection_snapshot_id,
                    model_evaluation_snapshot_id,
                    feature_version_id,
                    target_task,
                    source_kind,
                    scenario_key,
                    opportunity_key,
                    team_code,
                    opponent_code,
                    season_label,
                    canonical_game_id,
                    game_date,
                    policy_name,
                    status,
                    prediction_value,
                    signal_strength,
                    evidence_rating,
                    recommendation_status,
                    payload_json
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
                )
                ON CONFLICT (opportunity_key)
                DO UPDATE SET
                    model_scoring_run_id = EXCLUDED.model_scoring_run_id,
                    model_selection_snapshot_id = EXCLUDED.model_selection_snapshot_id,
                    model_evaluation_snapshot_id = EXCLUDED.model_evaluation_snapshot_id,
                    feature_version_id = EXCLUDED.feature_version_id,
                    target_task = EXCLUDED.target_task,
                    source_kind = EXCLUDED.source_kind,
                    scenario_key = EXCLUDED.scenario_key,
                    team_code = EXCLUDED.team_code,
                    opponent_code = EXCLUDED.opponent_code,
                    season_label = EXCLUDED.season_label,
                    canonical_game_id = EXCLUDED.canonical_game_id,
                    game_date = EXCLUDED.game_date,
                    policy_name = EXCLUDED.policy_name,
                    status = EXCLUDED.status,
                    prediction_value = EXCLUDED.prediction_value,
                    signal_strength = EXCLUDED.signal_strength,
                    evidence_rating = EXCLUDED.evidence_rating,
                    recommendation_status = EXCLUDED.recommendation_status,
                    payload_json = EXCLUDED.payload_json,
                    updated_at = NOW()
                RETURNING id, created_at, updated_at
                """,
                (
                    opportunity.model_scoring_run_id,
                    opportunity.model_selection_snapshot_id,
                    opportunity.model_evaluation_snapshot_id,
                    opportunity.feature_version_id,
                    opportunity.target_task,
                    opportunity.source_kind,
                    opportunity.scenario_key,
                    opportunity.opportunity_key,
                    opportunity.team_code,
                    opportunity.opponent_code,
                    opportunity.season_label,
                    opportunity.canonical_game_id,
                    opportunity.game_date,
                    opportunity.policy_name,
                    opportunity.status,
                    opportunity.prediction_value,
                    opportunity.signal_strength,
                    opportunity.evidence_rating,
                    opportunity.recommendation_status,
                    _json_dumps(opportunity.payload),
                ),
            )
            row = cursor.fetchone()
            persisted.append(
                ModelOpportunityRecord(
                    id=int(row[0]),
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
                    game_date=opportunity.game_date,
                    policy_name=opportunity.policy_name,
                    status=opportunity.status,
                    prediction_value=opportunity.prediction_value,
                    signal_strength=opportunity.signal_strength,
                    evidence_rating=opportunity.evidence_rating,
                    recommendation_status=opportunity.recommendation_status,
                    payload=opportunity.payload,
                    created_at=row[1],
                    updated_at=row[2],
                )
            )
    connection.commit()
    return persisted


def save_model_backtest_run_in_memory(
    repository: InMemoryIngestionRepository,
    backtest_run: ModelBacktestRunRecord,
) -> ModelBacktestRunRecord:
    payload = asdict(backtest_run)
    payload["id"] = len(repository.model_backtest_runs) + 1
    payload["created_at"] = datetime.now(timezone.utc)
    payload["completed_at"] = payload["created_at"]
    repository.model_backtest_runs.append(payload)
    return ModelBacktestRunRecord(**payload)


def save_model_backtest_run_postgres(
    connection: Any,
    backtest_run: ModelBacktestRunRecord,
) -> ModelBacktestRunRecord:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO model_backtest_run (
                feature_version_id,
                target_task,
                scope_team_code,
                scope_season_label,
                status,
                selection_policy_name,
                strategy_name,
                minimum_train_games,
                test_window_games,
                train_ratio,
                validation_ratio,
                fold_count,
                payload_json,
                completed_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW()
            )
            RETURNING id, created_at, completed_at
            """,
            (
                backtest_run.feature_version_id,
                backtest_run.target_task,
                backtest_run.team_code or "",
                backtest_run.season_label or "",
                backtest_run.status,
                backtest_run.selection_policy_name,
                backtest_run.strategy_name,
                backtest_run.minimum_train_games,
                backtest_run.test_window_games,
                backtest_run.train_ratio,
                backtest_run.validation_ratio,
                backtest_run.fold_count,
                _json_dumps(backtest_run.payload),
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return ModelBacktestRunRecord(
        id=int(row[0]),
        feature_version_id=backtest_run.feature_version_id,
        target_task=backtest_run.target_task,
        team_code=backtest_run.team_code,
        season_label=backtest_run.season_label,
        status=backtest_run.status,
        selection_policy_name=backtest_run.selection_policy_name,
        strategy_name=backtest_run.strategy_name,
        minimum_train_games=backtest_run.minimum_train_games,
        test_window_games=backtest_run.test_window_games,
        train_ratio=backtest_run.train_ratio,
        validation_ratio=backtest_run.validation_ratio,
        fold_count=backtest_run.fold_count,
        payload=backtest_run.payload,
        created_at=row[1],
        completed_at=row[2],
    )


def _serialize_model_backtest_run(
    backtest_run: ModelBacktestRunRecord | None,
) -> dict[str, Any] | None:
    if backtest_run is None:
        return None
    return {
        "id": backtest_run.id,
        "feature_version_id": backtest_run.feature_version_id,
        "target_task": backtest_run.target_task,
        "team_code": backtest_run.team_code,
        "season_label": backtest_run.season_label,
        "status": backtest_run.status,
        "selection_policy_name": backtest_run.selection_policy_name,
        "strategy_name": backtest_run.strategy_name,
        "minimum_train_games": backtest_run.minimum_train_games,
        "test_window_games": backtest_run.test_window_games,
        "train_ratio": backtest_run.train_ratio,
        "validation_ratio": backtest_run.validation_ratio,
        "fold_count": backtest_run.fold_count,
        "payload": backtest_run.payload,
        "created_at": backtest_run.created_at.isoformat() if backtest_run.created_at else None,
        "completed_at": (
            backtest_run.completed_at.isoformat() if backtest_run.completed_at else None
        ),
    }


def _summarize_model_scoring_history(
    scoring_runs: list[ModelScoringRunRecord],
    *,
    recent_limit: int,
) -> dict[str, Any]:
    policy_counts: dict[str, int] = {}
    daily_buckets: dict[str, dict[str, Any]] = {}
    total_prediction_count = 0
    surfaced_run_count = 0
    opportunity_status_counts = {
        "candidate_signal": 0,
        "review_manually": 0,
        "discarded": 0,
    }
    for scoring_run in scoring_runs:
        if scoring_run.policy_name is not None:
            policy_counts[scoring_run.policy_name] = (
                policy_counts.get(scoring_run.policy_name, 0) + 1
            )
        total_prediction_count += scoring_run.prediction_count
        surfaced_count = (
            scoring_run.candidate_opportunity_count + scoring_run.review_opportunity_count
        )
        if surfaced_count > 0:
            surfaced_run_count += 1
        opportunity_status_counts["candidate_signal"] += scoring_run.candidate_opportunity_count
        opportunity_status_counts["review_manually"] += scoring_run.review_opportunity_count
        opportunity_status_counts["discarded"] += scoring_run.discarded_opportunity_count
        if scoring_run.created_at is None:
            continue
        day_key = scoring_run.created_at.date().isoformat()
        bucket = daily_buckets.setdefault(
            day_key,
            {
                "date": day_key,
                "scoring_run_count": 0,
                "prediction_count": 0,
                "surfaced_run_count": 0,
                "opportunity_status_counts": {
                    "candidate_signal": 0,
                    "review_manually": 0,
                    "discarded": 0,
                },
            },
        )
        bucket["scoring_run_count"] += 1
        bucket["prediction_count"] += scoring_run.prediction_count
        if surfaced_count > 0:
            bucket["surfaced_run_count"] += 1
        bucket_status_counts = bucket["opportunity_status_counts"]
        bucket_status_counts["candidate_signal"] += scoring_run.candidate_opportunity_count
        bucket_status_counts["review_manually"] += scoring_run.review_opportunity_count
        bucket_status_counts["discarded"] += scoring_run.discarded_opportunity_count
    return {
        "overview": {
            "scoring_run_count": len(scoring_runs),
            "policy_counts": policy_counts,
            "prediction_count": total_prediction_count,
            "surfaced_run_count": surfaced_run_count,
            "opportunity_status_counts": opportunity_status_counts,
            "latest_scoring_run": _serialize_model_scoring_run(
                scoring_runs[0] if scoring_runs else None
            ),
        },
        "daily_buckets": [daily_buckets[key] for key in sorted(daily_buckets.keys())],
        "recent_scoring_runs": [
            _serialize_model_scoring_run(entry) for entry in scoring_runs[:recent_limit]
        ],
    }


def _summarize_model_opportunity_history(
    opportunities: list[ModelOpportunityRecord],
    *,
    recent_limit: int,
) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    policy_counts: dict[str, int] = {}
    rating_counts: dict[str, int] = {}
    source_kind_counts: dict[str, int] = {}
    daily_buckets: dict[str, dict[str, Any]] = {}
    for opportunity in opportunities:
        status_counts[opportunity.status] = status_counts.get(opportunity.status, 0) + 1
        policy_counts[opportunity.policy_name] = policy_counts.get(opportunity.policy_name, 0) + 1
        source_kind_counts[opportunity.source_kind] = (
            source_kind_counts.get(opportunity.source_kind, 0) + 1
        )
        if opportunity.evidence_rating is not None:
            rating_counts[opportunity.evidence_rating] = (
                rating_counts.get(opportunity.evidence_rating, 0) + 1
            )
        created_at = opportunity.created_at or opportunity.updated_at
        if created_at is None:
            continue
        day_key = created_at.date().isoformat()
        bucket = daily_buckets.setdefault(
            day_key,
            {
                "date": day_key,
                "opportunity_count": 0,
                "status_counts": {},
                "max_signal_strength": None,
            },
        )
        bucket["opportunity_count"] += 1
        bucket_status_counts = bucket["status_counts"]
        bucket_status_counts[opportunity.status] = (
            bucket_status_counts.get(opportunity.status, 0) + 1
        )
        current_max = bucket["max_signal_strength"]
        if current_max is None or float(opportunity.signal_strength) > float(current_max):
            bucket["max_signal_strength"] = opportunity.signal_strength
    return {
        "overview": {
            "opportunity_count": len(opportunities),
            "status_counts": status_counts,
            "policy_counts": policy_counts,
            "source_kind_counts": source_kind_counts,
            "evidence_rating_counts": rating_counts,
            "latest_opportunity": _serialize_model_opportunity(
                opportunities[0] if opportunities else None
            ),
        },
        "daily_buckets": [daily_buckets[key] for key in sorted(daily_buckets.keys())],
        "recent_opportunities": [
            _serialize_model_opportunity(entry) for entry in opportunities[:recent_limit]
        ],
    }


def _summarize_model_backtest_history(
    runs: list[ModelBacktestRunRecord],
    *,
    recent_limit: int,
) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    target_task_counts: dict[str, int] = {}
    strategy_counts: dict[str, int] = {}
    daily_buckets: dict[str, dict[str, Any]] = {}
    best_run = None
    best_roi = None
    for run in runs:
        status_counts[run.status] = status_counts.get(run.status, 0) + 1
        target_task_counts[run.target_task] = target_task_counts.get(run.target_task, 0) + 1
        strategy_counts[run.strategy_name] = strategy_counts.get(run.strategy_name, 0) + 1
        candidate_roi = _nested_get(
            run.payload,
            "strategy_results",
            "candidate_threshold",
            "roi",
        )
        if candidate_roi is not None and (
            best_roi is None or float(candidate_roi) > float(best_roi)
        ):
            best_roi = candidate_roi
            best_run = run
        created_at = run.completed_at or run.created_at
        if created_at is None:
            continue
        day_key = created_at.date().isoformat()
        bucket = daily_buckets.setdefault(
            day_key,
            {
                "date": day_key,
                "run_count": 0,
                "fold_count": 0,
                "bet_count": 0,
                "profit_units": 0.0,
            },
        )
        bucket["run_count"] += 1
        bucket["fold_count"] += int(run.fold_count)
        bucket["bet_count"] += int(
            _nested_get(run.payload, "strategy_results", "candidate_threshold", "bet_count") or 0
        )
        bucket["profit_units"] = round(
            float(bucket["profit_units"])
            + float(
                _nested_get(run.payload, "strategy_results", "candidate_threshold", "profit_units")
                or 0.0
            ),
            4,
        )
    return {
        "overview": {
            "run_count": len(runs),
            "status_counts": status_counts,
            "target_task_counts": target_task_counts,
            "strategy_counts": strategy_counts,
            "best_candidate_threshold_run": _serialize_model_backtest_run(best_run),
            "latest_run": _serialize_model_backtest_run(runs[0] if runs else None),
        },
        "daily_buckets": [daily_buckets[key] for key in sorted(daily_buckets.keys())],
        "recent_runs": [_serialize_model_backtest_run(entry) for entry in runs[:recent_limit]],
    }


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
        "recent_runs": [
            _serialize_model_training_run(run)
            for run in runs[:recent_limit]
        ],
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


def get_model_selection_history_in_memory(
    repository: InMemoryIngestionRepository,
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


def _model_run_rank_key(run: ModelTrainingRunRecord) -> tuple[float, int]:
    validation_metrics = run.metrics.get("validation", {})
    return (
        1 if run.artifact.get("fallback_strategy") is not None else 0,
        _metric_value_or_inf(validation_metrics.get("mae")),
        -int(validation_metrics.get("prediction_count", 0)),
    )


def _model_run_rank_key_object(run: dict[str, Any]) -> tuple[float, int]:
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
        "validation_metric_value": _float_or_none(run.metrics.get("validation", {}).get("mae")),
        "test_metric_value": _float_or_none(run.metrics.get("test", {}).get("mae")),
        "validation_prediction_count": int(
            run.metrics.get("validation", {}).get("prediction_count", 0)
        ),
        "test_prediction_count": int(
            run.metrics.get("test", {}).get("prediction_count", 0)
        ),
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


def save_model_evaluation_snapshot_in_memory(
    repository: InMemoryIngestionRepository,
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
                validation_metric_value,
                test_metric_value,
                validation_prediction_count,
                test_prediction_count,
                snapshot_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (model_training_run_id)
            DO UPDATE SET
                model_registry_id = EXCLUDED.model_registry_id,
                feature_version_id = EXCLUDED.feature_version_id,
                target_task = EXCLUDED.target_task,
                model_family = EXCLUDED.model_family,
                selected_feature = EXCLUDED.selected_feature,
                fallback_strategy = EXCLUDED.fallback_strategy,
                primary_metric_name = EXCLUDED.primary_metric_name,
                validation_metric_value = EXCLUDED.validation_metric_value,
                test_metric_value = EXCLUDED.test_metric_value,
                validation_prediction_count = EXCLUDED.validation_prediction_count,
                test_prediction_count = EXCLUDED.test_prediction_count,
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
                payload["validation_metric_value"],
                payload["test_metric_value"],
                payload["validation_prediction_count"],
                payload["test_prediction_count"],
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
    repository: InMemoryIngestionRepository,
    snapshot: ModelEvaluationSnapshotRecord,
    *,
    selection_policy_name: str,
) -> ModelSelectionSnapshotRecord:
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
        "selection_policy_name": selection_policy_name,
        "rationale": {
            "primary_metric_name": snapshot.primary_metric_name,
            "validation_metric_value": snapshot.validation_metric_value,
            "fallback_strategy": snapshot.fallback_strategy,
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
    rationale = {
        "primary_metric_name": snapshot.primary_metric_name,
        "validation_metric_value": snapshot.validation_metric_value,
        "fallback_strategy": snapshot.fallback_strategy,
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
                selection_policy_name,
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
        selection_policy_name=selection_policy_name,
        rationale=rationale,
        is_active=True,
        created_at=row[1],
    )


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _positive_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    integer_value = int(value)
    return integer_value if integer_value > 0 else None


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


def _build_model_key(
    *,
    target_task: str,
    model_family: str,
    team_code: str | None,
) -> str:
    scope = team_code.lower() if team_code is not None else "global"
    return f"{target_task}_{model_family}_{scope}_v1"
