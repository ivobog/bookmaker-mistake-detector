from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from bookmaker_detector_api.config import settings
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
MARKET_BOARD_SOURCE_CONFIGS = {
    "demo_daily_lines_v1": {
        "description": (
            "Deterministic built-in upcoming slate source for local scoring "
            "and refresh flows."
        ),
        "default_game_count": 2,
        "supports_refresh": True,
    },
    "demo_source_failure_v1": {
        "description": (
            "Intentional failing source provider for refresh diagnostics and "
            "error-path testing."
        ),
        "default_game_count": 2,
        "supports_refresh": True,
        "demo_failure": True,
    },
    "demo_partial_lines_v1": {
        "description": (
            "Mixed-quality source provider with valid and invalid rows for "
            "normalization and validation-path testing."
        ),
        "default_game_count": 3,
        "supports_refresh": True,
        "mixed_quality_demo": True,
    },
    "file_market_board_v1": {
        "description": (
            "File-backed market board source that loads upcoming games from a "
            "JSON or CSV file path."
        ),
        "default_game_count": 0,
        "supports_refresh": True,
        "requires_source_path": True,
        "supported_formats": ["json", "csv"],
    },
    "the_odds_api_v4_nba": {
        "description": (
            "External NBA odds source backed by The Odds API v4 upcoming odds "
            "endpoint."
        ),
        "default_game_count": 0,
        "supports_refresh": True,
        "requires_api_key": True,
        "requires_network": True,
    },
}
DEMO_MARKET_BOARD_GAME_TEMPLATES = [
    {
        "home_team_code": "LAL",
        "away_team_code": "BOS",
        "home_spread_line": -3.5,
        "total_line": 228.5,
    },
    {
        "home_team_code": "NYK",
        "away_team_code": "MIA",
        "home_spread_line": -1.5,
        "total_line": 219.5,
    },
    {
        "home_team_code": "GSW",
        "away_team_code": "DEN",
        "home_spread_line": 2.0,
        "total_line": 231.0,
    },
    {
        "home_team_code": "PHX",
        "away_team_code": "DAL",
        "home_spread_line": 1.5,
        "total_line": 226.5,
    },
]


@dataclass(slots=True)
class ModelRegistryRecord:
    id: int
    model_key: str
    target_task: str
    model_family: str
    version_label: str
    description: str
    config: dict[str, Any]
    created_at: datetime | None = None


@dataclass(slots=True)
class ModelTrainingRunRecord:
    id: int
    model_registry_id: int
    feature_version_id: int
    target_task: str
    team_code: str | None
    season_label: str | None
    status: str
    train_ratio: float
    validation_ratio: float
    artifact: dict[str, Any]
    metrics: dict[str, Any]
    created_at: datetime | None = None
    completed_at: datetime | None = None


@dataclass(slots=True)
class ModelEvaluationSnapshotRecord:
    id: int
    model_training_run_id: int
    model_registry_id: int
    feature_version_id: int
    target_task: str
    model_family: str
    selected_feature: str | None
    fallback_strategy: str | None
    primary_metric_name: str
    validation_metric_value: float | None
    test_metric_value: float | None
    validation_prediction_count: int
    test_prediction_count: int
    snapshot: dict[str, Any]
    created_at: datetime | None = None


@dataclass(slots=True)
class ModelSelectionSnapshotRecord:
    id: int
    model_evaluation_snapshot_id: int
    model_training_run_id: int
    model_registry_id: int
    feature_version_id: int
    target_task: str
    model_family: str
    selection_policy_name: str
    rationale: dict[str, Any]
    is_active: bool
    created_at: datetime | None = None


@dataclass(slots=True)
class ModelMarketBoardRecord:
    id: int
    board_key: str
    slate_label: str | None
    target_task: str
    season_label: str | None
    game_count: int
    game_date_start: date | None
    game_date_end: date | None
    payload: dict[str, Any]
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(slots=True)
class ModelMarketBoardRefreshRecord:
    id: int
    model_market_board_id: int
    board_key: str
    target_task: str
    source_name: str
    refresh_status: str
    game_count: int
    payload: dict[str, Any]
    created_at: datetime | None = None


@dataclass(slots=True)
class ModelMarketBoardSourceRunRecord:
    id: int
    source_name: str
    target_task: str
    season_label: str
    game_date: date
    slate_label: str | None
    requested_game_count: int
    generated_game_count: int
    status: str
    payload: dict[str, Any]
    created_at: datetime | None = None


MarketBoardSourceProvider = Callable[..., list[dict[str, Any]]]


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


@dataclass(slots=True)
class ModelMarketBoardRefreshBatchRecord:
    id: int
    target_task: str
    source_name: str | None
    season_label: str | None
    freshness_status: str | None
    pending_only: bool
    candidate_board_count: int
    refreshed_board_count: int
    created_board_count: int
    updated_board_count: int
    unchanged_board_count: int
    payload: dict[str, Any]
    created_at: datetime | None = None


@dataclass(slots=True)
class ModelScoringRunRecord:
    id: int
    model_market_board_id: int | None
    model_selection_snapshot_id: int | None
    model_evaluation_snapshot_id: int | None
    feature_version_id: int
    target_task: str
    scenario_key: str
    season_label: str
    game_date: date
    home_team_code: str
    away_team_code: str
    home_spread_line: float | None
    total_line: float | None
    policy_name: str | None
    prediction_count: int
    candidate_opportunity_count: int
    review_opportunity_count: int
    discarded_opportunity_count: int
    payload: dict[str, Any]
    created_at: datetime | None = None


@dataclass(slots=True)
class ModelMarketBoardScoringBatchRecord:
    id: int
    target_task: str
    source_name: str | None
    season_label: str | None
    freshness_status: str | None
    pending_only: bool
    candidate_board_count: int
    scored_board_count: int
    materialized_scoring_run_count: int
    materialized_opportunity_count: int
    payload: dict[str, Any]
    created_at: datetime | None = None


@dataclass(slots=True)
class ModelMarketBoardCadenceBatchRecord:
    id: int
    target_task: str
    source_name: str | None
    season_label: str | None
    refresh_freshness_status: str | None
    scoring_freshness_status: str | None
    refreshed_board_count: int
    scored_board_count: int
    materialized_scoring_run_count: int
    materialized_opportunity_count: int
    payload: dict[str, Any]
    created_at: datetime | None = None


@dataclass(slots=True)
class ModelOpportunityRecord:
    id: int
    model_scoring_run_id: int | None
    model_selection_snapshot_id: int | None
    model_evaluation_snapshot_id: int | None
    feature_version_id: int
    target_task: str
    source_kind: str
    scenario_key: str | None
    opportunity_key: str
    team_code: str
    opponent_code: str
    season_label: str
    canonical_game_id: int | None
    game_date: date
    policy_name: str
    status: str
    prediction_value: float
    signal_strength: float
    evidence_rating: str | None
    recommendation_status: str | None
    payload: dict[str, Any]
    created_at: datetime | None = None
    updated_at: datetime | None = None


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


def ensure_model_tables(connection: Any) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS model_registry (
                id BIGSERIAL PRIMARY KEY,
                model_key VARCHAR(128) NOT NULL UNIQUE,
                target_task VARCHAR(64) NOT NULL,
                model_family VARCHAR(64) NOT NULL,
                version_label VARCHAR(128) NOT NULL,
                description TEXT,
                config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS model_training_run (
                id BIGSERIAL PRIMARY KEY,
                model_registry_id BIGINT NOT NULL REFERENCES model_registry(id) ON DELETE CASCADE,
                feature_version_id BIGINT NOT NULL REFERENCES feature_version(id) ON DELETE CASCADE,
                target_task VARCHAR(64) NOT NULL,
                scope_team_code VARCHAR(16) NOT NULL DEFAULT '',
                scope_season_label VARCHAR(32) NOT NULL DEFAULT '',
                status VARCHAR(32) NOT NULL,
                train_ratio DOUBLE PRECISION NOT NULL,
                validation_ratio DOUBLE PRECISION NOT NULL,
                artifact_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                completed_at TIMESTAMPTZ
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS model_evaluation_snapshot (
                id BIGSERIAL PRIMARY KEY,
                model_training_run_id BIGINT NOT NULL UNIQUE
                    REFERENCES model_training_run(id) ON DELETE CASCADE,
                model_registry_id BIGINT NOT NULL REFERENCES model_registry(id) ON DELETE CASCADE,
                feature_version_id BIGINT NOT NULL REFERENCES feature_version(id) ON DELETE CASCADE,
                target_task VARCHAR(64) NOT NULL,
                model_family VARCHAR(64) NOT NULL,
                selected_feature VARCHAR(128),
                fallback_strategy VARCHAR(64),
                primary_metric_name VARCHAR(32) NOT NULL,
                validation_metric_value DOUBLE PRECISION,
                test_metric_value DOUBLE PRECISION,
                validation_prediction_count INTEGER NOT NULL DEFAULT 0,
                test_prediction_count INTEGER NOT NULL DEFAULT 0,
                snapshot_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS model_selection_snapshot (
                id BIGSERIAL PRIMARY KEY,
                model_evaluation_snapshot_id BIGINT NOT NULL
                    REFERENCES model_evaluation_snapshot(id) ON DELETE CASCADE,
                model_training_run_id BIGINT NOT NULL
                    REFERENCES model_training_run(id) ON DELETE CASCADE,
                model_registry_id BIGINT NOT NULL REFERENCES model_registry(id) ON DELETE CASCADE,
                feature_version_id BIGINT NOT NULL REFERENCES feature_version(id) ON DELETE CASCADE,
                target_task VARCHAR(64) NOT NULL,
                model_family VARCHAR(64) NOT NULL,
                selection_policy_name VARCHAR(64) NOT NULL,
                rationale_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS model_market_board (
                id BIGSERIAL PRIMARY KEY,
                board_key VARCHAR(255) NOT NULL UNIQUE,
                slate_label VARCHAR(128),
                target_task VARCHAR(64) NOT NULL,
                season_label VARCHAR(32),
                game_count INTEGER NOT NULL DEFAULT 0,
                game_date_start DATE,
                game_date_end DATE,
                payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS model_market_board_refresh_event (
                id BIGSERIAL PRIMARY KEY,
                model_market_board_id BIGINT NOT NULL
                    REFERENCES model_market_board(id) ON DELETE CASCADE,
                board_key VARCHAR(255) NOT NULL,
                target_task VARCHAR(64) NOT NULL,
                source_name VARCHAR(64) NOT NULL,
                refresh_status VARCHAR(32) NOT NULL,
                game_count INTEGER NOT NULL DEFAULT 0,
                payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS model_market_board_source_run (
                id BIGSERIAL PRIMARY KEY,
                source_name VARCHAR(64) NOT NULL,
                target_task VARCHAR(64) NOT NULL,
                season_label VARCHAR(32) NOT NULL,
                game_date DATE NOT NULL,
                slate_label VARCHAR(128),
                requested_game_count INTEGER NOT NULL DEFAULT 0,
                generated_game_count INTEGER NOT NULL DEFAULT 0,
                status VARCHAR(32) NOT NULL,
                payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS model_scoring_run (
                id BIGSERIAL PRIMARY KEY,
                model_market_board_id BIGINT
                    REFERENCES model_market_board(id) ON DELETE SET NULL,
                model_selection_snapshot_id BIGINT
                    REFERENCES model_selection_snapshot(id) ON DELETE SET NULL,
                model_evaluation_snapshot_id BIGINT
                    REFERENCES model_evaluation_snapshot(id) ON DELETE SET NULL,
                feature_version_id BIGINT NOT NULL REFERENCES feature_version(id) ON DELETE CASCADE,
                target_task VARCHAR(64) NOT NULL,
                season_label VARCHAR(32) NOT NULL,
                scenario_key VARCHAR(255) NOT NULL,
                game_date DATE NOT NULL,
                home_team_code VARCHAR(16) NOT NULL,
                away_team_code VARCHAR(16) NOT NULL,
                home_spread_line DOUBLE PRECISION,
                total_line DOUBLE PRECISION,
                policy_name VARCHAR(64),
                prediction_count INTEGER NOT NULL DEFAULT 0,
                candidate_opportunity_count INTEGER NOT NULL DEFAULT 0,
                review_opportunity_count INTEGER NOT NULL DEFAULT 0,
                discarded_opportunity_count INTEGER NOT NULL DEFAULT 0,
                payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cursor.execute(
            """
            ALTER TABLE model_scoring_run
            ADD COLUMN IF NOT EXISTS model_market_board_id BIGINT
                REFERENCES model_market_board(id) ON DELETE SET NULL
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS model_market_board_refresh_batch (
                id BIGSERIAL PRIMARY KEY,
                target_task VARCHAR(64) NOT NULL,
                source_name VARCHAR(64),
                season_label VARCHAR(32),
                freshness_status VARCHAR(32),
                pending_only BOOLEAN NOT NULL DEFAULT TRUE,
                candidate_board_count INTEGER NOT NULL DEFAULT 0,
                refreshed_board_count INTEGER NOT NULL DEFAULT 0,
                created_board_count INTEGER NOT NULL DEFAULT 0,
                updated_board_count INTEGER NOT NULL DEFAULT 0,
                unchanged_board_count INTEGER NOT NULL DEFAULT 0,
                payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS model_market_board_scoring_batch (
                id BIGSERIAL PRIMARY KEY,
                target_task VARCHAR(64) NOT NULL,
                source_name VARCHAR(64),
                season_label VARCHAR(32),
                freshness_status VARCHAR(32),
                pending_only BOOLEAN NOT NULL DEFAULT TRUE,
                candidate_board_count INTEGER NOT NULL DEFAULT 0,
                scored_board_count INTEGER NOT NULL DEFAULT 0,
                materialized_scoring_run_count INTEGER NOT NULL DEFAULT 0,
                materialized_opportunity_count INTEGER NOT NULL DEFAULT 0,
                payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS model_market_board_cadence_batch (
                id BIGSERIAL PRIMARY KEY,
                target_task VARCHAR(64) NOT NULL,
                source_name VARCHAR(64),
                season_label VARCHAR(32),
                refresh_freshness_status VARCHAR(32),
                scoring_freshness_status VARCHAR(32),
                refreshed_board_count INTEGER NOT NULL DEFAULT 0,
                scored_board_count INTEGER NOT NULL DEFAULT 0,
                materialized_scoring_run_count INTEGER NOT NULL DEFAULT 0,
                materialized_opportunity_count INTEGER NOT NULL DEFAULT 0,
                payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS model_opportunity (
                id BIGSERIAL PRIMARY KEY,
                model_scoring_run_id BIGINT
                    REFERENCES model_scoring_run(id) ON DELETE SET NULL,
                model_selection_snapshot_id BIGINT
                    REFERENCES model_selection_snapshot(id) ON DELETE SET NULL,
                model_evaluation_snapshot_id BIGINT
                    REFERENCES model_evaluation_snapshot(id) ON DELETE SET NULL,
                feature_version_id BIGINT NOT NULL REFERENCES feature_version(id) ON DELETE CASCADE,
                target_task VARCHAR(64) NOT NULL,
                source_kind VARCHAR(32) NOT NULL DEFAULT 'historical_game',
                scenario_key VARCHAR(255),
                season_label VARCHAR(32) NOT NULL,
                opportunity_key VARCHAR(255) NOT NULL UNIQUE,
                game_date DATE NOT NULL,
                team_code VARCHAR(16) NOT NULL,
                opponent_code VARCHAR(16) NOT NULL,
                canonical_game_id BIGINT,
                policy_name VARCHAR(64) NOT NULL,
                status VARCHAR(32) NOT NULL,
                prediction_value DOUBLE PRECISION NOT NULL,
                signal_strength DOUBLE PRECISION NOT NULL,
                evidence_rating VARCHAR(32),
                recommendation_status VARCHAR(32),
                payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cursor.execute(
            """
            ALTER TABLE model_opportunity
            ADD COLUMN IF NOT EXISTS model_scoring_run_id BIGINT
                REFERENCES model_scoring_run(id) ON DELETE SET NULL
            """
        )
        cursor.execute(
            """
            ALTER TABLE model_opportunity
            ADD COLUMN IF NOT EXISTS source_kind VARCHAR(32) NOT NULL DEFAULT 'historical_game'
            """
        )
        cursor.execute(
            """
            ALTER TABLE model_opportunity
            ADD COLUMN IF NOT EXISTS scenario_key VARCHAR(255)
            """
        )
        cursor.execute(
            """
            ALTER TABLE model_opportunity
            ALTER COLUMN canonical_game_id DROP NOT NULL
            """
        )
    connection.commit()


def ensure_model_registry_postgres(
    connection: Any,
    *,
    target_task: str,
    model_family: str,
    team_code: str | None,
) -> ModelRegistryRecord:
    ensure_model_tables(connection)
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
    ensure_model_tables(connection)
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
    ensure_model_tables(connection)
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
    ensure_model_tables(connection)
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


def list_model_evaluation_snapshots_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    model_family: str | None = None,
) -> list[ModelEvaluationSnapshotRecord]:
    ensure_model_tables(connection)
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


def list_model_selection_snapshots_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    active_only: bool = False,
) -> list[ModelSelectionSnapshotRecord]:
    ensure_model_tables(connection)
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
    ensure_model_tables(connection)
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


def list_model_market_board_sources() -> dict[str, Any]:
    return {
        "sources": [
            {
                "source_name": source_name,
                **config,
            }
            for source_name, config in MARKET_BOARD_SOURCE_CONFIGS.items()
        ]
    }


def _build_demo_daily_lines_source_games(
    *,
    season_label: str,
    game_date: date,
    game_count: int | None = None,
    source_path: str | None = None,
) -> list[dict[str, Any]]:
    del source_path
    resolved_count = game_count or int(
        MARKET_BOARD_SOURCE_CONFIGS["demo_daily_lines_v1"]["default_game_count"]
    )
    template_count = len(DEMO_MARKET_BOARD_GAME_TEMPLATES)
    start_index = game_date.toordinal() % template_count
    games = []
    for offset in range(resolved_count):
        template = DEMO_MARKET_BOARD_GAME_TEMPLATES[(start_index + offset) % template_count]
        games.append(
            {
                "season_label": season_label,
                "game_date": game_date,
                "home_team_code": template["home_team_code"],
                "away_team_code": template["away_team_code"],
                "home_spread_line": template["home_spread_line"],
                "total_line": template["total_line"],
            }
        )
    return games


def _build_demo_failing_source_games(
    *,
    season_label: str,
    game_date: date,
    game_count: int | None = None,
    source_path: str | None = None,
) -> list[dict[str, Any]]:
    del season_label, game_date, game_count, source_path
    raise RuntimeError(
        "Demo market-board source failure triggered intentionally for diagnostics."
    )


def _build_demo_partial_source_games(
    *,
    season_label: str,
    game_date: date,
    game_count: int | None = None,
    source_path: str | None = None,
) -> list[dict[str, Any]]:
    del game_count, source_path
    return [
        {
            "season_label": season_label,
            "game_date": game_date,
            "home_team_code": "lal",
            "away_team_code": "bos",
            "home_spread_line": "-3.5",
            "total_line": "228.5",
        },
        {
            "season_label": season_label,
            "game_date": game_date,
            "home_team_code": "NYK",
            "away_team_code": "NYK",
            "home_spread_line": "-1.5",
            "total_line": "219.5",
        },
        {
            "season_label": season_label,
            "game_date": "not-a-date",
            "home_team_code": "GSW",
            "away_team_code": "DEN",
            "home_spread_line": "bad-number",
            "total_line": "231.0",
        },
    ]


def _resolve_market_board_source_path(source_path: str) -> Path:
    if source_path.startswith("fixture://"):
        fixture_name = source_path.removeprefix("fixture://")
        return (
            Path(__file__).resolve().parents[1] / "fixtures" / fixture_name
        ).resolve()
    return Path(source_path).expanduser().resolve()


def _build_file_market_board_source_games(
    *,
    season_label: str,
    game_date: date,
    game_count: int | None = None,
    source_path: str | None = None,
) -> list[dict[str, Any]]:
    del game_count
    if source_path is None:
        raise ValueError("file_market_board_v1 requires source_path.")
    resolved_path = _resolve_market_board_source_path(source_path)
    if not resolved_path.exists():
        raise FileNotFoundError(f"Market board source file not found: {resolved_path}")
    suffix = resolved_path.suffix.lower()
    if suffix == ".json":
        payload = json.loads(resolved_path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError("JSON market board source must contain a list of games.")
        rows = payload
    elif suffix == ".csv":
        with resolved_path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
    else:
        raise ValueError(
            "Unsupported market board source file format. Expected .json or .csv."
        )
    games = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Each market board source row must be an object.")
        payload = dict(row)
        payload.setdefault("season_label", season_label)
        payload.setdefault("game_date", game_date)
        games.append(payload)
    return games


def _fetch_the_odds_api_games() -> list[dict[str, Any]]:
    if not settings.the_odds_api_key:
        raise ValueError("the_odds_api_v4_nba requires THE_ODDS_API_KEY.")
    params = {
        "apiKey": settings.the_odds_api_key,
        "regions": settings.the_odds_api_regions,
        "markets": settings.the_odds_api_markets,
        "oddsFormat": settings.the_odds_api_odds_format,
    }
    if settings.the_odds_api_bookmakers:
        params["bookmakers"] = settings.the_odds_api_bookmakers
    base_url = settings.the_odds_api_base_url.rstrip("/")
    request_url = (
        f"{base_url}/sports/{settings.the_odds_api_sport_key}/odds?{urlencode(params)}"
    )
    request = Request(
        request_url,
        headers={
            "Accept": "application/json",
            "User-Agent": "bookmaker-mistake-detector/0.1.0",
        },
    )
    try:
        with urlopen(request, timeout=settings.the_odds_api_timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(
            f"The Odds API request failed with status {exc.code}: {body or exc.reason}"
        ) from exc
    except URLError as exc:
        raise RuntimeError(f"The Odds API request failed: {exc.reason}") from exc
    if not isinstance(payload, list):
        raise ValueError("The Odds API response must be a list of event objects.")
    return payload


def _extract_the_odds_api_game(
    *,
    season_label: str,
    event: dict[str, Any],
) -> dict[str, Any]:
    home_team = str(event.get("home_team") or "").strip().upper()
    away_team = str(event.get("away_team") or "").strip().upper()
    commence_time_raw = event.get("commence_time")
    if not commence_time_raw:
        raise ValueError("The Odds API event is missing commence_time.")
    commence_time = datetime.fromisoformat(
        str(commence_time_raw).replace("Z", "+00:00")
    ).astimezone(timezone.utc)
    spread_points: list[float] = []
    total_points: list[float] = []
    for bookmaker in event.get("bookmakers", []):
        if not isinstance(bookmaker, dict):
            continue
        for market in bookmaker.get("markets", []):
            if not isinstance(market, dict):
                continue
            market_key = str(market.get("key") or "")
            outcomes = market.get("outcomes", [])
            if market_key == "spreads":
                for outcome in outcomes:
                    if not isinstance(outcome, dict):
                        continue
                    if str(outcome.get("name") or "").strip().upper() != home_team:
                        continue
                    point = _float_or_none(outcome.get("point"))
                    if point is not None:
                        spread_points.append(point)
            if market_key == "totals":
                for outcome in outcomes:
                    if not isinstance(outcome, dict):
                        continue
                    if str(outcome.get("name") or "").strip().lower() != "over":
                        continue
                    point = _float_or_none(outcome.get("point"))
                    if point is not None:
                        total_points.append(point)
    return {
        "season_label": season_label,
        "game_date": commence_time.date(),
        "home_team_code": home_team,
        "away_team_code": away_team,
        "home_spread_line": median(spread_points) if spread_points else None,
        "total_line": median(total_points) if total_points else None,
    }


def _build_the_odds_api_source_games(
    *,
    season_label: str,
    game_date: date,
    game_count: int | None = None,
    source_path: str | None = None,
) -> list[dict[str, Any]]:
    del game_date, source_path
    events = _fetch_the_odds_api_games()
    games = [
        _extract_the_odds_api_game(season_label=season_label, event=event)
        for event in events
        if isinstance(event, dict)
    ]
    if game_count is not None and game_count > 0:
        return games[:game_count]
    return games


MARKET_BOARD_SOURCE_PROVIDERS: dict[str, MarketBoardSourceProvider] = {
    "demo_daily_lines_v1": _build_demo_daily_lines_source_games,
    "demo_source_failure_v1": _build_demo_failing_source_games,
    "demo_partial_lines_v1": _build_demo_partial_source_games,
    "file_market_board_v1": _build_file_market_board_source_games,
    "the_odds_api_v4_nba": _build_the_odds_api_source_games,
}


def _resolve_market_board_source_provider(source_name: str) -> MarketBoardSourceProvider:
    if source_name not in MARKET_BOARD_SOURCE_CONFIGS:
        raise ValueError(f"Unsupported market board source: {source_name}")
    provider = MARKET_BOARD_SOURCE_PROVIDERS.get(source_name)
    if provider is None:
        raise ValueError(f"No provider registered for market board source: {source_name}")
    return provider


def _run_market_board_source_provider(
    *,
    source_name: str,
    season_label: str,
    game_date: date,
    game_count: int | None = None,
    source_path: str | None = None,
) -> list[dict[str, Any]]:
    provider = _resolve_market_board_source_provider(source_name)
    return provider(
        season_label=season_label,
        game_date=game_date,
        game_count=game_count,
        source_path=source_path,
    )


def _normalize_market_board_source_games(
    *,
    source_name: str,
    season_label: str,
    game_date: date,
    raw_games: list[dict[str, Any]],
) -> dict[str, Any]:
    normalized_games: list[dict[str, Any]] = []
    invalid_entries: list[dict[str, Any]] = []
    warning_entries: list[dict[str, Any]] = []
    for row_index, raw_game in enumerate(raw_games):
        issues: list[str] = []
        raw_payload = dict(raw_game)
        resolved_season_label = str(raw_game.get("season_label") or season_label).strip()
        resolved_home_team = str(raw_game.get("home_team_code") or "").strip().upper()
        resolved_away_team = str(raw_game.get("away_team_code") or "").strip().upper()
        try:
            resolved_game_date = _coerce_date(raw_game.get("game_date") or game_date)
        except Exception:
            issues.append("invalid_game_date")
            resolved_game_date = None
        if not resolved_season_label:
            issues.append("missing_season_label")
        if not resolved_home_team:
            issues.append("missing_home_team_code")
        if not resolved_away_team:
            issues.append("missing_away_team_code")
        if resolved_home_team and resolved_away_team and resolved_home_team == resolved_away_team:
            issues.append("duplicate_team_codes")

        normalized_home_spread_line = None
        if raw_game.get("home_spread_line") is not None:
            try:
                normalized_home_spread_line = _float_or_none(raw_game.get("home_spread_line"))
            except Exception:
                issues.append("invalid_home_spread_line")
        normalized_total_line = None
        if raw_game.get("total_line") is not None:
            try:
                normalized_total_line = _float_or_none(raw_game.get("total_line"))
            except Exception:
                issues.append("invalid_total_line")

        if issues:
            invalid_entries.append(
                {
                    "row_index": row_index,
                    "issues": issues,
                    "raw_game": raw_payload,
                }
            )
            continue

        normalized_game = {
            "season_label": resolved_season_label,
            "game_date": resolved_game_date,
            "home_team_code": resolved_home_team,
            "away_team_code": resolved_away_team,
            "home_spread_line": normalized_home_spread_line,
            "total_line": normalized_total_line,
        }
        serialized_normalized_game = _serialize_future_game_input(normalized_game)
        if _normalize_source_payload_value(raw_payload) != _normalize_source_payload_value(
            serialized_normalized_game
        ):
            warning_entries.append(
                {
                    "row_index": row_index,
                    "warning_type": "normalized_fields",
                    "raw_game": raw_payload,
                    "normalized_game": serialized_normalized_game,
                }
            )
        normalized_games.append(normalized_game)

    validation_summary = {
        "source_name": source_name,
        "raw_row_count": len(raw_games),
        "valid_row_count": len(normalized_games),
        "invalid_row_count": len(invalid_entries),
        "warning_count": len(warning_entries),
        "invalid_entries": invalid_entries,
        "warnings": warning_entries,
    }
    return {
        "normalized_games": normalized_games,
        "validation_summary": validation_summary,
    }


def _normalize_source_payload_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _normalize_source_payload_value(value[key])
            for key in sorted(value.keys(), key=str)
        }
    if isinstance(value, list):
        return [_normalize_source_payload_value(entry) for entry in value]
    if isinstance(value, date):
        return value.isoformat()
    return value


def _build_market_board_source_payload_fingerprints(
    *,
    raw_games: list[dict[str, Any]],
    normalized_games: list[dict[str, Any]],
) -> dict[str, Any]:
    raw_payload = _normalize_source_payload_value(raw_games)
    normalized_payload = _normalize_source_payload_value(
        [_serialize_future_game_input(game) for game in normalized_games]
    )
    raw_serialized = _json_dumps(raw_payload)
    normalized_serialized = _json_dumps(normalized_payload)
    return {
        "raw_game_count": len(raw_games),
        "normalized_game_count": len(normalized_games),
        "raw_payload_sha256": hashlib.sha256(raw_serialized.encode("utf-8")).hexdigest(),
        "normalized_payload_sha256": hashlib.sha256(
            normalized_serialized.encode("utf-8")
        ).hexdigest(),
    }


def _build_market_board_source_request_context(
    *,
    source_name: str,
    source_path: str | None,
) -> dict[str, Any] | None:
    if source_name == "file_market_board_v1":
        return {"source_path": source_path}
    if source_name == "the_odds_api_v4_nba":
        return {
            "base_url": settings.the_odds_api_base_url,
            "sport_key": settings.the_odds_api_sport_key,
            "regions": settings.the_odds_api_regions,
            "markets": settings.the_odds_api_markets,
            "odds_format": settings.the_odds_api_odds_format,
            "bookmakers": settings.the_odds_api_bookmakers,
        }
    return None


def _build_market_board_source_fingerprint_comparison(
    *,
    existing_board: ModelMarketBoardRecord | None,
    current_fingerprints: dict[str, Any],
) -> dict[str, Any]:
    source_payload = existing_board.payload.get("source", {}) if existing_board else {}
    previous_fingerprints = source_payload.get("source_payload_fingerprints")
    if not isinstance(previous_fingerprints, dict):
        return {
            "previous_fingerprints_available": False,
            "raw_payload_changed": None,
            "normalized_payload_changed": None,
            "raw_changed_but_normalized_same": None,
        }
    previous_raw_hash = previous_fingerprints.get("raw_payload_sha256")
    previous_normalized_hash = previous_fingerprints.get("normalized_payload_sha256")
    current_raw_hash = current_fingerprints.get("raw_payload_sha256")
    current_normalized_hash = current_fingerprints.get("normalized_payload_sha256")
    raw_payload_changed = previous_raw_hash != current_raw_hash
    normalized_payload_changed = previous_normalized_hash != current_normalized_hash
    return {
        "previous_fingerprints_available": True,
        "previous_raw_payload_sha256": previous_raw_hash,
        "previous_normalized_payload_sha256": previous_normalized_hash,
        "current_raw_payload_sha256": current_raw_hash,
        "current_normalized_payload_sha256": current_normalized_hash,
        "raw_payload_changed": raw_payload_changed,
        "normalized_payload_changed": normalized_payload_changed,
        "raw_changed_but_normalized_same": raw_payload_changed and not normalized_payload_changed,
    }


def list_model_market_board_source_runs_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    season_label: str | None = None,
    recent_limit: int | None = None,
) -> list[ModelMarketBoardSourceRunRecord]:
    selected = [
        ModelMarketBoardSourceRunRecord(**entry)
        for entry in repository.model_market_board_source_runs
        if (target_task is None or entry["target_task"] == target_task)
        and (source_name is None or entry["source_name"] == source_name)
        and (season_label is None or entry["season_label"] == season_label)
    ]
    ordered = sorted(
        selected,
        key=lambda entry: (
            entry.created_at or datetime.min.replace(tzinfo=timezone.utc),
            entry.id,
        ),
        reverse=True,
    )
    if recent_limit is None:
        return ordered
    return ordered[:recent_limit]


def list_model_market_board_source_runs_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    season_label: str | None = None,
    recent_limit: int | None = None,
) -> list[ModelMarketBoardSourceRunRecord]:
    ensure_model_tables(connection)
    query = """
        SELECT
            id,
            source_name,
            target_task,
            season_label,
            game_date,
            slate_label,
            requested_game_count,
            generated_game_count,
            status,
            payload_json,
            created_at
        FROM model_market_board_source_run
        WHERE 1=1
    """
    params: list[Any] = []
    if target_task is not None:
        query += " AND target_task = %s"
        params.append(target_task)
    if source_name is not None:
        query += " AND source_name = %s"
        params.append(source_name)
    if season_label is not None:
        query += " AND season_label = %s"
        params.append(season_label)
    query += " ORDER BY created_at DESC, id DESC"
    if recent_limit is not None:
        query += " LIMIT %s"
        params.append(recent_limit)
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelMarketBoardSourceRunRecord(
            id=int(row[0]),
            source_name=row[1],
            target_task=row[2],
            season_label=row[3],
            game_date=row[4],
            slate_label=row[5],
            requested_game_count=int(row[6]),
            generated_game_count=int(row[7]),
            status=row[8],
            payload=row[9],
            created_at=row[10],
        )
        for row in rows
    ]


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


def list_model_market_boards_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str | None = None,
    season_label: str | None = None,
) -> list[ModelMarketBoardRecord]:
    selected = [
        ModelMarketBoardRecord(**entry)
        for entry in repository.model_market_boards
        if (target_task is None or entry["target_task"] == target_task)
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


def list_model_market_boards_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    season_label: str | None = None,
) -> list[ModelMarketBoardRecord]:
    ensure_model_tables(connection)
    query = """
        SELECT
            id,
            board_key,
            slate_label,
            target_task,
            season_label,
            game_count,
            game_date_start,
            game_date_end,
            payload_json,
            created_at,
            updated_at
        FROM model_market_board
        WHERE 1 = 1
    """
    params: list[Any] = []
    if target_task is not None:
        query += " AND target_task = %s"
        params.append(target_task)
    if season_label is not None:
        query += " AND season_label = %s"
        params.append(season_label)
    query += " ORDER BY created_at DESC, id DESC"
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelMarketBoardRecord(
            id=int(row[0]),
            board_key=row[1],
            slate_label=row[2],
            target_task=row[3],
            season_label=row[4],
            game_count=int(row[5]),
            game_date_start=row[6],
            game_date_end=row[7],
            payload=row[8],
            created_at=row[9],
            updated_at=row[10],
        )
        for row in rows
    ]


def list_model_market_board_refresh_events_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int | None = None,
) -> list[ModelMarketBoardRefreshRecord]:
    selected = [
        ModelMarketBoardRefreshRecord(**entry)
        for entry in repository.model_market_board_refresh_events
        if (target_task is None or entry["target_task"] == target_task)
        and (source_name is None or entry["source_name"] == source_name)
    ]
    sorted_events = sorted(
        selected,
        key=lambda entry: (
            entry.created_at or datetime.min.replace(tzinfo=timezone.utc),
            entry.id,
        ),
        reverse=True,
    )
    return sorted_events[:recent_limit] if recent_limit is not None else sorted_events


def list_model_market_board_refresh_events_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int | None = None,
) -> list[ModelMarketBoardRefreshRecord]:
    ensure_model_tables(connection)
    query = """
        SELECT
            id,
            model_market_board_id,
            board_key,
            target_task,
            source_name,
            refresh_status,
            game_count,
            payload_json,
            created_at
        FROM model_market_board_refresh_event
        WHERE 1 = 1
    """
    params: list[Any] = []
    if target_task is not None:
        query += " AND target_task = %s"
        params.append(target_task)
    if source_name is not None:
        query += " AND source_name = %s"
        params.append(source_name)
    query += " ORDER BY created_at DESC, id DESC"
    if recent_limit is not None:
        query += " LIMIT %s"
        params.append(recent_limit)
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelMarketBoardRefreshRecord(
            id=int(row[0]),
            model_market_board_id=int(row[1]),
            board_key=row[2],
            target_task=row[3],
            source_name=row[4],
            refresh_status=row[5],
            game_count=int(row[6]),
            payload=row[7],
            created_at=row[8],
        )
        for row in rows
    ]


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


def list_model_market_board_refresh_batches_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int | None = None,
) -> list[ModelMarketBoardRefreshBatchRecord]:
    selected = [
        ModelMarketBoardRefreshBatchRecord(**entry)
        for entry in repository.model_market_board_refresh_batches
        if (target_task is None or entry["target_task"] == target_task)
        and (source_name is None or entry.get("source_name") == source_name)
    ]
    ordered = sorted(
        selected,
        key=lambda entry: (
            entry.created_at or datetime.min.replace(tzinfo=timezone.utc),
            entry.id,
        ),
        reverse=True,
    )
    if recent_limit is None:
        return ordered
    return ordered[:recent_limit]


def list_model_market_board_refresh_batches_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int | None = None,
) -> list[ModelMarketBoardRefreshBatchRecord]:
    ensure_model_tables(connection)
    query = """
        SELECT
            id,
            target_task,
            source_name,
            season_label,
            freshness_status,
            pending_only,
            candidate_board_count,
            refreshed_board_count,
            created_board_count,
            updated_board_count,
            unchanged_board_count,
            payload_json,
            created_at
        FROM model_market_board_refresh_batch
        WHERE 1=1
    """
    params: list[Any] = []
    if target_task is not None:
        query += " AND target_task = %s"
        params.append(target_task)
    if source_name is not None:
        query += " AND source_name = %s"
        params.append(source_name)
    query += " ORDER BY created_at DESC, id DESC"
    if recent_limit is not None:
        query += " LIMIT %s"
        params.append(recent_limit)
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelMarketBoardRefreshBatchRecord(
            id=int(row[0]),
            target_task=row[1],
            source_name=row[2],
            season_label=row[3],
            freshness_status=row[4],
            pending_only=bool(row[5]),
            candidate_board_count=int(row[6]),
            refreshed_board_count=int(row[7]),
            created_board_count=int(row[8]),
            updated_board_count=int(row[9]),
            unchanged_board_count=int(row[10]),
            payload=row[11],
            created_at=row[12],
        )
        for row in rows
    ]


def list_model_market_board_scoring_batches_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int | None = None,
) -> list[ModelMarketBoardScoringBatchRecord]:
    selected = [
        ModelMarketBoardScoringBatchRecord(**entry)
        for entry in repository.model_market_board_scoring_batches
        if (target_task is None or entry["target_task"] == target_task)
        and (source_name is None or entry.get("source_name") == source_name)
    ]
    ordered = sorted(
        selected,
        key=lambda entry: (
            entry.created_at or datetime.min.replace(tzinfo=timezone.utc),
            entry.id,
        ),
        reverse=True,
    )
    if recent_limit is None:
        return ordered
    return ordered[:recent_limit]


def list_model_market_board_scoring_batches_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int | None = None,
) -> list[ModelMarketBoardScoringBatchRecord]:
    ensure_model_tables(connection)
    query = """
        SELECT
            id,
            target_task,
            source_name,
            season_label,
            freshness_status,
            pending_only,
            candidate_board_count,
            scored_board_count,
            materialized_scoring_run_count,
            materialized_opportunity_count,
            payload_json,
            created_at
        FROM model_market_board_scoring_batch
        WHERE 1=1
    """
    params: list[Any] = []
    if target_task is not None:
        query += " AND target_task = %s"
        params.append(target_task)
    if source_name is not None:
        query += " AND source_name = %s"
        params.append(source_name)
    query += " ORDER BY created_at DESC, id DESC"
    if recent_limit is not None:
        query += " LIMIT %s"
        params.append(recent_limit)
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelMarketBoardScoringBatchRecord(
            id=int(row[0]),
            target_task=row[1],
            source_name=row[2],
            season_label=row[3],
            freshness_status=row[4],
            pending_only=bool(row[5]),
            candidate_board_count=int(row[6]),
            scored_board_count=int(row[7]),
            materialized_scoring_run_count=int(row[8]),
            materialized_opportunity_count=int(row[9]),
            payload=row[10],
            created_at=row[11],
        )
        for row in rows
    ]


def list_model_market_board_cadence_batches_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int | None = None,
) -> list[ModelMarketBoardCadenceBatchRecord]:
    selected = [
        ModelMarketBoardCadenceBatchRecord(**entry)
        for entry in repository.model_market_board_cadence_batches
        if (target_task is None or entry["target_task"] == target_task)
        and (source_name is None or entry.get("source_name") == source_name)
    ]
    ordered = sorted(
        selected,
        key=lambda entry: (
            entry.created_at or datetime.min.replace(tzinfo=timezone.utc),
            entry.id,
        ),
        reverse=True,
    )
    if recent_limit is None:
        return ordered
    return ordered[:recent_limit]


def list_model_market_board_cadence_batches_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int | None = None,
) -> list[ModelMarketBoardCadenceBatchRecord]:
    ensure_model_tables(connection)
    query = """
        SELECT
            id,
            target_task,
            source_name,
            season_label,
            refresh_freshness_status,
            scoring_freshness_status,
            refreshed_board_count,
            scored_board_count,
            materialized_scoring_run_count,
            materialized_opportunity_count,
            payload_json,
            created_at
        FROM model_market_board_cadence_batch
        WHERE 1=1
    """
    params: list[Any] = []
    if target_task is not None:
        query += " AND target_task = %s"
        params.append(target_task)
    if source_name is not None:
        query += " AND source_name = %s"
        params.append(source_name)
    query += " ORDER BY created_at DESC, id DESC"
    if recent_limit is not None:
        query += " LIMIT %s"
        params.append(recent_limit)
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelMarketBoardCadenceBatchRecord(
            id=int(row[0]),
            target_task=row[1],
            source_name=row[2],
            season_label=row[3],
            refresh_freshness_status=row[4],
            scoring_freshness_status=row[5],
            refreshed_board_count=int(row[6]),
            scored_board_count=int(row[7]),
            materialized_scoring_run_count=int(row[8]),
            materialized_opportunity_count=int(row[9]),
            payload=row[10],
            created_at=row[11],
        )
        for row in rows
    ]


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
    ensure_model_tables(connection)
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


def build_model_market_board_source_games(
    *,
    source_name: str,
    season_label: str,
    game_date: date,
    game_count: int | None = None,
    source_path: str | None = None,
) -> list[dict[str, Any]]:
    return _run_market_board_source_provider(
        source_name=source_name,
        season_label=season_label,
        game_date=game_date,
        game_count=game_count,
        source_path=source_path,
    )


def _find_model_market_board_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    board_key: str,
) -> ModelMarketBoardRecord | None:
    match = next(
        (entry for entry in repository.model_market_boards if entry["board_key"] == board_key),
        None,
    )
    return ModelMarketBoardRecord(**match) if match is not None else None


def _find_model_market_board_postgres(
    connection: Any,
    *,
    board_key: str,
) -> ModelMarketBoardRecord | None:
    matches = [
        entry
        for entry in list_model_market_boards_postgres(connection)
        if entry.board_key == board_key
    ]
    return matches[0] if matches else None


def _resolve_market_board_refresh_status(
    *,
    existing_board: ModelMarketBoardRecord | None,
    generated_games: list[dict[str, Any]],
) -> str:
    if existing_board is None:
        return "created"
    previous_games = list(existing_board.payload.get("games", []))
    normalized_generated_games = [
        _serialize_future_game_input(game) for game in generated_games
    ]
    return "unchanged" if previous_games == normalized_generated_games else "updated"


def _build_market_board_refresh_change_summary(
    *,
    existing_board: ModelMarketBoardRecord | None,
    generated_games: list[dict[str, Any]],
) -> dict[str, Any]:
    previous_games = (
        list(existing_board.payload.get("games", [])) if existing_board is not None else []
    )
    normalized_generated_games = [
        _serialize_future_game_input(game) for game in generated_games
    ]
    previous_by_key = {
        _build_market_board_game_key(entry): entry for entry in previous_games
    }
    generated_by_key = {
        _build_market_board_game_key(entry): entry for entry in normalized_generated_games
    }
    added_keys = sorted(set(generated_by_key) - set(previous_by_key))
    removed_keys = sorted(set(previous_by_key) - set(generated_by_key))
    common_keys = sorted(set(previous_by_key) & set(generated_by_key))
    changed_games = []
    unchanged_count = 0
    for game_key in common_keys:
        previous = previous_by_key[game_key]
        current = generated_by_key[game_key]
        if previous == current:
            unchanged_count += 1
            continue
        changed_fields = {}
        for field_name in ("home_spread_line", "total_line"):
            if previous.get(field_name) != current.get(field_name):
                changed_fields[field_name] = {
                    "previous": previous.get(field_name),
                    "current": current.get(field_name),
                }
        changed_games.append(
            {
                "game_key": game_key,
                "previous": previous,
                "current": current,
                "changed_fields": changed_fields,
            }
        )
    return {
        "previous_game_count": len(previous_games),
        "generated_game_count": len(normalized_generated_games),
        "added_game_count": len(added_keys),
        "removed_game_count": len(removed_keys),
        "changed_game_count": len(changed_games),
        "unchanged_game_count": unchanged_count,
        "added_game_keys": added_keys,
        "removed_game_keys": removed_keys,
        "changed_games": changed_games,
    }


def _build_market_board_game_key(game: dict[str, Any]) -> str:
    return (
        f"{game['season_label']}:{game['game_date']}:"
        f"{game['home_team_code']}:{game['away_team_code']}"
    )


def _resolve_market_board_refresh_count(
    existing_board: ModelMarketBoardRecord | None,
) -> int:
    if existing_board is None:
        return 0
    source_payload = existing_board.payload.get("source", {})
    return int(source_payload.get("refresh_count", 0))


def _build_model_market_board(
    *,
    target_task: str,
    games: list[dict[str, Any]],
    slate_label: str | None,
) -> ModelMarketBoardRecord:
    serialized_games = [_serialize_future_game_input(game) for game in games]
    game_dates = sorted(entry["game_date"] for entry in serialized_games)
    season_labels = sorted({entry["season_label"] for entry in serialized_games})
    return ModelMarketBoardRecord(
        id=0,
        board_key=_build_future_slate_key(
            target_task=target_task,
            slate_label=slate_label,
            serialized_inputs=serialized_games,
        ),
        slate_label=slate_label,
        target_task=target_task,
        season_label=season_labels[0] if len(season_labels) == 1 else None,
        game_count=len(serialized_games),
        game_date_start=(_coerce_date(game_dates[0]) if game_dates else None),
        game_date_end=(_coerce_date(game_dates[-1]) if game_dates else None),
        payload={
            "games": serialized_games,
            "season_labels": season_labels,
        },
    )


def save_model_market_board_in_memory(
    repository: InMemoryIngestionRepository,
    board: ModelMarketBoardRecord,
) -> ModelMarketBoardRecord:
    payload = asdict(board)
    existing_index = next(
        (
            index
            for index, entry in enumerate(repository.model_market_boards)
            if entry["board_key"] == board.board_key
        ),
        None,
    )
    now = datetime.now(timezone.utc)
    if existing_index is None:
        payload["id"] = len(repository.model_market_boards) + 1
        payload["created_at"] = now
        payload["updated_at"] = now
        repository.model_market_boards.append(payload)
        return ModelMarketBoardRecord(**payload)
    current = repository.model_market_boards[existing_index]
    payload["id"] = current["id"]
    payload["created_at"] = current["created_at"]
    payload["updated_at"] = now
    repository.model_market_boards[existing_index] = payload
    return ModelMarketBoardRecord(**payload)


def save_model_market_board_postgres(
    connection: Any,
    board: ModelMarketBoardRecord,
) -> ModelMarketBoardRecord:
    ensure_model_tables(connection)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO model_market_board (
                board_key,
                slate_label,
                target_task,
                season_label,
                game_count,
                game_date_start,
                game_date_end,
                payload_json,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
            ON CONFLICT (board_key)
            DO UPDATE SET
                slate_label = EXCLUDED.slate_label,
                target_task = EXCLUDED.target_task,
                season_label = EXCLUDED.season_label,
                game_count = EXCLUDED.game_count,
                game_date_start = EXCLUDED.game_date_start,
                game_date_end = EXCLUDED.game_date_end,
                payload_json = EXCLUDED.payload_json,
                updated_at = NOW()
            RETURNING
                id,
                board_key,
                slate_label,
                target_task,
                season_label,
                game_count,
                game_date_start,
                game_date_end,
                payload_json,
                created_at,
                updated_at
            """,
            (
                board.board_key,
                board.slate_label,
                board.target_task,
                board.season_label,
                board.game_count,
                board.game_date_start,
                board.game_date_end,
                _json_dumps(board.payload),
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return ModelMarketBoardRecord(
        id=int(row[0]),
        board_key=row[1],
        slate_label=row[2],
        target_task=row[3],
        season_label=row[4],
        game_count=int(row[5]),
        game_date_start=row[6],
        game_date_end=row[7],
        payload=row[8],
        created_at=row[9],
        updated_at=row[10],
    )


def save_model_market_board_refresh_event_in_memory(
    repository: InMemoryIngestionRepository,
    refresh_event: ModelMarketBoardRefreshRecord,
) -> ModelMarketBoardRefreshRecord:
    payload = asdict(refresh_event)
    payload["id"] = len(repository.model_market_board_refresh_events) + 1
    payload["created_at"] = datetime.now(timezone.utc)
    repository.model_market_board_refresh_events.append(payload)
    return ModelMarketBoardRefreshRecord(**payload)


def save_model_market_board_refresh_event_postgres(
    connection: Any,
    refresh_event: ModelMarketBoardRefreshRecord,
) -> ModelMarketBoardRefreshRecord:
    ensure_model_tables(connection)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO model_market_board_refresh_event (
                model_market_board_id,
                board_key,
                target_task,
                source_name,
                refresh_status,
                game_count,
                payload_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
            RETURNING id, created_at
            """,
            (
                refresh_event.model_market_board_id,
                refresh_event.board_key,
                refresh_event.target_task,
                refresh_event.source_name,
                refresh_event.refresh_status,
                refresh_event.game_count,
                _json_dumps(refresh_event.payload),
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return ModelMarketBoardRefreshRecord(
        id=int(row[0]),
        model_market_board_id=refresh_event.model_market_board_id,
        board_key=refresh_event.board_key,
        target_task=refresh_event.target_task,
        source_name=refresh_event.source_name,
        refresh_status=refresh_event.refresh_status,
        game_count=refresh_event.game_count,
        payload=refresh_event.payload,
        created_at=row[1],
    )


def _serialize_model_market_board(
    board: ModelMarketBoardRecord | None,
) -> dict[str, Any] | None:
    if board is None:
        return None
    freshness = _build_market_board_freshness(board)
    return {
        "id": board.id,
        "board_key": board.board_key,
        "slate_label": board.slate_label,
        "target_task": board.target_task,
        "season_label": board.season_label,
        "game_count": board.game_count,
        "game_date_start": board.game_date_start.isoformat() if board.game_date_start else None,
        "game_date_end": board.game_date_end.isoformat() if board.game_date_end else None,
        "payload": board.payload,
        "freshness": freshness,
        "created_at": board.created_at.isoformat() if board.created_at else None,
        "updated_at": board.updated_at.isoformat() if board.updated_at else None,
    }


def _serialize_model_market_board_refresh_event(
    refresh_event: ModelMarketBoardRefreshRecord,
) -> dict[str, Any]:
    return {
        "id": refresh_event.id,
        "model_market_board_id": refresh_event.model_market_board_id,
        "board_key": refresh_event.board_key,
        "target_task": refresh_event.target_task,
        "source_name": refresh_event.source_name,
        "refresh_status": refresh_event.refresh_status,
        "game_count": refresh_event.game_count,
        "payload": refresh_event.payload,
        "created_at": refresh_event.created_at.isoformat() if refresh_event.created_at else None,
    }


def _serialize_model_market_board_source_run(
    source_run: ModelMarketBoardSourceRunRecord | None,
) -> dict[str, Any] | None:
    if source_run is None:
        return None
    return {
        "id": source_run.id,
        "source_name": source_run.source_name,
        "target_task": source_run.target_task,
        "season_label": source_run.season_label,
        "game_date": source_run.game_date.isoformat(),
        "slate_label": source_run.slate_label,
        "requested_game_count": source_run.requested_game_count,
        "generated_game_count": source_run.generated_game_count,
        "status": source_run.status,
        "payload": source_run.payload,
        "created_at": source_run.created_at.isoformat() if source_run.created_at else None,
    }


def _serialize_model_market_board_refresh_batch(
    batch: ModelMarketBoardRefreshBatchRecord | None,
) -> dict[str, Any] | None:
    if batch is None:
        return None
    return {
        "id": batch.id,
        "target_task": batch.target_task,
        "source_name": batch.source_name,
        "season_label": batch.season_label,
        "freshness_status": batch.freshness_status,
        "pending_only": batch.pending_only,
        "candidate_board_count": batch.candidate_board_count,
        "refreshed_board_count": batch.refreshed_board_count,
        "created_board_count": batch.created_board_count,
        "updated_board_count": batch.updated_board_count,
        "unchanged_board_count": batch.unchanged_board_count,
        "payload": batch.payload,
        "created_at": batch.created_at.isoformat() if batch.created_at else None,
    }


def _serialize_model_market_board_scoring_batch(
    batch: ModelMarketBoardScoringBatchRecord | None,
) -> dict[str, Any] | None:
    if batch is None:
        return None
    return {
        "id": batch.id,
        "target_task": batch.target_task,
        "source_name": batch.source_name,
        "season_label": batch.season_label,
        "freshness_status": batch.freshness_status,
        "pending_only": batch.pending_only,
        "candidate_board_count": batch.candidate_board_count,
        "scored_board_count": batch.scored_board_count,
        "materialized_scoring_run_count": batch.materialized_scoring_run_count,
        "materialized_opportunity_count": batch.materialized_opportunity_count,
        "payload": batch.payload,
        "created_at": batch.created_at.isoformat() if batch.created_at else None,
    }


def _serialize_model_market_board_cadence_batch(
    batch: ModelMarketBoardCadenceBatchRecord | None,
) -> dict[str, Any] | None:
    if batch is None:
        return None
    return {
        "id": batch.id,
        "target_task": batch.target_task,
        "source_name": batch.source_name,
        "season_label": batch.season_label,
        "refresh_freshness_status": batch.refresh_freshness_status,
        "scoring_freshness_status": batch.scoring_freshness_status,
        "refreshed_board_count": batch.refreshed_board_count,
        "scored_board_count": batch.scored_board_count,
        "materialized_scoring_run_count": batch.materialized_scoring_run_count,
        "materialized_opportunity_count": batch.materialized_opportunity_count,
        "payload": batch.payload,
        "created_at": batch.created_at.isoformat() if batch.created_at else None,
    }


def _build_market_board_freshness(
    board: ModelMarketBoardRecord,
) -> dict[str, Any] | None:
    source_payload = board.payload.get("source")
    if not isinstance(source_payload, dict):
        return None
    refreshed_at = source_payload.get("refreshed_at")
    if refreshed_at is None:
        return None
    refreshed_date = _coerce_date(refreshed_at)
    days_since_refresh = (_utc_today() - refreshed_date).days
    if days_since_refresh <= 0:
        freshness_status = "fresh"
    elif days_since_refresh == 1:
        freshness_status = "aging"
    else:
        freshness_status = "stale"
    return {
        "source_name": source_payload.get("source_name"),
        "refresh_target_date": source_payload.get("refresh_target_date"),
        "refreshed_at": refreshed_date.isoformat(),
        "refresh_count": int(source_payload.get("refresh_count", 0)),
        "last_refresh_status": source_payload.get("last_refresh_status"),
        "days_since_refresh": days_since_refresh,
        "freshness_status": freshness_status,
    }


def _summarize_market_board_refresh_history(
    refresh_events: list[ModelMarketBoardRefreshRecord],
    *,
    recent_limit: int,
) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    for refresh_event in refresh_events:
        status_counts[refresh_event.refresh_status] = (
            status_counts.get(refresh_event.refresh_status, 0) + 1
        )
        source_counts[refresh_event.source_name] = (
            source_counts.get(refresh_event.source_name, 0) + 1
        )
    return {
        "overview": {
            "refresh_event_count": len(refresh_events),
            "status_counts": status_counts,
            "source_counts": source_counts,
            "latest_refresh_event": (
                _serialize_model_market_board_refresh_event(refresh_events[0])
                if refresh_events
                else None
            ),
        },
        "recent_refresh_events": [
            _serialize_model_market_board_refresh_event(entry)
            for entry in refresh_events[:recent_limit]
        ],
    }


def _summarize_model_market_board_source_run_history(
    source_runs: list[ModelMarketBoardSourceRunRecord],
    *,
    recent_limit: int,
) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    total_generated_game_count = 0
    total_invalid_row_count = 0
    total_warning_count = 0
    daily_buckets: dict[str, dict[str, Any]] = {}
    for source_run in source_runs:
        status_counts[source_run.status] = status_counts.get(source_run.status, 0) + 1
        source_counts[source_run.source_name] = source_counts.get(source_run.source_name, 0) + 1
        total_generated_game_count += source_run.generated_game_count
        validation_summary = source_run.payload.get("validation_summary", {})
        total_invalid_row_count += int(validation_summary.get("invalid_row_count", 0))
        total_warning_count += int(validation_summary.get("warning_count", 0))
        if source_run.created_at is None:
            continue
        day_key = source_run.created_at.date().isoformat()
        bucket = daily_buckets.setdefault(
            day_key,
            {
                "date": day_key,
                "run_count": 0,
                "generated_game_count": 0,
                "invalid_row_count": 0,
                "warning_count": 0,
            },
        )
        bucket["run_count"] += 1
        bucket["generated_game_count"] += source_run.generated_game_count
        bucket["invalid_row_count"] += int(validation_summary.get("invalid_row_count", 0))
        bucket["warning_count"] += int(validation_summary.get("warning_count", 0))
    return {
        "overview": {
            "source_run_count": len(source_runs),
            "generated_game_count": total_generated_game_count,
            "invalid_row_count": total_invalid_row_count,
            "warning_count": total_warning_count,
            "status_counts": status_counts,
            "source_counts": source_counts,
            "latest_source_run": _serialize_model_market_board_source_run(
                source_runs[0] if source_runs else None
            ),
        },
        "daily_buckets": [daily_buckets[key] for key in sorted(daily_buckets.keys())],
        "recent_source_runs": [
            _serialize_model_market_board_source_run(entry)
            for entry in source_runs[:recent_limit]
        ],
    }


def _build_model_market_board_refresh_batch(
    *,
    result: dict[str, Any],
    target_task: str | None,
    source_name: str | None,
    season_label: str | None,
    freshness_status: str | None,
    pending_only: bool,
) -> ModelMarketBoardRefreshBatchRecord:
    payload = {
        "queue_before": result.get("queue_before", {}),
        "queue_after": result.get("queue_after", {}),
        "refresh_runs": result.get("refresh_runs", []),
    }
    return ModelMarketBoardRefreshBatchRecord(
        id=0,
        target_task=target_task or "unknown",
        source_name=source_name,
        season_label=season_label,
        freshness_status=freshness_status,
        pending_only=pending_only,
        candidate_board_count=int(result.get("candidate_board_count", 0)),
        refreshed_board_count=int(result.get("refreshed_board_count", 0)),
        created_board_count=int(result.get("created_board_count", 0)),
        updated_board_count=int(result.get("updated_board_count", 0)),
        unchanged_board_count=int(result.get("unchanged_board_count", 0)),
        payload=payload,
    )


def save_model_market_board_refresh_batch_in_memory(
    repository: InMemoryIngestionRepository,
    batch: ModelMarketBoardRefreshBatchRecord,
) -> ModelMarketBoardRefreshBatchRecord:
    payload = asdict(batch)
    payload["id"] = len(repository.model_market_board_refresh_batches) + 1
    payload["created_at"] = datetime.now(timezone.utc)
    repository.model_market_board_refresh_batches.append(payload)
    return ModelMarketBoardRefreshBatchRecord(**payload)


def save_model_market_board_refresh_batch_postgres(
    connection: Any,
    batch: ModelMarketBoardRefreshBatchRecord,
) -> ModelMarketBoardRefreshBatchRecord:
    ensure_model_tables(connection)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO model_market_board_refresh_batch (
                target_task,
                source_name,
                season_label,
                freshness_status,
                pending_only,
                candidate_board_count,
                refreshed_board_count,
                created_board_count,
                updated_board_count,
                unchanged_board_count,
                payload_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            RETURNING id, created_at
            """,
            (
                batch.target_task,
                batch.source_name,
                batch.season_label,
                batch.freshness_status,
                batch.pending_only,
                batch.candidate_board_count,
                batch.refreshed_board_count,
                batch.created_board_count,
                batch.updated_board_count,
                batch.unchanged_board_count,
                _json_dumps(batch.payload),
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return ModelMarketBoardRefreshBatchRecord(
        id=int(row[0]),
        target_task=batch.target_task,
        source_name=batch.source_name,
        season_label=batch.season_label,
        freshness_status=batch.freshness_status,
        pending_only=batch.pending_only,
        candidate_board_count=batch.candidate_board_count,
        refreshed_board_count=batch.refreshed_board_count,
        created_board_count=batch.created_board_count,
        updated_board_count=batch.updated_board_count,
        unchanged_board_count=batch.unchanged_board_count,
        payload=batch.payload,
        created_at=row[1],
    )


def _build_model_market_board_source_run(
    *,
    source_name: str,
    target_task: str,
    season_label: str,
    game_date: date,
    slate_label: str | None,
    requested_game_count: int,
    generated_games: list[dict[str, Any]],
    raw_generated_games: list[dict[str, Any]] | None = None,
    source_path: str | None = None,
    source_request_context: dict[str, Any] | None = None,
    status: str = "SUCCESS",
    error_message: str | None = None,
    validation_summary: dict[str, Any] | None = None,
    source_payload_fingerprints: dict[str, Any] | None = None,
) -> ModelMarketBoardSourceRunRecord:
    payload = {
        "request": {
            "source_name": source_name,
            "target_task": target_task,
            "season_label": season_label,
            "game_date": game_date.isoformat(),
            "slate_label": slate_label,
            "requested_game_count": requested_game_count,
            "source_path": source_path,
        },
        "generated_games": generated_games,
    }
    if raw_generated_games is not None:
        payload["raw_generated_games"] = raw_generated_games
    if validation_summary is not None:
        payload["validation_summary"] = validation_summary
    if source_payload_fingerprints is not None:
        payload["source_payload_fingerprints"] = source_payload_fingerprints
    if source_request_context is not None:
        payload["source_request_context"] = source_request_context
    if error_message is not None:
        payload["error_message"] = error_message
    return ModelMarketBoardSourceRunRecord(
        id=0,
        source_name=source_name,
        target_task=target_task,
        season_label=season_label,
        game_date=game_date,
        slate_label=slate_label,
        requested_game_count=requested_game_count,
        generated_game_count=len(generated_games),
        status=status,
        payload=payload,
    )


def save_model_market_board_source_run_in_memory(
    repository: InMemoryIngestionRepository,
    source_run: ModelMarketBoardSourceRunRecord,
) -> ModelMarketBoardSourceRunRecord:
    payload = asdict(source_run)
    payload["id"] = len(repository.model_market_board_source_runs) + 1
    payload["created_at"] = datetime.now(timezone.utc)
    repository.model_market_board_source_runs.append(payload)
    return ModelMarketBoardSourceRunRecord(**payload)


def save_model_market_board_source_run_postgres(
    connection: Any,
    source_run: ModelMarketBoardSourceRunRecord,
) -> ModelMarketBoardSourceRunRecord:
    ensure_model_tables(connection)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO model_market_board_source_run (
                source_name,
                target_task,
                season_label,
                game_date,
                slate_label,
                requested_game_count,
                generated_game_count,
                status,
                payload_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            RETURNING id, created_at
            """,
            (
                source_run.source_name,
                source_run.target_task,
                source_run.season_label,
                source_run.game_date,
                source_run.slate_label,
                source_run.requested_game_count,
                source_run.generated_game_count,
                source_run.status,
                _json_dumps(source_run.payload),
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return ModelMarketBoardSourceRunRecord(
        id=int(row[0]),
        source_name=source_run.source_name,
        target_task=source_run.target_task,
        season_label=source_run.season_label,
        game_date=source_run.game_date,
        slate_label=source_run.slate_label,
        requested_game_count=source_run.requested_game_count,
        generated_game_count=source_run.generated_game_count,
        status=source_run.status,
        payload=source_run.payload,
        created_at=row[1],
    )


def _build_model_market_board_scoring_batch(
    *,
    result: dict[str, Any],
    target_task: str | None,
    source_name: str | None,
    season_label: str | None,
    freshness_status: str | None,
    pending_only: bool,
) -> ModelMarketBoardScoringBatchRecord:
    payload = {
        "queue_before": result.get("queue_before", {}),
        "queue_after": result.get("queue_after", {}),
        "orchestration_runs": result.get("orchestration_runs", []),
    }
    return ModelMarketBoardScoringBatchRecord(
        id=0,
        target_task=target_task or "unknown",
        source_name=source_name,
        season_label=season_label,
        freshness_status=freshness_status,
        pending_only=pending_only,
        candidate_board_count=int(result.get("candidate_board_count", 0)),
        scored_board_count=int(result.get("scored_board_count", 0)),
        materialized_scoring_run_count=int(
            result.get("materialized_scoring_run_count", 0)
        ),
        materialized_opportunity_count=int(
            result.get("materialized_opportunity_count", 0)
        ),
        payload=payload,
    )


def save_model_market_board_scoring_batch_in_memory(
    repository: InMemoryIngestionRepository,
    batch: ModelMarketBoardScoringBatchRecord,
) -> ModelMarketBoardScoringBatchRecord:
    payload = asdict(batch)
    payload["id"] = len(repository.model_market_board_scoring_batches) + 1
    payload["created_at"] = datetime.now(timezone.utc)
    repository.model_market_board_scoring_batches.append(payload)
    return ModelMarketBoardScoringBatchRecord(**payload)


def save_model_market_board_scoring_batch_postgres(
    connection: Any,
    batch: ModelMarketBoardScoringBatchRecord,
) -> ModelMarketBoardScoringBatchRecord:
    ensure_model_tables(connection)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO model_market_board_scoring_batch (
                target_task,
                source_name,
                season_label,
                freshness_status,
                pending_only,
                candidate_board_count,
                scored_board_count,
                materialized_scoring_run_count,
                materialized_opportunity_count,
                payload_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            RETURNING id, created_at
            """,
            (
                batch.target_task,
                batch.source_name,
                batch.season_label,
                batch.freshness_status,
                batch.pending_only,
                batch.candidate_board_count,
                batch.scored_board_count,
                batch.materialized_scoring_run_count,
                batch.materialized_opportunity_count,
                _json_dumps(batch.payload),
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return ModelMarketBoardScoringBatchRecord(
        id=int(row[0]),
        target_task=batch.target_task,
        source_name=batch.source_name,
        season_label=batch.season_label,
        freshness_status=batch.freshness_status,
        pending_only=batch.pending_only,
        candidate_board_count=batch.candidate_board_count,
        scored_board_count=batch.scored_board_count,
        materialized_scoring_run_count=batch.materialized_scoring_run_count,
        materialized_opportunity_count=batch.materialized_opportunity_count,
        payload=batch.payload,
        created_at=row[1],
    )


def _build_model_market_board_cadence_batch(
    *,
    result: dict[str, Any],
    target_task: str | None,
    source_name: str | None,
    season_label: str | None,
    refresh_freshness_status: str | None,
    scoring_freshness_status: str | None,
) -> ModelMarketBoardCadenceBatchRecord:
    payload = {
        "refresh_result": result.get("refresh_result", {}),
        "scoring_result": result.get("scoring_result", {}),
    }
    return ModelMarketBoardCadenceBatchRecord(
        id=0,
        target_task=target_task or "unknown",
        source_name=source_name,
        season_label=season_label,
        refresh_freshness_status=refresh_freshness_status,
        scoring_freshness_status=scoring_freshness_status,
        refreshed_board_count=int(result.get("refreshed_board_count", 0)),
        scored_board_count=int(result.get("scored_board_count", 0)),
        materialized_scoring_run_count=int(
            result.get("materialized_scoring_run_count", 0)
        ),
        materialized_opportunity_count=int(
            result.get("materialized_opportunity_count", 0)
        ),
        payload=payload,
    )


def save_model_market_board_cadence_batch_in_memory(
    repository: InMemoryIngestionRepository,
    batch: ModelMarketBoardCadenceBatchRecord,
) -> ModelMarketBoardCadenceBatchRecord:
    payload = asdict(batch)
    payload["id"] = len(repository.model_market_board_cadence_batches) + 1
    payload["created_at"] = datetime.now(timezone.utc)
    repository.model_market_board_cadence_batches.append(payload)
    return ModelMarketBoardCadenceBatchRecord(**payload)


def save_model_market_board_cadence_batch_postgres(
    connection: Any,
    batch: ModelMarketBoardCadenceBatchRecord,
) -> ModelMarketBoardCadenceBatchRecord:
    ensure_model_tables(connection)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO model_market_board_cadence_batch (
                target_task,
                source_name,
                season_label,
                refresh_freshness_status,
                scoring_freshness_status,
                refreshed_board_count,
                scored_board_count,
                materialized_scoring_run_count,
                materialized_opportunity_count,
                payload_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            RETURNING id, created_at
            """,
            (
                batch.target_task,
                batch.source_name,
                batch.season_label,
                batch.refresh_freshness_status,
                batch.scoring_freshness_status,
                batch.refreshed_board_count,
                batch.scored_board_count,
                batch.materialized_scoring_run_count,
                batch.materialized_opportunity_count,
                _json_dumps(batch.payload),
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return ModelMarketBoardCadenceBatchRecord(
        id=int(row[0]),
        target_task=batch.target_task,
        source_name=batch.source_name,
        season_label=batch.season_label,
        refresh_freshness_status=batch.refresh_freshness_status,
        scoring_freshness_status=batch.scoring_freshness_status,
        refreshed_board_count=batch.refreshed_board_count,
        scored_board_count=batch.scored_board_count,
        materialized_scoring_run_count=batch.materialized_scoring_run_count,
        materialized_opportunity_count=batch.materialized_opportunity_count,
        payload=batch.payload,
        created_at=row[1],
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
    ensure_model_tables(connection)
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
    ensure_model_tables(connection)
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


def _serialize_model_scoring_run(
    scoring_run: ModelScoringRunRecord | None,
) -> dict[str, Any] | None:
    if scoring_run is None:
        return None
    return {
        "id": scoring_run.id,
        "model_market_board_id": scoring_run.model_market_board_id,
        "model_selection_snapshot_id": scoring_run.model_selection_snapshot_id,
        "model_evaluation_snapshot_id": scoring_run.model_evaluation_snapshot_id,
        "feature_version_id": scoring_run.feature_version_id,
        "target_task": scoring_run.target_task,
        "scenario_key": scoring_run.scenario_key,
        "season_label": scoring_run.season_label,
        "game_date": scoring_run.game_date.isoformat() if scoring_run.game_date else None,
        "home_team_code": scoring_run.home_team_code,
        "away_team_code": scoring_run.away_team_code,
        "home_spread_line": scoring_run.home_spread_line,
        "total_line": scoring_run.total_line,
        "policy_name": scoring_run.policy_name,
        "prediction_count": scoring_run.prediction_count,
        "candidate_opportunity_count": scoring_run.candidate_opportunity_count,
        "review_opportunity_count": scoring_run.review_opportunity_count,
        "discarded_opportunity_count": scoring_run.discarded_opportunity_count,
        "payload": scoring_run.payload,
        "created_at": scoring_run.created_at.isoformat() if scoring_run.created_at else None,
    }


def _serialize_model_opportunity(
    opportunity: ModelOpportunityRecord | None,
) -> dict[str, Any] | None:
    if opportunity is None:
        return None
    return {
        "id": opportunity.id,
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
        "game_date": opportunity.game_date.isoformat() if opportunity.game_date else None,
        "policy_name": opportunity.policy_name,
        "status": opportunity.status,
        "prediction_value": opportunity.prediction_value,
        "signal_strength": opportunity.signal_strength,
        "evidence_rating": opportunity.evidence_rating,
        "recommendation_status": opportunity.recommendation_status,
        "payload": opportunity.payload,
        "created_at": opportunity.created_at.isoformat() if opportunity.created_at else None,
        "updated_at": opportunity.updated_at.isoformat() if opportunity.updated_at else None,
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


def _summarize_model_market_board_scoring_batch_history(
    batches: list[ModelMarketBoardScoringBatchRecord],
    *,
    recent_limit: int,
) -> dict[str, Any]:
    total_candidate_board_count = 0
    total_scored_board_count = 0
    total_scoring_run_count = 0
    total_opportunity_count = 0
    source_counts: dict[str, int] = {}
    freshness_counts: dict[str, int] = {}
    daily_buckets: dict[str, dict[str, Any]] = {}
    for batch in batches:
        total_candidate_board_count += batch.candidate_board_count
        total_scored_board_count += batch.scored_board_count
        total_scoring_run_count += batch.materialized_scoring_run_count
        total_opportunity_count += batch.materialized_opportunity_count
        source_key = batch.source_name or "unspecified"
        source_counts[source_key] = source_counts.get(source_key, 0) + 1
        freshness_key = batch.freshness_status or "unspecified"
        freshness_counts[freshness_key] = freshness_counts.get(freshness_key, 0) + 1
        if batch.created_at is None:
            continue
        day_key = batch.created_at.date().isoformat()
        bucket = daily_buckets.setdefault(
            day_key,
            {
                "date": day_key,
                "batch_count": 0,
                "candidate_board_count": 0,
                "scored_board_count": 0,
                "materialized_scoring_run_count": 0,
                "materialized_opportunity_count": 0,
            },
        )
        bucket["batch_count"] += 1
        bucket["candidate_board_count"] += batch.candidate_board_count
        bucket["scored_board_count"] += batch.scored_board_count
        bucket["materialized_scoring_run_count"] += batch.materialized_scoring_run_count
        bucket["materialized_opportunity_count"] += (
            batch.materialized_opportunity_count
        )
    return {
        "overview": {
            "batch_count": len(batches),
            "candidate_board_count": total_candidate_board_count,
            "scored_board_count": total_scored_board_count,
            "materialized_scoring_run_count": total_scoring_run_count,
            "materialized_opportunity_count": total_opportunity_count,
            "source_counts": source_counts,
            "freshness_status_counts": freshness_counts,
            "latest_batch": _serialize_model_market_board_scoring_batch(
                batches[0] if batches else None
            ),
        },
        "daily_buckets": [daily_buckets[key] for key in sorted(daily_buckets.keys())],
        "recent_batches": [
            _serialize_model_market_board_scoring_batch(entry)
            for entry in batches[:recent_limit]
        ],
    }


def _summarize_model_market_board_refresh_batch_history(
    batches: list[ModelMarketBoardRefreshBatchRecord],
    *,
    recent_limit: int,
) -> dict[str, Any]:
    total_candidate_board_count = 0
    total_refreshed_board_count = 0
    total_created_board_count = 0
    total_updated_board_count = 0
    total_unchanged_board_count = 0
    source_counts: dict[str, int] = {}
    freshness_counts: dict[str, int] = {}
    daily_buckets: dict[str, dict[str, Any]] = {}
    for batch in batches:
        total_candidate_board_count += batch.candidate_board_count
        total_refreshed_board_count += batch.refreshed_board_count
        total_created_board_count += batch.created_board_count
        total_updated_board_count += batch.updated_board_count
        total_unchanged_board_count += batch.unchanged_board_count
        source_key = batch.source_name or "unspecified"
        source_counts[source_key] = source_counts.get(source_key, 0) + 1
        freshness_key = batch.freshness_status or "unspecified"
        freshness_counts[freshness_key] = freshness_counts.get(freshness_key, 0) + 1
        if batch.created_at is None:
            continue
        day_key = batch.created_at.date().isoformat()
        bucket = daily_buckets.setdefault(
            day_key,
            {
                "date": day_key,
                "batch_count": 0,
                "candidate_board_count": 0,
                "refreshed_board_count": 0,
                "created_board_count": 0,
                "updated_board_count": 0,
                "unchanged_board_count": 0,
            },
        )
        bucket["batch_count"] += 1
        bucket["candidate_board_count"] += batch.candidate_board_count
        bucket["refreshed_board_count"] += batch.refreshed_board_count
        bucket["created_board_count"] += batch.created_board_count
        bucket["updated_board_count"] += batch.updated_board_count
        bucket["unchanged_board_count"] += batch.unchanged_board_count
    return {
        "overview": {
            "batch_count": len(batches),
            "candidate_board_count": total_candidate_board_count,
            "refreshed_board_count": total_refreshed_board_count,
            "created_board_count": total_created_board_count,
            "updated_board_count": total_updated_board_count,
            "unchanged_board_count": total_unchanged_board_count,
            "source_counts": source_counts,
            "freshness_status_counts": freshness_counts,
            "latest_batch": _serialize_model_market_board_refresh_batch(
                batches[0] if batches else None
            ),
        },
        "daily_buckets": [daily_buckets[key] for key in sorted(daily_buckets.keys())],
        "recent_batches": [
            _serialize_model_market_board_refresh_batch(entry)
            for entry in batches[:recent_limit]
        ],
    }


def _summarize_model_market_board_cadence_batch_history(
    batches: list[ModelMarketBoardCadenceBatchRecord],
    *,
    recent_limit: int,
) -> dict[str, Any]:
    total_refreshed_board_count = 0
    total_scored_board_count = 0
    total_scoring_run_count = 0
    total_opportunity_count = 0
    source_counts: dict[str, int] = {}
    daily_buckets: dict[str, dict[str, Any]] = {}
    for batch in batches:
        total_refreshed_board_count += batch.refreshed_board_count
        total_scored_board_count += batch.scored_board_count
        total_scoring_run_count += batch.materialized_scoring_run_count
        total_opportunity_count += batch.materialized_opportunity_count
        source_key = batch.source_name or "unspecified"
        source_counts[source_key] = source_counts.get(source_key, 0) + 1
        if batch.created_at is None:
            continue
        day_key = batch.created_at.date().isoformat()
        bucket = daily_buckets.setdefault(
            day_key,
            {
                "date": day_key,
                "batch_count": 0,
                "refreshed_board_count": 0,
                "scored_board_count": 0,
                "materialized_scoring_run_count": 0,
                "materialized_opportunity_count": 0,
            },
        )
        bucket["batch_count"] += 1
        bucket["refreshed_board_count"] += batch.refreshed_board_count
        bucket["scored_board_count"] += batch.scored_board_count
        bucket["materialized_scoring_run_count"] += batch.materialized_scoring_run_count
        bucket["materialized_opportunity_count"] += batch.materialized_opportunity_count
    return {
        "overview": {
            "batch_count": len(batches),
            "refreshed_board_count": total_refreshed_board_count,
            "scored_board_count": total_scored_board_count,
            "materialized_scoring_run_count": total_scoring_run_count,
            "materialized_opportunity_count": total_opportunity_count,
            "source_counts": source_counts,
            "latest_batch": _serialize_model_market_board_cadence_batch(
                batches[0] if batches else None
            ),
        },
        "daily_buckets": [daily_buckets[key] for key in sorted(daily_buckets.keys())],
        "recent_batches": [
            _serialize_model_market_board_cadence_batch(entry)
            for entry in batches[:recent_limit]
        ],
    }


def _build_market_board_operations_summary(
    board: ModelMarketBoardRecord,
    *,
    source_runs: list[ModelMarketBoardSourceRunRecord],
    refresh_events: list[ModelMarketBoardRefreshRecord],
    refresh_batches: list[ModelMarketBoardRefreshBatchRecord],
    cadence_batches: list[ModelMarketBoardCadenceBatchRecord],
    scoring_runs: list[ModelScoringRunRecord],
    opportunities: list[ModelOpportunityRecord],
    batches: list[ModelMarketBoardScoringBatchRecord],
    recent_limit: int,
) -> dict[str, Any]:
    board_source_runs = [
        run
        for run in source_runs
        if run.slate_label == board.slate_label
        and run.season_label == board.season_label
        and run.target_task == board.target_task
    ]
    board_refresh_events = [
        event for event in refresh_events if event.model_market_board_id == board.id
    ]
    queue = _build_market_board_scoring_queue(
        [board],
        scoring_runs,
        source_name=None,
        freshness_status=None,
        pending_only=False,
        recent_limit=1,
    )
    queue_entry = queue["queue_entries"][0] if queue["queue_entries"] else None
    opportunity_status_counts: dict[str, int] = {}
    for opportunity in opportunities:
        opportunity_status_counts[opportunity.status] = (
            opportunity_status_counts.get(opportunity.status, 0) + 1
        )
    board_refresh_batches = [
        batch for batch in refresh_batches if _refresh_batch_includes_board(batch, board.id)
    ]
    board_cadence_batches = [
        batch for batch in cadence_batches if _cadence_batch_includes_board(batch, board.id)
    ]
    board_batches = [batch for batch in batches if _scoring_batch_includes_board(batch, board.id)]
    return {
        "board": _serialize_model_market_board(board),
        "queue_entry": queue_entry,
        "source_runs": {
            "source_run_count": len(board_source_runs),
            "latest_source_run": _serialize_model_market_board_source_run(
                board_source_runs[0] if board_source_runs else None
            ),
            "recent_source_runs": [
                _serialize_model_market_board_source_run(entry)
                for entry in board_source_runs[:recent_limit]
            ],
        },
        "refresh": {
            "refresh_event_count": len(board_refresh_events),
            "latest_refresh_event": (
                _serialize_model_market_board_refresh_event(board_refresh_events[0])
                if board_refresh_events
                else None
            ),
            "recent_refresh_events": [
                _serialize_model_market_board_refresh_event(entry)
                for entry in board_refresh_events[:recent_limit]
            ],
        },
        "refresh_orchestration": {
            "batch_count": len(board_refresh_batches),
            "latest_batch": _serialize_model_market_board_refresh_batch(
                board_refresh_batches[0] if board_refresh_batches else None
            ),
            "recent_batches": [
                _serialize_model_market_board_refresh_batch(entry)
                for entry in board_refresh_batches[:recent_limit]
            ],
        },
        "cadence": {
            "batch_count": len(board_cadence_batches),
            "latest_batch": _serialize_model_market_board_cadence_batch(
                board_cadence_batches[0] if board_cadence_batches else None
            ),
            "recent_batches": [
                _serialize_model_market_board_cadence_batch(entry)
                for entry in board_cadence_batches[:recent_limit]
            ],
        },
        "scoring": {
            "scoring_run_count": len(scoring_runs),
            "latest_scoring_run": _serialize_model_scoring_run(
                scoring_runs[0] if scoring_runs else None
            ),
            "recent_scoring_runs": [
                _serialize_model_scoring_run(entry) for entry in scoring_runs[:recent_limit]
            ],
        },
        "opportunities": {
            "opportunity_count": len(opportunities),
            "status_counts": opportunity_status_counts,
            "latest_opportunity": _serialize_model_opportunity(
                opportunities[0] if opportunities else None
            ),
            "recent_opportunities": [
                _serialize_model_opportunity(entry) for entry in opportunities[:recent_limit]
            ],
        },
        "orchestration": {
            "batch_count": len(board_batches),
            "latest_batch": _serialize_model_market_board_scoring_batch(
                board_batches[0] if board_batches else None
            ),
            "recent_batches": [
                _serialize_model_market_board_scoring_batch(entry)
                for entry in board_batches[:recent_limit]
            ],
        },
    }


def _build_market_board_cadence_dashboard(
    boards: list[ModelMarketBoardRecord],
    *,
    scoring_runs: list[ModelScoringRunRecord],
    batches: list[ModelMarketBoardScoringBatchRecord],
    source_name: str | None,
    recent_limit: int,
) -> dict[str, Any]:
    queue = _build_market_board_scoring_queue(
        boards,
        scoring_runs,
        source_name=source_name,
        freshness_status=None,
        pending_only=False,
        recent_limit=recent_limit,
    )
    cadence_entries = []
    cadence_status_counts: dict[str, int] = {}
    priority_counts: dict[str, int] = {}
    for queue_entry in queue["queue_entries"]:
        board_payload = queue_entry.get("board")
        if not isinstance(board_payload, dict):
            continue
        board_id = int(board_payload["id"])
        freshness = board_payload.get("freshness")
        freshness_status = (
            str(freshness.get("freshness_status"))
            if isinstance(freshness, dict) and freshness.get("freshness_status") is not None
            else None
        )
        latest_scoring_run = queue_entry.get("latest_scoring_run")
        latest_batch = next(
            (
                _serialize_model_market_board_scoring_batch(batch)
                for batch in batches
                if _scoring_batch_includes_board(batch, board_id)
            ),
            None,
        )
        cadence_status, priority = _resolve_market_board_cadence_status(
            queue_status=str(queue_entry.get("queue_status")),
            freshness_status=freshness_status,
            latest_scoring_run=latest_scoring_run,
        )
        cadence_status_counts[cadence_status] = cadence_status_counts.get(cadence_status, 0) + 1
        priority_counts[priority] = priority_counts.get(priority, 0) + 1
        cadence_entries.append(
            {
                "board": board_payload,
                "queue_status": queue_entry.get("queue_status"),
                "scoring_status": queue_entry.get("scoring_status"),
                "freshness_status": freshness_status,
                "cadence_status": cadence_status,
                "priority": priority,
                "latest_scoring_run": latest_scoring_run,
                "latest_orchestration_batch": latest_batch,
            }
        )
    cadence_entries.sort(
        key=lambda entry: (
            _market_board_priority_rank(str(entry["priority"])),
            _market_board_cadence_sort_key(entry),
        )
    )
    return {
        "overview": {
            "board_count": len(cadence_entries),
            "cadence_status_counts": cadence_status_counts,
            "priority_counts": priority_counts,
        },
        "cadence_entries": cadence_entries,
        "recent_cadence_entries": cadence_entries[:recent_limit],
    }


def _build_market_board_refresh_queue(
    boards: list[ModelMarketBoardRecord],
    refresh_events: list[ModelMarketBoardRefreshRecord],
    *,
    source_name: str | None,
    freshness_status: str | None,
    pending_only: bool,
    recent_limit: int,
) -> dict[str, Any]:
    queue_entries = []
    queue_status_counts: dict[str, int] = {}
    freshness_status_counts: dict[str, int] = {}
    for board in boards:
        board_payload = _serialize_model_market_board(board)
        board_freshness = board_payload.get("freshness")
        source_payload = board.payload.get("source")
        resolved_source_name = (
            str(source_payload.get("source_name"))
            if isinstance(source_payload, dict) and source_payload.get("source_name") is not None
            else None
        )
        resolved_freshness_status = (
            str(board_freshness.get("freshness_status"))
            if isinstance(board_freshness, dict)
            and board_freshness.get("freshness_status") is not None
            else None
        )
        if source_name is not None and resolved_source_name != source_name:
            continue
        if freshness_status is not None and resolved_freshness_status != freshness_status:
            continue
        latest_refresh_event = next(
            (
                _serialize_model_market_board_refresh_event(event)
                for event in refresh_events
                if event.model_market_board_id == board.id
            ),
            None,
        )
        queue_status, refresh_status, refreshable, needs_refresh = (
            _resolve_market_board_refresh_queue_status(
                source_name=resolved_source_name,
                freshness_status=resolved_freshness_status,
                latest_refresh_event=latest_refresh_event,
            )
        )
        if pending_only and not needs_refresh:
            continue
        queue_status_counts[queue_status] = queue_status_counts.get(queue_status, 0) + 1
        freshness_key = resolved_freshness_status or "unspecified"
        freshness_status_counts[freshness_key] = (
            freshness_status_counts.get(freshness_key, 0) + 1
        )
        queue_entries.append(
            {
                "board": board_payload,
                "source_name": resolved_source_name,
                "freshness_status": resolved_freshness_status,
                "queue_status": queue_status,
                "refresh_status": refresh_status,
                "refreshable": refreshable,
                "needs_refresh": needs_refresh,
                "latest_refresh_event": latest_refresh_event,
            }
        )
    queue_entries.sort(
        key=lambda entry: (
            _market_board_refresh_priority_rank(str(entry["queue_status"])),
            str(entry["board"]["board_key"]),
        )
    )
    return {
        "overview": {
            "board_count": len(queue_entries),
            "refreshable_board_count": sum(1 for entry in queue_entries if entry["refreshable"]),
            "pending_refresh_count": sum(1 for entry in queue_entries if entry["needs_refresh"]),
            "queue_status_counts": queue_status_counts,
            "freshness_status_counts": freshness_status_counts,
        },
        "queue_entries": queue_entries,
        "recent_queue_entries": queue_entries[:recent_limit],
    }


def _build_market_board_scoring_queue(
    boards: list[ModelMarketBoardRecord],
    scoring_runs: list[ModelScoringRunRecord],
    *,
    source_name: str | None,
    freshness_status: str | None,
    pending_only: bool,
    recent_limit: int,
) -> dict[str, Any]:
    scoring_runs_by_board: dict[int, list[ModelScoringRunRecord]] = {}
    for scoring_run in scoring_runs:
        if scoring_run.model_market_board_id is None:
            continue
        scoring_runs_by_board.setdefault(scoring_run.model_market_board_id, []).append(
            scoring_run
        )

    queue_entries = []
    scoring_status_counts: dict[str, int] = {}
    queue_status_counts: dict[str, int] = {}
    freshness_status_counts: dict[str, int] = {}
    pending_count = 0
    for board in boards:
        serialized_board = _serialize_model_market_board(board)
        if serialized_board is None:
            continue
        board_source = board.payload.get("source")
        board_source_name = (
            str(board_source.get("source_name"))
            if isinstance(board_source, dict) and board_source.get("source_name") is not None
            else None
        )
        if source_name is not None and board_source_name != source_name:
            continue
        board_freshness = serialized_board.get("freshness")
        resolved_freshness_status = (
            str(board_freshness.get("freshness_status"))
            if isinstance(board_freshness, dict)
            and board_freshness.get("freshness_status") is not None
            else None
        )
        if freshness_status is not None and resolved_freshness_status != freshness_status:
            continue

        board_scoring_runs = sorted(
            scoring_runs_by_board.get(board.id, []),
            key=lambda entry: (
                entry.created_at or datetime.min.replace(tzinfo=timezone.utc),
                entry.id,
            ),
            reverse=True,
        )
        latest_scoring_run = board_scoring_runs[0] if board_scoring_runs else None
        if latest_scoring_run is None:
            scoring_status = "unscored"
            needs_scoring = True
        else:
            board_updated_at = board.updated_at or board.created_at
            latest_scored_at = latest_scoring_run.created_at
            if (
                board_updated_at is not None
                and latest_scored_at is not None
                and latest_scored_at < board_updated_at
            ):
                scoring_status = "stale_after_board_update"
                needs_scoring = True
            else:
                scoring_status = "current"
                needs_scoring = False
        queue_status = "pending_score" if needs_scoring else "up_to_date"
        if pending_only and not needs_scoring:
            continue

        scoring_status_counts[scoring_status] = scoring_status_counts.get(scoring_status, 0) + 1
        queue_status_counts[queue_status] = queue_status_counts.get(queue_status, 0) + 1
        freshness_key = resolved_freshness_status or "unspecified"
        freshness_status_counts[freshness_key] = freshness_status_counts.get(freshness_key, 0) + 1
        if needs_scoring:
            pending_count += 1

        queue_entries.append(
            {
                "board": serialized_board,
                "source_name": board_source_name,
                "freshness_status": resolved_freshness_status,
                "scoring_status": scoring_status,
                "queue_status": queue_status,
                "needs_scoring": needs_scoring,
                "scoring_run_count": len(board_scoring_runs),
                "latest_scoring_run": _serialize_model_scoring_run(latest_scoring_run),
            }
        )

    return {
        "overview": {
            "board_count": len(queue_entries),
            "pending_board_count": pending_count,
            "scoring_status_counts": scoring_status_counts,
            "queue_status_counts": queue_status_counts,
            "freshness_status_counts": freshness_status_counts,
        },
        "queue_entries": queue_entries,
        "recent_queue_entries": queue_entries[:recent_limit],
    }


def _scoring_batch_includes_board(
    batch: ModelMarketBoardScoringBatchRecord,
    board_id: int,
) -> bool:
    orchestration_runs = batch.payload.get("orchestration_runs", [])
    for entry in orchestration_runs:
        if not isinstance(entry, dict):
            continue
        board_payload = entry.get("board")
        if isinstance(board_payload, dict) and int(board_payload.get("id", 0)) == board_id:
            return True
    return False


def _refresh_batch_includes_board(
    batch: ModelMarketBoardRefreshBatchRecord,
    board_id: int,
) -> bool:
    refresh_runs = batch.payload.get("refresh_runs", [])
    for entry in refresh_runs:
        if not isinstance(entry, dict):
            continue
        board_payload = entry.get("board")
        if isinstance(board_payload, dict) and int(board_payload.get("id", 0)) == board_id:
            return True
    return False


def _cadence_batch_includes_board(
    batch: ModelMarketBoardCadenceBatchRecord,
    board_id: int,
) -> bool:
    refresh_runs = batch.payload.get("refresh_result", {}).get("refresh_runs", [])
    for entry in refresh_runs:
        if not isinstance(entry, dict):
            continue
        board_payload = entry.get("board")
        if isinstance(board_payload, dict) and int(board_payload.get("id", 0)) == board_id:
            return True
    orchestration_runs = batch.payload.get("scoring_result", {}).get("orchestration_runs", [])
    for entry in orchestration_runs:
        if not isinstance(entry, dict):
            continue
        board_payload = entry.get("board")
        if isinstance(board_payload, dict) and int(board_payload.get("id", 0)) == board_id:
            return True
    return False


def _build_market_board_refresh_orchestration_result(
    *,
    queue_before: dict[str, Any],
    queue_after: dict[str, Any],
    refresh_runs: list[dict[str, Any]],
) -> dict[str, Any]:
    status_counts = {"created": 0, "updated": 0, "unchanged": 0}
    for entry in refresh_runs:
        status = str(entry.get("refresh_result_status") or "unchanged")
        if status not in status_counts:
            status_counts[status] = 0
        status_counts[status] += 1
    return {
        "queue_before": queue_before,
        "queue_after": queue_after,
        "candidate_board_count": len(refresh_runs),
        "refreshed_board_count": len(refresh_runs),
        "created_board_count": int(status_counts.get("created", 0)),
        "updated_board_count": int(status_counts.get("updated", 0)),
        "unchanged_board_count": int(status_counts.get("unchanged", 0)),
        "refresh_runs": refresh_runs,
    }


def _build_market_board_cadence_result(
    *,
    refresh_result: dict[str, Any],
    scoring_result: dict[str, Any],
) -> dict[str, Any]:
    return {
        "refresh_result": refresh_result,
        "scoring_result": scoring_result,
        "refreshed_board_count": int(refresh_result.get("refreshed_board_count", 0)),
        "scored_board_count": int(scoring_result.get("scored_board_count", 0)),
        "materialized_scoring_run_count": int(
            scoring_result.get("materialized_scoring_run_count", 0)
        ),
        "materialized_opportunity_count": int(
            scoring_result.get("materialized_opportunity_count", 0)
        ),
    }


def _resolve_market_board_refresh_game_date(board_payload: dict[str, Any]) -> date:
    freshness = board_payload.get("freshness")
    refresh_target_date = (
        freshness.get("refresh_target_date")
        if isinstance(freshness, dict)
        else None
    )
    if isinstance(refresh_target_date, str):
        return date.fromisoformat(refresh_target_date)
    game_date_start = board_payload.get("game_date_start")
    if isinstance(game_date_start, str):
        return date.fromisoformat(game_date_start)
    return _utc_today()


def _resolve_market_board_refresh_queue_status(
    *,
    source_name: str | None,
    freshness_status: str | None,
    latest_refresh_event: dict[str, Any] | None,
) -> tuple[str, str, bool, bool]:
    if source_name is None:
        return ("manual_only", "manual_only", False, False)
    if source_name not in MARKET_BOARD_SOURCE_CONFIGS:
        return ("unsupported_source", "unsupported_source", False, False)
    if freshness_status is None:
        refresh_status = (
            str(latest_refresh_event.get("refresh_status"))
            if latest_refresh_event is not None
            else "never_refreshed"
        )
        return ("pending_refresh", refresh_status, True, True)
    if freshness_status == "stale":
        return ("pending_refresh", "stale", True, True)
    if freshness_status == "aging":
        return ("monitor_refresh", "aging", True, False)
    return ("up_to_date", "current", True, False)


def _resolve_market_board_cadence_status(
    *,
    queue_status: str,
    freshness_status: str | None,
    latest_scoring_run: dict[str, Any] | None,
) -> tuple[str, str]:
    if freshness_status == "stale":
        return ("needs_refresh", "high")
    if queue_status == "pending_score":
        if freshness_status == "fresh":
            return ("ready_to_score", "high")
        return ("needs_scoring_attention", "medium")
    if latest_scoring_run is None:
        return ("awaiting_first_score", "medium")
    created_at = latest_scoring_run.get("created_at")
    if isinstance(created_at, str):
        scored_at = datetime.fromisoformat(created_at)
        if scored_at.astimezone(timezone.utc).date() == _utc_today():
            return ("recently_scored", "low")
    return ("monitoring", "low")


def _market_board_priority_rank(priority: str) -> int:
    if priority == "high":
        return 0
    if priority == "medium":
        return 1
    return 2


def _market_board_refresh_priority_rank(queue_status: str) -> int:
    return {
        "pending_refresh": 0,
        "monitor_refresh": 1,
        "up_to_date": 2,
        "manual_only": 3,
        "unsupported_source": 4,
    }.get(queue_status, 5)


def _market_board_cadence_sort_key(entry: dict[str, Any]) -> tuple[int, str]:
    board = entry.get("board")
    board_key = str(board.get("board_key")) if isinstance(board, dict) else ""
    return (_market_board_priority_rank(str(entry.get("priority"))), board_key)


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
    ensure_model_tables(connection)
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
    ensure_model_tables(connection)
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
