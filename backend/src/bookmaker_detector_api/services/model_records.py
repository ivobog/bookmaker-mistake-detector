from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable


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
    primary_metric_direction: str = "lower_is_better"
    selection_score: float | None = None
    selection_score_name: str | None = None
    created_at: datetime | None = None


@dataclass(slots=True)
class TargetTaskDefinitionRecord:
    task_key: str
    task_kind: str
    label: str
    description: str
    market_type: str
    primary_metric_name: str
    metric_direction: str
    opportunity_policy_name: str
    is_enabled: bool
    config: dict[str, Any]
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(slots=True)
class ModelFamilyCapabilityRecord:
    id: int | None
    model_family: str
    target_task: str
    is_enabled: bool
    config: dict[str, Any]
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
    materialization_batch_id: str
    materialized_at: datetime
    materialization_scope_team_code: str | None
    materialization_scope_season_label: str | None
    materialization_scope_canonical_game_id: int | None
    materialization_scope_source: str
    materialization_scope_key: str
    payload: dict[str, Any]
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(slots=True)
class ModelBacktestRunRecord:
    id: int
    feature_version_id: int
    target_task: str
    team_code: str | None
    season_label: str | None
    status: str
    selection_policy_name: str
    strategy_name: str
    minimum_train_games: int
    test_window_games: int
    train_ratio: float
    validation_ratio: float
    fold_count: int
    payload: dict[str, Any]
    created_at: datetime | None = None
    completed_at: datetime | None = None
