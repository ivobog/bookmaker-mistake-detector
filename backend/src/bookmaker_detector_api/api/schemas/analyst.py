from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

RepositoryMode = Literal["in_memory", "postgres"]


class AnalystBacktestListFilters(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    target_task: str | None = None
    team_code: str | None = None
    season_label: str | None = None


class AnalystBacktestRun(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    id: int
    feature_version_id: int
    target_task: str
    team_code: str | None = None
    season_label: str | None = None
    status: str
    selection_policy_name: str
    strategy_name: str
    minimum_train_games: int
    test_window_games: int
    train_ratio: float
    validation_ratio: float
    fold_count: int
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: str | None = None
    completed_at: str | None = None


class AnalystBacktestListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    backtest_run_count: int
    backtest_runs: list[AnalystBacktestRun]


class AnalystBacktestDetailResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    backtest_run: AnalystBacktestRun | None = None


class AnalystOpportunityListFilters(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    target_task: str | None = None
    team_code: str | None = None
    season_label: str | None = None
    source_kind: str | None = None
    scenario_key: str | None = None
    status: str | None = None
    limit: int = Field(default=10, ge=1, le=100)


class AnalystOpportunity(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    id: int
    model_scoring_run_id: int | None = None
    model_selection_snapshot_id: int | None = None
    model_evaluation_snapshot_id: int | None = None
    feature_version_id: int
    target_task: str
    source_kind: str
    scenario_key: str | None = None
    opportunity_key: str
    team_code: str
    opponent_code: str
    season_label: str
    canonical_game_id: int | None = None
    game_date: str | None = None
    policy_name: str
    status: str
    prediction_value: float
    signal_strength: float
    evidence_rating: str | None = None
    recommendation_status: str | None = None
    materialization_batch_id: str
    materialized_at: str | None = None
    materialization_scope: dict[str, Any] = Field(default_factory=dict)
    model_explainability: dict[str, Any] | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: str | None = None
    updated_at: str | None = None


class AnalystOpportunityListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    queue_batch_id: str | None = None
    queue_materialized_at: str | None = None
    queue_scope: dict[str, Any] = Field(default_factory=dict)
    queue_scope_label: str | None = None
    queue_scope_is_scoped: bool = False
    opportunity_count: int
    opportunities: list[AnalystOpportunity]


class AnalystOpportunityDetailResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    opportunity: AnalystOpportunity | None = None


class AnalystTrendFilters(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    feature_key: str = "baseline_team_features_v1"
    team_code: str | None = None
    season_label: str | None = None


class AnalystTrendResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    filters: AnalystTrendFilters
    feature_version: dict[str, Any] | None = None
    snapshot_count: int
    perspective_count: int
    summary: dict[str, Any] = Field(default_factory=dict)
    latest_perspective: dict[str, Any] | None = None


class AnalystPatternFilters(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    feature_key: str = "baseline_team_features_v1"
    target_task: str = "spread_error_regression"
    team_code: str | None = None
    season_label: str | None = None
    dimensions: list[str] | None = None
    min_sample_size: int = Field(default=2, ge=1, le=100)
    limit: int = Field(default=50, ge=1, le=200)


class AnalystPatternResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    filters: AnalystPatternFilters
    feature_version: dict[str, Any] | None = None
    row_count: int
    task: dict[str, Any] | None = None
    pattern_count: int
    patterns: list[dict[str, Any]] = Field(default_factory=list)


class AnalystComparableFilters(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    feature_key: str = "baseline_team_features_v1"
    target_task: str = "spread_error_regression"
    team_code: str | None = None
    season_label: str | None = None
    dimensions: list[str] | None = None
    canonical_game_id: int | None = None
    condition_values: list[str] | None = None
    pattern_key: str | None = None
    limit: int = Field(default=20, ge=1, le=100)


class AnalystComparableResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    filters: AnalystComparableFilters
    feature_version: dict[str, Any] | None = None
    row_count: int
    task: dict[str, Any] | None = None
    anchor_case: dict[str, Any] | None = None
    comparable_count: int
    comparables: list[dict[str, Any]] = Field(default_factory=list)
    pattern_key: str | None = None


class AnalystEvidenceFilters(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    feature_key: str = "baseline_team_features_v1"
    target_task: str = "spread_error_regression"
    team_code: str | None = None
    season_label: str | None = None
    dimensions: list[str] | None = None
    canonical_game_id: int | None = None
    condition_values: list[str] | None = None
    pattern_key: str | None = None
    comparable_limit: int = Field(default=10, ge=1, le=100)
    min_pattern_sample_size: int = Field(default=1, ge=1, le=100)
    train_ratio: float = 0.7
    validation_ratio: float = 0.15
    drop_null_targets: bool = True


class AnalystEvidenceResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    filters: AnalystEvidenceFilters
    feature_version: dict[str, Any] | None = None
    row_count: int
    task: dict[str, Any] | None = None
    evidence: dict[str, Any] = Field(default_factory=dict)
