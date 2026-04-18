from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

RepositoryMode = Literal["in_memory", "postgres"]


class AdminBacktestHistoryFilters(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    target_task: str | None = None
    team_code: str | None = None
    season_label: str | None = None
    recent_limit: int = Field(default=10, ge=1, le=50)


class AdminModelRegistryFilters(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    feature_key: str = "baseline_team_features_v1"
    target_task: str | None = None
    team_code: str | None = None
    season_label: str | None = None


class AdminModelRunsFilters(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    feature_key: str = "baseline_team_features_v1"
    target_task: str | None = "spread_error_regression"
    team_code: str | None = None
    season_label: str | None = None


class AdminModelHistoryFilters(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    feature_key: str = "baseline_team_features_v1"
    target_task: str | None = None
    team_code: str | None = None
    season_label: str | None = None
    recent_limit: int = Field(default=10, ge=1, le=50)


class AdminModelSummaryFilters(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    feature_key: str = "baseline_team_features_v1"
    target_task: str | None = "spread_error_regression"
    team_code: str | None = None
    season_label: str | None = None


class AdminModelEvaluationsFilters(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    feature_key: str = "baseline_team_features_v1"
    target_task: str | None = "spread_error_regression"
    model_family: str | None = None
    team_code: str | None = None
    season_label: str | None = None


class AdminEvaluationHistoryFilters(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    feature_key: str = "baseline_team_features_v1"
    target_task: str | None = None
    model_family: str | None = None
    team_code: str | None = None
    season_label: str | None = None
    recent_limit: int = Field(default=10, ge=1, le=50)


class AdminModelSelectionsFilters(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    feature_key: str = "baseline_team_features_v1"
    target_task: str | None = "spread_error_regression"
    team_code: str | None = None
    season_label: str | None = None
    active_only: bool = False


class AdminSelectionHistoryFilters(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    feature_key: str = "baseline_team_features_v1"
    target_task: str | None = None
    team_code: str | None = None
    season_label: str | None = None
    recent_limit: int = Field(default=10, ge=1, le=50)


class AdminScoringPreviewFilters(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    feature_key: str = "baseline_team_features_v1"
    target_task: str = "spread_error_regression"
    team_code: str | None = None
    season_label: str | None = None
    canonical_game_id: int | None = None
    limit: int = Field(default=10, ge=1, le=100)
    include_evidence: bool = True
    dimensions: list[str] | None = None
    comparable_limit: int = Field(default=5, ge=1, le=50)
    min_pattern_sample_size: int = Field(default=1, ge=1, le=100)


class AdminFutureGamePreviewFilters(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    feature_key: str = "baseline_team_features_v1"
    target_task: str | None = "spread_error_regression"
    season_label: str = "2025-2026"
    game_date: str = "2026-04-20"
    team_code: str | None = None
    home_team_code: str = "LAL"
    away_team_code: str = "BOS"
    home_spread_line: float | None = None
    total_line: float | None = None
    include_evidence: bool = True
    dimensions: list[str] | None = None
    comparable_limit: int = Field(default=5, ge=1, le=50)
    min_pattern_sample_size: int = Field(default=1, ge=1, le=100)
    recent_limit: int | None = Field(default=None, ge=1, le=50)


class AdminOpportunityHistoryFilters(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    feature_key: str = "baseline_team_features_v1"
    target_task: str | None = None
    team_code: str | None = None
    season_label: str | None = None
    source_kind: str | None = None
    scenario_key: str | None = None
    recent_limit: int = Field(default=10, ge=1, le=50)


class AdminModelRun(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    id: int
    model_registry_id: int
    feature_version_id: int
    target_task: str
    team_code: str | None = None
    season_label: str | None = None
    status: str
    train_ratio: float
    validation_ratio: float
    artifact: dict[str, Any] = Field(default_factory=dict)
    metrics: dict[str, Any] = Field(default_factory=dict)
    created_at: str | None = None
    completed_at: str | None = None


class AdminModelRegistryEntry(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    id: int
    model_key: str
    target_task: str
    model_family: str
    version_label: str
    description: str
    config: dict[str, Any] = Field(default_factory=dict)
    created_at: str | None = None


class AdminEvaluationSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    id: int
    model_training_run_id: int
    model_registry_id: int
    feature_version_id: int
    target_task: str
    model_family: str
    selected_feature: str | None = None
    fallback_strategy: str | None = None
    primary_metric_name: str
    validation_metric_value: float | None = None
    test_metric_value: float | None = None
    validation_prediction_count: int
    test_prediction_count: int
    snapshot: dict[str, Any] = Field(default_factory=dict)
    created_at: str | None = None


class AdminSelectionSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    id: int
    model_evaluation_snapshot_id: int
    model_training_run_id: int
    model_registry_id: int
    feature_version_id: int
    target_task: str
    model_family: str
    selection_policy_name: str
    rationale: dict[str, Any] = Field(default_factory=dict)
    is_active: bool
    created_at: str | None = None


class AdminScoringRun(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    id: int
    model_selection_snapshot_id: int | None = None
    model_evaluation_snapshot_id: int | None = None
    feature_version_id: int
    target_task: str
    scenario_key: str
    season_label: str
    game_date: str
    home_team_code: str
    away_team_code: str
    home_spread_line: float | None = None
    total_line: float | None = None
    policy_name: str | None = None
    prediction_count: int
    candidate_opportunity_count: int
    review_opportunity_count: int
    discarded_opportunity_count: int
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: str | None = None


class AdminBacktestHistoryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    filters: AdminBacktestHistoryFilters
    model_backtest_history: dict[str, Any] = Field(default_factory=dict)


class AdminModelRegistryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    filters: AdminModelRegistryFilters
    model_registry_count: int
    model_registry: list[AdminModelRegistryEntry] = Field(default_factory=list)


class AdminModelRunsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    filters: AdminModelRunsFilters
    model_run_count: int
    model_runs: list[AdminModelRun] = Field(default_factory=list)


class AdminModelHistoryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    filters: AdminModelHistoryFilters
    model_history: dict[str, Any] = Field(default_factory=dict)


class AdminModelSummaryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    filters: AdminModelSummaryFilters
    model_summary: dict[str, Any] = Field(default_factory=dict)


class AdminModelRunDetailResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    model_run: AdminModelRun | None = None


class AdminModelEvaluationsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    filters: AdminModelEvaluationsFilters
    evaluation_snapshot_count: int
    evaluation_snapshots: list[AdminEvaluationSnapshot] = Field(default_factory=list)


class AdminEvaluationHistoryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    filters: AdminEvaluationHistoryFilters
    model_evaluation_history: dict[str, Any] = Field(default_factory=dict)


class AdminEvaluationDetailResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    evaluation_snapshot: AdminEvaluationSnapshot | None = None


class AdminModelSelectionsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    filters: AdminModelSelectionsFilters
    selection_count: int
    selections: list[AdminSelectionSnapshot] = Field(default_factory=list)


class AdminSelectionHistoryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    filters: AdminSelectionHistoryFilters
    model_selection_history: dict[str, Any] = Field(default_factory=dict)


class AdminSelectionDetailResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    selection: AdminSelectionSnapshot | None = None


class AdminScoringPreviewResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    filters: AdminScoringPreviewFilters
    feature_version: dict[str, Any] | None = None
    active_selection: dict[str, Any] | None = None
    active_evaluation_snapshot: dict[str, Any] | None = None
    row_count: int = 0
    scored_prediction_count: int = 0
    prediction_summary: dict[str, Any] = Field(default_factory=dict)
    predictions: list[dict[str, Any]] = Field(default_factory=list)


class AdminFutureGamePreviewResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    filters: AdminFutureGamePreviewFilters
    feature_version: dict[str, Any] | None = None
    active_selection: dict[str, Any] | None = None
    active_evaluation_snapshot: dict[str, Any] | None = None
    scenario: dict[str, Any] | None = None
    scored_prediction_count: int = 0
    prediction_summary: dict[str, Any] = Field(default_factory=dict)
    predictions: list[dict[str, Any]] = Field(default_factory=list)
    opportunity_preview: list[dict[str, Any]] = Field(default_factory=list)


class AdminScoringRunsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    filters: AdminFutureGamePreviewFilters
    scoring_run_count: int
    scoring_runs: list[AdminScoringRun] = Field(default_factory=list)


class AdminScoringRunDetailResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    filters: AdminFutureGamePreviewFilters
    scoring_run: dict[str, Any] | None = None


class AdminScoringHistoryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    filters: AdminFutureGamePreviewFilters
    model_scoring_history: dict[str, Any] = Field(default_factory=dict)


class AdminOpportunityHistoryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    repository_mode: RepositoryMode
    filters: AdminOpportunityHistoryFilters
    model_opportunity_history: dict[str, Any] = Field(default_factory=dict)
