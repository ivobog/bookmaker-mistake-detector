export type ModelAdminFeatureVersion = {
  feature_key: string;
  version_label?: string | null;
  id?: number | null;
};

export type ModelAdminTaskCapability = {
  task_key: string;
  task_kind: string;
  label: string;
  description: string;
  market_type: string;
  primary_metric_name: string;
  metric_direction: string;
  supported_model_families: string[];
  default_selection_policy_name: string;
  valid_selection_policy_names: string[];
  default_opportunity_policy_name: string;
  scoring_output_semantics?: string | null;
  signal_strength_interpretation?: string | null;
  workflow_support: Record<string, boolean>;
  is_enabled: boolean;
  config: Record<string, unknown>;
};

export type ModelAdminCapabilitiesUiDefaults = {
  default_feature_key: string;
  default_target_task: string | null;
  default_train_ratio: number;
  default_validation_ratio: number;
};

export type ModelAdminCapabilitiesResponse = {
  task_count: number;
  target_tasks: ModelAdminTaskCapability[];
  ui_defaults: ModelAdminCapabilitiesUiDefaults;
};

export type ModelAdminQueryOptions = {
  featureKey?: string;
  recentLimit?: number;
  seasonLabel?: string | null;
  targetTask?: string | null;
  teamCode?: string | null;
  trainRatio?: number;
  validationRatio?: number;
};

export type ModelAdminRunFilters = ModelAdminQueryOptions;

export type ModelAdminEvaluationFilters = ModelAdminQueryOptions & {
  modelFamily?: string | null;
};

export type ModelAdminSelectionFilters = ModelAdminQueryOptions & {
  activeOnly?: boolean;
};

export type ModelAdminSelectionMutationInput = ModelAdminQueryOptions & {
  selectionPolicyName?: string;
};

export type ModelAdminTrainingMutationInput = ModelAdminQueryOptions;

export type ModelAdminRegistryFilters = ModelAdminQueryOptions;

export type ModelAdminFilterResponse = {
  feature_key?: string;
  target_task?: string | null;
  team_code?: string | null;
  season_label?: string | null;
  train_ratio?: number;
  validation_ratio?: number;
  recent_limit?: number;
  model_family?: string | null;
  active_only?: boolean;
};

export type ModelAdminRun = {
  id: number;
  model_registry_id: number | null;
  feature_version_id: number | null;
  target_task: string;
  team_code: string | null;
  season_label: string | null;
  status: string;
  train_ratio: number;
  validation_ratio: number;
  artifact: Record<string, unknown>;
  metrics: Record<string, unknown>;
  created_at: string | null;
  completed_at: string | null;
};

export type ModelAdminRegistryEntry = {
  id: number;
  model_key: string;
  target_task: string;
  model_family: string;
  version_label: string;
  description: string;
  config: Record<string, unknown>;
  created_at: string | null;
};

export type ModelAdminEvaluationSnapshot = {
  id: number;
  model_training_run_id: number | null;
  model_registry_id: number | null;
  feature_version_id: number | null;
  target_task: string;
  model_family: string;
  selected_feature: string | null;
  fallback_strategy: string | null;
  primary_metric_name: string | null;
  validation_metric_value: number | null;
  test_metric_value: number | null;
  validation_prediction_count: number;
  test_prediction_count: number;
  snapshot: Record<string, unknown>;
  created_at: string | null;
};

export type ModelAdminSelectionSnapshot = {
  id: number;
  model_evaluation_snapshot_id: number | null;
  model_training_run_id: number | null;
  model_registry_id: number | null;
  feature_version_id: number | null;
  target_task: string;
  model_family: string;
  selection_policy_name: string;
  rationale: Record<string, unknown>;
  is_active: boolean;
  created_at: string | null;
};

export type ModelAdminTrainingDailyBucket = {
  date: string;
  run_count: number;
  fallback_count?: number;
};

export type ModelAdminEvaluationDailyBucket = {
  date: string;
  snapshot_count: number;
  fallback_count?: number;
};

export type ModelAdminTrainingHistory = {
  overview: {
    run_count: number;
    fallback_run_count: number;
    best_overall: ModelAdminRun | null;
    latest_run: ModelAdminRun | null;
  };
  daily_buckets: ModelAdminTrainingDailyBucket[];
  recent_runs: ModelAdminRun[];
};

export type ModelAdminSummary = {
  run_count: number;
  status_counts: Record<string, number>;
  usable_run_count?: number;
  fallback_run_count?: number;
  best_overall: ModelAdminRun | null;
  latest_run: ModelAdminRun | null;
  best_by_family?: Record<string, ModelAdminRun | null>;
};

export type ModelAdminEvaluationHistory = {
  overview: {
    snapshot_count: number;
    fallback_strategy_counts?: Record<string, number>;
    latest_snapshot?: ModelAdminEvaluationSnapshot | null;
  };
  daily_buckets: ModelAdminEvaluationDailyBucket[];
  recent_snapshots: ModelAdminEvaluationSnapshot[];
};

export type ModelAdminSelectionHistory = {
  overview: {
    selection_count: number;
    active_selection_count?: number;
    model_family_counts?: Record<string, number>;
    latest_selection?: ModelAdminSelectionSnapshot | null;
  };
  recent_selections: ModelAdminSelectionSnapshot[];
};

export type ModelAdminRegistryResponse = {
  filters: ModelAdminFilterResponse;
  model_registry_count: number;
  model_registry: ModelAdminRegistryEntry[];
};

export type ModelAdminRunsResponse = {
  filters: ModelAdminFilterResponse;
  model_run_count: number;
  model_runs: ModelAdminRun[];
};

export type ModelAdminRunDetailResponse = {
  model_run: ModelAdminRun | null;
};

export type ModelAdminSummaryResponse = {
  filters: ModelAdminFilterResponse;
  model_summary: ModelAdminSummary;
};

export type ModelAdminHistoryResponse = {
  filters: ModelAdminFilterResponse;
  model_history: ModelAdminTrainingHistory;
};

export type ModelAdminEvaluationsResponse = {
  filters: ModelAdminFilterResponse;
  evaluation_snapshot_count: number;
  evaluation_snapshots: ModelAdminEvaluationSnapshot[];
};

export type ModelAdminEvaluationHistoryResponse = {
  filters: ModelAdminFilterResponse;
  model_evaluation_history: ModelAdminEvaluationHistory;
};

export type ModelAdminEvaluationDetailResponse = {
  evaluation_snapshot: ModelAdminEvaluationSnapshot | null;
};

export type ModelAdminSelectionsResponse = {
  filters: ModelAdminFilterResponse;
  selection_count: number;
  selections: ModelAdminSelectionSnapshot[];
};

export type ModelAdminSelectionHistoryResponse = {
  filters: ModelAdminFilterResponse;
  model_selection_history: ModelAdminSelectionHistory;
};

export type ModelAdminSelectionDetailResponse = {
  selection: ModelAdminSelectionSnapshot | null;
};

export type ModelAdminTrainResponse = {
  filters: ModelAdminFilterResponse;
  feature_version: ModelAdminFeatureVersion | null;
  dataset_row_count: number;
  model_runs: ModelAdminRun[];
  best_model: ModelAdminRun | null;
  persisted_run_count?: number;
};

export type ModelAdminFeatureMaterializeResponse = {
  filters: {
    feature_key: string;
  };
  feature_version: ModelAdminFeatureVersion | null;
  canonical_game_count: number;
  snapshots_saved: number;
};

export type ModelAdminSelectResponse = {
  filters: ModelAdminFilterResponse;
  selection_policy_name: string;
  selected_snapshot: ModelAdminEvaluationSnapshot | null;
  active_selection: ModelAdminSelectionSnapshot | null;
  selection_count: number;
};
