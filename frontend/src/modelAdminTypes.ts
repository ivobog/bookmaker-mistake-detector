export type ModelAdminRepositoryMode = "in_memory" | "postgres";

export type ModelAdminFeatureVersion = {
  feature_key: string;
  version_label?: string | null;
  id?: number | null;
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
  repository_mode: ModelAdminRepositoryMode;
  filters: ModelAdminFilterResponse;
  model_registry_count: number;
  model_registry: ModelAdminRegistryEntry[];
};

export type ModelAdminRunsResponse = {
  repository_mode: ModelAdminRepositoryMode;
  filters: ModelAdminFilterResponse;
  model_run_count: number;
  model_runs: ModelAdminRun[];
};

export type ModelAdminRunDetailResponse = {
  repository_mode: ModelAdminRepositoryMode;
  model_run: ModelAdminRun | null;
};

export type ModelAdminSummaryResponse = {
  repository_mode: ModelAdminRepositoryMode;
  filters: ModelAdminFilterResponse;
  model_summary: ModelAdminSummary;
};

export type ModelAdminHistoryResponse = {
  repository_mode: ModelAdminRepositoryMode;
  filters: ModelAdminFilterResponse;
  model_history: ModelAdminTrainingHistory;
};

export type ModelAdminEvaluationsResponse = {
  repository_mode: ModelAdminRepositoryMode;
  filters: ModelAdminFilterResponse;
  evaluation_snapshot_count: number;
  evaluation_snapshots: ModelAdminEvaluationSnapshot[];
};

export type ModelAdminEvaluationHistoryResponse = {
  repository_mode: ModelAdminRepositoryMode;
  filters: ModelAdminFilterResponse;
  model_evaluation_history: ModelAdminEvaluationHistory;
};

export type ModelAdminEvaluationDetailResponse = {
  repository_mode: ModelAdminRepositoryMode;
  evaluation_snapshot: ModelAdminEvaluationSnapshot | null;
};

export type ModelAdminSelectionsResponse = {
  repository_mode: ModelAdminRepositoryMode;
  filters: ModelAdminFilterResponse;
  selection_count: number;
  selections: ModelAdminSelectionSnapshot[];
};

export type ModelAdminSelectionHistoryResponse = {
  repository_mode: ModelAdminRepositoryMode;
  filters: ModelAdminFilterResponse;
  model_selection_history: ModelAdminSelectionHistory;
};

export type ModelAdminSelectionDetailResponse = {
  repository_mode: ModelAdminRepositoryMode;
  selection: ModelAdminSelectionSnapshot | null;
};

export type ModelAdminTrainResponse = {
  repository_mode: ModelAdminRepositoryMode;
  filters: ModelAdminFilterResponse;
  feature_version: ModelAdminFeatureVersion | null;
  dataset_row_count: number;
  model_runs: ModelAdminRun[];
  best_model: ModelAdminRun | null;
  persisted_run_count?: number;
};

export type ModelAdminSelectResponse = {
  repository_mode: ModelAdminRepositoryMode;
  filters: ModelAdminFilterResponse;
  selection_policy_name: string;
  selected_snapshot: ModelAdminEvaluationSnapshot | null;
  active_selection: ModelAdminSelectionSnapshot | null;
  selection_count: number;
};
