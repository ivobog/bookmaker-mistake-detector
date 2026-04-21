export type SharedOpportunityRecord = {
  id: number;
  model_scoring_run_id?: number | null;
  model_selection_snapshot_id?: number | null;
  model_evaluation_snapshot_id?: number | null;
  feature_version_id?: number | null;
  target_task: string;
  source_kind: string;
  scenario_key: string | null;
  opportunity_key: string;
  team_code: string;
  opponent_code: string;
  season_label: string;
  canonical_game_id: number | null;
  game_date: string;
  policy_name: string;
  status: string;
  prediction_value: number;
  signal_strength: number;
  evidence_rating: string | null;
  recommendation_status: string | null;
  materialization_batch_id: string;
  materialized_at: string | null;
  materialization_scope: {
    team_code: string | null;
    season_label: string | null;
    canonical_game_id: number | null;
    source: string | null;
    scope_key: string | null;
  };
  model_explainability: {
    model_family: string | null;
    selected_feature: string | null;
    threshold: number | null;
    left_prediction: number | null;
    right_prediction: number | null;
    selected_feature_value: number | null;
    branch: string | null;
  } | null;
  payload: Record<string, unknown>;
  created_at: string | null;
  updated_at: string | null;
};

export type SharedBacktestPredictionMetrics = {
  prediction_count: number;
  mae: number | null;
  rmse: number | null;
  average_prediction_value: number | null;
  average_realized_residual: number | null;
};

export type SharedBacktestEdgeBucketPerformance = {
  bet_count: number;
  win_count: number;
  loss_count: number;
  push_count: number;
  profit_units: number;
  hit_rate: number | null;
  push_rate: number | null;
  roi: number | null;
};

export type SharedBacktestStrategySummary = {
  strategy_name: string;
  threshold?: number;
  bet_count: number;
  win_count: number;
  loss_count: number;
  push_count: number;
  hit_rate: number | null;
  push_rate: number | null;
  roi: number | null;
  profit_units: number;
  edge_bucket_performance: Record<string, SharedBacktestEdgeBucketPerformance>;
};

export type SharedBacktestSelectedModel = {
  evaluation_snapshot_id: number;
  model_training_run_id: number;
  model_family: string;
  selected_feature: string | null;
  fallback_strategy: string | null;
  validation_metric_value: number | null;
  test_metric_value: number | null;
};

export type SharedBacktestFoldSummary = {
  fold_index: number;
  train_game_count: number;
  test_game_count: number;
  train_game_ids: number[];
  test_game_ids: number[];
  selected_model: SharedBacktestSelectedModel;
  prediction_metrics: SharedBacktestPredictionMetrics;
  strategies: Record<string, SharedBacktestStrategySummary>;
};

export type SharedBacktestSummary = {
  target_task: string;
  selection_policy_name: string;
  strategy_name: string;
  minimum_train_games: number;
  test_window_games: number;
  dataset_row_count: number;
  dataset_game_count: number;
  fold_count: number;
  selected_model_family_counts: Record<string, number>;
  prediction_metrics: SharedBacktestPredictionMetrics;
  strategy_results: Record<string, SharedBacktestStrategySummary>;
  folds: SharedBacktestFoldSummary[];
};

export type SharedBacktestRun = {
  id: number;
  target_task: string;
  strategy_name: string;
  fold_count: number;
  selection_policy_name: string;
  minimum_train_games: number;
  test_window_games: number;
  payload: SharedBacktestSummary;
  created_at: string | null;
  completed_at: string | null;
};

export type SharedBacktestDailyBucket = {
  date: string;
  run_count: number;
  fold_count: number;
  bet_count: number;
  profit_units: number;
};

export type SharedBacktestHistory = {
  overview: {
    run_count: number;
    status_counts: Record<string, number>;
    target_task_counts: Record<string, number>;
    strategy_counts: Record<string, number>;
    best_candidate_threshold_run: SharedBacktestRun | null;
    latest_run: SharedBacktestRun | null;
  };
  daily_buckets: SharedBacktestDailyBucket[];
  recent_runs: SharedBacktestRun[];
};

export type SharedBacktestHistoryResponse = {
  filters: {
    target_task: string;
    team_code: string | null;
    season_label: string | null;
    recent_limit: number;
  };
  model_backtest_history: SharedBacktestHistory;
};

export type SharedBacktestRunResponse = {
  filters: Record<string, string | number | null>;
  feature_version: SharedModelFeatureVersion | null;
  backtest_run: SharedBacktestRun | null;
  summary: SharedBacktestSummary;
};

export type SharedOpportunityHistoryResponse = {
  filters: {
    target_task: string;
    team_code: string | null;
    season_label: string | null;
    recent_limit: number;
  };
  model_opportunity_history: {
    overview: {
      opportunity_count: number;
      status_counts: Record<string, number>;
      source_kind_counts: Record<string, number>;
      evidence_rating_counts?: Record<string, number>;
      latest_opportunity: SharedOpportunityRecord | null;
    };
    recent_opportunities: SharedOpportunityRecord[];
  };
};

export type SharedOpportunityListResponse = {
  queue_batch_id: string | null;
  queue_materialized_at: string | null;
  queue_scope: {
    team_code: string | null;
    season_label: string | null;
    canonical_game_id: number | null;
    source: string | null;
    scope_key: string | null;
  };
  queue_scope_label: string | null;
  queue_scope_is_scoped: boolean;
  opportunity_count: number;
  opportunities: SharedOpportunityRecord[];
};

export type SharedOpportunityDetailResponse = {
  opportunity: SharedOpportunityRecord | null;
};

export type SharedOpportunityMaterializeResponse = {
  materialized_count: number;
  opportunity_count: number;
  opportunities: SharedOpportunityRecord[];
};

export type SharedModelTrainingRun = {
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

export type SharedModelFeatureVersion = {
  feature_key: string;
  version_label?: string | null;
  id?: number | null;
};

export type SharedModelRegistryEntry = {
  id: number;
  model_key: string;
  target_task: string;
  model_family: string;
  version_label: string;
  description: string;
  config: Record<string, unknown>;
  created_at: string | null;
};

export type SharedModelTrainingDailyBucket = {
  date: string;
  run_count: number;
  fallback_count?: number;
};

export type SharedModelTrainingHistory = {
  overview: {
    run_count: number;
    fallback_run_count: number;
    best_overall: SharedModelTrainingRun | null;
    latest_run: SharedModelTrainingRun | null;
  };
  daily_buckets?: SharedModelTrainingDailyBucket[];
  recent_runs: SharedModelTrainingRun[];
};

export type SharedModelHistoryResponse = {
  model_history: SharedModelTrainingHistory;
};

export type SharedSelectionSnapshot = {
  id: number;
  model_evaluation_snapshot_id: number | null;
  model_training_run_id: number | null;
  model_registry_id: number | null;
  feature_version_id: number | null;
  target_task: string;
  model_family: string;
  selection_policy_name: string;
  rationale: string | Record<string, unknown> | null;
  is_active: boolean;
  created_at: string | null;
};

export type SharedEvaluationSnapshot = {
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

export type SharedModelEvaluationDailyBucket = {
  date: string;
  snapshot_count: number;
  fallback_count?: number;
};

export type SharedEvaluationHistory = {
  overview: {
    snapshot_count: number;
    fallback_strategy_counts?: Record<string, number>;
    latest_snapshot?: SharedEvaluationSnapshot | null;
  };
  daily_buckets?: SharedModelEvaluationDailyBucket[];
  recent_snapshots: SharedEvaluationSnapshot[];
};

export type SharedScoringRunDetail = {
  id: number;
  model_selection_snapshot_id: number | null;
  model_evaluation_snapshot_id: number | null;
  feature_version_id: number | null;
  target_task: string;
  scenario_key: string;
  season_label: string;
  game_date: string;
  home_team_code: string;
  away_team_code: string;
  home_spread_line: number | null;
  total_line: number | null;
  policy_name: string;
  prediction_count: number;
  candidate_opportunity_count: number;
  review_opportunity_count: number;
  discarded_opportunity_count: number;
  payload: Record<string, unknown>;
  created_at: string | null;
};

export type SharedScoringRunDetailResponse = {
  scoring_run: SharedScoringRunDetail | null;
};

export type SharedModelRunDetailResponse = {
  model_run: SharedModelTrainingRun | null;
};

export type SharedSelectionDetailResponse = {
  selection: SharedSelectionSnapshot | null;
};

export type SharedEvaluationDetailResponse = {
  evaluation_snapshot: SharedEvaluationSnapshot | null;
};

export type SharedModelRegistryEnvelope = {
  model_registry_count: number;
  model_registry: SharedModelRegistryEntry[];
};

export type SharedModelRunsEnvelope = {
  model_run_count: number;
  model_runs: SharedModelTrainingRun[];
};

export type SharedEvaluationHistoryEnvelope = {
  model_evaluation_history: SharedEvaluationHistory;
};

export type SharedModelEvaluationsEnvelope = {
  evaluation_snapshot_count: number;
  evaluation_snapshots: SharedEvaluationSnapshot[];
};

export type SharedTaskCapability = {
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

export type SharedCapabilitiesUiDefaults = {
  default_feature_key: string;
  default_target_task: string | null;
  default_train_ratio: number;
  default_validation_ratio: number;
};

export type SharedCapabilitiesResponse = {
  task_count: number;
  target_tasks: SharedTaskCapability[];
  ui_defaults: SharedCapabilitiesUiDefaults;
};

export type SharedModelSummary = {
  run_count: number;
  status_counts?: Record<string, number>;
  usable_run_count?: number;
  fallback_run_count?: number;
  best_overall?: SharedModelTrainingRun | null;
  latest_run: SharedModelTrainingRun | null;
  best_by_family?: Record<string, SharedModelTrainingRun | null>;
};

export type SharedModelSummaryEnvelope = {
  model_summary: SharedModelSummary;
};

export type SharedSelectionHistory = {
  overview: {
    selection_count: number;
    active_selection_count?: number;
    model_family_counts?: Record<string, number>;
    latest_selection?: SharedSelectionSnapshot | null;
  };
  recent_selections: SharedSelectionSnapshot[];
};

export type SharedSelectionHistoryEnvelope = {
  model_selection_history: SharedSelectionHistory;
};

export type SharedModelSelectionsEnvelope = {
  selection_count: number;
  selections: SharedSelectionSnapshot[];
};

export type SharedModelTrainEnvelope = {
  feature_version: SharedModelFeatureVersion | null;
  dataset_row_count: number;
  model_runs: SharedModelTrainingRun[];
  best_model: SharedModelTrainingRun | null;
  persisted_run_count?: number;
};

export type SharedFeatureMaterializeEnvelope = {
  feature_version: SharedModelFeatureVersion | null;
  canonical_game_count: number;
  snapshots_saved: number;
};

export type SharedModelSelectEnvelope = {
  selection_policy_name: string;
  selected_snapshot: SharedEvaluationSnapshot | null;
  active_selection: SharedSelectionSnapshot | null;
  selection_count: number;
};
