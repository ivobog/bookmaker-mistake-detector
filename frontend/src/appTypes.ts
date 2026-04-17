export type BacktestHistoryResponse = {
  repository_mode: string;
  filters: {
    target_task: string;
    team_code: string | null;
    season_label: string | null;
    recent_limit: number;
  };
  model_backtest_history: {
    overview: {
      run_count: number;
      status_counts: Record<string, number>;
      target_task_counts: Record<string, number>;
      strategy_counts: Record<string, number>;
      best_candidate_threshold_run: BacktestRun | null;
      latest_run: BacktestRun | null;
    };
    daily_buckets: Array<{
      date: string;
      run_count: number;
      fold_count: number;
      bet_count: number;
      profit_units: number;
    }>;
    recent_runs: BacktestRun[];
  };
};

export type BacktestRunResponse = {
  repository_mode: string;
  filters: Record<string, string | number | null>;
  feature_version: {
    feature_key: string;
    version_label: string;
  } | null;
  backtest_run: BacktestRun | null;
  summary: BacktestSummary;
};

export type BacktestRun = {
  id: number;
  target_task: string;
  strategy_name: string;
  fold_count: number;
  selection_policy_name: string;
  minimum_train_games: number;
  test_window_games: number;
  payload: BacktestSummary;
  created_at: string | null;
  completed_at: string | null;
};

export type BacktestSummary = {
  target_task: string;
  selection_policy_name: string;
  strategy_name: string;
  minimum_train_games: number;
  test_window_games: number;
  dataset_row_count: number;
  dataset_game_count: number;
  fold_count: number;
  selected_model_family_counts: Record<string, number>;
  prediction_metrics: {
    prediction_count: number;
    mae: number | null;
    rmse: number | null;
    average_prediction_value: number | null;
    average_realized_residual: number | null;
  };
  strategy_results: Record<string, StrategySummary>;
  folds: FoldSummary[];
};

export type StrategySummary = {
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
  edge_bucket_performance: Record<
    string,
    {
      bet_count: number;
      win_count: number;
      loss_count: number;
      push_count: number;
      profit_units: number;
      hit_rate: number | null;
      push_rate: number | null;
      roi: number | null;
    }
  >;
};

export type FoldSummary = {
  fold_index: number;
  train_game_count: number;
  test_game_count: number;
  train_game_ids: number[];
  test_game_ids: number[];
  selected_model: {
    evaluation_snapshot_id: number;
    model_training_run_id: number;
    model_family: string;
    selected_feature: string | null;
    fallback_strategy: string | null;
    validation_metric_value: number | null;
    test_metric_value: number | null;
  };
  prediction_metrics: BacktestSummary["prediction_metrics"];
  strategies: Record<string, StrategySummary>;
};

export type OpportunityRecord = {
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
  payload: Record<string, unknown>;
  created_at: string | null;
  updated_at: string | null;
};

export type OpportunityHistoryResponse = {
  repository_mode: string;
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
      latest_opportunity: OpportunityRecord | null;
    };
    recent_opportunities: OpportunityRecord[];
  };
};

export type OpportunityListResponse = {
  repository_mode: string;
  opportunity_count: number;
  opportunities: OpportunityRecord[];
};

export type OpportunityDetailResponse = {
  repository_mode: string;
  opportunity: OpportunityRecord | null;
};

export type OpportunityMaterializeResponse = {
  repository_mode: string;
  materialized_count: number;
  opportunity_count: number;
  opportunities: OpportunityRecord[];
};

export type ModelTrainingRun = {
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

export type ModelHistoryResponse = {
  repository_mode: string;
  model_history: {
    overview: {
      run_count: number;
      fallback_run_count: number;
      best_overall: ModelTrainingRun | null;
      latest_run: ModelTrainingRun | null;
    };
    recent_runs: ModelTrainingRun[];
  };
};

export type SelectionSnapshot = {
  id: number;
  model_evaluation_snapshot_id: number | null;
  model_training_run_id: number | null;
  model_registry_id: number | null;
  feature_version_id: number | null;
  target_task: string;
  model_family: string;
  selection_policy_name: string;
  rationale: string | null;
  is_active: boolean;
  created_at: string | null;
};

export type EvaluationSnapshot = {
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

export type ScoringRunDetail = {
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

export type ScoringRunDetailResponse = {
  repository_mode: string;
  scoring_run: ScoringRunDetail | null;
};

export type AppRoute =
  | { name: "backtests" }
  | { name: "backtest-run"; runId: number }
  | { name: "backtest-fold"; runId: number; foldIndex: number }
  | { name: "backtest-fold-model-run"; runId: number; foldIndex: number; modelRunId: number }
  | {
      name: "backtest-fold-evaluation";
      runId: number;
      foldIndex: number;
      evaluationId: number;
    }
  | {
      name: "artifact-compare";
      runId: number;
      foldIndex: number;
      opportunityId: number;
    }
  | { name: "opportunities" }
  | { name: "opportunity-detail"; opportunityId: number }
  | { name: "comparable-case"; opportunityId: number; comparableIndex: number }
  | { name: "opportunity-model-run"; opportunityId: number; runId: number }
  | { name: "opportunity-selection"; opportunityId: number; selectionId: number }
  | { name: "opportunity-evaluation"; opportunityId: number; evaluationId: number }
  | { name: "opportunity-scoring-run"; opportunityId: number; scoringRunId: number };

export type ProvenanceItem = {
  label: string;
  value: string;
  href?: string;
};

export type ProvenanceInspectorData = {
  modelRun?: ModelTrainingRun | null;
  modelHistory?: ModelHistoryResponse["model_history"] | null;
  selection: SelectionSnapshot | null;
  evaluation: EvaluationSnapshot | null;
  scoringRun: ScoringRunDetail | null;
};

export type ModelRunDetailResponse = {
  repository_mode: string;
  model_run: ModelTrainingRun | null;
};

export type SelectionDetailResponse = {
  repository_mode: string;
  selection: SelectionSnapshot | null;
};

export type EvaluationDetailResponse = {
  repository_mode: string;
  evaluation_snapshot: EvaluationSnapshot | null;
};
