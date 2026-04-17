import { useEffect, useState } from "react";

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";
const analystBacktestQuery = new URLSearchParams({
  target_task: "spread_error_regression",
  minimum_train_games: "1",
  test_window_games: "1",
  train_ratio: "0.5",
  validation_ratio: "0.25"
});

const adminBacktestHistoryQuery = new URLSearchParams({
  target_task: "spread_error_regression",
  minimum_train_games: "1",
  test_window_games: "1",
  train_ratio: "0.5",
  validation_ratio: "0.25",
  recent_limit: "6"
});

const adminBacktestMutationQuery = new URLSearchParams({
  target_task: "spread_error_regression",
  minimum_train_games: "1",
  test_window_games: "1",
  train_ratio: "0.5",
  validation_ratio: "0.25",
  recent_limit: "6"
});

const analystOpportunityQuery = new URLSearchParams({
  target_task: "spread_error_regression",
  team_code: "LAL",
  season_label: "2024-2025",
  canonical_game_id: "3",
  train_ratio: "0.5",
  validation_ratio: "0.25"
});

const adminOpportunityHistoryQuery = new URLSearchParams({
  target_task: "spread_error_regression",
  team_code: "LAL",
  season_label: "2024-2025",
  canonical_game_id: "3",
  train_ratio: "0.5",
  validation_ratio: "0.25",
  recent_limit: "6"
});

const adminOpportunityMutationQuery = new URLSearchParams({
  target_task: "spread_error_regression",
  team_code: "LAL",
  season_label: "2024-2025",
  canonical_game_id: "3",
  train_ratio: "0.5",
  validation_ratio: "0.25",
  recent_limit: "6"
});

const defaultModelArtifactQuery = new URLSearchParams({
  target_task: "spread_error_regression",
  train_ratio: "0.5",
  validation_ratio: "0.25"
});

type BacktestHistoryResponse = {
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

type BacktestRunResponse = {
  repository_mode: string;
  filters: Record<string, string | number | null>;
  feature_version: {
    feature_key: string;
    version_label: string;
  } | null;
  backtest_run: BacktestRun | null;
  summary: BacktestSummary;
};

type BacktestRun = {
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

type BacktestSummary = {
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

type StrategySummary = {
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

type FoldSummary = {
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

type OpportunityRecord = {
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

type OpportunityHistoryResponse = {
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

type OpportunityListResponse = {
  repository_mode: string;
  opportunity_count: number;
  opportunities: OpportunityRecord[];
};

type OpportunityDetailResponse = {
  repository_mode: string;
  opportunity: OpportunityRecord | null;
};

type OpportunityMaterializeResponse = {
  repository_mode: string;
  materialized_count: number;
  opportunity_count: number;
  opportunities: OpportunityRecord[];
};

type ModelTrainingRun = {
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

type ModelHistoryResponse = {
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

type SelectionSnapshot = {
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

type EvaluationSnapshot = {
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

type ScoringRunDetail = {
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

type ScoringRunDetailResponse = {
  repository_mode: string;
  scoring_run: ScoringRunDetail | null;
};

type AppRoute =
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

type ProvenanceItem = {
  label: string;
  value: string;
  href?: string;
};

type ProvenanceInspectorData = {
  modelRun?: ModelTrainingRun | null;
  modelHistory?: ModelHistoryResponse["model_history"] | null;
  selection: SelectionSnapshot | null;
  evaluation: EvaluationSnapshot | null;
  scoringRun: ScoringRunDetail | null;
};

async function fetchBacktestHistory(): Promise<BacktestHistoryResponse> {
  const response = await fetch(`${apiBaseUrl}/api/v1/admin/models/backtests/history?${adminBacktestHistoryQuery}`);
  if (!response.ok) {
    throw new Error(`Failed to load backtest history (${response.status})`);
  }
  return (await response.json()) as BacktestHistoryResponse;
}

async function runBacktest(): Promise<BacktestRunResponse> {
  const runQuery = new URLSearchParams(adminBacktestMutationQuery);
  runQuery.delete("auto_run_demo");
  runQuery.delete("recent_limit");
  const response = await fetch(`${apiBaseUrl}/api/v1/admin/models/backtests/run?${runQuery}`, {
    method: "POST"
  });
  if (!response.ok) {
    throw new Error(`Failed to run backtest (${response.status})`);
  }
  return (await response.json()) as BacktestRunResponse;
}

async function fetchBacktestRunDetail(backtestRunId: number): Promise<BacktestRunResponse> {
  const response = await fetch(
    `${apiBaseUrl}/api/v1/analyst/backtests/${backtestRunId}?${analystBacktestQuery}`
  );
  if (!response.ok) {
    throw new Error(`Failed to load backtest run (${response.status})`);
  }
  return (await response.json()) as BacktestRunResponse;
}

async function fetchOpportunityHistory(): Promise<OpportunityHistoryResponse> {
  const response = await fetch(
    `${apiBaseUrl}/api/v1/admin/models/opportunities/history?${adminOpportunityHistoryQuery}`
  );
  if (!response.ok) {
    throw new Error(`Failed to load opportunity history (${response.status})`);
  }
  return (await response.json()) as OpportunityHistoryResponse;
}

async function fetchOpportunities(): Promise<OpportunityListResponse> {
  const response = await fetch(`${apiBaseUrl}/api/v1/analyst/opportunities?${analystOpportunityQuery}`);
  if (!response.ok) {
    throw new Error(`Failed to load opportunities (${response.status})`);
  }
  return (await response.json()) as OpportunityListResponse;
}

async function fetchOpportunityDetail(opportunityId: number): Promise<OpportunityDetailResponse> {
  const response = await fetch(
    `${apiBaseUrl}/api/v1/analyst/opportunities/${opportunityId}?${analystOpportunityQuery}`
  );
  if (!response.ok) {
    throw new Error(`Failed to load opportunity detail (${response.status})`);
  }
  return (await response.json()) as OpportunityDetailResponse;
}

async function materializeOpportunities(): Promise<OpportunityMaterializeResponse> {
  const materializeQuery = new URLSearchParams(adminOpportunityMutationQuery);
  materializeQuery.delete("auto_materialize_demo");
  materializeQuery.delete("recent_limit");
  const response = await fetch(
    `${apiBaseUrl}/api/v1/admin/models/opportunities/materialize?${materializeQuery}`,
    { method: "POST" }
  );
  if (!response.ok) {
    throw new Error(`Failed to materialize opportunities (${response.status})`);
  }
  return (await response.json()) as OpportunityMaterializeResponse;
}

async function fetchModelHistory(): Promise<ModelHistoryResponse> {
  const response = await fetch(`${apiBaseUrl}/api/v1/admin/models/history?${defaultModelArtifactQuery}&recent_limit=5`);
  if (!response.ok) {
    throw new Error(`Failed to load model history (${response.status})`);
  }
  return (await response.json()) as ModelHistoryResponse;
}

type ModelRunDetailResponse = {
  repository_mode: string;
  model_run: ModelTrainingRun | null;
};

type SelectionDetailResponse = {
  repository_mode: string;
  selection: SelectionSnapshot | null;
};

type EvaluationDetailResponse = {
  repository_mode: string;
  evaluation_snapshot: EvaluationSnapshot | null;
};

async function fetchModelRunDetail(runId: number): Promise<ModelRunDetailResponse> {
  const response = await fetch(
    `${apiBaseUrl}/api/v1/admin/models/runs/${runId}?${defaultModelArtifactQuery}`
  );
  if (!response.ok) {
    throw new Error(`Failed to load model run detail (${response.status})`);
  }
  return (await response.json()) as ModelRunDetailResponse;
}

async function fetchSelectionDetail(selectionId: number): Promise<SelectionDetailResponse> {
  const response = await fetch(
    `${apiBaseUrl}/api/v1/admin/models/selections/${selectionId}?${defaultModelArtifactQuery}`
  );
  if (!response.ok) {
    throw new Error(`Failed to load model selection detail (${response.status})`);
  }
  return (await response.json()) as SelectionDetailResponse;
}

async function fetchEvaluationDetail(snapshotId: number): Promise<EvaluationDetailResponse> {
  const response = await fetch(
    `${apiBaseUrl}/api/v1/admin/models/evaluations/${snapshotId}?${defaultModelArtifactQuery}`
  );
  if (!response.ok) {
    throw new Error(`Failed to load model evaluation detail (${response.status})`);
  }
  return (await response.json()) as EvaluationDetailResponse;
}

async function fetchScoringRunDetail(
  scoringRunId: number,
  scenario: Record<string, unknown>
): Promise<ScoringRunDetailResponse> {
  const query = new URLSearchParams({
    target_task: String(readNested(scenario, "target_task") ?? adminOpportunityMutationQuery.get("target_task") ?? "spread_error_regression"),
    season_label: String(readNested(scenario, "season_label") ?? "2025-2026"),
    game_date: String(readNested(scenario, "game_date") ?? "2026-04-20"),
    home_team_code: String(readNested(scenario, "home_team_code") ?? "LAL"),
    away_team_code: String(readNested(scenario, "away_team_code") ?? "BOS"),
    train_ratio: adminOpportunityMutationQuery.get("train_ratio") ?? "0.5",
    validation_ratio: adminOpportunityMutationQuery.get("validation_ratio") ?? "0.25"
  });

  const homeSpreadLine = readNested(scenario, "home_spread_line");
  const totalLine = readNested(scenario, "total_line");
  if (homeSpreadLine !== undefined && homeSpreadLine !== null) {
    query.set("home_spread_line", String(homeSpreadLine));
  }
  if (totalLine !== undefined && totalLine !== null) {
    query.set("total_line", String(totalLine));
  }

  const response = await fetch(
    `${apiBaseUrl}/api/v1/admin/models/future-game-preview/runs/${scoringRunId}?${query}`
  );
  if (!response.ok) {
    throw new Error(`Failed to load scoring run detail (${response.status})`);
  }
  return (await response.json()) as ScoringRunDetailResponse;
}

function formatMetric(value: number | null, digits = 4): string {
  return value === null ? "n/a" : value.toFixed(digits);
}

function formatPercent(value: number | null): string {
  return value === null ? "n/a" : `${(value * 100).toFixed(1)}%`;
}

function formatTimestamp(value: string | null): string {
  if (!value) {
    return "n/a";
  }
  return new Date(value).toLocaleString();
}

function formatCompactNumber(value: number | null, digits = 2): string {
  return value === null ? "n/a" : value.toFixed(digits);
}

function getMetricDelta(left: number | null, right: number | null): number | null {
  if (left === null || right === null) {
    return null;
  }
  return right - left;
}

function formatDelta(value: number | null, digits = 4): string {
  if (value === null) {
    return "n/a";
  }
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(digits)}`;
}

function getAlignmentLabel(isAligned: boolean | null): string {
  if (isAligned === null) {
    return "unknown";
  }
  return isAligned ? "aligned" : "mismatch";
}

function getAlignmentTone(isAligned: boolean | null): "good" | "warning" | "neutral" {
  if (isAligned === null) {
    return "neutral";
  }
  return isAligned ? "good" : "warning";
}

function formatLabel(value: string | null | undefined): string {
  if (!value) {
    return "n/a";
  }
  return value.replace(/_/g, " ");
}

function readNested(source: unknown, ...path: string[]): unknown {
  let current = source;
  for (const key of path) {
    if (!current || typeof current !== "object" || !(key in current)) {
      return undefined;
    }
    current = (current as Record<string, unknown>)[key];
  }
  return current;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function parseRouteFromHash(hash: string): AppRoute {
  const normalized = hash.replace(/^#/, "").replace(/^\//, "");
  if (!normalized || normalized === "backtests") {
    return { name: "backtests" };
  }
  const backtestMatch = /^backtests\/(\d+)$/.exec(normalized);
  if (backtestMatch) {
    return {
      name: "backtest-run",
      runId: Number(backtestMatch[1])
    };
  }
  const backtestFoldMatch = /^backtests\/(\d+)\/folds\/(\d+)$/.exec(normalized);
  if (backtestFoldMatch) {
    return {
      name: "backtest-fold",
      runId: Number(backtestFoldMatch[1]),
      foldIndex: Number(backtestFoldMatch[2])
    };
  }
  const backtestFoldModelRunMatch = /^backtests\/(\d+)\/folds\/(\d+)\/model-runs\/(\d+)$/.exec(normalized);
  if (backtestFoldModelRunMatch) {
    return {
      name: "backtest-fold-model-run",
      runId: Number(backtestFoldModelRunMatch[1]),
      foldIndex: Number(backtestFoldModelRunMatch[2]),
      modelRunId: Number(backtestFoldModelRunMatch[3])
    };
  }
  const backtestFoldEvaluationMatch = /^backtests\/(\d+)\/folds\/(\d+)\/evaluations\/(\d+)$/.exec(normalized);
  if (backtestFoldEvaluationMatch) {
    return {
      name: "backtest-fold-evaluation",
      runId: Number(backtestFoldEvaluationMatch[1]),
      foldIndex: Number(backtestFoldEvaluationMatch[2]),
      evaluationId: Number(backtestFoldEvaluationMatch[3])
    };
  }
  const artifactCompareMatch =
    /^compare\/backtests\/(\d+)\/folds\/(\d+)\/opportunities\/(\d+)$/.exec(normalized);
  if (artifactCompareMatch) {
    return {
      name: "artifact-compare",
      runId: Number(artifactCompareMatch[1]),
      foldIndex: Number(artifactCompareMatch[2]),
      opportunityId: Number(artifactCompareMatch[3])
    };
  }
  if (normalized === "opportunities") {
    return { name: "opportunities" };
  }
  const detailMatch = /^opportunities\/(\d+)$/.exec(normalized);
  if (detailMatch) {
    return {
      name: "opportunity-detail",
      opportunityId: Number(detailMatch[1])
    };
  }
  const modelRunMatch = /^opportunities\/(\d+)\/model-runs\/(\d+)$/.exec(normalized);
  if (modelRunMatch) {
    return {
      name: "opportunity-model-run",
      opportunityId: Number(modelRunMatch[1]),
      runId: Number(modelRunMatch[2])
    };
  }
  const selectionMatch = /^opportunities\/(\d+)\/selections\/(\d+)$/.exec(normalized);
  if (selectionMatch) {
    return {
      name: "opportunity-selection",
      opportunityId: Number(selectionMatch[1]),
      selectionId: Number(selectionMatch[2])
    };
  }
  const evaluationMatch = /^opportunities\/(\d+)\/evaluations\/(\d+)$/.exec(normalized);
  if (evaluationMatch) {
    return {
      name: "opportunity-evaluation",
      opportunityId: Number(evaluationMatch[1]),
      evaluationId: Number(evaluationMatch[2])
    };
  }
  const scoringRunMatch = /^opportunities\/(\d+)\/scoring-runs\/(\d+)$/.exec(normalized);
  if (scoringRunMatch) {
    return {
      name: "opportunity-scoring-run",
      opportunityId: Number(scoringRunMatch[1]),
      scoringRunId: Number(scoringRunMatch[2])
    };
  }
  const comparableMatch = /^opportunities\/(\d+)\/comparables\/(\d+)$/.exec(normalized);
  if (comparableMatch) {
    return {
      name: "comparable-case",
      opportunityId: Number(comparableMatch[1]),
      comparableIndex: Number(comparableMatch[2])
    };
  }
  return { name: "backtests" };
}

function routeHash(route: AppRoute): string {
  if (route.name === "backtests") {
    return "#/backtests";
  }
  if (route.name === "backtest-run") {
    return `#/backtests/${route.runId}`;
  }
  if (route.name === "backtest-fold") {
    return `#/backtests/${route.runId}/folds/${route.foldIndex}`;
  }
  if (route.name === "backtest-fold-model-run") {
    return `#/backtests/${route.runId}/folds/${route.foldIndex}/model-runs/${route.modelRunId}`;
  }
  if (route.name === "backtest-fold-evaluation") {
    return `#/backtests/${route.runId}/folds/${route.foldIndex}/evaluations/${route.evaluationId}`;
  }
  if (route.name === "artifact-compare") {
    return `#/compare/backtests/${route.runId}/folds/${route.foldIndex}/opportunities/${route.opportunityId}`;
  }
  if (route.name === "opportunities") {
    return "#/opportunities";
  }
  if (route.name === "opportunity-model-run") {
    return `#/opportunities/${route.opportunityId}/model-runs/${route.runId}`;
  }
  if (route.name === "opportunity-selection") {
    return `#/opportunities/${route.opportunityId}/selections/${route.selectionId}`;
  }
  if (route.name === "opportunity-evaluation") {
    return `#/opportunities/${route.opportunityId}/evaluations/${route.evaluationId}`;
  }
  if (route.name === "opportunity-scoring-run") {
    return `#/opportunities/${route.opportunityId}/scoring-runs/${route.scoringRunId}`;
  }
  if (route.name === "comparable-case") {
    return `#/opportunities/${route.opportunityId}/comparables/${route.comparableIndex}`;
  }
  return `#/opportunities/${route.opportunityId}`;
}

function StatTile({
  label,
  value,
  detail
}: {
  label: string;
  value: string;
  detail?: string;
}) {
  return (
    <article className="stat-tile">
      <p className="stat-label">{label}</p>
      <p className="stat-value">{value}</p>
      {detail ? <p className="stat-detail">{detail}</p> : null}
    </article>
  );
}

function ProvenanceRibbon({
  title,
  items
}: {
  title: string;
  items: ProvenanceItem[];
}) {
  if (items.length === 0) {
    return null;
  }

  return (
    <section className="provenance-ribbon">
      <p className="eyebrow">{title}</p>
      <div className="provenance-grid">
        {items.map((item) => (
          <div className="provenance-item" key={`${item.label}-${item.value}`}>
            <span className="provenance-label">{item.label}</span>
            {item.href ? (
              <a className="provenance-link" href={item.href}>
                {item.value}
              </a>
            ) : (
              <strong className="provenance-value">{item.value}</strong>
            )}
          </div>
        ))}
      </div>
    </section>
  );
}

function ProvenanceInspector({
  data
}: {
  data: ProvenanceInspectorData;
}) {
  const modelRunValidationMetricValue = data.modelRun
    ? readNested(data.modelRun.artifact, "selection_metrics", "validation", "metric_value")
    : null;
  const modelRunSelectedFeatureValue = data.modelRun
    ? readNested(data.modelRun.artifact, "selected_feature")
    : null;

  if (!data.selection && !data.evaluation && !data.scoringRun && !data.modelHistory && !data.modelRun) {
    return null;
  }

  return (
    <section className="section-stack detail-section-stack">
      <div className="section-heading standalone">
        <div>
          <p className="eyebrow">Provenance inspector</p>
          <h3>Resolved model artifacts</h3>
        </div>
      </div>

      <div className="detail-section-grid">
        {data.modelHistory ? (
          <section className="sub-panel">
            <p className="sub-panel-title">Training history</p>
            <div className="detail-list compact-list">
              <div className="detail-list-item">
                <span>Run count</span>
                <strong>{String(data.modelHistory.overview.run_count)}</strong>
              </div>
              <div className="detail-list-item">
                <span>Fallback runs</span>
                <strong>{String(data.modelHistory.overview.fallback_run_count)}</strong>
              </div>
              <div className="detail-list-item">
                <span>Latest family</span>
                <strong>
                  {String(readNested(data.modelHistory.overview.latest_run, "artifact", "model_family") ?? "n/a")}
                </strong>
              </div>
              <div className="detail-list-item">
                <span>Best family</span>
                <strong>
                  {String(readNested(data.modelHistory.overview.best_overall, "artifact", "model_family") ?? "n/a")}
                </strong>
              </div>
            </div>
          </section>
        ) : null}

        {data.modelRun ? (
          <section className="sub-panel">
            <p className="sub-panel-title">Training run</p>
            <div className="detail-list compact-list">
              <div className="detail-list-item">
                <span>ID</span>
                <strong>Run #{data.modelRun.id}</strong>
              </div>
              <div className="detail-list-item">
                <span>Model family</span>
                <strong>{String(readNested(data.modelRun.artifact, "model_family") ?? "n/a")}</strong>
              </div>
              <div className="detail-list-item">
                <span>Selected feature</span>
                <strong>{String(modelRunSelectedFeatureValue ?? "n/a")}</strong>
              </div>
              <div className="detail-list-item">
                <span>Validation metric</span>
                <strong>
                  {typeof modelRunValidationMetricValue === "number"
                    ? formatCompactNumber(modelRunValidationMetricValue, 4)
                    : "n/a"}
                </strong>
              </div>
            </div>
          </section>
        ) : null}

        {data.selection ? (
          <section className="sub-panel">
            <p className="sub-panel-title">Selection snapshot</p>
            <div className="detail-list compact-list">
              <div className="detail-list-item">
                <span>ID</span>
                <strong>Selection #{data.selection.id}</strong>
              </div>
              <div className="detail-list-item">
                <span>Model family</span>
                <strong>{data.selection.model_family}</strong>
              </div>
              <div className="detail-list-item">
                <span>Policy</span>
                <strong>{data.selection.selection_policy_name}</strong>
              </div>
              <div className="detail-list-item">
                <span>Active</span>
                <strong>{data.selection.is_active ? "true" : "false"}</strong>
              </div>
            </div>
          </section>
        ) : null}

        {data.evaluation ? (
          <section className="sub-panel">
            <p className="sub-panel-title">Evaluation snapshot</p>
            <div className="detail-list compact-list">
              <div className="detail-list-item">
                <span>ID</span>
                <strong>Evaluation #{data.evaluation.id}</strong>
              </div>
              <div className="detail-list-item">
                <span>Model family</span>
                <strong>{data.evaluation.model_family}</strong>
              </div>
              <div className="detail-list-item">
                <span>Primary metric</span>
                <strong>{data.evaluation.primary_metric_name ?? "n/a"}</strong>
              </div>
              <div className="detail-list-item">
                <span>Validation</span>
                <strong>{formatCompactNumber(data.evaluation.validation_metric_value, 4)}</strong>
              </div>
              <div className="detail-list-item">
                <span>Test</span>
                <strong>{formatCompactNumber(data.evaluation.test_metric_value, 4)}</strong>
              </div>
            </div>
          </section>
        ) : null}
      </div>

      {data.scoringRun ? (
        <section className="sub-panel">
          <p className="sub-panel-title">Scoring run detail</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>ID</span>
              <strong>Scoring #{data.scoringRun.id}</strong>
            </div>
            <div className="detail-list-item">
              <span>Scenario key</span>
              <strong>{data.scoringRun.scenario_key}</strong>
            </div>
            <div className="detail-list-item">
              <span>Prediction count</span>
              <strong>{String(data.scoringRun.prediction_count)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Review opportunities</span>
              <strong>{String(data.scoringRun.review_opportunity_count)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Candidate opportunities</span>
              <strong>{String(data.scoringRun.candidate_opportunity_count)}</strong>
            </div>
          </div>
        </section>
      ) : null}
    </section>
  );
}

function StrategyCard({ label, strategy }: { label: string; strategy: StrategySummary }) {
  const edgeBuckets = Object.entries(strategy.edge_bucket_performance);

  return (
    <article className="panel strategy-card">
      <div className="section-heading">
        <div>
          <p className="eyebrow">{label}</p>
          <h3>{strategy.strategy_name}</h3>
        </div>
        <div className="pill-row">
          {"threshold" in strategy && strategy.threshold !== undefined ? (
            <span className="pill">Threshold {formatMetric(strategy.threshold, 1)}</span>
          ) : null}
          <span className="pill">ROI {formatPercent(strategy.roi)}</span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile label="Bets" value={String(strategy.bet_count)} />
        <StatTile label="Hit rate" value={formatPercent(strategy.hit_rate)} />
        <StatTile label="Push rate" value={formatPercent(strategy.push_rate)} />
        <StatTile label="Profit" value={formatMetric(strategy.profit_units, 2)} detail="Units" />
      </div>

      <div className="table-shell">
        <table>
          <thead>
            <tr>
              <th>Edge bucket</th>
              <th>Bets</th>
              <th>Hit rate</th>
              <th>ROI</th>
              <th>Profit</th>
            </tr>
          </thead>
          <tbody>
            {edgeBuckets.length === 0 ? (
              <tr>
                <td colSpan={5}>No bets in this strategy yet.</td>
              </tr>
            ) : (
              edgeBuckets.map(([bucket, bucketSummary]) => (
                <tr key={bucket}>
                  <td>{bucket}</td>
                  <td>{bucketSummary.bet_count}</td>
                  <td>{formatPercent(bucketSummary.hit_rate)}</td>
                  <td>{formatPercent(bucketSummary.roi)}</td>
                  <td>{formatMetric(bucketSummary.profit_units, 2)}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </article>
  );
}

function FoldCard({ fold }: { fold: FoldSummary }) {
  return (
    <article className="panel fold-card">
      <div className="section-heading">
        <div>
          <p className="eyebrow">Fold {fold.fold_index}</p>
          <h3>{fold.selected_model.model_family}</h3>
        </div>
        <div className="pill-row">
          <span className="pill">Train {fold.train_game_count} games</span>
          <span className="pill">Test {fold.test_game_count} games</span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile
          label="Selected feature"
          value={fold.selected_model.selected_feature ?? "fallback"}
          detail={fold.selected_model.fallback_strategy ?? "primary fit"}
        />
        <StatTile label="Validation MAE" value={formatMetric(fold.selected_model.validation_metric_value)} />
        <StatTile label="Test MAE" value={formatMetric(fold.selected_model.test_metric_value)} />
        <StatTile label="Predictions" value={String(fold.prediction_metrics.prediction_count)} />
      </div>

      <div className="mini-grid strategy-mini-grid">
        {Object.entries(fold.strategies).map(([key, strategy]) => (
          <div className="sub-panel" key={key}>
            <p className="sub-panel-title">{key.replace("_", " ")}</p>
            <p className="sub-panel-stat">ROI {formatPercent(strategy.roi)}</p>
            <p className="sub-panel-meta">
              {strategy.bet_count} bets • {formatPercent(strategy.hit_rate)} hit rate
            </p>
          </div>
        ))}
      </div>
    </article>
  );
}

function FoldDetailCard({
  fold,
  runId,
  provenanceItems,
  compareHref
}: {
  fold: FoldSummary | null;
  runId: number | null;
  provenanceItems?: ProvenanceItem[];
  compareHref?: string;
}) {
  if (!fold) {
    return (
      <article className="panel focus-panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Fold detail</p>
            <h2>Fold not available</h2>
          </div>
        </div>
        <p className="lead detail-copy">
          The selected walk-forward fold could not be resolved from this backtest run.
        </p>
      </article>
    );
  }

  return (
    <article className="panel focus-panel">
      <ProvenanceRibbon items={provenanceItems ?? []} title="Provenance" />

      <div className="section-heading">
        <div>
          <p className="eyebrow">Fold detail</p>
          <h2>
            Run {runId ? `#${runId}` : ""} | Fold {fold.fold_index}
          </h2>
        </div>
        <div className="pill-row">
          <span className="pill">{fold.selected_model.model_family}</span>
          <span className="pill">{fold.test_game_count} test games</span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile label="Train games" value={String(fold.train_game_count)} />
        <StatTile label="Test games" value={String(fold.test_game_count)} />
        <StatTile label="Validation MAE" value={formatMetric(fold.selected_model.validation_metric_value)} />
        <StatTile label="Test MAE" value={formatMetric(fold.selected_model.test_metric_value)} />
      </div>

      {compareHref ? (
        <div className="route-action-row">
          <a className="secondary-button inline-link-button" href={compareHref}>
            Compare with active opportunity
          </a>
        </div>
      ) : null}

      <div className="detail-section-grid">
        <section className="sub-panel">
          <p className="sub-panel-title">Selected model</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Family</span>
              <strong>{fold.selected_model.model_family}</strong>
            </div>
            <div className="detail-list-item">
              <span>Selected feature</span>
              <strong>{fold.selected_model.selected_feature ?? "fallback"}</strong>
            </div>
            <div className="detail-list-item">
              <span>Fallback strategy</span>
              <strong>{fold.selected_model.fallback_strategy ?? "primary fit"}</strong>
            </div>
          </div>
          {runId ? (
            <div className="pill-row fold-link-row">
              {fold.selected_model.model_training_run_id > 0 ? (
                <a
                  className="secondary-button inline-link-button"
                  href={routeHash({
                    name: "backtest-fold-model-run",
                    runId,
                    foldIndex: fold.fold_index,
                    modelRunId: fold.selected_model.model_training_run_id
                  })}
                >
                  Open training run
                </a>
              ) : null}
              <a
                className="secondary-button inline-link-button"
                href={routeHash({
                  name: "backtest-fold-evaluation",
                  runId,
                  foldIndex: fold.fold_index,
                  evaluationId: fold.selected_model.evaluation_snapshot_id
                })}
              >
                Open evaluation
              </a>
            </div>
          ) : null}
        </section>

        <section className="sub-panel">
          <p className="sub-panel-title">Prediction metrics</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Prediction count</span>
              <strong>{String(fold.prediction_metrics.prediction_count)}</strong>
            </div>
            <div className="detail-list-item">
              <span>MAE</span>
              <strong>{formatMetric(fold.prediction_metrics.mae)}</strong>
            </div>
            <div className="detail-list-item">
              <span>RMSE</span>
              <strong>{formatMetric(fold.prediction_metrics.rmse)}</strong>
            </div>
          </div>
        </section>
      </div>

      <section className="section-stack detail-section-stack">
        <div className="section-heading standalone">
          <div>
            <p className="eyebrow">Strategy outcomes</p>
            <h3>Per-fold thresholds</h3>
          </div>
        </div>
        <div className="strategy-grid">
          {Object.entries(fold.strategies).map(([label, strategy]) => (
            <StrategyCard key={label} label={label.replace("_", " ")} strategy={strategy} />
          ))}
        </div>
      </section>

      <section className="detail-section-grid">
        <section className="sub-panel">
          <p className="sub-panel-title">Train game ids</p>
          <p className="sub-panel-meta">{fold.train_game_ids.join(", ") || "n/a"}</p>
        </section>

        <section className="sub-panel">
          <p className="sub-panel-title">Test game ids</p>
          <p className="sub-panel-meta">{fold.test_game_ids.join(", ") || "n/a"}</p>
        </section>
      </section>
    </article>
  );
}

function BacktestRunDetailCard({
  run,
  provenanceItems,
  provenanceData
}: {
  run: BacktestRun | null;
  provenanceItems?: ProvenanceItem[];
  provenanceData?: ProvenanceInspectorData;
}) {
  if (!run) {
    return (
      <article className="panel focus-panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Backtest run</p>
            <h2>Select a run</h2>
          </div>
        </div>
        <p className="lead detail-copy">
          Pick a backtest run from history to inspect the exact validation result, selected model
          mix, and fold-level strategy performance.
        </p>
      </article>
    );
  }

  const summary = run.payload;

  return (
    <article className="panel focus-panel">
      <ProvenanceRibbon items={provenanceItems ?? []} title="Provenance" />
      {provenanceData ? <ProvenanceInspector data={provenanceData} /> : null}

      <div className="section-heading">
        <div>
          <p className="eyebrow">Backtest run</p>
          <h2>{summary.strategy_name}</h2>
        </div>
        <div className="pill-row">
          <span className="pill">Run #{run.id}</span>
          <span className="pill">{summary.fold_count} folds</span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile label="Dataset games" value={String(summary.dataset_game_count)} />
        <StatTile label="Dataset rows" value={String(summary.dataset_row_count)} />
        <StatTile label="Prediction MAE" value={formatMetric(summary.prediction_metrics.mae)} />
        <StatTile label="Prediction RMSE" value={formatMetric(summary.prediction_metrics.rmse)} />
      </div>

      <div className="detail-list">
        <div className="detail-list-item">
          <span>Selection policy</span>
          <strong>{summary.selection_policy_name}</strong>
        </div>
        <div className="detail-list-item">
          <span>Target task</span>
          <strong>{summary.target_task}</strong>
        </div>
        <div className="detail-list-item">
          <span>Minimum train games</span>
          <strong>{String(summary.minimum_train_games)}</strong>
        </div>
        <div className="detail-list-item">
          <span>Test window games</span>
          <strong>{String(summary.test_window_games)}</strong>
        </div>
      </div>

      <div className="mini-grid family-grid">
        {Object.entries(summary.selected_model_family_counts).map(([family, count]) => (
          <div className="sub-panel" key={family}>
            <p className="sub-panel-title">{family}</p>
            <p className="sub-panel-stat">{count}</p>
            <p className="sub-panel-meta">fold selections</p>
          </div>
        ))}
      </div>
    </article>
  );
}

function OpportunityListItem({
  opportunity,
  active,
  onSelect
}: {
  opportunity: OpportunityRecord;
  active: boolean;
  onSelect: (opportunityId: number) => void;
}) {
  return (
    <button
      className={`opportunity-list-item${active ? " opportunity-list-item-active" : ""}`}
      onClick={() => onSelect(opportunity.id)}
      type="button"
    >
      <div className="section-heading compact-heading">
        <div>
          <p className="eyebrow">{formatLabel(opportunity.source_kind)}</p>
          <h3>
            {opportunity.team_code} vs {opportunity.opponent_code}
          </h3>
        </div>
        <div className="pill-row">
          <span className="pill">{formatLabel(opportunity.status)}</span>
        </div>
      </div>

      <div className="opportunity-item-grid">
        <div>
          <p className="sub-panel-title">Prediction</p>
          <p className="sub-panel-stat">{formatCompactNumber(opportunity.prediction_value, 3)}</p>
        </div>
        <div>
          <p className="sub-panel-title">Signal</p>
          <p className="sub-panel-stat">{formatCompactNumber(opportunity.signal_strength, 2)}</p>
        </div>
        <div>
          <p className="sub-panel-title">Evidence</p>
          <p className="sub-panel-stat">{formatLabel(opportunity.evidence_rating)}</p>
        </div>
      </div>

      <p className="sub-panel-meta">
        {formatTimestamp(opportunity.updated_at ?? opportunity.created_at)} | {opportunity.season_label}
      </p>
    </button>
  );
}

function OpportunityDetailCard({
  opportunity,
  onSelectComparable,
  provenanceItems,
  provenanceData,
  compareHref
}: {
  opportunity: OpportunityRecord | null;
  onSelectComparable?: (comparableIndex: number) => void;
  provenanceItems?: ProvenanceItem[];
  provenanceData?: ProvenanceInspectorData;
  compareHref?: string;
}) {
  if (!opportunity) {
    return (
      <article className="panel focus-panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Opportunity detail</p>
            <h2>Select an opportunity</h2>
          </div>
        </div>
        <p className="lead detail-copy">
          Pick a row from the queue to inspect its evidence strength, recommendation state, market
          context, and model metadata.
        </p>
      </article>
    );
  }

  const prediction = asRecord(readNested(opportunity.payload, "prediction"));
  const evidence = asRecord(readNested(prediction, "evidence"));
  const strength = asRecord(readNested(evidence, "strength"));
  const recommendation = asRecord(readNested(evidence, "recommendation"));
  const evidenceSummary = asRecord(readNested(evidence, "summary"));
  const marketContext = asRecord(readNested(prediction, "market_context"));
  const modelContext = asRecord(readNested(prediction, "model"));
  const pattern = asRecord(readNested(evidence, "pattern"));
  const selectedPattern = asRecord(readNested(pattern, "selected_pattern"));
  const comparables = asRecord(readNested(evidence, "comparables"));
  const comparablesSummary = asRecord(readNested(comparables, "summary"));
  const comparableCases = asArray(readNested(comparables, "cases"));
  const benchmarkContext = asRecord(readNested(evidence, "benchmark_context"));
  const benchmarkRankings = asArray(readNested(benchmarkContext, "benchmark_rankings"));
  const policyProfile = asRecord(readNested(recommendation, "policy_profile"));
  const thresholds = asRecord(readNested(policyProfile, "thresholds"));
  const rationale = asArray(readNested(recommendation, "rationale"));
  const blockingFactors = asArray(readNested(recommendation, "blocking_factors"));
  const nextSteps = asArray(readNested(recommendation, "next_steps"));
  const activeSelection = asRecord(readNested(opportunity.payload, "active_selection"));
  const activeEvaluationSnapshot = asRecord(readNested(opportunity.payload, "active_evaluation_snapshot"));
  const scenario = asRecord(readNested(opportunity.payload, "scenario"));

  return (
    <article className="panel focus-panel">
      <ProvenanceRibbon items={provenanceItems ?? []} title="Provenance" />
      {provenanceData ? <ProvenanceInspector data={provenanceData} /> : null}

      <div className="section-heading">
        <div>
          <p className="eyebrow">Opportunity detail</p>
          <h2>
            {opportunity.team_code} vs {opportunity.opponent_code}
          </h2>
        </div>
        <div className="pill-row">
          <span className="pill">{formatLabel(opportunity.status)}</span>
          <span className="pill">{formatLabel(opportunity.recommendation_status)}</span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile label="Prediction value" value={formatCompactNumber(opportunity.prediction_value, 3)} />
        <StatTile label="Signal strength" value={formatCompactNumber(opportunity.signal_strength, 2)} />
        <StatTile label="Evidence rating" value={formatLabel(opportunity.evidence_rating)} />
        <StatTile label="Game date" value={opportunity.game_date} detail={opportunity.season_label} />
      </div>

      {compareHref ? (
        <div className="route-action-row">
          <a className="secondary-button inline-link-button" href={compareHref}>
            Compare with active backtest fold
          </a>
        </div>
      ) : null}

      <div className="opportunity-detail-grid">
        <div className="sub-panel">
          <p className="sub-panel-title">Recommendation</p>
          <p className="sub-panel-stat">
            {String(readNested(recommendation, "headline") ?? formatLabel(opportunity.recommendation_status))}
          </p>
          <p className="sub-panel-meta">
            {String(readNested(recommendation, "recommended_action") ?? "Inspect manually")}
          </p>
        </div>

        <div className="sub-panel">
          <p className="sub-panel-title">Evidence strength</p>
          <p className="sub-panel-stat">
            {formatLabel(String(readNested(strength, "rating") ?? opportunity.evidence_rating ?? "n/a"))}
          </p>
          <p className="sub-panel-meta">
            Overall score {formatCompactNumber(Number(readNested(strength, "overall_score") ?? null), 3)}
          </p>
        </div>

        <div className="sub-panel">
          <p className="sub-panel-title">Pattern support</p>
          <p className="sub-panel-stat">{String(readNested(evidenceSummary, "pattern_sample_size") ?? "n/a")}</p>
          <p className="sub-panel-meta">
            Comparables {String(readNested(evidenceSummary, "comparable_count") ?? "n/a")}
          </p>
        </div>

        <div className="sub-panel">
          <p className="sub-panel-title">Market context</p>
          <p className="sub-panel-stat">
            Spread {String(readNested(marketContext, "home_spread_line") ?? "n/a")}
          </p>
          <p className="sub-panel-meta">
            Total {String(readNested(marketContext, "total_line") ?? "n/a")}
          </p>
        </div>
      </div>

      <div className="detail-list">
        <div className="detail-list-item">
          <span>Policy</span>
          <strong>{opportunity.policy_name}</strong>
        </div>
        <div className="detail-list-item">
          <span>Model family</span>
          <strong>{String(readNested(modelContext, "model_family") ?? "n/a")}</strong>
        </div>
        <div className="detail-list-item">
          <span>Selected feature</span>
          <strong>{String(readNested(modelContext, "selected_feature") ?? "n/a")}</strong>
        </div>
        <div className="detail-list-item">
          <span>Scenario key</span>
          <strong>{opportunity.scenario_key ?? "historical_game"}</strong>
        </div>
      </div>

      <div className="detail-section-grid">
        <section className="sub-panel">
          <p className="sub-panel-title">Pattern evidence</p>
          <p className="sub-panel-stat">
            {String(readNested(selectedPattern, "pattern_key") ?? readNested(evidenceSummary, "pattern_key") ?? "n/a")}
          </p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Sample size</span>
              <strong>{String(readNested(selectedPattern, "sample_size") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Signal strength</span>
              <strong>{String(readNested(selectedPattern, "signal_strength") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Target mean</span>
              <strong>{String(readNested(selectedPattern, "target_mean") ?? "n/a")}</strong>
            </div>
          </div>
        </section>

        <section className="sub-panel">
          <p className="sub-panel-title">Benchmark context</p>
          <p className="sub-panel-stat">
            {String(readNested(evidenceSummary, "best_benchmark", "baseline_name") ?? "n/a")}
          </p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Primary metric</span>
              <strong>{String(readNested(evidenceSummary, "best_benchmark", "primary_metric") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Validation</span>
              <strong>{String(readNested(evidenceSummary, "best_benchmark", "validation_primary_metric") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Test</span>
              <strong>{String(readNested(evidenceSummary, "best_benchmark", "test_primary_metric") ?? "n/a")}</strong>
            </div>
          </div>
        </section>
      </div>

      <section className="section-stack detail-section-stack">
        <div className="section-heading standalone">
          <div>
            <p className="eyebrow">Comparable cases</p>
            <h3>Matched history</h3>
          </div>
          <div className="pill-row">
            <span className="pill">Count {String(readNested(comparablesSummary, "comparable_count") ?? comparableCases.length)}</span>
            <span className="pill">
              Top similarity {String(readNested(comparablesSummary, "top_similarity_score") ?? "n/a")}
            </span>
          </div>
        </div>

        <div className="list-stack">
          {comparableCases.length === 0 ? (
            <p className="sub-panel-meta">No comparable cases were attached to this opportunity.</p>
          ) : (
            comparableCases.slice(0, 5).map((entry, index) => {
              const comparable = asRecord(entry);
              const matchedConditions = asRecord(readNested(comparable, "matched_conditions"));
              return (
                <button
                  className={`sub-panel comparable-card${onSelectComparable ? " comparable-card-actionable" : ""}`}
                  key={String(readNested(comparable, "canonical_game_id") ?? index)}
                  onClick={onSelectComparable ? () => onSelectComparable(index) : undefined}
                  type={onSelectComparable ? "button" : undefined}
                >
                  <div className="section-heading compact-heading">
                    <div>
                      <p className="sub-panel-title">
                        Game {String(readNested(comparable, "canonical_game_id") ?? "n/a")}
                      </p>
                      <p className="sub-panel-stat">
                        {String(readNested(comparable, "team_code") ?? "n/a")} vs{" "}
                        {String(readNested(comparable, "opponent_code") ?? "n/a")}
                      </p>
                    </div>
                    <div className="pill-row">
                      <span className="pill">
                        Similarity {String(readNested(comparable, "similarity_score") ?? "n/a")}
                      </span>
                    </div>
                  </div>
                  <div className="detail-list compact-list">
                    <div className="detail-list-item">
                      <span>Prediction target</span>
                      <strong>{String(readNested(comparable, "target_value") ?? "n/a")}</strong>
                    </div>
                    <div className="detail-list-item">
                      <span>Venue</span>
                      <strong>{String(readNested(matchedConditions, "venue") ?? "n/a")}</strong>
                    </div>
                    <div className="detail-list-item">
                      <span>Rest bucket</span>
                      <strong>{String(readNested(matchedConditions, "days_rest_bucket") ?? "n/a")}</strong>
                    </div>
                  </div>
                </button>
              );
            })
          )}
        </div>
      </section>

      <section className="section-stack detail-section-stack">
        <div className="section-heading standalone">
          <div>
            <p className="eyebrow">Recommendation policy</p>
            <h3>Decision framing</h3>
          </div>
        </div>

        <div className="detail-section-grid">
          <section className="sub-panel">
            <p className="sub-panel-title">Rationale</p>
            <div className="chip-list">
              {rationale.length === 0 ? (
                <span className="pill">No rationale provided</span>
              ) : (
                rationale.map((item, index) => (
                  <span className="pill" key={`${String(item)}-${index}`}>
                    {String(item)}
                  </span>
                ))
              )}
            </div>
          </section>

          <section className="sub-panel">
            <p className="sub-panel-title">Blocking factors</p>
            <div className="chip-list">
              {blockingFactors.length === 0 ? (
                <span className="pill">No blockers</span>
              ) : (
                blockingFactors.map((item, index) => (
                  <span className="pill" key={`${String(item)}-${index}`}>
                    {formatLabel(String(item))}
                  </span>
                ))
              )}
            </div>
          </section>
        </div>

        <div className="detail-section-grid">
          <section className="sub-panel">
            <p className="sub-panel-title">Next steps</p>
            <ul className="detail-bullets">
              {nextSteps.length === 0 ? (
                <li>No next steps provided.</li>
              ) : (
                nextSteps.map((item, index) => <li key={`${String(item)}-${index}`}>{String(item)}</li>)
              )}
            </ul>
          </section>

          <section className="sub-panel">
            <p className="sub-panel-title">Policy thresholds</p>
            <div className="detail-list compact-list">
              {Object.entries(thresholds ?? {}).map(([key, value]) => (
                <div className="detail-list-item" key={key}>
                  <span>{formatLabel(key)}</span>
                  <strong>{String(value)}</strong>
                </div>
              ))}
            </div>
          </section>
        </div>
      </section>

      <section className="section-stack detail-section-stack">
        <div className="section-heading standalone">
          <div>
            <p className="eyebrow">Model provenance</p>
            <h3>Selection and evaluation snapshot</h3>
          </div>
        </div>

        <div className="detail-section-grid">
          <section className="sub-panel">
            <p className="sub-panel-title">Active selection</p>
            <div className="detail-list compact-list">
              <div className="detail-list-item">
                <span>Family</span>
                <strong>{String(readNested(activeSelection, "model_family") ?? "n/a")}</strong>
              </div>
              <div className="detail-list-item">
                <span>Policy</span>
                <strong>{String(readNested(activeSelection, "selection_policy_name") ?? "n/a")}</strong>
              </div>
              <div className="detail-list-item">
                <span>Activated</span>
                <strong>{String(readNested(activeSelection, "activated_at") ?? "n/a")}</strong>
              </div>
            </div>
          </section>

          <section className="sub-panel">
            <p className="sub-panel-title">Evaluation snapshot</p>
            <div className="detail-list compact-list">
              <div className="detail-list-item">
                <span>Primary metric</span>
                <strong>{String(readNested(activeEvaluationSnapshot, "primary_metric") ?? "n/a")}</strong>
              </div>
              <div className="detail-list-item">
                <span>Validation value</span>
                <strong>{String(readNested(activeEvaluationSnapshot, "validation_primary_metric") ?? "n/a")}</strong>
              </div>
              <div className="detail-list-item">
                <span>Test value</span>
                <strong>{String(readNested(activeEvaluationSnapshot, "test_primary_metric") ?? "n/a")}</strong>
              </div>
            </div>
          </section>
        </div>

        {scenario ? (
          <section className="sub-panel">
            <p className="sub-panel-title">Scenario context</p>
            <div className="detail-list compact-list">
              <div className="detail-list-item">
                <span>Home team</span>
                <strong>{String(readNested(scenario, "home_team_code") ?? "n/a")}</strong>
              </div>
              <div className="detail-list-item">
                <span>Away team</span>
                <strong>{String(readNested(scenario, "away_team_code") ?? "n/a")}</strong>
              </div>
              <div className="detail-list-item">
                <span>Scenario date</span>
                <strong>{String(readNested(scenario, "game_date") ?? "n/a")}</strong>
              </div>
            </div>
          </section>
        ) : null}
      </section>

      {benchmarkRankings.length > 0 ? (
        <section className="section-stack detail-section-stack">
          <div className="section-heading standalone">
            <div>
              <p className="eyebrow">Benchmark rankings</p>
              <h3>Reference baselines</h3>
            </div>
          </div>
          <div className="list-stack">
            {benchmarkRankings.slice(0, 3).map((entry, index) => {
              const benchmark = asRecord(entry);
              return (
                <article className="sub-panel" key={String(readNested(benchmark, "baseline_name") ?? index)}>
                  <div className="section-heading compact-heading">
                    <div>
                      <p className="sub-panel-title">
                        {String(readNested(benchmark, "baseline_name") ?? "n/a")}
                      </p>
                      <p className="sub-panel-stat">
                        {String(readNested(benchmark, "primary_metric") ?? "n/a")}
                      </p>
                    </div>
                  </div>
                  <div className="detail-list compact-list">
                    <div className="detail-list-item">
                      <span>Validation metric</span>
                      <strong>{String(readNested(benchmark, "validation_primary_metric") ?? "n/a")}</strong>
                    </div>
                    <div className="detail-list-item">
                      <span>Test metric</span>
                      <strong>{String(readNested(benchmark, "test_primary_metric") ?? "n/a")}</strong>
                    </div>
                    <div className="detail-list-item">
                      <span>Test predictions</span>
                      <strong>{String(readNested(benchmark, "test_prediction_count") ?? "n/a")}</strong>
                    </div>
                  </div>
                </article>
              );
            })}
          </div>
        </section>
      ) : null}
    </article>
  );
}

function ComparableCaseDetail({
  opportunity,
  comparableIndex,
  provenanceItems,
  provenanceData
}: {
  opportunity: OpportunityRecord | null;
  comparableIndex: number;
  provenanceItems?: ProvenanceItem[];
  provenanceData?: ProvenanceInspectorData;
}) {
  const evidence = asRecord(readNested(readNested(opportunity?.payload, "prediction"), "evidence"));
  const comparables = asRecord(readNested(evidence, "comparables"));
  const comparableCases = asArray(readNested(comparables, "cases"));
  const comparable = asRecord(comparableCases[comparableIndex]);
  const matchedConditions = asRecord(readNested(comparable, "matched_conditions"));
  const anchorCase = asRecord(readNested(comparables, "anchor_case"));
  const comparableSummary = asRecord(readNested(comparables, "summary"));

  if (!opportunity || !comparable) {
    return (
      <article className="panel focus-panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Comparable case</p>
            <h2>Comparable not available</h2>
          </div>
        </div>
        <p className="lead detail-copy">
          This comparable case could not be resolved from the current opportunity payload.
        </p>
      </article>
    );
  }

  return (
    <article className="panel focus-panel">
      <ProvenanceRibbon items={provenanceItems ?? []} title="Provenance" />
      {provenanceData ? <ProvenanceInspector data={provenanceData} /> : null}

      <div className="section-heading">
        <div>
          <p className="eyebrow">Comparable case</p>
          <h2>
            Game {String(readNested(comparable, "canonical_game_id") ?? "n/a")} |{" "}
            {String(readNested(comparable, "team_code") ?? "n/a")} vs{" "}
            {String(readNested(comparable, "opponent_code") ?? "n/a")}
          </h2>
        </div>
        <div className="pill-row">
          <span className="pill">
            Similarity {String(readNested(comparable, "similarity_score") ?? "n/a")}
          </span>
          <span className="pill">
            Target {String(readNested(comparable, "target_value") ?? "n/a")}
          </span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile
          label="Comparable game"
          value={String(readNested(comparable, "canonical_game_id") ?? "n/a")}
          detail={String(readNested(comparable, "game_date") ?? "n/a")}
        />
        <StatTile
          label="Prediction target"
          value={String(readNested(comparable, "target_value") ?? "n/a")}
          detail={String(readNested(comparable, "target_column") ?? "n/a")}
        />
        <StatTile
          label="Venue bucket"
          value={String(readNested(matchedConditions, "venue") ?? "n/a")}
        />
        <StatTile
          label="Rest bucket"
          value={String(readNested(matchedConditions, "days_rest_bucket") ?? "n/a")}
        />
      </div>

      <div className="detail-section-grid">
        <section className="sub-panel">
          <p className="sub-panel-title">Anchor context</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Anchor game</span>
              <strong>{String(readNested(anchorCase, "canonical_game_id") ?? opportunity.canonical_game_id ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Anchor team</span>
              <strong>{String(readNested(anchorCase, "team_code") ?? opportunity.team_code)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Pattern key</span>
              <strong>{String(readNested(comparables, "pattern_key") ?? "n/a")}</strong>
            </div>
          </div>
        </section>

        <section className="sub-panel">
          <p className="sub-panel-title">Comparable summary</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Comparable count</span>
              <strong>{String(readNested(comparableSummary, "comparable_count") ?? comparableCases.length)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Top similarity</span>
              <strong>{String(readNested(comparableSummary, "top_similarity_score") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Average similarity</span>
              <strong>{String(readNested(comparableSummary, "average_similarity_score") ?? "n/a")}</strong>
            </div>
          </div>
        </section>
      </div>

      <section className="section-stack detail-section-stack">
        <div className="section-heading standalone">
          <div>
            <p className="eyebrow">Matched conditions</p>
            <h3>Why this case matched</h3>
          </div>
        </div>

        <div className="detail-list">
          {Object.entries(matchedConditions ?? {}).map(([key, value]) => (
            <div className="detail-list-item" key={key}>
              <span>{formatLabel(key)}</span>
              <strong>{String(value)}</strong>
            </div>
          ))}
        </div>
      </section>
    </article>
  );
}

function ModelRunArtifactDetail({
  modelRun,
  provenanceItems
}: {
  modelRun: ModelTrainingRun | null;
  provenanceItems?: ProvenanceItem[];
}) {
  if (!modelRun) {
    return (
      <article className="panel focus-panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Training run detail</p>
            <h2>Training run not available</h2>
          </div>
        </div>
        <p className="lead detail-copy">
          The linked training run could not be resolved from the current opportunity provenance.
        </p>
      </article>
    );
  }

  return (
    <article className="panel focus-panel">
      <ProvenanceRibbon items={provenanceItems ?? []} title="Provenance" />

      <div className="section-heading">
        <div>
          <p className="eyebrow">Training run detail</p>
          <h2>Run #{modelRun.id}</h2>
        </div>
        <div className="pill-row">
          <span className="pill">{String(readNested(modelRun.artifact, "model_family") ?? "n/a")}</span>
          <span className="pill">{modelRun.status}</span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile label="Target task" value={modelRun.target_task} />
        <StatTile label="Season" value={modelRun.season_label ?? "n/a"} />
        <StatTile label="Train ratio" value={formatCompactNumber(modelRun.train_ratio, 2)} />
        <StatTile label="Validation ratio" value={formatCompactNumber(modelRun.validation_ratio, 2)} />
      </div>

      <div className="detail-section-grid">
        <section className="sub-panel">
          <p className="sub-panel-title">Artifact</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Selected feature</span>
              <strong>{String(readNested(modelRun.artifact, "selected_feature") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Fallback strategy</span>
              <strong>{String(readNested(modelRun.artifact, "fallback_strategy") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Fallback reason</span>
              <strong>{String(readNested(modelRun.artifact, "fallback_reason") ?? "n/a")}</strong>
            </div>
          </div>
        </section>

        <section className="sub-panel">
          <p className="sub-panel-title">Metrics</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Validation count</span>
              <strong>{String(readNested(modelRun.metrics, "validation", "prediction_count") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Validation MAE</span>
              <strong>{String(readNested(modelRun.metrics, "validation", "mae") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Test MAE</span>
              <strong>{String(readNested(modelRun.metrics, "test", "mae") ?? "n/a")}</strong>
            </div>
          </div>
        </section>
      </div>
    </article>
  );
}

function SelectionArtifactDetail({
  selection,
  provenanceItems
}: {
  selection: SelectionSnapshot | null;
  provenanceItems?: ProvenanceItem[];
}) {
  if (!selection) {
    return (
      <article className="panel focus-panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Selection detail</p>
            <h2>Selection snapshot not available</h2>
          </div>
        </div>
      </article>
    );
  }

  return (
    <article className="panel focus-panel">
      <ProvenanceRibbon items={provenanceItems ?? []} title="Provenance" />

      <div className="section-heading">
        <div>
          <p className="eyebrow">Selection detail</p>
          <h2>Selection #{selection.id}</h2>
        </div>
        <div className="pill-row">
          <span className="pill">{selection.model_family}</span>
          <span className="pill">{selection.is_active ? "active" : "inactive"}</span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile label="Target task" value={selection.target_task} />
        <StatTile label="Policy" value={selection.selection_policy_name} />
        <StatTile label="Training run" value={selection.model_training_run_id ? `#${selection.model_training_run_id}` : "n/a"} />
        <StatTile label="Evaluation" value={selection.model_evaluation_snapshot_id ? `#${selection.model_evaluation_snapshot_id}` : "n/a"} />
      </div>

      <section className="sub-panel">
        <p className="sub-panel-title">Rationale</p>
        <p className="sub-panel-meta">{selection.rationale ?? "No rationale was stored for this snapshot."}</p>
      </section>
    </article>
  );
}

function EvaluationArtifactDetail({
  evaluation,
  provenanceItems
}: {
  evaluation: EvaluationSnapshot | null;
  provenanceItems?: ProvenanceItem[];
}) {
  if (!evaluation) {
    return (
      <article className="panel focus-panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Evaluation detail</p>
            <h2>Evaluation snapshot not available</h2>
          </div>
        </div>
      </article>
    );
  }

  return (
    <article className="panel focus-panel">
      <ProvenanceRibbon items={provenanceItems ?? []} title="Provenance" />

      <div className="section-heading">
        <div>
          <p className="eyebrow">Evaluation detail</p>
          <h2>Evaluation #{evaluation.id}</h2>
        </div>
        <div className="pill-row">
          <span className="pill">{evaluation.model_family}</span>
          <span className="pill">{evaluation.primary_metric_name ?? "n/a"}</span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile label="Selected feature" value={evaluation.selected_feature ?? "n/a"} />
        <StatTile label="Fallback" value={evaluation.fallback_strategy ?? "n/a"} />
        <StatTile label="Validation" value={formatCompactNumber(evaluation.validation_metric_value, 4)} />
        <StatTile label="Test" value={formatCompactNumber(evaluation.test_metric_value, 4)} />
      </div>

      <section className="detail-section-grid">
        <section className="sub-panel">
          <p className="sub-panel-title">Prediction counts</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Validation predictions</span>
              <strong>{String(evaluation.validation_prediction_count)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Test predictions</span>
              <strong>{String(evaluation.test_prediction_count)}</strong>
            </div>
          </div>
        </section>

        <section className="sub-panel">
          <p className="sub-panel-title">Snapshot metadata</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Training run</span>
              <strong>{evaluation.model_training_run_id ? `#${evaluation.model_training_run_id}` : "n/a"}</strong>
            </div>
            <div className="detail-list-item">
              <span>Created</span>
              <strong>{formatTimestamp(evaluation.created_at)}</strong>
            </div>
          </div>
        </section>
      </section>
    </article>
  );
}

function ScoringRunArtifactDetail({
  scoringRun,
  provenanceItems
}: {
  scoringRun: ScoringRunDetail | null;
  provenanceItems?: ProvenanceItem[];
}) {
  if (!scoringRun) {
    return (
      <article className="panel focus-panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Scoring run detail</p>
            <h2>Scoring run not available</h2>
          </div>
        </div>
      </article>
    );
  }

  return (
    <article className="panel focus-panel">
      <ProvenanceRibbon items={provenanceItems ?? []} title="Provenance" />

      <div className="section-heading">
        <div>
          <p className="eyebrow">Scoring run detail</p>
          <h2>Scoring #{scoringRun.id}</h2>
        </div>
        <div className="pill-row">
          <span className="pill">{scoringRun.policy_name}</span>
          <span className="pill">{scoringRun.target_task}</span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile label="Scenario key" value={scoringRun.scenario_key} />
        <StatTile label="Prediction count" value={String(scoringRun.prediction_count)} />
        <StatTile label="Review opportunities" value={String(scoringRun.review_opportunity_count)} />
        <StatTile label="Candidate opportunities" value={String(scoringRun.candidate_opportunity_count)} />
      </div>

      <div className="detail-section-grid">
        <section className="sub-panel">
          <p className="sub-panel-title">Scenario</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Home team</span>
              <strong>{scoringRun.home_team_code}</strong>
            </div>
            <div className="detail-list-item">
              <span>Away team</span>
              <strong>{scoringRun.away_team_code}</strong>
            </div>
            <div className="detail-list-item">
              <span>Game date</span>
              <strong>{scoringRun.game_date}</strong>
            </div>
          </div>
        </section>

        <section className="sub-panel">
          <p className="sub-panel-title">Market context</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Home spread</span>
              <strong>{formatCompactNumber(scoringRun.home_spread_line, 2)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Total line</span>
              <strong>{formatCompactNumber(scoringRun.total_line, 2)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Discarded opportunities</span>
              <strong>{String(scoringRun.discarded_opportunity_count)}</strong>
            </div>
          </div>
        </section>
      </div>
    </article>
  );
}

function ArtifactCompareView({
  runId,
  fold,
  foldEvaluation,
  opportunity,
  opportunityEvaluation,
  selection,
  compareHref
}: {
  runId: number | null;
  fold: FoldSummary | null;
  foldEvaluation: EvaluationSnapshot | null;
  opportunity: OpportunityRecord | null;
  opportunityEvaluation: EvaluationSnapshot | null;
  selection: SelectionSnapshot | null;
  compareHref?: string;
}) {
  if (!fold || !opportunity) {
    return (
      <article className="panel focus-panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Artifact comparison</p>
            <h2>Comparison context not available</h2>
          </div>
        </div>
        <p className="lead detail-copy">
          Open one backtest fold and one active opportunity to compare their evaluation artifacts,
          promoted selection, and drift signals side by side.
        </p>
      </article>
    );
  }

  const foldValidation = foldEvaluation?.validation_metric_value ?? fold.selected_model.validation_metric_value;
  const foldTest = foldEvaluation?.test_metric_value ?? fold.selected_model.test_metric_value;
  const opportunityValidation = opportunityEvaluation?.validation_metric_value ?? null;
  const opportunityTest = opportunityEvaluation?.test_metric_value ?? null;
  const familyAligned =
    opportunityEvaluation?.model_family !== undefined
      ? fold.selected_model.model_family === opportunityEvaluation.model_family
      : selection?.model_family !== undefined
        ? fold.selected_model.model_family === selection.model_family
        : null;
  const featureAligned =
    opportunityEvaluation?.selected_feature !== undefined
      ? (fold.selected_model.selected_feature ?? "fallback") ===
        (opportunityEvaluation.selected_feature ?? "fallback")
      : null;
  const selectionEvaluationLinked =
    selection?.model_evaluation_snapshot_id !== undefined &&
    selection?.model_evaluation_snapshot_id !== null &&
    opportunityEvaluation?.id !== undefined
      ? selection.model_evaluation_snapshot_id === opportunityEvaluation.id
      : null;
  const validationDelta = getMetricDelta(foldValidation, opportunityValidation);
  const testDelta = getMetricDelta(foldTest, opportunityTest);
  const validationDeltaAbs = validationDelta === null ? null : Math.abs(validationDelta);
  const testDeltaAbs = testDelta === null ? null : Math.abs(testDelta);
  const mismatchMessages = [
    familyAligned === false
      ? `Backtest fold selected ${fold.selected_model.model_family}, but the opportunity evaluation is using ${opportunityEvaluation?.model_family ?? "a different model context"}.`
      : null,
    featureAligned === false
      ? `Selected feature drifted from ${fold.selected_model.selected_feature ?? "fallback"} to ${opportunityEvaluation?.selected_feature ?? "fallback"}.`
      : null,
    selectionEvaluationLinked === false
      ? `The promoted selection points to evaluation #${selection?.model_evaluation_snapshot_id}, while the active opportunity is using evaluation #${opportunityEvaluation?.id}.`
      : null,
    validationDelta !== null && Math.abs(validationDelta) >= 0.05
      ? `Validation metric moved by ${formatDelta(validationDelta)} between the backtest fold and the active opportunity evaluation.`
      : null,
    testDelta !== null && Math.abs(testDelta) >= 0.05
      ? `Test metric moved by ${formatDelta(testDelta)} between the backtest fold and the active opportunity evaluation.`
      : null
  ].filter((message): message is string => Boolean(message));
  const alignmentSummary = [
    {
      label: "Model family",
      value: getAlignmentLabel(familyAligned),
      tone: getAlignmentTone(familyAligned),
      detail:
        opportunityEvaluation?.model_family !== undefined
          ? `${fold.selected_model.model_family} vs ${opportunityEvaluation.model_family}`
          : selection?.model_family ?? "No opportunity model family"
    },
    {
      label: "Selected feature",
      value: getAlignmentLabel(featureAligned),
      tone: getAlignmentTone(featureAligned),
      detail: `${fold.selected_model.selected_feature ?? "fallback"} vs ${
        opportunityEvaluation?.selected_feature ?? "n/a"
      }`
    },
    {
      label: "Selection link",
      value: getAlignmentLabel(selectionEvaluationLinked),
      tone: getAlignmentTone(selectionEvaluationLinked),
      detail:
        selection?.model_evaluation_snapshot_id && opportunityEvaluation?.id
          ? `#${selection.model_evaluation_snapshot_id} vs #${opportunityEvaluation.id}`
          : "Missing evaluation linkage"
    }
  ];
  const artifactLinks = [
    runId
      ? {
          href: routeHash({
            name: "backtest-fold-evaluation",
            runId,
            foldIndex: fold.fold_index,
            evaluationId: fold.selected_model.evaluation_snapshot_id
          }),
          label: "Open fold evaluation"
        }
      : null,
    opportunity.model_evaluation_snapshot_id
      ? {
          href: routeHash({
            name: "opportunity-evaluation",
            opportunityId: opportunity.id,
            evaluationId: opportunity.model_evaluation_snapshot_id
          }),
          label: "Open opportunity evaluation"
        }
      : null,
    opportunity.model_selection_snapshot_id
      ? {
          href: routeHash({
            name: "opportunity-selection",
            opportunityId: opportunity.id,
            selectionId: opportunity.model_selection_snapshot_id
          }),
          label: "Open promoted selection"
        }
      : null
  ].filter((link): link is { href: string; label: string } => Boolean(link));
  const mismatchCount = mismatchMessages.length;
  const severeDrift =
    (validationDeltaAbs !== null && validationDeltaAbs >= 0.1) ||
    (testDeltaAbs !== null && testDeltaAbs >= 0.1) ||
    familyAligned === false;
  const moderateDrift =
    severeDrift ||
    mismatchCount >= 2 ||
    (validationDeltaAbs !== null && validationDeltaAbs >= 0.05) ||
    (testDeltaAbs !== null && testDeltaAbs >= 0.05) ||
    featureAligned === false ||
    selectionEvaluationLinked === false;
  const comparisonStatus = severeDrift
    ? "high_drift"
    : moderateDrift
      ? "review_drift"
      : "aligned";
  const comparisonTone =
    comparisonStatus === "aligned"
      ? "good"
      : comparisonStatus === "review_drift"
        ? "warning"
        : "critical";
  const comparisonHeadline =
    comparisonStatus === "aligned"
      ? "Backtest and live opportunity artifacts are materially aligned."
      : comparisonStatus === "review_drift"
        ? "There is measurable drift between the validation fold and the active opportunity."
        : "This opportunity has drifted far enough from the backtest fold to warrant extra caution.";
  const comparisonAction =
    comparisonStatus === "aligned"
      ? "Proceed with normal analyst review and keep this opportunity in the current workflow."
      : comparisonStatus === "review_drift"
        ? "Review the evaluation artifact and promoted selection before trusting the live opportunity."
        : "Treat this as a high-risk mismatch until the opportunity evaluation and promoted selection are reconciled.";
  const nextStepItems =
    comparisonStatus === "aligned"
      ? [
          "Confirm the market context still matches the scenario assumptions.",
          "Use the opportunity deep-dive to inspect comparables and recommendation rationale."
        ]
      : comparisonStatus === "review_drift"
        ? [
            "Open the opportunity evaluation artifact and compare its metrics to the fold evaluation.",
            "Verify the promoted selection still points to the expected evaluation snapshot.",
            "Re-check the comparable cases before escalating the opportunity."
          ]
        : [
            "Open both evaluation artifacts and inspect why the model family or metrics diverged.",
            "Confirm the promoted selection is still the right active artifact for this target task.",
            "Do not treat this opportunity as production-grade without analyst sign-off."
          ];

  return (
    <article className="panel focus-panel">
      <ProvenanceRibbon
        items={[
          ...(runId
            ? [
                {
                  href: routeHash({ name: "backtest-run", runId }),
                  label: "Run route",
                  value: `Run #${runId}`
                }
              ]
            : []),
          ...(compareHref ? [{ href: compareHref, label: "Compare route", value: "Artifact compare" }] : []),
          {
            href: routeHash({
              name: "opportunity-detail",
              opportunityId: opportunity.id
            }),
            label: "Opportunity",
            value: `Opportunity #${opportunity.id}`
          }
        ]}
        title="Provenance"
      />

      <div className="section-heading">
        <div>
          <p className="eyebrow">Artifact comparison</p>
          <h2>Fold vs opportunity evidence</h2>
        </div>
        <div className="pill-row">
          <span className="pill">Fold {fold.fold_index}</span>
          <span className="pill">{opportunity.team_code} vs {opportunity.opponent_code}</span>
        </div>
      </div>

      <div className="route-action-row">
        {artifactLinks.map((link) => (
          <a className="secondary-button inline-link-button" href={link.href} key={link.href}>
            {link.label}
          </a>
        ))}
      </div>

      <section className="section-stack detail-section-stack">
        <div className="section-heading standalone">
          <div>
            <p className="eyebrow">Decision summary</p>
            <h3>Analyst guidance</h3>
          </div>
        </div>

        <div className={`sub-panel compare-decision-card compare-decision-card-${comparisonTone}`}>
          <div className="section-heading compact-heading">
            <div>
              <p className="sub-panel-title">Comparison verdict</p>
              <p className="sub-panel-stat">{formatLabel(comparisonStatus)}</p>
            </div>
            <div className="pill-row">
              <span className={`compare-status-pill compare-status-pill-${comparisonTone}`}>
                {formatLabel(comparisonStatus)}
              </span>
            </div>
          </div>
          <p className="detail-copy">{comparisonHeadline}</p>
          <p className="sub-panel-meta">{comparisonAction}</p>
        </div>

        <div className="compare-decision-grid">
          <div className="sub-panel alignment-card">
            <p className="sub-panel-title">Mismatch count</p>
            <p className="sub-panel-stat">{String(mismatchCount)}</p>
            <p className="sub-panel-meta">Explicit drift conditions currently triggered.</p>
          </div>
          <div className="sub-panel alignment-card">
            <p className="sub-panel-title">Largest metric drift</p>
            <p className="sub-panel-stat">
              {formatDelta(
                validationDeltaAbs !== null && testDeltaAbs !== null
                  ? (validationDeltaAbs >= testDeltaAbs ? validationDelta : testDelta)
                  : validationDelta ?? testDelta
              )}
            </p>
            <p className="sub-panel-meta">Largest observed gap between fold and opportunity metrics.</p>
          </div>
          <div className="sub-panel alignment-card">
            <p className="sub-panel-title">Recommended posture</p>
            <p className="sub-panel-stat">
              {comparisonStatus === "aligned"
                ? "Normal review"
                : comparisonStatus === "review_drift"
                  ? "Manual review"
                  : "Escalate"}
            </p>
            <p className="sub-panel-meta">Suggested analyst handling for this comparison state.</p>
          </div>
        </div>
      </section>

      <section className="section-stack detail-section-stack">
        <div className="section-heading standalone">
          <div>
            <p className="eyebrow">Alignment summary</p>
            <h3>Cross-artifact checks</h3>
          </div>
        </div>

        <div className="compare-summary-grid">
          {alignmentSummary.map((item) => (
            <div className={`sub-panel alignment-card alignment-card-${item.tone}`} key={item.label}>
              <p className="sub-panel-title">{item.label}</p>
              <p className="sub-panel-stat">{formatLabel(item.value)}</p>
              <p className="sub-panel-meta">{item.detail}</p>
            </div>
          ))}
          <div className="sub-panel alignment-card">
            <p className="sub-panel-title">Validation delta</p>
            <p className="sub-panel-stat">{formatDelta(validationDelta)}</p>
            <p className="sub-panel-meta">
              Fold {formatCompactNumber(foldValidation, 4)} vs opportunity{" "}
              {formatCompactNumber(opportunityValidation, 4)}
            </p>
          </div>
          <div className="sub-panel alignment-card">
            <p className="sub-panel-title">Test delta</p>
            <p className="sub-panel-stat">{formatDelta(testDelta)}</p>
            <p className="sub-panel-meta">
              Fold {formatCompactNumber(foldTest, 4)} vs opportunity{" "}
              {formatCompactNumber(opportunityTest, 4)}
            </p>
          </div>
        </div>
      </section>

      <div className="compare-grid">
        <section className="sub-panel compare-card">
          <p className="sub-panel-title">Backtest fold evaluation</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Model family</span>
              <strong>{fold.selected_model.model_family}</strong>
            </div>
            <div className="detail-list-item">
              <span>Selected feature</span>
              <strong>{fold.selected_model.selected_feature ?? "fallback"}</strong>
            </div>
            <div className="detail-list-item">
              <span>Validation metric</span>
              <strong>{formatCompactNumber(foldValidation, 4)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Test metric</span>
              <strong>{formatCompactNumber(foldTest, 4)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Evaluation id</span>
              <strong>#{fold.selected_model.evaluation_snapshot_id}</strong>
            </div>
          </div>
        </section>

        <section className="sub-panel compare-card">
          <p className="sub-panel-title">Opportunity evaluation</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Model family</span>
              <strong>{opportunityEvaluation?.model_family ?? "n/a"}</strong>
            </div>
            <div className="detail-list-item">
              <span>Selected feature</span>
              <strong>{opportunityEvaluation?.selected_feature ?? "n/a"}</strong>
            </div>
            <div className="detail-list-item">
              <span>Validation metric</span>
              <strong>{formatCompactNumber(opportunityEvaluation?.validation_metric_value ?? null, 4)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Test metric</span>
              <strong>{formatCompactNumber(opportunityEvaluation?.test_metric_value ?? null, 4)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Evaluation id</span>
              <strong>{opportunityEvaluation ? `#${opportunityEvaluation.id}` : "n/a"}</strong>
            </div>
          </div>
        </section>

        <section className="sub-panel compare-card">
          <p className="sub-panel-title">Promoted selection</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Model family</span>
              <strong>{selection?.model_family ?? "n/a"}</strong>
            </div>
            <div className="detail-list-item">
              <span>Selection policy</span>
              <strong>{selection?.selection_policy_name ?? "n/a"}</strong>
            </div>
            <div className="detail-list-item">
              <span>Active</span>
              <strong>{selection ? String(selection.is_active) : "n/a"}</strong>
            </div>
            <div className="detail-list-item">
              <span>Evaluation link</span>
              <strong>
                {selection?.model_evaluation_snapshot_id
                  ? `#${selection.model_evaluation_snapshot_id}`
                  : "n/a"}
              </strong>
            </div>
            <div className="detail-list-item">
              <span>Rationale</span>
              <strong>{selection?.rationale ?? "n/a"}</strong>
            </div>
          </div>
        </section>
      </div>

      <section className="section-stack detail-section-stack">
        <div className="section-heading standalone">
          <div>
            <p className="eyebrow">Mismatch review</p>
            <h3>Where the artifacts diverge</h3>
          </div>
        </div>

        {mismatchMessages.length > 0 ? (
          <div className="compare-warning-list">
            {mismatchMessages.map((message) => (
              <article className="sub-panel compare-warning-card" key={message}>
                <p className="sub-panel-title">Attention point</p>
                <p className="detail-copy">{message}</p>
              </article>
            ))}
          </div>
        ) : (
          <article className="sub-panel compare-warning-card compare-warning-card-good">
            <p className="sub-panel-title">Alignment status</p>
            <p className="detail-copy">
              No material mismatches were detected across the backtest fold, opportunity evaluation,
              and promoted selection.
            </p>
          </article>
        )}
      </section>

      <section className="section-stack detail-section-stack">
        <div className="section-heading standalone">
          <div>
            <p className="eyebrow">Next steps</p>
            <h3>Recommended review path</h3>
          </div>
        </div>

        <article className="sub-panel">
          <ul className="detail-bullets">
            {nextStepItems.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </article>
      </section>
    </article>
  );
}

export default function App() {
  const [history, setHistory] = useState<BacktestHistoryResponse | null>(null);
  const [opportunityHistory, setOpportunityHistory] = useState<OpportunityHistoryResponse | null>(null);
  const [opportunities, setOpportunities] = useState<OpportunityRecord[]>([]);
  const [modelHistory, setModelHistory] = useState<ModelHistoryResponse["model_history"] | null>(null);
  const [activeModelRun, setActiveModelRun] = useState<ModelTrainingRun | null>(null);
  const [activeSelectionSnapshot, setActiveSelectionSnapshot] = useState<SelectionSnapshot | null>(null);
  const [activeEvaluationSnapshot, setActiveEvaluationSnapshot] = useState<EvaluationSnapshot | null>(null);
  const [activeBacktestFoldModelRun, setActiveBacktestFoldModelRun] = useState<ModelTrainingRun | null>(null);
  const [activeBacktestFoldEvaluation, setActiveBacktestFoldEvaluation] = useState<EvaluationSnapshot | null>(null);
  const [activeScoringRun, setActiveScoringRun] = useState<ScoringRunDetail | null>(null);
  const [activeRun, setActiveRun] = useState<BacktestRun | null>(null);
  const [activeOpportunityId, setActiveOpportunityId] = useState<number | null>(null);
  const [activeOpportunity, setActiveOpportunity] = useState<OpportunityRecord | null>(null);
  const [route, setRoute] = useState<AppRoute>(() => parseRouteFromHash(window.location.hash));
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(false);
  const [materializingOpportunity, setMaterializingOpportunity] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    function handleHashChange() {
      setRoute(parseRouteFromHash(window.location.hash));
    }

    window.addEventListener("hashchange", handleHashChange);

    if (!window.location.hash) {
      window.location.hash = routeHash({ name: "backtests" });
    } else {
      handleHashChange();
    }

    return () => {
      window.removeEventListener("hashchange", handleHashChange);
    };
  }, []);

  function navigate(nextRoute: AppRoute) {
    window.location.hash = routeHash(nextRoute);
  }

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        setLoading(true);
        setError(null);
        const [
          nextHistory,
          nextOpportunityHistory,
          nextOpportunityList,
          nextModelHistory
        ] = await Promise.all([
          fetchBacktestHistory(),
          fetchOpportunityHistory(),
          fetchOpportunities(),
          fetchModelHistory()
        ]);
        if (cancelled) {
          return;
        }
        setHistory(nextHistory);
        const preferredRun =
          route.name === "backtest-run"
            ? nextHistory.model_backtest_history.recent_runs.find((run) => run.id === route.runId) ??
              nextHistory.model_backtest_history.overview.latest_run
            : nextHistory.model_backtest_history.overview.latest_run;
        setActiveRun(preferredRun);
        setOpportunityHistory(nextOpportunityHistory);
        setOpportunities(nextOpportunityList.opportunities);
        setModelHistory(nextModelHistory.model_history);
        const preferredOpportunityId =
          route.name === "opportunity-detail" ||
          route.name === "comparable-case" ||
          route.name === "opportunity-model-run" ||
          route.name === "opportunity-selection" ||
          route.name === "opportunity-evaluation" ||
          route.name === "opportunity-scoring-run" ||
          route.name === "artifact-compare"
            ? route.opportunityId
            : nextOpportunityList.opportunities[0]?.id ??
              nextOpportunityHistory.model_opportunity_history.overview.latest_opportunity?.id ??
              null;
        setActiveOpportunityId(
          preferredOpportunityId
        );
      } catch (loadError) {
        if (cancelled) {
          return;
        }
        setError(loadError instanceof Error ? loadError.message : "Failed to load the Phase 4 console.");
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void load();

    return () => {
      cancelled = true;
    };
  }, [route]);

  useEffect(() => {
    if (
      route.name === "opportunity-detail" ||
      route.name === "comparable-case" ||
      route.name === "opportunity-model-run" ||
      route.name === "opportunity-selection" ||
      route.name === "opportunity-evaluation" ||
      route.name === "opportunity-scoring-run" ||
      route.name === "artifact-compare"
    ) {
      setActiveOpportunityId(route.opportunityId);
      return;
    }
    if (route.name === "opportunities" && activeOpportunityId === null) {
      setActiveOpportunityId(opportunities[0]?.id ?? null);
    }
  }, [route, opportunities, activeOpportunityId]);

  useEffect(() => {
    let cancelled = false;

    async function loadOpportunityDetail() {
      if (activeOpportunityId === null) {
        setActiveOpportunity(null);
        return;
      }

      try {
        const detail = await fetchOpportunityDetail(activeOpportunityId);
        if (!cancelled) {
          setActiveOpportunity(detail.opportunity);
        }
      } catch (detailError) {
        if (!cancelled) {
          setError(detailError instanceof Error ? detailError.message : "Failed to load opportunity detail.");
        }
      }
    }

    void loadOpportunityDetail();

    return () => {
      cancelled = true;
    };
  }, [activeOpportunityId]);

  useEffect(() => {
    let cancelled = false;

    async function loadBacktestRunDetail() {
      if (
        route.name !== "backtest-run" &&
        route.name !== "backtest-fold" &&
        route.name !== "backtest-fold-model-run" &&
        route.name !== "backtest-fold-evaluation" &&
        route.name !== "artifact-compare"
      ) {
        return;
      }

      try {
        const detail = await fetchBacktestRunDetail(route.runId);
        if (!cancelled) {
          setActiveRun(detail.backtest_run);
        }
      } catch (detailError) {
        if (!cancelled) {
          setError(detailError instanceof Error ? detailError.message : "Failed to load backtest run.");
        }
      }
    }

    void loadBacktestRunDetail();

    return () => {
      cancelled = true;
    };
  }, [route]);

  useEffect(() => {
    let cancelled = false;

    async function loadBacktestFoldArtifactDetails() {
      if (
        route.name !== "backtest-fold" &&
        route.name !== "backtest-fold-model-run" &&
        route.name !== "backtest-fold-evaluation" &&
        route.name !== "artifact-compare"
      ) {
        setActiveBacktestFoldModelRun(null);
        setActiveBacktestFoldEvaluation(null);
        return;
      }

      const selectedFold =
        activeRun?.payload.folds.find((fold) => fold.fold_index === route.foldIndex) ?? null;

      if (!selectedFold) {
        setActiveBacktestFoldModelRun(null);
        setActiveBacktestFoldEvaluation(null);
        return;
      }

      try {
        const [modelRunDetail, evaluationDetail] = await Promise.all([
          fetchModelRunDetail(selectedFold.selected_model.model_training_run_id),
          fetchEvaluationDetail(selectedFold.selected_model.evaluation_snapshot_id)
        ]);
        if (!cancelled) {
          setActiveBacktestFoldModelRun(modelRunDetail.model_run);
          setActiveBacktestFoldEvaluation(evaluationDetail.evaluation_snapshot);
        }
      } catch (detailError) {
        if (!cancelled) {
          setActiveBacktestFoldModelRun(null);
          setActiveBacktestFoldEvaluation(null);
          setError(
            detailError instanceof Error
              ? detailError.message
              : "Failed to load backtest fold provenance details."
          );
        }
      }
    }

    void loadBacktestFoldArtifactDetails();

    return () => {
      cancelled = true;
    };
  }, [route, activeRun]);

  useEffect(() => {
    let cancelled = false;

    async function loadModelArtifactDetails() {
      if (!activeOpportunity) {
        setActiveSelectionSnapshot(null);
        setActiveEvaluationSnapshot(null);
        setActiveModelRun(null);
        return;
      }

      try {
        const [selectionDetail, evaluationDetail] = await Promise.all([
          activeOpportunity.model_selection_snapshot_id
            ? fetchSelectionDetail(activeOpportunity.model_selection_snapshot_id)
            : Promise.resolve({ repository_mode: "in_memory", selection: null }),
          activeOpportunity.model_evaluation_snapshot_id
            ? fetchEvaluationDetail(activeOpportunity.model_evaluation_snapshot_id)
            : Promise.resolve({ repository_mode: "in_memory", evaluation_snapshot: null })
        ]);
        if (cancelled) {
          return;
        }

        setActiveSelectionSnapshot(selectionDetail.selection);
        setActiveEvaluationSnapshot(evaluationDetail.evaluation_snapshot);

        const trainingRunId =
          selectionDetail.selection?.model_training_run_id ??
          evaluationDetail.evaluation_snapshot?.model_training_run_id ??
          null;

        if (trainingRunId === null) {
          setActiveModelRun(null);
          return;
        }

        const modelRunDetail = await fetchModelRunDetail(trainingRunId);
        if (!cancelled) {
          setActiveModelRun(modelRunDetail.model_run);
        }
      } catch (detailError) {
        if (!cancelled) {
          setActiveSelectionSnapshot(null);
          setActiveEvaluationSnapshot(null);
          setActiveModelRun(null);
          setError(
            detailError instanceof Error
              ? detailError.message
              : "Failed to load model provenance details."
          );
        }
      }
    }

    void loadModelArtifactDetails();

    return () => {
      cancelled = true;
    };
  }, [activeOpportunity]);

  useEffect(() => {
    let cancelled = false;

    async function loadScoringRunDetail() {
      if (!activeOpportunity || activeOpportunity.source_kind !== "future_scenario" || !activeOpportunity.model_scoring_run_id) {
        setActiveScoringRun(null);
        return;
      }

      const scenario = asRecord(readNested(activeOpportunity.payload, "scenario"));
      if (!scenario) {
        setActiveScoringRun(null);
        return;
      }

      try {
        const detail = await fetchScoringRunDetail(activeOpportunity.model_scoring_run_id, {
          ...scenario,
          target_task: activeOpportunity.target_task
        });
        if (!cancelled) {
          setActiveScoringRun(detail.scoring_run);
        }
      } catch {
        if (!cancelled) {
          setActiveScoringRun(null);
        }
      }
    }

    void loadScoringRunDetail();

    return () => {
      cancelled = true;
    };
  }, [activeOpportunity]);

  async function handleRunBacktest() {
    try {
      setRunning(true);
      setError(null);
      const result = await runBacktest();
      setActiveRun(result.backtest_run);
      const refreshedHistory = await fetchBacktestHistory();
      setHistory(refreshedHistory);
    } catch (runError) {
      setError(runError instanceof Error ? runError.message : "Failed to run backtest.");
    } finally {
      setRunning(false);
    }
  }

  async function handleMaterializeOpportunities() {
    try {
      setMaterializingOpportunity(true);
      setError(null);
      const materialized = await materializeOpportunities();
      const [nextOpportunityHistory, nextOpportunityList] = await Promise.all([
        fetchOpportunityHistory(),
        fetchOpportunities()
      ]);
      setOpportunityHistory(nextOpportunityHistory);
      setOpportunities(nextOpportunityList.opportunities);
      const nextOpportunityId = materialized.opportunities[0]?.id ?? nextOpportunityList.opportunities[0]?.id ?? null;
      setActiveOpportunityId(nextOpportunityId);
      if (nextOpportunityId !== null) {
        navigate({ name: "opportunity-detail", opportunityId: nextOpportunityId });
      }
    } catch (materializeError) {
      setError(
        materializeError instanceof Error ? materializeError.message : "Failed to materialize opportunities."
      );
    } finally {
      setMaterializingOpportunity(false);
    }
  }

  const overview = history?.model_backtest_history.overview;
  const summary = activeRun?.payload ?? null;
  const opportunityOverview = opportunityHistory?.model_opportunity_history.overview;
  const viewMode =
    route.name === "backtests" ||
    route.name === "backtest-run" ||
    route.name === "backtest-fold" ||
    route.name === "backtest-fold-model-run" ||
    route.name === "backtest-fold-evaluation" ||
    route.name === "artifact-compare"
      ? "backtests"
      : "opportunities";
  const inBacktestDetail = route.name === "backtest-run";
  const inBacktestFoldDetail = route.name === "backtest-fold";
  const inBacktestFoldModelRunDetail = route.name === "backtest-fold-model-run";
  const inBacktestFoldEvaluationDetail = route.name === "backtest-fold-evaluation";
  const inArtifactCompare = route.name === "artifact-compare";
  const inBacktestArtifactDetail =
    inBacktestFoldModelRunDetail || inBacktestFoldEvaluationDetail;
  const inOpportunityDetail = route.name === "opportunity-detail";
  const inComparableCase = route.name === "comparable-case";
  const inModelRunDetail = route.name === "opportunity-model-run";
  const inSelectionDetail = route.name === "opportunity-selection";
  const inEvaluationDetail = route.name === "opportunity-evaluation";
  const inScoringRunDetail = route.name === "opportunity-scoring-run";
  const inOpportunityArtifactDetail =
    inModelRunDetail || inSelectionDetail || inEvaluationDetail || inScoringRunDetail;
  const inOpportunityContextDetail =
    inOpportunityDetail || inComparableCase || inOpportunityArtifactDetail;
  const heroTitle =
    route.name === "backtests"
      ? "Phase 4 backtest console is live."
      : inBacktestDetail
        ? "Backtest run inspection is open."
      : inBacktestFoldDetail
        ? "Backtest fold inspection is open."
      : inBacktestFoldModelRunDetail
        ? "Backtest fold training provenance is open."
      : inBacktestFoldEvaluationDetail
        ? "Backtest fold evaluation provenance is open."
      : inArtifactCompare
        ? "Artifact comparison is open."
      : inModelRunDetail
        ? "Training run provenance is open."
      : inSelectionDetail
        ? "Selection snapshot provenance is open."
      : inEvaluationDetail
        ? "Evaluation snapshot provenance is open."
      : inScoringRunDetail
        ? "Scoring run provenance is open."
      : inComparableCase
        ? "Comparable case inspection is open."
        : inOpportunityDetail
        ? "Opportunity investigation is open."
        : "Analyst opportunity desk is online.";
  const heroLead =
    route.name === "backtests"
      ? "This dashboard runs and inspects the first walk-forward validation layer on top of the predictive stack. It shows whether the current spread or totals edge logic holds up once we retrain chronologically and simulate threshold-based decisions."
      : inBacktestDetail
        ? "This route focuses on one exact walk-forward validation run, so provenance links can target the specific model-selection and threshold simulation result behind a review workflow."
      : inBacktestFoldDetail
        ? "This route focuses on one exact walk-forward fold, so you can inspect the selected model, game split, and threshold outcomes behind a single chronological validation step."
      : inBacktestFoldModelRunDetail
        ? "This route resolves the exact training run chosen inside a walk-forward fold, so the backtest side can inspect the concrete fitted artifact behind the fold decision."
      : inBacktestFoldEvaluationDetail
        ? "This route resolves the exact evaluation snapshot chosen inside a walk-forward fold, so the backtest side can inspect the concrete metric record behind the fold decision."
      : inArtifactCompare
        ? "This route compares one backtest fold’s evaluation artifact against the active opportunity evaluation and promoted selection, so analysts can inspect where validation and live scoring line up or drift."
      : inModelRunDetail
        ? "This route resolves the exact training run behind the selected opportunity, including the chosen feature, fallback behavior, and validation metrics that shaped the downstream signal."
      : inSelectionDetail
        ? "This route focuses on the promoted selection snapshot, so you can inspect the exact policy decision that made one trained model active for scoring."
      : inEvaluationDetail
        ? "This route focuses on the exact evaluation snapshot behind the active selection, so you can inspect the metric values and prediction counts that justified promotion."
      : inScoringRunDetail
        ? "This route focuses on the exact scoring run behind a future-style opportunity, so you can inspect the market scenario and generated opportunity counts without staying inside the parent card."
      : inComparableCase
        ? "This route focuses on one comparable historical case from the evidence bundle, so you can inspect exactly why it matched the parent opportunity and what it contributed to the analyst judgment."
        : inOpportunityDetail
        ? "This route is the analyst deep-dive for one materialized opportunity. It keeps the evidence bundle, comparables, benchmark context, and model provenance in one inspectable workspace."
        : "This view turns the Phase 3 scoring pipeline into an analyst workflow. It surfaces recent opportunities, keeps the evidence bundle attached, and lets you inspect why a case is only reviewable or strong enough to escalate.";
  const activeServicePath =
    route.name === "backtests"
      ? `${apiBaseUrl}/api/v1/analyst/backtests`
      : `${apiBaseUrl}/api/v1/analyst/opportunities`;
  const backtestOverviewHref = routeHash({ name: "backtests" });
  const activeRunHref = activeRun ? routeHash({ name: "backtest-run", runId: activeRun.id }) : undefined;
  const activeOpportunityHref =
    activeOpportunityId !== null
      ? routeHash({ name: "opportunity-detail", opportunityId: activeOpportunityId })
      : undefined;
  const activeModelRunHref =
    activeOpportunityId !== null && activeModelRun
      ? routeHash({
          name: "opportunity-model-run",
          opportunityId: activeOpportunityId,
          runId: activeModelRun.id
        })
      : undefined;
  const activeSelectionHref =
    activeOpportunityId !== null && activeSelectionSnapshot
      ? routeHash({
          name: "opportunity-selection",
          opportunityId: activeOpportunityId,
          selectionId: activeSelectionSnapshot.id
        })
      : undefined;
  const activeEvaluationHref =
    activeOpportunityId !== null && activeEvaluationSnapshot
      ? routeHash({
          name: "opportunity-evaluation",
          opportunityId: activeOpportunityId,
          evaluationId: activeEvaluationSnapshot.id
        })
      : undefined;
  const activeScoringRunHref =
    activeOpportunityId !== null && activeScoringRun
      ? routeHash({
          name: "opportunity-scoring-run",
          opportunityId: activeOpportunityId,
          scoringRunId: activeScoringRun.id
        })
      : undefined;
  const backtestProvenanceItems: ProvenanceItem[] = activeRun
    ? [
        { href: backtestOverviewHref, label: "Dashboard", value: "Backtest overview" },
        { href: activeRunHref, label: "Run route", value: `Run #${activeRun.id}` },
        { label: "Selection policy", value: activeRun.selection_policy_name },
        { label: "Completed", value: formatTimestamp(activeRun.completed_at) }
      ]
    : [];
  const activeFold =
    route.name === "backtest-fold" ||
    route.name === "backtest-fold-model-run" ||
    route.name === "backtest-fold-evaluation" ||
    route.name === "artifact-compare"
      ? activeRun?.payload.folds.find((fold) => fold.fold_index === route.foldIndex) ?? null
      : null;
  const artifactCompareHref =
    activeRun && activeFold && activeOpportunityId !== null
      ? routeHash({
          name: "artifact-compare",
          runId: activeRun.id,
          foldIndex: activeFold.fold_index,
          opportunityId: activeOpportunityId
        })
      : undefined;
  const backtestFoldProvenanceItems: ProvenanceItem[] = activeRun
    ? [
        { href: backtestOverviewHref, label: "Dashboard", value: "Backtest overview" },
        { href: activeRunHref, label: "Run route", value: `Run #${activeRun.id}` },
        ...(activeFold
          ? [
              {
                href: routeHash({
                  name: "backtest-fold",
                  runId: activeRun.id,
                  foldIndex: activeFold.fold_index
                }),
                label: "Fold",
                value: `Fold ${activeFold.fold_index}`
              }
            ]
          : []),
        ...(activeFold && activeFold.selected_model.model_training_run_id > 0
          ? [
              {
                href: routeHash({
                  name: "backtest-fold-model-run",
                  runId: activeRun.id,
                  foldIndex: activeFold.fold_index,
                  modelRunId: activeFold.selected_model.model_training_run_id
                }),
                label: "Training run",
                value: `Run #${activeFold.selected_model.model_training_run_id}`
              }
            ]
          : []),
        ...(activeFold
          ? [
              {
                href: routeHash({
                  name: "backtest-fold-evaluation",
                  runId: activeRun.id,
                  foldIndex: activeFold.fold_index,
                  evaluationId: activeFold.selected_model.evaluation_snapshot_id
                }),
                label: "Evaluation",
                value: `Evaluation #${activeFold.selected_model.evaluation_snapshot_id}`
              }
            ]
          : [])
      ]
    : [];
  const opportunityProvenanceItems: ProvenanceItem[] = activeOpportunity
    ? [
        ...(activeRunHref
          ? [{ href: activeRunHref, label: "Validation run", value: `Run #${activeRun?.id}` }]
          : [{ href: backtestOverviewHref, label: "Validation", value: "Backtest overview" }]),
        ...(activeOpportunity.model_selection_snapshot_id
          ? [
              {
                href: activeSelectionHref,
                label: "Selection snapshot",
                value: `Selection #${activeOpportunity.model_selection_snapshot_id}`
              }
            ]
          : []),
        ...(activeOpportunity.model_evaluation_snapshot_id
          ? [
              {
                href: activeEvaluationHref,
                label: "Evaluation snapshot",
                value: `Evaluation #${activeOpportunity.model_evaluation_snapshot_id}`
              }
            ]
          : []),
        ...(activeModelRun
          ? [{ href: activeModelRunHref, label: "Training run", value: `Run #${activeModelRun.id}` }]
          : []),
        ...(activeOpportunity.model_scoring_run_id
          ? [{ href: activeScoringRunHref, label: "Scoring run", value: `Scoring #${activeOpportunity.model_scoring_run_id}` }]
          : []),
        ...(activeOpportunity.feature_version_id
          ? [{ label: "Feature version", value: `Version #${activeOpportunity.feature_version_id}` }]
          : [])
      ]
    : [];
  const comparableProvenanceItems: ProvenanceItem[] = [
    ...(activeOpportunityHref
      ? [{ href: activeOpportunityHref, label: "Parent opportunity", value: `Opportunity #${activeOpportunityId}` }]
      : []),
    ...opportunityProvenanceItems
  ];
  const opportunityProvenanceData: ProvenanceInspectorData = {
    evaluation: activeEvaluationSnapshot,
    modelHistory: null,
    modelRun: activeModelRun,
    scoringRun: activeScoringRun,
    selection: activeSelectionSnapshot
  };
  const backtestProvenanceData: ProvenanceInspectorData = {
    evaluation: null,
    modelHistory,
    modelRun: null,
    scoringRun: null,
    selection: null
  };
  const comparableProvenanceData: ProvenanceInspectorData = {
    evaluation: activeEvaluationSnapshot,
    modelHistory: null,
    modelRun: activeModelRun,
    scoringRun: activeScoringRun,
    selection: activeSelectionSnapshot
  };

  return (
    <main className="app-shell">
      <section className="hero">
        <div className="hero-copy">
          <p className="eyebrow">Bookmaker Mistake Detector</p>
          <h1>{heroTitle}</h1>
          <p className="lead">{heroLead}</p>
        </div>

        <div className="hero-actions">
          <div className="mode-switch">
            <button
              className={`mode-button${viewMode === "backtests" ? " mode-button-active" : ""}`}
              onClick={() => navigate({ name: "backtests" })}
              type="button"
            >
              Backtests
            </button>
            <button
              className={`mode-button${viewMode === "opportunities" ? " mode-button-active" : ""}`}
              onClick={() => navigate({ name: "opportunities" })}
              type="button"
            >
              Opportunities
            </button>
          </div>

          {viewMode === "backtests" ? (
            <button className="primary-button" disabled={running} onClick={() => void handleRunBacktest()}>
              {running ? "Running backtest..." : "Run new backtest"}
            </button>
          ) : (
            <button
              className="primary-button"
              disabled={materializingOpportunity}
              onClick={() => void handleMaterializeOpportunities()}
            >
              {materializingOpportunity ? "Refreshing opportunities..." : "Materialize opportunities"}
            </button>
          )}

          <p className="service-note">API target: {activeServicePath}</p>
        </div>
      </section>

      {inBacktestDetail || inBacktestFoldDetail || inBacktestArtifactDetail || inArtifactCompare || inOpportunityContextDetail ? (
        <section className="route-toolbar">
          {inBacktestDetail || inBacktestFoldDetail || inBacktestArtifactDetail || inArtifactCompare ? (
            <button className="secondary-button" onClick={() => navigate({ name: "backtests" })} type="button">
              Back to runs
            </button>
          ) : (
            <button className="secondary-button" onClick={() => navigate({ name: "opportunities" })} type="button">
              Back to queue
            </button>
          )}
          {(inBacktestFoldDetail || inBacktestArtifactDetail || inArtifactCompare) && activeRun ? (
            <button
              className="secondary-button"
              onClick={() => navigate({ name: "backtest-run", runId: activeRun.id })}
              type="button"
            >
              Back to run
            </button>
          ) : null}
          {(inBacktestArtifactDetail || inArtifactCompare) && activeRun && activeFold ? (
            <button
              className="secondary-button"
              onClick={() =>
                navigate({
                  name: "backtest-fold",
                  runId: activeRun.id,
                  foldIndex: activeFold.fold_index
                })
              }
              type="button"
            >
              Back to fold
            </button>
          ) : null}
          {inComparableCase || inOpportunityArtifactDetail || inArtifactCompare ? (
            <button
              className="secondary-button"
              onClick={() =>
                activeOpportunityId !== null
                  ? navigate({ name: "opportunity-detail", opportunityId: activeOpportunityId })
                  : navigate({ name: "opportunities" })
              }
              type="button"
            >
              Back to opportunity
            </button>
          ) : null}
          {!inBacktestDetail && !inBacktestFoldDetail && !inBacktestArtifactDetail && !inArtifactCompare ? (
            <button className="secondary-button" onClick={() => navigate({ name: "backtests" })} type="button">
              Open backtests
            </button>
          ) : null}
          {(inBacktestDetail || inBacktestFoldDetail || inBacktestArtifactDetail || inArtifactCompare) && activeRun ? (
            <p className="route-note">
              Run #{activeRun.id} | {activeRun.payload.strategy_name} | {activeRun.fold_count} folds
            </p>
          ) : null}
          {!inBacktestDetail && activeOpportunity ? (
            <p className="route-note">
              Opportunity #{activeOpportunity.id} | {activeOpportunity.team_code} vs {activeOpportunity.opponent_code}
            </p>
          ) : null}
        </section>
      ) : null}

      {error ? <section className="banner banner-error">{error}</section> : null}
      {loading ? <section className="banner">Loading the Phase 4 analyst workspace...</section> : null}

      {!loading && viewMode === "backtests" && history && overview ? (
        <>
          <section className="stat-grid">
            <StatTile label="Backtest runs" value={String(overview.run_count)} />
            <StatTile
              label="Latest task"
              value={history.filters.target_task}
              detail={activeRun?.selection_policy_name ?? "no active run"}
            />
            <StatTile
              label="Best candidate ROI"
              value={formatPercent(
                overview.best_candidate_threshold_run?.payload.strategy_results.candidate_threshold.roi ?? null
              )}
            />
            <StatTile
              label="Latest completed"
              value={formatTimestamp(overview.latest_run?.completed_at ?? null)}
            />
          </section>

          {route.name === "backtests" && summary ? (
            <section className="dashboard-grid">
              <article className="panel focus-panel">
                <div className="section-heading">
                  <div>
                    <p className="eyebrow">Active run</p>
                    <h2>{summary.strategy_name}</h2>
                  </div>
                  <div className="pill-row">
                    <span className="pill">Run #{activeRun?.id}</span>
                    <span className="pill">{summary.fold_count} folds</span>
                  </div>
                </div>

                <div className="mini-grid">
                  <StatTile label="Dataset games" value={String(summary.dataset_game_count)} />
                  <StatTile label="Dataset rows" value={String(summary.dataset_row_count)} />
                  <StatTile label="Prediction MAE" value={formatMetric(summary.prediction_metrics.mae)} />
                  <StatTile label="Prediction RMSE" value={formatMetric(summary.prediction_metrics.rmse)} />
                </div>

                <div className="mini-grid family-grid">
                  {Object.entries(summary.selected_model_family_counts).map(([family, count]) => (
                    <div className="sub-panel" key={family}>
                      <p className="sub-panel-title">{family}</p>
                      <p className="sub-panel-stat">{count}</p>
                      <p className="sub-panel-meta">fold selections</p>
                    </div>
                  ))}
                </div>
              </article>

              <article className="panel">
                <div className="section-heading">
                  <div>
                    <p className="eyebrow">Recent runs</p>
                    <h2>History</h2>
                  </div>
                </div>

                <div className="table-shell">
                  <table>
                    <thead>
                      <tr>
                        <th>Run</th>
                        <th>Completed</th>
                        <th>Folds</th>
                        <th>Candidate ROI</th>
                      </tr>
                    </thead>
                    <tbody>
                      {history.model_backtest_history.recent_runs.map((run) => (
                        <tr
                          className={activeRun?.id === run.id ? "row-active" : undefined}
                          key={run.id}
                          onClick={() => navigate({ name: "backtest-run", runId: run.id })}
                        >
                          <td>#{run.id}</td>
                          <td>{formatTimestamp(run.completed_at)}</td>
                          <td>{run.fold_count}</td>
                          <td>{formatPercent(run.payload.strategy_results.candidate_threshold.roi)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </article>
            </section>
          ) : null}

          {route.name === "backtest-run" ? (
            <>
              <section className="section-stack">
                <BacktestRunDetailCard
                  provenanceData={backtestProvenanceData}
                  provenanceItems={backtestProvenanceItems}
                  run={activeRun}
                />
              </section>

              {summary ? (
                <>
                  <section className="section-stack">
                    <div className="section-heading standalone">
                      <div>
                        <p className="eyebrow">Strategy results</p>
                        <h2>Threshold simulation</h2>
                      </div>
                    </div>

                    <div className="strategy-grid">
                      {Object.entries(summary.strategy_results).map(([label, strategy]) => (
                        <StrategyCard key={label} label={label.replace("_", " ")} strategy={strategy} />
                      ))}
                    </div>
                  </section>

                  <section className="section-stack">
                    <div className="section-heading standalone">
                      <div>
                        <p className="eyebrow">Walk-forward folds</p>
                        <h2>Chronological validation</h2>
                      </div>
                    </div>

                    <div className="fold-grid">
                      {summary.folds.map((fold) => (
                        <button
                          className="unstyled-card-button"
                          key={fold.fold_index}
                          onClick={() =>
                            activeRun
                              ? navigate({
                                  name: "backtest-fold",
                                  runId: activeRun.id,
                                  foldIndex: fold.fold_index
                                })
                              : undefined
                          }
                          type="button"
                        >
                          <FoldCard fold={fold} />
                        </button>
                      ))}
                    </div>
                  </section>
                </>
              ) : null}
            </>
          ) : route.name === "backtest-fold" ? (
            <section className="section-stack">
              <FoldDetailCard
                compareHref={artifactCompareHref}
                fold={activeFold}
                provenanceItems={backtestFoldProvenanceItems}
                runId={activeRun?.id ?? null}
              />
            </section>
          ) : route.name === "backtest-fold-model-run" ? (
            <section className="section-stack">
              <ModelRunArtifactDetail
                modelRun={activeBacktestFoldModelRun}
                provenanceItems={backtestFoldProvenanceItems}
              />
            </section>
          ) : route.name === "backtest-fold-evaluation" ? (
            <section className="section-stack">
              <EvaluationArtifactDetail
                evaluation={activeBacktestFoldEvaluation}
                provenanceItems={backtestFoldProvenanceItems}
              />
            </section>
          ) : route.name === "artifact-compare" ? (
            <section className="section-stack">
              <ArtifactCompareView
                compareHref={artifactCompareHref}
                fold={activeFold}
                foldEvaluation={activeBacktestFoldEvaluation}
                opportunity={activeOpportunity}
                opportunityEvaluation={activeEvaluationSnapshot}
                runId={activeRun?.id ?? null}
                selection={activeSelectionSnapshot}
              />
            </section>
          ) : summary ? (
            <>
              <section className="section-stack">
                <div className="section-heading standalone">
                  <div>
                    <p className="eyebrow">Strategy results</p>
                    <h2>Threshold simulation</h2>
                  </div>
                </div>

                <div className="strategy-grid">
                  {Object.entries(summary.strategy_results).map(([label, strategy]) => (
                    <StrategyCard key={label} label={label.replace("_", " ")} strategy={strategy} />
                  ))}
                </div>
              </section>

              <section className="section-stack">
                <div className="section-heading standalone">
                  <div>
                    <p className="eyebrow">Walk-forward folds</p>
                    <h2>Chronological validation</h2>
                  </div>
                </div>

                <div className="fold-grid">
                  {summary.folds.map((fold) => (
                    <button
                      className="unstyled-card-button"
                      key={fold.fold_index}
                      onClick={() =>
                        activeRun
                          ? navigate({
                              name: "backtest-fold",
                              runId: activeRun.id,
                              foldIndex: fold.fold_index
                            })
                          : undefined
                      }
                      type="button"
                    >
                      <FoldCard fold={fold} />
                    </button>
                  ))}
                </div>
              </section>
            </>
          ) : null}
        </>
      ) : null}

      {!loading && viewMode === "opportunities" && opportunityHistory && opportunityOverview ? (
        <>
          <section className="stat-grid">
            <StatTile label="Opportunity count" value={String(opportunityOverview.opportunity_count)} />
            <StatTile
              label="Review queue"
              value={String(opportunityOverview.status_counts.review_manually ?? 0)}
            />
            <StatTile
              label="Candidate signals"
              value={String(opportunityOverview.status_counts.candidate_signal ?? 0)}
            />
            <StatTile
              label="Latest update"
              value={formatTimestamp(opportunityOverview.latest_opportunity?.updated_at ?? null)}
            />
          </section>

          {route.name === "opportunities" ? (
            <section className="dashboard-grid">
              <article className="panel">
                <div className="section-heading">
                  <div>
                    <p className="eyebrow">Recent opportunities</p>
                    <h2>Analyst queue</h2>
                  </div>
                  <div className="pill-row">
                    <span className="pill">
                      Historical {String(opportunityOverview.source_kind_counts.historical_game ?? 0)}
                    </span>
                    <span className="pill">
                      Future {String(opportunityOverview.source_kind_counts.future_scenario ?? 0)}
                    </span>
                  </div>
                </div>

                <div className="list-stack">
                  {opportunities.length === 0 ? (
                    <p className="sub-panel-meta">
                      No opportunities are available yet. Use the materialize action to build a
                      fresh analyst queue from the current scoring flow.
                    </p>
                  ) : (
                    opportunities.map((opportunity) => (
                    <OpportunityListItem
                      active={activeOpportunityId === opportunity.id}
                      key={opportunity.id}
                      onSelect={(opportunityId) => navigate({ name: "opportunity-detail", opportunityId })}
                      opportunity={opportunity}
                    />
                  ))
                )}
              </div>
            </article>

              <OpportunityDetailCard
                compareHref={artifactCompareHref}
                onSelectComparable={(comparableIndex) => {
                  if (activeOpportunityId !== null) {
                    navigate({ name: "comparable-case", opportunityId: activeOpportunityId, comparableIndex });
                  }
                }}
                provenanceData={opportunityProvenanceData}
                provenanceItems={opportunityProvenanceItems}
                opportunity={activeOpportunity}
              />
            </section>
          ) : route.name === "comparable-case" ? (
            <section className="section-stack">
              <ComparableCaseDetail
                comparableIndex={route.comparableIndex}
                provenanceData={comparableProvenanceData}
                provenanceItems={comparableProvenanceItems}
                opportunity={activeOpportunity}
              />
            </section>
          ) : route.name === "opportunity-model-run" ? (
            <section className="section-stack">
              <ModelRunArtifactDetail modelRun={activeModelRun} provenanceItems={opportunityProvenanceItems} />
            </section>
          ) : route.name === "opportunity-selection" ? (
            <section className="section-stack">
              <SelectionArtifactDetail
                provenanceItems={opportunityProvenanceItems}
                selection={activeSelectionSnapshot}
              />
            </section>
          ) : route.name === "opportunity-evaluation" ? (
            <section className="section-stack">
              <EvaluationArtifactDetail
                evaluation={activeEvaluationSnapshot}
                provenanceItems={opportunityProvenanceItems}
              />
            </section>
          ) : route.name === "opportunity-scoring-run" ? (
            <section className="section-stack">
              <ScoringRunArtifactDetail
                provenanceItems={opportunityProvenanceItems}
                scoringRun={activeScoringRun}
              />
            </section>
          ) : (
            <section className="section-stack">
              <OpportunityDetailCard
                compareHref={artifactCompareHref}
                onSelectComparable={(comparableIndex) => {
                  if (activeOpportunityId !== null) {
                    navigate({ name: "comparable-case", opportunityId: activeOpportunityId, comparableIndex });
                  }
                }}
                provenanceData={opportunityProvenanceData}
                provenanceItems={opportunityProvenanceItems}
                opportunity={activeOpportunity}
              />
            </section>
          )}

          <section className="section-stack">
            <div className="section-heading standalone">
              <div>
                <p className="eyebrow">History rollup</p>
                <h2>Status and evidence mix</h2>
              </div>
            </div>

            <div className="mini-grid family-grid">
              {Object.entries(opportunityOverview.status_counts).map(([status, count]) => (
                <div className="sub-panel" key={status}>
                  <p className="sub-panel-title">{formatLabel(status)}</p>
                  <p className="sub-panel-stat">{count}</p>
                  <p className="sub-panel-meta">status count</p>
                </div>
              ))}
              {Object.entries(opportunityOverview.evidence_rating_counts ?? {}).map(([rating, count]) => (
                <div className="sub-panel" key={rating}>
                  <p className="sub-panel-title">{formatLabel(rating)}</p>
                  <p className="sub-panel-stat">{count}</p>
                  <p className="sub-panel-meta">evidence count</p>
                </div>
              ))}
            </div>
          </section>
        </>
      ) : null}
    </main>
  );
}
