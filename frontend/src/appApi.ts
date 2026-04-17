import type {
  BacktestHistoryResponse,
  BacktestRunResponse,
  EvaluationDetailResponse,
  ModelHistoryResponse,
  ModelRunDetailResponse,
  OpportunityDetailResponse,
  OpportunityHistoryResponse,
  OpportunityListResponse,
  OpportunityMaterializeResponse,
  ScoringRunDetailResponse,
  SelectionDetailResponse
} from "./appTypes";
import { readNested } from "./appUtils";

export const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";

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

export async function fetchBacktestHistory(): Promise<BacktestHistoryResponse> {
  const response = await fetch(
    `${apiBaseUrl}/api/v1/admin/models/backtests/history?${adminBacktestHistoryQuery}`
  );
  if (!response.ok) {
    throw new Error(`Failed to load backtest history (${response.status})`);
  }
  return (await response.json()) as BacktestHistoryResponse;
}

export async function runBacktest(): Promise<BacktestRunResponse> {
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

export async function fetchBacktestRunDetail(backtestRunId: number): Promise<BacktestRunResponse> {
  const response = await fetch(
    `${apiBaseUrl}/api/v1/analyst/backtests/${backtestRunId}?${analystBacktestQuery}`
  );
  if (!response.ok) {
    throw new Error(`Failed to load backtest run (${response.status})`);
  }
  return (await response.json()) as BacktestRunResponse;
}

export async function fetchOpportunityHistory(): Promise<OpportunityHistoryResponse> {
  const response = await fetch(
    `${apiBaseUrl}/api/v1/admin/models/opportunities/history?${adminOpportunityHistoryQuery}`
  );
  if (!response.ok) {
    throw new Error(`Failed to load opportunity history (${response.status})`);
  }
  return (await response.json()) as OpportunityHistoryResponse;
}

export async function fetchOpportunities(): Promise<OpportunityListResponse> {
  const response = await fetch(`${apiBaseUrl}/api/v1/analyst/opportunities?${analystOpportunityQuery}`);
  if (!response.ok) {
    throw new Error(`Failed to load opportunities (${response.status})`);
  }
  return (await response.json()) as OpportunityListResponse;
}

export async function fetchOpportunityDetail(
  opportunityId: number
): Promise<OpportunityDetailResponse> {
  const response = await fetch(
    `${apiBaseUrl}/api/v1/analyst/opportunities/${opportunityId}?${analystOpportunityQuery}`
  );
  if (!response.ok) {
    throw new Error(`Failed to load opportunity detail (${response.status})`);
  }
  return (await response.json()) as OpportunityDetailResponse;
}

export async function materializeOpportunities(): Promise<OpportunityMaterializeResponse> {
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

export async function fetchModelHistory(): Promise<ModelHistoryResponse> {
  const response = await fetch(
    `${apiBaseUrl}/api/v1/admin/models/history?${defaultModelArtifactQuery}&recent_limit=5`
  );
  if (!response.ok) {
    throw new Error(`Failed to load model history (${response.status})`);
  }
  return (await response.json()) as ModelHistoryResponse;
}

export async function fetchModelRunDetail(runId: number): Promise<ModelRunDetailResponse> {
  const response = await fetch(
    `${apiBaseUrl}/api/v1/admin/models/runs/${runId}?${defaultModelArtifactQuery}`
  );
  if (!response.ok) {
    throw new Error(`Failed to load model run detail (${response.status})`);
  }
  return (await response.json()) as ModelRunDetailResponse;
}

export async function fetchSelectionDetail(
  selectionId: number
): Promise<SelectionDetailResponse> {
  const response = await fetch(
    `${apiBaseUrl}/api/v1/admin/models/selections/${selectionId}?${defaultModelArtifactQuery}`
  );
  if (!response.ok) {
    throw new Error(`Failed to load model selection detail (${response.status})`);
  }
  return (await response.json()) as SelectionDetailResponse;
}

export async function fetchEvaluationDetail(
  snapshotId: number
): Promise<EvaluationDetailResponse> {
  const response = await fetch(
    `${apiBaseUrl}/api/v1/admin/models/evaluations/${snapshotId}?${defaultModelArtifactQuery}`
  );
  if (!response.ok) {
    throw new Error(`Failed to load model evaluation detail (${response.status})`);
  }
  return (await response.json()) as EvaluationDetailResponse;
}

export async function fetchScoringRunDetail(
  scoringRunId: number,
  scenario: Record<string, unknown>
): Promise<ScoringRunDetailResponse> {
  const query = new URLSearchParams({
    target_task: String(
      readNested(scenario, "target_task") ??
        adminOpportunityMutationQuery.get("target_task") ??
        "spread_error_regression"
    ),
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
