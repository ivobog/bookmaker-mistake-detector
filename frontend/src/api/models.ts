import type {
  EvaluationDetailResponse,
  ModelHistoryResponse,
  ModelRunDetailResponse,
  ScoringRunDetailResponse,
  SelectionDetailResponse
} from "../appTypes";
import { readNested } from "../appUtils";
import { apiGet } from "./client";
import {
  buildModelArtifactQuery,
  buildSharedTrainingQuery,
  resolveDefaultTargetTask,
  resolveScenarioDefaults,
} from "./defaults";
import { applyScenarioQuery } from "../../../shared/frontend/query";

function requireScenarioValue(
  scenario: Record<string, unknown>,
  key: string,
  fallback: string | null,
  label: string
): string {
  const value = readNested(scenario, key);
  if (value !== undefined && value !== null && String(value).trim()) {
    return String(value);
  }
  if (fallback && fallback.trim()) {
    return fallback;
  }
  throw new Error(`Missing scoring scenario ${label}`);
}

export async function fetchModelHistory(): Promise<ModelHistoryResponse> {
  const query = await buildModelArtifactQuery(5);
  return apiGet<ModelHistoryResponse>("/api/v1/admin/models/history", {
    errorPrefix: "Failed to load model history",
    query
  });
}

export async function fetchModelRunDetail(runId: number): Promise<ModelRunDetailResponse> {
  const query = await buildModelArtifactQuery();
  return apiGet<ModelRunDetailResponse>(`/api/v1/admin/models/runs/${runId}`, {
    errorPrefix: "Failed to load model run detail",
    query
  });
}

export async function fetchSelectionDetail(selectionId: number): Promise<SelectionDetailResponse> {
  const query = await buildModelArtifactQuery();
  return apiGet<SelectionDetailResponse>(`/api/v1/admin/models/selections/${selectionId}`, {
    errorPrefix: "Failed to load model selection detail",
    query
  });
}

export async function fetchEvaluationDetail(snapshotId: number): Promise<EvaluationDetailResponse> {
  const query = await buildModelArtifactQuery();
  return apiGet<EvaluationDetailResponse>(`/api/v1/admin/models/evaluations/${snapshotId}`, {
    errorPrefix: "Failed to load model evaluation detail",
    query
  });
}

export async function fetchScoringRunDetail(
  scoringRunId: number,
  scenario: Record<string, unknown>
): Promise<ScoringRunDetailResponse> {
  const scenarioDefaults = resolveScenarioDefaults();
  const query = await buildSharedTrainingQuery();
  const defaultTargetTask = await resolveDefaultTargetTask();
  const resolvedTargetTask = readNested(scenario, "target_task") ?? defaultTargetTask;
  applyScenarioQuery(query, {
    targetTask: resolvedTargetTask === undefined ? null : String(resolvedTargetTask),
    seasonLabel: requireScenarioValue(scenario, "season_label", scenarioDefaults.seasonLabel, "season label"),
    gameDate: requireScenarioValue(scenario, "game_date", scenarioDefaults.gameDate, "game date"),
    homeTeamCode: requireScenarioValue(scenario, "home_team_code", scenarioDefaults.homeTeamCode, "home team code"),
    awayTeamCode: requireScenarioValue(scenario, "away_team_code", scenarioDefaults.awayTeamCode, "away team code"),
    homeSpreadLine: readNested(scenario, "home_spread_line") as string | number | null | undefined,
    totalLine: readNested(scenario, "total_line") as string | number | null | undefined
  });

  return apiGet<ScoringRunDetailResponse>(
    `/api/v1/admin/models/future-game-preview/runs/${scoringRunId}`,
    {
      errorPrefix: "Failed to load scoring run detail",
      query
    }
  );
}
