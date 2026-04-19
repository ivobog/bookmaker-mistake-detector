import { expect, type APIRequestContext } from "@playwright/test";

type QueryValue = boolean | number | string | null | undefined;

function buildQuery(params: Record<string, QueryValue>): string {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === "") {
      continue;
    }
    query.set(key, String(value));
  }
  const queryString = query.toString();
  return queryString ? `?${queryString}` : "";
}

export function apiBaseUrl(): string {
  return process.env.E2E_API_BASE_URL ?? "http://127.0.0.1:8000";
}

export async function waitForBackendHealth(request: APIRequestContext): Promise<void> {
  const response = await request.get(`${apiBaseUrl()}/api/v1/health`, { failOnStatusCode: false });
  expect(response.ok(), `backend health returned ${response.status()}`).toBeTruthy();
}

async function getJson<T>(
  request: APIRequestContext,
  path: string,
  params: Record<string, QueryValue> = {},
): Promise<T> {
  const response = await request.get(`${apiBaseUrl()}${path}${buildQuery(params)}`, {
    failOnStatusCode: false
  });
  expect(response.ok(), `GET ${path} returned ${response.status()}`).toBeTruthy();
  return (await response.json()) as T;
}

async function postJson<T>(
  request: APIRequestContext,
  path: string,
  params: Record<string, QueryValue> = {},
): Promise<T> {
  const response = await request.post(`${apiBaseUrl()}${path}${buildQuery(params)}`, {
    failOnStatusCode: false
  });
  expect(response.ok(), `POST ${path} returned ${response.status()}`).toBeTruthy();
  return (await response.json()) as T;
}

export async function resetTestState(request: APIRequestContext): Promise<void> {
  await postJson(request, "/api/v1/test/reset");
}

export async function seedMinimalDataset(request: APIRequestContext): Promise<void> {
  await postJson(request, "/api/v1/test/seed-minimal-dataset");
}

export async function seedE2eDataset(request: APIRequestContext): Promise<void> {
  await postJson(request, "/api/v1/test/seed-e2e-dataset");
}

export async function materializeBaselineFeatures(request: APIRequestContext): Promise<void> {
  await postJson(request, "/api/v1/test/materialize-baseline-features");
}

export async function trainModel(
  request: APIRequestContext,
  params: {
    featureKey?: string;
    seasonLabel?: string | null;
    targetTask?: string;
    teamCode?: string | null;
    trainRatio?: number;
    validationRatio?: number;
  },
): Promise<Record<string, unknown>> {
  return postJson(request, "/api/v1/admin/models/train", {
    feature_key: params.featureKey,
    season_label: params.seasonLabel,
    target_task: params.targetTask,
    team_code: params.teamCode,
    train_ratio: params.trainRatio,
    validation_ratio: params.validationRatio
  });
}

export async function selectBestModel(
  request: APIRequestContext,
  params: {
    selectionPolicyName?: string;
    targetTask?: string;
  },
): Promise<Record<string, unknown>> {
  return postJson(request, "/api/v1/admin/models/select", {
    selection_policy_name: params.selectionPolicyName,
    target_task: params.targetTask
  });
}

export async function runBacktest(
  request: APIRequestContext,
  params: {
    featureKey?: string;
    targetTask?: string;
    teamCode?: string | null;
    seasonLabel?: string | null;
    minimumTrainGames?: number;
    testWindowGames?: number;
  } = {},
): Promise<Record<string, unknown>> {
  return postJson(request, "/api/v1/admin/models/backtests/run", {
    feature_key: params.featureKey,
    target_task: params.targetTask,
    team_code: params.teamCode,
    season_label: params.seasonLabel,
    minimum_train_games: params.minimumTrainGames,
    test_window_games: params.testWindowGames
  });
}

export async function materializeHistoricalOpportunities(
  request: APIRequestContext,
  params: {
    featureKey?: string;
    includeEvidence?: boolean;
    limit?: number;
    seasonLabel?: string | null;
    targetTask?: string;
    teamCode?: string | null;
  } = {},
): Promise<Record<string, unknown>> {
  return postJson(request, "/api/v1/admin/models/opportunities/materialize", {
    feature_key: params.featureKey,
    include_evidence: params.includeEvidence,
    limit: params.limit,
    season_label: params.seasonLabel,
    target_task: params.targetTask,
    team_code: params.teamCode
  });
}

export async function getOpportunityQueue(
  request: APIRequestContext,
  params: {
    seasonLabel?: string | null;
    targetTask?: string;
    teamCode?: string | null;
  } = {},
): Promise<Record<string, unknown>> {
  return getJson(request, "/api/v1/analyst/opportunities", {
    season_label: params.seasonLabel,
    target_task: params.targetTask,
    team_code: params.teamCode,
    limit: 25
  });
}

export async function getOpportunityDetail(
  request: APIRequestContext,
  opportunityId: number,
): Promise<Record<string, unknown>> {
  return getJson(request, `/api/v1/analyst/opportunities/${opportunityId}`);
}

export async function activateSelection(
  request: APIRequestContext,
  params: {
    modelFamily: string;
    selectionPolicyName?: string;
    targetTask?: string;
  },
): Promise<Record<string, unknown>> {
  return postJson(request, "/api/v1/test/activate-selection", {
    model_family: params.modelFamily,
    selection_policy_name: params.selectionPolicyName,
    target_task: params.targetTask
  });
}
