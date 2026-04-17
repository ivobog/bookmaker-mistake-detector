import { expect, test } from "@playwright/test";

const backendBaseUrl = process.env.PLAYWRIGHT_BACKEND_URL ?? "http://127.0.0.1:8000";

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

type BacktestHistoryResponse = {
  model_backtest_history: {
    recent_runs: Array<{ id: number }>;
  };
};

type BacktestRunResponse = {
  backtest_run: {
    id: number;
    payload: {
      folds: Array<{ fold_index: number }>;
    };
  } | null;
};

type OpportunityHistoryResponse = {
  model_opportunity_history: {
    recent_opportunities: Array<{ id: number }>;
  };
};

async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(`${backendBaseUrl}${path}`);
  expect(response.ok).toBeTruthy();
  return (await response.json()) as T;
}

async function getSeededRouteIds() {
  const history = await fetchJson<BacktestHistoryResponse>(
    `/api/v1/admin/models/backtests/history?${adminBacktestHistoryQuery.toString()}`
  );
  const runId = history.model_backtest_history.recent_runs[0]?.id;
  expect(runId).toBeTruthy();

  const runDetail = await fetchJson<BacktestRunResponse>(
    `/api/v1/analyst/backtests/${runId}?${analystBacktestQuery.toString()}`
  );
  const foldIndex = runDetail.backtest_run?.payload.folds[0]?.fold_index;
  expect(foldIndex).not.toBeUndefined();
  expect(foldIndex).not.toBeNull();

  const opportunityHistory = await fetchJson<OpportunityHistoryResponse>(
    `/api/v1/admin/models/opportunities/history?${adminOpportunityHistoryQuery.toString()}`
  );
  const opportunityId = opportunityHistory.model_opportunity_history.recent_opportunities[0]?.id;
  expect(opportunityId).toBeTruthy();

  return {
    runId,
    foldIndex,
    opportunityId
  };
}

test.describe("Phase 5 browser smoke", () => {
  test("loads the core Phase 4 analyst routes in a real browser", async ({ page }) => {
    const { runId, foldIndex, opportunityId } = await getSeededRouteIds();
    const compareHash = `#/compare/backtests/${runId}/folds/${foldIndex}/opportunities/${opportunityId}`;

    await page.goto("#/backtests");
    await expect(page.getByRole("heading", { name: "Chronological validation" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Analyst queue" })).toBeVisible();

    await page.goto(`#/backtests/${runId}`);
    await expect(page.getByText("Backtest run", { exact: true })).toBeVisible();
    await expect(page.getByRole("heading", { name: `Run #${runId}` })).toBeVisible();

    await page.goto(`#/backtests/${runId}/folds/${foldIndex}`);
    await expect(page.getByText("Fold detail", { exact: true })).toBeVisible();
    await expect(page.getByRole("heading", { name: `Fold ${foldIndex}` })).toBeVisible();

    await page.goto(`#/opportunities/${opportunityId}`);
    await expect(page.getByText("Opportunity detail", { exact: true })).toBeVisible();
    await expect(page.getByRole("heading", { name: /.+ vs .+/i })).toBeVisible();

    const comparableCard = page
      .getByRole("button")
      .filter({ hasText: /Game \d+/ })
      .first();
    await expect(comparableCard).toBeVisible();

    await comparableCard.click();
    await expect(page.getByText("Comparable case", { exact: true })).toBeVisible();
    await expect(page.getByRole("heading", { name: /Game \d+ \|/i })).toBeVisible();

    await page.goto(compareHash);
    await expect(page.getByText("Artifact comparison", { exact: true })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Fold vs opportunity evidence" })).toBeVisible();
    await expect(page.getByText("Decision summary", { exact: true })).toBeVisible();
  });
});
