import { test, expect } from "../fixtures/appHarness";
import { getLatestBacktestRun, row, scalar } from "../helpers/db";
import { collectPageErrors, expectNoFatalUiErrors, openRoute } from "../helpers/ui";

test("backtests run through the GUI and persist fold payloads that match the rendered view", async ({
  page,
  targetTask
}) => {
  const pageErrors = collectPageErrors(page);
  const beforeCount = Number(
    await scalar<string>("select count(*)::text from model_backtest_run where target_task = $1", [targetTask]),
  );

  await openRoute(page, "#/backtests");
  await page.getByTestId("run-backtest-button").click();

  await expect
    .poll(
      async () =>
        Number(
          await scalar<string>("select count(*)::text from model_backtest_run where target_task = $1", [targetTask]),
        ),
      { timeout: 15000 },
    )
    .toBeGreaterThan(beforeCount);

  const backtestRun = await getLatestBacktestRun(targetTask);
  const runId = Number(backtestRun?.id ?? 0);
  expect(runId).toBeGreaterThan(0);

  const afterCount = Number(
    await scalar<string>("select count(*)::text from model_backtest_run where target_task = $1", [targetTask]),
  );
  expect(afterCount).toBeGreaterThan(beforeCount);

  const payload = (backtestRun?.payload ?? {}) as Record<string, unknown>;
  await expect(page.getByTestId("backtests-history-table")).toContainText(`#${runId}`);
  await expect(page.getByText(String(backtestRun?.strategy_name ?? ""))).toBeVisible();
  await expect(page.getByText(`Run #${runId}`)).toBeVisible();
  await expect(page.getByText(`${String(backtestRun?.fold_count ?? "")} folds`)).toBeVisible();

  const firstFold = Array.isArray(payload.folds) ? (payload.folds[0] as Record<string, unknown>) : null;
  expect(firstFold).not.toBeNull();
  const foldIndex = Number(firstFold?.fold_index ?? 0);
  expect(foldIndex).toBeGreaterThan(0);

  await page.getByTestId(`backtest-fold-card-${foldIndex}`).click();
  await expect(page.getByText("Fold detail", { exact: true })).toBeVisible();

  const selectedModel = (firstFold?.selected_model ?? {}) as Record<string, unknown>;
  const foldEvaluationId = Number(selectedModel.evaluation_snapshot_id);
  expect(foldEvaluationId).toBeGreaterThan(0);

  const analystBacktest = await row<{ payload: Record<string, unknown> }>(
    "select payload_json as payload from model_backtest_run where id = $1",
    [runId],
  );
  expect(analystBacktest?.payload.prediction_metrics).toEqual(payload.prediction_metrics);
  const persistedFirstFold = Array.isArray(analystBacktest?.payload.folds)
    ? (analystBacktest?.payload.folds[0] as Record<string, unknown>)
    : null;
  expect(persistedFirstFold).not.toBeNull();
  expect((persistedFirstFold?.selected_model as Record<string, unknown>).evaluation_snapshot_id).toBe(
    foldEvaluationId,
  );
  expect((persistedFirstFold?.selected_model as Record<string, unknown>).model_family).toBe(
    selectedModel.model_family,
  );

  await expectNoFatalUiErrors(page, pageErrors);
});
