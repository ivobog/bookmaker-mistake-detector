import { test, expect } from "../fixtures/appHarness";
import {
  getOpportunityQueue,
  materializeHistoricalOpportunities,
  selectBestModel,
  trainModel
} from "../helpers/api";
import { getLatestBatch } from "../helpers/db";
import { collectPageErrors, expectNoFatalUiErrors, openRoute } from "../helpers/ui";

test("scoped batches stay scoped and never masquerade as the current global queue", async ({
  page,
  request,
  featureKey,
  seasonLabel,
  targetTask,
  teamCode
}) => {
  const pageErrors = collectPageErrors(page);

  await trainModel(request, {
    featureKey,
    seasonLabel,
    targetTask,
    teamCode
  });
  await selectBestModel(request, { targetTask });

  await materializeHistoricalOpportunities(request, {
    featureKey,
    limit: 25,
    targetTask
  });
  const operatorBatch = await getLatestBatch(targetTask, "operator");
  expect(operatorBatch).not.toBeNull();

  await materializeHistoricalOpportunities(request, {
    featureKey,
    limit: 25,
    seasonLabel,
    targetTask,
    teamCode
  });
  const scopedBatch = await getLatestBatch(targetTask, "team_scoped", teamCode, seasonLabel);
  expect(scopedBatch).not.toBeNull();
  expect(scopedBatch?.materialization_batch_id).not.toBe(operatorBatch?.materialization_batch_id);

  const globalQueue = await getOpportunityQueue(request, { targetTask });
  expect(globalQueue.queue_scope_is_scoped).toBe(false);
  expect(globalQueue.queue_scope_label).toBe("Operator-wide queue");
  expect(globalQueue.queue_batch_id).toBe(operatorBatch?.materialization_batch_id);

  const scopedQueue = await getOpportunityQueue(request, {
    seasonLabel,
    targetTask,
    teamCode
  });
  expect(scopedQueue.queue_scope_is_scoped).toBe(true);
  expect(String(scopedQueue.queue_scope_label)).toContain("Scoped queue");
  expect(scopedQueue.queue_batch_id).toBe(scopedBatch?.materialization_batch_id);

  await openRoute(page, "#/opportunities");
  await expect(page.getByTestId("opportunities-queue-scope-badge")).toHaveText("Global");
  await expect(page.getByTestId("opportunities-queue-scope-label")).toHaveText("Operator-wide queue");
  await expect(page.getByTestId("opportunities-queue-batch-id")).toContainText(
    String(operatorBatch?.materialization_batch_id).slice(0, 8),
  );

  await page.getByTestId("opportunities-refresh-button").click();
  await expect(page).toHaveURL(/#\/opportunities\/\d+$/);
  const refreshedOperatorBatch = await getLatestBatch(targetTask, "operator");
  await openRoute(page, "#/opportunities");
  await expect(page.getByTestId("opportunities-queue-scope-badge")).toHaveText("Global");
  await expect(page.getByTestId("opportunities-queue-scope-label")).toHaveText("Operator-wide queue");
  await expect(page.getByTestId("opportunities-queue-batch-id")).toContainText(
    String(refreshedOperatorBatch?.materialization_batch_id).slice(0, 8),
  );

  await expectNoFatalUiErrors(page, pageErrors);
});
