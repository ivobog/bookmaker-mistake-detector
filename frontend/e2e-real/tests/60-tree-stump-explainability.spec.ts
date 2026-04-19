import { test, expect } from "../fixtures/appHarness";
import {
  activateSelection,
  getOpportunityDetail,
  getOpportunityQueue,
  materializeHistoricalOpportunities,
  trainModel
} from "../helpers/api";
import {
  getLatestActiveSelection,
  getLatestBatch,
  getLatestEvaluationByFamily,
  getOpportunitiesForBatch
} from "../helpers/db";
import { collectPageErrors, expectNoFatalUiErrors, openRoute } from "../helpers/ui";

test("tree-stump opportunities render explainability and repeated leaf predictions stay explainable", async ({
  page,
  request,
  featureKey,
  targetTask
}) => {
  const pageErrors = collectPageErrors(page);

  await trainModel(request, {
    featureKey,
    targetTask
  });
  await activateSelection(request, {
    modelFamily: "tree_stump",
    targetTask
  });
  await materializeHistoricalOpportunities(request, {
    featureKey,
    limit: 25,
    targetTask
  });

  const activeSelection = await getLatestActiveSelection(targetTask);
  expect(activeSelection?.model_family).toBe("tree_stump");

  const evaluation = await getLatestEvaluationByFamily(targetTask, "tree_stump");
  expect(evaluation).not.toBeNull();
  const snapshot = (evaluation?.snapshot ?? {}) as Record<string, unknown>;
  const artifact = (snapshot.artifact ?? {}) as Record<string, unknown>;
  expect(artifact.selected_feature).toBeTruthy();
  expect(artifact.threshold).not.toBeNull();
  expect(artifact.left_prediction).not.toBeNull();
  expect(artifact.right_prediction).not.toBeNull();

  const batch = await getLatestBatch(targetTask, "operator");
  expect(batch?.materialization_batch_id).toBeTruthy();
  const opportunities = await getOpportunitiesForBatch(String(batch?.materialization_batch_id));
  expect(opportunities.length).toBeGreaterThan(0);

  const queue = await getOpportunityQueue(request, { targetTask });
  const firstOpportunity = Array.isArray(queue.opportunities)
    ? (queue.opportunities[0] as Record<string, unknown>)
    : null;
  expect(firstOpportunity?.id).toBeTruthy();

  const opportunityDetail = await getOpportunityDetail(request, Number(firstOpportunity?.id));
  const explainability = (opportunityDetail.opportunity as Record<string, unknown>).model_explainability as Record<
    string,
    unknown
  >;
  expect(explainability.model_family).toBe("tree_stump");
  expect(explainability.selected_feature).toBe(artifact.selected_feature);
  expect(explainability.threshold).toBe(artifact.threshold);
  expect(explainability.left_prediction).toBe(artifact.left_prediction);
  expect(explainability.right_prediction).toBe(artifact.right_prediction);

  await openRoute(page, `#/opportunities/${firstOpportunity?.id as number}`);
  await expect(page.getByTestId("stump-explainability-card")).toBeVisible();
  await expect(page.getByTestId("stump-selected-feature")).toHaveText(
    String(artifact.selected_feature),
  );
  await expect(page.getByTestId("stump-threshold")).toHaveText(String(artifact.threshold));
  await expect(page.getByTestId("stump-left-prediction")).toHaveText(
    String(artifact.left_prediction),
  );
  await expect(page.getByTestId("stump-right-prediction")).toHaveText(
    String(artifact.right_prediction),
  );

  const repeatedPredictions = new Map<string, number>();
  for (const opportunity of opportunities) {
    const predictionValue = String(opportunity.prediction_value);
    repeatedPredictions.set(predictionValue, (repeatedPredictions.get(predictionValue) ?? 0) + 1);
  }
  const repeatedLeafPrediction = [...repeatedPredictions.entries()].find(([, count]) => count > 1);
  expect(repeatedLeafPrediction).toBeDefined();
  expect([
    String(artifact.left_prediction),
    String(artifact.right_prediction)
  ]).toContain(repeatedLeafPrediction?.[0] ?? "");

  await expectNoFatalUiErrors(page, pageErrors);
});
