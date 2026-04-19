import { test, expect } from "../fixtures/appHarness";
import {
  materializeHistoricalOpportunities,
  runBacktest,
  selectBestModel,
  trainModel,
  waitForBackendHealth
} from "../helpers/api";
import { assertDbInvariants, scalar } from "../helpers/db";

test("mixed real-stack mutations leave the backend healthy and invariants intact", async ({
  request,
  featureKey,
  seasonLabel,
  targetTask,
  teamCode
}) => {
  await trainModel(request, {
    featureKey,
    seasonLabel,
    targetTask,
    teamCode
  });
  await selectBestModel(request, { targetTask });
  await runBacktest(request, {
    featureKey,
    targetTask
  });
  await materializeHistoricalOpportunities(request, {
    featureKey,
    limit: 25,
    targetTask
  });

  await waitForBackendHealth(request);
  await assertDbInvariants(targetTask);

  expect(
    Number(
      await scalar<string>("select count(*)::text from model_backtest_run where target_task = $1", [targetTask]),
    ),
  ).toBeGreaterThan(0);
  expect(
    Number(
      await scalar<string>("select count(*)::text from model_opportunity where target_task = $1", [targetTask]),
    ),
  ).toBeGreaterThan(0);
});
