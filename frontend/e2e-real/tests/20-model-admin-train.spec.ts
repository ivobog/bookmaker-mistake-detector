import { test, expect } from "../fixtures/appHarness";
import { getEvaluationForRun, getTrainingRunById, scalar } from "../helpers/db";
import { collectPageErrors, expectNoFatalUiErrors, openRoute, readHeadingNumber } from "../helpers/ui";

test("training from the GUI creates persisted runs and evaluation snapshots", async ({
  page,
  seasonLabel,
  targetTask,
  teamCode
}) => {
  const pageErrors = collectPageErrors(page);
  const beforeRunCount = Number(
    await scalar<string>("select count(*)::text from model_training_run where target_task = $1", [targetTask]),
  );
  const beforeEvaluationCount = Number(
    await scalar<string>("select count(*)::text from model_evaluation_snapshot where target_task = $1", [
      targetTask
    ]),
  );

  await openRoute(page, "#/models");
  await page.getByTestId("model-admin-train-action").click();
  await page.getByTestId("train-target-task").fill(targetTask);
  await page.getByTestId("train-team-code").fill(teamCode);
  await page.getByTestId("train-season-label").fill(seasonLabel);
  await page.getByTestId("train-submit").click();

  const runId = await readHeadingNumber(page, /Run #\d+/);
  const runRecord = await getTrainingRunById(runId);
  expect(Number(runRecord?.id ?? 0)).toBe(runId);

  const evaluationRecord = await getEvaluationForRun(runId);
  expect(evaluationRecord).not.toBeNull();

  const afterRunCount = Number(
    await scalar<string>("select count(*)::text from model_training_run where target_task = $1", [targetTask]),
  );
  const afterEvaluationCount = Number(
    await scalar<string>("select count(*)::text from model_evaluation_snapshot where target_task = $1", [
      targetTask
    ]),
  );

  expect(afterRunCount).toBeGreaterThan(beforeRunCount);
  expect(afterEvaluationCount).toBeGreaterThan(beforeEvaluationCount);

  await expect(page.getByTestId("run-detail-card")).toContainText(`Run #${runId}`);
  await expect(page.getByTestId("run-detail-card")).toContainText(String(runRecord?.status ?? ""));
  if (runRecord?.team_code) {
    await expect(page.getByTestId("run-detail-card")).toContainText(String(runRecord.team_code));
  }
  if (runRecord?.season_label) {
    await expect(page.getByTestId("run-detail-card")).toContainText(String(runRecord.season_label));
  }

  const artifact = (runRecord?.artifact ?? {}) as Record<string, unknown>;
  if (artifact.selected_feature) {
    await expect(page.getByTestId("run-detail-card")).toContainText(String(artifact.selected_feature));
  }
  await expect(page.getByTestId("run-detail-card")).toContainText(
    String((evaluationRecord?.validation_prediction_count as number | undefined) ?? ""),
  );

  await openRoute(page, "#/models/evaluations");
  await expect(page.getByTestId("evaluations-table")).toContainText(`#${evaluationRecord?.id as number}`);

  await expectNoFatalUiErrors(page, pageErrors);
});
