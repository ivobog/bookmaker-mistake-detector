import { test, expect } from "../fixtures/appHarness";
import { trainModel } from "../helpers/api";
import { getLatestActiveSelection, scalar } from "../helpers/db";
import { collectPageErrors, expectNoFatalUiErrors, openRoute, readHeadingNumber } from "../helpers/ui";

test("selection from the GUI creates a single active persisted selection", async ({
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

  await openRoute(page, "#/models/selections");
  await page.getByTestId("model-admin-select-action").click();
  await page.getByTestId("select-target-task").fill(targetTask);
  await page.getByTestId("select-submit").click();

  const selectionId = await readHeadingNumber(page, /Selection #\d+/);
  const activeSelection = await getLatestActiveSelection(targetTask);
  expect(Number(activeSelection?.id ?? 0)).toBe(selectionId);
  expect(activeSelection?.is_active).toBe(true);

  expect(
    Number(
      await scalar<string>(
        "select count(*)::text from model_selection_snapshot where target_task = $1 and is_active = true",
        [targetTask],
      ),
    ),
  ).toBe(1);

  await expect(page.getByTestId("selection-detail-card")).toContainText(`Selection #${selectionId}`);
  await expect(page.getByTestId("selection-detail-card")).toContainText(
    String(activeSelection?.selection_policy_name ?? ""),
  );
  await expect(page.getByTestId("selection-detail-card")).toContainText(
    String(activeSelection?.model_family ?? ""),
  );
  await expect(page.getByTestId("selections-table")).toContainText(`#${selectionId}`);

  await expectNoFatalUiErrors(page, pageErrors);
});
