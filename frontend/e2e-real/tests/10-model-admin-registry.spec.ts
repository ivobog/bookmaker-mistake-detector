import { test, expect } from "../fixtures/appHarness";
import { trainModel } from "../helpers/api";
import { rows, scalar } from "../helpers/db";
import { collectPageErrors, expectNoFatalUiErrors, openRoute } from "../helpers/ui";

test("registry rows match Postgres and target-task filtering stays consistent", async ({
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

  await openRoute(page, "#/models");
  await page.getByTestId("model-admin-registry-tab").click();

  await expect(page.getByTestId("registry-table")).toBeVisible();
  const registryRows = await rows<{ id: number; model_family: string; version_label: string }>(
    `
      SELECT id, model_family, version_label
      FROM model_registry
      WHERE target_task = $1
      ORDER BY id ASC
    `,
    [targetTask],
  );
  expect(await page.locator("[data-testid^='registry-row-']").count()).toBe(registryRows.length);

  const firstRegistry = registryRows[0];
  expect(firstRegistry).toBeDefined();
  await page.getByTestId(`registry-row-${firstRegistry.id}`).click();
  await expect(page.getByTestId("registry-detail-card")).toContainText(firstRegistry.model_family);
  await expect(page.getByTestId("registry-detail-card")).toContainText(firstRegistry.version_label);

  const missingTask = "missing_registry_task";
  await page.getByLabel("Target task").fill(missingTask);
  await page.getByRole("button", { name: "Apply filters" }).click();
  await expect(page.getByTestId("registry-table")).toContainText(
    "No model registry entries were found for the current scope.",
  );
  expect(
    Number(
      await scalar<string>("select count(*)::text from model_registry where target_task = $1", [missingTask]),
    ),
  ).toBe(0);

  await expectNoFatalUiErrors(page, pageErrors);
});
