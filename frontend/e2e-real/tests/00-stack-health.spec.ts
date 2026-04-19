import { test, expect } from "../fixtures/appHarness";
import { scalar } from "../helpers/db";
import { collectPageErrors, expectNoFatalUiErrors, openRoute } from "../helpers/ui";

test("frontend, backend, and seeded Postgres state are all reachable", async ({ page }) => {
  const pageErrors = collectPageErrors(page);

  await openRoute(page, "#/backtests");
  await expect(page.getByTestId("backtests-page")).toBeVisible();
  await expect(page.getByText("Backtest runs")).toBeVisible();

  await openRoute(page, "#/opportunities");
  await expect(page.getByTestId("opportunities-page")).toBeVisible();
  await expect(page.getByTestId("opportunities-queue-scope-label")).toBeVisible();

  await openRoute(page, "#/models");
  await expect(page.getByRole("heading", { name: "Recent training activity" })).toBeVisible();

  expect(Number(await scalar<string>("select count(*)::text from canonical_game"))).toBeGreaterThan(0);
  expect(Number(await scalar<string>("select count(*)::text from feature_version"))).toBeGreaterThan(0);

  await expectNoFatalUiErrors(page, pageErrors);
});
