import { expect, test as base } from "@playwright/test";

import { assertDbInvariants } from "../helpers/db";
import {
  resetTestState,
  seedE2eDataset,
  waitForBackendHealth
} from "../helpers/api";

type Harness = {
  featureKey: string;
  seasonLabel: string;
  targetTask: string;
  teamCode: string;
};

export const test = base.extend<Harness>({
  featureKey: async (_args, use) => use(process.env.E2E_FEATURE_KEY ?? "baseline_team_features_v1"),
  seasonLabel: async (_args, use) => use(process.env.E2E_SEASON_LABEL ?? "2024-2025"),
  targetTask: async (_args, use) => use(process.env.E2E_TARGET_TASK ?? "spread_error_regression"),
  teamCode: async (_args, use) => use(process.env.E2E_TEAM_CODE ?? "LAL")
});

test.beforeEach(async ({ request }) => {
  await waitForBackendHealth(request);
  await resetTestState(request);
  await seedE2eDataset(request);
});

test.afterEach(async ({ request, targetTask }) => {
  await waitForBackendHealth(request);
  await assertDbInvariants(targetTask);
});

export { expect };
