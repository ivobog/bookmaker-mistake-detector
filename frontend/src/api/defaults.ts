import type { ModelAdminCapabilitiesResponse } from "../modelAdminTypes";
import { apiGet } from "./client";

type NumericString = `${number}`;

const configuredDefaultTargetTask = import.meta.env.VITE_DEFAULT_TARGET_TASK?.trim() || null;
const lastResortTargetTask = "spread_error_regression";
let cachedDefaultTargetTask: string | null = configuredDefaultTargetTask;
let defaultTargetTaskPromise: Promise<string> | null = null;

export const sharedTrainingDefaults = {
  trainRatio: (import.meta.env.VITE_DEFAULT_TRAIN_RATIO ?? "0.7") as NumericString,
  validationRatio: (import.meta.env.VITE_DEFAULT_VALIDATION_RATIO ?? "0.15") as NumericString
};

export const sharedBacktestDefaults = {
  minimumTrainGames: (import.meta.env.VITE_DEFAULT_MINIMUM_TRAIN_GAMES ?? "1") as NumericString,
  testWindowGames: (import.meta.env.VITE_DEFAULT_TEST_WINDOW_GAMES ?? "1") as NumericString
};

const scenarioDefaults = {
  seasonLabel: import.meta.env.VITE_DEFAULT_SEASON_LABEL ?? null,
  gameDate: import.meta.env.VITE_DEFAULT_GAME_DATE ?? null,
  homeTeamCode: import.meta.env.VITE_DEFAULT_HOME_TEAM_CODE ?? null,
  awayTeamCode: import.meta.env.VITE_DEFAULT_AWAY_TEAM_CODE ?? null
};

export async function resolveDefaultTargetTask(): Promise<string> {
  if (cachedDefaultTargetTask) {
    return cachedDefaultTargetTask;
  }
  if (!defaultTargetTaskPromise) {
    defaultTargetTaskPromise = apiGet<ModelAdminCapabilitiesResponse>("/api/v1/admin/model-capabilities", {
      errorPrefix: "Failed to load model capabilities"
    })
      .then((response) => {
        const resolvedTargetTask =
          response.ui_defaults.default_target_task || response.target_tasks[0]?.task_key || lastResortTargetTask;
        cachedDefaultTargetTask = resolvedTargetTask;
        return resolvedTargetTask;
      })
      .catch(() => lastResortTargetTask)
      .finally(() => {
        defaultTargetTaskPromise = null;
      });
  }
  return defaultTargetTaskPromise;
}

export async function buildSharedTrainingQuery(): Promise<URLSearchParams> {
  const targetTask = await resolveDefaultTargetTask();
  return new URLSearchParams({
    target_task: targetTask,
    train_ratio: sharedTrainingDefaults.trainRatio,
    validation_ratio: sharedTrainingDefaults.validationRatio
  });
}

export async function buildBacktestQuery(): Promise<URLSearchParams> {
  const query = await buildSharedTrainingQuery();
  query.set("minimum_train_games", sharedBacktestDefaults.minimumTrainGames);
  query.set("test_window_games", sharedBacktestDefaults.testWindowGames);
  return query;
}

export async function buildOpportunityQuery(options?: {
  includeLimit?: boolean;
  recentLimit?: number;
}): Promise<URLSearchParams> {
  const query = await buildSharedTrainingQuery();
  if (options?.includeLimit) {
    query.set("limit", "25");
  }
  if (options?.recentLimit !== undefined) {
    query.set("recent_limit", String(options.recentLimit));
  }
  return query;
}

export async function buildModelArtifactQuery(recentLimit?: number): Promise<URLSearchParams> {
  const query = await buildSharedTrainingQuery();
  if (recentLimit !== undefined) {
    query.set("recent_limit", String(recentLimit));
  }
  return query;
}

export function resolveScenarioDefaults(): {
  seasonLabel: string | null;
  gameDate: string | null;
  homeTeamCode: string | null;
  awayTeamCode: string | null;
} {
  return scenarioDefaults;
}
