export type FrontendAppMode = "operator" | "demo";

type NumericString = `${number}`;

function parseOptionalInteger(value: string | undefined): string | null {
  if (!value) {
    return null;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isNaN(parsed) ? null : String(parsed);
}

function parseMode(value: string | undefined): FrontendAppMode {
  return value === "demo" ? "demo" : "operator";
}

export const frontendAppMode = parseMode(import.meta.env.VITE_APP_MODE);

export const sharedTrainingDefaults = {
  targetTask: import.meta.env.VITE_DEFAULT_TARGET_TASK ?? "spread_error_regression",
  trainRatio: (import.meta.env.VITE_DEFAULT_TRAIN_RATIO ?? "0.7") as NumericString,
  validationRatio: (import.meta.env.VITE_DEFAULT_VALIDATION_RATIO ?? "0.15") as NumericString
};

export const sharedBacktestDefaults = {
  minimumTrainGames: (import.meta.env.VITE_DEFAULT_MINIMUM_TRAIN_GAMES ?? "1") as NumericString,
  testWindowGames: (import.meta.env.VITE_DEFAULT_TEST_WINDOW_GAMES ?? "1") as NumericString
};

const demoDefaults =
  frontendAppMode === "demo"
    ? {
        teamCode: import.meta.env.VITE_DEMO_TEAM_CODE ?? null,
        seasonLabel: import.meta.env.VITE_DEMO_SEASON_LABEL ?? null,
        canonicalGameId: parseOptionalInteger(import.meta.env.VITE_DEMO_CANONICAL_GAME_ID),
        homeTeamCode: import.meta.env.VITE_DEMO_HOME_TEAM_CODE ?? null,
        awayTeamCode: import.meta.env.VITE_DEMO_AWAY_TEAM_CODE ?? null,
        gameDate: import.meta.env.VITE_DEMO_GAME_DATE ?? null
      }
    : {
        teamCode: null,
        seasonLabel: null,
        canonicalGameId: null,
        homeTeamCode: null,
        awayTeamCode: null,
        gameDate: null
      };

export function appendDemoScope(query: URLSearchParams): URLSearchParams {
  if (frontendAppMode !== "demo") {
    return query;
  }
  if (demoDefaults.teamCode) {
    query.set("team_code", demoDefaults.teamCode);
  }
  if (demoDefaults.seasonLabel) {
    query.set("season_label", demoDefaults.seasonLabel);
  }
  if (demoDefaults.canonicalGameId) {
    query.set("canonical_game_id", demoDefaults.canonicalGameId);
  }
  return query;
}

export function buildSharedTrainingQuery(): URLSearchParams {
  return new URLSearchParams({
    target_task: sharedTrainingDefaults.targetTask,
    train_ratio: sharedTrainingDefaults.trainRatio,
    validation_ratio: sharedTrainingDefaults.validationRatio
  });
}

export function buildBacktestQuery(): URLSearchParams {
  const query = buildSharedTrainingQuery();
  query.set("minimum_train_games", sharedBacktestDefaults.minimumTrainGames);
  query.set("test_window_games", sharedBacktestDefaults.testWindowGames);
  return appendDemoScope(query);
}

export function buildOpportunityQuery(options?: {
  includeLimit?: boolean;
  recentLimit?: number;
}): URLSearchParams {
  const query = appendDemoScope(buildSharedTrainingQuery());
  if (options?.includeLimit) {
    query.set("limit", "25");
  }
  if (options?.recentLimit !== undefined) {
    query.set("recent_limit", String(options.recentLimit));
  }
  return query;
}

export function buildModelArtifactQuery(recentLimit?: number): URLSearchParams {
  const query = buildSharedTrainingQuery();
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
  return {
    seasonLabel: demoDefaults.seasonLabel,
    gameDate: demoDefaults.gameDate,
    homeTeamCode: demoDefaults.homeTeamCode,
    awayTeamCode: demoDefaults.awayTeamCode
  };
}
