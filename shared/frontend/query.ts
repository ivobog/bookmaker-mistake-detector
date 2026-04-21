type QueryScalar = string | number | null | undefined;

type TrainingQueryInput = {
  featureKey?: QueryScalar;
  targetTask?: QueryScalar;
  trainRatio: string | number;
  validationRatio: string | number;
};

type BacktestQueryInput = TrainingQueryInput & {
  minimumTrainGames: string | number;
  testWindowGames: string | number;
};

type OpportunityQueryOptions = {
  limit?: string | number | null;
  recentLimit?: number;
};

type SlateQueryInput = TrainingQueryInput & {
  seasonLabel?: QueryScalar;
  sourceName?: QueryScalar;
  includeEvidence?: boolean;
  pendingOnly?: boolean;
  freshnessStatus?: QueryScalar;
};

type ScenarioOverlayInput = {
  targetTask?: QueryScalar;
  seasonLabel?: QueryScalar;
  gameDate?: QueryScalar;
  homeTeamCode?: QueryScalar;
  awayTeamCode?: QueryScalar;
  homeSpreadLine?: QueryScalar;
  totalLine?: QueryScalar;
};

function setIfPresent(query: URLSearchParams, key: string, value: QueryScalar): void {
  if (value === null || value === undefined) {
    return;
  }
  const normalized = String(value).trim();
  if (!normalized) {
    return;
  }
  query.set(key, normalized);
}

export function buildTrainingQuery(input: TrainingQueryInput): URLSearchParams {
  const query = new URLSearchParams({
    train_ratio: String(input.trainRatio),
    validation_ratio: String(input.validationRatio)
  });
  setIfPresent(query, "feature_key", input.featureKey);
  setIfPresent(query, "target_task", input.targetTask);
  return query;
}

export function buildBacktestQuery(input: BacktestQueryInput): URLSearchParams {
  const query = buildTrainingQuery(input);
  query.set("minimum_train_games", String(input.minimumTrainGames));
  query.set("test_window_games", String(input.testWindowGames));
  return query;
}

export function buildOpportunityQuery(
  input: TrainingQueryInput,
  options?: OpportunityQueryOptions
): URLSearchParams {
  const query = buildTrainingQuery(input);
  if (options?.limit !== undefined && options.limit !== null) {
    query.set("limit", String(options.limit));
  }
  if (options?.recentLimit !== undefined) {
    query.set("recent_limit", String(options.recentLimit));
  }
  return query;
}

export function buildModelArtifactQuery(input: TrainingQueryInput, recentLimit?: number): URLSearchParams {
  const query = buildTrainingQuery(input);
  if (recentLimit !== undefined) {
    query.set("recent_limit", String(recentLimit));
  }
  return query;
}

export function buildSlateQuery(input: SlateQueryInput): URLSearchParams {
  const query = buildTrainingQuery(input);
  setIfPresent(query, "season_label", input.seasonLabel);
  setIfPresent(query, "source_name", input.sourceName);
  if (input.includeEvidence) {
    query.set("include_evidence", "true");
  }
  if (input.pendingOnly) {
    query.set("pending_only", "true");
  }
  setIfPresent(query, "freshness_status", input.freshnessStatus);
  return query;
}

export function applyScenarioQuery(
  query: URLSearchParams,
  scenario: ScenarioOverlayInput,
  fallbacks?: ScenarioOverlayInput
): URLSearchParams {
  setIfPresent(query, "target_task", scenario.targetTask ?? fallbacks?.targetTask);
  setIfPresent(query, "season_label", scenario.seasonLabel ?? fallbacks?.seasonLabel);
  setIfPresent(query, "game_date", scenario.gameDate ?? fallbacks?.gameDate);
  setIfPresent(query, "home_team_code", scenario.homeTeamCode ?? fallbacks?.homeTeamCode);
  setIfPresent(query, "away_team_code", scenario.awayTeamCode ?? fallbacks?.awayTeamCode);
  setIfPresent(query, "home_spread_line", scenario.homeSpreadLine ?? fallbacks?.homeSpreadLine);
  setIfPresent(query, "total_line", scenario.totalLine ?? fallbacks?.totalLine);
  return query;
}
