export type WorkflowFreshness = "ready" | "review" | "stale" | "missing";

type ModelArtifactLike = {
  artifact?: Record<string, unknown> | null;
  completed_at?: string | null;
  created_at?: string | null;
  id?: number | null;
  metrics?: Record<string, unknown> | null;
  model_family?: string | null;
  season_label?: string | null;
  selected_feature?: string | null;
  target_task?: string | null;
  team_code?: string | null;
  validation_metric_value?: number | null;
};

type OpportunityLike = {
  created_at?: string | null;
  evidence_rating?: string | null;
  id?: number | null;
  model_scoring_run_id?: number | null;
  opponent_code?: string | null;
  recommendation_status?: string | null;
  source_kind?: string | null;
  status?: string | null;
  target_task?: string | null;
  team_code?: string | null;
  updated_at?: string | null;
};

function readArtifactValue(source: ModelArtifactLike, key: string): string | null {
  const artifactValue = source.artifact?.[key];
  return typeof artifactValue === "string" ? artifactValue : null;
}

export function humanizeToken(value: string | null | undefined, fallback = "n/a"): string {
  if (!value) {
    return fallback;
  }
  return value.replace(/_/g, " ");
}

export function getModelFamilyLabel(source: ModelArtifactLike, fallback = "n/a"): string {
  return source.model_family ?? readArtifactValue(source, "model_family") ?? fallback;
}

export function getSelectedFeatureLabel(source: ModelArtifactLike, fallback = "n/a"): string {
  return source.selected_feature ?? readArtifactValue(source, "selected_feature") ?? fallback;
}

export function getModelMetricValue(source: ModelArtifactLike): number | null {
  if (typeof source.validation_metric_value === "number") {
    return source.validation_metric_value;
  }

  const metrics = source.metrics ?? {};
  const metricKeys = ["validation_metric_value", "mae", "rmse", "selection_score"];
  for (const key of metricKeys) {
    const value = metrics[key];
    if (typeof value === "number") {
      return value;
    }
  }

  return null;
}

export function getModelMetricDisplay(source: ModelArtifactLike, digits = 2, fallback = "Pending"): string {
  const value = getModelMetricValue(source);
  return value === null ? fallback : value.toFixed(digits);
}

export function getModelRunLabel(runId: number | null | undefined, prefix = "Run"): string {
  return runId ? `${prefix} #${runId}` : "n/a";
}

export function getModelScopeLabel(source: ModelArtifactLike, fallback = "global"): string {
  return source.team_code ?? source.season_label ?? fallback;
}

export function minutesSince(timestamp: string | null | undefined): number | null {
  if (!timestamp) {
    return null;
  }
  const parsed = Date.parse(timestamp);
  if (Number.isNaN(parsed)) {
    return null;
  }
  return Math.round((Date.now() - parsed) / 60000);
}

export function formatRelativeTime(timestamp: string | null | undefined): string {
  const minutes = minutesSince(timestamp);
  if (minutes === null) {
    return "Unknown";
  }
  if (minutes < 60) {
    return `${minutes} min ago`;
  }
  if (minutes < 1440) {
    return `${Math.round(minutes / 60)} hr ago`;
  }
  return `${Math.round(minutes / 1440)} d ago`;
}

export function getWorkflowFreshnessStatus(timestamp: string | null | undefined): WorkflowFreshness {
  const minutes = minutesSince(timestamp);
  if (minutes === null) {
    return "missing";
  }
  if (minutes <= 180) {
    return "ready";
  }
  if (minutes <= 1440) {
    return "review";
  }
  return "stale";
}

export function getOpportunityMatchupLabel(opportunity: OpportunityLike, fallback = "Unknown matchup"): string {
  if (!opportunity.team_code || !opportunity.opponent_code) {
    return fallback;
  }
  return `${opportunity.team_code} vs ${opportunity.opponent_code}`;
}

export function getOpportunityMarketLabel(targetTask: string | null | undefined): string {
  return targetTask?.includes("total") ? "Total" : "Spread";
}

export function getEvidenceLabel(evidenceRating: string | null | undefined, fallback = "Unknown"): string {
  return humanizeToken(evidenceRating, fallback);
}

export function getRecommendationLabel(
  recommendationStatus: string | null | undefined,
  fallback = "Needs review"
): string {
  return humanizeToken(recommendationStatus, fallback);
}

export function getSourceKindLabel(sourceKind: string | null | undefined, fallback = "n/a"): string {
  return humanizeToken(sourceKind, fallback);
}

export function getSignalStrengthLabel(evidenceRating: string | null | undefined): string {
  if (evidenceRating === "strong") {
    return "High";
  }
  if (evidenceRating === "weak") {
    return "Low";
  }
  return "Medium";
}
