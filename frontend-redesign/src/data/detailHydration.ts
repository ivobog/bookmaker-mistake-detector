import { fetchJson } from "./http";
import type { CandidateCard, DetailFact, SignalCard, WorkspaceDefaults } from "../types";
import { getModelFamilyLabel, getSelectedFeatureLabel } from "../../../shared/frontend/domain";
import { formatStoredRationaleValue } from "../../../shared/frontend/detailFormatting";
import { applyScenarioQuery, buildTrainingQuery as buildSharedTrainingQuery } from "../../../shared/frontend/query";
import type {
  SharedModelRunDetailResponse,
  SharedOpportunityDetailResponse,
  SharedScoringRunDetailResponse,
  SharedSelectionDetailResponse,
} from "../../../shared/frontend/apiTypes";

type DetailPanelContent = {
  title: string;
  subtitle: string;
  facts: DetailFact[];
  evidence: string[];
  nextActions: string[];
};

function formatNumber(value: unknown): string {
  return typeof value === "number" ? value.toFixed(2) : "n/a";
}

function readNested(record: Record<string, unknown> | null | undefined, ...keys: string[]): unknown {
  let current: unknown = record;
  for (const key of keys) {
    if (!current || typeof current !== "object" || !(key in current)) {
      return null;
    }
    current = (current as Record<string, unknown>)[key];
  }
  return current;
}

function asScenario(payload: Record<string, unknown>): Record<string, unknown> | null {
  const scenario = readNested(payload, "scenario");
  return scenario && typeof scenario === "object" ? (scenario as Record<string, unknown>) : null;
}

function buildTrainingQuery(defaults: WorkspaceDefaults, targetTask: string): URLSearchParams {
  return buildSharedTrainingQuery({
    featureKey: defaults.featureKey,
    targetTask,
    trainRatio: defaults.trainRatio,
    validationRatio: defaults.validationRatio
  });
}

export async function hydrateCandidateDetail(
  candidate: CandidateCard,
  defaults: WorkspaceDefaults
): Promise<DetailPanelContent | null> {
  if (!candidate.runId) {
    return null;
  }

  const targetTask = candidate.targetTaskKey ?? defaults.targetTask ?? "spread_error_regression";
  const query = buildTrainingQuery(defaults, targetTask);
  const response = await fetchJson<SharedModelRunDetailResponse>(`/api/v1/admin/models/runs/${candidate.runId}`, query);
  const run = response.model_run;
  if (!run) {
    return null;
  }

  return {
    title: `Run #${run.id}`,
    subtitle: `${run.status} | ${run.target_task}`,
    facts: [
      { label: "Model family", value: getModelFamilyLabel(run, candidate.modelFamily) },
      { label: "Selected feature", value: getSelectedFeatureLabel(run) },
      { label: "Train ratio", value: formatNumber(run.train_ratio) },
      { label: "Validation ratio", value: formatNumber(run.validation_ratio) }
    ],
    evidence: [
      `Validation metric: ${formatNumber(readNested(run.metrics, "validation", "mae") ?? readNested(run.metrics, "validation_metric_value"))}.`,
      `Test metric: ${formatNumber(readNested(run.metrics, "test", "mae") ?? readNested(run.metrics, "test_metric_value"))}.`,
      `Created at: ${run.created_at ?? "unknown"}, completed at: ${run.completed_at ?? "unknown"}.`
    ],
    nextActions: ["Compare against the active selection", "Review validation notes", "Move to Model Decision if it remains strongest"]
  };
}

export async function hydrateDecisionDetail(
  candidate: CandidateCard,
  defaults: WorkspaceDefaults
): Promise<DetailPanelContent | null> {
  if (candidate.selectionId) {
    const targetTask = candidate.targetTaskKey ?? defaults.targetTask ?? "spread_error_regression";
    const query = buildTrainingQuery(defaults, targetTask);
    const response = await fetchJson<SharedSelectionDetailResponse>(
      `/api/v1/admin/models/selections/${candidate.selectionId}`,
      query
    );
    const selection = response.selection;
    if (!selection) {
      return null;
    }
    return {
      title: `Selection #${selection.id}`,
      subtitle: `${selection.is_active ? "active" : "inactive"} | ${selection.target_task}`,
      facts: [
        { label: "Model family", value: selection.model_family },
        { label: "Policy", value: selection.selection_policy_name },
        { label: "Training run", value: selection.model_training_run_id ? `#${selection.model_training_run_id}` : "n/a" },
        { label: "Evaluation", value: selection.model_evaluation_snapshot_id ? `#${selection.model_evaluation_snapshot_id}` : "n/a" }
      ],
      evidence: [
        formatStoredRationaleValue(selection.rationale, "No rationale captured."),
        `Selection created at ${selection.created_at ?? "unknown"}.`,
        "This release detail was hydrated from the live selection endpoint."
      ],
      nextActions: ["Confirm release note", "Run today's slate", "Compare this release against the freshest training run"]
    };
  }

  return hydrateCandidateDetail(candidate, defaults);
}

export async function hydrateSignalDetail(
  signal: SignalCard,
  defaults: WorkspaceDefaults
): Promise<DetailPanelContent | null> {
  if (!signal.opportunityId) {
    return null;
  }

  const targetTask = signal.targetTaskKey ?? defaults.targetTask ?? "spread_error_regression";
  const query = buildTrainingQuery(defaults, targetTask);
  const opportunityResponse = await fetchJson<SharedOpportunityDetailResponse>(
    `/api/v1/analyst/opportunities/${signal.opportunityId}`,
    query
  );
  const opportunity = opportunityResponse.opportunity;
  if (!opportunity) {
    return null;
  }

  const facts: DetailFact[] = [
    { label: "Opportunity", value: `#${opportunity.id}` },
    { label: "Task", value: opportunity.target_task },
    { label: "Evidence", value: opportunity.evidence_rating ?? "unknown" },
    { label: "Queue status", value: opportunity.recommendation_status ?? "Needs review" }
  ];

  const evidence = [
    `Opportunity created at ${opportunity.created_at ?? "unknown"}.`,
    `Analyst status is ${opportunity.recommendation_status ?? "Needs review"}.`,
    "This detail was hydrated from the live analyst opportunities endpoint."
  ];

  if (opportunity.model_scoring_run_id) {
    const scenario = asScenario(opportunity.payload);
    if (scenario) {
      const scoringQuery = buildTrainingQuery(defaults, targetTask);
      applyScenarioQuery(
        scoringQuery,
        {
          seasonLabel: readNested(scenario, "season_label") as string | number | null | undefined,
          gameDate: readNested(scenario, "game_date") as string | number | null | undefined,
          homeTeamCode: readNested(scenario, "home_team_code") as string | number | null | undefined,
          awayTeamCode: readNested(scenario, "away_team_code") as string | number | null | undefined
        },
        {
          seasonLabel: defaults.seasonLabel,
          gameDate: "2026-04-20",
          homeTeamCode: "HOME",
          awayTeamCode: "AWAY"
        }
      );
      const scoringResponse = await fetchJson<SharedScoringRunDetailResponse>(
        `/api/v1/admin/models/future-game-preview/runs/${opportunity.model_scoring_run_id}`,
        scoringQuery
      );
      const scoringRun = scoringResponse.scoring_run;
      if (scoringRun) {
        facts.push(
          { label: "Scoring run", value: `#${scoringRun.id}` },
          { label: "Scenario", value: scoringRun.scenario_key }
        );
        evidence.push(
          `Prediction count: ${scoringRun.prediction_count}.`,
          `Candidate opportunities: ${scoringRun.candidate_opportunity_count}, review opportunities: ${scoringRun.review_opportunity_count}.`,
          `Scenario teams: ${scoringRun.home_team_code} vs ${scoringRun.away_team_code} on ${scoringRun.game_date}.`
        );
      }
    }
  }

  return {
    title: signal.game,
    subtitle: `${signal.id} | ${signal.status}`,
    facts,
    evidence,
    nextActions: ["Inspect comparables in the legacy app if needed", "Decide queue disposition", "Save an analyst note if escalated"]
  };
}
