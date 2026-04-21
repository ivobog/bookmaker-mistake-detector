import { postJson } from "./http";
import type { WorkspaceDefaults } from "../types";
import { buildSlateQuery, buildTrainingQuery } from "../../../shared/frontend/query";

type FeatureMaterializeResponse = {
  snapshots_saved: number;
  canonical_game_count: number;
};

type TrainResponse = {
  persisted_run_count?: number;
  model_runs: Array<{ id: number }>;
  best_model: { id: number } | null;
};

type SelectResponse = {
  selection_policy_name: string;
  active_selection: { id: number } | null;
};

type SlateRunResponse = {
  candidate_board_count: number;
  scored_board_count: number;
  materialized_scoring_run_count: number;
  materialized_opportunity_count: number;
};

export type WorkflowMutationResult = {
  message: string;
  routeHint?: "training-lab" | "model-decision" | "slate-runner" | "signals-desk";
};

function buildDefaultsQuery(defaults: WorkspaceDefaults, targetTask: string): URLSearchParams {
  return buildTrainingQuery({
    featureKey: defaults.featureKey,
    targetTask,
    trainRatio: defaults.trainRatio,
    validationRatio: defaults.validationRatio
  });
}

export async function refreshFeatures(defaults: WorkspaceDefaults): Promise<WorkflowMutationResult> {
  const query = new URLSearchParams({
    feature_key: defaults.featureKey
  });
  const result = await postJson<FeatureMaterializeResponse>("/api/v1/admin/features/snapshots/materialize", query);
  return {
    message: `Feature refresh saved ${result.snapshots_saved} snapshots across ${result.canonical_game_count} canonical games.`,
    routeHint: "training-lab"
  };
}

export async function trainRun(defaults: WorkspaceDefaults, targetTask: string): Promise<WorkflowMutationResult> {
  const query = buildDefaultsQuery(defaults, targetTask);
  const result = await postJson<TrainResponse>("/api/v1/admin/models/train", query);
  const runCount = result.persisted_run_count ?? result.model_runs.length;
  const bestRunId = result.best_model?.id ?? result.model_runs[0]?.id ?? null;
  return {
    message:
      bestRunId !== null
        ? `Training completed with ${runCount} run(s). Best candidate is run #${bestRunId}.`
        : `Training completed with ${runCount} run(s).`,
    routeHint: "training-lab"
  };
}

export async function activateModel(
  defaults: WorkspaceDefaults,
  targetTask: string,
  selectionPolicyName: string
): Promise<WorkflowMutationResult> {
  const query = buildDefaultsQuery(defaults, targetTask);
  query.set("selection_policy_name", selectionPolicyName);
  const result = await postJson<SelectResponse>("/api/v1/admin/models/select", query);
  return {
    message:
      result.active_selection?.id !== null && result.active_selection?.id !== undefined
        ? `Activation completed using ${result.selection_policy_name}. Selection #${result.active_selection.id} is now active.`
        : `Activation completed using ${result.selection_policy_name}.`,
    routeHint: "model-decision"
  };
}

export async function runSlate(defaults: WorkspaceDefaults, targetTask: string): Promise<WorkflowMutationResult> {
  const query = buildSlateQuery({
    featureKey: defaults.featureKey,
    targetTask,
    trainRatio: defaults.trainRatio,
    validationRatio: defaults.validationRatio,
    seasonLabel: defaults.seasonLabel,
    sourceName: defaults.sourceName,
    includeEvidence: true,
    pendingOnly: true,
    freshnessStatus: "fresh"
  });
  const result = await postJson<SlateRunResponse>("/api/v1/admin/models/market-board/orchestrate-score", query);
  return {
    message: `Slate run scored ${result.scored_board_count} board(s), saved ${result.materialized_scoring_run_count} scoring run(s), and produced ${result.materialized_opportunity_count} opportunities.`,
    routeHint: result.materialized_opportunity_count > 0 ? "signals-desk" : "slate-runner"
  };
}
