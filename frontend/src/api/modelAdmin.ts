import type {
  ModelAdminCapabilitiesResponse,
  ModelAdminEvaluationDetailResponse,
  ModelAdminEvaluationFilters,
  ModelAdminEvaluationHistoryResponse,
  ModelAdminEvaluationsResponse,
  ModelAdminHistoryResponse,
  ModelAdminQueryOptions,
  ModelAdminRegistryFilters,
  ModelAdminRegistryResponse,
  ModelAdminRunDetailResponse,
  ModelAdminRunsResponse,
  ModelAdminSelectionDetailResponse,
  ModelAdminSelectionFilters,
  ModelAdminSelectionHistoryResponse,
  ModelAdminSelectionMutationInput,
  ModelAdminSelectionsResponse,
  ModelAdminSelectResponse,
  ModelAdminSummaryResponse,
  ModelAdminTrainingMutationInput,
  ModelAdminTrainResponse
} from "../modelAdminTypes";
import { apiGet, apiPost } from "./client";
import { buildSharedTrainingQuery } from "./defaults";

export async function fetchModelCapabilities(): Promise<ModelAdminCapabilitiesResponse> {
  return apiGet<ModelAdminCapabilitiesResponse>("/api/v1/admin/model-capabilities", {
    errorPrefix: "Failed to load model capabilities"
  });
}

async function buildModelAdminQuery(options?: ModelAdminQueryOptions): Promise<URLSearchParams> {
  const query = await buildSharedTrainingQuery();
  if (options?.featureKey) {
    query.set("feature_key", options.featureKey);
  }
  if (options?.targetTask !== undefined && options.targetTask !== null) {
    query.set("target_task", options.targetTask);
  }
  if (options?.teamCode) {
    query.set("team_code", options.teamCode);
  }
  if (options?.seasonLabel) {
    query.set("season_label", options.seasonLabel);
  }
  if (options?.trainRatio !== undefined) {
    query.set("train_ratio", String(options.trainRatio));
  }
  if (options?.validationRatio !== undefined) {
    query.set("validation_ratio", String(options.validationRatio));
  }
  if (options?.recentLimit !== undefined) {
    query.set("recent_limit", String(options.recentLimit));
  }
  return query;
}

export async function fetchModelAdminRegistry(
  options?: ModelAdminRegistryFilters
): Promise<ModelAdminRegistryResponse> {
  const query = await buildModelAdminQuery(options);
  return apiGet<ModelAdminRegistryResponse>("/api/v1/admin/models/registry", {
    errorPrefix: "Failed to load model registry",
    query
  });
}

export async function fetchModelAdminRuns(
  options?: ModelAdminQueryOptions
): Promise<ModelAdminRunsResponse> {
  const query = await buildModelAdminQuery(options);
  return apiGet<ModelAdminRunsResponse>("/api/v1/admin/models/runs", {
    errorPrefix: "Failed to load model runs",
    query
  });
}

export async function fetchModelAdminRunDetail(runId: number): Promise<ModelAdminRunDetailResponse> {
  return apiGet<ModelAdminRunDetailResponse>(`/api/v1/admin/models/runs/${runId}`, {
    errorPrefix: "Failed to load model run detail"
  });
}

export async function fetchModelAdminSummary(
  options?: ModelAdminQueryOptions
): Promise<ModelAdminSummaryResponse> {
  const query = await buildModelAdminQuery(options);
  return apiGet<ModelAdminSummaryResponse>("/api/v1/admin/models/summary", {
    errorPrefix: "Failed to load model summary",
    query
  });
}

export async function fetchModelAdminHistory(
  options?: ModelAdminQueryOptions
): Promise<ModelAdminHistoryResponse> {
  const query = await buildModelAdminQuery(options);
  return apiGet<ModelAdminHistoryResponse>("/api/v1/admin/models/history", {
    errorPrefix: "Failed to load model history",
    query
  });
}

export async function fetchModelAdminEvaluations(
  options?: ModelAdminEvaluationFilters
): Promise<ModelAdminEvaluationsResponse> {
  const query = await buildModelAdminQuery(options);
  if (options?.modelFamily) {
    query.set("model_family", options.modelFamily);
  }
  return apiGet<ModelAdminEvaluationsResponse>("/api/v1/admin/models/evaluations", {
    errorPrefix: "Failed to load model evaluations",
    query
  });
}

export async function fetchModelAdminEvaluationHistory(
  options?: ModelAdminEvaluationFilters
): Promise<ModelAdminEvaluationHistoryResponse> {
  const query = await buildModelAdminQuery(options);
  if (options?.modelFamily) {
    query.set("model_family", options.modelFamily);
  }
  return apiGet<ModelAdminEvaluationHistoryResponse>("/api/v1/admin/models/evaluations/history", {
    errorPrefix: "Failed to load model evaluation history",
    query
  });
}

export async function fetchModelAdminEvaluationDetail(
  snapshotId: number
): Promise<ModelAdminEvaluationDetailResponse> {
  return apiGet<ModelAdminEvaluationDetailResponse>(`/api/v1/admin/models/evaluations/${snapshotId}`, {
    errorPrefix: "Failed to load model evaluation detail"
  });
}

export async function fetchModelAdminSelections(
  options?: ModelAdminSelectionFilters
): Promise<ModelAdminSelectionsResponse> {
  const query = await buildModelAdminQuery(options);
  if (options?.activeOnly !== undefined) {
    query.set("active_only", String(options.activeOnly));
  }
  return apiGet<ModelAdminSelectionsResponse>("/api/v1/admin/models/selections", {
    errorPrefix: "Failed to load model selections",
    query
  });
}

export async function fetchModelAdminSelectionHistory(
  options?: ModelAdminSelectionFilters
): Promise<ModelAdminSelectionHistoryResponse> {
  const query = await buildModelAdminQuery(options);
  if (options?.activeOnly !== undefined) {
    query.set("active_only", String(options.activeOnly));
  }
  return apiGet<ModelAdminSelectionHistoryResponse>("/api/v1/admin/models/selections/history", {
    errorPrefix: "Failed to load model selection history",
    query
  });
}

export async function fetchModelAdminSelectionDetail(
  selectionId: number
): Promise<ModelAdminSelectionDetailResponse> {
  return apiGet<ModelAdminSelectionDetailResponse>(`/api/v1/admin/models/selections/${selectionId}`, {
    errorPrefix: "Failed to load model selection detail"
  });
}

export async function trainModels(
  options?: ModelAdminTrainingMutationInput
): Promise<ModelAdminTrainResponse> {
  const query = await buildModelAdminQuery(options);
  return apiPost<ModelAdminTrainResponse>("/api/v1/admin/models/train", {
    errorPrefix: "Failed to train models",
    query
  });
}

export async function selectBestModel(
  options?: ModelAdminSelectionMutationInput
): Promise<ModelAdminSelectResponse> {
  const query = await buildModelAdminQuery(options);
  if (options?.selectionPolicyName) {
    query.set("selection_policy_name", options.selectionPolicyName);
  }
  return apiPost<ModelAdminSelectResponse>("/api/v1/admin/models/select", {
    errorPrefix: "Failed to select the best model",
    query
  });
}
