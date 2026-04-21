import type {
  SharedCapabilitiesResponse,
  SharedCapabilitiesUiDefaults,
  SharedEvaluationDetailResponse,
  SharedEvaluationHistory,
  SharedEvaluationHistoryEnvelope,
  SharedEvaluationSnapshot,
  SharedFeatureMaterializeEnvelope,
  SharedModelEvaluationDailyBucket,
  SharedModelEvaluationsEnvelope,
  SharedModelFeatureVersion,
  SharedModelHistoryResponse,
  SharedModelRegistryEntry,
  SharedModelRegistryEnvelope,
  SharedModelRunsEnvelope,
  SharedModelSummary,
  SharedModelSummaryEnvelope,
  SharedModelTrainingDailyBucket,
  SharedModelTrainingHistory,
  SharedModelTrainingRun,
  SharedModelTrainEnvelope,
  SharedModelRunDetailResponse,
  SharedModelSelectEnvelope,
  SharedModelSelectionsEnvelope,
  SharedSelectionHistory,
  SharedSelectionHistoryEnvelope,
  SharedSelectionDetailResponse,
  SharedSelectionSnapshot,
  SharedTaskCapability
} from "../../shared/frontend/apiTypes";

export type ModelAdminFeatureVersion = SharedModelFeatureVersion;

export type ModelAdminTaskCapability = SharedTaskCapability;
export type ModelAdminCapabilitiesUiDefaults = SharedCapabilitiesUiDefaults;
export type ModelAdminCapabilitiesResponse = SharedCapabilitiesResponse;

export type ModelAdminQueryOptions = {
  featureKey?: string;
  recentLimit?: number;
  seasonLabel?: string | null;
  targetTask?: string | null;
  teamCode?: string | null;
  trainRatio?: number;
  validationRatio?: number;
};

export type ModelAdminRunFilters = ModelAdminQueryOptions;

export type ModelAdminEvaluationFilters = ModelAdminQueryOptions & {
  modelFamily?: string | null;
};

export type ModelAdminSelectionFilters = ModelAdminQueryOptions & {
  activeOnly?: boolean;
};

export type ModelAdminSelectionMutationInput = ModelAdminQueryOptions & {
  selectionPolicyName?: string;
};

export type ModelAdminTrainingMutationInput = ModelAdminQueryOptions;

export type ModelAdminRegistryFilters = ModelAdminQueryOptions;

export type ModelAdminFilterResponse = {
  feature_key?: string;
  target_task?: string | null;
  team_code?: string | null;
  season_label?: string | null;
  train_ratio?: number;
  validation_ratio?: number;
  recent_limit?: number;
  model_family?: string | null;
  active_only?: boolean;
};

export type ModelAdminRun = SharedModelTrainingRun;
export type ModelAdminRegistryEntry = SharedModelRegistryEntry;
export type ModelAdminEvaluationSnapshot = SharedEvaluationSnapshot;
export type ModelAdminSelectionSnapshot = SharedSelectionSnapshot;
export type ModelAdminTrainingDailyBucket = SharedModelTrainingDailyBucket;
export type ModelAdminEvaluationDailyBucket = SharedModelEvaluationDailyBucket;
export type ModelAdminTrainingHistory = SharedModelTrainingHistory;

export type ModelAdminSummary = SharedModelSummary;

export type ModelAdminEvaluationHistory = SharedEvaluationHistory;

export type ModelAdminSelectionHistory = SharedSelectionHistory;

export type ModelAdminRegistryResponse = SharedModelRegistryEnvelope & {
  filters: ModelAdminFilterResponse;
};

export type ModelAdminRunsResponse = SharedModelRunsEnvelope & {
  filters: ModelAdminFilterResponse;
};

export type ModelAdminRunDetailResponse = SharedModelRunDetailResponse;

export type ModelAdminSummaryResponse = SharedModelSummaryEnvelope & {
  filters: ModelAdminFilterResponse;
};

export type ModelAdminHistoryResponse = SharedModelHistoryResponse & {
  filters: ModelAdminFilterResponse;
};

export type ModelAdminEvaluationsResponse = SharedModelEvaluationsEnvelope & {
  filters: ModelAdminFilterResponse;
};

export type ModelAdminEvaluationHistoryResponse = SharedEvaluationHistoryEnvelope & {
  filters: ModelAdminFilterResponse;
};

export type ModelAdminEvaluationDetailResponse = SharedEvaluationDetailResponse;

export type ModelAdminSelectionsResponse = SharedModelSelectionsEnvelope & {
  filters: ModelAdminFilterResponse;
};

export type ModelAdminSelectionHistoryResponse = SharedSelectionHistoryEnvelope & {
  filters: ModelAdminFilterResponse;
};

export type ModelAdminSelectionDetailResponse = SharedSelectionDetailResponse;

export type ModelAdminTrainResponse = SharedModelTrainEnvelope & {
  filters: ModelAdminFilterResponse;
};

export type ModelAdminFeatureMaterializeResponse = SharedFeatureMaterializeEnvelope & {
  filters: {
    feature_key: string;
  };
};

export type ModelAdminSelectResponse = SharedModelSelectEnvelope & {
  filters: ModelAdminFilterResponse;
};
