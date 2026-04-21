import type {
  SharedEvaluationSnapshot,
  SharedModelHistoryResponse,
  SharedModelTrainingRun,
  SharedScoringRunDetail,
  SharedSelectionSnapshot
} from "./apiTypes";

export type SharedDetailFact = {
  label: string;
  value: string;
  href?: string;
};

export type SharedProvenanceInspectorData = {
  modelRun?: SharedModelTrainingRun | null;
  modelHistory?: SharedModelHistoryResponse["model_history"] | null;
  selection: SharedSelectionSnapshot | null;
  evaluation: SharedEvaluationSnapshot | null;
  scoringRun: SharedScoringRunDetail | null;
};
