import type {
  SharedBacktestFoldSummary,
  SharedBacktestHistoryResponse,
  SharedBacktestRun,
  SharedBacktestRunResponse,
  SharedBacktestStrategySummary,
  SharedBacktestSummary,
  SharedEvaluationDetailResponse,
  SharedEvaluationSnapshot,
  SharedModelHistoryResponse,
  SharedModelRunDetailResponse,
  SharedModelTrainingRun,
  SharedOpportunityDetailResponse,
  SharedOpportunityHistoryResponse,
  SharedOpportunityListResponse,
  SharedOpportunityMaterializeResponse,
  SharedOpportunityRecord,
  SharedScoringRunDetail,
  SharedScoringRunDetailResponse,
  SharedSelectionDetailResponse,
  SharedSelectionSnapshot
} from "../../shared/frontend/apiTypes";
import type {
  SharedDetailFact,
  SharedProvenanceInspectorData
} from "../../shared/frontend/viewTypes";

export type BacktestHistoryResponse = SharedBacktestHistoryResponse;
export type BacktestRunResponse = SharedBacktestRunResponse;
export type BacktestRun = SharedBacktestRun;
export type BacktestSummary = SharedBacktestSummary;
export type StrategySummary = SharedBacktestStrategySummary;
export type FoldSummary = SharedBacktestFoldSummary;

export type OpportunityRecord = SharedOpportunityRecord;
export type OpportunityHistoryResponse = SharedOpportunityHistoryResponse;
export type OpportunityListResponse = SharedOpportunityListResponse;
export type OpportunityDetailResponse = SharedOpportunityDetailResponse;
export type OpportunityMaterializeResponse = SharedOpportunityMaterializeResponse;
export type ModelTrainingRun = SharedModelTrainingRun;
export type ModelHistoryResponse = SharedModelHistoryResponse;
export type SelectionSnapshot = SharedSelectionSnapshot;
export type EvaluationSnapshot = SharedEvaluationSnapshot;
export type ScoringRunDetail = SharedScoringRunDetail;
export type ScoringRunDetailResponse = SharedScoringRunDetailResponse;

export type AppRoute =
  | { name: "backtests" }
  | { name: "backtest-run"; runId: number }
  | { name: "backtest-fold"; runId: number; foldIndex: number }
  | { name: "backtest-fold-model-run"; runId: number; foldIndex: number; modelRunId: number }
  | {
      name: "backtest-fold-evaluation";
      runId: number;
      foldIndex: number;
      evaluationId: number;
    }
  | {
      name: "artifact-compare";
      runId: number;
      foldIndex: number;
      opportunityId: number;
    }
  | { name: "models" }
  | { name: "model-registry" }
  | { name: "model-runs" }
  | { name: "model-run-detail"; runId: number }
  | { name: "model-evaluations" }
  | { name: "model-evaluation-detail"; evaluationId: number }
  | { name: "model-selections" }
  | { name: "model-selection-detail"; selectionId: number }
  | { name: "opportunities" }
  | { name: "opportunity-detail"; opportunityId: number }
  | { name: "comparable-case"; opportunityId: number; comparableIndex: number }
  | { name: "opportunity-model-run"; opportunityId: number; runId: number }
  | { name: "opportunity-selection"; opportunityId: number; selectionId: number }
  | { name: "opportunity-evaluation"; opportunityId: number; evaluationId: number }
  | { name: "opportunity-scoring-run"; opportunityId: number; scoringRunId: number }
  | { name: "workflow-desk"; deskPath?: string };

export type ProvenanceItem = SharedDetailFact;
export type ProvenanceInspectorData = SharedProvenanceInspectorData;

export type ModelRunDetailResponse = SharedModelRunDetailResponse;
export type SelectionDetailResponse = SharedSelectionDetailResponse;
export type EvaluationDetailResponse = SharedEvaluationDetailResponse;
