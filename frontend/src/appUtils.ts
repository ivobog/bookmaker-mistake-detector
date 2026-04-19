import type { AppRoute } from "./appTypes";

export function formatMetric(value: number | null, digits = 4): string {
  return value === null ? "n/a" : value.toFixed(digits);
}

export function formatPercent(value: number | null): string {
  return value === null ? "n/a" : `${(value * 100).toFixed(1)}%`;
}

export function formatTimestamp(value: string | null): string {
  if (!value) {
    return "n/a";
  }
  return new Date(value).toLocaleString();
}

export function formatCompactNumber(value: number | null, digits = 2): string {
  return value === null ? "n/a" : value.toFixed(digits);
}

export function getMetricDelta(left: number | null, right: number | null): number | null {
  if (left === null || right === null) {
    return null;
  }
  return right - left;
}

export function formatDelta(value: number | null, digits = 4): string {
  if (value === null) {
    return "n/a";
  }
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(digits)}`;
}

export function getAlignmentLabel(isAligned: boolean | null): string {
  if (isAligned === null) {
    return "unknown";
  }
  return isAligned ? "aligned" : "mismatch";
}

export function getAlignmentTone(isAligned: boolean | null): "good" | "warning" | "neutral" {
  if (isAligned === null) {
    return "neutral";
  }
  return isAligned ? "good" : "warning";
}

export function formatLabel(value: string | null | undefined): string {
  if (!value) {
    return "n/a";
  }
  return value.replace(/_/g, " ");
}

export function readNested(source: unknown, ...path: string[]): unknown {
  let current = source;
  for (const key of path) {
    if (!current || typeof current !== "object" || !(key in current)) {
      return undefined;
    }
    current = (current as Record<string, unknown>)[key];
  }
  return current;
}

export function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

export function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

export function parseRouteFromHash(hash: string): AppRoute {
  const normalized = hash.replace(/^#/, "").replace(/^\//, "");
  if (!normalized || normalized === "backtests") {
    return { name: "backtests" };
  }
  if (normalized === "models") {
    return { name: "models" };
  }
  if (normalized === "models/registry") {
    return { name: "model-registry" };
  }
  if (normalized === "models/runs") {
    return { name: "model-runs" };
  }
  const modelRunDetailMatch = /^models\/runs\/(\d+)$/.exec(normalized);
  if (modelRunDetailMatch) {
    return {
      name: "model-run-detail",
      runId: Number(modelRunDetailMatch[1])
    };
  }
  if (normalized === "models/evaluations") {
    return { name: "model-evaluations" };
  }
  const modelEvaluationDetailMatch = /^models\/evaluations\/(\d+)$/.exec(normalized);
  if (modelEvaluationDetailMatch) {
    return {
      name: "model-evaluation-detail",
      evaluationId: Number(modelEvaluationDetailMatch[1])
    };
  }
  if (normalized === "models/selections") {
    return { name: "model-selections" };
  }
  const modelSelectionDetailMatch = /^models\/selections\/(\d+)$/.exec(normalized);
  if (modelSelectionDetailMatch) {
    return {
      name: "model-selection-detail",
      selectionId: Number(modelSelectionDetailMatch[1])
    };
  }
  const backtestMatch = /^backtests\/(\d+)$/.exec(normalized);
  if (backtestMatch) {
    return {
      name: "backtest-run",
      runId: Number(backtestMatch[1])
    };
  }
  const backtestFoldMatch = /^backtests\/(\d+)\/folds\/(\d+)$/.exec(normalized);
  if (backtestFoldMatch) {
    return {
      name: "backtest-fold",
      runId: Number(backtestFoldMatch[1]),
      foldIndex: Number(backtestFoldMatch[2])
    };
  }
  const backtestFoldModelRunMatch = /^backtests\/(\d+)\/folds\/(\d+)\/model-runs\/(\d+)$/.exec(
    normalized
  );
  if (backtestFoldModelRunMatch) {
    return {
      name: "backtest-fold-model-run",
      runId: Number(backtestFoldModelRunMatch[1]),
      foldIndex: Number(backtestFoldModelRunMatch[2]),
      modelRunId: Number(backtestFoldModelRunMatch[3])
    };
  }
  const backtestFoldEvaluationMatch = /^backtests\/(\d+)\/folds\/(\d+)\/evaluations\/(\d+)$/.exec(
    normalized
  );
  if (backtestFoldEvaluationMatch) {
    return {
      name: "backtest-fold-evaluation",
      runId: Number(backtestFoldEvaluationMatch[1]),
      foldIndex: Number(backtestFoldEvaluationMatch[2]),
      evaluationId: Number(backtestFoldEvaluationMatch[3])
    };
  }
  const artifactCompareMatch =
    /^compare\/backtests\/(\d+)\/folds\/(\d+)\/opportunities\/(\d+)$/.exec(normalized);
  if (artifactCompareMatch) {
    return {
      name: "artifact-compare",
      runId: Number(artifactCompareMatch[1]),
      foldIndex: Number(artifactCompareMatch[2]),
      opportunityId: Number(artifactCompareMatch[3])
    };
  }
  if (normalized === "opportunities") {
    return { name: "opportunities" };
  }
  const detailMatch = /^opportunities\/(\d+)$/.exec(normalized);
  if (detailMatch) {
    return {
      name: "opportunity-detail",
      opportunityId: Number(detailMatch[1])
    };
  }
  const modelRunMatch = /^opportunities\/(\d+)\/model-runs\/(\d+)$/.exec(normalized);
  if (modelRunMatch) {
    return {
      name: "opportunity-model-run",
      opportunityId: Number(modelRunMatch[1]),
      runId: Number(modelRunMatch[2])
    };
  }
  const selectionMatch = /^opportunities\/(\d+)\/selections\/(\d+)$/.exec(normalized);
  if (selectionMatch) {
    return {
      name: "opportunity-selection",
      opportunityId: Number(selectionMatch[1]),
      selectionId: Number(selectionMatch[2])
    };
  }
  const evaluationMatch = /^opportunities\/(\d+)\/evaluations\/(\d+)$/.exec(normalized);
  if (evaluationMatch) {
    return {
      name: "opportunity-evaluation",
      opportunityId: Number(evaluationMatch[1]),
      evaluationId: Number(evaluationMatch[2])
    };
  }
  const scoringRunMatch = /^opportunities\/(\d+)\/scoring-runs\/(\d+)$/.exec(normalized);
  if (scoringRunMatch) {
    return {
      name: "opportunity-scoring-run",
      opportunityId: Number(scoringRunMatch[1]),
      scoringRunId: Number(scoringRunMatch[2])
    };
  }
  const comparableMatch = /^opportunities\/(\d+)\/comparables\/(\d+)$/.exec(normalized);
  if (comparableMatch) {
    return {
      name: "comparable-case",
      opportunityId: Number(comparableMatch[1]),
      comparableIndex: Number(comparableMatch[2])
    };
  }
  return { name: "backtests" };
}

export function routeHash(route: AppRoute): string {
  if (route.name === "backtests") {
    return "#/backtests";
  }
  if (route.name === "models") {
    return "#/models";
  }
  if (route.name === "model-registry") {
    return "#/models/registry";
  }
  if (route.name === "model-runs") {
    return "#/models/runs";
  }
  if (route.name === "model-run-detail") {
    return `#/models/runs/${route.runId}`;
  }
  if (route.name === "model-evaluations") {
    return "#/models/evaluations";
  }
  if (route.name === "model-evaluation-detail") {
    return `#/models/evaluations/${route.evaluationId}`;
  }
  if (route.name === "model-selections") {
    return "#/models/selections";
  }
  if (route.name === "model-selection-detail") {
    return `#/models/selections/${route.selectionId}`;
  }
  if (route.name === "backtest-run") {
    return `#/backtests/${route.runId}`;
  }
  if (route.name === "backtest-fold") {
    return `#/backtests/${route.runId}/folds/${route.foldIndex}`;
  }
  if (route.name === "backtest-fold-model-run") {
    return `#/backtests/${route.runId}/folds/${route.foldIndex}/model-runs/${route.modelRunId}`;
  }
  if (route.name === "backtest-fold-evaluation") {
    return `#/backtests/${route.runId}/folds/${route.foldIndex}/evaluations/${route.evaluationId}`;
  }
  if (route.name === "artifact-compare") {
    return `#/compare/backtests/${route.runId}/folds/${route.foldIndex}/opportunities/${route.opportunityId}`;
  }
  if (route.name === "opportunities") {
    return "#/opportunities";
  }
  if (route.name === "opportunity-model-run") {
    return `#/opportunities/${route.opportunityId}/model-runs/${route.runId}`;
  }
  if (route.name === "opportunity-selection") {
    return `#/opportunities/${route.opportunityId}/selections/${route.selectionId}`;
  }
  if (route.name === "opportunity-evaluation") {
    return `#/opportunities/${route.opportunityId}/evaluations/${route.evaluationId}`;
  }
  if (route.name === "opportunity-scoring-run") {
    return `#/opportunities/${route.opportunityId}/scoring-runs/${route.scoringRunId}`;
  }
  if (route.name === "comparable-case") {
    return `#/opportunities/${route.opportunityId}/comparables/${route.comparableIndex}`;
  }
  return `#/opportunities/${route.opportunityId}`;
}
