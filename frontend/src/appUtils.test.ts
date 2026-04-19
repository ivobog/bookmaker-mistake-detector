import { describe, expect, it } from "vitest";

import type { AppRoute } from "./appTypes";
import { parseRouteFromHash, routeHash } from "./appUtils";

const roundTripRoutes: AppRoute[] = [
  { name: "backtests" },
  { name: "backtest-run", runId: 101 },
  { name: "backtest-fold", runId: 101, foldIndex: 0 },
  { name: "backtest-fold-model-run", runId: 101, foldIndex: 0, modelRunId: 301 },
  { name: "backtest-fold-evaluation", runId: 101, foldIndex: 0, evaluationId: 401 },
  { name: "artifact-compare", runId: 101, foldIndex: 0, opportunityId: 7 },
  { name: "models" },
  { name: "model-registry" },
  { name: "model-runs" },
  { name: "model-run-detail", runId: 301 },
  { name: "model-evaluations" },
  { name: "model-evaluation-detail", evaluationId: 401 },
  { name: "model-selections" },
  { name: "model-selection-detail", selectionId: 501 },
  { name: "opportunities" },
  { name: "opportunity-detail", opportunityId: 7 },
  { name: "comparable-case", opportunityId: 7, comparableIndex: 0 },
  { name: "opportunity-model-run", opportunityId: 7, runId: 301 },
  { name: "opportunity-selection", opportunityId: 7, selectionId: 501 },
  { name: "opportunity-evaluation", opportunityId: 7, evaluationId: 401 },
  { name: "opportunity-scoring-run", opportunityId: 7, scoringRunId: 601 }
];

describe("parseRouteFromHash", () => {
  it("defaults unknown hashes back to backtests", () => {
    expect(parseRouteFromHash("#/does-not-exist")).toEqual({ name: "backtests" });
  });

  it("treats empty and root hashes as the backtests landing route", () => {
    expect(parseRouteFromHash("")).toEqual({ name: "backtests" });
    expect(parseRouteFromHash("#/")).toEqual({ name: "backtests" });
  });

  it("parses the full model admin route family", () => {
    expect(parseRouteFromHash("#/models")).toEqual({ name: "models" });
    expect(parseRouteFromHash("#/models/registry")).toEqual({ name: "model-registry" });
    expect(parseRouteFromHash("#/models/runs/301")).toEqual({
      name: "model-run-detail",
      runId: 301
    });
    expect(parseRouteFromHash("#/models/evaluations/401")).toEqual({
      name: "model-evaluation-detail",
      evaluationId: 401
    });
    expect(parseRouteFromHash("#/models/selections/501")).toEqual({
      name: "model-selection-detail",
      selectionId: 501
    });
  });
});

describe("routeHash", () => {
  it("serializes the full model admin route family", () => {
    expect(routeHash({ name: "models" })).toBe("#/models");
    expect(routeHash({ name: "model-registry" })).toBe("#/models/registry");
    expect(routeHash({ name: "model-runs" })).toBe("#/models/runs");
    expect(routeHash({ name: "model-run-detail", runId: 301 })).toBe("#/models/runs/301");
    expect(routeHash({ name: "model-evaluations" })).toBe("#/models/evaluations");
    expect(routeHash({ name: "model-evaluation-detail", evaluationId: 401 })).toBe(
      "#/models/evaluations/401"
    );
    expect(routeHash({ name: "model-selections" })).toBe("#/models/selections");
    expect(routeHash({ name: "model-selection-detail", selectionId: 501 })).toBe(
      "#/models/selections/501"
    );
  });

  it("round-trips all supported routes through parseRouteFromHash", () => {
    for (const route of roundTripRoutes) {
      expect(parseRouteFromHash(routeHash(route))).toEqual(route);
    }
  });
});
