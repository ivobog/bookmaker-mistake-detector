// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import App from "./App";

vi.mock("./api", () => ({
  apiBaseUrl: "http://127.0.0.1:8000",
  fetchBacktestHistory: vi.fn(),
  fetchBacktestRunDetail: vi.fn(),
  fetchEvaluationDetail: vi.fn(),
  fetchModelAdminHistory: vi.fn(),
  fetchModelRunDetail: vi.fn(),
  fetchOpportunityDetail: vi.fn(),
  fetchOpportunityHistory: vi.fn(),
  fetchOpportunities: vi.fn(),
  fetchScoringRunDetail: vi.fn(),
  fetchSelectionDetail: vi.fn(),
  materializeOpportunities: vi.fn(),
  runBacktest: vi.fn()
}));

import {
  fetchBacktestHistory,
  fetchModelAdminHistory,
  fetchOpportunityDetail,
  fetchOpportunityHistory,
  fetchOpportunities
} from "./api";

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
  window.location.hash = "";
});

beforeEach(() => {
  window.location.hash = "#/opportunities";

  vi.mocked(fetchBacktestHistory).mockResolvedValue({
    repository_mode: "in_memory",
    filters: {
      target_task: "spread_error_regression",
      team_code: null,
      season_label: null,
      recent_limit: 10
    },
    model_backtest_history: {
      overview: {
        run_count: 0,
        status_counts: {},
        target_task_counts: {},
        strategy_counts: {},
        best_candidate_threshold_run: null,
        latest_run: null
      },
      daily_buckets: [],
      recent_runs: []
    }
  });

  vi.mocked(fetchModelAdminHistory).mockResolvedValue({
    repository_mode: "in_memory",
    filters: {
      target_task: "spread_error_regression",
      team_code: null,
      season_label: null,
      recent_limit: 8
    },
    model_history: {
      overview: {
        run_count: 0,
        fallback_run_count: 0,
        best_overall: null,
        latest_run: null
      },
      daily_buckets: [],
      recent_runs: []
    }
  });

  const opportunityOne = {
    id: 101,
    target_task: "spread_error_regression",
    source_kind: "historical_game",
    scenario_key: null,
    opportunity_key: "opp-101",
    team_code: "LAL",
    opponent_code: "BOS",
    season_label: "2024-2025",
    canonical_game_id: 1,
    game_date: "2024-11-05",
    policy_name: "candidate_v1",
    status: "candidate_signal",
    prediction_value: 1.2,
    signal_strength: 0.8,
    evidence_rating: "strong",
    recommendation_status: "candidate_signal",
    materialization_batch_id: "batch-1",
    materialized_at: "2026-04-19T10:00:00Z",
    materialization_scope: {
      team_code: null,
      season_label: null,
      canonical_game_id: null,
      source: "operator",
      scope_key: "operator-wide"
    },
    model_explainability: null,
    payload: {},
    created_at: "2026-04-19T10:00:00Z",
    updated_at: "2026-04-19T10:00:00Z"
  };

  const opportunityTwo = {
    ...opportunityOne,
    id: 102,
    opportunity_key: "opp-102",
    team_code: "DAL",
    opponent_code: "NYK",
    canonical_game_id: 2,
    game_date: "2024-11-03"
  };

  vi.mocked(fetchOpportunityHistory).mockResolvedValue({
    repository_mode: "in_memory",
    filters: {
      target_task: "spread_error_regression",
      team_code: null,
      season_label: null,
      recent_limit: 6
    },
    model_opportunity_history: {
      overview: {
        opportunity_count: 2,
        status_counts: { candidate_signal: 2 },
        source_kind_counts: { historical_game: 2 },
        evidence_rating_counts: { strong: 2 },
        latest_opportunity: opportunityOne
      },
      recent_opportunities: [opportunityOne, opportunityTwo]
    }
  });

  vi.mocked(fetchOpportunities).mockResolvedValue({
    repository_mode: "in_memory",
    queue_batch_id: "batch-1",
    queue_materialized_at: "2026-04-19T10:00:00Z",
    queue_scope: {
      team_code: null,
      season_label: null,
      canonical_game_id: null,
      source: "operator",
      scope_key: "operator-wide"
    },
    queue_scope_label: "Operator-wide queue",
    queue_scope_is_scoped: false,
    opportunity_count: 2,
    opportunities: [opportunityOne, opportunityTwo]
  });

  vi.mocked(fetchOpportunityDetail).mockImplementation(async (opportunityId: number) => ({
    repository_mode: "in_memory",
    opportunity: opportunityId === 102 ? opportunityTwo : opportunityOne
  }));
});

describe("App opportunities selection", () => {
  it("updates the in-page opportunity detail when a different queue row is clicked", async () => {
    render(<App />);

    await screen.findByText("Analyst queue");
    await screen.findByText("2024-11-05");

    fireEvent.click(screen.getByTestId("opportunity-row-102"));

    await waitFor(() => expect(screen.getByText("2024-11-03")).not.toBeNull());
    expect(window.location.hash).toBe("#/opportunities");
  });
});
