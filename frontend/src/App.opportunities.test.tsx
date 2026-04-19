// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
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

function expectByTestIdValue(testId: string, value: string) {
  const element = screen.getByTestId(testId);
  expect(within(element).getByText(value)).not.toBeNull();
}

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
    payload: {
      prediction: {
        model: {
          model_family: "linear_feature",
          selected_feature: "rolling_3_avg_total_error"
        },
        evidence: {
          recommendation: {
            headline: "Back the Lakers",
            recommended_action: "Review"
          },
          strength: {
            rating: "strong",
            overall_score: 0.81
          },
          summary: {
            pattern_sample_size: 11,
            comparable_count: 4
          }
        },
        market_context: {
          home_spread_line: -3.5,
          total_line: 214.5
        }
      }
    },
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
    game_date: "2024-11-03",
    policy_name: "review_only_v2",
    status: "review_manually",
    prediction_value: -2.75,
    signal_strength: 1.7,
    evidence_rating: "moderate",
    recommendation_status: "review_manually",
    materialized_at: "2026-04-20T11:15:00Z",
    materialization_scope: {
      team_code: "DAL",
      season_label: "2024-2025",
      canonical_game_id: null,
      source: "team_scoped",
      scope_key: "team=DAL|season=2024-2025"
    },
    payload: {
      prediction: {
        model: {
          model_family: "tree_stump",
          selected_feature: "days_rest"
        },
        evidence: {
          recommendation: {
            headline: "Pass on Dallas",
            recommended_action: "Hold"
          },
          strength: {
            rating: "moderate",
            overall_score: 0.44
          },
          summary: {
            pattern_sample_size: 7,
            comparable_count: 2
          }
        },
        market_context: {
          home_spread_line: 4.5,
          total_line: 221
        }
      }
    }
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
    await screen.findByTestId("opportunity-detail-card");
    await screen.findByTestId("opportunity-detail-matchup");
    expectByTestIdValue("opportunity-detail-matchup", "LAL vs BOS");
    expectByTestIdValue("opportunity-detail-game-date", "2024-11-05");
    expectByTestIdValue("opportunity-detail-prediction-value", "1.200");
    expectByTestIdValue("opportunity-detail-signal-strength", "0.80");
    expectByTestIdValue("opportunity-detail-evidence-rating", "strong");
    expectByTestIdValue("opportunity-detail-policy", "candidate_v1");
    expectByTestIdValue("opportunity-detail-model-family", "linear_feature");
    expectByTestIdValue("opportunity-detail-selected-feature", "rolling_3_avg_total_error");
    expectByTestIdValue("opportunity-detail-queue-scope", "operator-wide");
    expectByTestIdValue("opportunity-detail-recommendation", "Back the Lakers");
    expectByTestIdValue("opportunity-detail-market-context", "Spread -3.5");

    fireEvent.click(screen.getByTestId("opportunity-row-102"));

    await waitFor(() => {
      expectByTestIdValue("opportunity-detail-matchup", "DAL vs NYK");
    });
    expectByTestIdValue("opportunity-detail-game-date", "2024-11-03");
    expectByTestIdValue("opportunity-detail-prediction-value", "-2.750");
    expectByTestIdValue("opportunity-detail-signal-strength", "1.70");
    expectByTestIdValue("opportunity-detail-evidence-rating", "moderate");
    expectByTestIdValue("opportunity-detail-policy", "review_only_v2");
    expectByTestIdValue("opportunity-detail-model-family", "tree_stump");
    expectByTestIdValue("opportunity-detail-selected-feature", "days_rest");
    expectByTestIdValue("opportunity-detail-queue-scope", "team=DAL|season=2024-2025");
    expectByTestIdValue("opportunity-detail-recommendation", "Pass on Dallas");
    expectByTestIdValue("opportunity-detail-market-context", "Spread 4.5");
    expect(window.location.hash).toBe("#/opportunities");
  });
});
