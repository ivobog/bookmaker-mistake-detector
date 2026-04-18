import { expect, test, type Page } from "@playwright/test";

type QueueVariant = "global" | "scoped";

function buildOpportunity(variant: QueueVariant) {
  const scoped = variant === "scoped";
  return {
    id: 7,
    model_scoring_run_id: null,
    model_selection_snapshot_id: null,
    model_evaluation_snapshot_id: null,
    feature_version_id: 1,
    target_task: "spread_error_regression",
    source_kind: "historical_game",
    scenario_key: null,
    opportunity_key: "opp-7",
    team_code: "LAL",
    opponent_code: "BOS",
    season_label: "2025-2026",
    canonical_game_id: 3,
    game_date: "2026-04-20",
    policy_name: "spread_edge_policy_v1",
    status: "review_manually",
    prediction_value: -1.0352,
    signal_strength: 1.0352,
    evidence_rating: "medium",
    recommendation_status: "lean",
    materialization_batch_id: scoped ? "batch-lal-2026-04-20" : "batch-global-2026-04-20",
    materialized_at: "2026-04-18T00:05:00+00:00",
    materialization_scope: {
      team_code: scoped ? "LAL" : null,
      season_label: scoped ? "2025-2026" : null,
      canonical_game_id: null,
      source: scoped ? "team_scoped" : "operator",
      scope_key: scoped ? "team=LAL|season=2025-2026" : "operator-wide"
    },
    model_explainability: {
      model_family: "tree_stump",
      selected_feature: "rolling_10_avg_total_error",
      threshold: -0.475,
      left_prediction: 0.5142,
      right_prediction: -1.0352,
      selected_feature_value: 0.61,
      branch: "right"
    },
    payload: {
      prediction: {
        model: {
          model_family: "tree_stump",
          selected_feature: "rolling_10_avg_total_error"
        }
      },
      active_selection: {},
      active_evaluation_snapshot: {},
      scenario: null
    },
    created_at: "2026-04-18T00:00:00+00:00",
    updated_at: "2026-04-18T00:05:00+00:00"
  };
}

async function stubOpportunityWorkspaceApis(page: Page, variant: QueueVariant) {
  const opportunity = buildOpportunity(variant);
  await page.route("**/api/v1/admin/models/backtests/history*", async (route) => {
    await route.fulfill({
      json: {
        repository_mode: "in_memory",
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
      }
    });
  });
  await page.route("**/api/v1/admin/models/history*", async (route) => {
    await route.fulfill({
      json: {
        repository_mode: "in_memory",
        model_history: {
          overview: {
            run_count: 0,
            fallback_run_count: 0,
            best_overall: null,
            latest_run: null
          },
          recent_runs: []
        }
      }
    });
  });
  await page.route("**/api/v1/admin/models/opportunities/history*", async (route) => {
    await route.fulfill({
      json: {
        repository_mode: "in_memory",
        filters: {
          target_task: "spread_error_regression",
          team_code: variant === "scoped" ? "LAL" : null,
          season_label: "2025-2026",
          recent_limit: 6
        },
        model_opportunity_history: {
          overview: {
            opportunity_count: 1,
            status_counts: {
              review_manually: 1
            },
            source_kind_counts: {
              historical_game: 1
            },
            evidence_rating_counts: {
              medium: 1
            },
            latest_opportunity: opportunity
          },
          recent_opportunities: [opportunity]
        }
      }
    });
  });
  await page.route("**/api/v1/analyst/opportunities*", async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.endsWith("/7")) {
      await route.fulfill({
        json: {
          repository_mode: "in_memory",
          opportunity
        }
      });
      return;
    }
    await route.fulfill({
      json: {
        repository_mode: "in_memory",
        queue_batch_id: opportunity.materialization_batch_id,
        queue_materialized_at: opportunity.materialized_at,
        queue_scope: opportunity.materialization_scope,
        queue_scope_label:
          variant === "scoped"
            ? "Scoped queue: team=LAL, season=2025-2026"
            : "Operator-wide queue",
        queue_scope_is_scoped: variant === "scoped",
        opportunity_count: 1,
        opportunities: [opportunity]
      }
    });
  });
}

test.describe("Opportunity queue scope", () => {
  test("renders scoped queue guardrails and stump explanation", async ({ page }) => {
    await stubOpportunityWorkspaceApis(page, "scoped");

    await page.goto("#/opportunities");

    await expect(page.getByText("Scoped queue: team=LAL, season=2025-2026")).toBeVisible();
    await expect(
      page.getByText(
        "This queue was materialized from a scoped run and may not represent the global analyst queue."
      )
    ).toBeVisible();
    await expect(page.getByText("Tree stump explanation")).toBeVisible();
    await expect(page.getByText("Branch taken")).toBeVisible();
  });

  test("renders operator-wide queue labeling without the scoped warning", async ({ page }) => {
    await stubOpportunityWorkspaceApis(page, "global");

    await page.goto("#/opportunities");

    await expect(page.getByText("Operator-wide queue")).toBeVisible();
    await expect(page.getByText("Queue scope: operator-wide.")).toBeVisible();
    await expect(
      page.getByText(
        "This queue was materialized from a scoped run and may not represent the global analyst queue."
      )
    ).toHaveCount(0);
  });
});
