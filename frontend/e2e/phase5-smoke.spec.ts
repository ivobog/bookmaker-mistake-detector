import { expect, test, type Page } from "@playwright/test";

const backtestRunId = 101;
const foldIndex = 0;
const opportunityId = 7;
const modelRunId = 301;
const evaluationId = 401;
const selectionId = 501;

const modelRun = {
  id: modelRunId,
  model_registry_id: 11,
  feature_version_id: 21,
  target_task: "spread_error_regression",
  team_code: "LAL",
  season_label: "2025-2026",
  status: "COMPLETED",
  train_ratio: 0.7,
  validation_ratio: 0.15,
  artifact: {
    model_family: "linear_feature",
    selected_feature: "rolling_10_avg_total_error",
    fallback_strategy: null,
    fallback_reason: null,
    selection_metrics: {
      selected_branch: "primary_fit"
    },
    split_summary: {
      train_game_count: 12,
      validation_game_count: 4,
      test_game_count: 3
    }
  },
  metrics: {
    train: { prediction_count: 12, mae: 0.78 },
    validation: { prediction_count: 4, mae: 0.61 },
    test: { prediction_count: 3, mae: 0.73 }
  },
  created_at: "2026-04-18T00:00:00+00:00",
  completed_at: "2026-04-18T00:10:00+00:00"
};

const evaluationSnapshot = {
  id: evaluationId,
  model_training_run_id: modelRunId,
  model_registry_id: 11,
  feature_version_id: 21,
  target_task: "spread_error_regression",
  model_family: "linear_feature",
  selected_feature: "rolling_10_avg_total_error",
  fallback_strategy: null,
  primary_metric_name: "mae",
  validation_metric_value: 0.61,
  test_metric_value: 0.73,
  validation_prediction_count: 4,
  test_prediction_count: 3,
  snapshot: {
    artifact: modelRun.artifact,
    metrics: modelRun.metrics
  },
  created_at: "2026-04-18T00:11:00+00:00"
};

const selectionSnapshot = {
  id: selectionId,
  model_evaluation_snapshot_id: evaluationId,
  model_training_run_id: modelRunId,
  model_registry_id: 11,
  feature_version_id: 21,
  target_task: "spread_error_regression",
  model_family: "linear_feature",
  selection_policy_name: "validation_mae_candidate_v1",
  rationale: {
    reason: "lowest_validation_mae",
    candidate_count: 2
  },
  is_active: true,
  created_at: "2026-04-18T00:12:00+00:00"
};

const registryEntry = {
  id: 11,
  model_key: "spread_error_regression_linear_feature_global",
  target_task: "spread_error_regression",
  model_family: "linear_feature",
  version_label: "v1",
  description: "Baseline linear feature model",
  config: {
    team_code_scope: null
  },
  created_at: "2026-04-18T00:00:00+00:00"
};

function createModelRun(
  runId: number,
  overrides?: Partial<typeof modelRun>
): typeof modelRun {
  return {
    ...modelRun,
    ...overrides,
    id: runId,
    artifact: {
      ...modelRun.artifact,
      ...(overrides?.artifact ?? {})
    },
    metrics: {
      ...modelRun.metrics,
      ...(overrides?.metrics ?? {})
    }
  };
}

function createEvaluationSnapshot(
  snapshotId: number,
  run: typeof modelRun,
  overrides?: Partial<typeof evaluationSnapshot>
): typeof evaluationSnapshot {
  return {
    ...evaluationSnapshot,
    ...overrides,
    id: snapshotId,
    model_training_run_id: run.id,
    model_registry_id: run.model_registry_id,
    feature_version_id: run.feature_version_id,
    target_task: run.target_task,
    model_family: String(run.artifact.model_family),
    selected_feature: String(run.artifact.selected_feature ?? evaluationSnapshot.selected_feature),
    snapshot: {
      artifact: run.artifact,
      metrics: run.metrics
    }
  };
}

function createSelectionSnapshot(
  nextSelectionId: number,
  snapshot: typeof evaluationSnapshot,
  run: typeof modelRun,
  overrides?: Partial<typeof selectionSnapshot>
): typeof selectionSnapshot {
  return {
    ...selectionSnapshot,
    ...overrides,
    id: nextSelectionId,
    model_evaluation_snapshot_id: snapshot.id,
    model_training_run_id: run.id,
    model_registry_id: run.model_registry_id,
    feature_version_id: run.feature_version_id,
    target_task: run.target_task,
    model_family: snapshot.model_family
  };
}

type ModelAdminRequestLog = {
  evaluationDetail: string[];
  evaluationHistory: string[];
  evaluations: string[];
  history: string[];
  runDetail: string[];
  runs: string[];
  selectionDetail: string[];
  selectionHistory: string[];
  selections: string[];
  select: string[];
  summary: string[];
  train: string[];
};

const backtestRun = {
  id: backtestRunId,
  target_task: "spread_error_regression",
  strategy_name: "candidate_threshold",
  fold_count: 1,
  selection_policy_name: "validation_mae_candidate_v1",
  minimum_train_games: 1,
  test_window_games: 1,
  payload: {
    target_task: "spread_error_regression",
    selection_policy_name: "validation_mae_candidate_v1",
    strategy_name: "candidate_threshold",
    minimum_train_games: 1,
    test_window_games: 1,
    dataset_row_count: 19,
    dataset_game_count: 9,
    fold_count: 1,
    selected_model_family_counts: {
      linear_feature: 1
    },
    prediction_metrics: {
      prediction_count: 3,
      mae: 0.73,
      rmse: 0.81,
      average_prediction_value: -1.04,
      average_realized_residual: 0.12
    },
    strategy_results: {
      candidate_threshold: {
        strategy_name: "candidate_threshold",
        threshold: 0.5,
        bet_count: 3,
        win_count: 2,
        loss_count: 1,
        push_count: 0,
        hit_rate: 0.667,
        push_rate: 0,
        roi: 0.21,
        profit_units: 0.63,
        edge_bucket_performance: {}
      }
    },
    folds: [
      {
        fold_index: foldIndex,
        train_game_count: 12,
        test_game_count: 3,
        train_game_ids: [1, 2, 3],
        test_game_ids: [4, 5, 6],
        selected_model: {
          evaluation_snapshot_id: evaluationId,
          model_training_run_id: modelRunId,
          model_family: "linear_feature",
          selected_feature: "rolling_10_avg_total_error",
          fallback_strategy: null,
          validation_metric_value: 0.61,
          test_metric_value: 0.73
        },
        prediction_metrics: {
          prediction_count: 3,
          mae: 0.73,
          rmse: 0.81,
          average_prediction_value: -1.04,
          average_realized_residual: 0.12
        },
        strategies: {}
      }
    ]
  },
  created_at: "2026-04-18T00:13:00+00:00",
  completed_at: "2026-04-18T00:20:00+00:00"
};

const opportunity = {
  id: opportunityId,
  model_scoring_run_id: null,
  model_selection_snapshot_id: selectionId,
  model_evaluation_snapshot_id: evaluationId,
  feature_version_id: 21,
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
  materialization_batch_id: "batch-global-2026-04-20",
  materialized_at: "2026-04-18T00:25:00+00:00",
  materialization_scope: {
    team_code: null,
    season_label: null,
    canonical_game_id: null,
    source: "operator",
    scope_key: "operator-wide"
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
      market_context: {
        home_spread_line: -4.5,
        total_line: 227.5
      },
      model: {
        model_family: "linear_feature",
        selected_feature: "rolling_10_avg_total_error"
      },
      evidence: {
        strength: {
          rating: "medium",
          overall_score: 0.63
        },
        recommendation: {
          headline: "Review the spread signal",
          recommended_action: "Inspect manually",
          policy_profile: {
            thresholds: {
              candidate_signal: 0.5,
              escalate: 1.2
            }
          },
          rationale: ["Validation improvement held up in test split"],
          blocking_factors: [],
          next_steps: ["Compare against active backtest fold"]
        },
        summary: {
          pattern_sample_size: 8,
          comparable_count: 1,
          pattern_key: "venue=home|rest=2",
          best_benchmark: {
            baseline_name: "linear_feature",
            primary_metric: "mae",
            validation_primary_metric: 0.61,
            test_primary_metric: 0.73
          }
        },
        pattern: {
          selected_pattern: {
            pattern_key: "venue=home|rest=2",
            sample_size: 8,
            signal_strength: 0.82,
            target_mean: -1.03
          }
        },
        benchmark_context: {
          benchmark_rankings: []
        },
        comparables: {
          summary: {
            comparable_count: 1,
            top_similarity_score: 0.88
          },
          cases: [
            {
              canonical_game_id: 88,
              team_code: "LAL",
              opponent_code: "BOS",
              similarity_score: 0.88,
              target_value: -1.5,
              matched_conditions: {
                venue: "home",
                days_rest_bucket: "2"
              }
            }
          ]
        }
      }
    },
    active_selection: selectionSnapshot,
    active_evaluation_snapshot: evaluationSnapshot,
    scenario: null
  },
  created_at: "2026-04-18T00:21:00+00:00",
  updated_at: "2026-04-18T00:25:00+00:00"
};

async function stubPhaseFiveApis(page: Page) {
  const modelAdminRequests: ModelAdminRequestLog = {
    evaluationDetail: [],
    evaluationHistory: [],
    evaluations: [],
    history: [],
    runDetail: [],
    runs: [],
    selectionDetail: [],
    selectionHistory: [],
    selections: [],
    select: [],
    summary: [],
    train: []
  };
  const state = {
    evaluations: [evaluationSnapshot],
    runs: [modelRun],
    selections: [selectionSnapshot]
  };

  await page.route("**/api/v1/admin/models/backtests/history*", async (route) => {
    await route.fulfill({
      json: {
        repository_mode: "in_memory",
        filters: {
          target_task: "spread_error_regression",
          team_code: null,
          season_label: null,
          recent_limit: 6
        },
        model_backtest_history: {
          overview: {
            run_count: 1,
            status_counts: { COMPLETED: 1 },
            target_task_counts: { spread_error_regression: 1 },
            strategy_counts: { candidate_threshold: 1 },
            best_candidate_threshold_run: backtestRun,
            latest_run: backtestRun
          },
          daily_buckets: [],
          recent_runs: [backtestRun]
        }
      }
    });
  });

  await page.route("**/api/v1/analyst/backtests/*", async (route) => {
    await route.fulfill({
      json: {
        repository_mode: "in_memory",
        backtest_run: backtestRun
      }
    });
  });

  await page.route("**/api/v1/admin/models/opportunities/history*", async (route) => {
    await route.fulfill({
      json: {
        repository_mode: "in_memory",
        filters: {
          target_task: "spread_error_regression",
          team_code: null,
          season_label: null,
          recent_limit: 6
        },
        model_opportunity_history: {
          overview: {
            opportunity_count: 1,
            status_counts: { review_manually: 1, candidate_signal: 0 },
            source_kind_counts: { historical_game: 1, future_scenario: 0 },
            evidence_rating_counts: { medium: 1 },
            latest_opportunity: opportunity
          },
          recent_opportunities: [opportunity]
        }
      }
    });
  });

  await page.route("**/api/v1/analyst/opportunities*", async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname !== "/api/v1/analyst/opportunities") {
      await route.fallback();
      return;
    }

    await route.fulfill({
      json: {
        repository_mode: "in_memory",
        queue_batch_id: opportunity.materialization_batch_id,
        queue_materialized_at: opportunity.materialized_at,
        queue_scope: opportunity.materialization_scope,
        queue_scope_label: "Operator-wide queue",
        queue_scope_is_scoped: false,
        opportunity_count: 1,
        opportunities: [opportunity]
      }
    });
  });

  await page.route(`**/api/v1/analyst/opportunities/${opportunityId}*`, async (route) => {
    await route.fulfill({
      json: {
        repository_mode: "in_memory",
        opportunity
      }
    });
  });

  await page.route("**/api/v1/admin/models/history*", async (route) => {
    modelAdminRequests.history.push(route.request().url());
    const latestRun = state.runs[0] ?? null;
    await route.fulfill({
      json: {
        repository_mode: "in_memory",
        filters: {
          target_task: "spread_error_regression",
          team_code: null,
          season_label: null,
          recent_limit: 8
        },
        model_history: {
          overview: {
            run_count: state.runs.length,
            fallback_run_count: 0,
            best_overall: latestRun,
            latest_run: latestRun
          },
          daily_buckets: [{ date: "2026-04-18", run_count: state.runs.length }],
          recent_runs: state.runs
        }
      }
    });
  });

  await page.route("**/api/v1/admin/models/summary*", async (route) => {
    modelAdminRequests.summary.push(route.request().url());
    const latestRun = state.runs[0] ?? null;
    await route.fulfill({
      json: {
        repository_mode: "in_memory",
        filters: {
          target_task: "spread_error_regression"
        },
        model_summary: {
          run_count: state.runs.length,
          status_counts: { COMPLETED: state.runs.length },
          usable_run_count: state.runs.length,
          fallback_run_count: 0,
          best_overall: latestRun,
          latest_run: latestRun,
          best_by_family: {
            linear_feature: latestRun
          }
        }
      }
    });
  });

  await page.route("**/api/v1/admin/models/registry*", async (route) => {
    await route.fulfill({
      json: {
        repository_mode: "in_memory",
        filters: {
          target_task: "spread_error_regression"
        },
        model_registry_count: 1,
        model_registry: [registryEntry]
      }
    });
  });

  await page.route("**/api/v1/admin/models/runs*", async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname !== "/api/v1/admin/models/runs") {
      await route.fallback();
      return;
    }
    modelAdminRequests.runs.push(route.request().url());
    await route.fulfill({
      json: {
        repository_mode: "in_memory",
        filters: {
          target_task: url.searchParams.get("target_task") ?? "spread_error_regression"
        },
        model_run_count: state.runs.length,
        model_runs: state.runs
      }
    });
  });

  await page.route("**/api/v1/admin/models/runs/*", async (route) => {
    modelAdminRequests.runDetail.push(route.request().url());
    const runId = Number(route.request().url().split("/").pop()?.split("?")[0]);
    await route.fulfill({
      json: {
        repository_mode: "in_memory",
        model_run: state.runs.find((run) => run.id === runId) ?? null
      }
    });
  });

  await page.route("**/api/v1/admin/models/evaluations/history*", async (route) => {
    modelAdminRequests.evaluationHistory.push(route.request().url());
    const latestEvaluation = state.evaluations[0] ?? null;
    await route.fulfill({
      json: {
        repository_mode: "in_memory",
        filters: {
          target_task: "spread_error_regression",
          recent_limit: 8
        },
        model_evaluation_history: {
          overview: {
            snapshot_count: state.evaluations.length,
            fallback_strategy_counts: {},
            latest_snapshot: latestEvaluation
          },
          daily_buckets: [{ date: "2026-04-18", snapshot_count: state.evaluations.length }],
          recent_snapshots: state.evaluations
        }
      }
    });
  });

  await page.route("**/api/v1/admin/models/evaluations*", async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname !== "/api/v1/admin/models/evaluations") {
      await route.fallback();
      return;
    }
    modelAdminRequests.evaluations.push(route.request().url());
    await route.fulfill({
      json: {
        repository_mode: "in_memory",
        filters: {
          target_task: url.searchParams.get("target_task") ?? "spread_error_regression"
        },
        evaluation_snapshot_count: state.evaluations.length,
        evaluation_snapshots: state.evaluations
      }
    });
  });

  await page.route("**/api/v1/admin/models/evaluations/*", async (route) => {
    modelAdminRequests.evaluationDetail.push(route.request().url());
    const nextEvaluationId = Number(route.request().url().split("/").pop()?.split("?")[0]);
    await route.fulfill({
      json: {
        repository_mode: "in_memory",
        evaluation_snapshot: state.evaluations.find((entry) => entry.id === nextEvaluationId) ?? null
      }
    });
  });

  await page.route("**/api/v1/admin/models/selections/history*", async (route) => {
    modelAdminRequests.selectionHistory.push(route.request().url());
    const latestSelection = state.selections[0] ?? null;
    await route.fulfill({
      json: {
        repository_mode: "in_memory",
        filters: {
          target_task: "spread_error_regression",
          recent_limit: 8
        },
        model_selection_history: {
          overview: {
            selection_count: state.selections.length,
            active_selection_count: state.selections.filter((entry) => entry.is_active).length,
            model_family_counts: { linear_feature: state.selections.length },
            latest_selection: latestSelection
          },
          recent_selections: state.selections
        }
      }
    });
  });

  await page.route("**/api/v1/admin/models/selections*", async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname !== "/api/v1/admin/models/selections") {
      await route.fallback();
      return;
    }
    modelAdminRequests.selections.push(route.request().url());
    const activeOnly = url.searchParams.get("active_only") === "true";
    const selections = activeOnly ? state.selections.filter((entry) => entry.is_active) : state.selections;
    await route.fulfill({
      json: {
        repository_mode: "in_memory",
        filters: {
          target_task: url.searchParams.get("target_task") ?? "spread_error_regression",
          active_only: activeOnly
        },
        selection_count: selections.length,
        selections
      }
    });
  });

  await page.route("**/api/v1/admin/models/selections/*", async (route) => {
    modelAdminRequests.selectionDetail.push(route.request().url());
    const nextSelectionId = Number(route.request().url().split("/").pop()?.split("?")[0]);
    await route.fulfill({
      json: {
        repository_mode: "in_memory",
        selection: state.selections.find((entry) => entry.id === nextSelectionId) ?? null
      }
    });
  });

  await page.route("**/api/v1/admin/models/train*", async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname !== "/api/v1/admin/models/train") {
      await route.fallback();
      return;
    }
    modelAdminRequests.train.push(route.request().url());
    const trainedRun = createModelRun(302, {
      team_code: url.searchParams.get("team_code"),
      season_label: url.searchParams.get("season_label"),
      target_task: url.searchParams.get("target_task") ?? "spread_error_regression",
      created_at: "2026-04-19T00:00:00+00:00",
      completed_at: "2026-04-19T00:10:00+00:00"
    });
    const trainedEvaluation = createEvaluationSnapshot(402, trainedRun, {
      created_at: "2026-04-19T00:11:00+00:00"
    });
    state.runs = [trainedRun, ...state.runs];
    state.evaluations = [trainedEvaluation, ...state.evaluations];
    await route.fulfill({
      json: {
        repository_mode: "in_memory",
        filters: {
          target_task: trainedRun.target_task
        },
        feature_version: {
          feature_key: "baseline_team_features_v1",
          version_label: "v1"
        },
        dataset_row_count: 19,
        model_runs: [trainedRun],
        best_model: trainedRun,
        persisted_run_count: 1
      }
    });
  });

  await page.route("**/api/v1/admin/models/select*", async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname !== "/api/v1/admin/models/select") {
      await route.fallback();
      return;
    }
    modelAdminRequests.select.push(route.request().url());
    const selectedEvaluation = state.evaluations[0] ?? evaluationSnapshot;
    const selectedRun =
      state.runs.find((run) => run.id === selectedEvaluation.model_training_run_id) ?? state.runs[0] ?? modelRun;
    const promotedSelection = createSelectionSnapshot(502, selectedEvaluation, selectedRun, {
      created_at: "2026-04-19T00:12:00+00:00",
      selection_policy_name:
        url.searchParams.get("selection_policy_name") ?? "validation_mae_candidate_v1"
    });
    state.selections = [promotedSelection, ...state.selections.map((entry) => ({ ...entry, is_active: false }))];
    await route.fulfill({
      json: {
        repository_mode: "in_memory",
        filters: {
          target_task: url.searchParams.get("target_task") ?? "spread_error_regression"
        },
        selection_policy_name:
          url.searchParams.get("selection_policy_name") ?? "validation_mae_candidate_v1",
        selected_snapshot: selectedEvaluation,
        active_selection: promotedSelection,
        selection_count: state.selections.length
      }
    });
  });

  return {
    modelAdminRequests
  };
}

test.describe("Phase 5 browser smoke", () => {
  test("loads the core analyst routes in a real browser", async ({ page }) => {
    await stubPhaseFiveApis(page);
    const compareHash = `#/compare/backtests/${backtestRunId}/folds/${foldIndex}/opportunities/${opportunityId}`;

    await page.goto("#/backtests");
    await expect(page.getByRole("heading", { name: "Chronological validation" })).toBeVisible();
    await expect(page.getByText("Active run")).toBeVisible();

    await page.goto(`#/backtests/${backtestRunId}`);
    await expect(page.getByText("Backtest run", { exact: true })).toBeVisible();
    await expect(page.locator("article >> h2").filter({ hasText: backtestRun.strategy_name })).toBeVisible();
    await expect(page.getByRole("link", { name: `Run #${backtestRunId}` })).toBeVisible();

    await page.goto(`#/backtests/${backtestRunId}/folds/${foldIndex}`);
    await expect(page.getByText("Fold detail", { exact: true })).toBeVisible();
    await expect(page.getByRole("heading", { name: `Fold ${foldIndex}` })).toBeVisible();

    await page.goto(`#/opportunities/${opportunityId}`);
    await expect(page.getByText("Opportunity detail", { exact: true })).toBeVisible();
    await expect(page.getByRole("heading", { name: /LAL vs BOS/i })).toBeVisible();

    const comparableCard = page.getByRole("button").filter({ hasText: /Game 88/ }).first();
    await expect(comparableCard).toBeVisible();

    await comparableCard.click();
    await expect(page.getByText("Comparable case", { exact: true })).toBeVisible();
    await expect(page.getByRole("heading", { name: /Game 88 \|/i })).toBeVisible();

    await page.goto(compareHash);
    await expect(page.getByText("Artifact comparison", { exact: true })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Fold vs opportunity evidence" })).toBeVisible();
    await expect(page.getByText("Decision summary", { exact: true })).toBeVisible();
  });

  test("loads the model admin routes and artifact details in a real browser", async ({ page }) => {
    await stubPhaseFiveApis(page);
    await page.goto("#/models");
    await expect(page.getByRole("heading", { name: "Recent training activity" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Inspect artifacts" })).toBeVisible();

    await page.goto("#/models/registry");
    await expect(page.getByRole("heading", { name: "Model registry entries" })).toBeVisible();
    await expect(page.getByText("Registry detail", { exact: true })).toBeVisible();

    await page.goto("#/models/runs");
    await expect(page.getByRole("heading", { name: "Run history" })).toBeVisible();

    await page.goto(`#/models/runs/${modelRunId}`);
    await expect(page.getByRole("heading", { name: `Run #${modelRunId}` })).toBeVisible();
    await expect(page.getByText("Artifact summary", { exact: true })).toBeVisible();

    await page.goto("#/models/evaluations");
    await expect(page.getByRole("heading", { name: "Evaluation snapshots" })).toBeVisible();

    await page.goto(`#/models/evaluations/${evaluationId}`);
    await expect(page.getByRole("heading", { name: `Evaluation #${evaluationId}` })).toBeVisible();
    await expect(page.getByText("Prediction counts", { exact: true })).toBeVisible();

    await page.goto("#/models/selections");
    await expect(page.getByText("Selection snapshots", { exact: true })).toBeVisible();

    await page.goto(`#/models/selections/${selectionId}`);
    await expect(page.locator("article.focus-panel h2").filter({ hasText: `Selection #${selectionId}` })).toBeVisible();
    await expect(page.getByText("Rationale payload", { exact: true })).toBeVisible();
  });

  test("runs train and select mutations with browser-level refresh verification", async ({ page }) => {
    const { modelAdminRequests } = await stubPhaseFiveApis(page);

    await page.goto("#/models");
    await expect(page.getByRole("heading", { name: "Recent training activity" })).toBeVisible();

    const trainPanel = page.locator("section.action-panel");
    await trainPanel.getByRole("button", { name: "Train model" }).click();
    await trainPanel.getByLabel("Team code").fill("NYK");
    await trainPanel.getByLabel("Season label").fill("2026-2027");
    await trainPanel.getByRole("button", { name: "Start training" }).click();

    await expect(page).toHaveURL(/#\/models\/runs\/302$/);
    await expect(page.getByRole("heading", { name: "Run #302" })).toBeVisible();
    await expect.poll(() => modelAdminRequests.train.length).toBe(1);
    await expect.poll(() => modelAdminRequests.history.length).toBeGreaterThan(1);
    await expect.poll(() => modelAdminRequests.summary.length).toBeGreaterThan(1);
    await expect.poll(() => modelAdminRequests.runs.length).toBeGreaterThan(0);
    await expect.poll(() => modelAdminRequests.evaluations.length).toBeGreaterThan(0);
    await expect.poll(() => modelAdminRequests.runDetail.some((url) => url.includes("/302"))).toBeTruthy();

    await page.goto("#/models/runs");
    await expect(page.getByRole("cell", { name: "#302" })).toBeVisible();

    await page.goto("#/models/evaluations");
    await expect(page.getByRole("cell", { name: "#402" })).toBeVisible();

    await page.goto("#/models/selections");
    await expect(page.getByText("Selection snapshots", { exact: true })).toBeVisible();
    const selectionRequestsBefore = modelAdminRequests.selections.length;
    const evaluationRequestsBeforeSelection = modelAdminRequests.evaluations.length;

    const selectionPanel = page.locator("section.action-panel");
    await selectionPanel.getByRole("button", { name: "Select best model" }).click();
    await selectionPanel.getByRole("button", { name: "Promote best model" }).click();

    await expect(page).toHaveURL(/#\/models\/selections\/502$/);
    await expect(
      page.locator("article.focus-panel h2").filter({ hasText: "Selection #502" })
    ).toBeVisible();
    await expect.poll(() => modelAdminRequests.select.length).toBe(1);
    await expect.poll(() => modelAdminRequests.selections.length).toBeGreaterThan(selectionRequestsBefore);
    await expect.poll(() => modelAdminRequests.evaluations.length).toBeGreaterThan(
      evaluationRequestsBeforeSelection
    );
    await expect.poll(() => modelAdminRequests.selectionDetail.some((url) => url.includes("/502"))).toBeTruthy();

    await page.goto("#/models/selections");
    await expect(page.getByRole("cell", { name: "#502" })).toBeVisible();
  });
});
