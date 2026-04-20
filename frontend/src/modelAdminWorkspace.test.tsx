// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ModelAdminWorkspace } from "./modelAdminWorkspace";

vi.mock("./api", () => ({
  fetchModelCapabilities: vi.fn(),
  fetchModelAdminEvaluationDetail: vi.fn(),
  fetchModelAdminEvaluationHistory: vi.fn(),
  fetchModelAdminEvaluations: vi.fn(),
  fetchModelAdminHistory: vi.fn(),
  fetchModelAdminRegistry: vi.fn(),
  fetchModelAdminRunDetail: vi.fn(),
  fetchModelAdminRuns: vi.fn(),
  fetchModelAdminSelectionDetail: vi.fn(),
  fetchModelAdminSelectionHistory: vi.fn(),
  fetchModelAdminSelections: vi.fn(),
  fetchModelAdminSummary: vi.fn(),
  selectBestModel: vi.fn(),
  trainModels: vi.fn()
}));

import {
  fetchModelCapabilities,
  fetchModelAdminEvaluationDetail,
  fetchModelAdminEvaluationHistory,
  fetchModelAdminHistory,
  fetchModelAdminRunDetail,
  fetchModelAdminRuns,
  fetchModelAdminSelectionHistory,
  fetchModelAdminSummary,
  fetchModelAdminEvaluations,
  fetchModelAdminSelections,
  fetchModelAdminSelectionDetail,
  fetchModelAdminRegistry,
  selectBestModel,
  trainModels
} from "./api";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

beforeEach(() => {
  vi.mocked(fetchModelCapabilities).mockResolvedValue({
    task_count: 2,
    target_tasks: [
      {
        task_key: "spread_error_regression",
        task_kind: "regression",
        label: "Spread Error",
        description: "Spread task",
        market_type: "spread",
        primary_metric_name: "mae",
        metric_direction: "lower_is_better",
        supported_model_families: ["linear_feature", "tree_stump"],
        default_selection_policy_name: "validation_mae_candidate_v1",
        valid_selection_policy_names: ["validation_mae_candidate_v1"],
        default_opportunity_policy_name: "spread_signal_v1",
        workflow_support: { training: true, selection: true, scoring: true },
        is_enabled: true,
        config: {}
      },
      {
        task_key: "total_points_regression",
        task_kind: "regression",
        label: "Total Points",
        description: "Totals task",
        market_type: "total",
        primary_metric_name: "mae",
        metric_direction: "lower_is_better",
        supported_model_families: ["linear_feature"],
        default_selection_policy_name: "validation_mae_candidate_v1",
        valid_selection_policy_names: ["validation_mae_candidate_v1"],
        default_opportunity_policy_name: "totals_signal_v1",
        workflow_support: { training: true, selection: true, scoring: true },
        is_enabled: true,
        config: {}
      }
    ],
    ui_defaults: {
      default_feature_key: "baseline_team_features_v1",
      default_target_task: "spread_error_regression",
      default_train_ratio: 0.7,
      default_validation_ratio: 0.15
    }
  });
  vi.mocked(fetchModelAdminHistory).mockResolvedValue({
    filters: {
      target_task: "spread_error_regression",
      team_code: null,
      season_label: null,
      recent_limit: 8
    },
    model_history: {
      overview: {
        run_count: 1,
        fallback_run_count: 0,
        best_overall: null,
        latest_run: null
      },
      daily_buckets: [],
      recent_runs: []
    }
  });
  vi.mocked(fetchModelAdminSummary).mockResolvedValue({
    filters: {
      target_task: "spread_error_regression"
    },
    model_summary: {
      run_count: 1,
      status_counts: { COMPLETED: 1 },
      fallback_run_count: 0,
      best_overall: null,
      latest_run: null
    }
  });
  vi.mocked(fetchModelAdminEvaluationHistory).mockResolvedValue({
    filters: {
      target_task: "spread_error_regression",
      recent_limit: 8
    },
    model_evaluation_history: {
      overview: {
        snapshot_count: 0,
        latest_snapshot: null
      },
      daily_buckets: [],
      recent_snapshots: []
    }
  });
  vi.mocked(fetchModelAdminSelectionHistory).mockResolvedValue({
    filters: {
      target_task: "spread_error_regression",
      recent_limit: 8
    },
    model_selection_history: {
      overview: {
        selection_count: 0,
        active_selection_count: 0,
        latest_selection: null
      },
      recent_selections: []
    }
  });
  vi.mocked(fetchModelAdminRuns).mockResolvedValue({
    filters: {
      target_task: "spread_error_regression"
    },
    model_run_count: 0,
    model_runs: []
  });
  vi.mocked(fetchModelAdminEvaluations).mockResolvedValue({
    filters: {
      target_task: "spread_error_regression"
    },
    evaluation_snapshot_count: 0,
    evaluation_snapshots: []
  });
  vi.mocked(fetchModelAdminSelections).mockResolvedValue({
    filters: {
      target_task: "spread_error_regression",
      active_only: false
    },
    selection_count: 0,
    selections: []
  });
  vi.mocked(fetchModelAdminRunDetail).mockResolvedValue({
    model_run: null
  });
  vi.mocked(fetchModelAdminEvaluationDetail).mockResolvedValue({
    evaluation_snapshot: null
  });
  vi.mocked(fetchModelAdminSelectionDetail).mockResolvedValue({
    selection: null
  });
  vi.mocked(fetchModelAdminRegistry).mockResolvedValue({
    filters: {
      target_task: "spread_error_regression"
    },
    model_registry_count: 0,
    model_registry: []
  });
  vi.mocked(trainModels).mockResolvedValue({
    filters: {
      target_task: "spread_error_regression"
    },
    feature_version: {
      feature_key: "baseline_team_features_v1"
    },
    dataset_row_count: 19,
    model_runs: [],
    best_model: null,
    persisted_run_count: 0
  });
  vi.mocked(selectBestModel).mockResolvedValue({
    filters: {
      target_task: "spread_error_regression"
    },
    selection_policy_name: "validation_mae_candidate_v1",
    selected_snapshot: null,
    active_selection: null,
    selection_count: 0
  });
});

describe("ModelAdminWorkspace", () => {
  it("shows the loading banner while dashboard requests are in flight", async () => {
    const historyRequest = deferred<Awaited<ReturnType<typeof fetchModelAdminHistory>>>();
    const summaryRequest = deferred<Awaited<ReturnType<typeof fetchModelAdminSummary>>>();
    const evaluationHistoryRequest = deferred<
      Awaited<ReturnType<typeof fetchModelAdminEvaluationHistory>>
    >();
    const selectionHistoryRequest = deferred<
      Awaited<ReturnType<typeof fetchModelAdminSelectionHistory>>
    >();

    vi.mocked(fetchModelAdminHistory).mockReturnValueOnce(historyRequest.promise);
    vi.mocked(fetchModelAdminSummary).mockReturnValueOnce(summaryRequest.promise);
    vi.mocked(fetchModelAdminEvaluationHistory).mockReturnValueOnce(
      evaluationHistoryRequest.promise
    );
    vi.mocked(fetchModelAdminSelectionHistory).mockReturnValueOnce(
      selectionHistoryRequest.promise
    );

    render(
      <ModelAdminWorkspace
        modelHistory={null}
        onNavigate={vi.fn()}
        route={{ name: "models" }}
      />
    );

    await screen.findByText("Loading Model Admin workspace...");

    historyRequest.resolve({
      filters: {
        target_task: "spread_error_regression",
        team_code: null,
        season_label: null,
        recent_limit: 8
      },
      model_history: {
        overview: {
          run_count: 1,
          fallback_run_count: 0,
          best_overall: null,
          latest_run: null
        },
        daily_buckets: [],
        recent_runs: []
      }
    });
    summaryRequest.resolve({
      filters: { target_task: "spread_error_regression" },
      model_summary: {
        run_count: 1,
        status_counts: { COMPLETED: 1 },
        fallback_run_count: 0,
        best_overall: null,
        latest_run: null
      }
    });
    evaluationHistoryRequest.resolve({
      filters: { target_task: "spread_error_regression", recent_limit: 8 },
      model_evaluation_history: {
        overview: { snapshot_count: 0, latest_snapshot: null },
        daily_buckets: [],
        recent_snapshots: []
      }
    });
    selectionHistoryRequest.resolve({
      filters: { target_task: "spread_error_regression", recent_limit: 8 },
      model_selection_history: {
        overview: {
          selection_count: 0,
          active_selection_count: 0,
          latest_selection: null
        },
        recent_selections: []
      }
    });

    await screen.findByRole("heading", { name: "Recent training activity" });
  });

  it("loads task options from model capabilities into the admin actions form", async () => {
    render(
      <ModelAdminWorkspace
        modelHistory={null}
        onNavigate={vi.fn()}
        route={{ name: "models" }}
      />
    );

    await screen.findByRole("heading", { name: "Recent training activity" });
    fireEvent.click(screen.getByRole("button", { name: "Train model" }));

    const targetTaskSelect = screen.getByTestId("train-target-task");
    expect(screen.getAllByRole("option", { name: "Spread Error" }).length).toBeGreaterThan(0);
    expect(screen.getAllByRole("option", { name: "Total Points" }).length).toBeGreaterThan(0);
    expect((targetTaskSelect as HTMLSelectElement).value).toBe("spread_error_regression");
  });

  it("shows an error banner when dashboard loading fails", async () => {
    vi.mocked(fetchModelAdminHistory).mockRejectedValueOnce(new Error("Dashboard unavailable"));

    render(
      <ModelAdminWorkspace
        modelHistory={null}
        onNavigate={vi.fn()}
        route={{ name: "models" }}
      />
    );

    await screen.findByText("Dashboard unavailable");
  });

  it("shows a success mutation banner after training and exposes the follow-up action", async () => {
    const onNavigate = vi.fn();
    vi.mocked(trainModels).mockResolvedValueOnce({
      filters: {
        target_task: "spread_error_regression"
      },
      feature_version: {
        feature_key: "baseline_team_features_v1"
      },
      dataset_row_count: 19,
      model_runs: [
        {
          id: 301,
          model_registry_id: 11,
          feature_version_id: 21,
          target_task: "spread_error_regression",
          team_code: null,
          season_label: null,
          status: "COMPLETED",
          train_ratio: 0.7,
          validation_ratio: 0.15,
          artifact: {
            model_family: "linear_feature"
          },
          metrics: {},
          created_at: "2026-04-18T00:00:00+00:00",
          completed_at: "2026-04-18T00:10:00+00:00"
        }
      ],
      best_model: {
        id: 301,
        model_registry_id: 11,
        feature_version_id: 21,
        target_task: "spread_error_regression",
        team_code: null,
        season_label: null,
        status: "COMPLETED",
        train_ratio: 0.7,
        validation_ratio: 0.15,
        artifact: {
          model_family: "linear_feature"
        },
        metrics: {},
        created_at: "2026-04-18T00:00:00+00:00",
        completed_at: "2026-04-18T00:10:00+00:00"
      },
      persisted_run_count: 1
    });
    vi.mocked(fetchModelAdminRuns).mockResolvedValueOnce({
      filters: {
        target_task: "spread_error_regression"
      },
      model_run_count: 1,
      model_runs: [
        {
          id: 301,
          model_registry_id: 11,
          feature_version_id: 21,
          target_task: "spread_error_regression",
          team_code: null,
          season_label: null,
          status: "COMPLETED",
          train_ratio: 0.7,
          validation_ratio: 0.15,
          artifact: {
            model_family: "linear_feature"
          },
          metrics: {},
          created_at: "2026-04-18T00:00:00+00:00",
          completed_at: "2026-04-18T00:10:00+00:00"
        }
      ]
    });
    vi.mocked(fetchModelAdminRunDetail).mockResolvedValueOnce({
      model_run: {
        id: 301,
        model_registry_id: 11,
        feature_version_id: 21,
        target_task: "spread_error_regression",
        team_code: null,
        season_label: null,
        status: "COMPLETED",
        train_ratio: 0.7,
        validation_ratio: 0.15,
        artifact: {
          model_family: "linear_feature"
        },
        metrics: {},
        created_at: "2026-04-18T00:00:00+00:00",
        completed_at: "2026-04-18T00:10:00+00:00"
      }
    });

    render(
      <ModelAdminWorkspace
        modelHistory={null}
        onNavigate={onNavigate}
        route={{ name: "models" }}
      />
    );

    await screen.findByRole("heading", { name: "Recent training activity" });
    fireEvent.click(screen.getByRole("button", { name: "Train model" }));
    fireEvent.click(screen.getByRole("button", { name: "Start training" }));

    await screen.findByText("Training completed. 1 run(s) are now available.");
    expect(screen.getByRole("button", { name: "Open run detail" })).not.toBeNull();
    await waitFor(() =>
      expect(vi.mocked(fetchModelAdminRuns)).toHaveBeenLastCalledWith({
        seasonLabel: null,
        targetTask: "spread_error_regression",
        teamCode: null
      })
    );
    await waitFor(() =>
      expect(vi.mocked(fetchModelAdminEvaluations)).toHaveBeenLastCalledWith({
        targetTask: "spread_error_regression"
      })
    );
    await waitFor(() =>
      expect(onNavigate).toHaveBeenCalledWith({ name: "model-run-detail", runId: 301 })
    );
  });

  it("shows a success mutation banner after selection and exposes the follow-up action", async () => {
    const onNavigate = vi.fn();
    vi.mocked(selectBestModel).mockResolvedValueOnce({
      filters: {
        target_task: "spread_error_regression"
      },
      selection_policy_name: "validation_mae_candidate_v1",
      selected_snapshot: null,
      active_selection: {
        id: 501,
        model_evaluation_snapshot_id: 401,
        model_training_run_id: 301,
        model_registry_id: 11,
        feature_version_id: 21,
        target_task: "spread_error_regression",
        model_family: "linear_feature",
        selection_policy_name: "validation_mae_candidate_v1",
        rationale: {
          reason: "lowest_validation_mae"
        },
        is_active: true,
        created_at: "2026-04-18T00:12:00+00:00"
      },
      selection_count: 1
    });
    vi.mocked(fetchModelAdminSelections).mockResolvedValueOnce({
      filters: {
        target_task: "spread_error_regression",
        active_only: false
      },
      selection_count: 1,
      selections: [
        {
          id: 501,
          model_evaluation_snapshot_id: 401,
          model_training_run_id: 301,
          model_registry_id: 11,
          feature_version_id: 21,
          target_task: "spread_error_regression",
          model_family: "linear_feature",
          selection_policy_name: "validation_mae_candidate_v1",
          rationale: {
            reason: "lowest_validation_mae"
          },
          is_active: true,
          created_at: "2026-04-18T00:12:00+00:00"
        }
      ]
    });
    vi.mocked(fetchModelAdminSelectionDetail).mockResolvedValueOnce({
      selection: {
        id: 501,
        model_evaluation_snapshot_id: 401,
        model_training_run_id: 301,
        model_registry_id: 11,
        feature_version_id: 21,
        target_task: "spread_error_regression",
        model_family: "linear_feature",
        selection_policy_name: "validation_mae_candidate_v1",
        rationale: {
          reason: "lowest_validation_mae"
        },
        is_active: true,
        created_at: "2026-04-18T00:12:00+00:00"
      }
    });

    render(
      <ModelAdminWorkspace
        modelHistory={null}
        onNavigate={onNavigate}
        route={{ name: "model-selections" }}
      />
    );

    await screen.findByRole("heading", { name: "Selection snapshots" });
    fireEvent.click(screen.getByRole("button", { name: "Select best model" }));
    fireEvent.click(screen.getByRole("button", { name: "Promote best model" }));

    await screen.findByText(
      "Promotion completed using validation_mae_candidate_v1. The active selection is now updated."
    );
    expect(screen.getByRole("button", { name: "Open selection detail" })).not.toBeNull();
    await waitFor(() =>
      expect(vi.mocked(fetchModelAdminSelections)).toHaveBeenLastCalledWith({
        activeOnly: false,
        targetTask: "spread_error_regression"
      })
    );
    await waitFor(() =>
      expect(vi.mocked(fetchModelAdminEvaluations)).toHaveBeenLastCalledWith({
        modelFamily: null,
        targetTask: "spread_error_regression"
      })
    );
    await waitFor(() =>
      expect(onNavigate).toHaveBeenCalledWith({
        name: "model-selection-detail",
        selectionId: 501
      })
    );
  });

  it("reloads section data when the workspace route changes", async () => {
    const { rerender } = render(
      <ModelAdminWorkspace
        modelHistory={null}
        onNavigate={vi.fn()}
        route={{ name: "models" }}
      />
    );

    await screen.findByRole("heading", { name: "Recent training activity" });

    vi.mocked(fetchModelAdminRuns).mockResolvedValueOnce({
      filters: {
        target_task: "spread_error_regression"
      },
      model_run_count: 1,
      model_runs: [
        {
          id: 301,
          model_registry_id: 11,
          feature_version_id: 21,
          target_task: "spread_error_regression",
          team_code: "LAL",
          season_label: "2025-2026",
          status: "COMPLETED",
          train_ratio: 0.7,
          validation_ratio: 0.15,
          artifact: {
            model_family: "linear_feature"
          },
          metrics: {},
          created_at: "2026-04-18T00:00:00+00:00",
          completed_at: "2026-04-18T00:10:00+00:00"
        }
      ]
    });

    rerender(
      <ModelAdminWorkspace
        modelHistory={null}
        onNavigate={vi.fn()}
        route={{ name: "model-runs" }}
      />
    );

    await screen.findByRole("heading", { name: "Run history" });
    await screen.findByText("#301");

    vi.mocked(fetchModelAdminSelectionDetail).mockResolvedValueOnce({
      selection: {
        id: 501,
        model_evaluation_snapshot_id: 401,
        model_training_run_id: 301,
        model_registry_id: 11,
        feature_version_id: 21,
        target_task: "spread_error_regression",
        model_family: "linear_feature",
        selection_policy_name: "validation_mae_candidate_v1",
        rationale: {
          reason: "lowest_validation_mae"
        },
        is_active: true,
        created_at: "2026-04-18T00:12:00+00:00"
      }
    });
    vi.mocked(fetchModelAdminSelections).mockResolvedValueOnce({
      filters: {
        target_task: "spread_error_regression",
        active_only: false
      },
      selection_count: 1,
      selections: [
        {
          id: 501,
          model_evaluation_snapshot_id: 401,
          model_training_run_id: 301,
          model_registry_id: 11,
          feature_version_id: 21,
          target_task: "spread_error_regression",
          model_family: "linear_feature",
          selection_policy_name: "validation_mae_candidate_v1",
          rationale: {
            reason: "lowest_validation_mae"
          },
          is_active: true,
          created_at: "2026-04-18T00:12:00+00:00"
        }
      ]
    });

    rerender(
      <ModelAdminWorkspace
        modelHistory={null}
        onNavigate={vi.fn()}
        route={{ name: "model-selection-detail", selectionId: 501 }}
      />
    );

    await screen.findByRole("heading", { name: "Selection #501" });
    expect(vi.mocked(fetchModelAdminSelectionDetail)).toHaveBeenCalledWith(501);
  });

  it("resets selection filters back to the default query scope", async () => {
    render(
      <ModelAdminWorkspace
        modelHistory={null}
        onNavigate={vi.fn()}
        route={{ name: "model-selections" }}
      />
    );

    await screen.findByRole("heading", { name: "Selection snapshots" });
    await waitFor(() =>
      expect(vi.mocked(fetchModelAdminSelections)).toHaveBeenLastCalledWith({
        activeOnly: false,
        targetTask: "spread_error_regression"
      })
    );

    fireEvent.change(screen.getByLabelText("Target task"), {
      target: { value: "total_points_regression" }
    });
    fireEvent.click(screen.getByLabelText("Active selections only"));
    fireEvent.click(screen.getByRole("button", { name: "Apply filters" }));

    await waitFor(() =>
      expect(vi.mocked(fetchModelAdminSelections)).toHaveBeenLastCalledWith({
        activeOnly: true,
        targetTask: "total_points_regression"
      })
    );

    fireEvent.click(screen.getByRole("button", { name: "Reset" }));

    await waitFor(() =>
      expect(vi.mocked(fetchModelAdminSelections)).toHaveBeenLastCalledWith({
        activeOnly: false,
        targetTask: "spread_error_regression"
      })
    );
  });
});
