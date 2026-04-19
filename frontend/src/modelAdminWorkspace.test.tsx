// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ModelAdminWorkspace } from "./modelAdminWorkspace";

vi.mock("./api", () => ({
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
    repository_mode: "in_memory",
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
    repository_mode: "in_memory",
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
    repository_mode: "in_memory",
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
    repository_mode: "in_memory",
    filters: {
      target_task: "spread_error_regression"
    },
    model_run_count: 0,
    model_runs: []
  });
  vi.mocked(fetchModelAdminEvaluations).mockResolvedValue({
    repository_mode: "in_memory",
    filters: {
      target_task: "spread_error_regression"
    },
    evaluation_snapshot_count: 0,
    evaluation_snapshots: []
  });
  vi.mocked(fetchModelAdminSelections).mockResolvedValue({
    repository_mode: "in_memory",
    filters: {
      target_task: "spread_error_regression",
      active_only: false
    },
    selection_count: 0,
    selections: []
  });
  vi.mocked(fetchModelAdminRunDetail).mockResolvedValue({
    repository_mode: "in_memory",
    model_run: null
  });
  vi.mocked(fetchModelAdminEvaluationDetail).mockResolvedValue({
    repository_mode: "in_memory",
    evaluation_snapshot: null
  });
  vi.mocked(fetchModelAdminSelectionDetail).mockResolvedValue({
    repository_mode: "in_memory",
    selection: null
  });
  vi.mocked(fetchModelAdminRegistry).mockResolvedValue({
    repository_mode: "in_memory",
    filters: {
      target_task: "spread_error_regression"
    },
    model_registry_count: 0,
    model_registry: []
  });
  vi.mocked(trainModels).mockResolvedValue({
    repository_mode: "in_memory",
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
    repository_mode: "in_memory",
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
      repository_mode: "in_memory",
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
      repository_mode: "in_memory",
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
      repository_mode: "in_memory",
      filters: { target_task: "spread_error_regression", recent_limit: 8 },
      model_evaluation_history: {
        overview: { snapshot_count: 0, latest_snapshot: null },
        daily_buckets: [],
        recent_snapshots: []
      }
    });
    selectionHistoryRequest.resolve({
      repository_mode: "in_memory",
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
      repository_mode: "in_memory",
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
      repository_mode: "in_memory",
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
      repository_mode: "in_memory",
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
      repository_mode: "in_memory",
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
      repository_mode: "in_memory",
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
      repository_mode: "in_memory",
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
      repository_mode: "in_memory",
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
      repository_mode: "in_memory",
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
      repository_mode: "in_memory",
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
    expect(vi.mocked(fetchModelAdminSelections)).toHaveBeenLastCalledWith({
      activeOnly: false,
      targetTask: "spread_error_regression"
    });

    fireEvent.change(screen.getByLabelText("Target task"), {
      target: { value: "custom_target_task" }
    });
    fireEvent.click(screen.getByLabelText("Active selections only"));
    fireEvent.click(screen.getByRole("button", { name: "Apply filters" }));

    await waitFor(() =>
      expect(vi.mocked(fetchModelAdminSelections)).toHaveBeenLastCalledWith({
        activeOnly: true,
        targetTask: "custom_target_task"
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
