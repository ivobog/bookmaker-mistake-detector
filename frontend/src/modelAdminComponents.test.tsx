// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ModelAdminActionsPanel } from "./modelAdminActions";
import {
  ModelAdminEvaluationDetailCard,
  ModelRegistryDetailCard,
  ModelAdminRunDetailCard,
  ModelAdminSelectionDetailCard
} from "./modelAdminDetailComponents";
import { ModelRunsPage, ModelSelectionsPage } from "./modelAdminPages";

afterEach(() => {
  cleanup();
});

describe("ModelAdminActionsPanel", () => {
  it("shows validation feedback instead of submitting invalid train input", async () => {
    const onTrainSubmit = vi.fn().mockResolvedValue(undefined);

    render(
      <ModelAdminActionsPanel
        busyAction={null}
        defaultSeasonLabel=""
        defaultTargetTask="spread_error_regression"
        defaultTeamCode=""
        enableSelect={false}
        enableTrain
        onMaterializeFeaturesSubmit={vi.fn().mockResolvedValue(undefined)}
        onSelectSubmit={vi.fn().mockResolvedValue(undefined)}
        onTrainSubmit={onTrainSubmit}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Train model" }));
    fireEvent.change(screen.getByLabelText("Train ratio"), { target: { value: "0.90" } });
    fireEvent.change(screen.getByLabelText("Validation ratio"), { target: { value: "0.10" } });
    fireEvent.click(screen.getByRole("button", { name: "Start training" }));

    await screen.findByText("Train ratio plus validation ratio must leave room for a test split.");
    expect(onTrainSubmit).not.toHaveBeenCalled();
  });

  it("submits normalized training input and closes the train panel after success", async () => {
    const onTrainSubmit = vi.fn().mockResolvedValue(undefined);

    render(
      <ModelAdminActionsPanel
        busyAction={null}
        defaultSeasonLabel="2025-2026"
        defaultTargetTask="spread_error_regression"
        defaultTeamCode="LAL"
        enableSelect={false}
        enableTrain
        onMaterializeFeaturesSubmit={vi.fn().mockResolvedValue(undefined)}
        onSelectSubmit={vi.fn().mockResolvedValue(undefined)}
        onTrainSubmit={onTrainSubmit}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Train model" }));
    fireEvent.change(screen.getByLabelText("Feature key"), {
      target: { value: " baseline_team_features_v1 " }
    });
    fireEvent.change(screen.getByLabelText("Target task"), {
      target: { value: " spread_error_regression " }
    });
    fireEvent.change(screen.getByLabelText("Team code"), { target: { value: " " } });
    fireEvent.change(screen.getByLabelText("Season label"), { target: { value: " 2026-2027 " } });
    fireEvent.change(screen.getByLabelText("Train ratio"), { target: { value: "0.70" } });
    fireEvent.change(screen.getByLabelText("Validation ratio"), { target: { value: "0.15" } });
    fireEvent.click(screen.getByRole("button", { name: "Start training" }));

    await waitFor(() =>
      expect(onTrainSubmit).toHaveBeenCalledWith({
        featureKey: "baseline_team_features_v1",
        seasonLabel: "2026-2027",
        targetTask: "spread_error_regression",
        teamCode: null,
        trainRatio: 0.7,
        validationRatio: 0.15
      })
    );
    expect(screen.queryByRole("button", { name: "Start training" })).toBeNull();
  });

  it("reflects busy select state in the submit button", () => {
    render(
      <ModelAdminActionsPanel
        busyAction="select"
        defaultSeasonLabel=""
        defaultTargetTask="spread_error_regression"
        defaultTeamCode=""
        enableSelect
        enableTrain={false}
        onMaterializeFeaturesSubmit={vi.fn().mockResolvedValue(undefined)}
        onSelectSubmit={vi.fn().mockResolvedValue(undefined)}
        onTrainSubmit={vi.fn().mockResolvedValue(undefined)}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Select best model" }));
    const submitButton = screen.getByRole("button", { name: "Selecting model..." });
    expect(submitButton).toHaveProperty("disabled", true);
  });

  it("surfaces selection validation errors before submit", async () => {
    render(
      <ModelAdminActionsPanel
        busyAction={null}
        defaultSeasonLabel=""
        defaultTargetTask="spread_error_regression"
        defaultTeamCode=""
        enableSelect
        enableTrain={false}
        onMaterializeFeaturesSubmit={vi.fn().mockResolvedValue(undefined)}
        onSelectSubmit={vi.fn().mockResolvedValue(undefined)}
        onTrainSubmit={vi.fn().mockResolvedValue(undefined)}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Select best model" }));
    fireEvent.change(screen.getByLabelText("Selection policy"), { target: { value: " " } });
    fireEvent.click(screen.getByRole("button", { name: "Promote best model" }));

    await screen.findByText("Selection policy is required.");
  });

  it("submits normalized feature key and closes the materialize panel after success", async () => {
    const onMaterializeFeaturesSubmit = vi.fn().mockResolvedValue(undefined);

    render(
      <ModelAdminActionsPanel
        busyAction={null}
        defaultSeasonLabel=""
        defaultTargetTask="spread_error_regression"
        defaultTeamCode=""
        enableMaterializeFeatures
        enableSelect={false}
        enableTrain={false}
        onMaterializeFeaturesSubmit={onMaterializeFeaturesSubmit}
        onSelectSubmit={vi.fn().mockResolvedValue(undefined)}
        onTrainSubmit={vi.fn().mockResolvedValue(undefined)}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Materialize features" }));
    fireEvent.change(screen.getByLabelText("Feature key"), {
      target: { value: " baseline_team_features_v1 " }
    });
    fireEvent.click(screen.getByRole("button", { name: "Materialize snapshots" }));

    await waitFor(() =>
      expect(onMaterializeFeaturesSubmit).toHaveBeenCalledWith("baseline_team_features_v1")
    );
    expect(screen.queryByRole("button", { name: "Materialize snapshots" })).toBeNull();
  });

  it("shows materialization validation feedback instead of submitting an empty feature key", async () => {
    const onMaterializeFeaturesSubmit = vi.fn().mockResolvedValue(undefined);

    render(
      <ModelAdminActionsPanel
        busyAction={null}
        defaultSeasonLabel=""
        defaultTargetTask="spread_error_regression"
        defaultTeamCode=""
        enableMaterializeFeatures
        enableSelect={false}
        enableTrain={false}
        onMaterializeFeaturesSubmit={onMaterializeFeaturesSubmit}
        onSelectSubmit={vi.fn().mockResolvedValue(undefined)}
        onTrainSubmit={vi.fn().mockResolvedValue(undefined)}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Materialize features" }));
    fireEvent.change(screen.getByLabelText("Feature key"), { target: { value: " " } });
    fireEvent.click(screen.getByRole("button", { name: "Materialize snapshots" }));

    await screen.findByText("Feature key is required.");
    expect(onMaterializeFeaturesSubmit).not.toHaveBeenCalled();
  });
});

describe("Model Admin empty states", () => {
  it("renders the run history empty state and default detail placeholder", () => {
    render(
      <ModelRunsPage
        detailContent={undefined}
        onNavigate={vi.fn()}
        runs={[]}
        selectedRunId={null}
      />
    );

    expect(screen.getByText("No training runs were found for the current filters.")).not.toBeNull();
    expect(screen.getByText("Open a run to inspect artifact and metric details.")).not.toBeNull();
  });

  it("renders the selection history empty state and default detail placeholder", () => {
    render(
      <ModelSelectionsPage
        detailContent={undefined}
        onNavigate={vi.fn()}
        selectedSelectionId={null}
        selections={[]}
      />
    );

    expect(screen.getByText("No selection snapshots were found for the current filters.")).not.toBeNull();
    expect(screen.getByText("Open a selection snapshot to inspect promotion rationale.")).not.toBeNull();
  });
});

describe("Model Admin detail cards", () => {
  it("renders null-detail placeholders for every artifact card variant", () => {
    const { rerender } = render(<ModelRegistryDetailCard entry={null} />);
    expect(screen.getByRole("heading", { name: "Registry entry not available" })).not.toBeNull();
    expect(
      screen.getByText("Choose a registry row to inspect its configuration and metadata.")
    ).not.toBeNull();

    rerender(<ModelAdminRunDetailCard run={null} />);
    expect(screen.getByRole("heading", { name: "Training run not available" })).not.toBeNull();
    expect(screen.getByText("Open a run from the list to inspect its artifact and metrics.")).not.toBeNull();

    rerender(<ModelAdminEvaluationDetailCard evaluation={null} />);
    expect(screen.getByRole("heading", { name: "Evaluation snapshot not available" })).not.toBeNull();
    expect(
      screen.getByText("Open an evaluation snapshot from the list to inspect its metrics.")
    ).not.toBeNull();

    rerender(<ModelAdminSelectionDetailCard selection={null} />);
    expect(screen.getByRole("heading", { name: "Selection snapshot not available" })).not.toBeNull();
    expect(
      screen.getByText("Open a selection snapshot from the list to inspect policy and rationale.")
    ).not.toBeNull();
  });

  it("renders registry detail metadata and config payload", () => {
    render(
      <ModelRegistryDetailCard
        entry={{
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
        }}
      />
    );

    expect(screen.getByRole("heading", { name: "linear_feature" })).not.toBeNull();
    expect(screen.getByText("Baseline linear feature model")).not.toBeNull();
    expect(screen.getByText(/spread_error_regression_linear_feature_global/)).not.toBeNull();
    expect(screen.getByText(/team_code_scope/)).not.toBeNull();
  });

  it("renders run detail summaries and cross-workspace links", () => {
    render(
      <ModelAdminRunDetailCard
        run={{
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
            model_family: "linear_feature",
            selected_feature: "rolling_10_avg_total_error",
            fallback_strategy: null,
            fallback_reason: null,
            selection_metrics: {
              selected_branch: "primary_fit"
            }
          },
          metrics: {
            train: { prediction_count: 12 },
            validation: { prediction_count: 4, mae: 0.61 },
            test: { prediction_count: 3, mae: 0.73 }
          },
          created_at: "2026-04-18T00:00:00+00:00",
          completed_at: "2026-04-18T00:10:00+00:00"
        }}
      />
    );

    expect(screen.getByRole("heading", { name: "Run #301" })).not.toBeNull();
    expect(screen.getByText("Selected feature")).not.toBeNull();
    expect(screen.getByText("rolling_10_avg_total_error")).not.toBeNull();
    expect(screen.getByText("Selected branch: primary_fit")).not.toBeNull();
    expect(screen.getByRole("link", { name: "Open evaluations" }).getAttribute("href")).toBe(
      "#/models/evaluations"
    );
    expect(screen.getByRole("link", { name: "Backtests workspace" }).getAttribute("href")).toBe(
      "#/backtests"
    );
  });

  it("renders selection detail payload and related navigation links", () => {
    render(
      <ModelAdminSelectionDetailCard
        selection={{
          id: 501,
          model_evaluation_snapshot_id: 401,
          model_training_run_id: 301,
          model_registry_id: 11,
          feature_version_id: 21,
          target_task: "spread_error_regression",
          model_family: "linear_feature",
          selection_policy_name: "validation_regression_candidate_v1",
          rationale: {
            reason: "lowest_validation_mae",
            candidate_count: 2
          },
          is_active: true,
          created_at: "2026-04-18T00:12:00+00:00"
        }}
      />
    );

    expect(screen.getByRole("heading", { name: "Selection #501" })).not.toBeNull();
    expect(screen.getByText("Rationale payload")).not.toBeNull();
    expect(screen.getByText("Policy: validation_regression_candidate_v1")).not.toBeNull();
    expect(screen.getByText("Active: true")).not.toBeNull();
    expect(screen.getByText(/lowest_validation_mae/)).not.toBeNull();
    expect(screen.getByRole("link", { name: "Open related run" }).getAttribute("href")).toBe(
      "#/models/runs/301"
    );
    expect(
      screen.getByRole("link", { name: "Open related evaluation" }).getAttribute("href")
    ).toBe("#/models/evaluations/401");
  });

  it("renders evaluation detail summaries and related links", () => {
    render(
      <ModelAdminEvaluationDetailCard
        evaluation={{
          id: 401,
          model_training_run_id: 301,
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
            artifact: {
              model_family: "linear_feature"
            }
          },
          created_at: "2026-04-18T00:11:00+00:00"
        }}
      />
    );

    expect(screen.getByRole("heading", { name: "Evaluation #401" })).not.toBeNull();
    expect(screen.getByText("Prediction counts")).not.toBeNull();
    expect(screen.getByText("Primary metric: mae")).not.toBeNull();
    expect(screen.getByText("Selected feature: rolling_10_avg_total_error")).not.toBeNull();
    expect(screen.getByRole("link", { name: "Open related run" }).getAttribute("href")).toBe(
      "#/models/runs/301"
    );
    expect(screen.getByRole("link", { name: "Open selections" }).getAttribute("href")).toBe(
      "#/models/selections"
    );
  });
});
