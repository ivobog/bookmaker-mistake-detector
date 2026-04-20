import { useEffect, useState } from "react";

import {
  buildSelectionMutationInput,
  buildTrainingMutationInput
} from "./modelAdminActionValidation";
import type {
  ModelAdminSelectionMutationInput,
  ModelAdminTrainingMutationInput
} from "./modelAdminTypes";

type MutationAction = "select" | "train" | null;

type ActionPanelProps = {
  busyAction: MutationAction;
  defaultSeasonLabel: string;
  defaultSelectionPolicyName?: string;
  defaultTargetTask: string;
  defaultTeamCode: string;
  enableSelect: boolean;
  enableTrain: boolean;
  onSelectSubmit: (input: ModelAdminSelectionMutationInput) => Promise<void>;
  onTrainSubmit: (input: ModelAdminTrainingMutationInput) => Promise<void>;
  selectionPolicyOptions?: string[];
  targetTaskOptions?: Array<{ label: string; value: string }>;
};

export function ModelAdminActionsPanel({
  busyAction,
  defaultSeasonLabel,
  defaultSelectionPolicyName = "validation_mae_candidate_v1",
  defaultTargetTask,
  defaultTeamCode,
  enableSelect,
  enableTrain,
  onSelectSubmit,
  onTrainSubmit,
  selectionPolicyOptions,
  targetTaskOptions
}: ActionPanelProps) {
  const [openAction, setOpenAction] = useState<MutationAction>(null);
  const [formError, setFormError] = useState<string | null>(null);

  const [featureKey, setFeatureKey] = useState("baseline_team_features_v1");
  const [trainTargetTask, setTrainTargetTask] = useState(defaultTargetTask);
  const [trainTeamCode, setTrainTeamCode] = useState(defaultTeamCode);
  const [trainSeasonLabel, setTrainSeasonLabel] = useState(defaultSeasonLabel);
  const [trainRatio, setTrainRatio] = useState("0.70");
  const [validationRatio, setValidationRatio] = useState("0.15");

  const [selectionTargetTask, setSelectionTargetTask] = useState(defaultTargetTask);
  const [selectionPolicyName, setSelectionPolicyName] = useState(defaultSelectionPolicyName);

  useEffect(() => {
    setTrainTargetTask(defaultTargetTask);
    setSelectionTargetTask(defaultTargetTask);
  }, [defaultTargetTask]);

  useEffect(() => {
    setSelectionPolicyName(defaultSelectionPolicyName);
  }, [defaultSelectionPolicyName]);

  useEffect(() => {
    setTrainTeamCode(defaultTeamCode);
  }, [defaultTeamCode]);

  useEffect(() => {
    setTrainSeasonLabel(defaultSeasonLabel);
  }, [defaultSeasonLabel]);

  async function handleTrainSubmit() {
    try {
      setFormError(null);
      await onTrainSubmit(
        buildTrainingMutationInput({
          featureKey,
          seasonLabel: trainSeasonLabel,
          targetTask: trainTargetTask,
          teamCode: trainTeamCode,
          trainRatio,
          validationRatio
        })
      );
      setOpenAction(null);
    } catch (submissionError) {
      setFormError(
        submissionError instanceof Error ? submissionError.message : "Failed to submit the train request."
      );
    }
  }

  async function handleSelectSubmit() {
    try {
      setFormError(null);
      await onSelectSubmit(
        buildSelectionMutationInput({
          selectionPolicyName,
          targetTask: selectionTargetTask
        })
      );
      setOpenAction(null);
    } catch (submissionError) {
      setFormError(
        submissionError instanceof Error
          ? submissionError.message
          : "Failed to submit the selection request."
      );
    }
  }

  return (
    <section className="panel action-panel">
      <div className="section-heading">
        <div>
          <p className="eyebrow">Admin actions</p>
          <h2>Operate the training lifecycle</h2>
        </div>
        <div className="pill-row">
          {enableTrain ? (
            <button
              className={`secondary-button${openAction === "train" ? " action-toggle-active" : ""}`}
              data-testid="model-admin-train-action"
              onClick={() => {
                setFormError(null);
                setOpenAction((current) => (current === "train" ? null : "train"));
              }}
              type="button"
            >
              Train model
            </button>
          ) : null}
          {enableSelect ? (
            <button
              className={`secondary-button${openAction === "select" ? " action-toggle-active" : ""}`}
              data-testid="model-admin-select-action"
              onClick={() => {
                setFormError(null);
                setOpenAction((current) => (current === "select" ? null : "select"));
              }}
              type="button"
            >
              Select best model
            </button>
          ) : null}
        </div>
      </div>

      <p className="sub-panel-meta">
        These controls use the existing admin model endpoints and keep all mutation feedback inside the
        Model Admin workspace.
      </p>

      {formError ? <div className="banner banner-error action-banner">{formError}</div> : null}

      {openAction === "train" ? (
        <div className="action-form-grid" data-testid="train-model-form">
          <label className="filter-field">
            <span className="filter-label">Feature key</span>
            <input
              data-testid="train-feature-key"
              onChange={(event) => setFeatureKey(event.target.value)}
              value={featureKey}
            />
          </label>
          <label className="filter-field">
            <span className="filter-label">Target task</span>
            {targetTaskOptions?.length ? (
              <select
                data-testid="train-target-task"
                onChange={(event) => setTrainTargetTask(event.target.value)}
                value={trainTargetTask}
              >
                {targetTaskOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            ) : (
              <input
                data-testid="train-target-task"
                onChange={(event) => setTrainTargetTask(event.target.value)}
                value={trainTargetTask}
              />
            )}
          </label>
          <label className="filter-field">
            <span className="filter-label">Team code</span>
            <input
              data-testid="train-team-code"
              onChange={(event) => setTrainTeamCode(event.target.value)}
              placeholder="Optional"
              value={trainTeamCode}
            />
          </label>
          <label className="filter-field">
            <span className="filter-label">Season label</span>
            <input
              data-testid="train-season-label"
              onChange={(event) => setTrainSeasonLabel(event.target.value)}
              placeholder="Optional"
              value={trainSeasonLabel}
            />
          </label>
          <label className="filter-field">
            <span className="filter-label">Train ratio</span>
            <input
              data-testid="train-train-ratio"
              onChange={(event) => setTrainRatio(event.target.value)}
              value={trainRatio}
            />
          </label>
          <label className="filter-field">
            <span className="filter-label">Validation ratio</span>
            <input
              data-testid="train-validation-ratio"
              onChange={(event) => setValidationRatio(event.target.value)}
              value={validationRatio}
            />
          </label>

          <div className="action-submit-row">
            <button
              className="primary-button action-primary-button"
              data-testid="train-submit"
              disabled={busyAction === "train"}
              onClick={() => void handleTrainSubmit()}
              type="button"
            >
              {busyAction === "train" ? "Training model..." : "Start training"}
            </button>
          </div>
        </div>
      ) : null}

      {openAction === "select" ? (
        <div className="action-form-grid action-form-grid-compact" data-testid="select-model-form">
          <label className="filter-field">
            <span className="filter-label">Target task</span>
            {targetTaskOptions?.length ? (
              <select
                data-testid="select-target-task"
                onChange={(event) => setSelectionTargetTask(event.target.value)}
                value={selectionTargetTask}
              >
                {targetTaskOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            ) : (
              <input
                data-testid="select-target-task"
                onChange={(event) => setSelectionTargetTask(event.target.value)}
                value={selectionTargetTask}
              />
            )}
          </label>
          <label className="filter-field">
            <span className="filter-label">Selection policy</span>
            {selectionPolicyOptions?.length ? (
              <select
                data-testid="select-policy-name"
                onChange={(event) => setSelectionPolicyName(event.target.value)}
                value={selectionPolicyName}
              >
                {selectionPolicyOptions.map((option) => (
                  <option key={option} value={option}>
                    {option}
                  </option>
                ))}
              </select>
            ) : (
              <input
                data-testid="select-policy-name"
                onChange={(event) => setSelectionPolicyName(event.target.value)}
                value={selectionPolicyName}
              />
            )}
          </label>

          <div className="action-submit-row">
            <button
              className="primary-button action-primary-button"
              data-testid="select-submit"
              disabled={busyAction === "select"}
              onClick={() => void handleSelectSubmit()}
              type="button"
            >
              {busyAction === "select" ? "Selecting model..." : "Promote best model"}
            </button>
          </div>
        </div>
      ) : null}
    </section>
  );
}
