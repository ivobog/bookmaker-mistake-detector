import type {
  ModelAdminSelectionMutationInput,
  ModelAdminTrainingMutationInput
} from "./modelAdminTypes";

export type ModelAdminTrainingDraft = {
  featureKey: string;
  seasonLabel: string;
  targetTask: string;
  teamCode: string;
  trainRatio: string;
  validationRatio: string;
};

export type ModelAdminSelectionDraft = {
  selectionPolicyName: string;
  targetTask: string;
};

function parseRequiredNumber(value: string, label: string): number {
  const parsed = Number(value);
  if (Number.isNaN(parsed)) {
    throw new Error(`${label} must be a valid number.`);
  }
  return parsed;
}

export function buildTrainingMutationInput(
  draft: ModelAdminTrainingDraft
): ModelAdminTrainingMutationInput {
  const normalizedFeatureKey = draft.featureKey.trim();
  const normalizedTargetTask = draft.targetTask.trim();
  if (!normalizedFeatureKey) {
    throw new Error("Feature key is required.");
  }
  if (!normalizedTargetTask) {
    throw new Error("Target task is required.");
  }

  const parsedTrainRatio = parseRequiredNumber(draft.trainRatio, "Train ratio");
  const parsedValidationRatio = parseRequiredNumber(draft.validationRatio, "Validation ratio");
  if (parsedTrainRatio <= 0 || parsedTrainRatio >= 1) {
    throw new Error("Train ratio must be greater than 0 and less than 1.");
  }
  if (parsedValidationRatio < 0 || parsedValidationRatio >= 1) {
    throw new Error("Validation ratio must be 0 or greater and less than 1.");
  }
  if (parsedTrainRatio + parsedValidationRatio >= 1) {
    throw new Error("Train ratio plus validation ratio must leave room for a test split.");
  }

  return {
    featureKey: normalizedFeatureKey,
    seasonLabel: draft.seasonLabel.trim() || null,
    targetTask: normalizedTargetTask,
    teamCode: draft.teamCode.trim() || null,
    trainRatio: parsedTrainRatio,
    validationRatio: parsedValidationRatio
  };
}

export function buildSelectionMutationInput(
  draft: ModelAdminSelectionDraft
): ModelAdminSelectionMutationInput {
  const normalizedTargetTask = draft.targetTask.trim();
  const normalizedSelectionPolicy = draft.selectionPolicyName.trim();
  if (!normalizedTargetTask) {
    throw new Error("Target task is required.");
  }
  if (!normalizedSelectionPolicy) {
    throw new Error("Selection policy is required.");
  }

  return {
    selectionPolicyName: normalizedSelectionPolicy,
    targetTask: normalizedTargetTask
  };
}
