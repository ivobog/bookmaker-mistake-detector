import { describe, expect, it } from "vitest";

import {
  buildSelectionMutationInput,
  buildTrainingMutationInput
} from "./modelAdminActionValidation";

describe("buildTrainingMutationInput", () => {
  it("normalizes optional blanks and parses ratio strings", () => {
    expect(
      buildTrainingMutationInput({
        featureKey: " baseline_team_features_v1 ",
        seasonLabel: " ",
        targetTask: " spread_error_regression ",
        teamCode: " ",
        trainRatio: "0.70",
        validationRatio: "0.15"
      })
    ).toEqual({
      featureKey: "baseline_team_features_v1",
      seasonLabel: null,
      targetTask: "spread_error_regression",
      teamCode: null,
      trainRatio: 0.7,
      validationRatio: 0.15
    });
  });

  it("rejects missing required fields", () => {
    expect(() =>
      buildTrainingMutationInput({
        featureKey: " ",
        seasonLabel: "",
        targetTask: "spread_error_regression",
        teamCode: "",
        trainRatio: "0.70",
        validationRatio: "0.15"
      })
    ).toThrow("Feature key is required.");

    expect(() =>
      buildTrainingMutationInput({
        featureKey: "baseline_team_features_v1",
        seasonLabel: "",
        targetTask: " ",
        teamCode: "",
        trainRatio: "0.70",
        validationRatio: "0.15"
      })
    ).toThrow("Target task is required.");
  });

  it("rejects invalid ratio ranges and split overflow", () => {
    expect(() =>
      buildTrainingMutationInput({
        featureKey: "baseline_team_features_v1",
        seasonLabel: "",
        targetTask: "spread_error_regression",
        teamCode: "",
        trainRatio: "1",
        validationRatio: "0.15"
      })
    ).toThrow("Train ratio must be greater than 0 and less than 1.");

    expect(() =>
      buildTrainingMutationInput({
        featureKey: "baseline_team_features_v1",
        seasonLabel: "",
        targetTask: "spread_error_regression",
        teamCode: "",
        trainRatio: "0.70",
        validationRatio: "-0.1"
      })
    ).toThrow("Validation ratio must be 0 or greater and less than 1.");

    expect(() =>
      buildTrainingMutationInput({
        featureKey: "baseline_team_features_v1",
        seasonLabel: "",
        targetTask: "spread_error_regression",
        teamCode: "",
        trainRatio: "0.85",
        validationRatio: "0.15"
      })
    ).toThrow("Train ratio plus validation ratio must leave room for a test split.");
  });
});

describe("buildSelectionMutationInput", () => {
  it("trims values before submit", () => {
    expect(
      buildSelectionMutationInput({
        selectionPolicyName: " validation_regression_candidate_v1 ",
        targetTask: " spread_error_regression "
      })
    ).toEqual({
      selectionPolicyName: "validation_regression_candidate_v1",
      targetTask: "spread_error_regression"
    });
  });

  it("rejects missing target task or selection policy", () => {
    expect(() =>
      buildSelectionMutationInput({
        selectionPolicyName: "validation_regression_candidate_v1",
        targetTask: " "
      })
    ).toThrow("Target task is required.");

    expect(() =>
      buildSelectionMutationInput({
        selectionPolicyName: " ",
        targetTask: "spread_error_regression"
      })
    ).toThrow("Selection policy is required.");
  });
});
