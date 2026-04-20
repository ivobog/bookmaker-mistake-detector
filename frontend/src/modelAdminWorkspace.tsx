import { useEffect, useState } from "react";

import {
  fetchModelCapabilities,
  fetchModelAdminEvaluationDetail,
  fetchModelAdminEvaluationHistory,
  fetchModelAdminEvaluations,
  fetchModelAdminHistory,
  fetchModelAdminRegistry,
  fetchModelAdminRunDetail,
  fetchModelAdminRuns,
  fetchModelAdminSelectionDetail,
  fetchModelAdminSelectionHistory,
  fetchModelAdminSelections,
  fetchModelAdminSummary,
  selectBestModel,
  trainModels
} from "./api";
import { resolveCanonicalSelectionPolicyName } from "./api/defaults";
import type { AppRoute } from "./appTypes";
import { ModelAdminActionsPanel } from "./modelAdminActions";
import {
  ModelAdminEvaluationDetailCard,
  ModelAdminRunDetailCard,
  ModelAdminSelectionDetailCard,
  ModelRegistryDetailCard
} from "./modelAdminDetailComponents";
import {
  EvaluationFilters,
  ModelAdminDashboardPage,
  ModelEvaluationsPage,
  ModelRegistryPage,
  ModelRunsPage,
  ModelSelectionsPage,
  SelectionFilters,
  SharedTrainingFilters
} from "./modelAdminPages";
import type {
  ModelAdminCapabilitiesResponse,
  ModelAdminEvaluationHistory,
  ModelAdminEvaluationSnapshot,
  ModelAdminHistoryResponse,
  ModelAdminRegistryEntry,
  ModelAdminRun,
  ModelAdminSelectionHistory,
  ModelAdminSelectionMutationInput,
  ModelAdminSelectionSnapshot,
  ModelAdminSummary,
  ModelAdminTrainingMutationInput
} from "./modelAdminTypes";

type ModelAdminWorkspaceProps = {
  route: AppRoute;
  modelHistory: ModelAdminHistoryResponse["model_history"] | null;
  onNavigate: (route: AppRoute) => void;
};

const defaultTrainingFilters = {
  seasonLabel: "",
  targetTask: "",
  teamCode: ""
};

const defaultEvaluationFilters = {
  modelFamily: "",
  targetTask: ""
};

const defaultSelectionFilters = {
  activeOnly: false,
  targetTask: ""
};

function normalizeOptionalText(value: string): string | null {
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function SectionMessage({
  tone = "neutral",
  children
}: {
  children: string;
  tone?: "error" | "neutral";
}) {
  return (
    <section className={`banner${tone === "error" ? " banner-error" : ""}`}>
      {children}
    </section>
  );
}

export function ModelAdminWorkspace({
  route,
  modelHistory,
  onNavigate
}: ModelAdminWorkspaceProps) {
  const [capabilities, setCapabilities] = useState<ModelAdminCapabilitiesResponse | null>(null);
  const [dashboardHistory, setDashboardHistory] = useState<ModelAdminHistoryResponse["model_history"] | null>(
    modelHistory
  );
  const [summary, setSummary] = useState<ModelAdminSummary | null>(null);
  const [evaluationHistory, setEvaluationHistory] = useState<ModelAdminEvaluationHistory | null>(null);
  const [selectionHistory, setSelectionHistory] = useState<ModelAdminSelectionHistory | null>(null);
  const [registryEntries, setRegistryEntries] = useState<ModelAdminRegistryEntry[]>([]);
  const [selectedRegistryId, setSelectedRegistryId] = useState<number | null>(null);
  const [runs, setRuns] = useState<ModelAdminRun[]>([]);
  const [runDetail, setRunDetail] = useState<ModelAdminRun | null>(null);
  const [evaluations, setEvaluations] = useState<ModelAdminEvaluationSnapshot[]>([]);
  const [evaluationDetail, setEvaluationDetail] = useState<ModelAdminEvaluationSnapshot | null>(null);
  const [selections, setSelections] = useState<ModelAdminSelectionSnapshot[]>([]);
  const [selectionDetail, setSelectionDetail] = useState<ModelAdminSelectionSnapshot | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [mutationAction, setMutationAction] = useState<"select" | "train" | null>(null);
  const [mutationNotice, setMutationNotice] = useState<{
    actionLabel?: string;
    actionRoute?: AppRoute;
    message: string;
    tone: "error" | "success";
  } | null>(null);
  const [refreshToken, setRefreshToken] = useState(0);

  const [trainingDraftFilters, setTrainingDraftFilters] = useState(defaultTrainingFilters);
  const [trainingFilters, setTrainingFilters] = useState(defaultTrainingFilters);
  const [evaluationDraftFilters, setEvaluationDraftFilters] = useState(defaultEvaluationFilters);
  const [evaluationFilters, setEvaluationFilters] = useState(defaultEvaluationFilters);
  const [selectionDraftFilters, setSelectionDraftFilters] = useState(defaultSelectionFilters);
  const [selectionFilters, setSelectionFilters] = useState(defaultSelectionFilters);
  const resolvedDefaultTargetTask = capabilities?.ui_defaults.default_target_task ?? "";
  const hasResolvedDefaultTargetTask = resolvedDefaultTargetTask.length > 0;
  const taskOptions = (capabilities?.target_tasks ?? []).map((task) => ({
    label: task.label,
    value: task.task_key
  }));
  const selectedSelectionTaskCapability =
    capabilities?.target_tasks.find((task) => task.task_key === selectionDraftFilters.targetTask) ?? null;
  const defaultTaskCapability =
    capabilities?.target_tasks.find((task) => task.task_key === resolvedDefaultTargetTask) ?? null;
  const selectionPolicyOptions =
    selectedSelectionTaskCapability?.valid_selection_policy_names?.length
      ? selectedSelectionTaskCapability.valid_selection_policy_names
      : defaultTaskCapability?.valid_selection_policy_names?.length
        ? defaultTaskCapability.valid_selection_policy_names
        : [resolveCanonicalSelectionPolicyName()];
  const defaultSelectionPolicyName =
    selectedSelectionTaskCapability?.default_selection_policy_name ??
    defaultTaskCapability?.default_selection_policy_name ??
    selectionPolicyOptions[0];

  useEffect(() => {
    setDashboardHistory(modelHistory);
  }, [modelHistory]);

  useEffect(() => {
    let cancelled = false;

    async function loadCapabilities() {
      try {
        const response = await fetchModelCapabilities();
        if (cancelled) {
          return;
        }
        setCapabilities(response);
        const nextTargetTask =
          response.ui_defaults.default_target_task ?? response.target_tasks[0]?.task_key ?? defaultTrainingFilters.targetTask;
        const nextTrainingFilters = {
          seasonLabel: "",
          targetTask: nextTargetTask,
          teamCode: ""
        };
        const nextEvaluationFilters = {
          modelFamily: "",
          targetTask: nextTargetTask
        };
        const nextSelectionFilters = {
          activeOnly: false,
          targetTask: nextTargetTask
        };
        setTrainingDraftFilters(nextTrainingFilters);
        setTrainingFilters(nextTrainingFilters);
        setEvaluationDraftFilters(nextEvaluationFilters);
        setEvaluationFilters(nextEvaluationFilters);
        setSelectionDraftFilters(nextSelectionFilters);
        setSelectionFilters(nextSelectionFilters);
      } catch (loadError) {
        if (!cancelled) {
          setError(
            loadError instanceof Error ? loadError.message : "Failed to load model capabilities."
          );
        }
      }
    }

    void loadCapabilities();

    return () => {
      cancelled = true;
    };
  }, []);

  async function refreshDashboardState(scope?: {
    seasonLabel?: string | null;
    targetTask?: string | null;
    teamCode?: string | null;
  }) {
    const normalizedScope = {
      seasonLabel: scope?.seasonLabel ?? normalizeOptionalText(trainingFilters.seasonLabel),
      targetTask: scope?.targetTask ?? normalizeOptionalText(trainingFilters.targetTask) ?? resolvedDefaultTargetTask,
      teamCode: scope?.teamCode ?? normalizeOptionalText(trainingFilters.teamCode)
    };
    const [nextHistory, nextSummary, nextEvaluationHistory, nextSelectionHistory] = await Promise.all([
      fetchModelAdminHistory({
        recentLimit: 8,
        seasonLabel: normalizedScope.seasonLabel,
        targetTask: normalizedScope.targetTask,
        teamCode: normalizedScope.teamCode
      }),
      fetchModelAdminSummary({
        seasonLabel: normalizedScope.seasonLabel,
        targetTask: normalizedScope.targetTask,
        teamCode: normalizedScope.teamCode
      }),
      fetchModelAdminEvaluationHistory({
        recentLimit: 8,
        targetTask: normalizedScope.targetTask
      }),
      fetchModelAdminSelectionHistory({
        recentLimit: 8,
        targetTask: normalizedScope.targetTask
      })
    ]);
    setDashboardHistory(nextHistory.model_history);
    setSummary(nextSummary.model_summary);
    setEvaluationHistory(nextEvaluationHistory.model_evaluation_history);
    setSelectionHistory(nextSelectionHistory.model_selection_history);
  }

  async function handleTrainMutation(input: ModelAdminTrainingMutationInput) {
    try {
      setMutationAction("train");
      setError(null);
      setMutationNotice(null);
      const result = await trainModels(input);
      const nextTargetTask = input.targetTask ?? resolvedDefaultTargetTask;
      const nextTeamCode = input.teamCode ?? null;
      const nextSeasonLabel = input.seasonLabel ?? null;
      const nextTrainingFilters = {
        seasonLabel: nextSeasonLabel ?? "",
        targetTask: nextTargetTask,
        teamCode: nextTeamCode ?? ""
      };
      setTrainingDraftFilters(nextTrainingFilters);
      setTrainingFilters(nextTrainingFilters);

      await refreshDashboardState({
        seasonLabel: nextSeasonLabel,
        targetTask: nextTargetTask,
        teamCode: nextTeamCode
      });
      const [nextRuns, nextEvaluations] = await Promise.all([
        fetchModelAdminRuns({
          seasonLabel: nextSeasonLabel,
          targetTask: nextTargetTask,
          teamCode: nextTeamCode
        }),
        fetchModelAdminEvaluations({
          targetTask: nextTargetTask
        })
      ]);
      setRuns(nextRuns.model_runs);
      setEvaluations(nextEvaluations.evaluation_snapshots);
      const createdRunId = result.best_model?.id ?? result.model_runs[0]?.id ?? null;
      if (createdRunId !== null) {
        const detail = await fetchModelAdminRunDetail(createdRunId);
        setRunDetail(detail.model_run);
        setMutationNotice({
          actionLabel: "Open run detail",
          actionRoute: { name: "model-run-detail", runId: createdRunId },
          message: `Training completed. ${result.persisted_run_count ?? result.model_runs.length} run(s) are now available.`,
          tone: "success"
        });
        setRefreshToken((current) => current + 1);
        onNavigate({ name: "model-run-detail", runId: createdRunId });
      } else {
        setMutationNotice({
          message: "Training completed, but no run detail was returned by the backend.",
          tone: "success"
        });
        setRefreshToken((current) => current + 1);
      }
    } catch (mutationError) {
      const message =
        mutationError instanceof Error ? mutationError.message : "Failed to train models from the workspace.";
      setError(message);
      setMutationNotice({ message, tone: "error" });
      throw mutationError;
    } finally {
      setMutationAction(null);
    }
  }

  async function handleSelectMutation(input: ModelAdminSelectionMutationInput) {
    try {
      setMutationAction("select");
      setError(null);
      setMutationNotice(null);
      const result = await selectBestModel(input);
      const nextTargetTask = input.targetTask ?? resolvedDefaultTargetTask;
      const nextSelectionFilters = {
        activeOnly: selectionFilters.activeOnly,
        targetTask: nextTargetTask
      };
      const nextEvaluationFilters = {
        modelFamily: evaluationFilters.modelFamily,
        targetTask: nextTargetTask
      };
      setSelectionDraftFilters((current) => ({ ...current, targetTask: nextTargetTask }));
      setSelectionFilters(nextSelectionFilters);
      setEvaluationDraftFilters((current) => ({ ...current, targetTask: nextTargetTask }));
      setEvaluationFilters(nextEvaluationFilters);

      await refreshDashboardState({
        targetTask: nextTargetTask
      });
      const [nextSelections, nextEvaluations] = await Promise.all([
        fetchModelAdminSelections({
          activeOnly: nextSelectionFilters.activeOnly,
          targetTask: nextTargetTask
        }),
        fetchModelAdminEvaluations({
          modelFamily: normalizeOptionalText(nextEvaluationFilters.modelFamily),
          targetTask: nextTargetTask
        })
      ]);
      setSelections(nextSelections.selections);
      setEvaluations(nextEvaluations.evaluation_snapshots);
      const createdSelectionId = result.active_selection?.id ?? null;
      if (createdSelectionId !== null) {
        const detail = await fetchModelAdminSelectionDetail(createdSelectionId);
        setSelectionDetail(detail.selection);
        setMutationNotice({
          actionLabel: "Open selection detail",
          actionRoute: { name: "model-selection-detail", selectionId: createdSelectionId },
          message: `Promotion completed using ${result.selection_policy_name}. The active selection is now updated.`,
          tone: "success"
        });
        setRefreshToken((current) => current + 1);
        onNavigate({ name: "model-selection-detail", selectionId: createdSelectionId });
      } else {
        setMutationNotice({
          message: "Selection request completed, but no active selection was returned by the backend.",
          tone: "success"
        });
        setRefreshToken((current) => current + 1);
      }
    } catch (mutationError) {
      const message =
        mutationError instanceof Error
          ? mutationError.message
          : "Failed to select the best model from the workspace.";
      setError(message);
      setMutationNotice({ message, tone: "error" });
      throw mutationError;
    } finally {
      setMutationAction(null);
    }
  }

  useEffect(() => {
    let cancelled = false;

    async function loadDashboard() {
      if (route.name !== "models" || !hasResolvedDefaultTargetTask) {
        return;
      }
      try {
        setLoading(true);
        setError(null);
        const normalizedScope = {
          seasonLabel: normalizeOptionalText(trainingFilters.seasonLabel),
          targetTask: normalizeOptionalText(trainingFilters.targetTask) ?? resolvedDefaultTargetTask,
          teamCode: normalizeOptionalText(trainingFilters.teamCode)
        };
        const [nextHistory, nextSummary, nextEvaluationHistory, nextSelectionHistory] = await Promise.all([
          fetchModelAdminHistory({
            recentLimit: 8,
            seasonLabel: normalizedScope.seasonLabel,
            targetTask: normalizedScope.targetTask,
            teamCode: normalizedScope.teamCode
          }),
          fetchModelAdminSummary({
            seasonLabel: normalizedScope.seasonLabel,
            targetTask: normalizedScope.targetTask,
            teamCode: normalizedScope.teamCode
          }),
          fetchModelAdminEvaluationHistory({
            recentLimit: 8,
            targetTask: normalizedScope.targetTask
          }),
          fetchModelAdminSelectionHistory({
            recentLimit: 8,
            targetTask: normalizedScope.targetTask
          })
        ]);
        if (cancelled) {
          return;
        }
        setDashboardHistory(nextHistory.model_history);
        setSummary(nextSummary.model_summary);
        setEvaluationHistory(nextEvaluationHistory.model_evaluation_history);
        setSelectionHistory(nextSelectionHistory.model_selection_history);
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : "Failed to load the model dashboard.");
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void loadDashboard();
    return () => {
      cancelled = true;
    };
  }, [hasResolvedDefaultTargetTask, resolvedDefaultTargetTask, route, trainingFilters, refreshToken]);

  useEffect(() => {
    let cancelled = false;

    async function loadRegistry() {
      if (route.name !== "model-registry" || !hasResolvedDefaultTargetTask) {
        return;
      }
      try {
        setLoading(true);
        setError(null);
        const response = await fetchModelAdminRegistry({
          targetTask: normalizeOptionalText(trainingFilters.targetTask) ?? resolvedDefaultTargetTask
        });
        if (cancelled) {
          return;
        }
        setRegistryEntries(response.model_registry);
        setSelectedRegistryId((current) => {
          if (current && response.model_registry.some((entry) => entry.id === current)) {
            return current;
          }
          return response.model_registry[0]?.id ?? null;
        });
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : "Failed to load model registry.");
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void loadRegistry();
    return () => {
      cancelled = true;
    };
  }, [hasResolvedDefaultTargetTask, resolvedDefaultTargetTask, route, trainingFilters.targetTask, refreshToken]);

  useEffect(() => {
    let cancelled = false;

    async function loadRuns() {
      if (
        (route.name !== "model-runs" && route.name !== "model-run-detail") ||
        !hasResolvedDefaultTargetTask
      ) {
        return;
      }
      try {
        setLoading(true);
        setError(null);
        const normalizedScope = {
          seasonLabel: normalizeOptionalText(trainingFilters.seasonLabel),
          targetTask: normalizeOptionalText(trainingFilters.targetTask) ?? resolvedDefaultTargetTask,
          teamCode: normalizeOptionalText(trainingFilters.teamCode)
        };
        const runsResponse = await fetchModelAdminRuns(normalizedScope);
        if (cancelled) {
          return;
        }
        setRuns(runsResponse.model_runs);

        if (route.name === "model-run-detail") {
          const detailResponse = await fetchModelAdminRunDetail(route.runId);
          if (!cancelled) {
            setRunDetail(detailResponse.model_run);
          }
        } else if (!cancelled) {
          setRunDetail(null);
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : "Failed to load model runs.");
          setRunDetail(null);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void loadRuns();
    return () => {
      cancelled = true;
    };
  }, [hasResolvedDefaultTargetTask, resolvedDefaultTargetTask, route, trainingFilters, refreshToken]);

  useEffect(() => {
    let cancelled = false;

    async function loadEvaluations() {
      if (
        (route.name !== "model-evaluations" && route.name !== "model-evaluation-detail") ||
        !hasResolvedDefaultTargetTask
      ) {
        return;
      }
      try {
        setLoading(true);
        setError(null);
        const normalizedScope = {
          modelFamily: normalizeOptionalText(evaluationFilters.modelFamily),
          targetTask: normalizeOptionalText(evaluationFilters.targetTask) ?? resolvedDefaultTargetTask
        };
        const evaluationsResponse = await fetchModelAdminEvaluations(normalizedScope);
        if (cancelled) {
          return;
        }
        setEvaluations(evaluationsResponse.evaluation_snapshots);
        if (route.name === "model-evaluation-detail") {
          const detailResponse = await fetchModelAdminEvaluationDetail(route.evaluationId);
          if (!cancelled) {
            setEvaluationDetail(detailResponse.evaluation_snapshot);
          }
        } else if (!cancelled) {
          setEvaluationDetail(null);
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : "Failed to load evaluation snapshots.");
          setEvaluationDetail(null);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void loadEvaluations();
    return () => {
      cancelled = true;
    };
  }, [hasResolvedDefaultTargetTask, resolvedDefaultTargetTask, route, evaluationFilters, refreshToken]);

  useEffect(() => {
    let cancelled = false;

    async function loadSelections() {
      if (
        (route.name !== "model-selections" && route.name !== "model-selection-detail") ||
        !hasResolvedDefaultTargetTask
      ) {
        return;
      }
      try {
        setLoading(true);
        setError(null);
        const normalizedScope = {
          activeOnly: selectionFilters.activeOnly,
          targetTask: normalizeOptionalText(selectionFilters.targetTask) ?? resolvedDefaultTargetTask
        };
        const selectionsResponse = await fetchModelAdminSelections(normalizedScope);
        if (cancelled) {
          return;
        }
        setSelections(selectionsResponse.selections);
        if (route.name === "model-selection-detail") {
          const detailResponse = await fetchModelAdminSelectionDetail(route.selectionId);
          if (!cancelled) {
            setSelectionDetail(detailResponse.selection);
          }
        } else if (!cancelled) {
          setSelectionDetail(null);
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : "Failed to load selection snapshots.");
          setSelectionDetail(null);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void loadSelections();
    return () => {
      cancelled = true;
    };
  }, [hasResolvedDefaultTargetTask, resolvedDefaultTargetTask, route, selectionFilters, refreshToken]);

  const selectedRegistryEntry =
    registryEntries.find((entry) => entry.id === selectedRegistryId) ?? null;

  return (
    <>
      {loading ? <SectionMessage>Loading Model Admin workspace...</SectionMessage> : null}
      {error ? <SectionMessage tone="error">{error}</SectionMessage> : null}
      {mutationNotice ? (
        <section className={`banner${mutationNotice.tone === "error" ? " banner-error" : ""} action-banner`}>
          <div className="action-banner-row">
            <span>{mutationNotice.message}</span>
            {mutationNotice.actionRoute && mutationNotice.actionLabel ? (
              <button
                className="secondary-button"
                onClick={() => onNavigate(mutationNotice.actionRoute!)}
                type="button"
              >
                {mutationNotice.actionLabel}
              </button>
            ) : null}
          </div>
        </section>
      ) : null}

      {route.name === "models" ? (
        <>
          <ModelAdminActionsPanel
            busyAction={mutationAction}
            defaultSeasonLabel={trainingDraftFilters.seasonLabel}
            defaultSelectionPolicyName={defaultSelectionPolicyName}
            defaultTargetTask={trainingDraftFilters.targetTask}
            defaultTeamCode={trainingDraftFilters.teamCode}
            enableSelect
            enableTrain
            onSelectSubmit={handleSelectMutation}
            onTrainSubmit={handleTrainMutation}
            selectionPolicyOptions={selectionPolicyOptions}
            targetTaskOptions={taskOptions}
          />
          <SharedTrainingFilters
            onApply={() => setTrainingFilters(trainingDraftFilters)}
            onReset={() => {
              const nextFilters = { ...defaultTrainingFilters, targetTask: resolvedDefaultTargetTask };
              setTrainingDraftFilters(nextFilters);
              setTrainingFilters(nextFilters);
            }}
            seasonLabel={trainingDraftFilters.seasonLabel}
            setSeasonLabel={(value) =>
              setTrainingDraftFilters((current) => ({ ...current, seasonLabel: value }))
            }
            setTargetTask={(value) =>
              setTrainingDraftFilters((current) => ({ ...current, targetTask: value }))
            }
            setTeamCode={(value) =>
              setTrainingDraftFilters((current) => ({ ...current, teamCode: value }))
            }
            targetTask={trainingDraftFilters.targetTask}
            teamCode={trainingDraftFilters.teamCode}
            taskOptions={taskOptions}
          />
          <ModelAdminDashboardPage
            evaluationHistory={evaluationHistory}
            onNavigate={onNavigate}
            selectionHistory={selectionHistory}
            summary={summary}
            trainingHistory={dashboardHistory}
          />
        </>
      ) : null}

      {route.name === "model-registry" ? (
        <>
          <SharedTrainingFilters
            onApply={() => setTrainingFilters(trainingDraftFilters)}
            onReset={() => {
              const nextFilters = { ...defaultTrainingFilters, targetTask: resolvedDefaultTargetTask };
              setTrainingDraftFilters(nextFilters);
              setTrainingFilters(nextFilters);
            }}
            seasonLabel={trainingDraftFilters.seasonLabel}
            setSeasonLabel={(value) =>
              setTrainingDraftFilters((current) => ({ ...current, seasonLabel: value }))
            }
            setTargetTask={(value) =>
              setTrainingDraftFilters((current) => ({ ...current, targetTask: value }))
            }
            setTeamCode={(value) =>
              setTrainingDraftFilters((current) => ({ ...current, teamCode: value }))
            }
            targetTask={trainingDraftFilters.targetTask}
            teamCode={trainingDraftFilters.teamCode}
            taskOptions={taskOptions}
          />
          <ModelRegistryPage
            detailContent={<ModelRegistryDetailCard entry={selectedRegistryEntry} />}
            entries={registryEntries}
            onSelectEntry={setSelectedRegistryId}
            selectedEntryId={selectedRegistryId}
          />
        </>
      ) : null}

      {route.name === "model-runs" || route.name === "model-run-detail" ? (
        <>
          <SharedTrainingFilters
            onApply={() => setTrainingFilters(trainingDraftFilters)}
            onReset={() => {
              const nextFilters = { ...defaultTrainingFilters, targetTask: resolvedDefaultTargetTask };
              setTrainingDraftFilters(nextFilters);
              setTrainingFilters(nextFilters);
            }}
            seasonLabel={trainingDraftFilters.seasonLabel}
            setSeasonLabel={(value) =>
              setTrainingDraftFilters((current) => ({ ...current, seasonLabel: value }))
            }
            setTargetTask={(value) =>
              setTrainingDraftFilters((current) => ({ ...current, targetTask: value }))
            }
            setTeamCode={(value) =>
              setTrainingDraftFilters((current) => ({ ...current, teamCode: value }))
            }
            targetTask={trainingDraftFilters.targetTask}
            teamCode={trainingDraftFilters.teamCode}
            taskOptions={taskOptions}
          />
          <ModelRunsPage
            detailContent={<ModelAdminRunDetailCard run={runDetail} />}
            onNavigate={onNavigate}
            runs={runs}
            selectedRunId={route.name === "model-run-detail" ? route.runId : null}
          />
        </>
      ) : null}

      {route.name === "model-evaluations" || route.name === "model-evaluation-detail" ? (
        <>
          <EvaluationFilters
            modelFamily={evaluationDraftFilters.modelFamily}
            onApply={() => setEvaluationFilters(evaluationDraftFilters)}
            onReset={() => {
              const nextFilters = { ...defaultEvaluationFilters, targetTask: resolvedDefaultTargetTask };
              setEvaluationDraftFilters(nextFilters);
              setEvaluationFilters(nextFilters);
            }}
            setModelFamily={(value) =>
              setEvaluationDraftFilters((current) => ({ ...current, modelFamily: value }))
            }
            setTargetTask={(value) =>
              setEvaluationDraftFilters((current) => ({ ...current, targetTask: value }))
            }
            taskOptions={taskOptions}
            targetTask={evaluationDraftFilters.targetTask}
          />
          <ModelEvaluationsPage
            detailContent={<ModelAdminEvaluationDetailCard evaluation={evaluationDetail} />}
            evaluations={evaluations}
            onNavigate={onNavigate}
            selectedEvaluationId={route.name === "model-evaluation-detail" ? route.evaluationId : null}
          />
        </>
      ) : null}

      {route.name === "model-selections" || route.name === "model-selection-detail" ? (
        <>
          <ModelAdminActionsPanel
            busyAction={mutationAction}
            defaultSeasonLabel={trainingDraftFilters.seasonLabel}
            defaultSelectionPolicyName={defaultSelectionPolicyName}
            defaultTargetTask={selectionDraftFilters.targetTask}
            defaultTeamCode={trainingDraftFilters.teamCode}
            enableSelect
            enableTrain={false}
            onSelectSubmit={handleSelectMutation}
            onTrainSubmit={handleTrainMutation}
            selectionPolicyOptions={selectionPolicyOptions}
            targetTaskOptions={taskOptions}
          />
          <SelectionFilters
            activeOnly={selectionDraftFilters.activeOnly}
            onApply={() => setSelectionFilters(selectionDraftFilters)}
            onReset={() => {
              const nextFilters = { ...defaultSelectionFilters, targetTask: resolvedDefaultTargetTask };
              setSelectionDraftFilters(nextFilters);
              setSelectionFilters(nextFilters);
            }}
            setActiveOnly={(value) =>
              setSelectionDraftFilters((current) => ({ ...current, activeOnly: value }))
            }
            setTargetTask={(value) =>
              setSelectionDraftFilters((current) => ({ ...current, targetTask: value }))
            }
            taskOptions={taskOptions}
            targetTask={selectionDraftFilters.targetTask}
          />
          <ModelSelectionsPage
            detailContent={<ModelAdminSelectionDetailCard selection={selectionDetail} />}
            onNavigate={onNavigate}
            selectedSelectionId={route.name === "model-selection-detail" ? route.selectionId : null}
            selections={selections}
          />
        </>
      ) : null}
    </>
  );
}
