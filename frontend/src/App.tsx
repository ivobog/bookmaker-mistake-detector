import { useEffect, useState } from "react";

import {
  apiBaseUrl,
  fetchBacktestHistory,
  fetchBacktestRunDetail,
  fetchEvaluationDetail,
  fetchModelAdminHistory,
  fetchModelRunDetail,
  fetchOpportunityDetail,
  fetchOpportunityHistory,
  fetchOpportunities,
  fetchScoringRunDetail,
  fetchSelectionDetail,
  materializeOpportunities,
  runBacktest
} from "./api";
import type {
  AppRoute,
  BacktestHistoryResponse,
  BacktestRun,
  EvaluationSnapshot,
  ModelTrainingRun,
  OpportunityHistoryResponse,
  OpportunityListResponse,
  OpportunityRecord,
  ProvenanceInspectorData,
  ProvenanceItem,
  ScoringRunDetail,
  SelectionSnapshot
} from "./appTypes";
import type { ModelAdminHistoryResponse } from "./modelAdminTypes";
import {
  asRecord,
  formatTimestamp,
  parseRouteFromHash,
  readNested,
  routeHash
} from "./appUtils";
import {
  BacktestRunDetailCard as SharedBacktestRunDetailCard,
  FoldDetailCard as SharedFoldDetailCard
} from "./appBacktestDetailComponents";
import {
  ArtifactCompareView as SharedArtifactCompareView,
  EvaluationArtifactDetail as SharedEvaluationArtifactDetail,
  ModelRunArtifactDetail as SharedModelRunArtifactDetail,
  ScoringRunArtifactDetail as SharedScoringRunArtifactDetail,
  SelectionArtifactDetail as SharedSelectionArtifactDetail
} from "./appArtifactDetailComponents";
import {
  ComparableCaseDetail as SharedComparableCaseDetail,
  OpportunityDetailCard as SharedOpportunityDetailCard
} from "./appOpportunityDetailComponents";
import { BacktestsWorkspace } from "./backtestsWorkspace";
import { ModelAdminWorkspace } from "./modelAdminWorkspace";
import { OpportunitiesWorkspace } from "./opportunitiesWorkspace";

export default function App() {
  const [history, setHistory] = useState<BacktestHistoryResponse | null>(null);
  const [opportunityHistory, setOpportunityHistory] = useState<OpportunityHistoryResponse | null>(null);
  const [opportunityList, setOpportunityList] = useState<OpportunityListResponse | null>(null);
  const [opportunities, setOpportunities] = useState<OpportunityRecord[]>([]);
  const [modelHistory, setModelHistory] = useState<ModelAdminHistoryResponse["model_history"] | null>(null);
  const [activeModelRun, setActiveModelRun] = useState<ModelTrainingRun | null>(null);
  const [activeSelectionSnapshot, setActiveSelectionSnapshot] = useState<SelectionSnapshot | null>(null);
  const [activeEvaluationSnapshot, setActiveEvaluationSnapshot] = useState<EvaluationSnapshot | null>(null);
  const [activeBacktestFoldModelRun, setActiveBacktestFoldModelRun] = useState<ModelTrainingRun | null>(null);
  const [activeBacktestFoldEvaluation, setActiveBacktestFoldEvaluation] = useState<EvaluationSnapshot | null>(null);
  const [activeScoringRun, setActiveScoringRun] = useState<ScoringRunDetail | null>(null);
  const [activeRun, setActiveRun] = useState<BacktestRun | null>(null);
  const [activeOpportunityId, setActiveOpportunityId] = useState<number | null>(null);
  const [activeOpportunity, setActiveOpportunity] = useState<OpportunityRecord | null>(null);
  const [route, setRoute] = useState<AppRoute>(() => parseRouteFromHash(window.location.hash));
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(false);
  const [materializingOpportunity, setMaterializingOpportunity] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    function handleHashChange() {
      setRoute(parseRouteFromHash(window.location.hash));
    }

    window.addEventListener("hashchange", handleHashChange);

    if (!window.location.hash) {
      window.location.hash = routeHash({ name: "backtests" });
    } else {
      handleHashChange();
    }

    return () => {
      window.removeEventListener("hashchange", handleHashChange);
    };
  }, []);

  function navigate(nextRoute: AppRoute) {
    window.location.hash = routeHash(nextRoute);
  }

  function handleSelectOpportunity(opportunityId: number) {
    if (route.name === "opportunities") {
      setActiveOpportunityId(opportunityId);
      return;
    }
    navigate({ name: "opportunity-detail", opportunityId });
  }

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        setLoading(true);
        setError(null);
        const [
          nextHistory,
          nextOpportunityHistory,
          nextOpportunityList,
          nextModelHistory
        ] = await Promise.all([
          fetchBacktestHistory(),
          fetchOpportunityHistory(),
          fetchOpportunities(),
          fetchModelAdminHistory()
        ]);
        if (cancelled) {
          return;
        }
        setHistory(nextHistory);
        const preferredRun =
          route.name === "backtest-run"
            ? nextHistory.model_backtest_history.recent_runs.find((run) => run.id === route.runId) ??
              nextHistory.model_backtest_history.overview.latest_run
            : nextHistory.model_backtest_history.overview.latest_run;
        setActiveRun(preferredRun);
        setOpportunityHistory(nextOpportunityHistory);
        setOpportunityList(nextOpportunityList);
        setOpportunities(nextOpportunityList.opportunities);
        setModelHistory(nextModelHistory.model_history);
        const preferredOpportunityId =
          route.name === "opportunity-detail" ||
          route.name === "comparable-case" ||
          route.name === "opportunity-model-run" ||
          route.name === "opportunity-selection" ||
          route.name === "opportunity-evaluation" ||
          route.name === "opportunity-scoring-run" ||
          route.name === "artifact-compare"
            ? route.opportunityId
            : nextOpportunityList.opportunities[0]?.id ??
              nextOpportunityHistory.model_opportunity_history.overview.latest_opportunity?.id ??
              null;
        setActiveOpportunityId(
          preferredOpportunityId
        );
      } catch (loadError) {
        if (cancelled) {
          return;
        }
        setError(loadError instanceof Error ? loadError.message : "Failed to load the Phase 4 console.");
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void load();

    return () => {
      cancelled = true;
    };
  }, [route]);

  useEffect(() => {
    if (
      route.name === "opportunity-detail" ||
      route.name === "comparable-case" ||
      route.name === "opportunity-model-run" ||
      route.name === "opportunity-selection" ||
      route.name === "opportunity-evaluation" ||
      route.name === "opportunity-scoring-run" ||
      route.name === "artifact-compare"
    ) {
      setActiveOpportunityId(route.opportunityId);
      return;
    }
    if (route.name === "opportunities" && activeOpportunityId === null) {
      setActiveOpportunityId(opportunities[0]?.id ?? null);
    }
  }, [route, opportunities, activeOpportunityId]);

  useEffect(() => {
    let cancelled = false;

    async function loadOpportunityDetail() {
      if (activeOpportunityId === null) {
        setActiveOpportunity(null);
        return;
      }

      try {
        const detail = await fetchOpportunityDetail(activeOpportunityId);
        if (!cancelled) {
          setActiveOpportunity(detail.opportunity);
        }
      } catch (detailError) {
        if (!cancelled) {
          setError(detailError instanceof Error ? detailError.message : "Failed to load opportunity detail.");
        }
      }
    }

    void loadOpportunityDetail();

    return () => {
      cancelled = true;
    };
  }, [activeOpportunityId]);

  useEffect(() => {
    let cancelled = false;

    async function loadBacktestRunDetail() {
      if (
        route.name !== "backtest-run" &&
        route.name !== "backtest-fold" &&
        route.name !== "backtest-fold-model-run" &&
        route.name !== "backtest-fold-evaluation" &&
        route.name !== "artifact-compare"
      ) {
        return;
      }

      try {
        const detail = await fetchBacktestRunDetail(route.runId);
        if (!cancelled) {
          setActiveRun(detail.backtest_run);
        }
      } catch (detailError) {
        if (!cancelled) {
          setError(detailError instanceof Error ? detailError.message : "Failed to load backtest run.");
        }
      }
    }

    void loadBacktestRunDetail();

    return () => {
      cancelled = true;
    };
  }, [route]);

  useEffect(() => {
    let cancelled = false;

    async function loadBacktestFoldArtifactDetails() {
      if (
        route.name !== "backtest-fold" &&
        route.name !== "backtest-fold-model-run" &&
        route.name !== "backtest-fold-evaluation" &&
        route.name !== "artifact-compare"
      ) {
        setActiveBacktestFoldModelRun(null);
        setActiveBacktestFoldEvaluation(null);
        return;
      }

      const selectedFold =
        activeRun?.payload.folds.find((fold) => fold.fold_index === route.foldIndex) ?? null;

      if (!selectedFold) {
        setActiveBacktestFoldModelRun(null);
        setActiveBacktestFoldEvaluation(null);
        return;
      }

      try {
        const [modelRunDetail, evaluationDetail] = await Promise.all([
          fetchModelRunDetail(selectedFold.selected_model.model_training_run_id),
          fetchEvaluationDetail(selectedFold.selected_model.evaluation_snapshot_id)
        ]);
        if (!cancelled) {
          setActiveBacktestFoldModelRun(modelRunDetail.model_run);
          setActiveBacktestFoldEvaluation(evaluationDetail.evaluation_snapshot);
        }
      } catch (detailError) {
        if (!cancelled) {
          setActiveBacktestFoldModelRun(null);
          setActiveBacktestFoldEvaluation(null);
          setError(
            detailError instanceof Error
              ? detailError.message
              : "Failed to load backtest fold provenance details."
          );
        }
      }
    }

    void loadBacktestFoldArtifactDetails();

    return () => {
      cancelled = true;
    };
  }, [route, activeRun]);

  useEffect(() => {
    let cancelled = false;

    async function loadModelArtifactDetails() {
      if (!activeOpportunity) {
        setActiveSelectionSnapshot(null);
        setActiveEvaluationSnapshot(null);
        setActiveModelRun(null);
        return;
      }

      try {
        const [selectionDetail, evaluationDetail] = await Promise.all([
          activeOpportunity.model_selection_snapshot_id
            ? fetchSelectionDetail(activeOpportunity.model_selection_snapshot_id)
            : Promise.resolve({ repository_mode: "in_memory", selection: null }),
          activeOpportunity.model_evaluation_snapshot_id
            ? fetchEvaluationDetail(activeOpportunity.model_evaluation_snapshot_id)
            : Promise.resolve({ repository_mode: "in_memory", evaluation_snapshot: null })
        ]);
        if (cancelled) {
          return;
        }

        setActiveSelectionSnapshot(selectionDetail.selection);
        setActiveEvaluationSnapshot(evaluationDetail.evaluation_snapshot);

        const trainingRunId =
          selectionDetail.selection?.model_training_run_id ??
          evaluationDetail.evaluation_snapshot?.model_training_run_id ??
          null;

        if (trainingRunId === null) {
          setActiveModelRun(null);
          return;
        }

        const modelRunDetail = await fetchModelRunDetail(trainingRunId);
        if (!cancelled) {
          setActiveModelRun(modelRunDetail.model_run);
        }
      } catch (detailError) {
        if (!cancelled) {
          setActiveSelectionSnapshot(null);
          setActiveEvaluationSnapshot(null);
          setActiveModelRun(null);
          setError(
            detailError instanceof Error
              ? detailError.message
              : "Failed to load model provenance details."
          );
        }
      }
    }

    void loadModelArtifactDetails();

    return () => {
      cancelled = true;
    };
  }, [activeOpportunity]);

  useEffect(() => {
    let cancelled = false;

    async function loadScoringRunDetail() {
      if (!activeOpportunity || activeOpportunity.source_kind !== "future_scenario" || !activeOpportunity.model_scoring_run_id) {
        setActiveScoringRun(null);
        return;
      }

      const scenario = asRecord(readNested(activeOpportunity.payload, "scenario"));
      if (!scenario) {
        setActiveScoringRun(null);
        return;
      }

      try {
        const detail = await fetchScoringRunDetail(activeOpportunity.model_scoring_run_id, {
          ...scenario,
          target_task: activeOpportunity.target_task
        });
        if (!cancelled) {
          setActiveScoringRun(detail.scoring_run);
        }
      } catch {
        if (!cancelled) {
          setActiveScoringRun(null);
        }
      }
    }

    void loadScoringRunDetail();

    return () => {
      cancelled = true;
    };
  }, [activeOpportunity]);

  async function handleRunBacktest() {
    try {
      setRunning(true);
      setError(null);
      const result = await runBacktest();
      setActiveRun(result.backtest_run);
      const refreshedHistory = await fetchBacktestHistory();
      setHistory(refreshedHistory);
    } catch (runError) {
      setError(runError instanceof Error ? runError.message : "Failed to run backtest.");
    } finally {
      setRunning(false);
    }
  }

  async function handleMaterializeOpportunities() {
    try {
      setMaterializingOpportunity(true);
      setError(null);
      const materialized = await materializeOpportunities();
      const [nextOpportunityHistory, nextOpportunityList] = await Promise.all([
        fetchOpportunityHistory(),
        fetchOpportunities()
      ]);
      setOpportunityHistory(nextOpportunityHistory);
      setOpportunityList(nextOpportunityList);
      setOpportunities(nextOpportunityList.opportunities);
      const nextOpportunityId = materialized.opportunities[0]?.id ?? nextOpportunityList.opportunities[0]?.id ?? null;
      setActiveOpportunityId(nextOpportunityId);
      if (nextOpportunityId !== null) {
        navigate({ name: "opportunity-detail", opportunityId: nextOpportunityId });
      }
    } catch (materializeError) {
      setError(
        materializeError instanceof Error ? materializeError.message : "Failed to materialize opportunities."
      );
    } finally {
      setMaterializingOpportunity(false);
    }
  }

  const overview = history?.model_backtest_history.overview;
  const opportunityOverview = opportunityHistory?.model_opportunity_history.overview;
  const inModelAdminDashboard = route.name === "models";
  const inModelRegistry = route.name === "model-registry";
  const inModelRuns = route.name === "model-runs";
  const inModelRunDetailRoute = route.name === "model-run-detail";
  const inModelEvaluations = route.name === "model-evaluations";
  const inModelEvaluationDetailRoute = route.name === "model-evaluation-detail";
  const inModelSelections = route.name === "model-selections";
  const inModelSelectionDetailRoute = route.name === "model-selection-detail";
  const inModelAdmin =
    inModelAdminDashboard ||
    inModelRegistry ||
    inModelRuns ||
    inModelRunDetailRoute ||
    inModelEvaluations ||
    inModelEvaluationDetailRoute ||
    inModelSelections ||
    inModelSelectionDetailRoute;
  const viewMode =
    inModelAdmin
      ? "models"
      : route.name === "backtests" ||
          route.name === "backtest-run" ||
          route.name === "backtest-fold" ||
          route.name === "backtest-fold-model-run" ||
          route.name === "backtest-fold-evaluation" ||
          route.name === "artifact-compare"
      ? "backtests"
      : "opportunities";
  const inBacktestDetail = route.name === "backtest-run";
  const inBacktestFoldDetail = route.name === "backtest-fold";
  const inBacktestFoldModelRunDetail = route.name === "backtest-fold-model-run";
  const inBacktestFoldEvaluationDetail = route.name === "backtest-fold-evaluation";
  const inArtifactCompare = route.name === "artifact-compare";
  const inBacktestArtifactDetail =
    inBacktestFoldModelRunDetail || inBacktestFoldEvaluationDetail;
  const inOpportunityDetail = route.name === "opportunity-detail";
  const inComparableCase = route.name === "comparable-case";
  const inModelRunDetail = route.name === "opportunity-model-run";
  const inSelectionDetail = route.name === "opportunity-selection";
  const inEvaluationDetail = route.name === "opportunity-evaluation";
  const inScoringRunDetail = route.name === "opportunity-scoring-run";
  const inOpportunityArtifactDetail =
    inModelRunDetail || inSelectionDetail || inEvaluationDetail || inScoringRunDetail;
  const inOpportunityContextDetail =
    inOpportunityDetail || inComparableCase || inOpportunityArtifactDetail;
  const heroTitle =
    route.name === "models"
      ? "Model admin workspace shell is online."
      : inModelRegistry
        ? "Model registry route shell is online."
        : inModelRuns
          ? "Training runs route shell is online."
          : inModelRunDetailRoute
            ? "Training run detail route shell is online."
            : inModelEvaluations
              ? "Evaluation route shell is online."
              : inModelEvaluationDetailRoute
                ? "Evaluation detail route shell is online."
                : inModelSelections
                  ? "Selection route shell is online."
                  : inModelSelectionDetailRoute
                    ? "Selection detail route shell is online."
                    : route.name === "backtests"
      ? "Phase 4 backtest console is live."
      : inBacktestDetail
        ? "Backtest run inspection is open."
      : inBacktestFoldDetail
        ? "Backtest fold inspection is open."
      : inBacktestFoldModelRunDetail
        ? "Backtest fold training provenance is open."
      : inBacktestFoldEvaluationDetail
        ? "Backtest fold evaluation provenance is open."
      : inArtifactCompare
        ? "Artifact comparison is open."
      : inModelRunDetail
        ? "Training run provenance is open."
      : inSelectionDetail
        ? "Selection snapshot provenance is open."
      : inEvaluationDetail
        ? "Evaluation snapshot provenance is open."
      : inScoringRunDetail
        ? "Scoring run provenance is open."
      : inComparableCase
        ? "Comparable case inspection is open."
        : inOpportunityDetail
        ? "Opportunity investigation is open."
        : "Analyst opportunity desk is online.";
  const heroLead =
    route.name === "models"
      ? "This workspace is the dedicated shell for model lifecycle administration. Phase 1 locks the route family and component ownership so the training console can grow without adding more workspace logic to the main app shell."
      : inModelRegistry
        ? "This route reserves the model registry workspace surface. The next phase will add filtering, inline registry inspection, and reusable admin artifact cards."
        : inModelRuns
          ? "This route reserves the training run list surface. The next phase will add list filters, recent-run rendering, and detail navigation."
          : inModelRunDetailRoute
            ? "This route reserves the training run detail surface. The next phase will add the dedicated run artifact and metrics view."
            : inModelEvaluations
              ? "This route reserves the evaluation list surface. The next phase will add metric-first list rendering and detail navigation."
              : inModelEvaluationDetailRoute
                ? "This route reserves the evaluation detail surface. The next phase will add selected-feature, fallback, and prediction-count inspection."
                : inModelSelections
                  ? "This route reserves the selection list surface. The next phase will add active-selection indicators, filter controls, and historical selection rendering."
                  : inModelSelectionDetailRoute
                    ? "This route reserves the selection detail surface. The next phase will add policy and rationale rendering for promoted model snapshots."
                    : route.name === "backtests"
      ? "This dashboard runs and inspects the first walk-forward validation layer on top of the predictive stack. It shows whether the current spread or totals edge logic holds up once we retrain chronologically and simulate threshold-based decisions."
      : inBacktestDetail
        ? "This route focuses on one exact walk-forward validation run, so provenance links can target the specific model-selection and threshold simulation result behind a review workflow."
      : inBacktestFoldDetail
        ? "This route focuses on one exact walk-forward fold, so you can inspect the selected model, game split, and threshold outcomes behind a single chronological validation step."
      : inBacktestFoldModelRunDetail
        ? "This route resolves the exact training run chosen inside a walk-forward fold, so the backtest side can inspect the concrete fitted artifact behind the fold decision."
      : inBacktestFoldEvaluationDetail
        ? "This route resolves the exact evaluation snapshot chosen inside a walk-forward fold, so the backtest side can inspect the concrete metric record behind the fold decision."
      : inArtifactCompare
        ? "This route compares one backtest fold’s evaluation artifact against the active opportunity evaluation and promoted selection, so analysts can inspect where validation and live scoring line up or drift."
      : inModelRunDetail
        ? "This route resolves the exact training run behind the selected opportunity, including the chosen feature, fallback behavior, and validation metrics that shaped the downstream signal."
      : inSelectionDetail
        ? "This route focuses on the promoted selection snapshot, so you can inspect the exact policy decision that made one trained model active for scoring."
      : inEvaluationDetail
        ? "This route focuses on the exact evaluation snapshot behind the active selection, so you can inspect the metric values and prediction counts that justified promotion."
      : inScoringRunDetail
        ? "This route focuses on the exact scoring run behind a future-style opportunity, so you can inspect the market scenario and generated opportunity counts without staying inside the parent card."
      : inComparableCase
        ? "This route focuses on one comparable historical case from the evidence bundle, so you can inspect exactly why it matched the parent opportunity and what it contributed to the analyst judgment."
        : inOpportunityDetail
        ? "This route is the analyst deep-dive for one materialized opportunity. It keeps the evidence bundle, comparables, benchmark context, and model provenance in one inspectable workspace."
        : "This view turns the Phase 3 scoring pipeline into an analyst workflow. It surfaces recent opportunities, keeps the evidence bundle attached, and lets you inspect why a case is only reviewable or strong enough to escalate.";
  const activeServicePath =
    viewMode === "models"
      ? `${apiBaseUrl}/api/v1/admin/models`
      : viewMode === "backtests"
      ? `${apiBaseUrl}/api/v1/analyst/backtests`
      : `${apiBaseUrl}/api/v1/analyst/opportunities`;
  const modelSectionRoute =
    route.name === "model-run-detail"
      ? ({ name: "model-runs" } as const)
      : route.name === "model-evaluation-detail"
        ? ({ name: "model-evaluations" } as const)
        : route.name === "model-selection-detail"
          ? ({ name: "model-selections" } as const)
          : null;
  const backtestOverviewHref = routeHash({ name: "backtests" });
  const activeRunHref = activeRun ? routeHash({ name: "backtest-run", runId: activeRun.id }) : undefined;
  const activeOpportunityHref =
    activeOpportunityId !== null
      ? routeHash({ name: "opportunity-detail", opportunityId: activeOpportunityId })
      : undefined;
  const activeModelRunHref =
    activeOpportunityId !== null && activeModelRun
      ? routeHash({
          name: "opportunity-model-run",
          opportunityId: activeOpportunityId,
          runId: activeModelRun.id
        })
      : undefined;
  const activeSelectionHref =
    activeOpportunityId !== null && activeSelectionSnapshot
      ? routeHash({
          name: "opportunity-selection",
          opportunityId: activeOpportunityId,
          selectionId: activeSelectionSnapshot.id
        })
      : undefined;
  const activeEvaluationHref =
    activeOpportunityId !== null && activeEvaluationSnapshot
      ? routeHash({
          name: "opportunity-evaluation",
          opportunityId: activeOpportunityId,
          evaluationId: activeEvaluationSnapshot.id
        })
      : undefined;
  const activeScoringRunHref =
    activeOpportunityId !== null && activeScoringRun
      ? routeHash({
          name: "opportunity-scoring-run",
          opportunityId: activeOpportunityId,
          scoringRunId: activeScoringRun.id
        })
      : undefined;
  const backtestProvenanceItems: ProvenanceItem[] = activeRun
    ? [
        { href: backtestOverviewHref, label: "Dashboard", value: "Backtest overview" },
        { href: activeRunHref, label: "Run route", value: `Run #${activeRun.id}` },
        { label: "Selection policy", value: activeRun.selection_policy_name },
        { label: "Completed", value: formatTimestamp(activeRun.completed_at) }
      ]
    : [];
  const activeFold =
    route.name === "backtest-fold" ||
    route.name === "backtest-fold-model-run" ||
    route.name === "backtest-fold-evaluation" ||
    route.name === "artifact-compare"
      ? activeRun?.payload.folds.find((fold) => fold.fold_index === route.foldIndex) ?? null
      : null;
  const artifactCompareHref =
    activeRun && activeFold && activeOpportunityId !== null
      ? routeHash({
          name: "artifact-compare",
          runId: activeRun.id,
          foldIndex: activeFold.fold_index,
          opportunityId: activeOpportunityId
        })
      : undefined;
  const backtestFoldProvenanceItems: ProvenanceItem[] = activeRun
    ? [
        { href: backtestOverviewHref, label: "Dashboard", value: "Backtest overview" },
        { href: activeRunHref, label: "Run route", value: `Run #${activeRun.id}` },
        ...(activeFold
          ? [
              {
                href: routeHash({
                  name: "backtest-fold",
                  runId: activeRun.id,
                  foldIndex: activeFold.fold_index
                }),
                label: "Fold",
                value: `Fold ${activeFold.fold_index}`
              }
            ]
          : []),
        ...(activeFold && activeFold.selected_model.model_training_run_id > 0
          ? [
              {
                href: routeHash({
                  name: "backtest-fold-model-run",
                  runId: activeRun.id,
                  foldIndex: activeFold.fold_index,
                  modelRunId: activeFold.selected_model.model_training_run_id
                }),
                label: "Training run",
                value: `Run #${activeFold.selected_model.model_training_run_id}`
              }
            ]
          : []),
        ...(activeFold
          ? [
              {
                href: routeHash({
                  name: "backtest-fold-evaluation",
                  runId: activeRun.id,
                  foldIndex: activeFold.fold_index,
                  evaluationId: activeFold.selected_model.evaluation_snapshot_id
                }),
                label: "Evaluation",
                value: `Evaluation #${activeFold.selected_model.evaluation_snapshot_id}`
              }
            ]
          : [])
      ]
    : [];
  const opportunityProvenanceItems: ProvenanceItem[] = activeOpportunity
    ? [
        ...(activeRunHref
          ? [{ href: activeRunHref, label: "Validation run", value: `Run #${activeRun?.id}` }]
          : [{ href: backtestOverviewHref, label: "Validation", value: "Backtest overview" }]),
        ...(activeOpportunity.model_selection_snapshot_id
          ? [
              {
                href: activeSelectionHref,
                label: "Selection snapshot",
                value: `Selection #${activeOpportunity.model_selection_snapshot_id}`
              }
            ]
          : []),
        ...(activeOpportunity.model_evaluation_snapshot_id
          ? [
              {
                href: activeEvaluationHref,
                label: "Evaluation snapshot",
                value: `Evaluation #${activeOpportunity.model_evaluation_snapshot_id}`
              }
            ]
          : []),
        ...(activeModelRun
          ? [{ href: activeModelRunHref, label: "Training run", value: `Run #${activeModelRun.id}` }]
          : []),
        ...(activeOpportunity.model_scoring_run_id
          ? [{ href: activeScoringRunHref, label: "Scoring run", value: `Scoring #${activeOpportunity.model_scoring_run_id}` }]
          : []),
        ...(activeOpportunity.feature_version_id
          ? [{ label: "Feature version", value: `Version #${activeOpportunity.feature_version_id}` }]
          : [])
      ]
    : [];
  const comparableProvenanceItems: ProvenanceItem[] = [
    ...(activeOpportunityHref
      ? [{ href: activeOpportunityHref, label: "Parent opportunity", value: `Opportunity #${activeOpportunityId}` }]
      : []),
    ...opportunityProvenanceItems
  ];
  const opportunityProvenanceData: ProvenanceInspectorData = {
    evaluation: activeEvaluationSnapshot,
    modelHistory: null,
    modelRun: activeModelRun,
    scoringRun: activeScoringRun,
    selection: activeSelectionSnapshot
  };
  const backtestProvenanceData: ProvenanceInspectorData = {
    evaluation: null,
    modelHistory,
    modelRun: null,
    scoringRun: null,
    selection: null
  };
  const comparableProvenanceData: ProvenanceInspectorData = {
    evaluation: activeEvaluationSnapshot,
    modelHistory: null,
    modelRun: activeModelRun,
    scoringRun: activeScoringRun,
    selection: activeSelectionSnapshot
  };

  return (
    <main className="app-shell" data-testid="app-shell">
      <section className="hero">
        <div className="hero-copy">
          <p className="eyebrow">Bookmaker Mistake Detector</p>
          <h1>{heroTitle}</h1>
          <p className="lead">{heroLead}</p>
        </div>

        <div className="hero-actions">
          <div className="mode-switch" data-testid="model-admin-nav">
            <button
              data-testid="nav-backtests"
              className={`mode-button${viewMode === "backtests" ? " mode-button-active" : ""}`}
              onClick={() => navigate({ name: "backtests" })}
              type="button"
            >
              Backtests
            </button>
            <button
              data-testid="nav-opportunities"
              className={`mode-button${viewMode === "opportunities" ? " mode-button-active" : ""}`}
              onClick={() => navigate({ name: "opportunities" })}
              type="button"
            >
              Opportunities
            </button>
            <button
              data-testid="nav-model-admin"
              className={`mode-button${viewMode === "models" ? " mode-button-active" : ""}`}
              onClick={() => navigate({ name: "models" })}
              type="button"
            >
              Model Admin
            </button>
          </div>

          {viewMode === "backtests" ? (
            <button
              className="primary-button"
              data-testid="run-backtest-button"
              disabled={running}
              onClick={() => void handleRunBacktest()}
            >
              {running ? "Running backtest..." : "Run new backtest"}
            </button>
          ) : viewMode === "models" ? (
            <button
              className="primary-button"
              data-testid="open-model-dashboard-button"
              onClick={() => navigate({ name: "models" })}
              type="button"
            >
              Open model dashboard
            </button>
          ) : (
            <button
              className="primary-button"
              data-testid="opportunities-refresh-button"
              disabled={materializingOpportunity}
              onClick={() => void handleMaterializeOpportunities()}
            >
              {materializingOpportunity ? "Refreshing opportunities..." : "Materialize opportunities"}
            </button>
          )}

          <p className="service-note">API target: {activeServicePath}</p>
        </div>
      </section>

      {inBacktestDetail ||
      inBacktestFoldDetail ||
      inBacktestArtifactDetail ||
      inArtifactCompare ||
      inOpportunityContextDetail ||
      (inModelAdmin && !inModelAdminDashboard) ? (
        <section className="route-toolbar">
          {inModelAdmin && !inModelAdminDashboard ? (
            <button className="secondary-button" onClick={() => navigate({ name: "models" })} type="button">
              Back to model dashboard
            </button>
          ) : inBacktestDetail || inBacktestFoldDetail || inBacktestArtifactDetail || inArtifactCompare ? (
            <button className="secondary-button" onClick={() => navigate({ name: "backtests" })} type="button">
              Back to runs
            </button>
          ) : (
            <button className="secondary-button" onClick={() => navigate({ name: "opportunities" })} type="button">
              Back to queue
            </button>
          )}
          {(inBacktestFoldDetail || inBacktestArtifactDetail || inArtifactCompare) && activeRun ? (
            <button
              className="secondary-button"
              onClick={() => navigate({ name: "backtest-run", runId: activeRun.id })}
              type="button"
            >
              Back to run
            </button>
          ) : null}
          {(inBacktestArtifactDetail || inArtifactCompare) && activeRun && activeFold ? (
            <button
              className="secondary-button"
              onClick={() =>
                navigate({
                  name: "backtest-fold",
                  runId: activeRun.id,
                  foldIndex: activeFold.fold_index
                })
              }
              type="button"
            >
              Back to fold
            </button>
          ) : null}
          {inComparableCase || inOpportunityArtifactDetail || inArtifactCompare ? (
            <button
              className="secondary-button"
              onClick={() =>
                activeOpportunityId !== null
                  ? navigate({ name: "opportunity-detail", opportunityId: activeOpportunityId })
                  : navigate({ name: "opportunities" })
              }
              type="button"
            >
              Back to opportunity
            </button>
          ) : null}
          {modelSectionRoute ? (
            <button
              className="secondary-button"
              onClick={() => navigate(modelSectionRoute)}
              type="button"
            >
              Back to section
            </button>
          ) : null}
          {!inBacktestDetail && !inBacktestFoldDetail && !inBacktestArtifactDetail && !inArtifactCompare ? (
            inModelAdmin ? (
              <button className="secondary-button" onClick={() => navigate({ name: "opportunities" })} type="button">
                Open opportunities
              </button>
            ) : (
              <button className="secondary-button" onClick={() => navigate({ name: "backtests" })} type="button">
                Open backtests
              </button>
            )
          ) : null}
          {inModelAdmin && !inModelAdminDashboard ? (
            <p className="route-note">Model Admin workspace | {route.name}</p>
          ) : null}
          {(inBacktestDetail || inBacktestFoldDetail || inBacktestArtifactDetail || inArtifactCompare) && activeRun ? (
            <p className="route-note">
              Run #{activeRun.id} | {activeRun.payload.strategy_name} | {activeRun.fold_count} folds
            </p>
          ) : null}
          {!inBacktestDetail && activeOpportunity ? (
            <p className="route-note">
              Opportunity #{activeOpportunity.id} | {activeOpportunity.team_code} vs {activeOpportunity.opponent_code}
            </p>
          ) : null}
        </section>
      ) : null}

      {error ? <section className="banner banner-error">{error}</section> : null}
      {loading ? <section className="banner">Loading the Phase 4 analyst workspace...</section> : null}

      {!loading && viewMode === "backtests" && history && overview ? (
        <BacktestsWorkspace
          activeRun={activeRun}
          detailContent={
            route.name === "backtest-run" ? (
              <section className="section-stack">
                <SharedBacktestRunDetailCard
                  provenanceData={backtestProvenanceData}
                  provenanceItems={backtestProvenanceItems}
                  run={activeRun}
                />
              </section>
            ) : route.name === "backtest-fold" ? (
              <section className="section-stack">
                <SharedFoldDetailCard
                  compareHref={artifactCompareHref}
                  fold={activeFold}
                  provenanceItems={backtestFoldProvenanceItems}
                  runId={activeRun?.id ?? null}
                />
              </section>
            ) : route.name === "backtest-fold-model-run" ? (
              <section className="section-stack">
                <SharedModelRunArtifactDetail
                  modelRun={activeBacktestFoldModelRun}
                  provenanceItems={backtestFoldProvenanceItems}
                />
              </section>
            ) : route.name === "backtest-fold-evaluation" ? (
              <section className="section-stack">
                <SharedEvaluationArtifactDetail
                  evaluation={activeBacktestFoldEvaluation}
                  provenanceItems={backtestFoldProvenanceItems}
                />
              </section>
            ) : route.name === "artifact-compare" ? (
              <section className="section-stack">
                <SharedArtifactCompareView
                  compareHref={artifactCompareHref}
                  fold={activeFold}
                  foldEvaluation={activeBacktestFoldEvaluation}
                  opportunity={activeOpportunity}
                  opportunityEvaluation={activeEvaluationSnapshot}
                  runId={activeRun?.id ?? null}
                  selection={activeSelectionSnapshot}
                />
              </section>
            ) : undefined
          }
          history={history}
          onNavigate={navigate}
          route={route}
        />
      ) : null}

      {!loading && viewMode === "opportunities" && opportunityHistory && opportunityOverview && opportunityList ? (
        <OpportunitiesWorkspace
          activeOpportunityId={activeOpportunityId}
          detailContent={
            route.name === "opportunities" ? (
              <SharedOpportunityDetailCard
                compareHref={artifactCompareHref}
                onSelectComparable={(comparableIndex) => {
                  if (activeOpportunityId !== null) {
                    navigate({ name: "comparable-case", opportunityId: activeOpportunityId, comparableIndex });
                  }
                }}
                provenanceData={opportunityProvenanceData}
                provenanceItems={opportunityProvenanceItems}
                opportunity={activeOpportunity}
              />
            ) : route.name === "comparable-case" ? (
              <section className="section-stack">
                <SharedComparableCaseDetail
                  comparableIndex={route.comparableIndex}
                  provenanceData={comparableProvenanceData}
                  provenanceItems={comparableProvenanceItems}
                  opportunity={activeOpportunity}
                />
              </section>
            ) : route.name === "opportunity-model-run" ? (
              <section className="section-stack">
                <SharedModelRunArtifactDetail
                  modelRun={activeModelRun}
                  provenanceItems={opportunityProvenanceItems}
                />
              </section>
            ) : route.name === "opportunity-selection" ? (
              <section className="section-stack">
                <SharedSelectionArtifactDetail
                  provenanceItems={opportunityProvenanceItems}
                  selection={activeSelectionSnapshot}
                />
              </section>
            ) : route.name === "opportunity-evaluation" ? (
              <section className="section-stack">
                <SharedEvaluationArtifactDetail
                  evaluation={activeEvaluationSnapshot}
                  provenanceItems={opportunityProvenanceItems}
                />
              </section>
            ) : route.name === "opportunity-scoring-run" ? (
              <section className="section-stack">
                <SharedScoringRunArtifactDetail
                  provenanceItems={opportunityProvenanceItems}
                  scoringRun={activeScoringRun}
                />
              </section>
            ) : (
              <section className="section-stack">
                <SharedOpportunityDetailCard
                  compareHref={artifactCompareHref}
                  onSelectComparable={(comparableIndex) => {
                    if (activeOpportunityId !== null) {
                      navigate({ name: "comparable-case", opportunityId: activeOpportunityId, comparableIndex });
                    }
                  }}
                  provenanceData={opportunityProvenanceData}
                  provenanceItems={opportunityProvenanceItems}
                  opportunity={activeOpportunity}
                />
              </section>
            )
          }
          onSelectOpportunity={handleSelectOpportunity}
          opportunityList={opportunityList}
          opportunities={opportunities}
          opportunityHistory={opportunityHistory}
          showQueueDetail={route.name === "opportunities"}
        />
      ) : null}

      {!loading && viewMode === "models" ? (
        <ModelAdminWorkspace modelHistory={modelHistory} onNavigate={navigate} route={route} />
      ) : null}
    </main>
  );
}

