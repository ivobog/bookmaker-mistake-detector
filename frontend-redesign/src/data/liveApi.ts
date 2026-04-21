import { fetchJson } from "./http";
import { buildMockAppData } from "./mockData";
import type { AppData, CandidateCard, TaskOption, WorkflowStatus } from "../types";
import {
  formatRelativeTime,
  getEvidenceLabel,
  getModelFamilyLabel,
  getModelMetricDisplay,
  getOpportunityMarketLabel,
  getOpportunityMatchupLabel,
  getRecommendationLabel,
  getSignalStrengthLabel,
  getWorkflowFreshnessStatus
} from "../../../shared/frontend/domain";
import type {
  SharedBacktestHistoryResponse as BacktestHistoryResponse,
  SharedCapabilitiesResponse as CapabilityResponse,
  SharedModelHistoryResponse as ModelHistoryResponse,
  SharedModelSummaryEnvelope as ModelSummaryResponse,
  SharedOpportunityHistoryResponse as OpportunityHistoryResponse,
  SharedOpportunityListResponse as OpportunityListResponse,
  SharedSelectionHistoryEnvelope as SelectionHistoryResponse
} from "../../../shared/frontend/apiTypes";

function buildCandidateCards(history: ModelHistoryResponse | null): CandidateCard[] {
  if (!history || history.model_history.recent_runs.length === 0) {
    return buildMockAppData().trainingLab.candidates;
  }

  return history.model_history.recent_runs.slice(0, 3).map((run, index) => ({
    name: `Run #${run.id}`,
    modelFamily: getModelFamilyLabel(run, "candidate"),
    status: index === 0 ? "Fresh candidate" : "Recent run",
    metricLabel: "Selection score",
    metricValue: getModelMetricDisplay(run),
    whyItMatters: `Task ${run.target_task} completed with status ${run.status.toLowerCase()}.`,
    tags: [run.target_task, run.status],
    runId: run.id,
    targetTaskKey: run.target_task,
    evidence: [
      `Latest known backend status is ${run.status.toLowerCase()}.`,
      "Candidate card was derived from recent run history in the live API.",
      "Open deeper artifact detail in the legacy app if you need raw metrics beyond this redesign summary."
    ],
    provenance: [
      { label: "Run ID", value: String(run.id) },
      { label: "Task", value: run.target_task },
      { label: "Family", value: getModelFamilyLabel(run, "candidate") }
    ],
    nextActions: ["Compare against the active release", "Review validation evidence", "Decide whether to activate"]
  }));
}

function buildSignalCards(opportunities: OpportunityListResponse | null): AppData["signalsDesk"]["signals"] {
  if (!opportunities || opportunities.opportunities.length === 0) {
    return buildMockAppData().signalsDesk.signals;
  }

  return opportunities.opportunities.slice(0, 6).map((item) => ({
    id: `SIG-${item.id}`,
    game: getOpportunityMatchupLabel(item),
    market: getOpportunityMarketLabel(item.target_task),
    signalStrength: getSignalStrengthLabel(item.evidence_rating),
    evidenceRating: getEvidenceLabel(item.evidence_rating),
    status: getRecommendationLabel(item.recommendation_status),
    recommendation: "Review candidate",
    summary: `Live opportunity ${item.id} is available for analyst triage in the redesigned queue.`,
    tags: [item.target_task, item.recommendation_status ?? "pending"],
    opportunityId: item.id,
    scoringRunId: item.model_scoring_run_id ?? null,
    targetTaskKey: item.target_task,
    evidence: [
      `Evidence rating from the live payload is ${item.evidence_rating ?? "unknown"}.`,
      "This signal card was normalized from the analyst opportunities endpoint.",
      "Use this drill-down to triage quickly before jumping into deeper provenance."
    ],
    provenance: [
      { label: "Opportunity ID", value: String(item.id) },
      { label: "Task", value: item.target_task },
      { label: "Status", value: getRecommendationLabel(item.recommendation_status) }
    ],
    nextActions: ["Review queue status", "Inspect model rationale", "Escalate or leave in review"]
  }));
}

export async function loadLiveAppData(): Promise<AppData | null> {
  const mock = buildMockAppData();

  try {
    const capabilities = await fetchJson<CapabilityResponse>("/api/v1/admin/model-capabilities");
    const defaultTask = capabilities.ui_defaults.default_target_task ?? capabilities.target_tasks[0]?.task_key ?? null;
    const modelQuery = new URLSearchParams();
    if (defaultTask) {
      modelQuery.set("target_task", defaultTask);
    }
    modelQuery.set("recent_limit", "6");

    const requests = await Promise.allSettled([
      fetchJson<ModelSummaryResponse>("/api/v1/admin/models/summary", modelQuery),
      fetchJson<ModelHistoryResponse>("/api/v1/admin/models/history", modelQuery),
      fetchJson<SelectionHistoryResponse>(
        "/api/v1/admin/models/selections/history",
        new URLSearchParams([...modelQuery, ["active_only", "true"]])
      ),
      fetchJson<BacktestHistoryResponse>("/api/v1/admin/models/backtests/history", modelQuery),
      fetchJson<OpportunityHistoryResponse>("/api/v1/admin/models/opportunities/history", modelQuery),
      fetchJson<OpportunityListResponse>(
        "/api/v1/analyst/opportunities",
        new URLSearchParams([...modelQuery, ["limit", "6"]])
      )
    ]);

    const [summaryResult, historyResult, selectionResult, backtestResult, opportunityHistoryResult, opportunityListResult] =
      requests;
    const summary = summaryResult.status === "fulfilled" ? summaryResult.value : null;
    const history = historyResult.status === "fulfilled" ? historyResult.value : null;
    const selections = selectionResult.status === "fulfilled" ? selectionResult.value : null;
    const backtests = backtestResult.status === "fulfilled" ? backtestResult.value : null;
    const opportunityHistory = opportunityHistoryResult.status === "fulfilled" ? opportunityHistoryResult.value : null;
    const opportunityList = opportunityListResult.status === "fulfilled" ? opportunityListResult.value : null;

    const tasks: TaskOption[] =
      capabilities.target_tasks.length > 0
        ? capabilities.target_tasks.map((task) => ({
            key: task.task_key,
            label: task.label,
            description: task.description,
            metricName: task.primary_metric_name,
            defaultPolicy: task.default_selection_policy_name,
            semantics: task.scoring_output_semantics ?? "Live workflow scoring"
          }))
        : mock.tasks;

    const latestRun = summary?.model_summary.latest_run ?? null;
    const latestSelection = selections?.model_selection_history.overview.latest_selection ?? null;
    const latestOpportunity = opportunityHistory?.model_opportunity_history.overview.latest_opportunity ?? null;
    const activeSelectionCount = selections?.model_selection_history.overview.active_selection_count ?? 0;
    const candidateCards = buildCandidateCards(history);
    const signalCards = buildSignalCards(opportunityList);
    const activeTaskLabel = tasks.find((task) => task.key === defaultTask)?.label ?? defaultTask ?? "Unresolved";

    return {
      ...mock,
      mode: "live",
      sourceLabel: "Live backend snapshot",
      generatedAt: `Updated ${new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`,
      headline: "Workflow-first redesign loaded against the current backend contracts.",
      lead: "This shell is already reading live capability, training, selection, backtest, and opportunity data where available.",
      defaults: {
        featureKey: capabilities.ui_defaults.default_feature_key,
        targetTask: defaultTask,
        trainRatio: capabilities.ui_defaults.default_train_ratio,
        validationRatio: capabilities.ui_defaults.default_validation_ratio,
        seasonLabel: "2025-2026",
        sourceName: "demo_daily_lines_v1"
      },
      nextActionLabel: activeSelectionCount > 0 ? "Run today's slate" : "Activate a model",
      nextActionRoute: activeSelectionCount > 0 ? "slate-runner" : "model-decision",
      stats: [
        { label: "Target task", value: activeTaskLabel, tone: "blue" },
        { label: "Training runs", value: String(summary?.model_summary.run_count ?? 0), tone: "green" },
        { label: "Active selections", value: String(activeSelectionCount), tone: activeSelectionCount > 0 ? "green" : "amber" },
        { label: "Open opportunities", value: String(opportunityList?.opportunity_count ?? 0), tone: "amber" }
      ],
      tasks,
      workflow: [
        {
          id: "features",
          label: "Features ready",
          description: `Default feature key is ${capabilities.ui_defaults.default_feature_key}.`,
          status: capabilities.ui_defaults.default_feature_key ? "ready" : "missing",
          updatedAt: "Config-backed",
          ctaLabel: "Open Training Lab",
          route: "training-lab"
        },
        {
          id: "training",
          label: "Models trained",
          description: latestRun ? `Latest live run is #${latestRun.id} for ${latestRun.target_task}.` : "No completed training run found for the active task.",
          status: latestRun ? (getWorkflowFreshnessStatus(latestRun.completed_at ?? latestRun.created_at) as WorkflowStatus) : "missing",
          updatedAt: latestRun ? formatRelativeTime(latestRun.completed_at ?? latestRun.created_at) : "Never",
          ctaLabel: "Train new run",
          route: "training-lab"
        },
        {
          id: "activation",
          label: "Model activated",
          description: latestSelection
            ? `Active release exists for ${latestSelection.target_task} with model family ${latestSelection.model_family}.`
            : "No active release was discovered in selection history.",
          status: activeSelectionCount > 0 ? (getWorkflowFreshnessStatus(latestSelection?.created_at ?? null) as WorkflowStatus) : "missing",
          updatedAt: latestSelection ? formatRelativeTime(latestSelection.created_at) : "Never",
          ctaLabel: "Activate model",
          route: "model-decision"
        },
        {
          id: "slate",
          label: "Slate scored",
          description:
            (backtests?.model_backtest_history.overview.run_count ?? 0) > 0
              ? "Historical scoring evidence exists and can support today's slate workflow."
              : "No recent scoring or backtest history was found for the active task.",
          status:
            (opportunityList?.opportunity_count ?? 0) > 0
              ? "review"
              : (getWorkflowFreshnessStatus(backtests?.model_backtest_history.overview.latest_run?.completed_at ?? null) as WorkflowStatus),
          updatedAt: backtests?.model_backtest_history.overview.latest_run
            ? formatRelativeTime(backtests.model_backtest_history.overview.latest_run.completed_at)
            : "Never",
          ctaLabel: "Run slate",
          route: "slate-runner"
        },
        {
          id: "signals",
          label: "Signals reviewed",
          description:
            (opportunityList?.opportunity_count ?? 0) > 0
              ? `${opportunityList?.opportunity_count ?? 0} live opportunities are ready for analyst review.`
              : "No live opportunities are currently queued.",
          status: (opportunityList?.opportunity_count ?? 0) > 0 ? "review" : "missing",
          updatedAt: latestOpportunity ? formatRelativeTime(latestOpportunity.created_at) : "Never",
          ctaLabel: "Open Signals Desk",
          route: "signals-desk"
        }
      ],
      home: {
        ...mock.home,
        recentActivity: [
          {
            title: latestRun ? `Latest training run #${latestRun.id}` : "No recent training run",
            detail: latestRun
              ? `Live backend reports status ${latestRun.status.toLowerCase()} for ${latestRun.target_task}.`
              : "Train a fresh candidate to populate the redesign workflow strip.",
            timestamp: latestRun ? formatRelativeTime(latestRun.completed_at ?? latestRun.created_at) : "Now",
            tone: latestRun ? "green" : "amber"
          },
          {
            title: activeSelectionCount > 0 ? "Active model exists" : "Activation still needed",
            detail:
              activeSelectionCount > 0
                ? "Selection history confirms at least one active release."
                : "Model Decision should be the next stop before daily slate operations.",
            timestamp: latestSelection ? formatRelativeTime(latestSelection.created_at) : "Now",
            tone: activeSelectionCount > 0 ? "green" : "amber"
          },
          {
            title: `${opportunityList?.opportunity_count ?? 0} open live opportunities`,
            detail: "Signals Desk can already render current queue items from the analyst endpoint.",
            timestamp: latestOpportunity ? formatRelativeTime(latestOpportunity.created_at) : "Now",
            tone: (opportunityList?.opportunity_count ?? 0) > 0 ? "blue" : "slate"
          }
        ]
      },
      trainingLab: {
        ...mock.trainingLab,
        candidates: candidateCards
      },
      decision: {
        ...mock.decision,
        activeModel:
          activeSelectionCount > 0
            ? {
                name: `Active ${latestSelection?.model_family ?? "model"}`,
                modelFamily: latestSelection?.model_family ?? "selection",
                status: "Live selection",
                metricLabel: "Selection state",
                metricValue: "Active",
                whyItMatters: `Selection history confirms a live release for ${latestSelection?.target_task ?? activeTaskLabel}.`,
                tags: ["Live backend", "Active"],
                runId: latestSelection?.model_training_run_id ?? null,
                selectionId: latestSelection?.id ?? null,
                targetTaskKey: latestSelection?.target_task ?? defaultTask,
                evidence: [
                  "Selection history confirms at least one active release.",
                  "This summary was derived from the live selection-history endpoint.",
                  "Use the redesign panel for quick triage before opening legacy artifact detail."
                ],
                provenance: [
                  { label: "Task", value: latestSelection?.target_task ?? activeTaskLabel },
                  { label: "Family", value: latestSelection?.model_family ?? "selection" },
                  { label: "State", value: "Active" }
                ],
                nextActions: ["Preview today's slate", "Compare against the freshest candidate", "Confirm release timing"]
              }
            : mock.decision.activeModel,
        recommendedModel: candidateCards[0] ?? mock.decision.recommendedModel
      },
      slateRunner: {
        ...mock.slateRunner,
        queueSummary: {
          slateLabel: defaultTask ?? mock.slateRunner.queueSummary.slateLabel,
          openSignals: `${opportunityList?.opportunity_count ?? 0} live opportunities`,
          activeModel: activeSelectionCount > 0 ? latestSelection?.model_family ?? "Active release" : "No active model",
          note:
            (backtests?.model_backtest_history.overview.run_count ?? 0) > 0
              ? "Recent historical scoring evidence is available from the backend."
              : "No recent backtest support was found for the active task."
        }
      },
      signalsDesk: {
        ...mock.signalsDesk,
        signals: signalCards
      }
    };
  } catch {
    return null;
  }
}
