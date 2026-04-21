import { startTransition, useDeferredValue, useEffect, useMemo, useState } from "react";

import {
  hydrateCandidateDetail,
  hydrateDecisionDetail,
  hydrateSignalDetail
} from "./data/detailHydration";
import {
  activateModel,
  refreshFeatures,
  runSlate,
  trainRun,
  type WorkflowMutationResult
} from "./data/mutations";
import { useWorkflowData } from "./hooks/useWorkflowData";
import type {
  ActivityItem,
  CandidateCard,
  DecisionChecklistItem,
  FocusCard,
  PanelGroup,
  Preset,
  ReleaseHistoryItem,
  RouteKey,
  SignalCard,
  TaskOption,
  Tone,
  WorkflowStep
} from "./types";

export type RoutingAdapter = {
  getHash: () => string;
  pushHash: (nextHash: string) => void;
  replaceHash: (nextHash: string) => void;
  subscribe: (onChange: () => void) => () => void;
};

const browserRoutingAdapter: RoutingAdapter = {
  getHash: () => window.location.hash,
  pushHash: (nextHash) => {
    window.location.hash = nextHash;
  },
  replaceHash: (nextHash) => {
    window.history.replaceState(null, "", nextHash);
  },
  subscribe: (onChange) => {
    window.addEventListener("hashchange", onChange);
    return () => {
      window.removeEventListener("hashchange", onChange);
    };
  }
};

const primaryRoutes: Array<{ key: RouteKey; label: string }> = [
  { key: "home", label: "Home" },
  { key: "training-lab", label: "Training Lab" },
  { key: "model-decision", label: "Model Decision" },
  { key: "slate-runner", label: "Slate Runner" },
  { key: "signals-desk", label: "Signals Desk" }
];

const utilityRoutes: Array<{ key: RouteKey; label: string }> = [
  { key: "history", label: "History" },
  { key: "settings", label: "Settings" },
  { key: "help", label: "Help" }
];

type ActionKind = "refresh-features" | "train" | "activate" | "run-slate";
type DetailState = {
  title: string;
  subtitle: string;
  facts: Array<{ label: string; value: string }>;
  evidence: string[];
  nextActions: string[];
  loading: boolean;
  warning: string | null;
};

type HashState = {
  route: RouteKey;
  taskKey: string;
  trainingCandidate: string;
  trainingCompareCandidate: string;
  decisionCandidate: "active" | "recommended";
  signalId: string;
  signalQuery: string;
};

function parseHashState(hash: string): HashState {
  const normalized = hash.replace(/^#\/?/, "");
  const [routeSegment, querySegment = ""] = normalized.split("?");
  const candidate = routeSegment.trim();
  const known = [...primaryRoutes, ...utilityRoutes].map((route) => route.key);
  const route = known.includes(candidate as RouteKey) ? (candidate as RouteKey) : "home";
  const params = new URLSearchParams(querySegment);
  return {
    route,
    taskKey: params.get("task") ?? "",
    trainingCandidate: params.get("candidate") ?? "",
    trainingCompareCandidate: params.get("compare") ?? "",
    decisionCandidate: params.get("decision") === "active" ? "active" : "recommended",
    signalId: params.get("signal") ?? "",
    signalQuery: params.get("q") ?? ""
  };
}

function buildHash(state: HashState): string {
  const params = new URLSearchParams();
  if (state.taskKey) {
    params.set("task", state.taskKey);
  }
  if (state.route === "training-lab" && state.trainingCandidate) {
    params.set("candidate", state.trainingCandidate);
    if (state.trainingCompareCandidate) {
      params.set("compare", state.trainingCompareCandidate);
    }
  }
  if (state.route === "model-decision") {
    params.set("decision", state.decisionCandidate);
  }
  if (state.route === "signals-desk") {
    if (state.signalId) {
      params.set("signal", state.signalId);
    }
    if (state.signalQuery) {
      params.set("q", state.signalQuery);
    }
  }
  const query = params.toString();
  return `#/${state.route}${query ? `?${query}` : ""}`;
}

function toneClass(tone: Tone): string {
  return `tone-${tone}`;
}

function statusTone(status: WorkflowStep["status"]): Tone {
  switch (status) {
    case "ready":
    case "active":
      return "green";
    case "stale":
    case "review":
      return "amber";
    case "missing":
      return "red";
    default:
      return "blue";
  }
}

function checklistTone(state: DecisionChecklistItem["state"]): Tone {
  switch (state) {
    case "done":
      return "green";
    case "attention":
      return "amber";
    default:
      return "slate";
  }
}

function parseMetricNumber(value: string): number | null {
  const normalized = Number.parseFloat(value);
  return Number.isFinite(normalized) ? normalized : null;
}

function getCandidateFact(candidate: CandidateCard, label: string): string {
  return candidate.provenance.find((fact) => fact.label === label)?.value ?? "n/a";
}

function getDetailFact(detail: DetailState | null, label: string): string | null {
  return detail?.facts.find((fact) => fact.label === label)?.value ?? null;
}

function getAlternateCandidate(candidates: CandidateCard[], excludedName: string): CandidateCard | null {
  return candidates.find((candidate) => candidate.name !== excludedName) ?? null;
}

function describeComparison(activeModel: CandidateCard, recommendedModel: CandidateCard): {
  tone: Tone;
  headline: string;
  detail: string;
} {
  const activeMetric = parseMetricNumber(activeModel.metricValue);
  const recommendedMetric = parseMetricNumber(recommendedModel.metricValue);
  if (activeMetric !== null && recommendedMetric !== null) {
    const delta = Math.abs(activeMetric - recommendedMetric).toFixed(2);
    if (recommendedMetric < activeMetric) {
      return {
        tone: "green",
        headline: `Recommended candidate leads by ${delta} on ${recommendedModel.metricLabel}.`,
        detail: "The candidate is numerically ahead and still aligned with the current workflow posture."
      };
    }
    if (recommendedMetric > activeMetric) {
      return {
        tone: "amber",
        headline: `Active release still leads by ${delta} on ${activeModel.metricLabel}.`,
        detail: "Treat the recommendation carefully and confirm the release rationale before switching."
      };
    }
  }

  return {
    tone: "blue",
    headline: "Comparison is available, but no stable numeric delta could be resolved.",
    detail: "Use the evidence and provenance rows below to decide whether the newer candidate is truly ready."
  };
}

function describeTrainingComparison(primaryCandidate: CandidateCard, comparisonCandidate: CandidateCard): {
  tone: Tone;
  headline: string;
  detail: string;
} {
  const primaryMetric = parseMetricNumber(primaryCandidate.metricValue);
  const comparisonMetric = parseMetricNumber(comparisonCandidate.metricValue);
  if (primaryMetric !== null && comparisonMetric !== null) {
    const delta = Math.abs(primaryMetric - comparisonMetric).toFixed(2);
    if (primaryMetric < comparisonMetric) {
      return {
        tone: "green",
        headline: `${primaryCandidate.name} leads ${comparisonCandidate.name} by ${delta} on ${primaryCandidate.metricLabel}.`,
        detail: "This candidate is ahead numerically, so the remaining question is whether its evidence and release posture are also stronger."
      };
    }
    if (primaryMetric > comparisonMetric) {
      return {
        tone: "amber",
        headline: `${comparisonCandidate.name} still leads ${primaryCandidate.name} by ${delta} on ${comparisonCandidate.metricLabel}.`,
        detail: "Keep the current focus candidate in view, but verify why the alternate run is still outperforming it before promoting."
      };
    }
  }

  return {
    tone: "blue",
    headline: "Candidate comparison is available, but no stable numeric delta could be resolved.",
    detail: "Use provenance, run lineage, and evidence depth to decide which run deserves promotion."
  };
}

export default function App({ routingAdapter = browserRoutingAdapter }: { routingAdapter?: RoutingAdapter }) {
  const { data, loading, warning, reload } = useWorkflowData();
  const initialHashState = parseHashState(routingAdapter.getHash());
  const [route, setRoute] = useState<RouteKey>(initialHashState.route);
  const [signalQuery, setSignalQuery] = useState(initialHashState.signalQuery);
  const [selectedTaskKey, setSelectedTaskKey] = useState(initialHashState.taskKey);
  const [selectedTrainingCandidateName, setSelectedTrainingCandidateName] = useState(initialHashState.trainingCandidate);
  const [selectedTrainingCompareCandidateName, setSelectedTrainingCompareCandidateName] = useState(
    initialHashState.trainingCompareCandidate
  );
  const [selectedDecisionCandidateKey, setSelectedDecisionCandidateKey] =
    useState<"active" | "recommended">(initialHashState.decisionCandidate);
  const [selectedSignalId, setSelectedSignalId] = useState(initialHashState.signalId);
  const [actionState, setActionState] = useState<{ kind: ActionKind | null; busy: boolean }>({
    kind: null,
    busy: false
  });
  const [trainingDetail, setTrainingDetail] = useState<DetailState | null>(null);
  const [trainingCompareDetail, setTrainingCompareDetail] = useState<DetailState | null>(null);
  const [decisionDetail, setDecisionDetail] = useState<DetailState | null>(null);
  const [signalDetail, setSignalDetail] = useState<DetailState | null>(null);
  const [notice, setNotice] = useState<{
    tone: "success" | "warning" | "error";
    message: string;
  } | null>(null);
  const deferredSignalQuery = useDeferredValue(signalQuery);

  useEffect(() => {
    function handleHashChange() {
      const nextHashState = parseHashState(routingAdapter.getHash());
      startTransition(() => {
        setRoute(nextHashState.route);
        setSelectedTaskKey(nextHashState.taskKey);
        setSelectedTrainingCandidateName(nextHashState.trainingCandidate);
        setSelectedTrainingCompareCandidateName(nextHashState.trainingCompareCandidate);
        setSelectedDecisionCandidateKey(nextHashState.decisionCandidate);
        setSelectedSignalId(nextHashState.signalId);
        setSignalQuery(nextHashState.signalQuery);
      });
    }

    if (!routingAdapter.getHash()) {
      routingAdapter.pushHash(buildHash(parseHashState(routingAdapter.getHash())));
    }

    return routingAdapter.subscribe(handleHashChange);
  }, [routingAdapter]);

  function navigateTo(routeKey: RouteKey) {
    routingAdapter.pushHash(
      buildHash({
        route: routeKey,
        taskKey: selectedTaskKey,
        trainingCandidate: selectedTrainingCandidateName,
        trainingCompareCandidate: selectedTrainingCompareCandidateName,
        decisionCandidate: selectedDecisionCandidateKey,
        signalId: selectedSignalId,
        signalQuery
      })
    );
  }

  useEffect(() => {
    if (data.tasks.length === 0) {
      return;
    }
    if (data.tasks.some((task) => task.key === selectedTaskKey)) {
      return;
    }
    setSelectedTaskKey(data.defaults.targetTask ?? data.tasks[0].key);
  }, [data.defaults.targetTask, data.tasks, selectedTaskKey]);

  useEffect(() => {
    if (data.trainingLab.candidates.length === 0) {
      return;
    }
    if (data.trainingLab.candidates.some((candidate) => candidate.name === selectedTrainingCandidateName)) {
      return;
    }
    setSelectedTrainingCandidateName(data.trainingLab.candidates[0].name);
  }, [data.trainingLab.candidates, selectedTrainingCandidateName]);

  useEffect(() => {
    if (data.trainingLab.candidates.length < 2) {
      if (selectedTrainingCompareCandidateName) {
        setSelectedTrainingCompareCandidateName("");
      }
      return;
    }

    if (
      selectedTrainingCompareCandidateName &&
      selectedTrainingCompareCandidateName !== selectedTrainingCandidateName &&
      data.trainingLab.candidates.some((candidate) => candidate.name === selectedTrainingCompareCandidateName)
    ) {
      return;
    }

    const fallbackCompare = getAlternateCandidate(data.trainingLab.candidates, selectedTrainingCandidateName);
    setSelectedTrainingCompareCandidateName(fallbackCompare?.name ?? "");
  }, [data.trainingLab.candidates, selectedTrainingCandidateName, selectedTrainingCompareCandidateName]);

  const selectedTask = data.tasks.find((task) => task.key === selectedTaskKey) ?? data.tasks[0] ?? null;
  const selectedTrainingCandidate =
    data.trainingLab.candidates.find((candidate) => candidate.name === selectedTrainingCandidateName) ??
    data.trainingLab.candidates[0] ??
    null;
  const selectedTrainingCompareCandidate =
    data.trainingLab.candidates.find((candidate) => candidate.name === selectedTrainingCompareCandidateName) ??
    getAlternateCandidate(data.trainingLab.candidates, selectedTrainingCandidate?.name ?? "") ??
    null;
  const selectedDecisionCandidate =
    selectedDecisionCandidateKey === "active" ? data.decision.activeModel : data.decision.recommendedModel;

  const filteredSignals = useMemo(() => {
    const query = deferredSignalQuery.trim().toLowerCase();
    if (!query) {
      return data.signalsDesk.signals;
    }
    return data.signalsDesk.signals.filter((signal) =>
      [signal.id, signal.game, signal.market, signal.recommendation, signal.summary, ...signal.tags]
        .join(" ")
        .toLowerCase()
        .includes(query)
    );
  }, [data.signalsDesk.signals, deferredSignalQuery]);

  useEffect(() => {
    if (filteredSignals.length === 0) {
      return;
    }
    if (filteredSignals.some((signal) => signal.id === selectedSignalId)) {
      return;
    }
    setSelectedSignalId(filteredSignals[0].id);
  }, [filteredSignals, selectedSignalId]);

  useEffect(() => {
    const nextHash = buildHash({
      route,
      taskKey: selectedTaskKey,
      trainingCandidate: selectedTrainingCandidateName,
      trainingCompareCandidate: selectedTrainingCompareCandidateName,
      decisionCandidate: selectedDecisionCandidateKey,
      signalId: selectedSignalId,
      signalQuery
    });
    if (routingAdapter.getHash() !== nextHash) {
      routingAdapter.replaceHash(nextHash);
    }
  }, [
    routingAdapter,
    route,
    selectedTaskKey,
    selectedTrainingCandidateName,
    selectedTrainingCompareCandidateName,
    selectedDecisionCandidateKey,
    selectedSignalId,
    signalQuery
  ]);

  useEffect(() => {
    let cancelled = false;
    const base = selectedTrainingCandidate
      ? {
          title: selectedTrainingCandidate.name,
          subtitle: selectedTrainingCandidate.status,
          facts: selectedTrainingCandidate.provenance,
          evidence: selectedTrainingCandidate.evidence,
          nextActions: selectedTrainingCandidate.nextActions,
          loading: false,
          warning: null
        }
      : null;
    setTrainingDetail(base);

    if (!selectedTrainingCandidate || data.mode !== "live" || !selectedTrainingCandidate.runId) {
      return;
    }

    setTrainingDetail((current) => (current ? { ...current, loading: true, warning: null } : current));
    void hydrateCandidateDetail(selectedTrainingCandidate, data.defaults)
      .then((detail) => {
        if (cancelled || !detail) {
          return;
        }
        setTrainingDetail({
          ...detail,
          loading: false,
          warning: null
        });
      })
      .catch((error) => {
        if (cancelled) {
          return;
        }
        setTrainingDetail((current) =>
          current
            ? {
                ...current,
                loading: false,
                warning: error instanceof Error ? error.message : "Unable to load live candidate detail."
              }
            : current
        );
      });

    return () => {
      cancelled = true;
    };
  }, [data.defaults, data.mode, selectedTrainingCandidate]);

  useEffect(() => {
    let cancelled = false;
    const base = selectedTrainingCompareCandidate
      ? {
          title: selectedTrainingCompareCandidate.name,
          subtitle: selectedTrainingCompareCandidate.status,
          facts: selectedTrainingCompareCandidate.provenance,
          evidence: selectedTrainingCompareCandidate.evidence,
          nextActions: selectedTrainingCompareCandidate.nextActions,
          loading: false,
          warning: null
        }
      : null;
    setTrainingCompareDetail(base);

    if (!selectedTrainingCompareCandidate || data.mode !== "live" || !selectedTrainingCompareCandidate.runId) {
      return;
    }

    setTrainingCompareDetail((current) => (current ? { ...current, loading: true, warning: null } : current));
    void hydrateCandidateDetail(selectedTrainingCompareCandidate, data.defaults)
      .then((detail) => {
        if (cancelled || !detail) {
          return;
        }
        setTrainingCompareDetail({
          ...detail,
          loading: false,
          warning: null
        });
      })
      .catch((error) => {
        if (cancelled) {
          return;
        }
        setTrainingCompareDetail((current) =>
          current
            ? {
                ...current,
                loading: false,
                warning: error instanceof Error ? error.message : "Unable to load live comparison detail."
              }
            : current
        );
      });

    return () => {
      cancelled = true;
    };
  }, [data.defaults, data.mode, selectedTrainingCompareCandidate]);

  useEffect(() => {
    let cancelled = false;
    const base = selectedDecisionCandidate
      ? {
          title: selectedDecisionCandidate.name,
          subtitle: selectedDecisionCandidate.status,
          facts: selectedDecisionCandidate.provenance,
          evidence: selectedDecisionCandidate.evidence,
          nextActions: selectedDecisionCandidate.nextActions,
          loading: false,
          warning: null
        }
      : null;
    setDecisionDetail(base);

    if (
      !selectedDecisionCandidate ||
      data.mode !== "live" ||
      (!selectedDecisionCandidate.selectionId && !selectedDecisionCandidate.runId)
    ) {
      return;
    }

    setDecisionDetail((current) => (current ? { ...current, loading: true, warning: null } : current));
    void hydrateDecisionDetail(selectedDecisionCandidate, data.defaults)
      .then((detail) => {
        if (cancelled || !detail) {
          return;
        }
        setDecisionDetail({
          ...detail,
          loading: false,
          warning: null
        });
      })
      .catch((error) => {
        if (cancelled) {
          return;
        }
        setDecisionDetail((current) =>
          current
            ? {
                ...current,
                loading: false,
                warning: error instanceof Error ? error.message : "Unable to load live decision detail."
              }
            : current
        );
      });

    return () => {
      cancelled = true;
    };
  }, [data.defaults, data.mode, selectedDecisionCandidate]);

  const selectedSignal =
    filteredSignals.find((signal) => signal.id === selectedSignalId) ?? filteredSignals[0] ?? null;

  useEffect(() => {
    let cancelled = false;
    const base = selectedSignal
      ? {
          title: selectedSignal.game,
          subtitle: `${selectedSignal.id} | ${selectedSignal.status}`,
          facts: selectedSignal.provenance,
          evidence: selectedSignal.evidence,
          nextActions: selectedSignal.nextActions,
          loading: false,
          warning: null
        }
      : null;
    setSignalDetail(base);

    if (!selectedSignal || data.mode !== "live" || !selectedSignal.opportunityId) {
      return;
    }

    setSignalDetail((current) => (current ? { ...current, loading: true, warning: null } : current));
    void hydrateSignalDetail(selectedSignal, data.defaults)
      .then((detail) => {
        if (cancelled || !detail) {
          return;
        }
        setSignalDetail({
          ...detail,
          loading: false,
          warning: null
        });
      })
      .catch((error) => {
        if (cancelled) {
          return;
        }
        setSignalDetail((current) =>
          current
            ? {
                ...current,
                loading: false,
                warning: error instanceof Error ? error.message : "Unable to load live signal detail."
              }
            : current
        );
      });

    return () => {
      cancelled = true;
    };
  }, [data.defaults, data.mode, selectedSignal]);

  async function handleWorkflowAction(kind: ActionKind) {
    if (!selectedTask && kind !== "refresh-features") {
      setNotice({
        tone: "error",
        message: "Choose a target task before running workflow actions."
      });
      return;
    }

    if (data.mode !== "live") {
      const mockMessages: Record<ActionKind, string> = {
        "refresh-features": "Prototype mode simulated a feature refresh. Connect a live backend to persist snapshots.",
        train: `Prototype mode simulated a training run for ${selectedTask?.label ?? "the current task"}.`,
        activate: `Prototype mode simulated model activation using ${selectedTask?.defaultPolicy ?? "the default policy"}.`,
        "run-slate": "Prototype mode simulated today's slate orchestration and refreshed the analyst queue preview."
      };
      setNotice({ tone: "warning", message: mockMessages[kind] });
      return;
    }

    setActionState({ kind, busy: true });
    setNotice(null);

    try {
      let result: WorkflowMutationResult;
      switch (kind) {
        case "refresh-features":
          result = await refreshFeatures(data.defaults);
          break;
        case "train":
          result = await trainRun(data.defaults, selectedTask!.key);
          break;
        case "activate":
          result = await activateModel(data.defaults, selectedTask!.key, selectedTask!.defaultPolicy);
          break;
        case "run-slate":
          result = await runSlate(data.defaults, selectedTask!.key);
          break;
      }
      await reload();
      setNotice({ tone: "success", message: result.message });
      if (result.routeHint) {
        navigateTo(result.routeHint);
      }
    } catch (error) {
      setNotice({
        tone: "error",
        message: error instanceof Error ? error.message : "The workflow action failed."
      });
    } finally {
      setActionState({ kind: null, busy: false });
    }
  }

  return (
    <main className="shell">
      <aside className="rail">
        <div className="brand-block">
          <p className="eyebrow">Bookmaker Mistake Detector</p>
          <h1>Workflow Desk</h1>
          <p className="rail-copy">A clean-room frontend project shaped around operator intent and analyst follow-through.</p>
        </div>

        <nav className="nav-block" aria-label="Primary navigation">
          {primaryRoutes.map((item) => (
            <button
              key={item.key}
              className={`nav-button${route === item.key ? " nav-button-active" : ""}`}
              onClick={() => navigateTo(item.key)}
              type="button"
            >
              {item.label}
            </button>
          ))}
        </nav>

        <div className="utility-block">
          <p className="section-label">Utilities</p>
          {utilityRoutes.map((item) => (
            <button
              key={item.key}
              className={`utility-link${route === item.key ? " utility-link-active" : ""}`}
              onClick={() => navigateTo(item.key)}
              type="button"
            >
              {item.label}
            </button>
          ))}
        </div>
      </aside>

      <section className="content">
        <header className="hero-panel">
          <div>
            <p className="eyebrow">{data.sourceLabel}</p>
            <h2>{data.headline}</h2>
            <p className="hero-lead">{data.lead}</p>
          </div>

          <div className="hero-actions">
            <button className="primary-cta" onClick={() => navigateTo(data.nextActionRoute)} type="button">
              {data.nextActionLabel}
            </button>
            <p className="meta-note">{data.generatedAt}</p>
            <p className="meta-note">{data.mode === "live" ? "Live backend data" : "Mock redesign data"}</p>
          </div>
        </header>

        {warning ? <section className="banner banner-warning">{warning}</section> : null}
        {loading ? <section className="banner">Refreshing the redesign workspace...</section> : null}
        {notice ? <section className={`banner banner-${notice.tone}`}>{notice.message}</section> : null}

        <section className="stat-grid">
          {data.stats.map((stat) => (
            <article key={stat.label} className={`stat-card ${toneClass(stat.tone)}`}>
              <p className="section-label">{stat.label}</p>
              <strong>{stat.value}</strong>
            </article>
          ))}
        </section>

        <section className="workflow-strip">
          {data.workflow.map((step) => (
            <button
              key={step.id}
              className={`workflow-card ${toneClass(statusTone(step.status))}`}
              onClick={() => navigateTo(step.route)}
              type="button"
            >
              <div className="workflow-header">
                <span className="workflow-title">{step.label}</span>
                <span className="status-pill">{step.status}</span>
              </div>
              <p>{step.description}</p>
              <div className="workflow-footer">
                <span>{step.updatedAt}</span>
                <span>{step.ctaLabel}</span>
              </div>
            </button>
          ))}
        </section>

        <section className="context-bar panel-card tone-blue">
          <div>
            <p className="section-label">Workflow Context</p>
            <h3>{selectedTask?.label ?? "No task selected"}</h3>
            <p>
              {selectedTask?.description ??
                "Choose a target task to keep training, activation, and slate actions aligned."}
            </p>
          </div>
          <div className="chip-row">
            {data.tasks.map((task) => (
              <button
                key={task.key}
                className={`filter-chip filter-chip-button${selectedTask?.key === task.key ? " filter-chip-active" : ""}`}
                onClick={() => setSelectedTaskKey(task.key)}
                type="button"
              >
                {task.label}
              </button>
            ))}
          </div>
        </section>

        {route === "home" ? (
          <HomePage
            focusCards={data.home.focusCards}
            recentActivity={data.home.recentActivity}
            missionNotes={data.home.missionNotes}
            onNavigate={navigateTo}
          />
        ) : null}

        {route === "training-lab" ? (
          <TrainingLabPage
            busyAction={actionState.busy ? actionState.kind : null}
            candidates={data.trainingLab.candidates}
            comparisonCandidate={selectedTrainingCompareCandidate}
            comparisonDetail={trainingCompareDetail}
            onRefreshFeatures={() => void handleWorkflowAction("refresh-features")}
            onSelectComparisonCandidate={setSelectedTrainingCompareCandidateName}
            onSelectCandidate={setSelectedTrainingCandidateName}
            onTrain={() => void handleWorkflowAction("train")}
            parameterGroups={data.trainingLab.parameterGroups}
            primaryCandidate={selectedTrainingCandidate}
            presets={data.trainingLab.presets}
            selectedCandidate={trainingDetail}
            tasks={data.tasks}
            validationNotes={data.trainingLab.validationNotes}
          />
        ) : null}

        {route === "model-decision" ? (
          <ModelDecisionPage
            activeModel={data.decision.activeModel}
            busyAction={actionState.busy ? actionState.kind : null}
            checklist={data.decision.checklist}
            history={data.decision.history}
            onActivate={() => void handleWorkflowAction("activate")}
            onSelectCandidate={setSelectedDecisionCandidateKey}
            recommendedModel={data.decision.recommendedModel}
            selectedCandidate={decisionDetail}
            selectedTask={selectedTask}
          />
        ) : null}

        {route === "slate-runner" ? (
          <SlateRunnerPage
            busyAction={actionState.busy ? actionState.kind : null}
            onRunSlate={() => void handleWorkflowAction("run-slate")}
            presets={data.slateRunner.presets}
            queueSummary={data.slateRunner.queueSummary}
            scenarios={data.slateRunner.scenarios}
          />
        ) : null}

        {route === "signals-desk" ? (
          <SignalsDeskPage
            filters={data.signalsDesk.filters}
            onQueryChange={setSignalQuery}
            onSelectSignal={setSelectedSignalId}
            query={signalQuery}
            selectedSignal={signalDetail}
            signals={filteredSignals}
          />
        ) : null}

        {route === "history" ? <HistoryPage entries={data.history.entries} /> : null}
        {route === "settings" ? <SettingsPage groups={data.settings.groups} /> : null}
        {route === "help" ? <HelpPage glossary={data.help.glossary} /> : null}
      </section>
    </main>
  );
}

function HomePage({
  focusCards,
  recentActivity,
  missionNotes,
  onNavigate
}: {
  focusCards: FocusCard[];
  recentActivity: ActivityItem[];
  missionNotes: string[];
  onNavigate: (route: RouteKey) => void;
}) {
  return (
    <section className="page-grid two-column">
      <div className="stack">
        <SectionHeading title="Mission Control" detail="Answer what is active, stale, waiting, and next in one glance." />
        <div className="card-grid">
          {focusCards.map((card) => (
            <article key={card.title} className={`panel-card ${toneClass(card.tone)}`}>
              <p className="section-label">{card.eyebrow}</p>
              <h3>{card.title}</h3>
              <p>{card.description}</p>
              <button className="inline-cta" onClick={() => onNavigate(card.route)} type="button">
                {card.ctaLabel}
              </button>
            </article>
          ))}
        </div>
      </div>

      <div className="stack">
        <SectionHeading title="Recent Activity" detail="Outcome-oriented updates rather than raw subsystem logs." />
        <div className="list-panel">
          {recentActivity.map((item) => (
            <article key={`${item.title}-${item.timestamp}`} className="timeline-item">
              <span className={`status-dot ${toneClass(item.tone)}`} />
              <div>
                <h3>{item.title}</h3>
                <p>{item.detail}</p>
                <small>{item.timestamp}</small>
              </div>
            </article>
          ))}
        </div>

        <article className="panel-card tone-slate">
          <SectionHeading title="Product Rules" detail="The operating principles driving this clean-room redesign." />
          <ul className="flat-list">
            {missionNotes.map((note) => (
              <li key={note}>{note}</li>
            ))}
          </ul>
        </article>
      </div>
    </section>
  );
}

function TrainingLabPage({
  busyAction,
  tasks,
  presets,
  candidates,
  primaryCandidate,
  comparisonCandidate,
  comparisonDetail,
  selectedCandidate,
  validationNotes,
  parameterGroups,
  onRefreshFeatures,
  onSelectComparisonCandidate,
  onSelectCandidate,
  onTrain
}: {
  busyAction: ActionKind | null;
  tasks: TaskOption[];
  presets: Preset[];
  candidates: CandidateCard[];
  primaryCandidate: CandidateCard | null;
  comparisonCandidate: CandidateCard | null;
  comparisonDetail: DetailState | null;
  selectedCandidate: DetailState | null;
  validationNotes: Array<{ title: string; detail: string; tone: Tone }>;
  parameterGroups: PanelGroup[];
  onRefreshFeatures: () => void;
  onSelectComparisonCandidate: (name: string) => void;
  onSelectCandidate: (name: string) => void;
  onTrain: () => void;
}) {
  const comparison = primaryCandidate && comparisonCandidate ? describeTrainingComparison(primaryCandidate, comparisonCandidate) : null;
  const comparisonRows =
    primaryCandidate && comparisonCandidate
      ? [
          {
            label: "Model family",
            primary: getDetailFact(selectedCandidate, "Model family") ?? primaryCandidate.modelFamily,
            comparison: getDetailFact(comparisonDetail, "Model family") ?? comparisonCandidate.modelFamily
          },
          {
            label: primaryCandidate.metricLabel,
            primary: primaryCandidate.metricValue,
            comparison: comparisonCandidate.metricValue
          },
          {
            label: "Run",
            primary: primaryCandidate.runId ? `#${primaryCandidate.runId}` : "n/a",
            comparison: comparisonCandidate.runId ? `#${comparisonCandidate.runId}` : "n/a"
          },
          {
            label: "Task target",
            primary: getDetailFact(selectedCandidate, "Task") ?? getCandidateFact(primaryCandidate, "Task"),
            comparison: getDetailFact(comparisonDetail, "Task") ?? getCandidateFact(comparisonCandidate, "Task")
          },
          {
            label: "Selected feature",
            primary: getDetailFact(selectedCandidate, "Selected feature") ?? "n/a",
            comparison: getDetailFact(comparisonDetail, "Selected feature") ?? "n/a"
          },
          {
            label: "Validation ratio",
            primary: getDetailFact(selectedCandidate, "Validation ratio") ?? "n/a",
            comparison: getDetailFact(comparisonDetail, "Validation ratio") ?? "n/a"
          },
          {
            label: "Evidence items",
            primary: String(selectedCandidate?.evidence.length ?? primaryCandidate.evidence.length),
            comparison: String(comparisonDetail?.evidence.length ?? comparisonCandidate.evidence.length)
          }
        ]
      : [];

  return (
    <section className="page-grid">
      <SectionHeading
        title="Training Lab"
        detail="Train, compare, and validate candidates while keeping system detail behind the operator flow."
      />

      <section className="action-row">
        <button className="primary-cta" disabled={busyAction !== null} onClick={onRefreshFeatures} type="button">
          {busyAction === "refresh-features" ? "Refreshing features..." : "Refresh features"}
        </button>
        <button className="primary-cta" disabled={busyAction !== null} onClick={onTrain} type="button">
          {busyAction === "train" ? "Training run..." : "Train new run"}
        </button>
      </section>

      <section className="panel-card tone-blue">
        <SectionHeading title="Target Tasks" detail="Capability-driven choices loaded into plain-language cards." />
        <div className="chip-row">
          {tasks.map((task) => (
            <article key={task.key} className="chip-card">
              <h3>{task.label}</h3>
              <p>{task.description}</p>
              <small>
                {task.metricName} | {task.defaultPolicy}
              </small>
            </article>
          ))}
        </div>
      </section>

      <div className="page-grid two-column">
        <article className="panel-card">
          <SectionHeading title="Presets" detail="Essentials first, deeper controls later." />
          <div className="stack">
            {presets.map((preset) => (
              <article key={preset.name} className="subpanel">
                <h3>{preset.name}</h3>
                <p>{preset.summary}</p>
                <ul className="flat-list">
                  {preset.fields.map((field) => (
                    <li key={field}>{field}</li>
                  ))}
                </ul>
                <small>{preset.outcome}</small>
              </article>
            ))}
          </div>
        </article>

        <article className="panel-card">
          <SectionHeading title="Parameter Tiers" detail="The blueprint's essentials / expert / defaults model." />
          <div className="stack">
            {parameterGroups.map((group) => (
              <article key={group.title} className="subpanel">
                <h3>{group.title}</h3>
                <p>{group.description}</p>
                <ul className="flat-list">
                  {group.items.map((item) => (
                    <li key={item}>{item}</li>
                  ))}
                </ul>
              </article>
            ))}
          </div>
        </article>
      </div>

      <div className="page-grid two-column">
        <article className="panel-card">
          <SectionHeading title="Candidate Comparison" detail="Recommended models appear as cards first, drill-down later." />
          <div className="stack">
            {candidates.map((candidate) => (
              <CandidatePanel
                key={candidate.name}
                candidate={candidate}
                onCompare={candidate.name === primaryCandidate?.name ? undefined : () => onSelectComparisonCandidate(candidate.name)}
                onOpen={() => onSelectCandidate(candidate.name)}
              />
            ))}
          </div>
        </article>

        <article className="panel-card">
          <DetailPanel
            emptyMessage="Select a candidate to inspect evidence, provenance, and recommended next steps."
            evidence={selectedCandidate?.evidence ?? []}
            facts={selectedCandidate?.facts ?? []}
            loading={selectedCandidate?.loading ?? false}
            nextActions={selectedCandidate?.nextActions ?? []}
            subtitle={selectedCandidate?.subtitle ?? "Drill-down"}
            title={selectedCandidate?.title ?? "Candidate detail"}
            warning={selectedCandidate?.warning ?? null}
          />
          {primaryCandidate && comparisonCandidate && comparison ? (
            <article className={`subpanel top-gap ${toneClass(comparison.tone)}`}>
              <SectionHeading title="Candidate Matrix" detail={comparison.headline} />
              <p>{comparison.detail}</p>
              {comparisonDetail?.loading ? <p className="detail-meta">Loading live comparison detail...</p> : null}
              {comparisonDetail?.warning ? <p className="detail-meta">{comparisonDetail.warning}</p> : null}
              <div className="compare-table top-gap">
                <div className="compare-row compare-row-head">
                  <strong>Dimension</strong>
                  <strong>{primaryCandidate.name}</strong>
                  <strong>{comparisonCandidate.name}</strong>
                </div>
                {comparisonRows.map((row) => (
                  <div key={row.label} className="compare-row">
                    <span>{row.label}</span>
                    <strong>{row.primary}</strong>
                    <strong>{row.comparison}</strong>
                  </div>
                ))}
              </div>
            </article>
          ) : null}
          <div className="stack top-gap">
            {validationNotes.map((note) => (
              <article key={note.title} className={`subpanel ${toneClass(note.tone)}`}>
                <h3>{note.title}</h3>
                <p>{note.detail}</p>
              </article>
            ))}
          </div>
        </article>
      </div>
    </section>
  );
}

function ModelDecisionPage({
  activeModel,
  busyAction,
  recommendedModel,
  checklist,
  history,
  onActivate,
  onSelectCandidate,
  selectedCandidate,
  selectedTask
}: {
  activeModel: CandidateCard;
  busyAction: ActionKind | null;
  recommendedModel: CandidateCard;
  checklist: DecisionChecklistItem[];
  history: ReleaseHistoryItem[];
  onActivate: () => void;
  onSelectCandidate: (key: "active" | "recommended") => void;
  selectedCandidate: DetailState | null;
  selectedTask: TaskOption | null;
}) {
  const comparison = describeComparison(activeModel, recommendedModel);
  const comparisonRows = [
    {
      label: "Model family",
      active: activeModel.modelFamily,
      recommended: recommendedModel.modelFamily
    },
    {
      label: activeModel.metricLabel,
      active: activeModel.metricValue,
      recommended: recommendedModel.metricValue
    },
    {
      label: "Release state",
      active: getCandidateFact(activeModel, "Release state"),
      recommended: getCandidateFact(recommendedModel, "Release posture")
    },
    {
      label: "Training run",
      active: activeModel.runId ? `#${activeModel.runId}` : "n/a",
      recommended: recommendedModel.runId ? `#${recommendedModel.runId}` : "n/a"
    },
    {
      label: "Evidence items",
      active: String(activeModel.evidence.length),
      recommended: String(recommendedModel.evidence.length)
    }
  ];

  return (
    <section className="page-grid">
      <SectionHeading title="Model Decision" detail="Make a release decision explicitly, with readiness and risk visible." />

      <section className="action-row">
        <button className="primary-cta" disabled={busyAction !== null || !selectedTask} onClick={onActivate} type="button">
          {busyAction === "activate" ? "Activating model..." : `Activate model for ${selectedTask?.label ?? "task"}`}
        </button>
      </section>

      <div className="page-grid two-column">
        <article className="panel-card tone-slate">
          <SectionHeading title="Active Model" detail="What is live right now." />
          <CandidatePanel candidate={activeModel} onOpen={() => onSelectCandidate("active")} />
        </article>

        <article className="panel-card tone-green">
          <SectionHeading title="Recommended Candidate" detail="The most credible upgrade currently available." />
          <CandidatePanel candidate={recommendedModel} onOpen={() => onSelectCandidate("recommended")} />
        </article>
      </div>

      <article className={`panel-card ${toneClass(comparison.tone)}`}>
        <SectionHeading title="Release Comparison" detail={comparison.headline} />
        <p>{comparison.detail}</p>
        <div className="compare-table top-gap">
          <div className="compare-row compare-row-head">
            <strong>Dimension</strong>
            <strong>Active</strong>
            <strong>Recommended</strong>
          </div>
          {comparisonRows.map((row) => (
            <div key={row.label} className="compare-row">
              <span>{row.label}</span>
              <strong>{row.active}</strong>
              <strong>{row.recommended}</strong>
            </div>
          ))}
        </div>
      </article>

      <div className="page-grid two-column">
        <article className="panel-card">
          <SectionHeading title="Release Checklist" detail="Clear the decision with concrete, visible gates." />
          <div className="stack">
            {checklist.map((item) => (
              <article key={item.label} className="check-row">
                <span className={`status-pill ${toneClass(checklistTone(item.state))}`}>{item.state}</span>
                <span>{item.label}</span>
              </article>
            ))}
          </div>
        </article>

        <article className="panel-card">
          <DetailPanel
            emptyMessage="Pick the active or recommended model to inspect its release rationale."
            evidence={selectedCandidate?.evidence ?? []}
            facts={selectedCandidate?.facts ?? []}
            loading={selectedCandidate?.loading ?? false}
            nextActions={selectedCandidate?.nextActions ?? []}
            subtitle={selectedCandidate?.subtitle ?? "Release detail"}
            title={selectedCandidate?.title ?? "Release detail"}
            warning={selectedCandidate?.warning ?? null}
          />
          <div className="stack top-gap">
            <SectionHeading title="Decision History" detail="Selections become release history, not top-level navigation clutter." />
            {history.map((entry) => (
              <article key={`${entry.title}-${entry.timestamp}`} className="subpanel">
                <h3>{entry.title}</h3>
                <p>{entry.detail}</p>
                <small>{entry.timestamp}</small>
              </article>
            ))}
          </div>
        </article>
      </div>
    </section>
  );
}

function SlateRunnerPage({
  busyAction,
  onRunSlate,
  presets,
  scenarios,
  queueSummary
}: {
  busyAction: ActionKind | null;
  onRunSlate: () => void;
  presets: Preset[];
  scenarios: Array<{ title: string; detail: string; status: string; preset: string }>;
  queueSummary: { slateLabel: string; openSignals: string; activeModel: string; note: string };
}) {
  return (
    <section className="page-grid">
      <SectionHeading title="Slate Runner" detail="Score a game, a slate, or a historical batch without exposing unnecessary console complexity." />

      <section className="action-row">
        <button className="primary-cta" disabled={busyAction !== null} onClick={onRunSlate} type="button">
          {busyAction === "run-slate" ? "Running today's slate..." : "Run today's slate"}
        </button>
      </section>

      <div className="page-grid two-column">
        <article className="panel-card tone-blue">
          <SectionHeading title="Queue Summary" detail="The operator's quick-read before launching a scoring run." />
          <div className="stack">
            <MetricRow label="Slate" value={queueSummary.slateLabel} />
            <MetricRow label="Open signals" value={queueSummary.openSignals} />
            <MetricRow label="Active model" value={queueSummary.activeModel} />
            <p>{queueSummary.note}</p>
          </div>
        </article>

        <article className="panel-card">
          <SectionHeading title="Scoring Presets" detail="Preview and production paths should feel different." />
          <div className="stack">
            {presets.map((preset) => (
              <article key={preset.name} className="subpanel">
                <h3>{preset.name}</h3>
                <p>{preset.summary}</p>
                <small>{preset.outcome}</small>
              </article>
            ))}
          </div>
        </article>
      </div>

      <article className="panel-card">
        <SectionHeading title="Scenario Launchers" detail="Entry points framed as user goals instead of route families." />
        <div className="card-grid">
          {scenarios.map((scenario) => (
            <article key={scenario.title} className="subpanel">
              <p className="section-label">{scenario.status}</p>
              <h3>{scenario.title}</h3>
              <p>{scenario.detail}</p>
              <small>{scenario.preset}</small>
            </article>
          ))}
        </div>
      </article>
    </section>
  );
}

function SignalsDeskPage({
  filters,
  query,
  signals,
  selectedSignal,
  onQueryChange,
  onSelectSignal
}: {
  filters: string[];
  query: string;
  signals: SignalCard[];
  selectedSignal: DetailState | null;
  onQueryChange: (value: string) => void;
  onSelectSignal: (id: string) => void;
}) {
  return (
    <section className="page-grid">
      <SectionHeading title="Signals Desk" detail="Review signals and investigate why they were produced, without leaving the workflow." />

      <article className="panel-card tone-blue">
        <div className="toolbar">
          <input
            aria-label="Search signals"
            className="search-input"
            onChange={(event) => onQueryChange(event.target.value)}
            placeholder="Search game, market, recommendation, or tag"
            type="search"
            value={query}
          />
          <div className="chip-row">
            {filters.map((filter) => (
              <span key={filter} className="filter-chip">
                {filter}
              </span>
            ))}
          </div>
        </div>
      </article>

      <div className="page-grid detail-split">
        <div className="card-grid">
          {signals.map((signal) => (
            <article key={signal.id} className="panel-card tone-slate">
              <div className="signal-header">
                <div>
                  <p className="section-label">
                    {signal.id} | {signal.market}
                  </p>
                  <h3>{signal.game}</h3>
                </div>
                <span className="status-pill tone-amber">{signal.status}</span>
              </div>
              <p className="signal-recommendation">{signal.recommendation}</p>
              <p>{signal.summary}</p>
              <div className="signal-meta">
                <MetricRow label="Strength" value={signal.signalStrength} />
                <MetricRow label="Evidence" value={signal.evidenceRating} />
              </div>
              <div className="chip-row">
                {signal.tags.map((tag) => (
                  <span key={tag} className="filter-chip">
                    {tag}
                  </span>
                ))}
              </div>
              <button className="inline-cta top-gap" onClick={() => onSelectSignal(signal.id)} type="button">
                Inspect signal
              </button>
            </article>
          ))}
        </div>
        <article className="panel-card">
          <DetailPanel
            emptyMessage="Select a signal card to inspect evidence, provenance, and analyst follow-up steps."
            evidence={selectedSignal?.evidence ?? []}
            facts={selectedSignal?.facts ?? []}
            loading={selectedSignal?.loading ?? false}
            nextActions={selectedSignal?.nextActions ?? []}
            subtitle={selectedSignal?.subtitle ?? "Analyst detail"}
            title={selectedSignal?.title ?? "Signal detail"}
            warning={selectedSignal?.warning ?? null}
          />
        </article>
      </div>
    </section>
  );
}

function HistoryPage({ entries }: { entries: ActivityItem[] }) {
  return (
    <section className="page-grid">
      <SectionHeading title="History" detail="Archived runs, decisions, and workflow outcomes belong here, not in primary navigation." />
      <article className="panel-card">
        <div className="stack">
          {entries.map((entry) => (
            <article key={`${entry.title}-${entry.timestamp}`} className="subpanel">
              <p className="section-label">{entry.timestamp}</p>
              <h3>{entry.title}</h3>
              <p>{entry.detail}</p>
            </article>
          ))}
        </div>
      </article>
    </section>
  );
}

function SettingsPage({ groups }: { groups: PanelGroup[] }) {
  return (
    <section className="page-grid">
      <SectionHeading title="Settings" detail="Defaults and preferences remain available without crowding the operator path." />
      <div className="card-grid">
        {groups.map((group) => (
          <article key={group.title} className="panel-card">
            <h3>{group.title}</h3>
            <p>{group.description}</p>
            <ul className="flat-list">
              {group.items.map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>
          </article>
        ))}
      </div>
    </section>
  );
}

function HelpPage({ glossary }: { glossary: Array<{ term: string; definition: string }> }) {
  return (
    <section className="page-grid">
      <SectionHeading title="Help" detail="Glossary and quick-start guidance should explain the workflow in everyday language." />
      <article className="panel-card">
        <div className="stack">
          {glossary.map((entry) => (
            <article key={entry.term} className="subpanel">
              <h3>{entry.term}</h3>
              <p>{entry.definition}</p>
            </article>
          ))}
        </div>
      </article>
    </section>
  );
}

function CandidatePanel({
  candidate,
  onCompare,
  onOpen
}: {
  candidate: CandidateCard;
  onCompare?: () => void;
  onOpen?: () => void;
}) {
  return (
    <article className="subpanel">
      <div className="candidate-header">
        <div>
          <h3>{candidate.name}</h3>
          <p className="section-label">{candidate.modelFamily}</p>
        </div>
        <span className="status-pill tone-blue">{candidate.status}</span>
      </div>
      <MetricRow label={candidate.metricLabel} value={candidate.metricValue} />
      <p>{candidate.whyItMatters}</p>
      <div className="chip-row">
        {candidate.tags.map((tag) => (
          <span key={tag} className="filter-chip">
            {tag}
          </span>
        ))}
      </div>
      {onOpen || onCompare ? (
        <div className="candidate-actions top-gap">
          {onOpen ? (
            <button className="inline-cta" onClick={onOpen} type="button">
              Inspect detail
            </button>
          ) : null}
          {onCompare ? (
            <button className="inline-cta inline-cta-muted" onClick={onCompare} type="button">
              Compare here
            </button>
          ) : null}
        </div>
      ) : null}
    </article>
  );
}

function DetailPanel({
  title,
  subtitle,
  evidence,
  facts,
  nextActions,
  emptyMessage,
  loading,
  warning
}: {
  title: string;
  subtitle: string;
  evidence: string[];
  facts: Array<{ label: string; value: string }>;
  nextActions: string[];
  emptyMessage: string;
  loading: boolean;
  warning: string | null;
}) {
  const hasContent = evidence.length > 0 || facts.length > 0 || nextActions.length > 0;

  return (
    <section className="detail-panel">
      <SectionHeading title={title} detail={subtitle} />
      {loading ? <p className="detail-meta">Loading live detail...</p> : null}
      {warning ? <p className="detail-meta">{warning}</p> : null}
      {!hasContent ? <p>{emptyMessage}</p> : null}
      {facts.length > 0 ? (
        <div className="stack">
          <p className="section-label">Provenance</p>
          {facts.map((fact) => (
            <div key={`${fact.label}-${fact.value}`} className="metric-row">
              <span>{fact.label}</span>
              <strong>{fact.value}</strong>
            </div>
          ))}
        </div>
      ) : null}
      {evidence.length > 0 ? (
        <div className="stack">
          <p className="section-label">Evidence</p>
          <ul className="flat-list">
            {evidence.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </div>
      ) : null}
      {nextActions.length > 0 ? (
        <div className="stack">
          <p className="section-label">Next actions</p>
          <ul className="flat-list">
            {nextActions.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </div>
      ) : null}
    </section>
  );
}

function MetricRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="metric-row">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function SectionHeading({ title, detail }: { title: string; detail: string }) {
  return (
    <div className="section-heading">
      <h2>{title}</h2>
      <p>{detail}</p>
    </div>
  );
}
