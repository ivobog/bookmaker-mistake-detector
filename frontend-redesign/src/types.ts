import type { SharedDetailFact } from "../../shared/frontend/viewTypes";

export type RouteKey =
  | "home"
  | "training-lab"
  | "model-decision"
  | "slate-runner"
  | "signals-desk"
  | "history"
  | "settings"
  | "help";

export type Tone = "green" | "amber" | "red" | "blue" | "slate";

export type WorkflowStatus = "ready" | "stale" | "missing" | "active" | "review" | "info";

export type AppMode = "mock" | "live";

export type WorkspaceDefaults = {
  featureKey: string;
  targetTask: string | null;
  trainRatio: number;
  validationRatio: number;
  seasonLabel: string;
  sourceName: string;
};

export type MetricBadge = {
  label: string;
  value: string;
  tone: Tone;
};

export type DetailFact = SharedDetailFact;

export type TaskOption = {
  key: string;
  label: string;
  description: string;
  metricName: string;
  defaultPolicy: string;
  semantics: string;
};

export type WorkflowStep = {
  id: string;
  label: string;
  description: string;
  status: WorkflowStatus;
  updatedAt: string;
  ctaLabel: string;
  route: RouteKey;
};

export type FocusCard = {
  eyebrow: string;
  title: string;
  description: string;
  ctaLabel: string;
  route: RouteKey;
  tone: Tone;
};

export type ActivityItem = {
  title: string;
  detail: string;
  timestamp: string;
  tone: Tone;
};

export type Preset = {
  name: string;
  summary: string;
  fields: string[];
  outcome: string;
};

export type CandidateCard = {
  name: string;
  modelFamily: string;
  status: string;
  metricLabel: string;
  metricValue: string;
  whyItMatters: string;
  tags: string[];
  runId?: number | null;
  selectionId?: number | null;
  targetTaskKey?: string | null;
  evidence: string[];
  provenance: DetailFact[];
  nextActions: string[];
};

export type ValidationNote = {
  title: string;
  detail: string;
  tone: Tone;
};

export type PanelGroup = {
  title: string;
  description: string;
  items: string[];
};

export type DecisionChecklistItem = {
  label: string;
  state: "done" | "attention" | "pending";
};

export type ReleaseHistoryItem = {
  title: string;
  detail: string;
  timestamp: string;
};

export type ScenarioCard = {
  title: string;
  detail: string;
  status: string;
  preset: string;
};

export type SignalCard = {
  id: string;
  game: string;
  market: string;
  signalStrength: string;
  evidenceRating: string;
  status: string;
  recommendation: string;
  summary: string;
  tags: string[];
  opportunityId?: number | null;
  scoringRunId?: number | null;
  targetTaskKey?: string | null;
  evidence: string[];
  provenance: DetailFact[];
  nextActions: string[];
};

export type QueueSummary = {
  slateLabel: string;
  openSignals: string;
  activeModel: string;
  note: string;
};

export type AppData = {
  mode: AppMode;
  sourceLabel: string;
  generatedAt: string;
  headline: string;
  lead: string;
  defaults: WorkspaceDefaults;
  nextActionLabel: string;
  nextActionRoute: RouteKey;
  stats: MetricBadge[];
  tasks: TaskOption[];
  workflow: WorkflowStep[];
  home: {
    focusCards: FocusCard[];
    recentActivity: ActivityItem[];
    missionNotes: string[];
  };
  trainingLab: {
    presets: Preset[];
    candidates: CandidateCard[];
    validationNotes: ValidationNote[];
    parameterGroups: PanelGroup[];
  };
  decision: {
    activeModel: CandidateCard;
    recommendedModel: CandidateCard;
    checklist: DecisionChecklistItem[];
    history: ReleaseHistoryItem[];
  };
  slateRunner: {
    presets: Preset[];
    scenarios: ScenarioCard[];
    queueSummary: QueueSummary;
  };
  signalsDesk: {
    filters: string[];
    signals: SignalCard[];
  };
  history: {
    entries: ActivityItem[];
  };
  settings: {
    groups: PanelGroup[];
  };
  help: {
    glossary: Array<{ term: string; definition: string }>;
  };
};

export type WorkflowDataState = {
  data: AppData;
  loading: boolean;
  warning: string | null;
  reload: () => Promise<void>;
};
