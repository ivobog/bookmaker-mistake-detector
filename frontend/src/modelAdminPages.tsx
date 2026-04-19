import type { ReactNode } from "react";

import { StatTile } from "./appSharedComponents";
import type { AppRoute } from "./appTypes";
import type {
  ModelAdminEvaluationHistory,
  ModelAdminEvaluationSnapshot,
  ModelAdminRegistryEntry,
  ModelAdminRun,
  ModelAdminSelectionHistory,
  ModelAdminSelectionSnapshot,
  ModelAdminSummary,
  ModelAdminTrainingHistory
} from "./modelAdminTypes";
import { formatCompactNumber, formatLabel, formatTimestamp, readNested } from "./appUtils";

type SharedFiltersProps = {
  actions?: ReactNode;
  children: ReactNode;
  title: string;
};

function FilterShell({ actions, children, title }: SharedFiltersProps) {
  return (
    <section className="panel filter-panel">
      <div className="section-heading compact-heading">
        <div>
          <p className="eyebrow">Filters</p>
          <h3>{title}</h3>
        </div>
        {actions}
      </div>
      <div className="filter-grid">{children}</div>
    </section>
  );
}

function FilterField({
  children,
  label
}: {
  children: ReactNode;
  label: string;
}) {
  return (
    <label className="filter-field">
      <span className="filter-label">{label}</span>
      {children}
    </label>
  );
}

export function SharedTrainingFilters({
  onApply,
  onReset,
  seasonLabel,
  setSeasonLabel,
  setTargetTask,
  setTeamCode,
  targetTask,
  teamCode
}: {
  onApply: () => void;
  onReset: () => void;
  seasonLabel: string;
  setSeasonLabel: (value: string) => void;
  setTargetTask: (value: string) => void;
  setTeamCode: (value: string) => void;
  targetTask: string;
  teamCode: string;
}) {
  return (
    <FilterShell
      actions={
        <div className="pill-row">
          <button className="secondary-button" onClick={onReset} type="button">
            Reset
          </button>
          <button className="secondary-button" onClick={onApply} type="button">
            Apply filters
          </button>
        </div>
      }
      title="Training scope"
    >
      <FilterField label="Target task">
        <input onChange={(event) => setTargetTask(event.target.value)} value={targetTask} />
      </FilterField>
      <FilterField label="Team code">
        <input onChange={(event) => setTeamCode(event.target.value)} placeholder="Optional" value={teamCode} />
      </FilterField>
      <FilterField label="Season label">
        <input
          onChange={(event) => setSeasonLabel(event.target.value)}
          placeholder="Optional"
          value={seasonLabel}
        />
      </FilterField>
    </FilterShell>
  );
}

export function EvaluationFilters({
  modelFamily,
  onApply,
  onReset,
  setModelFamily,
  setTargetTask,
  targetTask
}: {
  modelFamily: string;
  onApply: () => void;
  onReset: () => void;
  setModelFamily: (value: string) => void;
  setTargetTask: (value: string) => void;
  targetTask: string;
}) {
  return (
    <FilterShell
      actions={
        <div className="pill-row">
          <button className="secondary-button" onClick={onReset} type="button">
            Reset
          </button>
          <button className="secondary-button" onClick={onApply} type="button">
            Apply filters
          </button>
        </div>
      }
      title="Evaluation scope"
    >
      <FilterField label="Target task">
        <input onChange={(event) => setTargetTask(event.target.value)} value={targetTask} />
      </FilterField>
      <FilterField label="Model family">
        <input
          onChange={(event) => setModelFamily(event.target.value)}
          placeholder="Optional"
          value={modelFamily}
        />
      </FilterField>
    </FilterShell>
  );
}

export function SelectionFilters({
  activeOnly,
  onApply,
  onReset,
  setActiveOnly,
  setTargetTask,
  targetTask
}: {
  activeOnly: boolean;
  onApply: () => void;
  onReset: () => void;
  setActiveOnly: (value: boolean) => void;
  setTargetTask: (value: string) => void;
  targetTask: string;
}) {
  return (
    <FilterShell
      actions={
        <div className="pill-row">
          <button className="secondary-button" onClick={onReset} type="button">
            Reset
          </button>
          <button className="secondary-button" onClick={onApply} type="button">
            Apply filters
          </button>
        </div>
      }
      title="Selection scope"
    >
      <FilterField label="Target task">
        <input onChange={(event) => setTargetTask(event.target.value)} value={targetTask} />
      </FilterField>
      <label className="checkbox-field">
        <input checked={activeOnly} onChange={(event) => setActiveOnly(event.target.checked)} type="checkbox" />
        <span>Active selections only</span>
      </label>
    </FilterShell>
  );
}

export function ModelAdminDashboardPage({
  evaluationHistory,
  onNavigate,
  selectionHistory,
  summary,
  trainingHistory
}: {
  evaluationHistory: ModelAdminEvaluationHistory | null;
  onNavigate: (route: AppRoute) => void;
  selectionHistory: ModelAdminSelectionHistory | null;
  summary: ModelAdminSummary | null;
  trainingHistory: ModelAdminTrainingHistory | null;
}) {
  const latestRun = trainingHistory?.overview.latest_run ?? summary?.latest_run ?? null;
  const bestOverall = trainingHistory?.overview.best_overall ?? summary?.best_overall ?? null;
  const latestEvaluation = evaluationHistory?.overview.latest_snapshot ?? evaluationHistory?.recent_snapshots[0] ?? null;
  const latestSelection = selectionHistory?.overview.latest_selection ?? selectionHistory?.recent_selections[0] ?? null;

  return (
    <>
      <section className="dashboard-grid">
        <article className="panel focus-panel">
          <div className="section-heading">
            <div>
              <p className="eyebrow">Workspace overview</p>
              <h2>Recent training activity</h2>
            </div>
            <div className="pill-row">
              <span className="pill">Lifecycle ready</span>
              <span className="pill">Phase 5</span>
            </div>
          </div>

          <div className="mini-grid">
            <StatTile label="Run count" value={String(summary?.run_count ?? trainingHistory?.overview.run_count ?? 0)} />
            <StatTile label="Fallback runs" value={String(summary?.fallback_run_count ?? trainingHistory?.overview.fallback_run_count ?? 0)} />
            <StatTile label="Evaluation snapshots" value={String(evaluationHistory?.overview.snapshot_count ?? 0)} />
            <StatTile label="Selections" value={String(selectionHistory?.overview.selection_count ?? 0)} />
          </div>

          <div className="mini-grid family-grid">
            <div className="sub-panel">
              <p className="sub-panel-title">Latest run</p>
              <p className="sub-panel-stat">{latestRun ? `Run #${latestRun.id}` : "n/a"}</p>
              <p className="sub-panel-meta">
                {latestRun
                  ? `${String(readNested(latestRun.artifact, "model_family") ?? "n/a")} | ${formatTimestamp(
                      latestRun.completed_at ?? latestRun.created_at
                    )}`
                  : "No run recorded"}
              </p>
            </div>
            <div className="sub-panel">
              <p className="sub-panel-title">Best overall</p>
              <p className="sub-panel-stat">
                {bestOverall ? String(readNested(bestOverall.artifact, "model_family") ?? "n/a") : "n/a"}
              </p>
              <p className="sub-panel-meta">
                {bestOverall ? formatLabel(bestOverall.target_task) : "No best model recorded"}
              </p>
            </div>
            <div className="sub-panel">
              <p className="sub-panel-title">Latest evaluation</p>
              <p className="sub-panel-stat">{latestEvaluation ? `#${latestEvaluation.id}` : "n/a"}</p>
              <p className="sub-panel-meta">
                {latestEvaluation
                  ? `${latestEvaluation.model_family} | ${formatCompactNumber(
                      latestEvaluation.validation_metric_value,
                      4
                    )}`
                  : "No evaluation snapshot recorded"}
              </p>
            </div>
            <div className="sub-panel">
              <p className="sub-panel-title">Active selection</p>
              <p className="sub-panel-stat">{latestSelection ? `#${latestSelection.id}` : "n/a"}</p>
              <p className="sub-panel-meta">
                {latestSelection
                  ? `${latestSelection.model_family} | ${latestSelection.is_active ? "active" : "historical"}`
                  : "No selection snapshot recorded"}
              </p>
            </div>
          </div>
        </article>

        <article className="panel focus-panel">
          <div className="section-heading">
            <div>
              <p className="eyebrow">Workspace routes</p>
              <h2>Inspect artifacts</h2>
            </div>
          </div>

          <div className="list-stack">
            <button className="model-admin-list-card" onClick={() => onNavigate({ name: "model-registry" })} type="button">
              <p className="sub-panel-title">Registry</p>
              <p className="sub-panel-meta">Inspect registered model families and version metadata.</p>
            </button>
            <button className="model-admin-list-card" onClick={() => onNavigate({ name: "model-runs" })} type="button">
              <p className="sub-panel-title">Runs</p>
              <p className="sub-panel-meta">Inspect recent training runs and open detailed artifacts.</p>
            </button>
            <button className="model-admin-list-card" onClick={() => onNavigate({ name: "model-evaluations" })} type="button">
              <p className="sub-panel-title">Evaluations</p>
              <p className="sub-panel-meta">Inspect snapshot metrics, selected feature, and fallback behavior.</p>
            </button>
            <button className="model-admin-list-card" onClick={() => onNavigate({ name: "model-selections" })} type="button">
              <p className="sub-panel-title">Selections</p>
              <p className="sub-panel-meta">Inspect active and historical promoted model snapshots.</p>
            </button>
            <button className="model-admin-list-card" onClick={() => onNavigate({ name: "backtests" })} type="button">
              <p className="sub-panel-title">Backtests</p>
              <p className="sub-panel-meta">Jump to chronological validation and compare training artifacts against validation outcomes.</p>
            </button>
            <button className="model-admin-list-card" onClick={() => onNavigate({ name: "opportunities" })} type="button">
              <p className="sub-panel-title">Opportunities</p>
              <p className="sub-panel-meta">Jump to the analyst queue and inspect how the active selection shows up in live scoring outputs.</p>
            </button>
          </div>
        </article>
      </section>

      <section className="section-stack">
        <div className="section-heading standalone">
          <div>
            <p className="eyebrow">Recent activity</p>
            <h2>Latest artifacts</h2>
          </div>
        </div>
        <div className="dashboard-grid">
          <article className="panel">
            <div className="section-heading compact-heading">
              <div>
                <p className="sub-panel-title">Recent runs</p>
                <p className="sub-panel-meta">Newest training executions.</p>
              </div>
            </div>
            <div className="list-stack">
              {trainingHistory?.recent_runs.length ? (
                trainingHistory.recent_runs.slice(0, 5).map((run) => (
                  <button
                    className="model-admin-list-card"
                    key={run.id}
                    onClick={() => onNavigate({ name: "model-run-detail", runId: run.id })}
                    type="button"
                  >
                    <p className="sub-panel-title">
                      Run #{run.id} | {String(readNested(run.artifact, "model_family") ?? "n/a")}
                    </p>
                    <p className="sub-panel-meta">
                      {formatTimestamp(run.completed_at ?? run.created_at)} | {formatLabel(run.target_task)}
                    </p>
                  </button>
                ))
              ) : (
                <p className="sub-panel-meta">No training runs are available for the current scope.</p>
              )}
            </div>
          </article>

          <article className="panel">
            <div className="section-heading compact-heading">
              <div>
                <p className="sub-panel-title">Recent evaluations</p>
                <p className="sub-panel-meta">Newest evaluation snapshots.</p>
              </div>
            </div>
            <div className="list-stack">
              {evaluationHistory?.recent_snapshots.length ? (
                evaluationHistory.recent_snapshots.slice(0, 5).map((snapshot) => (
                  <button
                    className="model-admin-list-card"
                    key={snapshot.id}
                    onClick={() => onNavigate({ name: "model-evaluation-detail", evaluationId: snapshot.id })}
                    type="button"
                  >
                    <p className="sub-panel-title">
                      Evaluation #{snapshot.id} | {snapshot.model_family}
                    </p>
                    <p className="sub-panel-meta">
                      Validation {formatCompactNumber(snapshot.validation_metric_value, 4)} |{" "}
                      {formatTimestamp(snapshot.created_at)}
                    </p>
                  </button>
                ))
              ) : (
                <p className="sub-panel-meta">No evaluation snapshots are available for the current scope.</p>
              )}
            </div>
          </article>
        </div>
      </section>
    </>
  );
}

type RegistryPageProps = {
  detailContent: ReactNode;
  entries: ModelAdminRegistryEntry[];
  onSelectEntry: (entryId: number) => void;
  selectedEntryId: number | null;
};

export function ModelRegistryPage({
  detailContent,
  entries,
  onSelectEntry,
  selectedEntryId
}: RegistryPageProps) {
  return (
    <section className="dashboard-grid">
      <article className="panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Registry</p>
            <h2>Model registry entries</h2>
          </div>
          <div className="pill-row">
            <span className="pill">{entries.length} entries</span>
          </div>
        </div>

        <div className="table-shell">
          <table>
            <thead>
              <tr>
                <th>Family</th>
                <th>Version</th>
                <th>Task</th>
                <th>Created</th>
              </tr>
            </thead>
            <tbody>
              {entries.length === 0 ? (
                <tr>
                  <td colSpan={4}>No model registry entries were found for the current scope.</td>
                </tr>
              ) : (
                entries.map((entry) => (
                  <tr
                    className={selectedEntryId === entry.id ? "row-active" : undefined}
                    key={entry.id}
                    onClick={() => onSelectEntry(entry.id)}
                  >
                    <td>{entry.model_family}</td>
                    <td>{entry.version_label}</td>
                    <td>{formatLabel(entry.target_task)}</td>
                    <td>{formatTimestamp(entry.created_at)}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </article>

      {detailContent}
    </section>
  );
}

type RunsPageProps = {
  detailContent?: ReactNode;
  onNavigate: (route: AppRoute) => void;
  runs: ModelAdminRun[];
  selectedRunId?: number | null;
};

export function ModelRunsPage({
  detailContent,
  onNavigate,
  runs,
  selectedRunId
}: RunsPageProps) {
  return (
    <section className="dashboard-grid">
      <article className="panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Training runs</p>
            <h2>Run history</h2>
          </div>
          <div className="pill-row">
            <span className="pill">{runs.length} runs</span>
          </div>
        </div>

        <div className="table-shell">
          <table>
            <thead>
              <tr>
                <th>Run</th>
                <th>Family</th>
                <th>Status</th>
                <th>Scope</th>
                <th>Completed</th>
              </tr>
            </thead>
            <tbody>
              {runs.length === 0 ? (
                <tr>
                  <td colSpan={5}>No training runs were found for the current filters.</td>
                </tr>
              ) : (
                runs.map((run) => (
                  <tr
                    className={selectedRunId === run.id ? "row-active" : undefined}
                    key={run.id}
                    onClick={() => onNavigate({ name: "model-run-detail", runId: run.id })}
                  >
                    <td>#{run.id}</td>
                    <td>{String(readNested(run.artifact, "model_family") ?? "n/a")}</td>
                    <td>{run.status}</td>
                    <td>{run.team_code ?? run.season_label ?? "global"}</td>
                    <td>{formatTimestamp(run.completed_at ?? run.created_at)}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </article>

      {detailContent ?? (
        <article className="panel focus-panel">
          <p className="sub-panel-meta">Open a run to inspect artifact and metric details.</p>
        </article>
      )}
    </section>
  );
}

type EvaluationsPageProps = {
  detailContent?: ReactNode;
  evaluations: ModelAdminEvaluationSnapshot[];
  onNavigate: (route: AppRoute) => void;
  selectedEvaluationId?: number | null;
};

export function ModelEvaluationsPage({
  detailContent,
  evaluations,
  onNavigate,
  selectedEvaluationId
}: EvaluationsPageProps) {
  return (
    <section className="dashboard-grid">
      <article className="panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Evaluations</p>
            <h2>Evaluation snapshots</h2>
          </div>
          <div className="pill-row">
            <span className="pill">{evaluations.length} snapshots</span>
          </div>
        </div>

        <div className="table-shell">
          <table>
            <thead>
              <tr>
                <th>Snapshot</th>
                <th>Family</th>
                <th>Selected feature</th>
                <th>Validation</th>
                <th>Created</th>
              </tr>
            </thead>
            <tbody>
              {evaluations.length === 0 ? (
                <tr>
                  <td colSpan={5}>No evaluation snapshots were found for the current filters.</td>
                </tr>
              ) : (
                evaluations.map((snapshot) => (
                  <tr
                    className={selectedEvaluationId === snapshot.id ? "row-active" : undefined}
                    key={snapshot.id}
                    onClick={() =>
                      onNavigate({ name: "model-evaluation-detail", evaluationId: snapshot.id })
                    }
                  >
                    <td>#{snapshot.id}</td>
                    <td>{snapshot.model_family}</td>
                    <td>{snapshot.selected_feature ?? "n/a"}</td>
                    <td>{formatCompactNumber(snapshot.validation_metric_value, 4)}</td>
                    <td>{formatTimestamp(snapshot.created_at)}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </article>

      {detailContent ?? (
        <article className="panel focus-panel">
          <p className="sub-panel-meta">Open an evaluation snapshot to inspect metric and payload details.</p>
        </article>
      )}
    </section>
  );
}

type SelectionsPageProps = {
  detailContent?: ReactNode;
  onNavigate: (route: AppRoute) => void;
  selectedSelectionId?: number | null;
  selections: ModelAdminSelectionSnapshot[];
};

export function ModelSelectionsPage({
  detailContent,
  onNavigate,
  selectedSelectionId,
  selections
}: SelectionsPageProps) {
  return (
    <section className="dashboard-grid">
      <article className="panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Selections</p>
            <h2>Selection snapshots</h2>
          </div>
          <div className="pill-row">
            <span className="pill">{selections.length} selections</span>
          </div>
        </div>

        <div className="table-shell">
          <table>
            <thead>
              <tr>
                <th>Selection</th>
                <th>Family</th>
                <th>Policy</th>
                <th>Active</th>
                <th>Created</th>
              </tr>
            </thead>
            <tbody>
              {selections.length === 0 ? (
                <tr>
                  <td colSpan={5}>No selection snapshots were found for the current filters.</td>
                </tr>
              ) : (
                selections.map((selection) => (
                  <tr
                    className={selectedSelectionId === selection.id ? "row-active" : undefined}
                    key={selection.id}
                    onClick={() =>
                      onNavigate({ name: "model-selection-detail", selectionId: selection.id })
                    }
                  >
                    <td>#{selection.id}</td>
                    <td>{selection.model_family}</td>
                    <td>{selection.selection_policy_name}</td>
                    <td>{selection.is_active ? "Yes" : "No"}</td>
                    <td>{formatTimestamp(selection.created_at)}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </article>

      {detailContent ?? (
        <article className="panel focus-panel">
          <p className="sub-panel-meta">Open a selection snapshot to inspect promotion rationale.</p>
        </article>
      )}
    </section>
  );
}
