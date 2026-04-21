import { ProvenanceRibbon, StatTile } from "./appSharedComponents";
import type { AppRoute } from "./appTypes";
import type {
  ModelAdminEvaluationSnapshot,
  ModelAdminRegistryEntry,
  ModelAdminRun,
  ModelAdminSelectionSnapshot
} from "./modelAdminTypes";
import { formatCompactNumber, formatLabel, formatTimestamp, readNested, routeHash } from "./appUtils";
import {
  getModelFamilyLabel,
  getModelRunLabel,
  getModelScopeLabel,
  getSelectedFeatureLabel
} from "../../shared/frontend/domain";
import { formatJsonLikeValue } from "../../shared/frontend/detailFormatting";

function JsonBlock({ value }: { value: unknown }) {
  const formattedValue = formatJsonLikeValue(value);
  if (!formattedValue) {
    return <p className="sub-panel-meta">No payload was stored for this section.</p>;
  }

  return <pre className="json-block">{formattedValue}</pre>;
}

function SummaryChips({
  entries
}: {
  entries: Array<{ label: string; value: string | null | undefined }>;
}) {
  const visibleEntries = entries.filter((entry) => entry.value && String(entry.value).trim());
  if (visibleEntries.length === 0) {
    return <p className="sub-panel-meta">No additional highlights were stored for this artifact.</p>;
  }

  return (
    <div className="chip-list">
      {visibleEntries.map((entry) => (
        <span className="pill" key={`${entry.label}-${entry.value}`}>
          {entry.label}: {entry.value}
        </span>
      ))}
    </div>
  );
}

function RelatedActions({
  actions,
  title = "Related navigation"
}: {
  actions: Array<{ label: string; route: AppRoute }>;
  title?: string;
}) {
  if (actions.length === 0) {
    return null;
  }

  return (
    <section className="sub-panel">
      <p className="sub-panel-title">{title}</p>
      <div className="route-action-row">
        {actions.map((action) => (
          <a
            className="secondary-button inline-link-button"
            href={routeHash(action.route)}
            key={`${action.label}-${action.route.name}`}
          >
            {action.label}
          </a>
        ))}
      </div>
    </section>
  );
}

export function ModelRegistryDetailCard({
  entry
}: {
  entry: ModelAdminRegistryEntry | null;
}) {
  if (!entry) {
    return (
      <article className="panel focus-panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Registry detail</p>
            <h2>Registry entry not available</h2>
          </div>
        </div>
        <p className="sub-panel-meta">Choose a registry row to inspect its configuration and metadata.</p>
      </article>
    );
  }

  return (
    <article className="panel focus-panel" data-testid="registry-detail-card">
      <div className="section-heading">
        <div>
          <p className="eyebrow">Registry detail</p>
          <h2>{entry.model_family}</h2>
        </div>
        <div className="pill-row">
          <span className="pill">{entry.version_label}</span>
          <span className="pill">{formatLabel(entry.target_task)}</span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile label="Model key" value={entry.model_key} />
        <StatTile label="Created" value={formatTimestamp(entry.created_at)} />
        <StatTile label="Target task" value={formatLabel(entry.target_task)} />
        <StatTile label="Version" value={entry.version_label} />
      </div>

      <section className="sub-panel">
        <p className="sub-panel-title">Description</p>
        <p className="sub-panel-meta">{entry.description || "No description stored."}</p>
      </section>

      <section className="sub-panel">
        <p className="sub-panel-title">Config</p>
        <JsonBlock value={entry.config} />
      </section>
    </article>
  );
}

export function ModelAdminRunDetailCard({
  run
}: {
  run: ModelAdminRun | null;
}) {
  if (!run) {
    return (
      <article className="panel focus-panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Training run detail</p>
            <h2>Training run not available</h2>
          </div>
        </div>
        <p className="sub-panel-meta">Open a run from the list to inspect its artifact and metrics.</p>
      </article>
    );
  }

  return (
    <article className="panel focus-panel" data-testid="run-detail-card">
      <ProvenanceRibbon
        items={[
          { label: "Run", value: getModelRunLabel(run.id) },
          { label: "Task", value: formatLabel(run.target_task) },
          { label: "Family", value: getModelFamilyLabel(run) }
        ]}
        title="Training artifact"
      />

      <div className="section-heading">
        <div>
          <p className="eyebrow">Training run detail</p>
          <h2>{getModelRunLabel(run.id)}</h2>
        </div>
        <div className="pill-row">
          <span className="pill">{getModelFamilyLabel(run)}</span>
          <span className="pill">{run.status}</span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile label="Target task" value={formatLabel(run.target_task)} />
        <StatTile label="Team scope" value={getModelScopeLabel(run)} />
        <StatTile label="Season" value={run.season_label ?? "all seasons"} />
        <StatTile label="Completed" value={formatTimestamp(run.completed_at ?? run.created_at)} />
      </div>

      <div className="detail-section-grid">
        <section className="sub-panel">
          <p className="sub-panel-title">Artifact summary</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Selected feature</span>
              <strong>{getSelectedFeatureLabel(run)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Fallback strategy</span>
              <strong>{String(readNested(run.artifact, "fallback_strategy") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Fallback reason</span>
              <strong>{String(readNested(run.artifact, "fallback_reason") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Train ratio</span>
              <strong>{formatCompactNumber(run.train_ratio, 2)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Validation ratio</span>
              <strong>{formatCompactNumber(run.validation_ratio, 2)}</strong>
            </div>
          </div>
          <SummaryChips
            entries={[
              {
                label: "Validation predictions",
                value: String(readNested(run.metrics, "validation", "prediction_count") ?? "")
              },
              {
                label: "Test predictions",
                value: String(readNested(run.metrics, "test", "prediction_count") ?? "")
              },
              {
                label: "Selected branch",
                value: String(readNested(run.artifact, "selection_metrics", "selected_branch") ?? "")
              }
            ]}
          />
        </section>

        <section className="sub-panel">
          <p className="sub-panel-title">Metrics summary</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Train count</span>
              <strong>{String(readNested(run.metrics, "train", "prediction_count") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Validation count</span>
              <strong>{String(readNested(run.metrics, "validation", "prediction_count") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Validation MAE</span>
              <strong>{String(readNested(run.metrics, "validation", "mae") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Test count</span>
              <strong>{String(readNested(run.metrics, "test", "prediction_count") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Test MAE</span>
              <strong>{String(readNested(run.metrics, "test", "mae") ?? "n/a")}</strong>
            </div>
          </div>
        </section>
      </div>

      <RelatedActions
        actions={[
          { label: "Open runs list", route: { name: "model-runs" } },
          { label: "Open evaluations", route: { name: "model-evaluations" } },
          { label: "Backtests workspace", route: { name: "backtests" } },
          { label: "Opportunities workspace", route: { name: "opportunities" } }
        ]}
      />

      <div className="detail-section-grid">
        <section className="sub-panel">
          <p className="sub-panel-title">Artifact payload</p>
          <JsonBlock value={run.artifact} />
        </section>
        <section className="sub-panel">
          <p className="sub-panel-title">Metrics payload</p>
          <JsonBlock value={run.metrics} />
        </section>
      </div>
    </article>
  );
}

export function ModelAdminEvaluationDetailCard({
  evaluation
}: {
  evaluation: ModelAdminEvaluationSnapshot | null;
}) {
  if (!evaluation) {
    return (
      <article className="panel focus-panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Evaluation detail</p>
            <h2>Evaluation snapshot not available</h2>
          </div>
        </div>
        <p className="sub-panel-meta">Open an evaluation snapshot from the list to inspect its metrics.</p>
      </article>
    );
  }

  return (
    <article className="panel focus-panel" data-testid="evaluation-detail-card">
      <ProvenanceRibbon
        items={[
          { label: "Evaluation", value: `#${evaluation.id}` },
          { label: "Family", value: getModelFamilyLabel(evaluation) },
          { label: "Task", value: formatLabel(evaluation.target_task) }
        ]}
        title="Evaluation artifact"
      />

      <div className="section-heading">
        <div>
          <p className="eyebrow">Evaluation detail</p>
          <h2>Evaluation #{evaluation.id}</h2>
        </div>
        <div className="pill-row">
          <span className="pill">{getModelFamilyLabel(evaluation)}</span>
          <span className="pill">{evaluation.primary_metric_name ?? "metric n/a"}</span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile label="Selected feature" value={getSelectedFeatureLabel(evaluation)} />
        <StatTile label="Fallback" value={evaluation.fallback_strategy ?? "primary fit"} />
        <StatTile label="Validation" value={formatCompactNumber(evaluation.validation_metric_value, 4)} />
        <StatTile label="Test" value={formatCompactNumber(evaluation.test_metric_value, 4)} />
      </div>

      <div className="detail-section-grid">
        <section className="sub-panel">
          <p className="sub-panel-title">Prediction counts</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Validation predictions</span>
              <strong>{String(evaluation.validation_prediction_count)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Test predictions</span>
              <strong>{String(evaluation.test_prediction_count)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Training run</span>
              <strong>{evaluation.model_training_run_id ? `#${evaluation.model_training_run_id}` : "n/a"}</strong>
            </div>
          </div>
          <SummaryChips
            entries={[
              { label: "Primary metric", value: evaluation.primary_metric_name },
              { label: "Selected feature", value: getSelectedFeatureLabel(evaluation) },
              { label: "Fallback strategy", value: evaluation.fallback_strategy }
            ]}
          />
        </section>

        <section className="sub-panel">
          <p className="sub-panel-title">Snapshot payload</p>
          <JsonBlock value={evaluation.snapshot} />
        </section>
      </div>

      <RelatedActions
        actions={[
          ...(evaluation.model_training_run_id
            ? [{ label: "Open related run", route: { name: "model-run-detail", runId: evaluation.model_training_run_id } as AppRoute }]
            : []),
          { label: "Open selections", route: { name: "model-selections" } },
          { label: "Backtests workspace", route: { name: "backtests" } },
          { label: "Opportunities workspace", route: { name: "opportunities" } }
        ]}
      />
    </article>
  );
}

export function ModelAdminSelectionDetailCard({
  selection
}: {
  selection: ModelAdminSelectionSnapshot | null;
}) {
  if (!selection) {
    return (
      <article className="panel focus-panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Selection detail</p>
            <h2>Selection snapshot not available</h2>
          </div>
        </div>
        <p className="sub-panel-meta">Open a selection snapshot from the list to inspect policy and rationale.</p>
      </article>
    );
  }

  return (
    <article className="panel focus-panel" data-testid="selection-detail-card">
      <ProvenanceRibbon
        items={[
          { label: "Selection", value: `#${selection.id}` },
          { label: "Family", value: getModelFamilyLabel(selection) },
          { label: "Policy", value: selection.selection_policy_name }
        ]}
        title="Promotion artifact"
      />

      <div className="section-heading">
        <div>
          <p className="eyebrow">Selection detail</p>
          <h2>Selection #{selection.id}</h2>
        </div>
        <div className="pill-row">
          <span className="pill">{getModelFamilyLabel(selection)}</span>
          <span className="pill">{selection.is_active ? "active" : "historical"}</span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile label="Target task" value={formatLabel(selection.target_task)} />
        <StatTile label="Policy" value={selection.selection_policy_name} />
        <StatTile label="Training run" value={selection.model_training_run_id ? `#${selection.model_training_run_id}` : "n/a"} />
        <StatTile label="Evaluation" value={selection.model_evaluation_snapshot_id ? `#${selection.model_evaluation_snapshot_id}` : "n/a"} />
      </div>

      <section className="sub-panel">
        <p className="sub-panel-title">Rationale payload</p>
        <SummaryChips
          entries={[
            { label: "Policy", value: selection.selection_policy_name },
            { label: "Active", value: selection.is_active ? "true" : "false" },
            {
              label: "Linked evaluation",
              value: selection.model_evaluation_snapshot_id ? `#${selection.model_evaluation_snapshot_id}` : null
            }
          ]}
        />
        <JsonBlock value={selection.rationale} />
      </section>

      <RelatedActions
        actions={[
          ...(selection.model_training_run_id
            ? [{ label: "Open related run", route: { name: "model-run-detail", runId: selection.model_training_run_id } as AppRoute }]
            : []),
          ...(selection.model_evaluation_snapshot_id
            ? [
                {
                  label: "Open related evaluation",
                  route: { name: "model-evaluation-detail", evaluationId: selection.model_evaluation_snapshot_id } as AppRoute
                }
              ]
            : []),
          { label: "Backtests workspace", route: { name: "backtests" } },
          { label: "Opportunities workspace", route: { name: "opportunities" } }
        ]}
      />
    </article>
  );
}
