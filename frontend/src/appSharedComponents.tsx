import type { ProvenanceInspectorData, ProvenanceItem } from "./appTypes";
import { formatCompactNumber } from "./appUtils";
import {
  getModelFamilyLabel,
  getModelMetricValue,
  getModelRunLabel,
  getSelectedFeatureLabel
} from "../../shared/frontend/domain";

export function StatTile({
  label,
  value,
  detail,
  testId
}: {
  label: string;
  value: string;
  detail?: string;
  testId?: string;
}) {
  return (
    <article className="stat-tile" data-testid={testId}>
      <p className="stat-label">{label}</p>
      <p className="stat-value">{value}</p>
      {detail ? <p className="stat-detail">{detail}</p> : null}
    </article>
  );
}

export function ProvenanceRibbon({
  title,
  items
}: {
  title: string;
  items: ProvenanceItem[];
}) {
  if (items.length === 0) {
    return null;
  }

  return (
    <section className="provenance-ribbon">
      <p className="eyebrow">{title}</p>
      <div className="provenance-grid">
        {items.map((item) => (
          <div className="provenance-item" key={`${item.label}-${item.value}`}>
            <span className="provenance-label">{item.label}</span>
            {item.href ? (
              <a className="provenance-link" href={item.href}>
                {item.value}
              </a>
            ) : (
              <strong className="provenance-value">{item.value}</strong>
            )}
          </div>
        ))}
      </div>
    </section>
  );
}

export function ProvenanceInspector({
  data
}: {
  data: ProvenanceInspectorData;
}) {
  const modelRunValidationMetricValue = data.modelRun ? getModelMetricValue(data.modelRun) : null;
  const modelRunSelectedFeatureValue = data.modelRun ? getSelectedFeatureLabel(data.modelRun) : null;

  if (!data.selection && !data.evaluation && !data.scoringRun && !data.modelHistory && !data.modelRun) {
    return null;
  }

  return (
    <section className="section-stack detail-section-stack">
      <div className="section-heading standalone">
        <div>
          <p className="eyebrow">Provenance inspector</p>
          <h3>Resolved model artifacts</h3>
        </div>
      </div>

      <div className="detail-section-grid">
        {data.modelHistory ? (
          <section className="sub-panel">
            <p className="sub-panel-title">Training history</p>
            <div className="detail-list compact-list">
              <div className="detail-list-item">
                <span>Run count</span>
                <strong>{String(data.modelHistory.overview.run_count)}</strong>
              </div>
              <div className="detail-list-item">
                <span>Fallback runs</span>
                <strong>{String(data.modelHistory.overview.fallback_run_count)}</strong>
              </div>
              <div className="detail-list-item">
                <span>Latest family</span>
                <strong>
                  {getModelFamilyLabel(data.modelHistory.overview.latest_run ?? {})}
                </strong>
              </div>
              <div className="detail-list-item">
                <span>Best family</span>
                <strong>
                  {getModelFamilyLabel(data.modelHistory.overview.best_overall ?? {})}
                </strong>
              </div>
            </div>
          </section>
        ) : null}

        {data.modelRun ? (
          <section className="sub-panel">
            <p className="sub-panel-title">Training run</p>
            <div className="detail-list compact-list">
              <div className="detail-list-item">
                <span>ID</span>
                <strong>{getModelRunLabel(data.modelRun.id)}</strong>
              </div>
              <div className="detail-list-item">
                <span>Model family</span>
                <strong>{getModelFamilyLabel(data.modelRun)}</strong>
              </div>
              <div className="detail-list-item">
                <span>Selected feature</span>
                <strong>{modelRunSelectedFeatureValue}</strong>
              </div>
              <div className="detail-list-item">
                <span>Validation metric</span>
                <strong>
                  {typeof modelRunValidationMetricValue === "number"
                    ? formatCompactNumber(modelRunValidationMetricValue, 4)
                    : "n/a"}
                </strong>
              </div>
            </div>
          </section>
        ) : null}

        {data.selection ? (
          <section className="sub-panel">
            <p className="sub-panel-title">Selection snapshot</p>
            <div className="detail-list compact-list">
              <div className="detail-list-item">
                <span>ID</span>
                <strong>Selection #{data.selection.id}</strong>
              </div>
              <div className="detail-list-item">
                <span>Model family</span>
                <strong>{getModelFamilyLabel(data.selection)}</strong>
              </div>
              <div className="detail-list-item">
                <span>Policy</span>
                <strong>{data.selection.selection_policy_name}</strong>
              </div>
              <div className="detail-list-item">
                <span>Active</span>
                <strong>{data.selection.is_active ? "true" : "false"}</strong>
              </div>
            </div>
          </section>
        ) : null}

        {data.evaluation ? (
          <section className="sub-panel">
            <p className="sub-panel-title">Evaluation snapshot</p>
            <div className="detail-list compact-list">
              <div className="detail-list-item">
                <span>ID</span>
                <strong>Evaluation #{data.evaluation.id}</strong>
              </div>
              <div className="detail-list-item">
                <span>Model family</span>
                <strong>{getModelFamilyLabel(data.evaluation)}</strong>
              </div>
              <div className="detail-list-item">
                <span>Primary metric</span>
                <strong>{data.evaluation.primary_metric_name ?? "n/a"}</strong>
              </div>
              <div className="detail-list-item">
                <span>Validation</span>
                <strong>{formatCompactNumber(data.evaluation.validation_metric_value, 4)}</strong>
              </div>
              <div className="detail-list-item">
                <span>Test</span>
                <strong>{formatCompactNumber(data.evaluation.test_metric_value, 4)}</strong>
              </div>
            </div>
          </section>
        ) : null}
      </div>

      {data.scoringRun ? (
        <section className="sub-panel">
          <p className="sub-panel-title">Scoring run detail</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>ID</span>
              <strong>Scoring #{data.scoringRun.id}</strong>
            </div>
            <div className="detail-list-item">
              <span>Scenario key</span>
              <strong>{data.scoringRun.scenario_key}</strong>
            </div>
            <div className="detail-list-item">
              <span>Prediction count</span>
              <strong>{String(data.scoringRun.prediction_count)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Review opportunities</span>
              <strong>{String(data.scoringRun.review_opportunity_count)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Candidate opportunities</span>
              <strong>{String(data.scoringRun.candidate_opportunity_count)}</strong>
            </div>
          </div>
        </section>
      ) : null}
    </section>
  );
}
