import { ProvenanceRibbon, StatTile } from "./appSharedComponents";
import type {
  EvaluationSnapshot,
  FoldSummary,
  ModelTrainingRun,
  OpportunityRecord,
  ProvenanceItem,
  ScoringRunDetail,
  SelectionSnapshot
} from "./appTypes";
import {
  formatCompactNumber,
  formatDelta,
  formatLabel,
  formatTimestamp,
  getAlignmentLabel,
  getAlignmentTone,
  getMetricDelta,
  readNested,
  routeHash
} from "./appUtils";

export function ModelRunArtifactDetail({
  modelRun,
  provenanceItems
}: {
  modelRun: ModelTrainingRun | null;
  provenanceItems?: ProvenanceItem[];
}) {
  if (!modelRun) {
    return (
      <article className="panel focus-panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Training run detail</p>
            <h2>Training run not available</h2>
          </div>
        </div>
        <p className="lead detail-copy">
          The linked training run could not be resolved from the current opportunity provenance.
        </p>
      </article>
    );
  }

  return (
    <article className="panel focus-panel">
      <ProvenanceRibbon items={provenanceItems ?? []} title="Provenance" />

      <div className="section-heading">
        <div>
          <p className="eyebrow">Training run detail</p>
          <h2>Run #{modelRun.id}</h2>
        </div>
        <div className="pill-row">
          <span className="pill">{String(readNested(modelRun.artifact, "model_family") ?? "n/a")}</span>
          <span className="pill">{modelRun.status}</span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile label="Target task" value={modelRun.target_task} />
        <StatTile label="Season" value={modelRun.season_label ?? "n/a"} />
        <StatTile label="Train ratio" value={formatCompactNumber(modelRun.train_ratio, 2)} />
        <StatTile label="Validation ratio" value={formatCompactNumber(modelRun.validation_ratio, 2)} />
      </div>

      <div className="detail-section-grid">
        <section className="sub-panel">
          <p className="sub-panel-title">Artifact</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Selected feature</span>
              <strong>{String(readNested(modelRun.artifact, "selected_feature") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Fallback strategy</span>
              <strong>{String(readNested(modelRun.artifact, "fallback_strategy") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Fallback reason</span>
              <strong>{String(readNested(modelRun.artifact, "fallback_reason") ?? "n/a")}</strong>
            </div>
          </div>
        </section>

        <section className="sub-panel">
          <p className="sub-panel-title">Metrics</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Validation count</span>
              <strong>{String(readNested(modelRun.metrics, "validation", "prediction_count") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Validation MAE</span>
              <strong>{String(readNested(modelRun.metrics, "validation", "mae") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Test MAE</span>
              <strong>{String(readNested(modelRun.metrics, "test", "mae") ?? "n/a")}</strong>
            </div>
          </div>
        </section>
      </div>
    </article>
  );
}

export function SelectionArtifactDetail({
  selection,
  provenanceItems
}: {
  selection: SelectionSnapshot | null;
  provenanceItems?: ProvenanceItem[];
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
      </article>
    );
  }

  return (
    <article className="panel focus-panel">
      <ProvenanceRibbon items={provenanceItems ?? []} title="Provenance" />

      <div className="section-heading">
        <div>
          <p className="eyebrow">Selection detail</p>
          <h2>Selection #{selection.id}</h2>
        </div>
        <div className="pill-row">
          <span className="pill">{selection.model_family}</span>
          <span className="pill">{selection.is_active ? "active" : "inactive"}</span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile label="Target task" value={selection.target_task} />
        <StatTile label="Policy" value={selection.selection_policy_name} />
        <StatTile label="Training run" value={selection.model_training_run_id ? `#${selection.model_training_run_id}` : "n/a"} />
        <StatTile label="Evaluation" value={selection.model_evaluation_snapshot_id ? `#${selection.model_evaluation_snapshot_id}` : "n/a"} />
      </div>

      <section className="sub-panel">
        <p className="sub-panel-title">Rationale</p>
        <p className="sub-panel-meta">{selection.rationale ?? "No rationale was stored for this snapshot."}</p>
      </section>
    </article>
  );
}

export function EvaluationArtifactDetail({
  evaluation,
  provenanceItems
}: {
  evaluation: EvaluationSnapshot | null;
  provenanceItems?: ProvenanceItem[];
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
      </article>
    );
  }

  return (
    <article className="panel focus-panel">
      <ProvenanceRibbon items={provenanceItems ?? []} title="Provenance" />

      <div className="section-heading">
        <div>
          <p className="eyebrow">Evaluation detail</p>
          <h2>Evaluation #{evaluation.id}</h2>
        </div>
        <div className="pill-row">
          <span className="pill">{evaluation.model_family}</span>
          <span className="pill">{evaluation.primary_metric_name ?? "n/a"}</span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile label="Selected feature" value={evaluation.selected_feature ?? "n/a"} />
        <StatTile label="Fallback" value={evaluation.fallback_strategy ?? "n/a"} />
        <StatTile label="Validation" value={formatCompactNumber(evaluation.validation_metric_value, 4)} />
        <StatTile label="Test" value={formatCompactNumber(evaluation.test_metric_value, 4)} />
      </div>

      <section className="detail-section-grid">
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
          </div>
        </section>

        <section className="sub-panel">
          <p className="sub-panel-title">Snapshot metadata</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Training run</span>
              <strong>{evaluation.model_training_run_id ? `#${evaluation.model_training_run_id}` : "n/a"}</strong>
            </div>
            <div className="detail-list-item">
              <span>Created</span>
              <strong>{formatTimestamp(evaluation.created_at)}</strong>
            </div>
          </div>
        </section>
      </section>
    </article>
  );
}

export function ScoringRunArtifactDetail({
  scoringRun,
  provenanceItems
}: {
  scoringRun: ScoringRunDetail | null;
  provenanceItems?: ProvenanceItem[];
}) {
  if (!scoringRun) {
    return (
      <article className="panel focus-panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Scoring run detail</p>
            <h2>Scoring run not available</h2>
          </div>
        </div>
      </article>
    );
  }

  return (
    <article className="panel focus-panel">
      <ProvenanceRibbon items={provenanceItems ?? []} title="Provenance" />

      <div className="section-heading">
        <div>
          <p className="eyebrow">Scoring run detail</p>
          <h2>Scoring #{scoringRun.id}</h2>
        </div>
        <div className="pill-row">
          <span className="pill">{scoringRun.policy_name}</span>
          <span className="pill">{scoringRun.target_task}</span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile label="Scenario key" value={scoringRun.scenario_key} />
        <StatTile label="Prediction count" value={String(scoringRun.prediction_count)} />
        <StatTile label="Review opportunities" value={String(scoringRun.review_opportunity_count)} />
        <StatTile label="Candidate opportunities" value={String(scoringRun.candidate_opportunity_count)} />
      </div>

      <div className="detail-section-grid">
        <section className="sub-panel">
          <p className="sub-panel-title">Scenario</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Home team</span>
              <strong>{scoringRun.home_team_code}</strong>
            </div>
            <div className="detail-list-item">
              <span>Away team</span>
              <strong>{scoringRun.away_team_code}</strong>
            </div>
            <div className="detail-list-item">
              <span>Game date</span>
              <strong>{scoringRun.game_date}</strong>
            </div>
          </div>
        </section>

        <section className="sub-panel">
          <p className="sub-panel-title">Market context</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Home spread</span>
              <strong>{formatCompactNumber(scoringRun.home_spread_line, 2)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Total line</span>
              <strong>{formatCompactNumber(scoringRun.total_line, 2)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Discarded opportunities</span>
              <strong>{String(scoringRun.discarded_opportunity_count)}</strong>
            </div>
          </div>
        </section>
      </div>
    </article>
  );
}

export function ArtifactCompareView({
  runId,
  fold,
  foldEvaluation,
  opportunity,
  opportunityEvaluation,
  selection,
  compareHref
}: {
  runId: number | null;
  fold: FoldSummary | null;
  foldEvaluation: EvaluationSnapshot | null;
  opportunity: OpportunityRecord | null;
  opportunityEvaluation: EvaluationSnapshot | null;
  selection: SelectionSnapshot | null;
  compareHref?: string;
}) {
  if (!fold || !opportunity) {
    return (
      <article className="panel focus-panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Artifact comparison</p>
            <h2>Comparison context not available</h2>
          </div>
        </div>
        <p className="lead detail-copy">
          Open one backtest fold and one active opportunity to compare their evaluation artifacts,
          promoted selection, and drift signals side by side.
        </p>
      </article>
    );
  }

  const foldValidation = foldEvaluation?.validation_metric_value ?? fold.selected_model.validation_metric_value;
  const foldTest = foldEvaluation?.test_metric_value ?? fold.selected_model.test_metric_value;
  const opportunityValidation = opportunityEvaluation?.validation_metric_value ?? null;
  const opportunityTest = opportunityEvaluation?.test_metric_value ?? null;
  const familyAligned =
    opportunityEvaluation?.model_family !== undefined
      ? fold.selected_model.model_family === opportunityEvaluation.model_family
      : selection?.model_family !== undefined
        ? fold.selected_model.model_family === selection.model_family
        : null;
  const featureAligned =
    opportunityEvaluation?.selected_feature !== undefined
      ? (fold.selected_model.selected_feature ?? "fallback") ===
        (opportunityEvaluation.selected_feature ?? "fallback")
      : null;
  const selectionEvaluationLinked =
    selection?.model_evaluation_snapshot_id !== undefined &&
    selection?.model_evaluation_snapshot_id !== null &&
    opportunityEvaluation?.id !== undefined
      ? selection.model_evaluation_snapshot_id === opportunityEvaluation.id
      : null;
  const validationDelta = getMetricDelta(foldValidation, opportunityValidation);
  const testDelta = getMetricDelta(foldTest, opportunityTest);
  const validationDeltaAbs = validationDelta === null ? null : Math.abs(validationDelta);
  const testDeltaAbs = testDelta === null ? null : Math.abs(testDelta);
  const mismatchMessages = [
    familyAligned === false
      ? `Backtest fold selected ${fold.selected_model.model_family}, but the opportunity evaluation is using ${opportunityEvaluation?.model_family ?? "a different model context"}.`
      : null,
    featureAligned === false
      ? `Selected feature drifted from ${fold.selected_model.selected_feature ?? "fallback"} to ${opportunityEvaluation?.selected_feature ?? "fallback"}.`
      : null,
    selectionEvaluationLinked === false
      ? `The promoted selection points to evaluation #${selection?.model_evaluation_snapshot_id}, while the active opportunity is using evaluation #${opportunityEvaluation?.id}.`
      : null,
    validationDelta !== null && Math.abs(validationDelta) >= 0.05
      ? `Validation metric moved by ${formatDelta(validationDelta)} between the backtest fold and the active opportunity evaluation.`
      : null,
    testDelta !== null && Math.abs(testDelta) >= 0.05
      ? `Test metric moved by ${formatDelta(testDelta)} between the backtest fold and the active opportunity evaluation.`
      : null
  ].filter((message): message is string => Boolean(message));
  const alignmentSummary = [
    {
      label: "Model family",
      value: getAlignmentLabel(familyAligned),
      tone: getAlignmentTone(familyAligned),
      detail:
        opportunityEvaluation?.model_family !== undefined
          ? `${fold.selected_model.model_family} vs ${opportunityEvaluation.model_family}`
          : selection?.model_family ?? "No opportunity model family"
    },
    {
      label: "Selected feature",
      value: getAlignmentLabel(featureAligned),
      tone: getAlignmentTone(featureAligned),
      detail: `${fold.selected_model.selected_feature ?? "fallback"} vs ${
        opportunityEvaluation?.selected_feature ?? "n/a"
      }`
    },
    {
      label: "Selection link",
      value: getAlignmentLabel(selectionEvaluationLinked),
      tone: getAlignmentTone(selectionEvaluationLinked),
      detail:
        selection?.model_evaluation_snapshot_id && opportunityEvaluation?.id
          ? `#${selection.model_evaluation_snapshot_id} vs #${opportunityEvaluation.id}`
          : "Missing evaluation linkage"
    }
  ];
  const artifactLinks = [
    runId
      ? {
          href: routeHash({
            name: "backtest-fold-evaluation",
            runId,
            foldIndex: fold.fold_index,
            evaluationId: fold.selected_model.evaluation_snapshot_id
          }),
          label: "Open fold evaluation"
        }
      : null,
    opportunity.model_evaluation_snapshot_id
      ? {
          href: routeHash({
            name: "opportunity-evaluation",
            opportunityId: opportunity.id,
            evaluationId: opportunity.model_evaluation_snapshot_id
          }),
          label: "Open opportunity evaluation"
        }
      : null,
    opportunity.model_selection_snapshot_id
      ? {
          href: routeHash({
            name: "opportunity-selection",
            opportunityId: opportunity.id,
            selectionId: opportunity.model_selection_snapshot_id
          }),
          label: "Open promoted selection"
        }
      : null
  ].filter((link): link is { href: string; label: string } => Boolean(link));
  const mismatchCount = mismatchMessages.length;
  const severeDrift =
    (validationDeltaAbs !== null && validationDeltaAbs >= 0.1) ||
    (testDeltaAbs !== null && testDeltaAbs >= 0.1) ||
    familyAligned === false;
  const moderateDrift =
    severeDrift ||
    mismatchCount >= 2 ||
    (validationDeltaAbs !== null && validationDeltaAbs >= 0.05) ||
    (testDeltaAbs !== null && testDeltaAbs >= 0.05) ||
    featureAligned === false ||
    selectionEvaluationLinked === false;
  const comparisonStatus = severeDrift
    ? "high_drift"
    : moderateDrift
      ? "review_drift"
      : "aligned";
  const comparisonTone =
    comparisonStatus === "aligned"
      ? "good"
      : comparisonStatus === "review_drift"
        ? "warning"
        : "critical";
  const comparisonHeadline =
    comparisonStatus === "aligned"
      ? "Backtest and live opportunity artifacts are materially aligned."
      : comparisonStatus === "review_drift"
        ? "There is measurable drift between the validation fold and the active opportunity."
        : "This opportunity has drifted far enough from the backtest fold to warrant extra caution.";
  const comparisonAction =
    comparisonStatus === "aligned"
      ? "Proceed with normal analyst review and keep this opportunity in the current workflow."
      : comparisonStatus === "review_drift"
        ? "Review the evaluation artifact and promoted selection before trusting the live opportunity."
        : "Treat this as a high-risk mismatch until the opportunity evaluation and promoted selection are reconciled.";
  const nextStepItems =
    comparisonStatus === "aligned"
      ? [
          "Confirm the market context still matches the scenario assumptions.",
          "Use the opportunity deep-dive to inspect comparables and recommendation rationale."
        ]
      : comparisonStatus === "review_drift"
        ? [
            "Open the opportunity evaluation artifact and compare its metrics to the fold evaluation.",
            "Verify the promoted selection still points to the expected evaluation snapshot.",
            "Re-check the comparable cases before escalating the opportunity."
          ]
        : [
            "Open both evaluation artifacts and inspect why the model family or metrics diverged.",
            "Confirm the promoted selection is still the right active artifact for this target task.",
            "Do not treat this opportunity as production-grade without analyst sign-off."
          ];

  return (
    <article className="panel focus-panel">
      <ProvenanceRibbon
        items={[
          ...(runId
            ? [
                {
                  href: routeHash({ name: "backtest-run", runId }),
                  label: "Run route",
                  value: `Run #${runId}`
                }
              ]
            : []),
          ...(compareHref ? [{ href: compareHref, label: "Compare route", value: "Artifact compare" }] : []),
          {
            href: routeHash({
              name: "opportunity-detail",
              opportunityId: opportunity.id
            }),
            label: "Opportunity",
            value: `Opportunity #${opportunity.id}`
          }
        ]}
        title="Provenance"
      />

      <div className="section-heading">
        <div>
          <p className="eyebrow">Artifact comparison</p>
          <h2>Fold vs opportunity evidence</h2>
        </div>
        <div className="pill-row">
          <span className="pill">Fold {fold.fold_index}</span>
          <span className="pill">{opportunity.team_code} vs {opportunity.opponent_code}</span>
        </div>
      </div>

      <div className="route-action-row">
        {artifactLinks.map((link) => (
          <a className="secondary-button inline-link-button" href={link.href} key={link.href}>
            {link.label}
          </a>
        ))}
      </div>

      <section className="section-stack detail-section-stack">
        <div className="section-heading standalone">
          <div>
            <p className="eyebrow">Decision summary</p>
            <h3>Analyst guidance</h3>
          </div>
        </div>

        <div className={`sub-panel compare-decision-card compare-decision-card-${comparisonTone}`}>
          <div className="section-heading compact-heading">
            <div>
              <p className="sub-panel-title">Comparison verdict</p>
              <p className="sub-panel-stat">{formatLabel(comparisonStatus)}</p>
            </div>
            <div className="pill-row">
              <span className={`compare-status-pill compare-status-pill-${comparisonTone}`}>
                {formatLabel(comparisonStatus)}
              </span>
            </div>
          </div>
          <p className="detail-copy">{comparisonHeadline}</p>
          <p className="sub-panel-meta">{comparisonAction}</p>
        </div>

        <div className="compare-decision-grid">
          <div className="sub-panel alignment-card">
            <p className="sub-panel-title">Mismatch count</p>
            <p className="sub-panel-stat">{String(mismatchCount)}</p>
            <p className="sub-panel-meta">Explicit drift conditions currently triggered.</p>
          </div>
          <div className="sub-panel alignment-card">
            <p className="sub-panel-title">Largest metric drift</p>
            <p className="sub-panel-stat">
              {formatDelta(
                validationDeltaAbs !== null && testDeltaAbs !== null
                  ? (validationDeltaAbs >= testDeltaAbs ? validationDelta : testDelta)
                  : validationDelta ?? testDelta
              )}
            </p>
            <p className="sub-panel-meta">Largest observed gap between fold and opportunity metrics.</p>
          </div>
          <div className="sub-panel alignment-card">
            <p className="sub-panel-title">Recommended posture</p>
            <p className="sub-panel-stat">
              {comparisonStatus === "aligned"
                ? "Normal review"
                : comparisonStatus === "review_drift"
                  ? "Manual review"
                  : "Escalate"}
            </p>
            <p className="sub-panel-meta">Suggested analyst handling for this comparison state.</p>
          </div>
        </div>
      </section>

      <section className="section-stack detail-section-stack">
        <div className="section-heading standalone">
          <div>
            <p className="eyebrow">Alignment summary</p>
            <h3>Cross-artifact checks</h3>
          </div>
        </div>

        <div className="compare-summary-grid">
          {alignmentSummary.map((item) => (
            <div className={`sub-panel alignment-card alignment-card-${item.tone}`} key={item.label}>
              <p className="sub-panel-title">{item.label}</p>
              <p className="sub-panel-stat">{formatLabel(item.value)}</p>
              <p className="sub-panel-meta">{item.detail}</p>
            </div>
          ))}
          <div className="sub-panel alignment-card">
            <p className="sub-panel-title">Validation delta</p>
            <p className="sub-panel-stat">{formatDelta(validationDelta)}</p>
            <p className="sub-panel-meta">
              Fold {formatCompactNumber(foldValidation, 4)} vs opportunity{" "}
              {formatCompactNumber(opportunityValidation, 4)}
            </p>
          </div>
          <div className="sub-panel alignment-card">
            <p className="sub-panel-title">Test delta</p>
            <p className="sub-panel-stat">{formatDelta(testDelta)}</p>
            <p className="sub-panel-meta">
              Fold {formatCompactNumber(foldTest, 4)} vs opportunity {formatCompactNumber(opportunityTest, 4)}
            </p>
          </div>
        </div>
      </section>

      <div className="compare-grid">
        <section className="sub-panel compare-card">
          <p className="sub-panel-title">Backtest fold evaluation</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Model family</span>
              <strong>{fold.selected_model.model_family}</strong>
            </div>
            <div className="detail-list-item">
              <span>Selected feature</span>
              <strong>{fold.selected_model.selected_feature ?? "fallback"}</strong>
            </div>
            <div className="detail-list-item">
              <span>Validation metric</span>
              <strong>{formatCompactNumber(foldValidation, 4)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Test metric</span>
              <strong>{formatCompactNumber(foldTest, 4)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Evaluation id</span>
              <strong>#{fold.selected_model.evaluation_snapshot_id}</strong>
            </div>
          </div>
        </section>

        <section className="sub-panel compare-card">
          <p className="sub-panel-title">Opportunity evaluation</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Model family</span>
              <strong>{opportunityEvaluation?.model_family ?? "n/a"}</strong>
            </div>
            <div className="detail-list-item">
              <span>Selected feature</span>
              <strong>{opportunityEvaluation?.selected_feature ?? "n/a"}</strong>
            </div>
            <div className="detail-list-item">
              <span>Validation metric</span>
              <strong>{formatCompactNumber(opportunityEvaluation?.validation_metric_value ?? null, 4)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Test metric</span>
              <strong>{formatCompactNumber(opportunityEvaluation?.test_metric_value ?? null, 4)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Evaluation id</span>
              <strong>{opportunityEvaluation ? `#${opportunityEvaluation.id}` : "n/a"}</strong>
            </div>
          </div>
        </section>

        <section className="sub-panel compare-card">
          <p className="sub-panel-title">Promoted selection</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Model family</span>
              <strong>{selection?.model_family ?? "n/a"}</strong>
            </div>
            <div className="detail-list-item">
              <span>Selection policy</span>
              <strong>{selection?.selection_policy_name ?? "n/a"}</strong>
            </div>
            <div className="detail-list-item">
              <span>Active</span>
              <strong>{selection ? String(selection.is_active) : "n/a"}</strong>
            </div>
            <div className="detail-list-item">
              <span>Evaluation link</span>
              <strong>
                {selection?.model_evaluation_snapshot_id ? `#${selection.model_evaluation_snapshot_id}` : "n/a"}
              </strong>
            </div>
            <div className="detail-list-item">
              <span>Rationale</span>
              <strong>{selection?.rationale ?? "n/a"}</strong>
            </div>
          </div>
        </section>
      </div>

      <section className="section-stack detail-section-stack">
        <div className="section-heading standalone">
          <div>
            <p className="eyebrow">Mismatch review</p>
            <h3>Where the artifacts diverge</h3>
          </div>
        </div>

        {mismatchMessages.length > 0 ? (
          <div className="compare-warning-list">
            {mismatchMessages.map((message) => (
              <article className="sub-panel compare-warning-card" key={message}>
                <p className="sub-panel-title">Attention point</p>
                <p className="detail-copy">{message}</p>
              </article>
            ))}
          </div>
        ) : (
          <article className="sub-panel compare-warning-card compare-warning-card-good">
            <p className="sub-panel-title">Alignment status</p>
            <p className="detail-copy">
              No material mismatches were detected across the backtest fold, opportunity evaluation,
              and promoted selection.
            </p>
          </article>
        )}
      </section>

      <section className="section-stack detail-section-stack">
        <div className="section-heading standalone">
          <div>
            <p className="eyebrow">Next steps</p>
            <h3>Recommended review path</h3>
          </div>
        </div>

        <article className="sub-panel">
          <ul className="detail-bullets">
            {nextStepItems.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </article>
      </section>
    </article>
  );
}
