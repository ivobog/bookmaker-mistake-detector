import { ProvenanceInspector, ProvenanceRibbon, StatTile } from "./appSharedComponents";
import type {
  BacktestRun,
  FoldSummary,
  ProvenanceInspectorData,
  ProvenanceItem,
  StrategySummary
} from "./appTypes";
import { formatMetric, formatPercent, routeHash } from "./appUtils";

function StrategyCard({ label, strategy }: { label: string; strategy: StrategySummary }) {
  const edgeBuckets = Object.entries(strategy.edge_bucket_performance);

  return (
    <article className="panel strategy-card">
      <div className="section-heading">
        <div>
          <p className="eyebrow">{label}</p>
          <h3>{strategy.strategy_name}</h3>
        </div>
        <div className="pill-row">
          {"threshold" in strategy && strategy.threshold !== undefined ? (
            <span className="pill">Threshold {formatMetric(strategy.threshold, 1)}</span>
          ) : null}
          <span className="pill">ROI {formatPercent(strategy.roi)}</span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile label="Bets" value={String(strategy.bet_count)} />
        <StatTile label="Hit rate" value={formatPercent(strategy.hit_rate)} />
        <StatTile label="Push rate" value={formatPercent(strategy.push_rate)} />
        <StatTile label="Profit" value={formatMetric(strategy.profit_units, 2)} detail="Units" />
      </div>

      <div className="table-shell">
        <table>
          <thead>
            <tr>
              <th>Edge bucket</th>
              <th>Bets</th>
              <th>Hit rate</th>
              <th>ROI</th>
              <th>Profit</th>
            </tr>
          </thead>
          <tbody>
            {edgeBuckets.length === 0 ? (
              <tr>
                <td colSpan={5}>No bets in this strategy yet.</td>
              </tr>
            ) : (
              edgeBuckets.map(([bucket, bucketSummary]) => (
                <tr key={bucket}>
                  <td>{bucket}</td>
                  <td>{bucketSummary.bet_count}</td>
                  <td>{formatPercent(bucketSummary.hit_rate)}</td>
                  <td>{formatPercent(bucketSummary.roi)}</td>
                  <td>{formatMetric(bucketSummary.profit_units, 2)}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </article>
  );
}

export function FoldDetailCard({
  fold,
  runId,
  provenanceItems,
  compareHref
}: {
  fold: FoldSummary | null;
  runId: number | null;
  provenanceItems?: ProvenanceItem[];
  compareHref?: string;
}) {
  if (!fold) {
    return (
      <article className="panel focus-panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Fold detail</p>
            <h2>Fold not available</h2>
          </div>
        </div>
        <p className="lead detail-copy">
          The selected walk-forward fold could not be resolved from this backtest run.
        </p>
      </article>
    );
  }

  return (
    <article className="panel focus-panel">
      <ProvenanceRibbon items={provenanceItems ?? []} title="Provenance" />

      <div className="section-heading">
        <div>
          <p className="eyebrow">Fold detail</p>
          <h2>
            Run {runId ? `#${runId}` : ""} | Fold {fold.fold_index}
          </h2>
        </div>
        <div className="pill-row">
          <span className="pill">{fold.selected_model.model_family}</span>
          <span className="pill">{fold.test_game_count} test games</span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile label="Train games" value={String(fold.train_game_count)} />
        <StatTile label="Test games" value={String(fold.test_game_count)} />
        <StatTile label="Validation MAE" value={formatMetric(fold.selected_model.validation_metric_value)} />
        <StatTile label="Test MAE" value={formatMetric(fold.selected_model.test_metric_value)} />
      </div>

      {compareHref ? (
        <div className="route-action-row">
          <a className="secondary-button inline-link-button" href={compareHref}>
            Compare with active opportunity
          </a>
        </div>
      ) : null}

      <div className="detail-section-grid">
        <section className="sub-panel">
          <p className="sub-panel-title">Selected model</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Family</span>
              <strong>{fold.selected_model.model_family}</strong>
            </div>
            <div className="detail-list-item">
              <span>Selected feature</span>
              <strong>{fold.selected_model.selected_feature ?? "fallback"}</strong>
            </div>
            <div className="detail-list-item">
              <span>Fallback strategy</span>
              <strong>{fold.selected_model.fallback_strategy ?? "primary fit"}</strong>
            </div>
          </div>
          {runId ? (
            <div className="pill-row fold-link-row">
              {fold.selected_model.model_training_run_id > 0 ? (
                <a
                  className="secondary-button inline-link-button"
                  href={routeHash({
                    name: "backtest-fold-model-run",
                    runId,
                    foldIndex: fold.fold_index,
                    modelRunId: fold.selected_model.model_training_run_id
                  })}
                >
                  Open training run
                </a>
              ) : null}
              <a
                className="secondary-button inline-link-button"
                href={routeHash({
                  name: "backtest-fold-evaluation",
                  runId,
                  foldIndex: fold.fold_index,
                  evaluationId: fold.selected_model.evaluation_snapshot_id
                })}
              >
                Open evaluation
              </a>
            </div>
          ) : null}
        </section>

        <section className="sub-panel">
          <p className="sub-panel-title">Prediction metrics</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Prediction count</span>
              <strong>{String(fold.prediction_metrics.prediction_count)}</strong>
            </div>
            <div className="detail-list-item">
              <span>MAE</span>
              <strong>{formatMetric(fold.prediction_metrics.mae)}</strong>
            </div>
            <div className="detail-list-item">
              <span>RMSE</span>
              <strong>{formatMetric(fold.prediction_metrics.rmse)}</strong>
            </div>
          </div>
        </section>
      </div>

      <section className="section-stack detail-section-stack">
        <div className="section-heading standalone">
          <div>
            <p className="eyebrow">Strategy outcomes</p>
            <h3>Per-fold thresholds</h3>
          </div>
        </div>
        <div className="strategy-grid">
          {Object.entries(fold.strategies).map(([label, strategy]) => (
            <StrategyCard key={label} label={label.replace("_", " ")} strategy={strategy} />
          ))}
        </div>
      </section>

      <section className="detail-section-grid">
        <section className="sub-panel">
          <p className="sub-panel-title">Train game ids</p>
          <p className="sub-panel-meta">{fold.train_game_ids.join(", ") || "n/a"}</p>
        </section>

        <section className="sub-panel">
          <p className="sub-panel-title">Test game ids</p>
          <p className="sub-panel-meta">{fold.test_game_ids.join(", ") || "n/a"}</p>
        </section>
      </section>
    </article>
  );
}

export function BacktestRunDetailCard({
  run,
  provenanceItems,
  provenanceData
}: {
  run: BacktestRun | null;
  provenanceItems?: ProvenanceItem[];
  provenanceData?: ProvenanceInspectorData;
}) {
  if (!run) {
    return (
      <article className="panel focus-panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Backtest run</p>
            <h2>Select a run</h2>
          </div>
        </div>
        <p className="lead detail-copy">
          Pick a backtest run from history to inspect the exact validation result, selected model
          mix, and fold-level strategy performance.
        </p>
      </article>
    );
  }

  const summary = run.payload;

  return (
    <article className="panel focus-panel">
      <ProvenanceRibbon items={provenanceItems ?? []} title="Provenance" />
      {provenanceData ? <ProvenanceInspector data={provenanceData} /> : null}

      <div className="section-heading">
        <div>
          <p className="eyebrow">Backtest run</p>
          <h2>{summary.strategy_name}</h2>
        </div>
        <div className="pill-row">
          <span className="pill">Run #{run.id}</span>
          <span className="pill">{summary.fold_count} folds</span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile label="Dataset games" value={String(summary.dataset_game_count)} />
        <StatTile label="Dataset rows" value={String(summary.dataset_row_count)} />
        <StatTile label="Prediction MAE" value={formatMetric(summary.prediction_metrics.mae)} />
        <StatTile label="Prediction RMSE" value={formatMetric(summary.prediction_metrics.rmse)} />
      </div>

      <div className="detail-list">
        <div className="detail-list-item">
          <span>Selection policy</span>
          <strong>{summary.selection_policy_name}</strong>
        </div>
        <div className="detail-list-item">
          <span>Target task</span>
          <strong>{summary.target_task}</strong>
        </div>
        <div className="detail-list-item">
          <span>Minimum train games</span>
          <strong>{String(summary.minimum_train_games)}</strong>
        </div>
        <div className="detail-list-item">
          <span>Test window games</span>
          <strong>{String(summary.test_window_games)}</strong>
        </div>
      </div>

      <div className="mini-grid family-grid">
        {Object.entries(summary.selected_model_family_counts).map(([family, count]) => (
          <div className="sub-panel" key={family}>
            <p className="sub-panel-title">{family}</p>
            <p className="sub-panel-stat">{count}</p>
            <p className="sub-panel-meta">fold selections</p>
          </div>
        ))}
      </div>
    </article>
  );
}
