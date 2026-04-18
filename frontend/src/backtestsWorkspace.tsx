import type { ReactNode } from "react";

import { StatTile } from "./appSharedComponents";
import type { AppRoute, BacktestHistoryResponse, BacktestRun, FoldSummary, StrategySummary } from "./appTypes";
import { formatMetric, formatPercent, formatTimestamp } from "./appUtils";

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

function FoldCard({ fold }: { fold: FoldSummary }) {
  return (
    <article className="panel fold-card">
      <div className="section-heading">
        <div>
          <p className="eyebrow">Fold {fold.fold_index}</p>
          <h3>{fold.selected_model.model_family}</h3>
        </div>
        <div className="pill-row">
          <span className="pill">Train {fold.train_game_count} games</span>
          <span className="pill">Test {fold.test_game_count} games</span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile label="Validation MAE" value={formatMetric(fold.selected_model.validation_metric_value)} />
        <StatTile label="Test MAE" value={formatMetric(fold.selected_model.test_metric_value)} />
        <StatTile label="Prediction MAE" value={formatMetric(fold.prediction_metrics.mae)} />
        <StatTile label="Prediction RMSE" value={formatMetric(fold.prediction_metrics.rmse)} />
      </div>

      <div className="detail-list compact-list">
        <div className="detail-list-item">
          <span>Selected feature</span>
          <strong>{fold.selected_model.selected_feature ?? "fallback"}</strong>
        </div>
        <div className="detail-list-item">
          <span>Fallback strategy</span>
          <strong>{fold.selected_model.fallback_strategy ?? "primary fit"}</strong>
        </div>
        <div className="detail-list-item">
          <span>Training run</span>
          <strong>#{fold.selected_model.model_training_run_id}</strong>
        </div>
      </div>
    </article>
  );
}

function BacktestStrategySections({
  activeRun,
  onNavigate
}: {
  activeRun: BacktestRun;
  onNavigate: (route: AppRoute) => void;
}) {
  const summary = activeRun.payload;

  return (
    <>
      <section className="section-stack">
        <div className="section-heading standalone">
          <div>
            <p className="eyebrow">Strategy results</p>
            <h2>Threshold simulation</h2>
          </div>
        </div>

        <div className="strategy-grid">
          {Object.entries(summary.strategy_results).map(([label, strategy]) => (
            <StrategyCard key={label} label={label.replace("_", " ")} strategy={strategy} />
          ))}
        </div>
      </section>

      <section className="section-stack">
        <div className="section-heading standalone">
          <div>
            <p className="eyebrow">Walk-forward folds</p>
            <h2>Chronological validation</h2>
          </div>
        </div>

        <div className="fold-grid">
          {summary.folds.map((fold) => (
            <button
              className="unstyled-card-button"
              key={fold.fold_index}
              onClick={() =>
                onNavigate({
                  name: "backtest-fold",
                  runId: activeRun.id,
                  foldIndex: fold.fold_index
                })
              }
              type="button"
            >
              <FoldCard fold={fold} />
            </button>
          ))}
        </div>
      </section>
    </>
  );
}

export function BacktestsWorkspace({
  history,
  route,
  activeRun,
  detailContent,
  onNavigate
}: {
  history: BacktestHistoryResponse;
  route: AppRoute;
  activeRun: BacktestRun | null;
  detailContent?: ReactNode;
  onNavigate: (route: AppRoute) => void;
}) {
  const overview = history.model_backtest_history.overview;
  const summary = activeRun?.payload ?? null;
  const showStrategySections =
    activeRun !== null && summary !== null && (route.name === "backtests" || route.name === "backtest-run");

  return (
    <>
      <section className="stat-grid">
        <StatTile label="Backtest runs" value={String(overview.run_count)} />
        <StatTile
          label="Latest task"
          value={history.filters.target_task}
          detail={activeRun?.selection_policy_name ?? "no active run"}
        />
        <StatTile
          label="Best candidate ROI"
          value={formatPercent(
            overview.best_candidate_threshold_run?.payload.strategy_results.candidate_threshold.roi ?? null
          )}
        />
        <StatTile label="Latest completed" value={formatTimestamp(overview.latest_run?.completed_at ?? null)} />
      </section>

      {route.name === "backtests" && summary ? (
        <section className="dashboard-grid">
          <article className="panel focus-panel">
            <div className="section-heading">
              <div>
                <p className="eyebrow">Active run</p>
                <h2>{summary.strategy_name}</h2>
              </div>
              <div className="pill-row">
                <span className="pill">Run #{activeRun?.id}</span>
                <span className="pill">{summary.fold_count} folds</span>
              </div>
            </div>

            <div className="mini-grid">
              <StatTile label="Dataset games" value={String(summary.dataset_game_count)} />
              <StatTile label="Dataset rows" value={String(summary.dataset_row_count)} />
              <StatTile label="Prediction MAE" value={formatMetric(summary.prediction_metrics.mae)} />
              <StatTile label="Prediction RMSE" value={formatMetric(summary.prediction_metrics.rmse)} />
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

          <article className="panel">
            <div className="section-heading">
              <div>
                <p className="eyebrow">Recent runs</p>
                <h2>History</h2>
              </div>
            </div>

            <div className="table-shell">
              <table>
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Completed</th>
                    <th>Folds</th>
                    <th>Candidate ROI</th>
                  </tr>
                </thead>
                <tbody>
                  {history.model_backtest_history.recent_runs.map((run) => (
                    <tr
                      className={activeRun?.id === run.id ? "row-active" : undefined}
                      key={run.id}
                      onClick={() => onNavigate({ name: "backtest-run", runId: run.id })}
                    >
                      <td>#{run.id}</td>
                      <td>{formatTimestamp(run.completed_at)}</td>
                      <td>{run.fold_count}</td>
                      <td>{formatPercent(run.payload.strategy_results.candidate_threshold.roi)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </article>
        </section>
      ) : null}

      {detailContent}
      {showStrategySections ? <BacktestStrategySections activeRun={activeRun} onNavigate={onNavigate} /> : null}
    </>
  );
}
