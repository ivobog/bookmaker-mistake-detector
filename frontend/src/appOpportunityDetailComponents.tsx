import { ProvenanceInspector, ProvenanceRibbon, StatTile } from "./appSharedComponents";
import type { OpportunityRecord, ProvenanceInspectorData, ProvenanceItem } from "./appTypes";
import { asArray, asRecord, formatCompactNumber, formatLabel, readNested } from "./appUtils";

export function OpportunityDetailCard({
  opportunity,
  onSelectComparable,
  provenanceItems,
  provenanceData,
  compareHref
}: {
  opportunity: OpportunityRecord | null;
  onSelectComparable?: (comparableIndex: number) => void;
  provenanceItems?: ProvenanceItem[];
  provenanceData?: ProvenanceInspectorData;
  compareHref?: string;
}) {
  if (!opportunity) {
    return (
      <article className="panel focus-panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Opportunity detail</p>
            <h2>Select an opportunity</h2>
          </div>
        </div>
        <p className="lead detail-copy">
          Pick a row from the queue to inspect its evidence strength, recommendation state, market
          context, and model metadata.
        </p>
      </article>
    );
  }

  const prediction = asRecord(readNested(opportunity.payload, "prediction"));
  const evidence = asRecord(readNested(prediction, "evidence"));
  const strength = asRecord(readNested(evidence, "strength"));
  const recommendation = asRecord(readNested(evidence, "recommendation"));
  const evidenceSummary = asRecord(readNested(evidence, "summary"));
  const marketContext = asRecord(readNested(prediction, "market_context"));
  const modelContext = asRecord(readNested(prediction, "model"));
  const pattern = asRecord(readNested(evidence, "pattern"));
  const selectedPattern = asRecord(readNested(pattern, "selected_pattern"));
  const comparables = asRecord(readNested(evidence, "comparables"));
  const comparablesSummary = asRecord(readNested(comparables, "summary"));
  const comparableCases = asArray(readNested(comparables, "cases"));
  const benchmarkContext = asRecord(readNested(evidence, "benchmark_context"));
  const benchmarkRankings = asArray(readNested(benchmarkContext, "benchmark_rankings"));
  const policyProfile = asRecord(readNested(recommendation, "policy_profile"));
  const thresholds = asRecord(readNested(policyProfile, "thresholds"));
  const rationale = asArray(readNested(recommendation, "rationale"));
  const blockingFactors = asArray(readNested(recommendation, "blocking_factors"));
  const nextSteps = asArray(readNested(recommendation, "next_steps"));
  const activeSelection = asRecord(readNested(opportunity.payload, "active_selection"));
  const activeEvaluationSnapshot = asRecord(readNested(opportunity.payload, "active_evaluation_snapshot"));
  const scenario = asRecord(readNested(opportunity.payload, "scenario"));

  return (
    <article className="panel focus-panel">
      <ProvenanceRibbon items={provenanceItems ?? []} title="Provenance" />
      {provenanceData ? <ProvenanceInspector data={provenanceData} /> : null}

      <div className="section-heading">
        <div>
          <p className="eyebrow">Opportunity detail</p>
          <h2>
            {opportunity.team_code} vs {opportunity.opponent_code}
          </h2>
        </div>
        <div className="pill-row">
          <span className="pill">{formatLabel(opportunity.status)}</span>
          <span className="pill">{formatLabel(opportunity.recommendation_status)}</span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile label="Prediction value" value={formatCompactNumber(opportunity.prediction_value, 3)} />
        <StatTile label="Signal strength" value={formatCompactNumber(opportunity.signal_strength, 2)} />
        <StatTile label="Evidence rating" value={formatLabel(opportunity.evidence_rating)} />
        <StatTile label="Game date" value={opportunity.game_date} detail={opportunity.season_label} />
      </div>

      {compareHref ? (
        <div className="route-action-row">
          <a className="secondary-button inline-link-button" href={compareHref}>
            Compare with active backtest fold
          </a>
        </div>
      ) : null}

      <div className="opportunity-detail-grid">
        <div className="sub-panel">
          <p className="sub-panel-title">Recommendation</p>
          <p className="sub-panel-stat">
            {String(readNested(recommendation, "headline") ?? formatLabel(opportunity.recommendation_status))}
          </p>
          <p className="sub-panel-meta">
            {String(readNested(recommendation, "recommended_action") ?? "Inspect manually")}
          </p>
        </div>

        <div className="sub-panel">
          <p className="sub-panel-title">Evidence strength</p>
          <p className="sub-panel-stat">
            {formatLabel(String(readNested(strength, "rating") ?? opportunity.evidence_rating ?? "n/a"))}
          </p>
          <p className="sub-panel-meta">
            Overall score {formatCompactNumber(Number(readNested(strength, "overall_score") ?? null), 3)}
          </p>
        </div>

        <div className="sub-panel">
          <p className="sub-panel-title">Pattern support</p>
          <p className="sub-panel-stat">{String(readNested(evidenceSummary, "pattern_sample_size") ?? "n/a")}</p>
          <p className="sub-panel-meta">
            Comparables {String(readNested(evidenceSummary, "comparable_count") ?? "n/a")}
          </p>
        </div>

        <div className="sub-panel">
          <p className="sub-panel-title">Market context</p>
          <p className="sub-panel-stat">
            Spread {String(readNested(marketContext, "home_spread_line") ?? "n/a")}
          </p>
          <p className="sub-panel-meta">
            Total {String(readNested(marketContext, "total_line") ?? "n/a")}
          </p>
        </div>
      </div>

      <div className="detail-list">
        <div className="detail-list-item">
          <span>Policy</span>
          <strong>{opportunity.policy_name}</strong>
        </div>
        <div className="detail-list-item">
          <span>Model family</span>
          <strong>{String(readNested(modelContext, "model_family") ?? "n/a")}</strong>
        </div>
        <div className="detail-list-item">
          <span>Selected feature</span>
          <strong>{String(readNested(modelContext, "selected_feature") ?? "n/a")}</strong>
        </div>
        <div className="detail-list-item">
          <span>Scenario key</span>
          <strong>{opportunity.scenario_key ?? "historical_game"}</strong>
        </div>
      </div>

      <div className="detail-section-grid">
        <section className="sub-panel">
          <p className="sub-panel-title">Pattern evidence</p>
          <p className="sub-panel-stat">
            {String(readNested(selectedPattern, "pattern_key") ?? readNested(evidenceSummary, "pattern_key") ?? "n/a")}
          </p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Sample size</span>
              <strong>{String(readNested(selectedPattern, "sample_size") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Signal strength</span>
              <strong>{String(readNested(selectedPattern, "signal_strength") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Target mean</span>
              <strong>{String(readNested(selectedPattern, "target_mean") ?? "n/a")}</strong>
            </div>
          </div>
        </section>

        <section className="sub-panel">
          <p className="sub-panel-title">Benchmark context</p>
          <p className="sub-panel-stat">
            {String(readNested(evidenceSummary, "best_benchmark", "baseline_name") ?? "n/a")}
          </p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Primary metric</span>
              <strong>{String(readNested(evidenceSummary, "best_benchmark", "primary_metric") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Validation</span>
              <strong>{String(readNested(evidenceSummary, "best_benchmark", "validation_primary_metric") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Test</span>
              <strong>{String(readNested(evidenceSummary, "best_benchmark", "test_primary_metric") ?? "n/a")}</strong>
            </div>
          </div>
        </section>
      </div>

      <section className="section-stack detail-section-stack">
        <div className="section-heading standalone">
          <div>
            <p className="eyebrow">Comparable cases</p>
            <h3>Matched history</h3>
          </div>
          <div className="pill-row">
            <span className="pill">Count {String(readNested(comparablesSummary, "comparable_count") ?? comparableCases.length)}</span>
            <span className="pill">
              Top similarity {String(readNested(comparablesSummary, "top_similarity_score") ?? "n/a")}
            </span>
          </div>
        </div>

        <div className="list-stack">
          {comparableCases.length === 0 ? (
            <p className="sub-panel-meta">No comparable cases were attached to this opportunity.</p>
          ) : (
            comparableCases.slice(0, 5).map((entry, index) => {
              const comparable = asRecord(entry);
              const matchedConditions = asRecord(readNested(comparable, "matched_conditions"));
              return (
                <button
                  className={`sub-panel comparable-card${onSelectComparable ? " comparable-card-actionable" : ""}`}
                  key={String(readNested(comparable, "canonical_game_id") ?? index)}
                  onClick={onSelectComparable ? () => onSelectComparable(index) : undefined}
                  type={onSelectComparable ? "button" : undefined}
                >
                  <div className="section-heading compact-heading">
                    <div>
                      <p className="sub-panel-title">
                        Game {String(readNested(comparable, "canonical_game_id") ?? "n/a")}
                      </p>
                      <p className="sub-panel-stat">
                        {String(readNested(comparable, "team_code") ?? "n/a")} vs{" "}
                        {String(readNested(comparable, "opponent_code") ?? "n/a")}
                      </p>
                    </div>
                    <div className="pill-row">
                      <span className="pill">
                        Similarity {String(readNested(comparable, "similarity_score") ?? "n/a")}
                      </span>
                    </div>
                  </div>
                  <div className="detail-list compact-list">
                    <div className="detail-list-item">
                      <span>Prediction target</span>
                      <strong>{String(readNested(comparable, "target_value") ?? "n/a")}</strong>
                    </div>
                    <div className="detail-list-item">
                      <span>Venue</span>
                      <strong>{String(readNested(matchedConditions, "venue") ?? "n/a")}</strong>
                    </div>
                    <div className="detail-list-item">
                      <span>Rest bucket</span>
                      <strong>{String(readNested(matchedConditions, "days_rest_bucket") ?? "n/a")}</strong>
                    </div>
                  </div>
                </button>
              );
            })
          )}
        </div>
      </section>

      <section className="section-stack detail-section-stack">
        <div className="section-heading standalone">
          <div>
            <p className="eyebrow">Recommendation policy</p>
            <h3>Decision framing</h3>
          </div>
        </div>

        <div className="detail-section-grid">
          <section className="sub-panel">
            <p className="sub-panel-title">Rationale</p>
            <div className="chip-list">
              {rationale.length === 0 ? (
                <span className="pill">No rationale provided</span>
              ) : (
                rationale.map((item, index) => (
                  <span className="pill" key={`${String(item)}-${index}`}>
                    {String(item)}
                  </span>
                ))
              )}
            </div>
          </section>

          <section className="sub-panel">
            <p className="sub-panel-title">Blocking factors</p>
            <div className="chip-list">
              {blockingFactors.length === 0 ? (
                <span className="pill">No blockers</span>
              ) : (
                blockingFactors.map((item, index) => (
                  <span className="pill" key={`${String(item)}-${index}`}>
                    {formatLabel(String(item))}
                  </span>
                ))
              )}
            </div>
          </section>
        </div>

        <div className="detail-section-grid">
          <section className="sub-panel">
            <p className="sub-panel-title">Next steps</p>
            <ul className="detail-bullets">
              {nextSteps.length === 0 ? (
                <li>No next steps provided.</li>
              ) : (
                nextSteps.map((item, index) => <li key={`${String(item)}-${index}`}>{String(item)}</li>)
              )}
            </ul>
          </section>

          <section className="sub-panel">
            <p className="sub-panel-title">Policy thresholds</p>
            <div className="detail-list compact-list">
              {Object.entries(thresholds ?? {}).map(([key, value]) => (
                <div className="detail-list-item" key={key}>
                  <span>{formatLabel(key)}</span>
                  <strong>{String(value)}</strong>
                </div>
              ))}
            </div>
          </section>
        </div>
      </section>

      <section className="section-stack detail-section-stack">
        <div className="section-heading standalone">
          <div>
            <p className="eyebrow">Model provenance</p>
            <h3>Selection and evaluation snapshot</h3>
          </div>
        </div>

        <div className="detail-section-grid">
          <section className="sub-panel">
            <p className="sub-panel-title">Active selection</p>
            <div className="detail-list compact-list">
              <div className="detail-list-item">
                <span>Family</span>
                <strong>{String(readNested(activeSelection, "model_family") ?? "n/a")}</strong>
              </div>
              <div className="detail-list-item">
                <span>Policy</span>
                <strong>{String(readNested(activeSelection, "selection_policy_name") ?? "n/a")}</strong>
              </div>
              <div className="detail-list-item">
                <span>Activated</span>
                <strong>{String(readNested(activeSelection, "activated_at") ?? "n/a")}</strong>
              </div>
            </div>
          </section>

          <section className="sub-panel">
            <p className="sub-panel-title">Evaluation snapshot</p>
            <div className="detail-list compact-list">
              <div className="detail-list-item">
                <span>Primary metric</span>
                <strong>{String(readNested(activeEvaluationSnapshot, "primary_metric") ?? "n/a")}</strong>
              </div>
              <div className="detail-list-item">
                <span>Validation value</span>
                <strong>{String(readNested(activeEvaluationSnapshot, "validation_primary_metric") ?? "n/a")}</strong>
              </div>
              <div className="detail-list-item">
                <span>Test value</span>
                <strong>{String(readNested(activeEvaluationSnapshot, "test_primary_metric") ?? "n/a")}</strong>
              </div>
            </div>
          </section>
        </div>

        {scenario ? (
          <section className="sub-panel">
            <p className="sub-panel-title">Scenario context</p>
            <div className="detail-list compact-list">
              <div className="detail-list-item">
                <span>Home team</span>
                <strong>{String(readNested(scenario, "home_team_code") ?? "n/a")}</strong>
              </div>
              <div className="detail-list-item">
                <span>Away team</span>
                <strong>{String(readNested(scenario, "away_team_code") ?? "n/a")}</strong>
              </div>
              <div className="detail-list-item">
                <span>Scenario date</span>
                <strong>{String(readNested(scenario, "game_date") ?? "n/a")}</strong>
              </div>
            </div>
          </section>
        ) : null}
      </section>

      {benchmarkRankings.length > 0 ? (
        <section className="section-stack detail-section-stack">
          <div className="section-heading standalone">
            <div>
              <p className="eyebrow">Benchmark rankings</p>
              <h3>Reference baselines</h3>
            </div>
          </div>
          <div className="list-stack">
            {benchmarkRankings.slice(0, 3).map((entry, index) => {
              const benchmark = asRecord(entry);
              return (
                <article className="sub-panel" key={String(readNested(benchmark, "baseline_name") ?? index)}>
                  <div className="section-heading compact-heading">
                    <div>
                      <p className="sub-panel-title">
                        {String(readNested(benchmark, "baseline_name") ?? "n/a")}
                      </p>
                      <p className="sub-panel-stat">
                        {String(readNested(benchmark, "primary_metric") ?? "n/a")}
                      </p>
                    </div>
                  </div>
                  <div className="detail-list compact-list">
                    <div className="detail-list-item">
                      <span>Validation metric</span>
                      <strong>{String(readNested(benchmark, "validation_primary_metric") ?? "n/a")}</strong>
                    </div>
                    <div className="detail-list-item">
                      <span>Test metric</span>
                      <strong>{String(readNested(benchmark, "test_primary_metric") ?? "n/a")}</strong>
                    </div>
                    <div className="detail-list-item">
                      <span>Test predictions</span>
                      <strong>{String(readNested(benchmark, "test_prediction_count") ?? "n/a")}</strong>
                    </div>
                  </div>
                </article>
              );
            })}
          </div>
        </section>
      ) : null}
    </article>
  );
}

export function ComparableCaseDetail({
  opportunity,
  comparableIndex,
  provenanceItems,
  provenanceData
}: {
  opportunity: OpportunityRecord | null;
  comparableIndex: number;
  provenanceItems?: ProvenanceItem[];
  provenanceData?: ProvenanceInspectorData;
}) {
  const evidence = asRecord(readNested(readNested(opportunity?.payload, "prediction"), "evidence"));
  const comparables = asRecord(readNested(evidence, "comparables"));
  const comparableCases = asArray(readNested(comparables, "cases"));
  const comparable = asRecord(comparableCases[comparableIndex]);
  const matchedConditions = asRecord(readNested(comparable, "matched_conditions"));
  const anchorCase = asRecord(readNested(comparables, "anchor_case"));
  const comparableSummary = asRecord(readNested(comparables, "summary"));

  if (!opportunity || !comparable) {
    return (
      <article className="panel focus-panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">Comparable case</p>
            <h2>Comparable not available</h2>
          </div>
        </div>
        <p className="lead detail-copy">
          This comparable case could not be resolved from the current opportunity payload.
        </p>
      </article>
    );
  }

  return (
    <article className="panel focus-panel">
      <ProvenanceRibbon items={provenanceItems ?? []} title="Provenance" />
      {provenanceData ? <ProvenanceInspector data={provenanceData} /> : null}

      <div className="section-heading">
        <div>
          <p className="eyebrow">Comparable case</p>
          <h2>
            Game {String(readNested(comparable, "canonical_game_id") ?? "n/a")} |{" "}
            {String(readNested(comparable, "team_code") ?? "n/a")} vs{" "}
            {String(readNested(comparable, "opponent_code") ?? "n/a")}
          </h2>
        </div>
        <div className="pill-row">
          <span className="pill">
            Similarity {String(readNested(comparable, "similarity_score") ?? "n/a")}
          </span>
          <span className="pill">
            Target {String(readNested(comparable, "target_value") ?? "n/a")}
          </span>
        </div>
      </div>

      <div className="mini-grid">
        <StatTile
          label="Comparable game"
          value={String(readNested(comparable, "canonical_game_id") ?? "n/a")}
          detail={String(readNested(comparable, "game_date") ?? "n/a")}
        />
        <StatTile
          label="Prediction target"
          value={String(readNested(comparable, "target_value") ?? "n/a")}
          detail={String(readNested(comparable, "target_column") ?? "n/a")}
        />
        <StatTile label="Venue bucket" value={String(readNested(matchedConditions, "venue") ?? "n/a")} />
        <StatTile
          label="Rest bucket"
          value={String(readNested(matchedConditions, "days_rest_bucket") ?? "n/a")}
        />
      </div>

      <div className="detail-section-grid">
        <section className="sub-panel">
          <p className="sub-panel-title">Anchor context</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Anchor game</span>
              <strong>{String(readNested(anchorCase, "canonical_game_id") ?? opportunity.canonical_game_id ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Anchor team</span>
              <strong>{String(readNested(anchorCase, "team_code") ?? opportunity.team_code)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Pattern key</span>
              <strong>{String(readNested(comparables, "pattern_key") ?? "n/a")}</strong>
            </div>
          </div>
        </section>

        <section className="sub-panel">
          <p className="sub-panel-title">Comparable summary</p>
          <div className="detail-list compact-list">
            <div className="detail-list-item">
              <span>Comparable count</span>
              <strong>{String(readNested(comparableSummary, "comparable_count") ?? comparableCases.length)}</strong>
            </div>
            <div className="detail-list-item">
              <span>Top similarity</span>
              <strong>{String(readNested(comparableSummary, "top_similarity_score") ?? "n/a")}</strong>
            </div>
            <div className="detail-list-item">
              <span>Average similarity</span>
              <strong>{String(readNested(comparableSummary, "average_similarity_score") ?? "n/a")}</strong>
            </div>
          </div>
        </section>
      </div>

      <section className="section-stack detail-section-stack">
        <div className="section-heading standalone">
          <div>
            <p className="eyebrow">Matched conditions</p>
            <h3>Why this case matched</h3>
          </div>
        </div>

        <div className="detail-list">
          {Object.entries(matchedConditions ?? {}).map(([key, value]) => (
            <div className="detail-list-item" key={key}>
              <span>{formatLabel(key)}</span>
              <strong>{String(value)}</strong>
            </div>
          ))}
        </div>
      </section>
    </article>
  );
}
