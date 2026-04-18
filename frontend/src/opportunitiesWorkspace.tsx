import type { ReactNode } from "react";

import { StatTile } from "./appSharedComponents";
import type { OpportunityHistoryResponse, OpportunityRecord } from "./appTypes";
import { formatCompactNumber, formatLabel, formatTimestamp } from "./appUtils";

function OpportunityListItem({
  opportunity,
  active,
  onSelect
}: {
  opportunity: OpportunityRecord;
  active: boolean;
  onSelect: (opportunityId: number) => void;
}) {
  return (
    <button
      className={`opportunity-list-item${active ? " opportunity-list-item-active" : ""}`}
      onClick={() => onSelect(opportunity.id)}
      type="button"
    >
      <div className="section-heading compact-heading">
        <div>
          <p className="eyebrow">{formatLabel(opportunity.source_kind)}</p>
          <h3>
            {opportunity.team_code} vs {opportunity.opponent_code}
          </h3>
        </div>
        <div className="pill-row">
          <span className="pill">{formatLabel(opportunity.status)}</span>
        </div>
      </div>

      <div className="opportunity-item-grid">
        <div>
          <p className="sub-panel-title">Prediction</p>
          <p className="sub-panel-stat">{formatCompactNumber(opportunity.prediction_value, 3)}</p>
        </div>
        <div>
          <p className="sub-panel-title">Signal</p>
          <p className="sub-panel-stat">{formatCompactNumber(opportunity.signal_strength, 2)}</p>
        </div>
        <div>
          <p className="sub-panel-title">Evidence</p>
          <p className="sub-panel-stat">{formatLabel(opportunity.evidence_rating)}</p>
        </div>
      </div>

      <p className="sub-panel-meta">
        {formatTimestamp(opportunity.updated_at ?? opportunity.created_at)} | {opportunity.season_label}
      </p>
    </button>
  );
}

export function OpportunitiesWorkspace({
  opportunityHistory,
  opportunities,
  activeOpportunityId,
  showQueueDetail,
  detailContent,
  onSelectOpportunity
}: {
  opportunityHistory: OpportunityHistoryResponse;
  opportunities: OpportunityRecord[];
  activeOpportunityId: number | null;
  showQueueDetail: boolean;
  detailContent: ReactNode;
  onSelectOpportunity: (opportunityId: number) => void;
}) {
  const opportunityOverview = opportunityHistory.model_opportunity_history.overview;

  return (
    <>
      <section className="stat-grid">
        <StatTile label="Opportunity count" value={String(opportunityOverview.opportunity_count)} />
        <StatTile
          label="Review queue"
          value={String(opportunityOverview.status_counts.review_manually ?? 0)}
        />
        <StatTile
          label="Candidate signals"
          value={String(opportunityOverview.status_counts.candidate_signal ?? 0)}
        />
        <StatTile
          label="Latest update"
          value={formatTimestamp(opportunityOverview.latest_opportunity?.updated_at ?? null)}
        />
      </section>

      {showQueueDetail ? (
        <section className="dashboard-grid">
          <article className="panel">
            <div className="section-heading">
              <div>
                <p className="eyebrow">Recent opportunities</p>
                <h2>Analyst queue</h2>
              </div>
              <div className="pill-row">
                <span className="pill">
                  Historical {String(opportunityOverview.source_kind_counts.historical_game ?? 0)}
                </span>
                <span className="pill">
                  Future {String(opportunityOverview.source_kind_counts.future_scenario ?? 0)}
                </span>
              </div>
            </div>

            <div className="list-stack">
              {opportunities.length === 0 ? (
                <p className="sub-panel-meta">
                  No opportunities are available yet. Use the materialize action to build a fresh
                  analyst queue from the current scoring flow.
                </p>
              ) : (
                opportunities.map((opportunity) => (
                  <OpportunityListItem
                    active={activeOpportunityId === opportunity.id}
                    key={opportunity.id}
                    onSelect={onSelectOpportunity}
                    opportunity={opportunity}
                  />
                ))
              )}
            </div>
          </article>

          {detailContent}
        </section>
      ) : (
        detailContent
      )}

      <section className="section-stack">
        <div className="section-heading standalone">
          <div>
            <p className="eyebrow">History rollup</p>
            <h2>Status and evidence mix</h2>
          </div>
        </div>

        <div className="mini-grid family-grid">
          {Object.entries(opportunityOverview.status_counts).map(([status, count]) => (
            <div className="sub-panel" key={status}>
              <p className="sub-panel-title">{formatLabel(status)}</p>
              <p className="sub-panel-stat">{count}</p>
              <p className="sub-panel-meta">status count</p>
            </div>
          ))}
          {Object.entries(opportunityOverview.evidence_rating_counts ?? {}).map(([rating, count]) => (
            <div className="sub-panel" key={rating}>
              <p className="sub-panel-title">{formatLabel(rating)}</p>
              <p className="sub-panel-stat">{count}</p>
              <p className="sub-panel-meta">evidence count</p>
            </div>
          ))}
        </div>
      </section>
    </>
  );
}
