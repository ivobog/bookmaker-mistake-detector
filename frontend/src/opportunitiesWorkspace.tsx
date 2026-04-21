import type { ReactNode } from "react";

import { StatTile } from "./appSharedComponents";
import type {
  OpportunityHistoryResponse,
  OpportunityListResponse,
  OpportunityRecord
} from "./appTypes";
import { formatCompactNumber, formatLabel, formatTimestamp } from "./appUtils";
import {
  getEvidenceLabel,
  getOpportunityMatchupLabel,
  getSourceKindLabel
} from "../../shared/frontend/domain";

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
      data-testid={`opportunity-row-${opportunity.id}`}
      onClick={() => onSelect(opportunity.id)}
      type="button"
    >
      <div className="section-heading compact-heading">
        <div>
          <p className="eyebrow">{getSourceKindLabel(opportunity.source_kind)}</p>
          <h3>{getOpportunityMatchupLabel(opportunity)}</h3>
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
          <p className="sub-panel-stat">{getEvidenceLabel(opportunity.evidence_rating)}</p>
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
  opportunityList,
  opportunities,
  activeOpportunityId,
  showQueueDetail,
  detailContent,
  onSelectOpportunity
}: {
  opportunityHistory: OpportunityHistoryResponse;
  opportunityList: OpportunityListResponse;
  opportunities: OpportunityRecord[];
  activeOpportunityId: number | null;
  showQueueDetail: boolean;
  detailContent: ReactNode;
  onSelectOpportunity: (opportunityId: number) => void;
}) {
  const opportunityOverview = opportunityHistory.model_opportunity_history.overview;
  const queueScope = opportunityList.queue_scope;
  const queueScopeLabel =
    opportunityList.queue_scope_label ??
    (opportunityList.queue_scope_is_scoped ? "Scoped queue" : "Operator-wide queue");
  const queueNote = opportunityList.queue_scope_is_scoped
    ? "This queue was materialized from a scoped run and may not represent the global analyst queue."
    : "Queue scope: operator-wide.";

  return (
    <>
      <section className="stat-grid" data-testid="opportunities-page">
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
                <span
                  className={`pill${opportunityList.queue_scope_is_scoped ? " pill-warning" : ""}`}
                  data-testid="opportunities-queue-scope-badge"
                >
                  {opportunityList.queue_scope_is_scoped ? "Scoped" : "Global"}
                </span>
                <span className="pill">
                  Historical {String(opportunityOverview.source_kind_counts.historical_game ?? 0)}
                </span>
                <span className="pill">
                  Future {String(opportunityOverview.source_kind_counts.future_scenario ?? 0)}
                </span>
              </div>
            </div>

            <div
              data-testid="opportunities-queue-scope-panel"
              className={`sub-panel queue-scope-panel${
                opportunityList.queue_scope_is_scoped ? " queue-scope-panel-warning" : ""
              }`}
            >
              <div className="section-heading compact-heading">
                <div>
                  <p className="sub-panel-title">Queue scope</p>
                  <p className="sub-panel-stat" data-testid="opportunities-queue-scope-label">
                    {queueScopeLabel}
                  </p>
                </div>
                <div className="pill-row">
                  {opportunityList.queue_batch_id ? (
                    <span className="pill" data-testid="opportunities-queue-batch-id">
                      Batch {opportunityList.queue_batch_id.slice(0, 8)}
                    </span>
                  ) : null}
                  {opportunityList.queue_materialized_at ? (
                    <span className="pill" data-testid="opportunities-queue-materialized-at">
                      {formatTimestamp(opportunityList.queue_materialized_at)}
                    </span>
                  ) : null}
                </div>
              </div>
              <p className="sub-panel-meta">{queueNote}</p>
              {opportunityList.queue_scope_is_scoped ? (
                <div className="detail-list compact-list">
                  <div className="detail-list-item">
                    <span>Team scope</span>
                    <strong>{queueScope.team_code ?? "n/a"}</strong>
                  </div>
                  <div className="detail-list-item">
                    <span>Season scope</span>
                    <strong>{queueScope.season_label ?? "n/a"}</strong>
                  </div>
                  <div className="detail-list-item">
                    <span>Scope source</span>
                    <strong>{formatLabel(queueScope.source)}</strong>
                  </div>
                </div>
              ) : null}
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
