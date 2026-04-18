from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class JsonPromotionCandidate:
    table_name: str
    column_name: str
    json_path: str
    candidate_column_name: str
    priority: str
    current_action: str
    promotion_trigger: str
    rationale: str


POSTGRES_JSON_PROMOTION_CANDIDATES: tuple[JsonPromotionCandidate, ...] = (
    JsonPromotionCandidate(
        table_name="model_backtest_run",
        column_name="payload_json",
        json_path="strategy_results.candidate_threshold.roi",
        candidate_column_name="candidate_roi",
        priority="high",
        current_action="defer",
        promotion_trigger=(
            "Promote when operator-facing dashboards or release gates need SQL-side ordering or filtering "
            "by best backtest ROI."
        ),
        rationale=(
            "Current history views compute ROI from payload after load, so no SQL filter depends on it yet. "
            "This is the strongest future candidate because it is a headline backtest outcome."
        ),
    ),
    JsonPromotionCandidate(
        table_name="model_backtest_run",
        column_name="payload_json",
        json_path="strategy_results.candidate_threshold.profit_units",
        candidate_column_name="candidate_profit_units",
        priority="high",
        current_action="defer",
        promotion_trigger=(
            "Promote alongside ROI if operators need SQL-side ranking, thresholds, or trend charts on "
            "profit performance."
        ),
        rationale=(
            "Profit units is used in history summaries, but only after loading payload JSON into Python."
        ),
    ),
    JsonPromotionCandidate(
        table_name="model_backtest_run",
        column_name="payload_json",
        json_path="strategy_results.candidate_threshold.hit_rate",
        candidate_column_name="candidate_hit_rate",
        priority="medium",
        current_action="defer",
        promotion_trigger=(
            "Promote if operator workflows require direct comparison or filtering on hit rate without "
            "loading full payloads."
        ),
        rationale=(
            "Useful as a companion metric, but lower priority than ROI and profit units for current review flows."
        ),
    ),
    JsonPromotionCandidate(
        table_name="model_scoring_run",
        column_name="payload_json",
        json_path="prediction_summary.top_signal_strength",
        candidate_column_name="top_signal_strength",
        priority="medium",
        current_action="defer",
        promotion_trigger=(
            "Promote when scoring-run queues or dashboards need SQL-side sorting by strongest surfaced signal."
        ),
        rationale=(
            "Scoring runs already expose structured counts and scenario identity, so payload summary metrics "
            "are not yet required in SQL."
        ),
    ),
    JsonPromotionCandidate(
        table_name="model_market_board_source_run",
        column_name="payload_json",
        json_path="validation_summary.invalid_row_count",
        candidate_column_name="invalid_row_count",
        priority="medium",
        current_action="defer",
        promotion_trigger=(
            "Promote if source-run operations move from in-memory summary calculation to SQL dashboards or alerts."
        ),
        rationale=(
            "The current history summary reads this from payload in Python, so there is no immediate schema need."
        ),
    ),
    JsonPromotionCandidate(
        table_name="feature_analysis_artifact",
        column_name="payload_json",
        json_path="recommendation.status",
        candidate_column_name="recommendation_status",
        priority="low",
        current_action="defer",
        promotion_trigger=(
            "Promote only if evidence-artifact reporting shifts to direct Postgres filtering instead of artifact "
            "materialization in service code."
        ),
        rationale=(
            "Current evidence aggregation is handled after artifact load, and operator opportunity flows already "
            "have dedicated structured columns for recommendation/evidence status."
        ),
    ),
)


def list_postgres_json_promotion_candidates() -> tuple[JsonPromotionCandidate, ...]:
    return POSTGRES_JSON_PROMOTION_CANDIDATES
