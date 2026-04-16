CREATE TABLE IF NOT EXISTS model_market_board_cadence_batch (
    id BIGSERIAL PRIMARY KEY,
    target_task VARCHAR(64) NOT NULL,
    source_name VARCHAR(64),
    season_label VARCHAR(32),
    refresh_freshness_status VARCHAR(32),
    scoring_freshness_status VARCHAR(32),
    refreshed_board_count INTEGER NOT NULL DEFAULT 0,
    scored_board_count INTEGER NOT NULL DEFAULT 0,
    materialized_scoring_run_count INTEGER NOT NULL DEFAULT 0,
    materialized_opportunity_count INTEGER NOT NULL DEFAULT 0,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
