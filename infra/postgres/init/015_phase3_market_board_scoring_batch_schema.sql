CREATE TABLE IF NOT EXISTS model_market_board_scoring_batch (
    id BIGSERIAL PRIMARY KEY,
    target_task VARCHAR(64) NOT NULL,
    source_name VARCHAR(64),
    season_label VARCHAR(32),
    freshness_status VARCHAR(32),
    pending_only BOOLEAN NOT NULL DEFAULT TRUE,
    candidate_board_count INTEGER NOT NULL DEFAULT 0,
    scored_board_count INTEGER NOT NULL DEFAULT 0,
    materialized_scoring_run_count INTEGER NOT NULL DEFAULT 0,
    materialized_opportunity_count INTEGER NOT NULL DEFAULT 0,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
