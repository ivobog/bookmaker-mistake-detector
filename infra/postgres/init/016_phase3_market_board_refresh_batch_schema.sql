CREATE TABLE IF NOT EXISTS model_market_board_refresh_batch (
    id BIGSERIAL PRIMARY KEY,
    target_task VARCHAR(64) NOT NULL,
    source_name VARCHAR(64),
    season_label VARCHAR(32),
    freshness_status VARCHAR(32),
    pending_only BOOLEAN NOT NULL DEFAULT TRUE,
    candidate_board_count INTEGER NOT NULL DEFAULT 0,
    refreshed_board_count INTEGER NOT NULL DEFAULT 0,
    created_board_count INTEGER NOT NULL DEFAULT 0,
    updated_board_count INTEGER NOT NULL DEFAULT 0,
    unchanged_board_count INTEGER NOT NULL DEFAULT 0,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
