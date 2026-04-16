CREATE TABLE IF NOT EXISTS model_market_board_refresh_event (
    id BIGSERIAL PRIMARY KEY,
    model_market_board_id BIGINT NOT NULL
        REFERENCES model_market_board(id) ON DELETE CASCADE,
    board_key VARCHAR(255) NOT NULL,
    target_task VARCHAR(64) NOT NULL,
    source_name VARCHAR(64) NOT NULL,
    refresh_status VARCHAR(32) NOT NULL,
    game_count INTEGER NOT NULL DEFAULT 0,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_model_market_board_refresh_target_task
    ON model_market_board_refresh_event (target_task, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_model_market_board_refresh_source_name
    ON model_market_board_refresh_event (source_name, created_at DESC);
