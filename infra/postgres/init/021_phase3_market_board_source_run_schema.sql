CREATE TABLE IF NOT EXISTS model_market_board_source_run (
    id BIGSERIAL PRIMARY KEY,
    source_name VARCHAR(64) NOT NULL,
    target_task VARCHAR(64) NOT NULL,
    season_label VARCHAR(32) NOT NULL,
    game_date DATE NOT NULL,
    slate_label VARCHAR(128),
    requested_game_count INTEGER NOT NULL DEFAULT 0,
    generated_game_count INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_model_market_board_source_run_target_task
    ON model_market_board_source_run (target_task, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_model_market_board_source_run_source_name
    ON model_market_board_source_run (source_name, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_model_market_board_source_run_season_label
    ON model_market_board_source_run (season_label, created_at DESC);
