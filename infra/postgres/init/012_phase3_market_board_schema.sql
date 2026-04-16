CREATE TABLE IF NOT EXISTS model_market_board (
    id BIGSERIAL PRIMARY KEY,
    board_key VARCHAR(255) NOT NULL UNIQUE,
    slate_label VARCHAR(128),
    target_task VARCHAR(64) NOT NULL,
    season_label VARCHAR(32),
    game_count INTEGER NOT NULL DEFAULT 0,
    game_date_start DATE,
    game_date_end DATE,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_model_market_board_target_task
    ON model_market_board (target_task, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_model_market_board_season_label
    ON model_market_board (season_label, created_at DESC);
