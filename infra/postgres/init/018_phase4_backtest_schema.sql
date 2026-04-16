CREATE TABLE IF NOT EXISTS model_backtest_run (
    id BIGSERIAL PRIMARY KEY,
    feature_version_id BIGINT NOT NULL REFERENCES feature_version(id) ON DELETE CASCADE,
    target_task VARCHAR(64) NOT NULL,
    scope_team_code VARCHAR(16) NOT NULL DEFAULT '',
    scope_season_label VARCHAR(32) NOT NULL DEFAULT '',
    status VARCHAR(32) NOT NULL,
    selection_policy_name VARCHAR(64) NOT NULL,
    strategy_name VARCHAR(64) NOT NULL,
    minimum_train_games INTEGER NOT NULL,
    test_window_games INTEGER NOT NULL,
    train_ratio DOUBLE PRECISION NOT NULL,
    validation_ratio DOUBLE PRECISION NOT NULL,
    fold_count INTEGER NOT NULL DEFAULT 0,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);
