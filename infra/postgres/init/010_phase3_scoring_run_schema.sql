CREATE TABLE IF NOT EXISTS model_scoring_run (
    id BIGSERIAL PRIMARY KEY,
    model_selection_snapshot_id BIGINT
        REFERENCES model_selection_snapshot(id) ON DELETE SET NULL,
    model_evaluation_snapshot_id BIGINT
        REFERENCES model_evaluation_snapshot(id) ON DELETE SET NULL,
    feature_version_id BIGINT NOT NULL REFERENCES feature_version(id) ON DELETE CASCADE,
    target_task VARCHAR(64) NOT NULL,
    scenario_key VARCHAR(255) NOT NULL,
    season_label VARCHAR(32) NOT NULL,
    game_date DATE NOT NULL,
    home_team_code VARCHAR(16) NOT NULL,
    away_team_code VARCHAR(16) NOT NULL,
    home_spread_line DOUBLE PRECISION,
    total_line DOUBLE PRECISION,
    policy_name VARCHAR(64),
    prediction_count INTEGER NOT NULL DEFAULT 0,
    candidate_opportunity_count INTEGER NOT NULL DEFAULT 0,
    review_opportunity_count INTEGER NOT NULL DEFAULT 0,
    discarded_opportunity_count INTEGER NOT NULL DEFAULT 0,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_model_scoring_run_target_task
    ON model_scoring_run (target_task, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_model_scoring_run_season
    ON model_scoring_run (season_label, game_date DESC);
