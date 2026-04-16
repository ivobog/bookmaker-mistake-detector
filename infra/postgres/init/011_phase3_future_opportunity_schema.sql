ALTER TABLE IF EXISTS model_opportunity
    ADD COLUMN IF NOT EXISTS model_scoring_run_id BIGINT
        REFERENCES model_scoring_run(id) ON DELETE SET NULL;

ALTER TABLE IF EXISTS model_opportunity
    ADD COLUMN IF NOT EXISTS source_kind VARCHAR(32) NOT NULL DEFAULT 'historical_game';

ALTER TABLE IF EXISTS model_opportunity
    ADD COLUMN IF NOT EXISTS scenario_key VARCHAR(255);

ALTER TABLE IF EXISTS model_opportunity
    ALTER COLUMN canonical_game_id DROP NOT NULL;

CREATE INDEX IF NOT EXISTS idx_model_opportunity_source_kind
    ON model_opportunity (source_kind, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_model_opportunity_scenario_key
    ON model_opportunity (scenario_key);
