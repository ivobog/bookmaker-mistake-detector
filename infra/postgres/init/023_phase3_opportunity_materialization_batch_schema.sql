ALTER TABLE IF EXISTS model_opportunity
    ADD COLUMN IF NOT EXISTS materialization_batch_id VARCHAR(64);

ALTER TABLE IF EXISTS model_opportunity
    ADD COLUMN IF NOT EXISTS materialized_at TIMESTAMPTZ;

ALTER TABLE IF EXISTS model_opportunity
    ADD COLUMN IF NOT EXISTS materialization_scope_team_code VARCHAR(16);

ALTER TABLE IF EXISTS model_opportunity
    ADD COLUMN IF NOT EXISTS materialization_scope_season_label VARCHAR(32);

ALTER TABLE IF EXISTS model_opportunity
    ADD COLUMN IF NOT EXISTS materialization_scope_canonical_game_id BIGINT;

ALTER TABLE IF EXISTS model_opportunity
    ADD COLUMN IF NOT EXISTS materialization_scope_source VARCHAR(32);

ALTER TABLE IF EXISTS model_opportunity
    ADD COLUMN IF NOT EXISTS materialization_scope_key VARCHAR(255);

UPDATE model_opportunity
SET
    materialization_batch_id = COALESCE(materialization_batch_id, CONCAT('legacy-', id)),
    materialized_at = COALESCE(materialized_at, updated_at, created_at, NOW()),
    materialization_scope_source = COALESCE(materialization_scope_source, 'legacy'),
    materialization_scope_key = COALESCE(materialization_scope_key, 'legacy')
WHERE materialization_batch_id IS NULL
   OR materialized_at IS NULL
   OR materialization_scope_source IS NULL
   OR materialization_scope_key IS NULL;

ALTER TABLE IF EXISTS model_opportunity
    ALTER COLUMN materialization_batch_id SET NOT NULL;

ALTER TABLE IF EXISTS model_opportunity
    ALTER COLUMN materialized_at SET NOT NULL;

ALTER TABLE IF EXISTS model_opportunity
    ALTER COLUMN materialization_scope_source SET NOT NULL;

ALTER TABLE IF EXISTS model_opportunity
    ALTER COLUMN materialization_scope_key SET NOT NULL;

ALTER TABLE IF EXISTS model_opportunity
    DROP CONSTRAINT IF EXISTS model_opportunity_opportunity_key_key;

DROP INDEX IF EXISTS idx_model_opportunity_opportunity_key;

CREATE UNIQUE INDEX IF NOT EXISTS idx_model_opportunity_batch_key
    ON model_opportunity (materialization_batch_id, opportunity_key);

CREATE INDEX IF NOT EXISTS idx_model_opportunity_materialized_at
    ON model_opportunity (materialized_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_model_opportunity_scope_source
    ON model_opportunity (
        target_task,
        materialization_scope_source,
        materialization_scope_team_code,
        materialization_scope_season_label,
        materialized_at DESC,
        id DESC
    );
