CREATE TABLE IF NOT EXISTS model_registry (
    id BIGSERIAL PRIMARY KEY,
    model_key VARCHAR(128) NOT NULL UNIQUE,
    target_task VARCHAR(64) NOT NULL,
    model_family VARCHAR(64) NOT NULL,
    version_label VARCHAR(128) NOT NULL,
    description TEXT,
    config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS model_training_run (
    id BIGSERIAL PRIMARY KEY,
    model_registry_id BIGINT NOT NULL REFERENCES model_registry(id) ON DELETE CASCADE,
    feature_version_id BIGINT NOT NULL REFERENCES feature_version(id) ON DELETE CASCADE,
    target_task VARCHAR(64) NOT NULL,
    scope_team_code VARCHAR(16) NOT NULL DEFAULT '',
    scope_season_label VARCHAR(32) NOT NULL DEFAULT '',
    status VARCHAR(32) NOT NULL,
    train_ratio DOUBLE PRECISION NOT NULL,
    validation_ratio DOUBLE PRECISION NOT NULL,
    artifact_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);
