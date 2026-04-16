CREATE TABLE IF NOT EXISTS model_evaluation_snapshot (
    id BIGSERIAL PRIMARY KEY,
    model_training_run_id BIGINT NOT NULL UNIQUE REFERENCES model_training_run(id) ON DELETE CASCADE,
    model_registry_id BIGINT NOT NULL REFERENCES model_registry(id) ON DELETE CASCADE,
    feature_version_id BIGINT NOT NULL REFERENCES feature_version(id) ON DELETE CASCADE,
    target_task VARCHAR(64) NOT NULL,
    model_family VARCHAR(64) NOT NULL,
    selected_feature VARCHAR(128),
    fallback_strategy VARCHAR(64),
    primary_metric_name VARCHAR(32) NOT NULL,
    validation_metric_value DOUBLE PRECISION,
    test_metric_value DOUBLE PRECISION,
    validation_prediction_count INTEGER NOT NULL DEFAULT 0,
    test_prediction_count INTEGER NOT NULL DEFAULT 0,
    snapshot_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
