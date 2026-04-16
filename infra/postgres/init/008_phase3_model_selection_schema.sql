CREATE TABLE IF NOT EXISTS model_selection_snapshot (
    id BIGSERIAL PRIMARY KEY,
    model_evaluation_snapshot_id BIGINT NOT NULL REFERENCES model_evaluation_snapshot(id) ON DELETE CASCADE,
    model_training_run_id BIGINT NOT NULL REFERENCES model_training_run(id) ON DELETE CASCADE,
    model_registry_id BIGINT NOT NULL REFERENCES model_registry(id) ON DELETE CASCADE,
    feature_version_id BIGINT NOT NULL REFERENCES feature_version(id) ON DELETE CASCADE,
    target_task VARCHAR(64) NOT NULL,
    model_family VARCHAR(64) NOT NULL,
    selection_policy_name VARCHAR(64) NOT NULL,
    rationale_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
