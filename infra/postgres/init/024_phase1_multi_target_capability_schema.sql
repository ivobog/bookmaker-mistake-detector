CREATE TABLE IF NOT EXISTS target_task_definition (
    task_key VARCHAR(64) PRIMARY KEY,
    task_kind VARCHAR(32) NOT NULL,
    label VARCHAR(128) NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    market_type VARCHAR(32) NOT NULL,
    primary_metric_name VARCHAR(32) NOT NULL,
    metric_direction VARCHAR(32) NOT NULL,
    opportunity_policy_name VARCHAR(64) NOT NULL,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS model_family_capability (
    id BIGSERIAL PRIMARY KEY,
    model_family VARCHAR(64) NOT NULL,
    target_task VARCHAR(64) NOT NULL REFERENCES target_task_definition(task_key) ON DELETE CASCADE,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (model_family, target_task)
);

ALTER TABLE model_evaluation_snapshot
    ADD COLUMN IF NOT EXISTS primary_metric_direction VARCHAR(32) NOT NULL DEFAULT 'lower_is_better',
    ADD COLUMN IF NOT EXISTS selection_score DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS selection_score_name VARCHAR(64);

UPDATE model_evaluation_snapshot
SET
    primary_metric_direction = COALESCE(primary_metric_direction, 'lower_is_better'),
    selection_score = COALESCE(selection_score, validation_metric_value, test_metric_value),
    selection_score_name = COALESCE(selection_score_name, 'validation_regression_candidate_v1');

INSERT INTO target_task_definition (
    task_key,
    task_kind,
    label,
    description,
    market_type,
    primary_metric_name,
    metric_direction,
    opportunity_policy_name,
    is_enabled,
    config_json
)
VALUES
    (
        'spread_error_regression',
        'regression',
        'Spread Error Regression',
        'Predicts spread error relative to the market line.',
        'spread',
        'mae',
        'lower_is_better',
        'spread_edge_policy_v1',
        TRUE,
        '{
            "default_selection_policy_name": "validation_regression_candidate_v1",
            "selection_policy_names": ["validation_regression_candidate_v1", "validation_mae_candidate_v1"],
            "scoring_output_semantics": "market_edge_regression",
            "signal_strength_interpretation": "predicted_spread_edge",
            "is_default_ui_task": true,
            "workflow_support": {
                "training": true,
                "selection": true,
                "scoring": true,
                "opportunity_materialization": true,
                "market_board": true,
                "backtesting": true
            }
        }'::jsonb
    ),
    (
        'total_error_regression',
        'regression',
        'Total Error Regression',
        'Predicts total error relative to the market total line.',
        'total',
        'mae',
        'lower_is_better',
        'total_edge_policy_v1',
        TRUE,
        '{
            "default_selection_policy_name": "validation_regression_candidate_v1",
            "selection_policy_names": ["validation_regression_candidate_v1", "validation_mae_candidate_v1"],
            "scoring_output_semantics": "market_edge_regression",
            "signal_strength_interpretation": "predicted_total_edge",
            "is_default_ui_task": false,
            "workflow_support": {
                "training": true,
                "selection": true,
                "scoring": true,
                "opportunity_materialization": true,
                "market_board": true,
                "backtesting": true
            }
        }'::jsonb
    ),
    (
        'point_margin_regression',
        'regression',
        'Point Margin Regression',
        'Predicts team margin and compares it against the market spread.',
        'spread',
        'mae',
        'lower_is_better',
        'margin_signal_policy_v1',
        TRUE,
        '{
            "default_selection_policy_name": "validation_regression_candidate_v1",
            "selection_policy_names": ["validation_regression_candidate_v1", "validation_mae_candidate_v1"],
            "scoring_output_semantics": "market_edge_regression",
            "signal_strength_interpretation": "predicted_margin_vs_market_line",
            "is_default_ui_task": false,
            "workflow_support": {
                "training": true,
                "selection": true,
                "scoring": true,
                "opportunity_materialization": true,
                "market_board": true,
                "backtesting": true
            }
        }'::jsonb
    ),
    (
        'total_points_regression',
        'regression',
        'Total Points Regression',
        'Predicts total points and compares the prediction against the market total.',
        'total',
        'mae',
        'lower_is_better',
        'totals_signal_policy_v1',
        TRUE,
        '{
            "default_selection_policy_name": "validation_regression_candidate_v1",
            "selection_policy_names": ["validation_regression_candidate_v1", "validation_mae_candidate_v1"],
            "scoring_output_semantics": "market_edge_regression",
            "signal_strength_interpretation": "predicted_total_points_vs_market_line",
            "is_default_ui_task": false,
            "workflow_support": {
                "training": true,
                "selection": true,
                "scoring": true,
                "opportunity_materialization": true,
                "market_board": true,
                "backtesting": true
            }
        }'::jsonb
    )
ON CONFLICT (task_key)
DO UPDATE SET
    task_kind = EXCLUDED.task_kind,
    label = EXCLUDED.label,
    description = EXCLUDED.description,
    market_type = EXCLUDED.market_type,
    primary_metric_name = EXCLUDED.primary_metric_name,
    metric_direction = EXCLUDED.metric_direction,
    opportunity_policy_name = EXCLUDED.opportunity_policy_name,
    is_enabled = EXCLUDED.is_enabled,
    config_json = EXCLUDED.config_json,
    updated_at = NOW();

INSERT INTO model_family_capability (
    model_family,
    target_task,
    is_enabled,
    config_json
)
VALUES
    ('linear_feature', 'spread_error_regression', TRUE, '{"task_kind": "regression", "prediction_output_type": "numeric"}'::jsonb),
    ('linear_feature', 'total_error_regression', TRUE, '{"task_kind": "regression", "prediction_output_type": "numeric"}'::jsonb),
    ('linear_feature', 'point_margin_regression', TRUE, '{"task_kind": "regression", "prediction_output_type": "numeric"}'::jsonb),
    ('linear_feature', 'total_points_regression', TRUE, '{"task_kind": "regression", "prediction_output_type": "numeric"}'::jsonb),
    ('tree_stump', 'spread_error_regression', TRUE, '{"task_kind": "regression", "prediction_output_type": "numeric"}'::jsonb),
    ('tree_stump', 'total_error_regression', TRUE, '{"task_kind": "regression", "prediction_output_type": "numeric"}'::jsonb),
    ('tree_stump', 'point_margin_regression', TRUE, '{"task_kind": "regression", "prediction_output_type": "numeric"}'::jsonb),
    ('tree_stump', 'total_points_regression', TRUE, '{"task_kind": "regression", "prediction_output_type": "numeric"}'::jsonb)
ON CONFLICT (model_family, target_task)
DO UPDATE SET
    is_enabled = EXCLUDED.is_enabled,
    config_json = EXCLUDED.config_json;
