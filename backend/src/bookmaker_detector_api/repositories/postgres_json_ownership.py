from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PostgresJsonColumnOwnership:
    table_name: str
    column_name: str
    classification: str
    structured_columns: tuple[str, ...]
    rationale: str
    promotion_candidates: tuple[str, ...] = ()


POSTGRES_JSON_COLUMN_OWNERSHIP: tuple[PostgresJsonColumnOwnership, ...] = (
    PostgresJsonColumnOwnership(
        table_name="job_run",
        column_name="payload_json",
        classification="workflow_request_provenance",
        structured_columns=("job_name", "status", "requested_by", "started_at", "completed_at"),
        rationale=(
            "Preserves raw ingestion request context such as run labels and source scope. "
            "Operator filtering should continue to flow through structured reporting snapshots."
        ),
        promotion_candidates=("run_label", "provider", "team_code", "season_label"),
    ),
    PostgresJsonColumnOwnership(
        table_name="job_run",
        column_name="summary_json",
        classification="workflow_result_provenance",
        structured_columns=("job_name", "status", "started_at", "completed_at"),
        rationale=(
            "Stores raw completion summaries and provenance-rich counts, while "
            "query-facing counts are "
            "copied into reporting snapshot tables."
        ),
        promotion_candidates=(
            "raw_rows_saved",
            "canonical_games_saved",
            "metrics_saved",
            "quality_issues_saved",
            "warning_count",
        ),
    ),
    PostgresJsonColumnOwnership(
        table_name="audit_event",
        column_name="details_json",
        classification="audit_details",
        structured_columns=("actor", "action", "created_at"),
        rationale="Keeps audit event payloads flexible while actor/action/time remain queryable.",
    ),
    PostgresJsonColumnOwnership(
        table_name="raw_team_game_row",
        column_name="parse_warning_codes_json",
        classification="parser_provenance",
        structured_columns=(
            "provider_id",
            "team_id",
            "season_id",
            "page_retrieval_id",
            "source_page_url",
            "source_page_season_label",
            "source_section",
            "source_row_index",
            "parse_status",
        ),
        rationale=(
            "Keeps warning code lists attached to a specific raw row. Row identity "
            "and parse status stay "
            "in structured columns for joins and operator filters."
        ),
    ),
    PostgresJsonColumnOwnership(
        table_name="canonical_game",
        column_name="source_row_indexes_json",
        classification="ingestion_lineage",
        structured_columns=(
            "season_id",
            "game_date",
            "home_team_id",
            "away_team_id",
            "reconciliation_status",
        ),
        rationale=(
            "Preserves raw-row lineage for reconciliation without turning "
            "variable-length source references "
            "into relational child tables."
        ),
    ),
    PostgresJsonColumnOwnership(
        table_name="canonical_game",
        column_name="warning_codes_json",
        classification="ingestion_lineage",
        structured_columns=(
            "season_id",
            "game_date",
            "home_team_id",
            "away_team_id",
            "reconciliation_status",
        ),
        rationale=(
            "Stores reconciliation warning code lists while the main game identity "
            "remains structured."
        ),
    ),
    PostgresJsonColumnOwnership(
        table_name="data_quality_issue",
        column_name="details_json",
        classification="quality_issue_provenance",
        structured_columns=("issue_type", "severity", "raw_team_game_row_id", "canonical_game_id"),
        rationale=(
            "Issue identity and severity are queryable columns; the nested issue "
            "explanation remains JSON "
            "for audit and review detail."
        ),
    ),
    PostgresJsonColumnOwnership(
        table_name="feature_version",
        column_name="config_json",
        classification="versioned_config",
        structured_columns=("feature_key", "version_label", "description", "created_at"),
        rationale=(
            "Feature version lookup is relational; configuration stays JSON because "
            "it is version metadata."
        ),
    ),
    PostgresJsonColumnOwnership(
        table_name="game_feature_snapshot",
        column_name="feature_payload_json",
        classification="derived_feature_payload",
        structured_columns=(
            "canonical_game_id",
            "feature_version_id",
            "season_id",
            "game_date",
            "home_team_id",
            "away_team_id",
        ),
        rationale=(
            "High-dimensional feature vectors are intentionally stored as JSON, "
            "while dataset slicing uses "
            "game, season, team, and version columns."
        ),
    ),
    PostgresJsonColumnOwnership(
        table_name="feature_analysis_artifact",
        column_name="dimensions_json",
        classification="artifact_scope_metadata",
        structured_columns=(
            "feature_version_id",
            "artifact_type",
            "target_task",
            "scope_team_code",
            "scope_season_label",
            "artifact_key",
        ),
        rationale=(
            "Artifact identity is structured; dimension lists remain JSON because "
            "they are variable metadata."
        ),
    ),
    PostgresJsonColumnOwnership(
        table_name="feature_analysis_artifact",
        column_name="payload_json",
        classification="artifact_payload",
        structured_columns=(
            "feature_version_id",
            "artifact_type",
            "target_task",
            "scope_team_code",
            "scope_season_label",
            "artifact_key",
        ),
        rationale="Feature analysis outputs are report payloads, not relational filter keys.",
    ),
    PostgresJsonColumnOwnership(
        table_name="model_registry",
        column_name="config_json",
        classification="versioned_config",
        structured_columns=(
            "model_key",
            "target_task",
            "model_family",
            "version_label",
            "description",
            "created_at",
        ),
        rationale=(
            "Registry identity and family are relational; config JSON captures "
            "model-family-specific metadata."
        ),
    ),
    PostgresJsonColumnOwnership(
        table_name="model_training_run",
        column_name="artifact_json",
        classification="training_artifact_payload",
        structured_columns=(
            "model_registry_id",
            "feature_version_id",
            "target_task",
            "scope_team_code",
            "scope_season_label",
            "status",
            "train_ratio",
            "validation_ratio",
            "created_at",
            "completed_at",
        ),
        rationale=(
            "Stores trainer-specific learned artifacts and split details. Training "
            "scope and lifecycle state "
            "remain structured."
        ),
        promotion_candidates=("selected_feature", "fallback_strategy"),
    ),
    PostgresJsonColumnOwnership(
        table_name="model_training_run",
        column_name="metrics_json",
        classification="training_metric_payload",
        structured_columns=(
            "model_registry_id",
            "feature_version_id",
            "target_task",
            "scope_team_code",
            "scope_season_label",
            "status",
            "created_at",
            "completed_at",
        ),
        rationale=(
            "Retains full training metrics payloads, while the operator-facing "
            "evaluation snapshot promotes the "
            "primary validation/test metrics and prediction counts."
        ),
        promotion_candidates=(
            "primary_metric_name",
            "validation_metric_value",
            "test_metric_value",
        ),
    ),
    PostgresJsonColumnOwnership(
        table_name="model_evaluation_snapshot",
        column_name="snapshot_json",
        classification="evaluation_snapshot_payload",
        structured_columns=(
            "model_training_run_id",
            "model_registry_id",
            "feature_version_id",
            "target_task",
            "model_family",
            "selected_feature",
            "fallback_strategy",
            "primary_metric_name",
            "validation_metric_value",
            "test_metric_value",
            "validation_prediction_count",
            "test_prediction_count",
            "created_at",
        ),
        rationale=(
            "Detailed evaluation payload stays JSON after the high-value summary "
            "fields are promoted."
        ),
    ),
    PostgresJsonColumnOwnership(
        table_name="model_selection_snapshot",
        column_name="rationale_json",
        classification="selection_rationale",
        structured_columns=(
            "model_evaluation_snapshot_id",
            "model_training_run_id",
            "model_registry_id",
            "feature_version_id",
            "target_task",
            "model_family",
            "selection_policy_name",
            "is_active",
            "created_at",
        ),
        rationale=(
            "Selection audit rationale is JSON; selection identity and active-state "
            "filters are structured."
        ),
    ),
    PostgresJsonColumnOwnership(
        table_name="model_scoring_run",
        column_name="payload_json",
        classification="scoring_provenance",
        structured_columns=(
            "model_selection_snapshot_id",
            "model_evaluation_snapshot_id",
            "feature_version_id",
            "target_task",
            "scenario_key",
            "season_label",
            "game_date",
            "home_team_code",
            "away_team_code",
            "home_spread_line",
            "total_line",
            "policy_name",
            "prediction_count",
            "candidate_opportunity_count",
            "review_opportunity_count",
            "discarded_opportunity_count",
            "created_at",
        ),
        rationale=(
            "Preserves full prediction and preview context, while counts, scenario "
            "identity, and task filters are "
            "already explicit columns."
        ),
    ),
    PostgresJsonColumnOwnership(
        table_name="model_opportunity",
        column_name="payload_json",
        classification="opportunity_provenance",
        structured_columns=(
            "model_selection_snapshot_id",
            "model_evaluation_snapshot_id",
            "feature_version_id",
            "target_task",
            "opportunity_key",
            "team_code",
            "opponent_code",
            "season_label",
            "canonical_game_id",
            "game_date",
            "policy_name",
            "status",
            "prediction_value",
            "signal_strength",
            "evidence_rating",
            "recommendation_status",
            "created_at",
            "updated_at",
        ),
        rationale=(
            "The payload keeps the full prediction, evidence, and policy snapshot "
            "used to materialize the "
            "opportunity; queue and detail filtering uses structured columns."
        ),
    ),
    PostgresJsonColumnOwnership(
        table_name="model_backtest_run",
        column_name="payload_json",
        classification="backtest_provenance",
        structured_columns=(
            "feature_version_id",
            "target_task",
            "scope_team_code",
            "scope_season_label",
            "status",
            "selection_policy_name",
            "strategy_name",
            "minimum_train_games",
            "test_window_games",
            "train_ratio",
            "validation_ratio",
            "fold_count",
            "created_at",
            "completed_at",
        ),
        rationale=(
            "Backtest payloads include fold summaries, bet ledgers, and performance "
            "detail that are too nested "
            "for direct relational modeling. The operator-facing execution knobs remain structured."
        ),
        promotion_candidates=("roi_pct", "win_rate", "net_units"),
    ),
    PostgresJsonColumnOwnership(
        table_name="model_market_board",
        column_name="payload_json",
        classification="market_board_snapshot",
        structured_columns=(
            "board_key",
            "slate_label",
            "target_task",
            "season_label",
            "game_count",
            "game_date_start",
            "game_date_end",
            "created_at",
            "updated_at",
        ),
        rationale=(
            "Board identity and cadence filters are structured; the full slate "
            "snapshot remains JSON."
        ),
    ),
    PostgresJsonColumnOwnership(
        table_name="model_market_board_refresh_event",
        column_name="payload_json",
        classification="market_board_operation_payload",
        structured_columns=(
            "model_market_board_id",
            "board_key",
            "target_task",
            "source_name",
            "refresh_status",
            "game_count",
            "created_at",
        ),
        rationale=(
            "Refresh event summaries are filterable by board/task/source/status; "
            "nested diffs remain JSON."
        ),
    ),
    PostgresJsonColumnOwnership(
        table_name="model_market_board_source_run",
        column_name="payload_json",
        classification="market_board_source_provenance",
        structured_columns=(
            "source_name",
            "target_task",
            "season_label",
            "game_date",
            "slate_label",
            "requested_game_count",
            "generated_game_count",
            "status",
            "created_at",
        ),
        rationale=(
            "Stores raw/generated game payloads and validation detail while "
            "source-run identity stays relational."
        ),
    ),
    PostgresJsonColumnOwnership(
        table_name="model_market_board_refresh_batch",
        column_name="payload_json",
        classification="market_board_operation_payload",
        structured_columns=(
            "target_task",
            "source_name",
            "season_label",
            "freshness_status",
            "pending_only",
            "candidate_board_count",
            "refreshed_board_count",
            "created_board_count",
            "updated_board_count",
            "unchanged_board_count",
            "created_at",
        ),
        rationale=(
            "Batch-level queue snapshots and refresh run detail remain JSON after "
            "summary counters are promoted."
        ),
    ),
    PostgresJsonColumnOwnership(
        table_name="model_market_board_scoring_batch",
        column_name="payload_json",
        classification="market_board_operation_payload",
        structured_columns=(
            "target_task",
            "source_name",
            "season_label",
            "freshness_status",
            "pending_only",
            "candidate_board_count",
            "scored_board_count",
            "materialized_scoring_run_count",
            "materialized_opportunity_count",
            "created_at",
        ),
        rationale=(
            "Batch orchestration context remains JSON while queue/counter dashboards "
            "stay in explicit columns."
        ),
    ),
    PostgresJsonColumnOwnership(
        table_name="model_market_board_cadence_batch",
        column_name="payload_json",
        classification="market_board_operation_payload",
        structured_columns=(
            "target_task",
            "source_name",
            "season_label",
            "refresh_freshness_status",
            "scoring_freshness_status",
            "refreshed_board_count",
            "scored_board_count",
            "materialized_scoring_run_count",
            "materialized_opportunity_count",
            "created_at",
        ),
        rationale=(
            "Combined cadence orchestration detail stays JSON after the operational "
            "summary is promoted."
        ),
    ),
)


def list_postgres_json_column_ownership() -> tuple[PostgresJsonColumnOwnership, ...]:
    return POSTGRES_JSON_COLUMN_OWNERSHIP
