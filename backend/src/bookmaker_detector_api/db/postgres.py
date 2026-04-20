from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from bookmaker_detector_api.config import settings

REQUIRED_POSTGRES_TABLES = (
    "team",
    "provider",
    "season",
    "job_run",
    "page_retrieval",
    "raw_team_game_row",
    "canonical_game",
    "game_metric",
    "data_quality_issue",
    "job_run_reporting_snapshot",
    "page_retrieval_reporting_snapshot",
    "job_run_quality_snapshot",
    "feature_version",
    "game_feature_snapshot",
    "feature_analysis_artifact",
    "model_registry",
    "model_training_run",
    "model_evaluation_snapshot",
    "model_selection_snapshot",
    "target_task_definition",
    "model_family_capability",
    "model_opportunity",
    "model_scoring_run",
    "model_market_board",
    "model_market_board_refresh_event",
    "model_market_board_source_run",
    "model_market_board_refresh_batch",
    "model_market_board_scoring_batch",
    "model_market_board_cadence_batch",
    "model_backtest_run",
)

_schema_verified = False


def reset_postgres_schema_verification_cache() -> None:
    global _schema_verified
    _schema_verified = False


def ensure_required_postgres_schema(connection: Any) -> None:
    global _schema_verified
    if _schema_verified:
        return

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT table_name
            FROM unnest(%s::text[]) AS required(table_name)
            WHERE to_regclass(required.table_name) IS NULL
            ORDER BY table_name ASC
            """,
            (list(REQUIRED_POSTGRES_TABLES),),
        )
        missing_tables = [row[0] for row in cursor.fetchall()]

    if missing_tables:
        missing = ", ".join(missing_tables)
        raise RuntimeError(
            "PostgreSQL schema is not ready. Missing required tables: "
            f"{missing}. Apply the SQL bootstrap in infra/postgres/init before "
            "running postgres-backed API or worker flows."
        )

    _schema_verified = True


@contextmanager
def postgres_connection() -> Iterator[Any]:
    # Imported lazily so unit tests do not require the driver.
    import psycopg

    with psycopg.connect(settings.database_url) as connection:
        ensure_required_postgres_schema(connection)
        yield connection
