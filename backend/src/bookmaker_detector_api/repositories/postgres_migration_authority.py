from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]


@dataclass(frozen=True, slots=True)
class BootstrapSqlFile:
    order: int
    relative_path: str
    purpose: str

    @property
    def abspath(self) -> Path:
        return REPO_ROOT / self.relative_path


@dataclass(frozen=True, slots=True)
class MigrationAuthorityDecision:
    current_authority: str
    alembic_status: str
    rationale: tuple[str, ...]
    introduction_triggers: tuple[str, ...]


BOOTSTRAP_SQL_CHAIN: tuple[BootstrapSqlFile, ...] = (
    BootstrapSqlFile(1, "infra/postgres/init/001_reference_schema.sql", "reference tables"),
    BootstrapSqlFile(2, "infra/postgres/init/002_job_schema.sql", "job and audit tables"),
    BootstrapSqlFile(3, "infra/postgres/init/003_phase1_ingestion_schema.sql", "ingestion tables"),
    BootstrapSqlFile(4, "infra/postgres/init/004_seed_reference_data.sql", "reference seed data"),
    BootstrapSqlFile(5, "infra/postgres/init/005_phase2_feature_schema.sql", "feature tables"),
    BootstrapSqlFile(
        6, "infra/postgres/init/006_phase3_model_schema.sql", "model registry and runs"
    ),
    BootstrapSqlFile(
        7,
        "infra/postgres/init/007_phase3_model_evaluation_schema.sql",
        "model evaluation snapshots",
    ),
    BootstrapSqlFile(
        8,
        "infra/postgres/init/008_phase3_model_selection_schema.sql",
        "model selection snapshots",
    ),
    BootstrapSqlFile(9, "infra/postgres/init/009_phase3_opportunity_schema.sql", "opportunities"),
    BootstrapSqlFile(10, "infra/postgres/init/010_phase3_scoring_run_schema.sql", "scoring runs"),
    BootstrapSqlFile(
        11,
        "infra/postgres/init/011_phase3_future_opportunity_schema.sql",
        "future opportunity support",
    ),
    BootstrapSqlFile(12, "infra/postgres/init/012_phase3_market_board_schema.sql", "market boards"),
    BootstrapSqlFile(
        13,
        "infra/postgres/init/013_phase3_market_board_refresh_schema.sql",
        "market board refresh events",
    ),
    BootstrapSqlFile(
        14,
        "infra/postgres/init/014_phase3_market_board_scoring_queue_schema.sql",
        "market board scoring queue",
    ),
    BootstrapSqlFile(
        15,
        "infra/postgres/init/015_phase3_market_board_scoring_batch_schema.sql",
        "market board scoring batches",
    ),
    BootstrapSqlFile(
        16,
        "infra/postgres/init/016_phase3_market_board_refresh_batch_schema.sql",
        "market board refresh batches",
    ),
    BootstrapSqlFile(
        17,
        "infra/postgres/init/017_phase3_market_board_cadence_batch_schema.sql",
        "market board cadence batches",
    ),
    BootstrapSqlFile(18, "infra/postgres/init/018_phase4_backtest_schema.sql", "backtests"),
    BootstrapSqlFile(
        19,
        "infra/postgres/init/019_phase5_raw_row_source_identity_schema.sql",
        "raw row source identity hardening",
    ),
    BootstrapSqlFile(
        20,
        "infra/postgres/init/020_phase7_data_quality_issue_identity_schema.sql",
        "data quality issue identity hardening",
    ),
    BootstrapSqlFile(
        21,
        "infra/postgres/init/021_phase3_market_board_source_run_schema.sql",
        "market board source runs",
    ),
    BootstrapSqlFile(
        22,
        "infra/postgres/init/022_seed_2025_2026_season.sql",
        "2025-2026 season seed",
    ),
    BootstrapSqlFile(
        23,
        "infra/postgres/init/023_phase3_opportunity_materialization_batch_schema.sql",
        "opportunity materialization provenance",
    ),
)


POSTGRES_MIGRATION_AUTHORITY = MigrationAuthorityDecision(
    current_authority="bootstrap_sql_chain",
    alembic_status="deferred",
    rationale=(
        "The repository has no Alembic or migration package today.",
        "Normal runtime flows are now verification-only and no longer depend on "
        "request-time schema mutation.",
        "Remaining schema ownership is explicitly mapped to bootstrap SQL files "
        "and JSON-column inventories.",
    ),
    introduction_triggers=(
        "a new schema change must be applied to already-populated environments "
        "without rebuilding from bootstrap SQL",
        "a JSON promotion candidate needs a multi-step backfill across existing "
        "production-like data",
        "parallel branches are expected to evolve Postgres schema in overlapping "
        "release windows",
    ),
)


def list_bootstrap_sql_chain() -> tuple[BootstrapSqlFile, ...]:
    return BOOTSTRAP_SQL_CHAIN


def get_postgres_migration_authority() -> MigrationAuthorityDecision:
    return POSTGRES_MIGRATION_AUTHORITY
