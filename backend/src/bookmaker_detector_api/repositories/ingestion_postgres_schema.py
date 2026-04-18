from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[4]


@dataclass(frozen=True, slots=True)
class RuntimeSchemaMutationOwnership:
    helper_name: str
    bootstrap_sql_path: str
    operations: tuple[str, ...]
    target_state: str

    @property
    def bootstrap_sql_abspath(self) -> Path:
        return REPO_ROOT / self.bootstrap_sql_path


RUNTIME_SCHEMA_MUTATION_OWNERSHIP: tuple[RuntimeSchemaMutationOwnership, ...] = (
    RuntimeSchemaMutationOwnership(
        helper_name="ensure_raw_row_source_identity_schema",
        bootstrap_sql_path="infra/postgres/init/019_phase5_raw_row_source_identity_schema.sql",
        operations=(
            "ALTER TABLE raw_team_game_row ADD COLUMN source_page_url",
            "ALTER TABLE raw_team_game_row ADD COLUMN source_page_season_label",
            "UPDATE raw_team_game_row backfill source_page identity values",
            "CREATE UNIQUE INDEX ux_raw_team_game_row_source_coordinates",
        ),
        target_state=(
            "Keep owned by bootstrap SQL today; move to versioned migration tooling if schema evolution "
            "resumes beyond the current init-chain."
        ),
    ),
    RuntimeSchemaMutationOwnership(
        helper_name="ensure_data_quality_issue_identity_schema",
        bootstrap_sql_path=(
            "infra/postgres/init/020_phase7_data_quality_issue_identity_schema.sql"
        ),
        operations=(
            "DELETE duplicate data_quality_issue identity rows",
            "CREATE UNIQUE INDEX ux_data_quality_issue_identity",
        ),
        target_state=(
            "Keep owned by bootstrap SQL today; treat duplicate cleanup plus index creation as migration-owned "
            "work if this table evolves again."
        ),
    ),
)


def list_runtime_schema_mutation_ownership() -> tuple[RuntimeSchemaMutationOwnership, ...]:
    return RUNTIME_SCHEMA_MUTATION_OWNERSHIP


def verify_raw_row_source_identity_schema(connection: Any) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'raw_team_game_row'
                  AND column_name = 'source_page_url'
            )
            """
        )
        has_source_page_url = bool(cursor.fetchone()[0])
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'raw_team_game_row'
                  AND column_name = 'source_page_season_label'
            )
            """
        )
        has_source_page_season_label = bool(cursor.fetchone()[0])
        cursor.execute(
            """
            SELECT to_regclass('ux_raw_team_game_row_source_coordinates') IS NOT NULL
            """
        )
        has_identity_index = bool(cursor.fetchone()[0])
    return has_source_page_url and has_source_page_season_label and has_identity_index


def ensure_raw_row_source_identity_schema(connection: Any) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            ALTER TABLE raw_team_game_row
            ADD COLUMN IF NOT EXISTS source_page_url TEXT
            """
        )
        cursor.execute(
            """
            ALTER TABLE raw_team_game_row
            ADD COLUMN IF NOT EXISTS source_page_season_label VARCHAR(32)
            """
        )
        cursor.execute(
            """
            UPDATE raw_team_game_row rr
            SET
                source_page_url = COALESCE(rr.source_page_url, rr.source_url),
                source_page_season_label = COALESCE(
                    rr.source_page_season_label,
                    s.label
                )
            FROM season s
            WHERE rr.season_id = s.id
              AND (
                rr.source_page_url IS NULL
                OR rr.source_page_season_label IS NULL
              )
            """
        )
        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_team_game_row_source_coordinates
            ON raw_team_game_row (
                provider_id,
                team_id,
                season_id,
                source_page_url,
                source_page_season_label,
                source_section,
                source_row_index
            )
            """
        )
    connection.commit()


def verify_data_quality_issue_identity_schema(connection: Any) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT to_regclass('ux_data_quality_issue_identity') IS NOT NULL
            """
        )
        return bool(cursor.fetchone()[0])


def ensure_data_quality_issue_identity_schema(connection: Any) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            DELETE FROM data_quality_issue dqi
            USING (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            issue_type,
                            COALESCE(raw_team_game_row_id, 0),
                            COALESCE(canonical_game_id, 0)
                        ORDER BY id DESC
                    ) AS duplicate_rank
                FROM data_quality_issue
            ) duplicates
            WHERE dqi.id = duplicates.id
              AND duplicates.duplicate_rank > 1
            """
        )
        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_data_quality_issue_identity
            ON data_quality_issue (
                issue_type,
                COALESCE(raw_team_game_row_id, 0),
                COALESCE(canonical_game_id, 0)
            )
            """
        )
    connection.commit()
