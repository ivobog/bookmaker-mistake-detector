from __future__ import annotations

from bookmaker_detector_api.repositories.ingestion_postgres_schema import (
    list_runtime_schema_mutation_ownership,
)


def test_runtime_schema_mutation_ownership_inventory_points_to_real_bootstrap_sql() -> None:
    ownership = list_runtime_schema_mutation_ownership()

    assert [entry.helper_name for entry in ownership] == [
        "ensure_raw_row_source_identity_schema",
        "ensure_data_quality_issue_identity_schema",
    ]

    for entry in ownership:
        assert entry.operations
        assert entry.target_state
        assert entry.bootstrap_sql_abspath.exists()

