from __future__ import annotations

from bookmaker_detector_api.repositories.postgres_json_ownership import (
    list_postgres_json_column_ownership,
)


def test_postgres_json_column_ownership_inventory_covers_key_workflows() -> None:
    ownership = list_postgres_json_column_ownership()

    lookup = {(entry.table_name, entry.column_name): entry for entry in ownership}

    assert ("job_run", "payload_json") in lookup
    assert ("game_feature_snapshot", "feature_payload_json") in lookup
    assert ("model_training_run", "artifact_json") in lookup
    assert ("model_scoring_run", "payload_json") in lookup
    assert ("model_opportunity", "payload_json") in lookup
    assert ("model_backtest_run", "payload_json") in lookup
    assert ("model_market_board", "payload_json") in lookup

    assert lookup[("model_scoring_run", "payload_json")].classification == "scoring_provenance"
    assert lookup[("model_opportunity", "payload_json")].classification == "opportunity_provenance"
    assert lookup[("model_backtest_run", "payload_json")].classification == "backtest_provenance"


def test_postgres_json_column_ownership_inventory_has_unique_entries() -> None:
    ownership = list_postgres_json_column_ownership()
    keys = [(entry.table_name, entry.column_name) for entry in ownership]

    assert len(keys) == len(set(keys))
    assert all(entry.structured_columns for entry in ownership)
    assert all(entry.rationale for entry in ownership)
