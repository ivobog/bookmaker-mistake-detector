from __future__ import annotations

from bookmaker_detector_api.repositories.postgres_json_promotion_candidates import (
    list_postgres_json_promotion_candidates,
)


def test_postgres_json_promotion_candidates_prioritize_backtest_metrics() -> None:
    candidates = list_postgres_json_promotion_candidates()

    assert candidates
    assert candidates[0].table_name == "model_backtest_run"
    assert candidates[0].candidate_column_name == "candidate_roi"
    assert candidates[0].priority == "high"
    assert all(candidate.current_action == "defer" for candidate in candidates)


def test_postgres_json_promotion_candidates_are_unique() -> None:
    candidates = list_postgres_json_promotion_candidates()
    keys = [
        (candidate.table_name, candidate.column_name, candidate.json_path)
        for candidate in candidates
    ]

    assert len(keys) == len(set(keys))
    assert all(candidate.promotion_trigger for candidate in candidates)
    assert all(candidate.rationale for candidate in candidates)
