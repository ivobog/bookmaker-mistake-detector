from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

import bookmaker_detector_api.main as main_module
from bookmaker_detector_api.api import admin_diagnostics_routes as admin_diagnostics_api
from bookmaker_detector_api.api import admin_model_routes as admin_model_api
from bookmaker_detector_api.api import analyst_opportunities as analyst_opportunities_api
from bookmaker_detector_api.api import analyst_trends as analyst_trends_api
from bookmaker_detector_api.services.model_records import (
    ModelOpportunityRecord,
    ModelRegistryRecord,
)


@contextmanager
def _fake_postgres_connection():
    yield object()


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(main_module, "postgres_connection", _fake_postgres_connection)
    monkeypatch.setattr(admin_model_api, "postgres_connection", _fake_postgres_connection)
    monkeypatch.setattr(
        analyst_opportunities_api,
        "postgres_connection",
        _fake_postgres_connection,
    )
    monkeypatch.setattr(
        analyst_trends_api,
        "postgres_connection",
        _fake_postgres_connection,
    )
    with TestClient(main_module.app) as test_client:
        yield test_client


def test_model_capabilities_endpoint_returns_task_registry_payload_without_repository_mode(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        admin_model_api,
        "_load_model_capabilities_payload",
        lambda: {
            "task_count": 2,
            "target_tasks": [
                {
                    "task_key": "spread_error_regression",
                    "task_kind": "regression",
                    "label": "Spread Error",
                    "description": "Spread-based regression task",
                    "market_type": "spread",
                    "primary_metric_name": "mae",
                    "metric_direction": "lower_is_better",
                    "supported_model_families": ["linear_feature", "tree_stump"],
                    "default_selection_policy_name": "validation_regression_candidate_v1",
                    "valid_selection_policy_names": [
                        "validation_regression_candidate_v1",
                        "validation_mae_candidate_v1",
                    ],
                    "default_opportunity_policy_name": "spread_signal_v1",
                    "workflow_support": {
                        "training": True,
                        "selection": True,
                        "scoring": True,
                        "backtesting": True,
                        "opportunity_materialization": True,
                    },
                    "is_enabled": True,
                    "config": {},
                },
                {
                    "task_key": "total_points_regression",
                    "task_kind": "regression",
                    "label": "Total Points",
                    "description": "Total-points regression task",
                    "market_type": "total",
                    "primary_metric_name": "mae",
                    "metric_direction": "lower_is_better",
                    "supported_model_families": ["linear_feature"],
                    "default_selection_policy_name": "validation_regression_candidate_v1",
                    "valid_selection_policy_names": [
                        "validation_regression_candidate_v1",
                        "validation_mae_candidate_v1",
                    ],
                    "default_opportunity_policy_name": "totals_signal_v1",
                    "workflow_support": {
                        "training": True,
                        "selection": True,
                        "scoring": True,
                        "backtesting": True,
                        "opportunity_materialization": True,
                    },
                    "is_enabled": True,
                    "config": {},
                },
            ],
            "ui_defaults": {
                "default_feature_key": "baseline_team_features_v1",
                "default_target_task": "spread_error_regression",
                "default_train_ratio": 0.7,
                "default_validation_ratio": 0.15,
            },
        },
    )

    response = client.get("/api/v1/admin/model-capabilities")

    assert response.status_code == 200
    payload = response.json()
    assert "repository_mode" not in payload
    assert payload["task_count"] == 2
    assert payload["target_tasks"][0]["task_key"] == "spread_error_regression"
    assert payload["ui_defaults"]["default_feature_key"] == "baseline_team_features_v1"


def test_model_registry_endpoint_returns_postgres_contract_without_repository_mode(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(admin_model_api, "_load_model_capabilities_payload", lambda: {"target_tasks": []})
    monkeypatch.setattr(
        admin_model_api,
        "_resolve_target_task",
        lambda target_task, capabilities_payload=None: (target_task or "spread_error_regression", {"target_tasks": []}),
    )
    monkeypatch.setattr(admin_model_api, "_validate_model_admin_inputs", lambda **_: None)
    monkeypatch.setattr(
        admin_model_api,
        "list_model_registry_postgres",
        lambda connection, **kwargs: [
            ModelRegistryRecord(
                id=1,
                model_key="spread_error_regression_linear_feature_global",
                target_task="spread_error_regression",
                model_family="linear_feature",
                version_label="v1",
                description="Baseline regression model",
                config={"team_code_scope": None},
                created_at=datetime(2026, 4, 18, tzinfo=timezone.utc),
            )
        ],
    )

    response = client.get("/api/v1/admin/models/registry?target_task=spread_error_regression")

    assert response.status_code == 200
    payload = response.json()
    assert "repository_mode" not in payload
    assert payload["filters"]["target_task"] == "spread_error_regression"
    assert payload["model_registry_count"] == 1
    assert payload["model_registry"][0]["model_key"] == "spread_error_regression_linear_feature_global"


def test_admin_diagnostics_endpoint_omits_repository_mode_from_wire_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        admin_diagnostics_api,
        "_run_admin_diagnostics",
        lambda **kwargs: {
            "repository_mode": "postgres",
            "filters": {"provider_name": kwargs.get("provider_name")},
            "stats": {"job_count": 3, "success_rate": 1.0},
        },
    )

    response = client.get("/api/v1/admin/ingestion/stats?provider_name=covers")

    assert response.status_code == 200
    payload = response.json()
    assert "repository_mode" not in payload
    assert payload["filters"]["provider_name"] == "covers"
    assert payload["stats"]["job_count"] == 3


def test_analyst_opportunities_endpoint_omits_repository_mode_from_wire_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        analyst_opportunities_api,
        "get_model_opportunity_queue_postgres",
        lambda connection, **kwargs: {
            "queue_batch_id": "batch-1",
            "queue_materialized_at": "2026-04-20T10:00:00+00:00",
            "queue_scope": {"target_task": kwargs.get("target_task")},
            "queue_scope_label": "Current queue",
            "queue_scope_is_scoped": True,
            "opportunities": [
                ModelOpportunityRecord(
                    id=1,
                    model_scoring_run_id=11,
                    model_selection_snapshot_id=21,
                    model_evaluation_snapshot_id=31,
                    feature_version_id=41,
                    target_task="spread_error_regression",
                    source_kind="scoring_run",
                    scenario_key="2026-04-20:LAL:BOS",
                    opportunity_key="opp-1",
                    team_code="LAL",
                    opponent_code="BOS",
                    season_label="2025-2026",
                    canonical_game_id=101,
                    game_date=datetime(2026, 4, 20, tzinfo=timezone.utc).date(),
                    policy_name="spread_signal_v1",
                    status="candidate",
                    prediction_value=4.5,
                    signal_strength=0.82,
                    evidence_rating="strong",
                    recommendation_status="review",
                    materialization_batch_id="batch-1",
                    materialized_at=datetime(2026, 4, 20, 10, 0, tzinfo=timezone.utc),
                    materialization_scope_team_code="LAL",
                    materialization_scope_season_label="2025-2026",
                    materialization_scope_canonical_game_id=101,
                    materialization_scope_source="latest",
                    materialization_scope_key="batch-1:latest",
                    payload={"summary": "Edge found"},
                    created_at=datetime(2026, 4, 20, 10, 0, tzinfo=timezone.utc),
                    updated_at=datetime(2026, 4, 20, 10, 5, tzinfo=timezone.utc),
                )
            ],
        },
    )

    response = client.get("/api/v1/analyst/opportunities?target_task=spread_error_regression")

    assert response.status_code == 200
    payload = response.json()
    assert "repository_mode" not in payload
    assert payload["queue_batch_id"] == "batch-1"
    assert payload["opportunity_count"] == 1
    assert payload["opportunities"][0]["team_code"] == "LAL"


def test_analyst_trend_summary_endpoint_omits_repository_mode_from_wire_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        analyst_trends_api,
        "get_feature_snapshot_summary_postgres",
        lambda connection, **kwargs: {
            "feature_version": {"feature_key": kwargs["feature_key"]},
            "snapshot_count": 12,
            "perspective_count": 2,
            "summary": {"team_count": 6},
            "latest_perspective": {"team_code": "LAL"},
        },
    )

    response = client.get("/api/v1/analyst/trends/summary?feature_key=baseline_team_features_v1")

    assert response.status_code == 200
    payload = response.json()
    assert "repository_mode" not in payload
    assert payload["feature_version"]["feature_key"] == "baseline_team_features_v1"
    assert payload["snapshot_count"] == 12
    assert payload["latest_perspective"]["team_code"] == "LAL"


def test_model_backtest_route_defaults_to_canonical_selection_policy_name(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        admin_model_api,
        "_resolve_target_task",
        lambda target_task, capabilities_payload=None: (
            target_task or "point_margin_regression",
            {"target_tasks": []},
        ),
    )
    monkeypatch.setattr(admin_model_api, "_validate_model_admin_inputs", lambda **_: None)
    captured: dict[str, object] = {}

    def _run_model_backtest_postgres(connection, **kwargs):
        captured.update(kwargs)
        return {
            "feature_version": {"feature_key": kwargs["feature_key"]},
            "backtest_run": {"id": 1},
            "summary": {
                "target_task": kwargs["target_task"],
                "selection_policy_name": kwargs["selection_policy_name"],
                "fold_count": 0,
                "strategy_results": {
                    "candidate_threshold": {"bet_count": 0},
                    "review_threshold": {"bet_count": 0},
                },
            },
        }

    monkeypatch.setattr(admin_model_api, "run_model_backtest_postgres", _run_model_backtest_postgres)

    response = client.post("/api/v1/admin/models/backtests/run?target_task=point_margin_regression")

    assert response.status_code == 200
    payload = response.json()
    assert captured["selection_policy_name"] == "validation_regression_candidate_v1"
    assert payload["filters"]["selection_policy_name"] == "validation_regression_candidate_v1"
    assert payload["summary"]["selection_policy_name"] == "validation_regression_candidate_v1"
