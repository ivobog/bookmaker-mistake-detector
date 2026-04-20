from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, status

from bookmaker_detector_api.config import settings
from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.ingestion.providers import CoversHistoricalTeamPageProvider
from bookmaker_detector_api.services.features import (
    materialize_baseline_feature_snapshots_for_postgres,
)
from bookmaker_detector_api.services.ingestion_pipeline import (
    HistoricalIngestionRequest,
    ingest_historical_team_page,
)
from bookmaker_detector_api.services.models import (
    list_model_evaluation_snapshots_postgres,
    save_model_selection_snapshot_postgres,
)
from bookmaker_detector_api.services.test_data_seed import (
    seed_phase_two_feature_postgres,
)
from bookmaker_detector_api.services.repository_factory import (
    build_bootstrap_postgres_ingestion_repository,
)
from .admin_model_support import _resolve_target_task, _validate_model_admin_inputs

router = APIRouter(prefix="/test", tags=["test"])

_FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures"
_E2E_SEED_RUNS = (
    {
        "fixture_name": "covers_sample_team_page.html",
        "team_code": "LAL",
        "season_label": "2024-2025",
        "source_url": "https://example.com/covers/lal/2024-2025",
    },
    {
        "fixture_name": "covers_team_page_dal_2024_2025.html",
        "team_code": "DAL",
        "season_label": "2024-2025",
        "source_url": "https://example.com/covers/dal/2024-2025",
    },
    {
        "fixture_name": "covers_team_page_chi_2024_2025.html",
        "team_code": "CHI",
        "season_label": "2024-2025",
        "source_url": "https://example.com/covers/chi/2024-2025",
    },
    {
        "fixture_name": "covers_team_page_phx_2024_2025.html",
        "team_code": "PHX",
        "season_label": "2024-2025",
        "source_url": "https://example.com/covers/phx/2024-2025",
    },
)

_RESET_TABLES = (
    "model_market_board_source_run",
    "model_market_board_refresh_event",
    "model_market_board_refresh_batch",
    "model_market_board_scoring_batch",
    "model_market_board_cadence_batch",
    "model_market_board",
    "model_scoring_run",
    "model_opportunity",
    "model_selection_snapshot",
    "model_evaluation_snapshot",
    "model_training_run",
    "model_registry",
    "model_backtest_run",
    "feature_analysis_artifact",
    "game_feature_snapshot",
    "feature_version",
    "data_quality_issue",
    "game_metric",
    "canonical_game",
    "raw_team_game_row",
    "page_retrieval_reporting_snapshot",
    "job_run_quality_snapshot",
    "job_run_reporting_snapshot",
    "page_retrieval",
    "job_run",
)


def _assert_test_helpers_enabled() -> None:
    if settings.allow_test_helpers:
        return
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Test helpers are not enabled for this environment.",
    )


def _ingest_fixture_page(
    connection: object,
    *,
    fixture_name: str,
    team_code: str,
    season_label: str,
    source_url: str,
    requested_by: str,
) -> object:
    provider = CoversHistoricalTeamPageProvider()
    fixture_html = provider.load_fixture(_FIXTURE_DIR / fixture_name)
    repository = build_bootstrap_postgres_ingestion_repository(connection)
    return ingest_historical_team_page(
        request=HistoricalIngestionRequest(
            provider_name=provider.provider_name,
            team_code=team_code,
            season_label=season_label,
            source_url=source_url,
            requested_by=requested_by,
            html=fixture_html,
        ),
        provider=provider,
        repository=repository,
    )


@router.post("/reset")
def reset_test_state() -> dict[str, object]:
    _assert_test_helpers_enabled()
    with postgres_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE " + ", ".join(_RESET_TABLES) + " RESTART IDENTITY CASCADE"
            )
        connection.commit()
    return {"status": "ok", "tables_reset": list(_RESET_TABLES)}


@router.post("/seed-minimal-dataset")
def seed_minimal_dataset() -> dict[str, object]:
    _assert_test_helpers_enabled()
    with postgres_connection() as connection:
        _, ingest_result, feature_result = seed_phase_two_feature_postgres(connection)
    return {
        "status": "ok",
        "dataset_seeded": True,
        "ingest_result": {
            "job_id": ingest_result.job_id,
            "page_retrieval_id": ingest_result.page_retrieval_id,
            "raw_rows_saved": ingest_result.raw_rows_saved,
            "canonical_games_saved": ingest_result.canonical_games_saved,
            "metrics_saved": ingest_result.metrics_saved,
        },
        "feature_result": {
            "feature_version": feature_result["feature_version"],
            "canonical_game_count": feature_result["canonical_game_count"],
            "snapshots_saved": feature_result["snapshots_saved"],
        },
    }


@router.post("/seed-e2e-dataset")
def seed_e2e_dataset() -> dict[str, object]:
    _assert_test_helpers_enabled()
    ingest_results: list[dict[str, object]] = []
    with postgres_connection() as connection:
        for seed_run in _E2E_SEED_RUNS:
            ingest_result = _ingest_fixture_page(
                connection,
                fixture_name=str(seed_run["fixture_name"]),
                team_code=str(seed_run["team_code"]),
                season_label=str(seed_run["season_label"]),
                source_url=str(seed_run["source_url"]),
                requested_by="playwright-real-e2e",
            )
            ingest_results.append(
                {
                    "team_code": seed_run["team_code"],
                    "season_label": seed_run["season_label"],
                    "raw_rows_saved": ingest_result.raw_rows_saved,
                    "canonical_games_saved": ingest_result.canonical_games_saved,
                    "metrics_saved": ingest_result.metrics_saved,
                }
            )
        feature_result = materialize_baseline_feature_snapshots_for_postgres(connection)
    return {
        "status": "ok",
        "dataset_seeded": True,
        "seed_runs": ingest_results,
        "feature_result": {
            "feature_version": feature_result["feature_version"],
            "canonical_game_count": feature_result["canonical_game_count"],
            "snapshots_saved": feature_result["snapshots_saved"],
        },
    }


@router.post("/materialize-baseline-features")
def materialize_baseline_features() -> dict[str, object]:
    _assert_test_helpers_enabled()
    with postgres_connection() as connection:
        result = materialize_baseline_feature_snapshots_for_postgres(connection)
    return {
        "status": "ok",
        "feature_version": result["feature_version"],
        "canonical_game_count": result["canonical_game_count"],
        "snapshots_saved": result["snapshots_saved"],
    }


@router.post("/activate-selection")
def activate_selection(
    target_task: str | None = Query(default=None),
    model_family: str = Query(default="tree_stump"),
    selection_policy_name: str = Query(default="test_forced_selection_v1"),
) -> dict[str, object]:
    _assert_test_helpers_enabled()
    resolved_target_task, capabilities_payload = _resolve_target_task(target_task)
    _validate_model_admin_inputs(
        capabilities_payload=capabilities_payload,
        target_task=resolved_target_task,
        model_family=model_family,
    )
    with postgres_connection() as connection:
        snapshots = list_model_evaluation_snapshots_postgres(
            connection,
            target_task=resolved_target_task,
            model_family=model_family,
        )
        if not snapshots:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    "No evaluation snapshot is available for the requested selection activation "
                    f"target_task={resolved_target_task}, model_family={model_family}."
                ),
            )
        snapshot = snapshots[0]
        selection = save_model_selection_snapshot_postgres(
            connection,
            snapshot,
            selection_policy_name=selection_policy_name,
        )
    return {
        "status": "ok",
        "selection": {
            "id": selection.id,
            "target_task": selection.target_task,
            "model_family": selection.model_family,
            "selection_policy_name": selection.selection_policy_name,
            "model_evaluation_snapshot_id": selection.model_evaluation_snapshot_id,
            "model_training_run_id": selection.model_training_run_id,
            "is_active": selection.is_active,
        },
    }
