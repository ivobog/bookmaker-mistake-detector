from datetime import date

from typing import Annotated

from fastapi import APIRouter, Body, Depends, Query

from bookmaker_detector_api.api.schemas import (
    AdminFutureGamePreviewFilters,
    AdminFutureGamePreviewResponse,
    AdminScoringHistoryResponse,
    AdminScoringPreviewFilters,
    AdminScoringPreviewResponse,
    AdminScoringRun,
    AdminScoringRunDetailResponse,
    AdminScoringRunsResponse,
)
from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.repositories import InMemoryIngestionRepository
from bookmaker_detector_api.services.models import (
    get_model_future_game_preview_in_memory,
    get_model_future_game_preview_postgres,
    get_model_future_slate_preview_in_memory,
    get_model_future_slate_preview_postgres,
    get_model_scoring_history_in_memory,
    get_model_scoring_history_postgres,
    get_model_scoring_preview_in_memory,
    get_model_scoring_preview_postgres,
    get_model_scoring_run_detail_in_memory,
    get_model_scoring_run_detail_postgres,
    list_model_scoring_runs_in_memory,
    list_model_scoring_runs_postgres,
    materialize_model_future_game_preview_in_memory,
    materialize_model_future_game_preview_postgres,
    materialize_model_future_slate_in_memory,
    materialize_model_future_slate_postgres,
)

from .admin_model_support import (
    FutureSlateRequest,
    _prepare_in_memory_future_game_scoring_repository,
    _prepare_in_memory_future_slate_repository,
    _use_postgres_stable_read_mode,
)

router = APIRouter(prefix="/admin", tags=["admin"])


def _serialize_scoring_run(scoring_run) -> AdminScoringRun:
    return AdminScoringRun(
        id=scoring_run.id,
        model_selection_snapshot_id=scoring_run.model_selection_snapshot_id,
        model_evaluation_snapshot_id=scoring_run.model_evaluation_snapshot_id,
        feature_version_id=scoring_run.feature_version_id,
        target_task=scoring_run.target_task,
        scenario_key=scoring_run.scenario_key,
        season_label=scoring_run.season_label,
        game_date=scoring_run.game_date.isoformat(),
        home_team_code=scoring_run.home_team_code,
        away_team_code=scoring_run.away_team_code,
        home_spread_line=scoring_run.home_spread_line,
        total_line=scoring_run.total_line,
        policy_name=scoring_run.policy_name,
        prediction_count=scoring_run.prediction_count,
        candidate_opportunity_count=scoring_run.candidate_opportunity_count,
        review_opportunity_count=scoring_run.review_opportunity_count,
        discarded_opportunity_count=scoring_run.discarded_opportunity_count,
        payload=scoring_run.payload,
        created_at=scoring_run.created_at.isoformat() if scoring_run.created_at else None,
    )


@router.get("/models/score-preview")
def phase_three_model_score_preview(
    filters: Annotated[AdminScoringPreviewFilters, Depends()],
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> AdminScoringPreviewResponse:
    dimensions = tuple(filters.dimensions or ["venue", "days_rest_bucket"])
    filters = filters.model_copy(update={"dimensions": list(dimensions)})
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            scoring_preview = get_model_scoring_preview_postgres(
                connection,
                feature_key=filters.feature_key,
                target_task=filters.target_task,
                team_code=filters.team_code,
                season_label=filters.season_label,
                canonical_game_id=filters.canonical_game_id,
                limit=filters.limit,
                include_evidence=filters.include_evidence,
                evidence_dimensions=dimensions,
                comparable_limit=filters.comparable_limit,
                min_pattern_sample_size=filters.min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        repository_mode = "postgres"
    else:
        repository = InMemoryIngestionRepository()
        scoring_preview = get_model_scoring_preview_in_memory(
            repository,
            feature_key=filters.feature_key,
            target_task=filters.target_task,
            team_code=filters.team_code,
            season_label=filters.season_label,
            canonical_game_id=filters.canonical_game_id,
            limit=filters.limit,
            include_evidence=filters.include_evidence,
            evidence_dimensions=dimensions,
            comparable_limit=filters.comparable_limit,
            min_pattern_sample_size=filters.min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
        repository_mode = "in_memory"

    return AdminScoringPreviewResponse(
        repository_mode=repository_mode,
        filters=filters,
        **scoring_preview,
    )


@router.get("/models/future-game-preview")
def phase_three_model_future_game_preview(
    filters: Annotated[AdminFutureGamePreviewFilters, Depends()],
    game_date: date = Query(default=date(2026, 4, 20)),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> AdminFutureGamePreviewResponse:
    dimensions = tuple(filters.dimensions or ["venue", "days_rest_bucket"])
    filters = filters.model_copy(
        update={"dimensions": list(dimensions), "game_date": game_date.isoformat()}
    )
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            preview = get_model_future_game_preview_postgres(
                connection,
                feature_key=filters.feature_key,
                target_task=filters.target_task or "spread_error_regression",
                season_label=filters.season_label,
                game_date=game_date,
                home_team_code=filters.home_team_code,
                away_team_code=filters.away_team_code,
                home_spread_line=filters.home_spread_line,
                total_line=filters.total_line,
                include_evidence=filters.include_evidence,
                evidence_dimensions=dimensions,
                comparable_limit=filters.comparable_limit,
                min_pattern_sample_size=filters.min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        repository_mode = "postgres"
    else:
        repository = InMemoryIngestionRepository()
        preview = get_model_future_game_preview_in_memory(
            repository,
            feature_key=filters.feature_key,
            target_task=filters.target_task or "spread_error_regression",
            season_label=filters.season_label,
            game_date=game_date,
            home_team_code=filters.home_team_code,
            away_team_code=filters.away_team_code,
            home_spread_line=filters.home_spread_line,
            total_line=filters.total_line,
            include_evidence=filters.include_evidence,
            evidence_dimensions=dimensions,
            comparable_limit=filters.comparable_limit,
            min_pattern_sample_size=filters.min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
        repository_mode = "in_memory"

    return AdminFutureGamePreviewResponse(
        repository_mode=repository_mode,
        filters=filters,
        **preview,
    )


@router.post("/models/future-game-preview/materialize")
def phase_three_model_future_game_preview_materialize(
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    home_team_code: str = Query(default="LAL"),
    away_team_code: str = Query(default="BOS"),
    home_spread_line: float | None = Query(default=None),
    total_line: float | None = Query(default=None),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            materialized = materialize_model_future_game_preview_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                season_label=season_label,
                game_date=game_date,
                home_team_code=home_team_code,
                away_team_code=away_team_code,
                home_spread_line=home_spread_line,
                total_line=total_line,
                include_evidence=include_evidence,
                evidence_dimensions=dimensions,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_future_game_scoring_repository(
            feature_key=feature_key,
            target_task=target_task,
            season_label=season_label,
            game_date=game_date,
            home_team_code=home_team_code,
            away_team_code=away_team_code,
            home_spread_line=home_spread_line,
            total_line=total_line,
            include_evidence=include_evidence,
            dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
        materialized = materialize_model_future_game_preview_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            season_label=season_label,
            game_date=game_date,
            home_team_code=home_team_code,
            away_team_code=away_team_code,
            home_spread_line=home_spread_line,
            total_line=total_line,
            include_evidence=include_evidence,
            evidence_dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "season_label": season_label,
            "game_date": game_date,
            "home_team_code": home_team_code,
            "away_team_code": away_team_code,
            "home_spread_line": home_spread_line,
            "total_line": total_line,
            "include_evidence": include_evidence,
            "dimensions": list(dimensions),
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
        },
        **materialized,
    }


@router.get("/models/future-game-preview/runs")
def phase_three_model_future_game_preview_runs(
    filters: Annotated[AdminFutureGamePreviewFilters, Depends()],
    game_date: date = Query(default=date(2026, 4, 20)),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    limit: int = Query(default=10, ge=1, le=100),
) -> AdminScoringRunsResponse:
    filters = filters.model_copy(
        update={
            "dimensions": list(filters.dimensions or ["venue", "days_rest_bucket"]),
            "game_date": game_date.isoformat(),
        }
    )
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            scoring_runs = list_model_scoring_runs_postgres(
                connection,
                target_task=filters.target_task,
                team_code=filters.team_code,
                season_label=filters.season_label,
            )
        repository_mode = "postgres"
    else:
        repository = InMemoryIngestionRepository()
        scoring_runs = list_model_scoring_runs_in_memory(
            repository,
            target_task=filters.target_task,
            team_code=filters.team_code,
            season_label=filters.season_label,
        )
        repository_mode = "in_memory"

    return AdminScoringRunsResponse(
        repository_mode=repository_mode,
        filters=filters,
        scoring_run_count=len(scoring_runs),
        scoring_runs=[
            _serialize_scoring_run(scoring_run) for scoring_run in scoring_runs[:limit]
        ],
    )


@router.get("/models/future-game-preview/runs/{scoring_run_id}")
def phase_three_model_future_game_preview_run_detail(
    scoring_run_id: int,
    filters: Annotated[AdminFutureGamePreviewFilters, Depends()],
    game_date: date = Query(default=date(2026, 4, 20)),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> AdminScoringRunDetailResponse:
    filters = filters.model_copy(
        update={
            "dimensions": list(filters.dimensions or ["venue", "days_rest_bucket"]),
            "game_date": game_date.isoformat(),
        }
    )
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            scoring_run = get_model_scoring_run_detail_postgres(
                connection,
                scoring_run_id=scoring_run_id,
            )
        repository_mode = "postgres"
    else:
        repository = InMemoryIngestionRepository()
        scoring_run = get_model_scoring_run_detail_in_memory(
            repository,
            scoring_run_id=scoring_run_id,
        )
        repository_mode = "in_memory"

    return AdminScoringRunDetailResponse(
        repository_mode=repository_mode,
        filters=filters,
        scoring_run=scoring_run,
    )


@router.get("/models/future-game-preview/history")
def phase_three_model_future_game_preview_history(
    filters: Annotated[AdminFutureGamePreviewFilters, Depends()],
    game_date: date = Query(default=date(2026, 4, 20)),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> AdminScoringHistoryResponse:
    filters = filters.model_copy(
        update={
            "dimensions": list(filters.dimensions or ["venue", "days_rest_bucket"]),
            "game_date": game_date.isoformat(),
            "recent_limit": recent_limit,
        }
    )
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            history = get_model_scoring_history_postgres(
                connection,
                target_task=filters.target_task,
                team_code=filters.team_code,
                season_label=filters.season_label,
                recent_limit=recent_limit,
            )
        repository_mode = "postgres"
    else:
        repository = InMemoryIngestionRepository()
        history = get_model_scoring_history_in_memory(
            repository,
            target_task=filters.target_task,
            team_code=filters.team_code,
            season_label=filters.season_label,
            recent_limit=recent_limit,
        )
        repository_mode = "in_memory"

    return AdminScoringHistoryResponse(
        repository_mode=repository_mode,
        filters=filters,
        model_scoring_history=history,
    )


@router.post("/models/future-slate/preview")
def phase_three_model_future_slate_preview(
    request: FutureSlateRequest = Body(...),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    games = [game.model_dump() for game in request.games]
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            preview = get_model_future_slate_preview_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                games=games,
                slate_label=request.slate_label,
                include_evidence=include_evidence,
                evidence_dimensions=dimensions,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_future_slate_repository(
            feature_key=feature_key,
            target_task=target_task,
            games=games,
            slate_label=request.slate_label,
            include_evidence=include_evidence,
            dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
        preview = get_model_future_slate_preview_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            games=games,
            slate_label=request.slate_label,
            include_evidence=include_evidence,
            evidence_dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "slate_label": request.slate_label,
            "include_evidence": include_evidence,
            "dimensions": list(dimensions),
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
        },
        **preview,
    }


@router.post("/models/future-slate/materialize")
def phase_three_model_future_slate_materialize(
    request: FutureSlateRequest = Body(...),
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    games = [game.model_dump() for game in request.games]
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            materialized = materialize_model_future_slate_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                games=games,
                slate_label=request.slate_label,
                include_evidence=include_evidence,
                evidence_dimensions=dimensions,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_future_slate_repository(
            feature_key=feature_key,
            target_task=target_task,
            games=games,
            slate_label=request.slate_label,
            include_evidence=include_evidence,
            dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
        materialized = materialize_model_future_slate_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            games=games,
            slate_label=request.slate_label,
            include_evidence=include_evidence,
            evidence_dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "slate_label": request.slate_label,
            "include_evidence": include_evidence,
            "dimensions": list(dimensions),
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
        },
        **materialized,
    }
