from datetime import date

from fastapi import APIRouter, Body, Query

from bookmaker_detector_api.db.postgres import postgres_connection
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
    _prepare_in_memory_phase_three_model_repository,
    _use_postgres_stable_read_mode,
)

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/models/score-preview")
def phase_three_model_score_preview(
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str = Query(default="spread_error_regression"),
    team_code: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    canonical_game_id: int | None = Query(default=None, ge=1),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    limit: int = Query(default=10, ge=1, le=100),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            scoring_preview = get_model_scoring_preview_postgres(
                connection,
                feature_key=feature_key,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                canonical_game_id=canonical_game_id,
                limit=limit,
                include_evidence=include_evidence,
                evidence_dimensions=dimensions,
                comparable_limit=comparable_limit,
                min_pattern_sample_size=min_pattern_sample_size,
                train_ratio=train_ratio,
                validation_ratio=validation_ratio,
            )
        repository_mode = "postgres"
    else:
        repository = _prepare_in_memory_phase_three_model_repository(
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            promote_best=True,
        )
        scoring_preview = get_model_scoring_preview_in_memory(
            repository,
            feature_key=feature_key,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            canonical_game_id=canonical_game_id,
            limit=limit,
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
            "team_code": team_code,
            "season_label": season_label,
            "canonical_game_id": canonical_game_id,
            "limit": limit,
            "include_evidence": include_evidence,
            "dimensions": list(dimensions),
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
        },
        **scoring_preview,
    }


@router.get("/models/future-game-preview")
def phase_three_model_future_game_preview(
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
            preview = get_model_future_game_preview_postgres(
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
        preview = get_model_future_game_preview_in_memory(
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
        **preview,
    }


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
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    team_code: str | None = Query(default=None),
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
    limit: int = Query(default=10, ge=1, le=100),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            scoring_runs = list_model_scoring_runs_postgres(
                connection,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
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
            materialize_preview=True,
        )
        scoring_runs = list_model_scoring_runs_in_memory(
            repository,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "season_label": season_label,
            "game_date": game_date,
            "team_code": team_code,
            "home_team_code": home_team_code,
            "away_team_code": away_team_code,
            "home_spread_line": home_spread_line,
            "total_line": total_line,
            "limit": limit,
        },
        "scoring_run_count": len(scoring_runs),
        "scoring_runs": [
            {
                "id": scoring_run.id,
                "model_selection_snapshot_id": scoring_run.model_selection_snapshot_id,
                "model_evaluation_snapshot_id": scoring_run.model_evaluation_snapshot_id,
                "feature_version_id": scoring_run.feature_version_id,
                "target_task": scoring_run.target_task,
                "scenario_key": scoring_run.scenario_key,
                "season_label": scoring_run.season_label,
                "game_date": scoring_run.game_date.isoformat(),
                "home_team_code": scoring_run.home_team_code,
                "away_team_code": scoring_run.away_team_code,
                "home_spread_line": scoring_run.home_spread_line,
                "total_line": scoring_run.total_line,
                "policy_name": scoring_run.policy_name,
                "prediction_count": scoring_run.prediction_count,
                "candidate_opportunity_count": scoring_run.candidate_opportunity_count,
                "review_opportunity_count": scoring_run.review_opportunity_count,
                "discarded_opportunity_count": scoring_run.discarded_opportunity_count,
                "payload": scoring_run.payload,
                "created_at": scoring_run.created_at.isoformat()
                if scoring_run.created_at
                else None,
            }
            for scoring_run in scoring_runs[:limit]
        ],
    }


@router.get("/models/future-game-preview/runs/{scoring_run_id}")
def phase_three_model_future_game_preview_run_detail(
    scoring_run_id: int,
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
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
            scoring_run = get_model_scoring_run_detail_postgres(
                connection,
                scoring_run_id=scoring_run_id,
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
            materialize_preview=True,
        )
        scoring_run = get_model_scoring_run_detail_in_memory(
            repository,
            scoring_run_id=scoring_run_id,
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
        },
        "scoring_run": scoring_run,
    }


@router.get("/models/future-game-preview/history")
def phase_three_model_future_game_preview_history(
    feature_key: str = Query(default="baseline_team_features_v1"),
    target_task: str | None = Query(default="spread_error_regression"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    team_code: str | None = Query(default=None),
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
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    if _use_postgres_stable_read_mode():
        with postgres_connection() as connection:
            history = get_model_scoring_history_postgres(
                connection,
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                recent_limit=recent_limit,
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
            materialize_preview=True,
        )
        history = get_model_scoring_history_in_memory(
            repository,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            recent_limit=recent_limit,
        )
        repository_mode = "in_memory"

    return {
        "repository_mode": repository_mode,
        "filters": {
            "feature_key": feature_key,
            "target_task": target_task,
            "season_label": season_label,
            "game_date": game_date,
            "team_code": team_code,
            "home_team_code": home_team_code,
            "away_team_code": away_team_code,
            "home_spread_line": home_spread_line,
            "total_line": total_line,
            "recent_limit": recent_limit,
        },
        "model_scoring_history": history,
    }


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
