from datetime import date

from fastapi import APIRouter, Body, Query

from bookmaker_detector_api.db.postgres import postgres_connection
from bookmaker_detector_api.services.models import (
    get_model_market_board_cadence_batch_history_postgres,
    get_model_market_board_cadence_dashboard_postgres,
    get_model_market_board_detail_postgres,
    get_model_market_board_operations_postgres,
    get_model_market_board_refresh_batch_history_postgres,
    get_model_market_board_refresh_history_postgres,
    get_model_market_board_refresh_queue_postgres,
    get_model_market_board_scoring_batch_history_postgres,
    get_model_market_board_scoring_queue_postgres,
    get_model_market_board_source_run_history_postgres,
    list_model_market_board_sources,
    list_model_market_boards_postgres,
    materialize_model_market_board_postgres,
    orchestrate_model_market_board_cadence_postgres,
    orchestrate_model_market_board_refresh_postgres,
    orchestrate_model_market_board_scoring_postgres,
    refresh_model_market_board_postgres,
    score_model_market_board_postgres,
)

from .admin_model_support import (
    FutureSlateRequest,
    _validate_model_admin_inputs,
)

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/models/market-board/sources")
def phase_three_model_market_board_sources() -> dict[str, object]:
    return list_model_market_board_sources()


@router.post("/models/market-board/refresh")
def phase_three_model_market_board_refresh(
    target_task: str = Query(default="spread_error_regression"),
    source_name: str = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default=None),
    game_count: int | None = Query(default=None, ge=1, le=20),
    source_path: str | None = Query(default=None),
) -> dict[str, object]:
    _validate_model_admin_inputs(
        target_task=target_task,
        workflow_name="market_board",
    )
    with postgres_connection() as connection:
        result = refresh_model_market_board_postgres(
            connection,
            target_task=target_task,
            source_name=source_name,
            season_label=season_label,
            game_date=game_date,
            slate_label=slate_label,
            game_count=game_count,
            source_path=source_path,
        )

    return {
        "filters": {
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "source_path": source_path,
        },
        **result,
    }


@router.get("/models/market-board/history")
def phase_three_model_market_board_history(
    target_task: str = Query(default="spread_error_regression"),
    source_name: str = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    source_path: str | None = Query(default=None),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    _validate_model_admin_inputs(
        target_task=target_task,
        workflow_name="market_board",
    )
    with postgres_connection() as connection:
        history = get_model_market_board_refresh_history_postgres(
            connection,
            target_task=target_task,
            source_name=source_name,
            recent_limit=recent_limit,
        )

    return {
        "filters": {
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "source_path": source_path,
            "recent_limit": recent_limit,
        },
        "market_board_refresh_history": history,
    }


@router.get("/models/market-board/source-runs")
def phase_three_model_market_board_source_runs(
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    source_path: str | None = Query(default=None),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    _validate_model_admin_inputs(
        target_task=target_task,
        workflow_name="market_board",
    )
    with postgres_connection() as connection:
        history = get_model_market_board_source_run_history_postgres(
            connection,
            target_task=target_task,
            source_name=source_name,
            season_label=season_label,
            recent_limit=recent_limit,
        )

    return {
        "filters": {
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "source_path": source_path,
            "recent_limit": recent_limit,
        },
        "market_board_source_run_history": history,
    }


@router.get("/models/market-board/refresh-queue")
def phase_three_model_market_board_refresh_queue(
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    freshness_status: str | None = Query(default=None),
    pending_only: bool = Query(default=False),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    _validate_model_admin_inputs(
        target_task=target_task,
        workflow_name="market_board",
    )
    with postgres_connection() as connection:
        queue = get_model_market_board_refresh_queue_postgres(
            connection,
            target_task=target_task,
            season_label=season_label,
            source_name=source_name,
            freshness_status=freshness_status,
            pending_only=pending_only,
            recent_limit=recent_limit,
        )

    return {
        "filters": {
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "freshness_status": freshness_status,
            "pending_only": pending_only,
            "recent_limit": recent_limit,
        },
        "market_board_refresh_queue": queue,
    }


@router.get("/models/market-board/queue")
def phase_three_model_market_board_queue(
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    freshness_status: str | None = Query(default="fresh"),
    pending_only: bool = Query(default=False),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    _validate_model_admin_inputs(
        target_task=target_task,
        workflow_name="market_board",
    )
    with postgres_connection() as connection:
        queue = get_model_market_board_scoring_queue_postgres(
            connection,
            target_task=target_task,
            season_label=season_label,
            source_name=source_name,
            freshness_status=freshness_status,
            pending_only=pending_only,
            recent_limit=recent_limit,
        )

    return {
        "filters": {
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "freshness_status": freshness_status,
            "pending_only": pending_only,
            "recent_limit": recent_limit,
        },
        "market_board_scoring_queue": queue,
    }


@router.post("/models/market-board/orchestrate-refresh")
def phase_three_model_market_board_orchestrate_refresh(
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    freshness_status: str | None = Query(default=None),
    pending_only: bool = Query(default=True),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    _validate_model_admin_inputs(
        target_task=target_task,
        workflow_name="market_board",
    )
    with postgres_connection() as connection:
        result = orchestrate_model_market_board_refresh_postgres(
            connection,
            target_task=target_task,
            season_label=season_label,
            source_name=source_name,
            freshness_status=freshness_status,
            pending_only=pending_only,
            recent_limit=recent_limit,
        )

    return {
        "filters": {
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "freshness_status": freshness_status,
            "pending_only": pending_only,
            "recent_limit": recent_limit,
        },
        **result,
    }


@router.post("/models/market-board/orchestrate-score")
def phase_three_model_market_board_orchestrate_score(
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    feature_key: str = Query(default="baseline_team_features_v1"),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    freshness_status: str | None = Query(default="fresh"),
    pending_only: bool = Query(default=True),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    _validate_model_admin_inputs(
        target_task=target_task,
        workflow_name="market_board",
    )
    with postgres_connection() as connection:
        result = orchestrate_model_market_board_scoring_postgres(
            connection,
            feature_key=feature_key,
            target_task=target_task,
            season_label=season_label,
            source_name=source_name,
            freshness_status=freshness_status,
            pending_only=pending_only,
            include_evidence=include_evidence,
            evidence_dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            recent_limit=recent_limit,
        )

    return {
        "filters": {
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "feature_key": feature_key,
            "include_evidence": include_evidence,
            "dimensions": dimensions,
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "freshness_status": freshness_status,
            "pending_only": pending_only,
            "recent_limit": recent_limit,
        },
        **result,
    }


@router.post("/models/market-board/orchestrate-cadence")
def phase_three_model_market_board_orchestrate_cadence(
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    feature_key: str = Query(default="baseline_team_features_v1"),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    refresh_freshness_status: str | None = Query(default=None),
    refresh_pending_only: bool = Query(default=False),
    scoring_freshness_status: str | None = Query(default="fresh"),
    scoring_pending_only: bool = Query(default=True),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    _validate_model_admin_inputs(
        target_task=target_task,
        workflow_name="market_board",
    )
    with postgres_connection() as connection:
        result = orchestrate_model_market_board_cadence_postgres(
            connection,
            feature_key=feature_key,
            target_task=target_task,
            season_label=season_label,
            source_name=source_name,
            refresh_freshness_status=refresh_freshness_status,
            refresh_pending_only=refresh_pending_only,
            scoring_freshness_status=scoring_freshness_status,
            scoring_pending_only=scoring_pending_only,
            include_evidence=include_evidence,
            evidence_dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            recent_limit=recent_limit,
        )

    return {
        "filters": {
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "feature_key": feature_key,
            "include_evidence": include_evidence,
            "dimensions": dimensions,
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "refresh_freshness_status": refresh_freshness_status,
            "refresh_pending_only": refresh_pending_only,
            "scoring_freshness_status": scoring_freshness_status,
            "scoring_pending_only": scoring_pending_only,
            "recent_limit": recent_limit,
        },
        **result,
    }


@router.get("/models/market-board/refresh-orchestration-history")
def phase_three_model_market_board_refresh_orchestration_history(
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    freshness_status: str | None = Query(default=None),
    pending_only: bool = Query(default=False),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    _validate_model_admin_inputs(
        target_task=target_task,
        workflow_name="market_board",
    )
    with postgres_connection() as connection:
        history = get_model_market_board_refresh_batch_history_postgres(
            connection,
            target_task=target_task,
            source_name=source_name,
            recent_limit=recent_limit,
        )

    return {
        "filters": {
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "freshness_status": freshness_status,
            "pending_only": pending_only,
            "recent_limit": recent_limit,
        },
        "market_board_refresh_orchestration_history": history,
    }


@router.get("/models/market-board/cadence-history")
def phase_three_model_market_board_cadence_history(
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    feature_key: str = Query(default="baseline_team_features_v1"),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    refresh_freshness_status: str | None = Query(default=None),
    refresh_pending_only: bool = Query(default=False),
    scoring_freshness_status: str | None = Query(default="fresh"),
    scoring_pending_only: bool = Query(default=True),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    _validate_model_admin_inputs(
        target_task=target_task,
        workflow_name="market_board",
    )
    with postgres_connection() as connection:
        history = get_model_market_board_cadence_batch_history_postgres(
            connection,
            target_task=target_task,
            source_name=source_name,
            recent_limit=recent_limit,
        )

    return {
        "filters": {
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "feature_key": feature_key,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "refresh_freshness_status": refresh_freshness_status,
            "refresh_pending_only": refresh_pending_only,
            "scoring_freshness_status": scoring_freshness_status,
            "scoring_pending_only": scoring_pending_only,
            "recent_limit": recent_limit,
        },
        "market_board_cadence_history": history,
    }


@router.get("/models/market-board/orchestration-history")
def phase_three_model_market_board_orchestration_history(
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    feature_key: str = Query(default="baseline_team_features_v1"),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    freshness_status: str | None = Query(default="fresh"),
    pending_only: bool = Query(default=True),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    _validate_model_admin_inputs(
        target_task=target_task,
        workflow_name="market_board",
    )
    with postgres_connection() as connection:
        history = get_model_market_board_scoring_batch_history_postgres(
            connection,
            target_task=target_task,
            source_name=source_name,
            recent_limit=recent_limit,
        )

    return {
        "filters": {
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "feature_key": feature_key,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "freshness_status": freshness_status,
            "pending_only": pending_only,
            "recent_limit": recent_limit,
        },
        "market_board_orchestration_history": history,
    }


@router.get("/models/market-board/cadence")
def phase_three_model_market_board_cadence(
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    feature_key: str = Query(default="baseline_team_features_v1"),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    freshness_status: str | None = Query(default="fresh"),
    pending_only: bool = Query(default=True),
    recent_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, object]:
    _validate_model_admin_inputs(
        target_task=target_task,
        workflow_name="market_board",
    )
    with postgres_connection() as connection:
        dashboard = get_model_market_board_cadence_dashboard_postgres(
            connection,
            target_task=target_task,
            season_label=season_label,
            source_name=source_name,
            recent_limit=recent_limit,
        )

    return {
        "filters": {
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "feature_key": feature_key,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "freshness_status": freshness_status,
            "pending_only": pending_only,
            "recent_limit": recent_limit,
        },
        "market_board_cadence": dashboard,
    }


@router.post("/models/market-board/materialize")
def phase_three_model_market_board_materialize(
    request: FutureSlateRequest = Body(...),
    target_task: str = Query(default="spread_error_regression"),
) -> dict[str, object]:
    _validate_model_admin_inputs(
        target_task=target_task,
        workflow_name="market_board",
    )
    games = [game.model_dump() for game in request.games]
    with postgres_connection() as connection:
        board_result = materialize_model_market_board_postgres(
            connection,
            target_task=target_task,
            games=games,
            slate_label=request.slate_label,
        )

    return {
        "filters": {
            "target_task": target_task,
            "slate_label": request.slate_label,
        },
        **board_result,
    }


@router.get("/models/market-board")
def phase_three_model_market_boards(
    target_task: str | None = Query(default=None),
    season_label: str | None = Query(default=None),
    slate_label: str | None = Query(default="demo-market-board"),
    game_date: date = Query(default=date(2026, 4, 20)),
    home_team_code: str = Query(default="LAL"),
    away_team_code: str = Query(default="BOS"),
    home_spread_line: float | None = Query(default=None),
    total_line: float | None = Query(default=None),
) -> dict[str, object]:
    _validate_model_admin_inputs(
        target_task=target_task,
        workflow_name="market_board" if target_task is not None else None,
    )
    with postgres_connection() as connection:
        boards = list_model_market_boards_postgres(
            connection,
            target_task=target_task,
            season_label=season_label,
        )

    return {
        "filters": {
            "target_task": target_task,
            "season_label": season_label,
        },
        "board_count": len(boards),
        "boards": boards,
    }


@router.get("/models/market-board/{board_id}")
def phase_three_model_market_board_detail(
    board_id: int,
    target_task: str = Query(default="spread_error_regression"),
    season_label: str = Query(default="2025-2026"),
    slate_label: str | None = Query(default="demo-market-board"),
    game_date: date = Query(default=date(2026, 4, 20)),
    home_team_code: str = Query(default="LAL"),
    away_team_code: str = Query(default="BOS"),
    home_spread_line: float | None = Query(default=None),
    total_line: float | None = Query(default=None),
) -> dict[str, object]:
    _validate_model_admin_inputs(
        target_task=target_task,
        workflow_name="market_board",
    )
    with postgres_connection() as connection:
        board = get_model_market_board_detail_postgres(connection, board_id=board_id)

    return {
        "filters": {
            "target_task": target_task,
            "season_label": season_label,
        },
        "board": board,
    }


@router.get("/models/market-board/{board_id}/operations")
def phase_three_model_market_board_operations(
    board_id: int,
    target_task: str = Query(default="spread_error_regression"),
    source_name: str | None = Query(default="demo_daily_lines_v1"),
    season_label: str = Query(default="2025-2026"),
    game_date: date = Query(default=date(2026, 4, 20)),
    slate_label: str | None = Query(default="demo-refresh-board"),
    game_count: int | None = Query(default=2, ge=1, le=20),
    feature_key: str = Query(default="baseline_team_features_v1"),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
    freshness_status: str | None = Query(default="fresh"),
    pending_only: bool = Query(default=True),
    recent_limit: int = Query(default=5, ge=1, le=20),
) -> dict[str, object]:
    _validate_model_admin_inputs(
        target_task=target_task,
        workflow_name="market_board",
    )
    with postgres_connection() as connection:
        operations = get_model_market_board_operations_postgres(
            connection,
            board_id=board_id,
            recent_limit=recent_limit,
        )

    return {
        "filters": {
            "target_task": target_task,
            "source_name": source_name,
            "season_label": season_label,
            "game_date": game_date,
            "slate_label": slate_label,
            "game_count": game_count,
            "feature_key": feature_key,
            "train_ratio": train_ratio,
            "validation_ratio": validation_ratio,
            "freshness_status": freshness_status,
            "pending_only": pending_only,
            "recent_limit": recent_limit,
        },
        "operations": operations,
    }


@router.post("/models/market-board/{board_id}/score")
def phase_three_model_market_board_score(
    board_id: int,
    target_task: str = Query(default="spread_error_regression"),
    season_label: str = Query(default="2025-2026"),
    slate_label: str | None = Query(default="demo-market-board"),
    game_date: date = Query(default=date(2026, 4, 20)),
    home_team_code: str = Query(default="LAL"),
    away_team_code: str = Query(default="BOS"),
    home_spread_line: float | None = Query(default=None),
    total_line: float | None = Query(default=None),
    feature_key: str = Query(default="baseline_team_features_v1"),
    include_evidence: bool = Query(default=True),
    dimensions: tuple[str, ...] = Query(default=("venue", "days_rest_bucket")),
    comparable_limit: int = Query(default=5, ge=1, le=50),
    min_pattern_sample_size: int = Query(default=1, ge=1, le=100),
    train_ratio: float = Query(default=0.7, gt=0, lt=1),
    validation_ratio: float = Query(default=0.15, ge=0, lt=1),
) -> dict[str, object]:
    _validate_model_admin_inputs(
        target_task=target_task,
        workflow_name="market_board",
    )
    with postgres_connection() as connection:
        result = score_model_market_board_postgres(
            connection,
            board_id=board_id,
            feature_key=feature_key,
            include_evidence=include_evidence,
            evidence_dimensions=dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
        )

    return {
        "filters": {
            "board_id": board_id,
            "target_task": target_task,
            "season_label": season_label,
            "slate_label": slate_label,
            "feature_key": feature_key,
            "include_evidence": include_evidence,
            "dimensions": list(dimensions),
            "comparable_limit": comparable_limit,
            "min_pattern_sample_size": min_pattern_sample_size,
        },
        **result,
    }
