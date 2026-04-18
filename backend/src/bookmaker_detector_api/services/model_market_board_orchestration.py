from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Callable

from bookmaker_detector_api.services import model_future_scenarios
from bookmaker_detector_api.services.model_market_board_views import (
    _build_market_board_cadence_dashboard,
    _build_market_board_cadence_result,
    _build_market_board_operations_summary,
    _build_market_board_refresh_orchestration_result,
    _build_market_board_refresh_queue,
    _build_market_board_scoring_queue,
    _resolve_market_board_refresh_game_date,
)
from bookmaker_detector_api.services.model_records import (
    ModelMarketBoardRecord,
    ModelMarketBoardRefreshRecord,
)


def materialize_model_market_board(
    *,
    target_task: str,
    games: list[dict[str, Any]],
    slate_label: str | None,
    build_market_board: Callable[..., ModelMarketBoardRecord],
    save_market_board: Callable[[ModelMarketBoardRecord], ModelMarketBoardRecord],
    serialize_market_board: Callable[[ModelMarketBoardRecord | None], dict[str, Any] | None],
) -> dict[str, Any]:
    board = build_market_board(
        target_task=target_task,
        games=games,
        slate_label=slate_label,
    )
    persisted = save_market_board(board)
    return {"board": serialize_market_board(persisted)}


def refresh_model_market_board(
    *,
    target_task: str,
    source_name: str,
    season_label: str,
    game_date: date,
    slate_label: str | None,
    game_count: int | None,
    source_path: str | None,
    default_game_count: int,
    build_source_request_context: Callable[..., dict[str, Any]],
    load_source_games: Callable[..., list[dict[str, Any]]],
    normalize_source_games: Callable[..., dict[str, Any]],
    build_source_payload_fingerprints: Callable[..., dict[str, Any]],
    build_source_run: Callable[..., Any],
    save_source_run: Callable[[Any], Any],
    serialize_source_run: Callable[[Any], dict[str, Any] | None],
    find_existing_board: Callable[..., ModelMarketBoardRecord | None],
    materialize_board: Callable[..., dict[str, Any]],
    build_fingerprint_comparison: Callable[..., dict[str, Any]],
    build_refresh_change_summary: Callable[..., dict[str, Any]],
    resolve_refresh_status: Callable[..., str],
    resolve_refresh_count: Callable[..., int],
    save_market_board: Callable[[ModelMarketBoardRecord], ModelMarketBoardRecord],
    save_refresh_event: Callable[[ModelMarketBoardRefreshRecord], Any],
    serialize_market_board: Callable[[ModelMarketBoardRecord | None], dict[str, Any] | None],
) -> dict[str, Any]:
    resolved_slate_label = slate_label or f"{source_name}:{game_date.isoformat()}"
    source_request_context = build_source_request_context(
        source_name=source_name,
        source_path=source_path,
    )
    requested_game_count = game_count or default_game_count
    try:
        games = load_source_games(
            source_name=source_name,
            season_label=season_label,
            game_date=game_date,
            game_count=game_count,
            source_path=source_path,
        )
    except Exception as exc:
        source_run = save_source_run(
            build_source_run(
                source_name=source_name,
                target_task=target_task,
                season_label=season_label,
                game_date=game_date,
                slate_label=resolved_slate_label,
                requested_game_count=requested_game_count,
                generated_games=[],
                source_path=source_path,
                source_request_context=source_request_context,
                status="FAILED",
                error_message=str(exc),
            )
        )
        return {
            "source_name": source_name,
            "status": "FAILED",
            "error_message": str(exc),
            "validation_summary": None,
            "generated_game_count": 0,
            "generated_games": [],
            "source_run": serialize_source_run(source_run),
            "change_summary": None,
            "board": None,
        }

    normalization_result = normalize_source_games(
        source_name=source_name,
        season_label=season_label,
        game_date=game_date,
        raw_games=games,
    )
    normalized_games = normalization_result["normalized_games"]
    validation_summary = normalization_result["validation_summary"]
    source_payload_fingerprints = build_source_payload_fingerprints(
        raw_games=games,
        normalized_games=normalized_games,
    )
    validation_invalid_count = int(validation_summary["invalid_row_count"])
    validation_error_message = None
    validation_status = "SUCCESS"
    if validation_invalid_count > 0 and normalized_games:
        validation_status = "SUCCESS_WITH_WARNINGS"
    elif validation_invalid_count > 0 and not normalized_games:
        validation_status = "FAILED_VALIDATION"
        validation_error_message = "Source provider returned no valid games after normalization."

    source_run = save_source_run(
        build_source_run(
            source_name=source_name,
            target_task=target_task,
            season_label=season_label,
            game_date=game_date,
            slate_label=resolved_slate_label,
            requested_game_count=requested_game_count,
            generated_games=normalized_games,
            raw_generated_games=games,
            source_path=source_path,
            source_request_context=source_request_context,
            status=validation_status,
            error_message=validation_error_message,
            validation_summary=validation_summary,
            source_payload_fingerprints=source_payload_fingerprints,
        )
    )
    if not normalized_games:
        return {
            "source_name": source_name,
            "status": validation_status,
            "error_message": validation_error_message,
            "validation_summary": validation_summary,
            "source_payload_fingerprints": source_payload_fingerprints,
            "generated_game_count": 0,
            "generated_games": [],
            "source_run": serialize_source_run(source_run),
            "change_summary": None,
            "board": None,
        }

    board_key = model_future_scenarios.build_future_slate_key(
        target_task=target_task,
        slate_label=resolved_slate_label,
        serialized_inputs=normalized_games,
    )
    existing_board = find_existing_board(board_key=board_key)
    result = materialize_board(
        target_task=target_task,
        games=normalized_games,
        slate_label=resolved_slate_label,
    )
    board = result["board"]
    change_summary = None
    fingerprint_comparison = build_fingerprint_comparison(
        existing_board=existing_board,
        current_fingerprints=source_payload_fingerprints,
    )
    if board is not None:
        change_summary = build_refresh_change_summary(
            existing_board=existing_board,
            generated_games=normalized_games,
        )
        change_summary["source_payload_fingerprints"] = source_payload_fingerprints
        change_summary["source_fingerprint_comparison"] = fingerprint_comparison
        refresh_status = resolve_refresh_status(
            existing_board=existing_board,
            generated_games=normalized_games,
        )
        refresh_count = resolve_refresh_count(existing_board) + 1
        board["payload"]["source"] = {
            "source_name": source_name,
            "refresh_target_date": game_date.isoformat(),
            "refreshed_at": utc_today().isoformat(),
            "refresh_count": refresh_count,
            "last_refresh_status": refresh_status,
            "source_run_id": source_run.id,
            "source_path": source_path,
            "source_request_context": source_request_context,
            "change_summary": change_summary,
            "source_payload_fingerprints": source_payload_fingerprints,
            "source_fingerprint_comparison": fingerprint_comparison,
        }
        refreshed = save_market_board(
            ModelMarketBoardRecord(
                id=int(board["id"]),
                board_key=str(board["board_key"]),
                slate_label=board.get("slate_label"),
                target_task=str(board["target_task"]),
                season_label=board.get("season_label"),
                game_count=int(board["game_count"]),
                game_date_start=model_future_scenarios.coerce_date(board["game_date_start"])
                if board.get("game_date_start")
                else None,
                game_date_end=model_future_scenarios.coerce_date(board["game_date_end"])
                if board.get("game_date_end")
                else None,
                payload=board["payload"],
                created_at=None,
                updated_at=None,
            )
        )
        save_refresh_event(
            ModelMarketBoardRefreshRecord(
                id=0,
                model_market_board_id=refreshed.id,
                board_key=refreshed.board_key,
                target_task=refreshed.target_task,
                source_name=source_name,
                refresh_status=refresh_status,
                game_count=len(normalized_games),
                payload={
                    "games": normalized_games,
                    "refresh_target_date": game_date.isoformat(),
                    "refreshed_at": utc_today().isoformat(),
                    "source_run_id": source_run.id,
                    "source_path": source_path,
                    "source_request_context": source_request_context,
                    "change_summary": change_summary,
                    "validation_summary": validation_summary,
                    "source_payload_fingerprints": source_payload_fingerprints,
                    "source_fingerprint_comparison": fingerprint_comparison,
                },
            )
        )
        board = serialize_market_board(refreshed)
    return {
        "source_name": source_name,
        "status": validation_status,
        "error_message": validation_error_message,
        "validation_summary": validation_summary,
        "source_payload_fingerprints": source_payload_fingerprints,
        "source_path": source_path,
        "source_request_context": source_request_context,
        "generated_game_count": len(normalized_games),
        "generated_games": normalized_games,
        "source_run": serialize_source_run(source_run),
        "change_summary": change_summary,
        "board": board,
    }


def get_model_market_board_refresh_queue(
    *,
    boards: list[Any],
    refresh_events: list[Any],
    source_name: str | None,
    freshness_status: str | None,
    pending_only: bool,
    recent_limit: int,
) -> dict[str, Any]:
    return _build_market_board_refresh_queue(
        boards,
        refresh_events,
        source_name=source_name,
        freshness_status=freshness_status,
        pending_only=pending_only,
        recent_limit=recent_limit,
    )


def get_model_market_board_scoring_queue(
    *,
    boards: list[Any],
    scoring_runs: list[Any],
    source_name: str | None,
    freshness_status: str | None,
    pending_only: bool,
    recent_limit: int,
) -> dict[str, Any]:
    return _build_market_board_scoring_queue(
        boards,
        scoring_runs,
        source_name=source_name,
        freshness_status=freshness_status,
        pending_only=pending_only,
        recent_limit=recent_limit,
    )


def orchestrate_model_market_board_refresh(
    *,
    target_task: str | None,
    season_label: str | None,
    source_name: str | None,
    freshness_status: str | None,
    pending_only: bool,
    recent_limit: int,
    get_queue: Callable[..., dict[str, Any]],
    refresh_board: Callable[..., dict[str, Any]],
    build_batch: Callable[..., Any],
    save_batch: Callable[[Any], Any],
    serialize_batch: Callable[[Any], dict[str, Any] | None],
) -> dict[str, Any]:
    queue_before = get_queue(
        target_task=target_task,
        season_label=season_label,
        source_name=source_name,
        freshness_status=freshness_status,
        pending_only=pending_only,
        recent_limit=recent_limit,
    )
    queue_entries = [
        entry for entry in queue_before["queue_entries"] if bool(entry.get("refreshable"))
    ]
    refresh_runs = []
    for entry in queue_entries:
        board_payload = entry.get("board")
        if not isinstance(board_payload, dict):
            continue
        source_payload = (
            board_payload.get("payload", {}).get("source", {})
            if isinstance(board_payload.get("payload"), dict)
            else {}
        )
        result = refresh_board(
            target_task=str(board_payload["target_task"]),
            source_name=str(entry["source_name"]),
            season_label=str(board_payload.get("season_label") or ""),
            game_date=_resolve_market_board_refresh_game_date(board_payload),
            slate_label=board_payload.get("slate_label"),
            game_count=int(board_payload.get("game_count", 0)) or None,
            source_path=(
                str(source_payload.get("source_path"))
                if isinstance(source_payload, dict) and source_payload.get("source_path")
                else None
            ),
        )
        refresh_runs.append(
            {
                "board": result.get("board") or board_payload,
                "queue_status_before": entry.get("queue_status"),
                "refresh_status_before": entry.get("refresh_status"),
                "refresh_result_status": (
                    result.get("board", {})
                    .get("payload", {})
                    .get("source", {})
                    .get("last_refresh_status")
                ),
                "change_summary": result.get("change_summary"),
            }
        )
    queue_after = get_queue(
        target_task=target_task,
        season_label=season_label,
        source_name=source_name,
        freshness_status=freshness_status,
        pending_only=pending_only,
        recent_limit=recent_limit,
    )
    result = _build_market_board_refresh_orchestration_result(
        queue_before=queue_before,
        queue_after=queue_after,
        refresh_runs=refresh_runs,
    )
    batch = save_batch(
        build_batch(
            result=result,
            target_task=target_task,
            source_name=source_name,
            season_label=season_label,
            freshness_status=freshness_status,
            pending_only=pending_only,
        )
    )
    return {**result, "refresh_batch": serialize_batch(batch)}


def score_model_market_board(
    *,
    board_id: int,
    feature_key: str,
    include_evidence: bool,
    evidence_dimensions: tuple[str, ...],
    comparable_limit: int,
    min_pattern_sample_size: int,
    train_ratio: float,
    validation_ratio: float,
    drop_null_targets: bool,
    get_board_detail: Callable[[int], Any],
    materialize_future_slate: Callable[..., dict[str, Any]],
    serialize_market_board: Callable[[Any], dict[str, Any] | None],
) -> dict[str, Any]:
    board = get_board_detail(board_id)
    if board is None:
        return {"board": None, "slate_result": None}
    slate_result = materialize_future_slate(
        model_market_board_id=board.id,
        feature_key=feature_key,
        target_task=board.target_task,
        games=list(board.payload.get("games", [])),
        slate_label=board.slate_label,
        include_evidence=include_evidence,
        evidence_dimensions=evidence_dimensions,
        comparable_limit=comparable_limit,
        min_pattern_sample_size=min_pattern_sample_size,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
    )
    return {"board": serialize_market_board(board), "slate_result": slate_result}


def orchestrate_model_market_board_scoring(
    *,
    feature_key: str,
    target_task: str | None,
    season_label: str | None,
    source_name: str | None,
    freshness_status: str | None,
    pending_only: bool,
    include_evidence: bool,
    evidence_dimensions: tuple[str, ...],
    comparable_limit: int,
    min_pattern_sample_size: int,
    train_ratio: float,
    validation_ratio: float,
    drop_null_targets: bool,
    recent_limit: int,
    get_queue: Callable[..., dict[str, Any]],
    score_board: Callable[..., dict[str, Any]],
    build_batch: Callable[..., Any],
    save_batch: Callable[[Any], Any],
    serialize_batch: Callable[[Any], dict[str, Any] | None],
) -> dict[str, Any]:
    queue_before = get_queue(
        target_task=target_task,
        season_label=season_label,
        source_name=source_name,
        freshness_status=freshness_status,
        pending_only=pending_only,
        recent_limit=recent_limit,
    )
    queue_entries = list(queue_before["queue_entries"])
    orchestration_runs = []
    for entry in queue_entries:
        board_payload = entry.get("board")
        if not isinstance(board_payload, dict):
            continue
        result = score_board(
            board_id=int(board_payload["id"]),
            feature_key=feature_key,
            include_evidence=include_evidence,
            evidence_dimensions=evidence_dimensions,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_pattern_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
        )
        slate_result = result.get("slate_result") or {}
        orchestration_runs.append(
            {
                "board": result.get("board") or board_payload,
                "queue_status_before": entry.get("queue_status"),
                "scoring_status_before": entry.get("scoring_status"),
                "materialized_scoring_run_count": int(
                    slate_result.get("materialized_scoring_run_count", 0)
                ),
                "materialized_opportunity_count": int(
                    slate_result.get("materialized_opportunity_count", 0)
                ),
                "slate_result": slate_result,
            }
        )
    queue_after = get_queue(
        target_task=target_task,
        season_label=season_label,
        source_name=source_name,
        freshness_status=freshness_status,
        pending_only=pending_only,
        recent_limit=recent_limit,
    )
    result = {
        "queue_before": queue_before,
        "queue_after": queue_after,
        "candidate_board_count": len(queue_entries),
        "scored_board_count": len(orchestration_runs),
        "materialized_scoring_run_count": sum(
            int(entry["materialized_scoring_run_count"]) for entry in orchestration_runs
        ),
        "materialized_opportunity_count": sum(
            int(entry["materialized_opportunity_count"]) for entry in orchestration_runs
        ),
        "orchestration_runs": orchestration_runs,
    }
    batch = save_batch(
        build_batch(
            result=result,
            target_task=target_task,
            source_name=source_name,
            season_label=season_label,
            freshness_status=freshness_status,
            pending_only=pending_only,
        )
    )
    return {**result, "orchestration_batch": serialize_batch(batch)}


def orchestrate_model_market_board_cadence(
    *,
    feature_key: str,
    target_task: str | None,
    season_label: str | None,
    source_name: str | None,
    refresh_freshness_status: str | None,
    refresh_pending_only: bool,
    scoring_freshness_status: str | None,
    scoring_pending_only: bool,
    include_evidence: bool,
    evidence_dimensions: tuple[str, ...],
    comparable_limit: int,
    min_pattern_sample_size: int,
    train_ratio: float,
    validation_ratio: float,
    drop_null_targets: bool,
    recent_limit: int,
    refresh_orchestrator: Callable[..., dict[str, Any]],
    scoring_orchestrator: Callable[..., dict[str, Any]],
    build_batch: Callable[..., Any],
    save_batch: Callable[[Any], Any],
    serialize_batch: Callable[[Any], dict[str, Any] | None],
) -> dict[str, Any]:
    refresh_result = refresh_orchestrator(
        target_task=target_task,
        season_label=season_label,
        source_name=source_name,
        freshness_status=refresh_freshness_status,
        pending_only=refresh_pending_only,
        recent_limit=recent_limit,
    )
    scoring_result = scoring_orchestrator(
        feature_key=feature_key,
        target_task=target_task,
        season_label=season_label,
        source_name=source_name,
        freshness_status=scoring_freshness_status,
        pending_only=scoring_pending_only,
        include_evidence=include_evidence,
        evidence_dimensions=evidence_dimensions,
        comparable_limit=comparable_limit,
        min_pattern_sample_size=min_pattern_sample_size,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
        recent_limit=recent_limit,
    )
    result = _build_market_board_cadence_result(
        refresh_result=refresh_result,
        scoring_result=scoring_result,
    )
    batch = save_batch(
        build_batch(
            result=result,
            target_task=target_task,
            source_name=source_name,
            season_label=season_label,
            refresh_freshness_status=refresh_freshness_status,
            scoring_freshness_status=scoring_freshness_status,
        )
    )
    return {**result, "cadence_batch": serialize_batch(batch)}


def get_model_market_board_operations(
    *,
    board_id: int,
    list_boards: Callable[[], list[Any]],
    list_source_runs: Callable[..., list[Any]],
    list_refresh_events: Callable[..., list[Any]],
    list_scoring_runs: Callable[..., list[Any]],
    list_opportunities: Callable[..., list[Any]],
    list_refresh_batches: Callable[[], list[Any]],
    list_cadence_batches: Callable[[], list[Any]],
    list_scoring_batches: Callable[[], list[Any]],
    recent_limit: int,
) -> dict[str, Any] | None:
    board = next((entry for entry in list_boards() if entry.id == board_id), None)
    if board is None:
        return None
    source_runs = list_source_runs(
        target_task=board.target_task,
        season_label=board.season_label,
    )
    refresh_events = list_refresh_events(
        target_task=board.target_task,
        source_name=None,
    )
    scoring_runs = list_scoring_runs(model_market_board_id=board.id)
    opportunities = [
        opportunity
        for opportunity in list_opportunities(
            target_task=board.target_task,
            season_label=board.season_label,
        )
        if opportunity.model_scoring_run_id is not None
        and any(scoring_run.id == opportunity.model_scoring_run_id for scoring_run in scoring_runs)
    ]
    return _build_market_board_operations_summary(
        board,
        source_runs=source_runs,
        refresh_events=refresh_events,
        refresh_batches=list_refresh_batches(),
        cadence_batches=list_cadence_batches(),
        scoring_runs=scoring_runs,
        opportunities=opportunities,
        batches=list_scoring_batches(),
        recent_limit=recent_limit,
    )


def get_model_market_board_cadence_dashboard(
    *,
    target_task: str | None,
    season_label: str | None,
    source_name: str | None,
    recent_limit: int,
    list_boards: Callable[..., list[Any]],
    list_scoring_runs: Callable[..., list[Any]],
    list_scoring_batches: Callable[..., list[Any]],
) -> dict[str, Any]:
    boards = list_boards(
        target_task=target_task,
        season_label=season_label,
    )
    scoring_runs = list_scoring_runs(
        target_task=target_task,
        season_label=season_label,
    )
    batches = list_scoring_batches(
        target_task=target_task,
        source_name=source_name,
    )
    return _build_market_board_cadence_dashboard(
        boards,
        scoring_runs=scoring_runs,
        batches=batches,
        source_name=source_name,
        recent_limit=recent_limit,
    )


def utc_today() -> date:
    return datetime.now(timezone.utc).date()
