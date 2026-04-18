from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from bookmaker_detector_api.services.model_market_board_sources import (
    MARKET_BOARD_SOURCE_CONFIGS,
)
from bookmaker_detector_api.services.model_market_board_store import (
    _serialize_model_market_board,
    _serialize_model_market_board_cadence_batch,
    _serialize_model_market_board_refresh_batch,
    _serialize_model_market_board_refresh_event,
    _serialize_model_market_board_scoring_batch,
    _serialize_model_market_board_source_run,
)
from bookmaker_detector_api.services.model_records import (
    ModelMarketBoardCadenceBatchRecord,
    ModelMarketBoardRecord,
    ModelMarketBoardRefreshBatchRecord,
    ModelMarketBoardRefreshRecord,
    ModelMarketBoardScoringBatchRecord,
    ModelMarketBoardSourceRunRecord,
    ModelOpportunityRecord,
    ModelScoringRunRecord,
)


def _build_market_board_operations_summary(
    board: ModelMarketBoardRecord,
    *,
    source_runs: list[ModelMarketBoardSourceRunRecord],
    refresh_events: list[ModelMarketBoardRefreshRecord],
    refresh_batches: list[ModelMarketBoardRefreshBatchRecord],
    cadence_batches: list[ModelMarketBoardCadenceBatchRecord],
    scoring_runs: list[ModelScoringRunRecord],
    opportunities: list[ModelOpportunityRecord],
    batches: list[ModelMarketBoardScoringBatchRecord],
    recent_limit: int,
) -> dict[str, Any]:
    board_source_runs = [
        run
        for run in source_runs
        if run.slate_label == board.slate_label
        and run.season_label == board.season_label
        and run.target_task == board.target_task
    ]
    board_refresh_events = [
        event for event in refresh_events if event.model_market_board_id == board.id
    ]
    queue = _build_market_board_scoring_queue(
        [board],
        scoring_runs,
        source_name=None,
        freshness_status=None,
        pending_only=False,
        recent_limit=1,
    )
    queue_entry = queue["queue_entries"][0] if queue["queue_entries"] else None
    opportunity_status_counts: dict[str, int] = {}
    for opportunity in opportunities:
        opportunity_status_counts[opportunity.status] = (
            opportunity_status_counts.get(opportunity.status, 0) + 1
        )
    board_refresh_batches = [
        batch for batch in refresh_batches if _refresh_batch_includes_board(batch, board.id)
    ]
    board_cadence_batches = [
        batch for batch in cadence_batches if _cadence_batch_includes_board(batch, board.id)
    ]
    board_batches = [batch for batch in batches if _scoring_batch_includes_board(batch, board.id)]
    return {
        "board": _serialize_model_market_board(board),
        "queue_entry": queue_entry,
        "source_runs": {
            "source_run_count": len(board_source_runs),
            "latest_source_run": _serialize_model_market_board_source_run(
                board_source_runs[0] if board_source_runs else None
            ),
            "recent_source_runs": [
                _serialize_model_market_board_source_run(entry)
                for entry in board_source_runs[:recent_limit]
            ],
        },
        "refresh": {
            "refresh_event_count": len(board_refresh_events),
            "latest_refresh_event": (
                _serialize_model_market_board_refresh_event(board_refresh_events[0])
                if board_refresh_events
                else None
            ),
            "recent_refresh_events": [
                _serialize_model_market_board_refresh_event(entry)
                for entry in board_refresh_events[:recent_limit]
            ],
        },
        "refresh_orchestration": {
            "batch_count": len(board_refresh_batches),
            "latest_batch": _serialize_model_market_board_refresh_batch(
                board_refresh_batches[0] if board_refresh_batches else None
            ),
            "recent_batches": [
                _serialize_model_market_board_refresh_batch(entry)
                for entry in board_refresh_batches[:recent_limit]
            ],
        },
        "cadence": {
            "batch_count": len(board_cadence_batches),
            "latest_batch": _serialize_model_market_board_cadence_batch(
                board_cadence_batches[0] if board_cadence_batches else None
            ),
            "recent_batches": [
                _serialize_model_market_board_cadence_batch(entry)
                for entry in board_cadence_batches[:recent_limit]
            ],
        },
        "scoring": {
            "scoring_run_count": len(scoring_runs),
            "latest_scoring_run": _serialize_model_scoring_run(
                scoring_runs[0] if scoring_runs else None
            ),
            "recent_scoring_runs": [
                _serialize_model_scoring_run(entry) for entry in scoring_runs[:recent_limit]
            ],
        },
        "opportunities": {
            "opportunity_count": len(opportunities),
            "status_counts": opportunity_status_counts,
            "latest_opportunity": _serialize_model_opportunity(
                opportunities[0] if opportunities else None
            ),
            "recent_opportunities": [
                _serialize_model_opportunity(entry) for entry in opportunities[:recent_limit]
            ],
        },
        "orchestration": {
            "batch_count": len(board_batches),
            "latest_batch": _serialize_model_market_board_scoring_batch(
                board_batches[0] if board_batches else None
            ),
            "recent_batches": [
                _serialize_model_market_board_scoring_batch(entry)
                for entry in board_batches[:recent_limit]
            ],
        },
    }


def _build_market_board_cadence_dashboard(
    boards: list[ModelMarketBoardRecord],
    *,
    scoring_runs: list[ModelScoringRunRecord],
    batches: list[ModelMarketBoardScoringBatchRecord],
    source_name: str | None,
    recent_limit: int,
) -> dict[str, Any]:
    queue = _build_market_board_scoring_queue(
        boards,
        scoring_runs,
        source_name=source_name,
        freshness_status=None,
        pending_only=False,
        recent_limit=recent_limit,
    )
    cadence_entries = []
    cadence_status_counts: dict[str, int] = {}
    priority_counts: dict[str, int] = {}
    for queue_entry in queue["queue_entries"]:
        board_payload = queue_entry.get("board")
        if not isinstance(board_payload, dict):
            continue
        board_id = int(board_payload["id"])
        freshness = board_payload.get("freshness")
        freshness_status = (
            str(freshness.get("freshness_status"))
            if isinstance(freshness, dict) and freshness.get("freshness_status") is not None
            else None
        )
        latest_scoring_run = queue_entry.get("latest_scoring_run")
        latest_batch = next(
            (
                _serialize_model_market_board_scoring_batch(batch)
                for batch in batches
                if _scoring_batch_includes_board(batch, board_id)
            ),
            None,
        )
        cadence_status, priority = _resolve_market_board_cadence_status(
            queue_status=str(queue_entry.get("queue_status")),
            freshness_status=freshness_status,
            latest_scoring_run=latest_scoring_run,
        )
        cadence_status_counts[cadence_status] = cadence_status_counts.get(cadence_status, 0) + 1
        priority_counts[priority] = priority_counts.get(priority, 0) + 1
        cadence_entries.append(
            {
                "board": board_payload,
                "queue_status": queue_entry.get("queue_status"),
                "scoring_status": queue_entry.get("scoring_status"),
                "freshness_status": freshness_status,
                "cadence_status": cadence_status,
                "priority": priority,
                "latest_scoring_run": latest_scoring_run,
                "latest_orchestration_batch": latest_batch,
            }
        )
    cadence_entries.sort(
        key=lambda entry: (
            _market_board_priority_rank(str(entry["priority"])),
            _market_board_cadence_sort_key(entry),
        )
    )
    return {
        "overview": {
            "board_count": len(cadence_entries),
            "cadence_status_counts": cadence_status_counts,
            "priority_counts": priority_counts,
        },
        "cadence_entries": cadence_entries,
        "recent_cadence_entries": cadence_entries[:recent_limit],
    }


def _build_market_board_refresh_queue(
    boards: list[ModelMarketBoardRecord],
    refresh_events: list[ModelMarketBoardRefreshRecord],
    *,
    source_name: str | None,
    freshness_status: str | None,
    pending_only: bool,
    recent_limit: int,
) -> dict[str, Any]:
    queue_entries = []
    queue_status_counts: dict[str, int] = {}
    freshness_status_counts: dict[str, int] = {}
    for board in boards:
        board_payload = _serialize_model_market_board(board)
        if board_payload is None:
            continue
        board_freshness = board_payload.get("freshness")
        source_payload = board.payload.get("source")
        resolved_source_name = (
            str(source_payload.get("source_name"))
            if isinstance(source_payload, dict) and source_payload.get("source_name") is not None
            else None
        )
        resolved_freshness_status = (
            str(board_freshness.get("freshness_status"))
            if isinstance(board_freshness, dict)
            and board_freshness.get("freshness_status") is not None
            else None
        )
        if source_name is not None and resolved_source_name != source_name:
            continue
        if freshness_status is not None and resolved_freshness_status != freshness_status:
            continue
        latest_refresh_event = next(
            (
                _serialize_model_market_board_refresh_event(event)
                for event in refresh_events
                if event.model_market_board_id == board.id
            ),
            None,
        )
        queue_status, refresh_status, refreshable, needs_refresh = (
            _resolve_market_board_refresh_queue_status(
                source_name=resolved_source_name,
                freshness_status=resolved_freshness_status,
                latest_refresh_event=latest_refresh_event,
            )
        )
        if pending_only and not needs_refresh:
            continue
        queue_status_counts[queue_status] = queue_status_counts.get(queue_status, 0) + 1
        freshness_key = resolved_freshness_status or "unspecified"
        freshness_status_counts[freshness_key] = freshness_status_counts.get(freshness_key, 0) + 1
        queue_entries.append(
            {
                "board": board_payload,
                "source_name": resolved_source_name,
                "freshness_status": resolved_freshness_status,
                "queue_status": queue_status,
                "refresh_status": refresh_status,
                "refreshable": refreshable,
                "needs_refresh": needs_refresh,
                "latest_refresh_event": latest_refresh_event,
            }
        )
    queue_entries.sort(
        key=lambda entry: (
            _market_board_refresh_priority_rank(str(entry["queue_status"])),
            str(entry["board"]["board_key"]),
        )
    )
    return {
        "overview": {
            "board_count": len(queue_entries),
            "refreshable_board_count": sum(1 for entry in queue_entries if entry["refreshable"]),
            "pending_refresh_count": sum(1 for entry in queue_entries if entry["needs_refresh"]),
            "queue_status_counts": queue_status_counts,
            "freshness_status_counts": freshness_status_counts,
        },
        "queue_entries": queue_entries,
        "recent_queue_entries": queue_entries[:recent_limit],
    }


def _build_market_board_scoring_queue(
    boards: list[ModelMarketBoardRecord],
    scoring_runs: list[ModelScoringRunRecord],
    *,
    source_name: str | None,
    freshness_status: str | None,
    pending_only: bool,
    recent_limit: int,
) -> dict[str, Any]:
    scoring_runs_by_board: dict[int, list[ModelScoringRunRecord]] = {}
    for scoring_run in scoring_runs:
        if scoring_run.model_market_board_id is None:
            continue
        scoring_runs_by_board.setdefault(scoring_run.model_market_board_id, []).append(scoring_run)

    queue_entries = []
    scoring_status_counts: dict[str, int] = {}
    queue_status_counts: dict[str, int] = {}
    freshness_status_counts: dict[str, int] = {}
    pending_count = 0
    for board in boards:
        serialized_board = _serialize_model_market_board(board)
        if serialized_board is None:
            continue
        board_source = board.payload.get("source")
        board_source_name = (
            str(board_source.get("source_name"))
            if isinstance(board_source, dict) and board_source.get("source_name") is not None
            else None
        )
        if source_name is not None and board_source_name != source_name:
            continue
        board_freshness = serialized_board.get("freshness")
        resolved_freshness_status = (
            str(board_freshness.get("freshness_status"))
            if isinstance(board_freshness, dict)
            and board_freshness.get("freshness_status") is not None
            else None
        )
        if freshness_status is not None and resolved_freshness_status != freshness_status:
            continue
        board_scoring_runs = sorted(
            scoring_runs_by_board.get(board.id, []),
            key=lambda entry: (
                entry.created_at or datetime.min.replace(tzinfo=timezone.utc),
                entry.id,
            ),
            reverse=True,
        )
        latest_scoring_run = board_scoring_runs[0] if board_scoring_runs else None
        if latest_scoring_run is None:
            scoring_status = "unscored"
            needs_scoring = True
        else:
            board_updated_at = board.updated_at or board.created_at
            latest_scored_at = latest_scoring_run.created_at
            if (
                board_updated_at is not None
                and latest_scored_at is not None
                and latest_scored_at < board_updated_at
            ):
                scoring_status = "stale_after_board_update"
                needs_scoring = True
            else:
                scoring_status = "current"
                needs_scoring = False
        queue_status = "pending_score" if needs_scoring else "up_to_date"
        if pending_only and not needs_scoring:
            continue
        scoring_status_counts[scoring_status] = scoring_status_counts.get(scoring_status, 0) + 1
        queue_status_counts[queue_status] = queue_status_counts.get(queue_status, 0) + 1
        freshness_key = resolved_freshness_status or "unspecified"
        freshness_status_counts[freshness_key] = freshness_status_counts.get(freshness_key, 0) + 1
        if needs_scoring:
            pending_count += 1
        queue_entries.append(
            {
                "board": serialized_board,
                "source_name": board_source_name,
                "freshness_status": resolved_freshness_status,
                "scoring_status": scoring_status,
                "queue_status": queue_status,
                "needs_scoring": needs_scoring,
                "scoring_run_count": len(board_scoring_runs),
                "latest_scoring_run": _serialize_model_scoring_run(latest_scoring_run),
            }
        )
    return {
        "overview": {
            "board_count": len(queue_entries),
            "pending_board_count": pending_count,
            "scoring_status_counts": scoring_status_counts,
            "queue_status_counts": queue_status_counts,
            "freshness_status_counts": freshness_status_counts,
        },
        "queue_entries": queue_entries,
        "recent_queue_entries": queue_entries[:recent_limit],
    }


def _summarize_model_market_board_scoring_batch_history(
    batches: list[ModelMarketBoardScoringBatchRecord],
    *,
    recent_limit: int,
) -> dict[str, Any]:
    total_candidate_board_count = 0
    total_scored_board_count = 0
    total_scoring_run_count = 0
    total_opportunity_count = 0
    source_counts: dict[str, int] = {}
    freshness_counts: dict[str, int] = {}
    daily_buckets: dict[str, dict[str, Any]] = {}
    for batch in batches:
        total_candidate_board_count += batch.candidate_board_count
        total_scored_board_count += batch.scored_board_count
        total_scoring_run_count += batch.materialized_scoring_run_count
        total_opportunity_count += batch.materialized_opportunity_count
        source_key = batch.source_name or "unspecified"
        source_counts[source_key] = source_counts.get(source_key, 0) + 1
        freshness_key = batch.freshness_status or "unspecified"
        freshness_counts[freshness_key] = freshness_counts.get(freshness_key, 0) + 1
        if batch.created_at is None:
            continue
        day_key = batch.created_at.date().isoformat()
        bucket = daily_buckets.setdefault(
            day_key,
            {
                "date": day_key,
                "batch_count": 0,
                "candidate_board_count": 0,
                "scored_board_count": 0,
                "materialized_scoring_run_count": 0,
                "materialized_opportunity_count": 0,
            },
        )
        bucket["batch_count"] += 1
        bucket["candidate_board_count"] += batch.candidate_board_count
        bucket["scored_board_count"] += batch.scored_board_count
        bucket["materialized_scoring_run_count"] += batch.materialized_scoring_run_count
        bucket["materialized_opportunity_count"] += batch.materialized_opportunity_count
    return {
        "overview": {
            "batch_count": len(batches),
            "candidate_board_count": total_candidate_board_count,
            "scored_board_count": total_scored_board_count,
            "materialized_scoring_run_count": total_scoring_run_count,
            "materialized_opportunity_count": total_opportunity_count,
            "source_counts": source_counts,
            "freshness_status_counts": freshness_counts,
            "latest_batch": _serialize_model_market_board_scoring_batch(
                batches[0] if batches else None
            ),
        },
        "daily_buckets": [daily_buckets[key] for key in sorted(daily_buckets.keys())],
        "recent_batches": [
            _serialize_model_market_board_scoring_batch(entry) for entry in batches[:recent_limit]
        ],
    }


def _summarize_model_market_board_refresh_batch_history(
    batches: list[ModelMarketBoardRefreshBatchRecord],
    *,
    recent_limit: int,
) -> dict[str, Any]:
    total_candidate_board_count = 0
    total_refreshed_board_count = 0
    total_created_board_count = 0
    total_updated_board_count = 0
    total_unchanged_board_count = 0
    source_counts: dict[str, int] = {}
    freshness_counts: dict[str, int] = {}
    daily_buckets: dict[str, dict[str, Any]] = {}
    for batch in batches:
        total_candidate_board_count += batch.candidate_board_count
        total_refreshed_board_count += batch.refreshed_board_count
        total_created_board_count += batch.created_board_count
        total_updated_board_count += batch.updated_board_count
        total_unchanged_board_count += batch.unchanged_board_count
        source_key = batch.source_name or "unspecified"
        source_counts[source_key] = source_counts.get(source_key, 0) + 1
        freshness_key = batch.freshness_status or "unspecified"
        freshness_counts[freshness_key] = freshness_counts.get(freshness_key, 0) + 1
        if batch.created_at is None:
            continue
        day_key = batch.created_at.date().isoformat()
        bucket = daily_buckets.setdefault(
            day_key,
            {
                "date": day_key,
                "batch_count": 0,
                "candidate_board_count": 0,
                "refreshed_board_count": 0,
                "created_board_count": 0,
                "updated_board_count": 0,
                "unchanged_board_count": 0,
            },
        )
        bucket["batch_count"] += 1
        bucket["candidate_board_count"] += batch.candidate_board_count
        bucket["refreshed_board_count"] += batch.refreshed_board_count
        bucket["created_board_count"] += batch.created_board_count
        bucket["updated_board_count"] += batch.updated_board_count
        bucket["unchanged_board_count"] += batch.unchanged_board_count
    return {
        "overview": {
            "batch_count": len(batches),
            "candidate_board_count": total_candidate_board_count,
            "refreshed_board_count": total_refreshed_board_count,
            "created_board_count": total_created_board_count,
            "updated_board_count": total_updated_board_count,
            "unchanged_board_count": total_unchanged_board_count,
            "source_counts": source_counts,
            "freshness_status_counts": freshness_counts,
            "latest_batch": _serialize_model_market_board_refresh_batch(
                batches[0] if batches else None
            ),
        },
        "daily_buckets": [daily_buckets[key] for key in sorted(daily_buckets.keys())],
        "recent_batches": [
            _serialize_model_market_board_refresh_batch(entry) for entry in batches[:recent_limit]
        ],
    }


def _summarize_model_market_board_cadence_batch_history(
    batches: list[ModelMarketBoardCadenceBatchRecord],
    *,
    recent_limit: int,
) -> dict[str, Any]:
    total_refreshed_board_count = 0
    total_scored_board_count = 0
    total_scoring_run_count = 0
    total_opportunity_count = 0
    source_counts: dict[str, int] = {}
    daily_buckets: dict[str, dict[str, Any]] = {}
    for batch in batches:
        total_refreshed_board_count += batch.refreshed_board_count
        total_scored_board_count += batch.scored_board_count
        total_scoring_run_count += batch.materialized_scoring_run_count
        total_opportunity_count += batch.materialized_opportunity_count
        source_key = batch.source_name or "unspecified"
        source_counts[source_key] = source_counts.get(source_key, 0) + 1
        if batch.created_at is None:
            continue
        day_key = batch.created_at.date().isoformat()
        bucket = daily_buckets.setdefault(
            day_key,
            {
                "date": day_key,
                "batch_count": 0,
                "refreshed_board_count": 0,
                "scored_board_count": 0,
                "materialized_scoring_run_count": 0,
                "materialized_opportunity_count": 0,
            },
        )
        bucket["batch_count"] += 1
        bucket["refreshed_board_count"] += batch.refreshed_board_count
        bucket["scored_board_count"] += batch.scored_board_count
        bucket["materialized_scoring_run_count"] += batch.materialized_scoring_run_count
        bucket["materialized_opportunity_count"] += batch.materialized_opportunity_count
    return {
        "overview": {
            "batch_count": len(batches),
            "refreshed_board_count": total_refreshed_board_count,
            "scored_board_count": total_scored_board_count,
            "materialized_scoring_run_count": total_scoring_run_count,
            "materialized_opportunity_count": total_opportunity_count,
            "source_counts": source_counts,
            "latest_batch": _serialize_model_market_board_cadence_batch(
                batches[0] if batches else None
            ),
        },
        "daily_buckets": [daily_buckets[key] for key in sorted(daily_buckets.keys())],
        "recent_batches": [
            _serialize_model_market_board_cadence_batch(entry) for entry in batches[:recent_limit]
        ],
    }


def _scoring_batch_includes_board(
    batch: ModelMarketBoardScoringBatchRecord,
    board_id: int,
) -> bool:
    orchestration_runs = batch.payload.get("orchestration_runs", [])
    for entry in orchestration_runs:
        if not isinstance(entry, dict):
            continue
        board_payload = entry.get("board")
        if isinstance(board_payload, dict) and int(board_payload.get("id", 0)) == board_id:
            return True
    return False


def _refresh_batch_includes_board(
    batch: ModelMarketBoardRefreshBatchRecord,
    board_id: int,
) -> bool:
    refresh_runs = batch.payload.get("refresh_runs", [])
    for entry in refresh_runs:
        if not isinstance(entry, dict):
            continue
        board_payload = entry.get("board")
        if isinstance(board_payload, dict) and int(board_payload.get("id", 0)) == board_id:
            return True
    return False


def _cadence_batch_includes_board(
    batch: ModelMarketBoardCadenceBatchRecord,
    board_id: int,
) -> bool:
    refresh_runs = batch.payload.get("refresh_result", {}).get("refresh_runs", [])
    for entry in refresh_runs:
        if not isinstance(entry, dict):
            continue
        board_payload = entry.get("board")
        if isinstance(board_payload, dict) and int(board_payload.get("id", 0)) == board_id:
            return True
    orchestration_runs = batch.payload.get("scoring_result", {}).get("orchestration_runs", [])
    for entry in orchestration_runs:
        if not isinstance(entry, dict):
            continue
        board_payload = entry.get("board")
        if isinstance(board_payload, dict) and int(board_payload.get("id", 0)) == board_id:
            return True
    return False


def _build_market_board_refresh_orchestration_result(
    *,
    queue_before: dict[str, Any],
    queue_after: dict[str, Any],
    refresh_runs: list[dict[str, Any]],
) -> dict[str, Any]:
    status_counts = {"created": 0, "updated": 0, "unchanged": 0}
    for entry in refresh_runs:
        status = str(entry.get("refresh_result_status") or "unchanged")
        if status not in status_counts:
            status_counts[status] = 0
        status_counts[status] += 1
    return {
        "queue_before": queue_before,
        "queue_after": queue_after,
        "candidate_board_count": len(refresh_runs),
        "refreshed_board_count": len(refresh_runs),
        "created_board_count": int(status_counts.get("created", 0)),
        "updated_board_count": int(status_counts.get("updated", 0)),
        "unchanged_board_count": int(status_counts.get("unchanged", 0)),
        "refresh_runs": refresh_runs,
    }


def _build_market_board_cadence_result(
    *,
    refresh_result: dict[str, Any],
    scoring_result: dict[str, Any],
) -> dict[str, Any]:
    return {
        "refresh_result": refresh_result,
        "scoring_result": scoring_result,
        "refreshed_board_count": int(refresh_result.get("refreshed_board_count", 0)),
        "scored_board_count": int(scoring_result.get("scored_board_count", 0)),
        "materialized_scoring_run_count": int(
            scoring_result.get("materialized_scoring_run_count", 0)
        ),
        "materialized_opportunity_count": int(
            scoring_result.get("materialized_opportunity_count", 0)
        ),
    }


def _resolve_market_board_refresh_game_date(board_payload: dict[str, Any]) -> date:
    freshness = board_payload.get("freshness")
    refresh_target_date = (
        freshness.get("refresh_target_date") if isinstance(freshness, dict) else None
    )
    if isinstance(refresh_target_date, str):
        return date.fromisoformat(refresh_target_date)
    game_date_start = board_payload.get("game_date_start")
    if isinstance(game_date_start, str):
        return date.fromisoformat(game_date_start)
    return _utc_today()


def _resolve_market_board_refresh_queue_status(
    *,
    source_name: str | None,
    freshness_status: str | None,
    latest_refresh_event: dict[str, Any] | None,
) -> tuple[str, str, bool, bool]:
    if source_name is None:
        return ("manual_only", "manual_only", False, False)
    if source_name not in MARKET_BOARD_SOURCE_CONFIGS:
        return ("unsupported_source", "unsupported_source", False, False)
    if freshness_status is None:
        refresh_status = (
            str(latest_refresh_event.get("refresh_status"))
            if latest_refresh_event is not None
            else "never_refreshed"
        )
        return ("pending_refresh", refresh_status, True, True)
    if freshness_status == "stale":
        return ("pending_refresh", "stale", True, True)
    if freshness_status == "aging":
        return ("monitor_refresh", "aging", True, False)
    return ("up_to_date", "current", True, False)


def _resolve_market_board_cadence_status(
    *,
    queue_status: str,
    freshness_status: str | None,
    latest_scoring_run: dict[str, Any] | None,
) -> tuple[str, str]:
    if freshness_status == "stale":
        return ("needs_refresh", "high")
    if queue_status == "pending_score":
        if freshness_status == "fresh":
            return ("ready_to_score", "high")
        return ("needs_scoring_attention", "medium")
    if latest_scoring_run is None:
        return ("awaiting_first_score", "medium")
    created_at = latest_scoring_run.get("created_at")
    if isinstance(created_at, str):
        scored_at = datetime.fromisoformat(created_at)
        if scored_at.astimezone(timezone.utc).date() == _utc_today():
            return ("recently_scored", "low")
    return ("monitoring", "low")


def _market_board_priority_rank(priority: str) -> int:
    if priority == "high":
        return 0
    if priority == "medium":
        return 1
    return 2


def _market_board_refresh_priority_rank(queue_status: str) -> int:
    return {
        "pending_refresh": 0,
        "monitor_refresh": 1,
        "up_to_date": 2,
        "manual_only": 3,
        "unsupported_source": 4,
    }.get(queue_status, 5)


def _market_board_cadence_sort_key(entry: dict[str, Any]) -> tuple[int, str]:
    board = entry.get("board")
    board_key = str(board.get("board_key")) if isinstance(board, dict) else ""
    return (_market_board_priority_rank(str(entry.get("priority"))), board_key)


def _serialize_model_scoring_run(
    scoring_run: ModelScoringRunRecord | None,
) -> dict[str, Any] | None:
    if scoring_run is None:
        return None
    return {
        "id": scoring_run.id,
        "model_market_board_id": scoring_run.model_market_board_id,
        "model_selection_snapshot_id": scoring_run.model_selection_snapshot_id,
        "model_evaluation_snapshot_id": scoring_run.model_evaluation_snapshot_id,
        "feature_version_id": scoring_run.feature_version_id,
        "target_task": scoring_run.target_task,
        "scenario_key": scoring_run.scenario_key,
        "season_label": scoring_run.season_label,
        "game_date": scoring_run.game_date.isoformat() if scoring_run.game_date else None,
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
        "created_at": scoring_run.created_at.isoformat() if scoring_run.created_at else None,
    }


def _serialize_model_opportunity(
    opportunity: ModelOpportunityRecord | None,
) -> dict[str, Any] | None:
    if opportunity is None:
        return None
    return {
        "id": opportunity.id,
        "model_scoring_run_id": opportunity.model_scoring_run_id,
        "model_selection_snapshot_id": opportunity.model_selection_snapshot_id,
        "model_evaluation_snapshot_id": opportunity.model_evaluation_snapshot_id,
        "feature_version_id": opportunity.feature_version_id,
        "target_task": opportunity.target_task,
        "source_kind": opportunity.source_kind,
        "scenario_key": opportunity.scenario_key,
        "opportunity_key": opportunity.opportunity_key,
        "team_code": opportunity.team_code,
        "opponent_code": opportunity.opponent_code,
        "season_label": opportunity.season_label,
        "canonical_game_id": opportunity.canonical_game_id,
        "game_date": opportunity.game_date.isoformat() if opportunity.game_date else None,
        "policy_name": opportunity.policy_name,
        "status": opportunity.status,
        "prediction_value": opportunity.prediction_value,
        "signal_strength": opportunity.signal_strength,
        "evidence_rating": opportunity.evidence_rating,
        "recommendation_status": opportunity.recommendation_status,
        "payload": opportunity.payload,
        "created_at": opportunity.created_at.isoformat() if opportunity.created_at else None,
        "updated_at": opportunity.updated_at.isoformat() if opportunity.updated_at else None,
    }


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()
