from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timezone
from typing import Any

from bookmaker_detector_api.repositories import MarketBoardOperationStore
from bookmaker_detector_api.repositories.ingestion_json import _json_dumps
from bookmaker_detector_api.services.model_records import (
    ModelMarketBoardCadenceBatchRecord,
    ModelMarketBoardRecord,
    ModelMarketBoardRefreshBatchRecord,
    ModelMarketBoardRefreshRecord,
    ModelMarketBoardScoringBatchRecord,
    ModelMarketBoardSourceRunRecord,
)


def list_model_market_board_source_runs_in_memory(
    repository: MarketBoardOperationStore,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    season_label: str | None = None,
    recent_limit: int | None = None,
) -> list[ModelMarketBoardSourceRunRecord]:
    selected = [
        ModelMarketBoardSourceRunRecord(**entry)
        for entry in repository.model_market_board_source_runs
        if (target_task is None or entry["target_task"] == target_task)
        and (source_name is None or entry["source_name"] == source_name)
        and (season_label is None or entry["season_label"] == season_label)
    ]
    ordered = sorted(
        selected,
        key=lambda entry: (
            entry.created_at or datetime.min.replace(tzinfo=timezone.utc),
            entry.id,
        ),
        reverse=True,
    )
    if recent_limit is None:
        return ordered
    return ordered[:recent_limit]


def list_model_market_board_source_runs_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    season_label: str | None = None,
    recent_limit: int | None = None,
) -> list[ModelMarketBoardSourceRunRecord]:
    query = """
        SELECT
            id,
            source_name,
            target_task,
            season_label,
            game_date,
            slate_label,
            requested_game_count,
            generated_game_count,
            status,
            payload_json,
            created_at
        FROM model_market_board_source_run
        WHERE 1=1
    """
    params: list[Any] = []
    if target_task is not None:
        query += " AND target_task = %s"
        params.append(target_task)
    if source_name is not None:
        query += " AND source_name = %s"
        params.append(source_name)
    if season_label is not None:
        query += " AND season_label = %s"
        params.append(season_label)
    query += " ORDER BY created_at DESC, id DESC"
    if recent_limit is not None:
        query += " LIMIT %s"
        params.append(recent_limit)
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelMarketBoardSourceRunRecord(
            id=int(row[0]),
            source_name=row[1],
            target_task=row[2],
            season_label=row[3],
            game_date=row[4],
            slate_label=row[5],
            requested_game_count=int(row[6]),
            generated_game_count=int(row[7]),
            status=row[8],
            payload=row[9],
            created_at=row[10],
        )
        for row in rows
    ]


def list_model_market_boards_in_memory(
    repository: MarketBoardOperationStore,
    *,
    target_task: str | None = None,
    season_label: str | None = None,
) -> list[ModelMarketBoardRecord]:
    selected = [
        ModelMarketBoardRecord(**entry)
        for entry in repository.model_market_boards
        if (target_task is None or entry["target_task"] == target_task)
        and (season_label is None or entry["season_label"] == season_label)
    ]
    return sorted(
        selected,
        key=lambda entry: (
            entry.created_at or datetime.min.replace(tzinfo=timezone.utc),
            entry.id,
        ),
        reverse=True,
    )


def list_model_market_boards_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    season_label: str | None = None,
) -> list[ModelMarketBoardRecord]:
    query = """
        SELECT
            id,
            board_key,
            slate_label,
            target_task,
            season_label,
            game_count,
            game_date_start,
            game_date_end,
            payload_json,
            created_at,
            updated_at
        FROM model_market_board
        WHERE 1 = 1
    """
    params: list[Any] = []
    if target_task is not None:
        query += " AND target_task = %s"
        params.append(target_task)
    if season_label is not None:
        query += " AND season_label = %s"
        params.append(season_label)
    query += " ORDER BY created_at DESC, id DESC"
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelMarketBoardRecord(
            id=int(row[0]),
            board_key=row[1],
            slate_label=row[2],
            target_task=row[3],
            season_label=row[4],
            game_count=int(row[5]),
            game_date_start=row[6],
            game_date_end=row[7],
            payload=row[8],
            created_at=row[9],
            updated_at=row[10],
        )
        for row in rows
    ]


def list_model_market_board_refresh_events_in_memory(
    repository: MarketBoardOperationStore,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int | None = None,
) -> list[ModelMarketBoardRefreshRecord]:
    selected = [
        ModelMarketBoardRefreshRecord(**entry)
        for entry in repository.model_market_board_refresh_events
        if (target_task is None or entry["target_task"] == target_task)
        and (source_name is None or entry["source_name"] == source_name)
    ]
    sorted_events = sorted(
        selected,
        key=lambda entry: (
            entry.created_at or datetime.min.replace(tzinfo=timezone.utc),
            entry.id,
        ),
        reverse=True,
    )
    return sorted_events[:recent_limit] if recent_limit is not None else sorted_events


def list_model_market_board_refresh_events_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int | None = None,
) -> list[ModelMarketBoardRefreshRecord]:
    query = """
        SELECT
            id,
            model_market_board_id,
            board_key,
            target_task,
            source_name,
            refresh_status,
            game_count,
            payload_json,
            created_at
        FROM model_market_board_refresh_event
        WHERE 1 = 1
    """
    params: list[Any] = []
    if target_task is not None:
        query += " AND target_task = %s"
        params.append(target_task)
    if source_name is not None:
        query += " AND source_name = %s"
        params.append(source_name)
    query += " ORDER BY created_at DESC, id DESC"
    if recent_limit is not None:
        query += " LIMIT %s"
        params.append(recent_limit)
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelMarketBoardRefreshRecord(
            id=int(row[0]),
            model_market_board_id=int(row[1]),
            board_key=row[2],
            target_task=row[3],
            source_name=row[4],
            refresh_status=row[5],
            game_count=int(row[6]),
            payload=row[7],
            created_at=row[8],
        )
        for row in rows
    ]


def list_model_market_board_refresh_batches_in_memory(
    repository: MarketBoardOperationStore,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int | None = None,
) -> list[ModelMarketBoardRefreshBatchRecord]:
    selected = [
        ModelMarketBoardRefreshBatchRecord(**entry)
        for entry in repository.model_market_board_refresh_batches
        if (target_task is None or entry["target_task"] == target_task)
        and (source_name is None or entry.get("source_name") == source_name)
    ]
    ordered = sorted(
        selected,
        key=lambda entry: (
            entry.created_at or datetime.min.replace(tzinfo=timezone.utc),
            entry.id,
        ),
        reverse=True,
    )
    if recent_limit is None:
        return ordered
    return ordered[:recent_limit]


def list_model_market_board_refresh_batches_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int | None = None,
) -> list[ModelMarketBoardRefreshBatchRecord]:
    query = """
        SELECT
            id,
            target_task,
            source_name,
            season_label,
            freshness_status,
            pending_only,
            candidate_board_count,
            refreshed_board_count,
            created_board_count,
            updated_board_count,
            unchanged_board_count,
            payload_json,
            created_at
        FROM model_market_board_refresh_batch
        WHERE 1=1
    """
    params: list[Any] = []
    if target_task is not None:
        query += " AND target_task = %s"
        params.append(target_task)
    if source_name is not None:
        query += " AND source_name = %s"
        params.append(source_name)
    query += " ORDER BY created_at DESC, id DESC"
    if recent_limit is not None:
        query += " LIMIT %s"
        params.append(recent_limit)
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelMarketBoardRefreshBatchRecord(
            id=int(row[0]),
            target_task=row[1],
            source_name=row[2],
            season_label=row[3],
            freshness_status=row[4],
            pending_only=bool(row[5]),
            candidate_board_count=int(row[6]),
            refreshed_board_count=int(row[7]),
            created_board_count=int(row[8]),
            updated_board_count=int(row[9]),
            unchanged_board_count=int(row[10]),
            payload=row[11],
            created_at=row[12],
        )
        for row in rows
    ]


def list_model_market_board_scoring_batches_in_memory(
    repository: MarketBoardOperationStore,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int | None = None,
) -> list[ModelMarketBoardScoringBatchRecord]:
    selected = [
        ModelMarketBoardScoringBatchRecord(**entry)
        for entry in repository.model_market_board_scoring_batches
        if (target_task is None or entry["target_task"] == target_task)
        and (source_name is None or entry.get("source_name") == source_name)
    ]
    ordered = sorted(
        selected,
        key=lambda entry: (
            entry.created_at or datetime.min.replace(tzinfo=timezone.utc),
            entry.id,
        ),
        reverse=True,
    )
    if recent_limit is None:
        return ordered
    return ordered[:recent_limit]


def list_model_market_board_scoring_batches_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int | None = None,
) -> list[ModelMarketBoardScoringBatchRecord]:
    query = """
        SELECT
            id,
            target_task,
            source_name,
            season_label,
            freshness_status,
            pending_only,
            candidate_board_count,
            scored_board_count,
            materialized_scoring_run_count,
            materialized_opportunity_count,
            payload_json,
            created_at
        FROM model_market_board_scoring_batch
        WHERE 1=1
    """
    params: list[Any] = []
    if target_task is not None:
        query += " AND target_task = %s"
        params.append(target_task)
    if source_name is not None:
        query += " AND source_name = %s"
        params.append(source_name)
    query += " ORDER BY created_at DESC, id DESC"
    if recent_limit is not None:
        query += " LIMIT %s"
        params.append(recent_limit)
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelMarketBoardScoringBatchRecord(
            id=int(row[0]),
            target_task=row[1],
            source_name=row[2],
            season_label=row[3],
            freshness_status=row[4],
            pending_only=bool(row[5]),
            candidate_board_count=int(row[6]),
            scored_board_count=int(row[7]),
            materialized_scoring_run_count=int(row[8]),
            materialized_opportunity_count=int(row[9]),
            payload=row[10],
            created_at=row[11],
        )
        for row in rows
    ]


def list_model_market_board_cadence_batches_in_memory(
    repository: MarketBoardOperationStore,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int | None = None,
) -> list[ModelMarketBoardCadenceBatchRecord]:
    selected = [
        ModelMarketBoardCadenceBatchRecord(**entry)
        for entry in repository.model_market_board_cadence_batches
        if (target_task is None or entry["target_task"] == target_task)
        and (source_name is None or entry.get("source_name") == source_name)
    ]
    ordered = sorted(
        selected,
        key=lambda entry: (
            entry.created_at or datetime.min.replace(tzinfo=timezone.utc),
            entry.id,
        ),
        reverse=True,
    )
    if recent_limit is None:
        return ordered
    return ordered[:recent_limit]


def list_model_market_board_cadence_batches_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    source_name: str | None = None,
    recent_limit: int | None = None,
) -> list[ModelMarketBoardCadenceBatchRecord]:
    query = """
        SELECT
            id,
            target_task,
            source_name,
            season_label,
            refresh_freshness_status,
            scoring_freshness_status,
            refreshed_board_count,
            scored_board_count,
            materialized_scoring_run_count,
            materialized_opportunity_count,
            payload_json,
            created_at
        FROM model_market_board_cadence_batch
        WHERE 1=1
    """
    params: list[Any] = []
    if target_task is not None:
        query += " AND target_task = %s"
        params.append(target_task)
    if source_name is not None:
        query += " AND source_name = %s"
        params.append(source_name)
    query += " ORDER BY created_at DESC, id DESC"
    if recent_limit is not None:
        query += " LIMIT %s"
        params.append(recent_limit)
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelMarketBoardCadenceBatchRecord(
            id=int(row[0]),
            target_task=row[1],
            source_name=row[2],
            season_label=row[3],
            refresh_freshness_status=row[4],
            scoring_freshness_status=row[5],
            refreshed_board_count=int(row[6]),
            scored_board_count=int(row[7]),
            materialized_scoring_run_count=int(row[8]),
            materialized_opportunity_count=int(row[9]),
            payload=row[10],
            created_at=row[11],
        )
        for row in rows
    ]


def _find_model_market_board_in_memory(
    repository: MarketBoardOperationStore,
    *,
    board_key: str,
) -> ModelMarketBoardRecord | None:
    match = next(
        (entry for entry in repository.model_market_boards if entry["board_key"] == board_key),
        None,
    )
    return ModelMarketBoardRecord(**match) if match is not None else None


def _find_model_market_board_postgres(
    connection: Any,
    *,
    board_key: str,
) -> ModelMarketBoardRecord | None:
    matches = [
        entry
        for entry in list_model_market_boards_postgres(connection)
        if entry.board_key == board_key
    ]
    return matches[0] if matches else None


def _resolve_market_board_refresh_status(
    *,
    existing_board: ModelMarketBoardRecord | None,
    generated_games: list[dict[str, Any]],
) -> str:
    if existing_board is None:
        return "created"
    previous_games = list(existing_board.payload.get("games", []))
    normalized_generated_games = [
        _serialize_future_game_input_value(game) for game in generated_games
    ]
    return "unchanged" if previous_games == normalized_generated_games else "updated"


def _build_market_board_refresh_change_summary(
    *,
    existing_board: ModelMarketBoardRecord | None,
    generated_games: list[dict[str, Any]],
) -> dict[str, Any]:
    previous_games = (
        list(existing_board.payload.get("games", [])) if existing_board is not None else []
    )
    normalized_generated_games = [
        _serialize_future_game_input_value(game) for game in generated_games
    ]
    previous_by_key = {
        _build_market_board_game_key(entry): entry for entry in previous_games
    }
    generated_by_key = {
        _build_market_board_game_key(entry): entry for entry in normalized_generated_games
    }
    added_keys = sorted(set(generated_by_key) - set(previous_by_key))
    removed_keys = sorted(set(previous_by_key) - set(generated_by_key))
    common_keys = sorted(set(previous_by_key) & set(generated_by_key))
    changed_games = []
    unchanged_count = 0
    for game_key in common_keys:
        previous = previous_by_key[game_key]
        current = generated_by_key[game_key]
        if previous == current:
            unchanged_count += 1
            continue
        changed_fields = {}
        for field_name in ("home_spread_line", "total_line"):
            if previous.get(field_name) != current.get(field_name):
                changed_fields[field_name] = {
                    "previous": previous.get(field_name),
                    "current": current.get(field_name),
                }
        changed_games.append(
            {
                "game_key": game_key,
                "previous": previous,
                "current": current,
                "changed_fields": changed_fields,
            }
        )
    return {
        "previous_game_count": len(previous_games),
        "generated_game_count": len(normalized_generated_games),
        "added_game_count": len(added_keys),
        "removed_game_count": len(removed_keys),
        "changed_game_count": len(changed_games),
        "unchanged_game_count": unchanged_count,
        "added_game_keys": added_keys,
        "removed_game_keys": removed_keys,
        "changed_games": changed_games,
    }


def _build_market_board_game_key(game: dict[str, Any]) -> str:
    return (
        f"{game['season_label']}:{game['game_date']}:"
        f"{game['home_team_code']}:{game['away_team_code']}"
    )


def _resolve_market_board_refresh_count(
    existing_board: ModelMarketBoardRecord | None,
) -> int:
    if existing_board is None:
        return 0
    source_payload = existing_board.payload.get("source", {})
    return int(source_payload.get("refresh_count", 0))


def _build_model_market_board(
    *,
    target_task: str,
    games: list[dict[str, Any]],
    slate_label: str | None,
) -> ModelMarketBoardRecord:
    serialized_games = [_serialize_future_game_input_value(game) for game in games]
    game_dates = sorted(entry["game_date"] for entry in serialized_games)
    season_labels = sorted({entry["season_label"] for entry in serialized_games})
    return ModelMarketBoardRecord(
        id=0,
        board_key=_build_future_slate_key_value(
            target_task=target_task,
            slate_label=slate_label,
            serialized_inputs=serialized_games,
        ),
        slate_label=slate_label,
        target_task=target_task,
        season_label=season_labels[0] if len(season_labels) == 1 else None,
        game_count=len(serialized_games),
        game_date_start=(_coerce_date_value(game_dates[0]) if game_dates else None),
        game_date_end=(_coerce_date_value(game_dates[-1]) if game_dates else None),
        payload={
            "games": serialized_games,
            "season_labels": season_labels,
        },
    )


def save_model_market_board_in_memory(
    repository: MarketBoardOperationStore,
    board: ModelMarketBoardRecord,
) -> ModelMarketBoardRecord:
    payload = asdict(board)
    existing_index = next(
        (
            index
            for index, entry in enumerate(repository.model_market_boards)
            if entry["board_key"] == board.board_key
        ),
        None,
    )
    now = datetime.now(timezone.utc)
    if existing_index is None:
        payload["id"] = len(repository.model_market_boards) + 1
        payload["created_at"] = now
        payload["updated_at"] = now
        repository.model_market_boards.append(payload)
        return ModelMarketBoardRecord(**payload)
    current = repository.model_market_boards[existing_index]
    payload["id"] = current["id"]
    payload["created_at"] = current["created_at"]
    payload["updated_at"] = now
    repository.model_market_boards[existing_index] = payload
    return ModelMarketBoardRecord(**payload)


def save_model_market_board_postgres(
    connection: Any,
    board: ModelMarketBoardRecord,
) -> ModelMarketBoardRecord:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO model_market_board (
                board_key,
                slate_label,
                target_task,
                season_label,
                game_count,
                game_date_start,
                game_date_end,
                payload_json,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
            ON CONFLICT (board_key)
            DO UPDATE SET
                slate_label = EXCLUDED.slate_label,
                target_task = EXCLUDED.target_task,
                season_label = EXCLUDED.season_label,
                game_count = EXCLUDED.game_count,
                game_date_start = EXCLUDED.game_date_start,
                game_date_end = EXCLUDED.game_date_end,
                payload_json = EXCLUDED.payload_json,
                updated_at = NOW()
            RETURNING
                id,
                board_key,
                slate_label,
                target_task,
                season_label,
                game_count,
                game_date_start,
                game_date_end,
                payload_json,
                created_at,
                updated_at
            """,
            (
                board.board_key,
                board.slate_label,
                board.target_task,
                board.season_label,
                board.game_count,
                board.game_date_start,
                board.game_date_end,
                _json_dumps(board.payload),
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return ModelMarketBoardRecord(
        id=int(row[0]),
        board_key=row[1],
        slate_label=row[2],
        target_task=row[3],
        season_label=row[4],
        game_count=int(row[5]),
        game_date_start=row[6],
        game_date_end=row[7],
        payload=row[8],
        created_at=row[9],
        updated_at=row[10],
    )


def save_model_market_board_refresh_event_in_memory(
    repository: MarketBoardOperationStore,
    refresh_event: ModelMarketBoardRefreshRecord,
) -> ModelMarketBoardRefreshRecord:
    payload = asdict(refresh_event)
    payload["id"] = len(repository.model_market_board_refresh_events) + 1
    payload["created_at"] = datetime.now(timezone.utc)
    repository.model_market_board_refresh_events.append(payload)
    return ModelMarketBoardRefreshRecord(**payload)


def save_model_market_board_refresh_event_postgres(
    connection: Any,
    refresh_event: ModelMarketBoardRefreshRecord,
) -> ModelMarketBoardRefreshRecord:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO model_market_board_refresh_event (
                model_market_board_id,
                board_key,
                target_task,
                source_name,
                refresh_status,
                game_count,
                payload_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
            RETURNING id, created_at
            """,
            (
                refresh_event.model_market_board_id,
                refresh_event.board_key,
                refresh_event.target_task,
                refresh_event.source_name,
                refresh_event.refresh_status,
                refresh_event.game_count,
                _json_dumps(refresh_event.payload),
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return ModelMarketBoardRefreshRecord(
        id=int(row[0]),
        model_market_board_id=refresh_event.model_market_board_id,
        board_key=refresh_event.board_key,
        target_task=refresh_event.target_task,
        source_name=refresh_event.source_name,
        refresh_status=refresh_event.refresh_status,
        game_count=refresh_event.game_count,
        payload=refresh_event.payload,
        created_at=row[1],
    )


def _serialize_model_market_board(
    board: ModelMarketBoardRecord | None,
) -> dict[str, Any] | None:
    if board is None:
        return None
    freshness = _build_market_board_freshness(board)
    return {
        "id": board.id,
        "board_key": board.board_key,
        "slate_label": board.slate_label,
        "target_task": board.target_task,
        "season_label": board.season_label,
        "game_count": board.game_count,
        "game_date_start": board.game_date_start.isoformat() if board.game_date_start else None,
        "game_date_end": board.game_date_end.isoformat() if board.game_date_end else None,
        "payload": board.payload,
        "freshness": freshness,
        "created_at": board.created_at.isoformat() if board.created_at else None,
        "updated_at": board.updated_at.isoformat() if board.updated_at else None,
    }


def _serialize_model_market_board_refresh_event(
    refresh_event: ModelMarketBoardRefreshRecord,
) -> dict[str, Any]:
    return {
        "id": refresh_event.id,
        "model_market_board_id": refresh_event.model_market_board_id,
        "board_key": refresh_event.board_key,
        "target_task": refresh_event.target_task,
        "source_name": refresh_event.source_name,
        "refresh_status": refresh_event.refresh_status,
        "game_count": refresh_event.game_count,
        "payload": refresh_event.payload,
        "created_at": refresh_event.created_at.isoformat() if refresh_event.created_at else None,
    }


def _serialize_model_market_board_source_run(
    source_run: ModelMarketBoardSourceRunRecord | None,
) -> dict[str, Any] | None:
    if source_run is None:
        return None
    return {
        "id": source_run.id,
        "source_name": source_run.source_name,
        "target_task": source_run.target_task,
        "season_label": source_run.season_label,
        "game_date": source_run.game_date.isoformat(),
        "slate_label": source_run.slate_label,
        "requested_game_count": source_run.requested_game_count,
        "generated_game_count": source_run.generated_game_count,
        "status": source_run.status,
        "payload": source_run.payload,
        "created_at": source_run.created_at.isoformat() if source_run.created_at else None,
    }


def _serialize_model_market_board_refresh_batch(
    batch: ModelMarketBoardRefreshBatchRecord | None,
) -> dict[str, Any] | None:
    if batch is None:
        return None
    return {
        "id": batch.id,
        "target_task": batch.target_task,
        "source_name": batch.source_name,
        "season_label": batch.season_label,
        "freshness_status": batch.freshness_status,
        "pending_only": batch.pending_only,
        "candidate_board_count": batch.candidate_board_count,
        "refreshed_board_count": batch.refreshed_board_count,
        "created_board_count": batch.created_board_count,
        "updated_board_count": batch.updated_board_count,
        "unchanged_board_count": batch.unchanged_board_count,
        "payload": batch.payload,
        "created_at": batch.created_at.isoformat() if batch.created_at else None,
    }


def _serialize_model_market_board_scoring_batch(
    batch: ModelMarketBoardScoringBatchRecord | None,
) -> dict[str, Any] | None:
    if batch is None:
        return None
    return {
        "id": batch.id,
        "target_task": batch.target_task,
        "source_name": batch.source_name,
        "season_label": batch.season_label,
        "freshness_status": batch.freshness_status,
        "pending_only": batch.pending_only,
        "candidate_board_count": batch.candidate_board_count,
        "scored_board_count": batch.scored_board_count,
        "materialized_scoring_run_count": batch.materialized_scoring_run_count,
        "materialized_opportunity_count": batch.materialized_opportunity_count,
        "payload": batch.payload,
        "created_at": batch.created_at.isoformat() if batch.created_at else None,
    }


def _serialize_model_market_board_cadence_batch(
    batch: ModelMarketBoardCadenceBatchRecord | None,
) -> dict[str, Any] | None:
    if batch is None:
        return None
    return {
        "id": batch.id,
        "target_task": batch.target_task,
        "source_name": batch.source_name,
        "season_label": batch.season_label,
        "refresh_freshness_status": batch.refresh_freshness_status,
        "scoring_freshness_status": batch.scoring_freshness_status,
        "refreshed_board_count": batch.refreshed_board_count,
        "scored_board_count": batch.scored_board_count,
        "materialized_scoring_run_count": batch.materialized_scoring_run_count,
        "materialized_opportunity_count": batch.materialized_opportunity_count,
        "payload": batch.payload,
        "created_at": batch.created_at.isoformat() if batch.created_at else None,
    }


def _build_market_board_freshness(
    board: ModelMarketBoardRecord,
) -> dict[str, Any] | None:
    source_payload = board.payload.get("source")
    if not isinstance(source_payload, dict):
        return None
    refreshed_at = source_payload.get("refreshed_at")
    if refreshed_at is None:
        return None
    refreshed_date = _coerce_date_value(refreshed_at)
    days_since_refresh = (_utc_today() - refreshed_date).days
    if days_since_refresh <= 0:
        freshness_status = "fresh"
    elif days_since_refresh == 1:
        freshness_status = "aging"
    else:
        freshness_status = "stale"
    return {
        "source_name": source_payload.get("source_name"),
        "refresh_target_date": source_payload.get("refresh_target_date"),
        "refreshed_at": refreshed_date.isoformat(),
        "refresh_count": int(source_payload.get("refresh_count", 0)),
        "last_refresh_status": source_payload.get("last_refresh_status"),
        "days_since_refresh": days_since_refresh,
        "freshness_status": freshness_status,
    }


def _summarize_market_board_refresh_history(
    refresh_events: list[ModelMarketBoardRefreshRecord],
    *,
    recent_limit: int,
) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    for refresh_event in refresh_events:
        status_counts[refresh_event.refresh_status] = (
            status_counts.get(refresh_event.refresh_status, 0) + 1
        )
        source_counts[refresh_event.source_name] = (
            source_counts.get(refresh_event.source_name, 0) + 1
        )
    return {
        "overview": {
            "refresh_event_count": len(refresh_events),
            "status_counts": status_counts,
            "source_counts": source_counts,
            "latest_refresh_event": (
                _serialize_model_market_board_refresh_event(refresh_events[0])
                if refresh_events
                else None
            ),
        },
        "recent_refresh_events": [
            _serialize_model_market_board_refresh_event(entry)
            for entry in refresh_events[:recent_limit]
        ],
    }


def _summarize_model_market_board_source_run_history(
    source_runs: list[ModelMarketBoardSourceRunRecord],
    *,
    recent_limit: int,
) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    total_generated_game_count = 0
    total_invalid_row_count = 0
    total_warning_count = 0
    daily_buckets: dict[str, dict[str, Any]] = {}
    for source_run in source_runs:
        status_counts[source_run.status] = status_counts.get(source_run.status, 0) + 1
        source_counts[source_run.source_name] = source_counts.get(source_run.source_name, 0) + 1
        total_generated_game_count += source_run.generated_game_count
        validation_summary = source_run.payload.get("validation_summary", {})
        total_invalid_row_count += int(validation_summary.get("invalid_row_count", 0))
        total_warning_count += int(validation_summary.get("warning_count", 0))
        if source_run.created_at is None:
            continue
        day_key = source_run.created_at.date().isoformat()
        bucket = daily_buckets.setdefault(
            day_key,
            {
                "date": day_key,
                "run_count": 0,
                "generated_game_count": 0,
                "invalid_row_count": 0,
                "warning_count": 0,
            },
        )
        bucket["run_count"] += 1
        bucket["generated_game_count"] += source_run.generated_game_count
        bucket["invalid_row_count"] += int(validation_summary.get("invalid_row_count", 0))
        bucket["warning_count"] += int(validation_summary.get("warning_count", 0))
    return {
        "overview": {
            "source_run_count": len(source_runs),
            "generated_game_count": total_generated_game_count,
            "invalid_row_count": total_invalid_row_count,
            "warning_count": total_warning_count,
            "status_counts": status_counts,
            "source_counts": source_counts,
            "latest_source_run": _serialize_model_market_board_source_run(
                source_runs[0] if source_runs else None
            ),
        },
        "daily_buckets": [daily_buckets[key] for key in sorted(daily_buckets.keys())],
        "recent_source_runs": [
            _serialize_model_market_board_source_run(entry)
            for entry in source_runs[:recent_limit]
        ],
    }


def _build_model_market_board_refresh_batch(
    *,
    result: dict[str, Any],
    target_task: str | None,
    source_name: str | None,
    season_label: str | None,
    freshness_status: str | None,
    pending_only: bool,
) -> ModelMarketBoardRefreshBatchRecord:
    payload = {
        "queue_before": result.get("queue_before", {}),
        "queue_after": result.get("queue_after", {}),
        "refresh_runs": result.get("refresh_runs", []),
    }
    return ModelMarketBoardRefreshBatchRecord(
        id=0,
        target_task=target_task or "unknown",
        source_name=source_name,
        season_label=season_label,
        freshness_status=freshness_status,
        pending_only=pending_only,
        candidate_board_count=int(result.get("candidate_board_count", 0)),
        refreshed_board_count=int(result.get("refreshed_board_count", 0)),
        created_board_count=int(result.get("created_board_count", 0)),
        updated_board_count=int(result.get("updated_board_count", 0)),
        unchanged_board_count=int(result.get("unchanged_board_count", 0)),
        payload=payload,
    )


def save_model_market_board_refresh_batch_in_memory(
    repository: MarketBoardOperationStore,
    batch: ModelMarketBoardRefreshBatchRecord,
) -> ModelMarketBoardRefreshBatchRecord:
    payload = asdict(batch)
    payload["id"] = len(repository.model_market_board_refresh_batches) + 1
    payload["created_at"] = datetime.now(timezone.utc)
    repository.model_market_board_refresh_batches.append(payload)
    return ModelMarketBoardRefreshBatchRecord(**payload)


def save_model_market_board_refresh_batch_postgres(
    connection: Any,
    batch: ModelMarketBoardRefreshBatchRecord,
) -> ModelMarketBoardRefreshBatchRecord:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO model_market_board_refresh_batch (
                target_task,
                source_name,
                season_label,
                freshness_status,
                pending_only,
                candidate_board_count,
                refreshed_board_count,
                created_board_count,
                updated_board_count,
                unchanged_board_count,
                payload_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            RETURNING id, created_at
            """,
            (
                batch.target_task,
                batch.source_name,
                batch.season_label,
                batch.freshness_status,
                batch.pending_only,
                batch.candidate_board_count,
                batch.refreshed_board_count,
                batch.created_board_count,
                batch.updated_board_count,
                batch.unchanged_board_count,
                _json_dumps(batch.payload),
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return ModelMarketBoardRefreshBatchRecord(
        id=int(row[0]),
        target_task=batch.target_task,
        source_name=batch.source_name,
        season_label=batch.season_label,
        freshness_status=batch.freshness_status,
        pending_only=batch.pending_only,
        candidate_board_count=batch.candidate_board_count,
        refreshed_board_count=batch.refreshed_board_count,
        created_board_count=batch.created_board_count,
        updated_board_count=batch.updated_board_count,
        unchanged_board_count=batch.unchanged_board_count,
        payload=batch.payload,
        created_at=row[1],
    )


def _build_model_market_board_source_run(
    *,
    source_name: str,
    target_task: str,
    season_label: str,
    game_date: date,
    slate_label: str | None,
    requested_game_count: int,
    generated_games: list[dict[str, Any]],
    raw_generated_games: list[dict[str, Any]] | None = None,
    source_path: str | None = None,
    source_request_context: dict[str, Any] | None = None,
    status: str = "SUCCESS",
    error_message: str | None = None,
    validation_summary: dict[str, Any] | None = None,
    source_payload_fingerprints: dict[str, Any] | None = None,
) -> ModelMarketBoardSourceRunRecord:
    payload = {
        "request": {
            "source_name": source_name,
            "target_task": target_task,
            "season_label": season_label,
            "game_date": game_date.isoformat(),
            "slate_label": slate_label,
            "requested_game_count": requested_game_count,
            "source_path": source_path,
        },
        "generated_games": generated_games,
    }
    if raw_generated_games is not None:
        payload["raw_generated_games"] = raw_generated_games
    if validation_summary is not None:
        payload["validation_summary"] = validation_summary
    if source_payload_fingerprints is not None:
        payload["source_payload_fingerprints"] = source_payload_fingerprints
    if source_request_context is not None:
        payload["source_request_context"] = source_request_context
    if error_message is not None:
        payload["error_message"] = error_message
    return ModelMarketBoardSourceRunRecord(
        id=0,
        source_name=source_name,
        target_task=target_task,
        season_label=season_label,
        game_date=game_date,
        slate_label=slate_label,
        requested_game_count=requested_game_count,
        generated_game_count=len(generated_games),
        status=status,
        payload=payload,
    )


def save_model_market_board_source_run_in_memory(
    repository: MarketBoardOperationStore,
    source_run: ModelMarketBoardSourceRunRecord,
) -> ModelMarketBoardSourceRunRecord:
    payload = asdict(source_run)
    payload["id"] = len(repository.model_market_board_source_runs) + 1
    payload["created_at"] = datetime.now(timezone.utc)
    repository.model_market_board_source_runs.append(payload)
    return ModelMarketBoardSourceRunRecord(**payload)


def save_model_market_board_source_run_postgres(
    connection: Any,
    source_run: ModelMarketBoardSourceRunRecord,
) -> ModelMarketBoardSourceRunRecord:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO model_market_board_source_run (
                source_name,
                target_task,
                season_label,
                game_date,
                slate_label,
                requested_game_count,
                generated_game_count,
                status,
                payload_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            RETURNING id, created_at
            """,
            (
                source_run.source_name,
                source_run.target_task,
                source_run.season_label,
                source_run.game_date,
                source_run.slate_label,
                source_run.requested_game_count,
                source_run.generated_game_count,
                source_run.status,
                _json_dumps(source_run.payload),
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return ModelMarketBoardSourceRunRecord(
        id=int(row[0]),
        source_name=source_run.source_name,
        target_task=source_run.target_task,
        season_label=source_run.season_label,
        game_date=source_run.game_date,
        slate_label=source_run.slate_label,
        requested_game_count=source_run.requested_game_count,
        generated_game_count=source_run.generated_game_count,
        status=source_run.status,
        payload=source_run.payload,
        created_at=row[1],
    )


def _build_model_market_board_scoring_batch(
    *,
    result: dict[str, Any],
    target_task: str | None,
    source_name: str | None,
    season_label: str | None,
    freshness_status: str | None,
    pending_only: bool,
) -> ModelMarketBoardScoringBatchRecord:
    payload = {
        "queue_before": result.get("queue_before", {}),
        "queue_after": result.get("queue_after", {}),
        "orchestration_runs": result.get("orchestration_runs", []),
    }
    return ModelMarketBoardScoringBatchRecord(
        id=0,
        target_task=target_task or "unknown",
        source_name=source_name,
        season_label=season_label,
        freshness_status=freshness_status,
        pending_only=pending_only,
        candidate_board_count=int(result.get("candidate_board_count", 0)),
        scored_board_count=int(result.get("scored_board_count", 0)),
        materialized_scoring_run_count=int(
            result.get("materialized_scoring_run_count", 0)
        ),
        materialized_opportunity_count=int(
            result.get("materialized_opportunity_count", 0)
        ),
        payload=payload,
    )


def save_model_market_board_scoring_batch_in_memory(
    repository: MarketBoardOperationStore,
    batch: ModelMarketBoardScoringBatchRecord,
) -> ModelMarketBoardScoringBatchRecord:
    payload = asdict(batch)
    payload["id"] = len(repository.model_market_board_scoring_batches) + 1
    payload["created_at"] = datetime.now(timezone.utc)
    repository.model_market_board_scoring_batches.append(payload)
    return ModelMarketBoardScoringBatchRecord(**payload)


def save_model_market_board_scoring_batch_postgres(
    connection: Any,
    batch: ModelMarketBoardScoringBatchRecord,
) -> ModelMarketBoardScoringBatchRecord:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO model_market_board_scoring_batch (
                target_task,
                source_name,
                season_label,
                freshness_status,
                pending_only,
                candidate_board_count,
                scored_board_count,
                materialized_scoring_run_count,
                materialized_opportunity_count,
                payload_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            RETURNING id, created_at
            """,
            (
                batch.target_task,
                batch.source_name,
                batch.season_label,
                batch.freshness_status,
                batch.pending_only,
                batch.candidate_board_count,
                batch.scored_board_count,
                batch.materialized_scoring_run_count,
                batch.materialized_opportunity_count,
                _json_dumps(batch.payload),
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return ModelMarketBoardScoringBatchRecord(
        id=int(row[0]),
        target_task=batch.target_task,
        source_name=batch.source_name,
        season_label=batch.season_label,
        freshness_status=batch.freshness_status,
        pending_only=batch.pending_only,
        candidate_board_count=batch.candidate_board_count,
        scored_board_count=batch.scored_board_count,
        materialized_scoring_run_count=batch.materialized_scoring_run_count,
        materialized_opportunity_count=batch.materialized_opportunity_count,
        payload=batch.payload,
        created_at=row[1],
    )


def _build_model_market_board_cadence_batch(
    *,
    result: dict[str, Any],
    target_task: str | None,
    source_name: str | None,
    season_label: str | None,
    refresh_freshness_status: str | None,
    scoring_freshness_status: str | None,
) -> ModelMarketBoardCadenceBatchRecord:
    payload = {
        "refresh_result": result.get("refresh_result", {}),
        "scoring_result": result.get("scoring_result", {}),
    }
    return ModelMarketBoardCadenceBatchRecord(
        id=0,
        target_task=target_task or "unknown",
        source_name=source_name,
        season_label=season_label,
        refresh_freshness_status=refresh_freshness_status,
        scoring_freshness_status=scoring_freshness_status,
        refreshed_board_count=int(result.get("refreshed_board_count", 0)),
        scored_board_count=int(result.get("scored_board_count", 0)),
        materialized_scoring_run_count=int(
            result.get("materialized_scoring_run_count", 0)
        ),
        materialized_opportunity_count=int(
            result.get("materialized_opportunity_count", 0)
        ),
        payload=payload,
    )


def save_model_market_board_cadence_batch_in_memory(
    repository: MarketBoardOperationStore,
    batch: ModelMarketBoardCadenceBatchRecord,
) -> ModelMarketBoardCadenceBatchRecord:
    payload = asdict(batch)
    payload["id"] = len(repository.model_market_board_cadence_batches) + 1
    payload["created_at"] = datetime.now(timezone.utc)
    repository.model_market_board_cadence_batches.append(payload)
    return ModelMarketBoardCadenceBatchRecord(**payload)


def save_model_market_board_cadence_batch_postgres(
    connection: Any,
    batch: ModelMarketBoardCadenceBatchRecord,
) -> ModelMarketBoardCadenceBatchRecord:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO model_market_board_cadence_batch (
                target_task,
                source_name,
                season_label,
                refresh_freshness_status,
                scoring_freshness_status,
                refreshed_board_count,
                scored_board_count,
                materialized_scoring_run_count,
                materialized_opportunity_count,
                payload_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            RETURNING id, created_at
            """,
            (
                batch.target_task,
                batch.source_name,
                batch.season_label,
                batch.refresh_freshness_status,
                batch.scoring_freshness_status,
                batch.refreshed_board_count,
                batch.scored_board_count,
                batch.materialized_scoring_run_count,
                batch.materialized_opportunity_count,
                _json_dumps(batch.payload),
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return ModelMarketBoardCadenceBatchRecord(
        id=int(row[0]),
        target_task=batch.target_task,
        source_name=batch.source_name,
        season_label=batch.season_label,
        refresh_freshness_status=batch.refresh_freshness_status,
        scoring_freshness_status=batch.scoring_freshness_status,
        refreshed_board_count=batch.refreshed_board_count,
        scored_board_count=batch.scored_board_count,
        materialized_scoring_run_count=batch.materialized_scoring_run_count,
        materialized_opportunity_count=batch.materialized_opportunity_count,
        payload=batch.payload,
        created_at=row[1],
    )


def _coerce_date_value(value: Any) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _float_or_none_value(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _serialize_future_game_input_value(game: dict[str, Any]) -> dict[str, Any]:
    game_date = _coerce_date_value(game["game_date"])
    return {
        "season_label": str(game["season_label"]),
        "game_date": game_date.isoformat(),
        "home_team_code": str(game["home_team_code"]),
        "away_team_code": str(game["away_team_code"]),
        "home_spread_line": _float_or_none_value(game.get("home_spread_line")),
        "total_line": _float_or_none_value(game.get("total_line")),
    }


def _build_future_slate_key_value(
    *,
    target_task: str,
    slate_label: str | None,
    serialized_inputs: list[dict[str, Any]],
) -> str:
    if slate_label:
        return f"{target_task}:{slate_label}"
    if not serialized_inputs:
        return f"{target_task}:empty-slate"
    ordered = sorted(
        serialized_inputs,
        key=lambda entry: (
            entry["game_date"],
            entry["home_team_code"],
            entry["away_team_code"],
        ),
    )
    first = ordered[0]
    last = ordered[-1]
    return (
        f"{target_task}:{first['game_date']}:{last['game_date']}:"
        f"{len(serialized_inputs)}-games"
    )


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()
