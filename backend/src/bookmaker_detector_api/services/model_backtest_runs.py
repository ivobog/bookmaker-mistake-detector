from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from bookmaker_detector_api.repositories.ingestion_json import _json_dumps
from bookmaker_detector_api.services.model_records import ModelBacktestRunRecord
from bookmaker_detector_api.services.task_registry import normalize_selection_policy_name


def _canonicalize_selection_policy_name(selection_policy_name: str) -> str:
    try:
        return normalize_selection_policy_name(selection_policy_name)
    except ValueError:
        return selection_policy_name


def save_model_backtest_run_postgres(
    connection: Any,
    backtest_run: ModelBacktestRunRecord,
) -> ModelBacktestRunRecord:
    canonical_selection_policy_name = _canonicalize_selection_policy_name(
        backtest_run.selection_policy_name
    )
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO model_backtest_run (
                feature_version_id,
                target_task,
                scope_team_code,
                scope_season_label,
                status,
                selection_policy_name,
                strategy_name,
                minimum_train_games,
                test_window_games,
                train_ratio,
                validation_ratio,
                fold_count,
                payload_json,
                completed_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW()
            )
            RETURNING id, created_at, completed_at
            """,
            (
                backtest_run.feature_version_id,
                backtest_run.target_task,
                backtest_run.team_code or "",
                backtest_run.season_label or "",
                backtest_run.status,
                canonical_selection_policy_name,
                backtest_run.strategy_name,
                backtest_run.minimum_train_games,
                backtest_run.test_window_games,
                backtest_run.train_ratio,
                backtest_run.validation_ratio,
                backtest_run.fold_count,
                _json_dumps(backtest_run.payload),
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return ModelBacktestRunRecord(
        id=int(row[0]),
        feature_version_id=backtest_run.feature_version_id,
        target_task=backtest_run.target_task,
        team_code=backtest_run.team_code,
        season_label=backtest_run.season_label,
        status=backtest_run.status,
        selection_policy_name=canonical_selection_policy_name,
        strategy_name=backtest_run.strategy_name,
        minimum_train_games=backtest_run.minimum_train_games,
        test_window_games=backtest_run.test_window_games,
        train_ratio=backtest_run.train_ratio,
        validation_ratio=backtest_run.validation_ratio,
        fold_count=backtest_run.fold_count,
        payload=backtest_run.payload,
        created_at=row[1],
        completed_at=row[2],
    )


def list_model_backtest_runs_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
) -> list[ModelBacktestRunRecord]:
    query = """
        SELECT
            id,
            feature_version_id,
            target_task,
            scope_team_code,
            scope_season_label,
            status,
            selection_policy_name,
            strategy_name,
            minimum_train_games,
            test_window_games,
            train_ratio,
            validation_ratio,
            fold_count,
            payload_json,
            created_at,
            completed_at
        FROM model_backtest_run
        WHERE 1=1
    """
    params: list[Any] = []
    if target_task is not None:
        query += " AND target_task = %s"
        params.append(target_task)
    if team_code is not None:
        query += " AND scope_team_code = %s"
        params.append(team_code)
    if season_label is not None:
        query += " AND scope_season_label = %s"
        params.append(season_label)
    query += " ORDER BY completed_at DESC NULLS LAST, created_at DESC, id DESC"
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelBacktestRunRecord(
            id=int(row[0]),
            feature_version_id=int(row[1]),
            target_task=row[2],
            team_code=row[3] or None,
            season_label=row[4] or None,
            status=row[5],
            selection_policy_name=_canonicalize_selection_policy_name(row[6]),
            strategy_name=row[7],
            minimum_train_games=int(row[8]),
            test_window_games=int(row[9]),
            train_ratio=float(row[10]),
            validation_ratio=float(row[11]),
            fold_count=int(row[12]),
            payload=row[13],
            created_at=row[14],
            completed_at=row[15],
        )
        for row in rows
    ]


def get_model_backtest_history_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    recent_limit: int = 10,
    nested_get,
) -> dict[str, Any]:
    runs = list_model_backtest_runs_postgres(
        connection,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
    )
    return summarize_model_backtest_history(runs, recent_limit=recent_limit, nested_get=nested_get)


def get_model_backtest_detail_postgres(
    connection: Any,
    *,
    backtest_run_id: int,
) -> dict[str, Any] | None:
    run = next(
        (
            entry
            for entry in list_model_backtest_runs_postgres(connection)
            if entry.id == backtest_run_id
        ),
        None,
    )
    return serialize_model_backtest_run(run)


def serialize_model_backtest_run(
    backtest_run: ModelBacktestRunRecord | None,
) -> dict[str, Any] | None:
    if backtest_run is None:
        return None
    return {
        "id": backtest_run.id,
        "feature_version_id": backtest_run.feature_version_id,
        "target_task": backtest_run.target_task,
        "team_code": backtest_run.team_code,
        "season_label": backtest_run.season_label,
        "status": backtest_run.status,
        "selection_policy_name": _canonicalize_selection_policy_name(
            backtest_run.selection_policy_name
        ),
        "strategy_name": backtest_run.strategy_name,
        "minimum_train_games": backtest_run.minimum_train_games,
        "test_window_games": backtest_run.test_window_games,
        "train_ratio": backtest_run.train_ratio,
        "validation_ratio": backtest_run.validation_ratio,
        "fold_count": backtest_run.fold_count,
        "payload": backtest_run.payload,
        "created_at": backtest_run.created_at.isoformat() if backtest_run.created_at else None,
        "completed_at": (
            backtest_run.completed_at.isoformat() if backtest_run.completed_at else None
        ),
    }


def summarize_model_backtest_history(
    runs: list[ModelBacktestRunRecord],
    *,
    recent_limit: int,
    nested_get,
) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    target_task_counts: dict[str, int] = {}
    strategy_counts: dict[str, int] = {}
    daily_buckets: dict[str, dict[str, Any]] = {}
    best_run = None
    best_roi = None
    for run in runs:
        status_counts[run.status] = status_counts.get(run.status, 0) + 1
        target_task_counts[run.target_task] = target_task_counts.get(run.target_task, 0) + 1
        strategy_counts[run.strategy_name] = strategy_counts.get(run.strategy_name, 0) + 1
        candidate_roi = nested_get(
            run.payload,
            "strategy_results",
            "candidate_threshold",
            "roi",
        )
        if candidate_roi is not None and (
            best_roi is None or float(candidate_roi) > float(best_roi)
        ):
            best_roi = candidate_roi
            best_run = run
        created_at = run.completed_at or run.created_at
        if created_at is None:
            continue
        day_key = created_at.date().isoformat()
        bucket = daily_buckets.setdefault(
            day_key,
            {
                "date": day_key,
                "run_count": 0,
                "fold_count": 0,
                "bet_count": 0,
                "profit_units": 0.0,
            },
        )
        bucket["run_count"] += 1
        bucket["fold_count"] += int(run.fold_count)
        bucket["bet_count"] += int(
            nested_get(run.payload, "strategy_results", "candidate_threshold", "bet_count") or 0
        )
        bucket["profit_units"] = round(
            float(bucket["profit_units"])
            + float(
                nested_get(
                    run.payload,
                    "strategy_results",
                    "candidate_threshold",
                    "profit_units",
                )
                or 0.0
            ),
            4,
        )
    return {
        "overview": {
            "run_count": len(runs),
            "status_counts": status_counts,
            "target_task_counts": target_task_counts,
            "strategy_counts": strategy_counts,
            "best_candidate_threshold_run": serialize_model_backtest_run(best_run),
            "latest_run": serialize_model_backtest_run(runs[0] if runs else None),
        },
        "daily_buckets": [daily_buckets[key] for key in sorted(daily_buckets.keys())],
        "recent_runs": [serialize_model_backtest_run(entry) for entry in runs[:recent_limit]],
    }
