from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timezone
from typing import Any

from bookmaker_detector_api.repositories import InMemoryIngestionRepository
from bookmaker_detector_api.repositories.ingestion import _json_dumps
from bookmaker_detector_api.services import model_training_views
from bookmaker_detector_api.services.model_market_board_views import _serialize_model_scoring_run
from bookmaker_detector_api.services.model_records import ModelScoringRunRecord


def build_model_scoring_run(
    *,
    preview: dict[str, Any],
    target_task: str,
    default_policy_name: str | None = None,
    model_market_board_id: int | None = None,
) -> ModelScoringRunRecord | None:
    active_selection = preview.get("active_selection")
    active_snapshot = preview.get("active_evaluation_snapshot")
    feature_version = preview.get("feature_version")
    scenario = preview.get("scenario")
    if feature_version is None or scenario is None:
        return None
    opportunity_preview = list(preview.get("opportunity_preview", []))
    status_counts = {
        "candidate_signal": 0,
        "review_manually": 0,
        "discarded": 0,
    }
    for entry in opportunity_preview:
        status = str(entry.get("status", "discarded"))
        if status not in status_counts:
            status_counts[status] = 0
        status_counts[status] += 1
    payload = {
        "scenario": scenario,
        "feature_version": feature_version,
        "active_selection": active_selection,
        "active_evaluation_snapshot": active_snapshot,
        "prediction_summary": preview.get("prediction_summary", {}),
        "predictions": preview.get("predictions", []),
        "opportunity_preview": opportunity_preview,
    }
    game_date = scenario.get("game_date")
    if isinstance(game_date, str):
        game_date = date.fromisoformat(game_date)
    return ModelScoringRunRecord(
        id=0,
        model_market_board_id=model_market_board_id,
        model_selection_snapshot_id=(
            int(active_selection["id"]) if active_selection is not None else None
        ),
        model_evaluation_snapshot_id=(
            int(active_snapshot["id"]) if active_snapshot is not None else None
        ),
        feature_version_id=int(feature_version["id"]),
        target_task=target_task,
        scenario_key=str(scenario["scenario_key"]),
        season_label=str(scenario["season_label"]),
        game_date=game_date,
        home_team_code=str(scenario["home_team_code"]),
        away_team_code=str(scenario["away_team_code"]),
        home_spread_line=model_training_views._float_or_none(scenario.get("home_spread_line")),
        total_line=model_training_views._float_or_none(scenario.get("total_line")),
        policy_name=(
            str(opportunity_preview[0]["policy_name"]) if opportunity_preview else default_policy_name
        ),
        prediction_count=int(preview.get("scored_prediction_count", 0)),
        candidate_opportunity_count=int(status_counts.get("candidate_signal", 0)),
        review_opportunity_count=int(status_counts.get("review_manually", 0)),
        discarded_opportunity_count=int(status_counts.get("discarded", 0)),
        payload=payload,
    )


def save_model_scoring_run_in_memory(
    repository: InMemoryIngestionRepository,
    scoring_run: ModelScoringRunRecord | None,
) -> ModelScoringRunRecord | None:
    if scoring_run is None:
        return None
    payload = asdict(scoring_run)
    payload["id"] = len(repository.model_scoring_runs) + 1
    payload["created_at"] = datetime.now(timezone.utc)
    repository.model_scoring_runs.append(payload)
    return ModelScoringRunRecord(**payload)


def save_model_scoring_run_postgres(
    connection: Any,
    scoring_run: ModelScoringRunRecord | None,
) -> ModelScoringRunRecord | None:
    if scoring_run is None:
        return None
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO model_scoring_run (
                model_market_board_id,
                model_selection_snapshot_id,
                model_evaluation_snapshot_id,
                feature_version_id,
                target_task,
                scenario_key,
                season_label,
                game_date,
                home_team_code,
                away_team_code,
                home_spread_line,
                total_line,
                policy_name,
                prediction_count,
                candidate_opportunity_count,
                review_opportunity_count,
                discarded_opportunity_count,
                payload_json
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
            )
            RETURNING id, created_at
            """,
            (
                scoring_run.model_market_board_id,
                scoring_run.model_selection_snapshot_id,
                scoring_run.model_evaluation_snapshot_id,
                scoring_run.feature_version_id,
                scoring_run.target_task,
                scoring_run.scenario_key,
                scoring_run.season_label,
                scoring_run.game_date,
                scoring_run.home_team_code,
                scoring_run.away_team_code,
                scoring_run.home_spread_line,
                scoring_run.total_line,
                scoring_run.policy_name,
                scoring_run.prediction_count,
                scoring_run.candidate_opportunity_count,
                scoring_run.review_opportunity_count,
                scoring_run.discarded_opportunity_count,
                _json_dumps(scoring_run.payload),
            ),
        )
        row = cursor.fetchone()
    connection.commit()
    return ModelScoringRunRecord(
        id=int(row[0]),
        model_market_board_id=scoring_run.model_market_board_id,
        model_selection_snapshot_id=scoring_run.model_selection_snapshot_id,
        model_evaluation_snapshot_id=scoring_run.model_evaluation_snapshot_id,
        feature_version_id=scoring_run.feature_version_id,
        target_task=scoring_run.target_task,
        scenario_key=scoring_run.scenario_key,
        season_label=scoring_run.season_label,
        game_date=scoring_run.game_date,
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
        created_at=row[1],
    )


def list_model_scoring_runs_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    model_market_board_id: int | None = None,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
) -> list[ModelScoringRunRecord]:
    selected = [
        ModelScoringRunRecord(**entry)
        for entry in repository.model_scoring_runs
        if (
            model_market_board_id is None
            or entry.get("model_market_board_id") == model_market_board_id
        )
        if (target_task is None or entry["target_task"] == target_task)
        and (
            team_code is None
            or entry["home_team_code"] == team_code
            or entry["away_team_code"] == team_code
        )
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


def list_model_scoring_runs_postgres(
    connection: Any,
    *,
    model_market_board_id: int | None = None,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
) -> list[ModelScoringRunRecord]:
    query = """
        SELECT
            id,
            model_market_board_id,
            model_selection_snapshot_id,
            model_evaluation_snapshot_id,
            feature_version_id,
            target_task,
            scenario_key,
            season_label,
            game_date,
            home_team_code,
            away_team_code,
            home_spread_line,
            total_line,
            policy_name,
            prediction_count,
            candidate_opportunity_count,
            review_opportunity_count,
            discarded_opportunity_count,
            payload_json,
            created_at
        FROM model_scoring_run
        WHERE 1=1
    """
    params: list[Any] = []
    if model_market_board_id is not None:
        query += " AND model_market_board_id = %s"
        params.append(model_market_board_id)
    if target_task is not None:
        query += " AND target_task = %s"
        params.append(target_task)
    if team_code is not None:
        query += " AND (home_team_code = %s OR away_team_code = %s)"
        params.extend([team_code, team_code])
    if season_label is not None:
        query += " AND season_label = %s"
        params.append(season_label)
    query += " ORDER BY created_at DESC, id DESC"
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelScoringRunRecord(
            id=int(row[0]),
            model_market_board_id=int(row[1]) if row[1] is not None else None,
            model_selection_snapshot_id=int(row[2]) if row[2] is not None else None,
            model_evaluation_snapshot_id=int(row[3]) if row[3] is not None else None,
            feature_version_id=int(row[4]),
            target_task=row[5],
            scenario_key=row[6],
            season_label=row[7],
            game_date=row[8],
            home_team_code=row[9],
            away_team_code=row[10],
            home_spread_line=model_training_views._float_or_none(row[11]),
            total_line=model_training_views._float_or_none(row[12]),
            policy_name=row[13],
            prediction_count=int(row[14]),
            candidate_opportunity_count=int(row[15]),
            review_opportunity_count=int(row[16]),
            discarded_opportunity_count=int(row[17]),
            payload=row[18],
            created_at=row[19],
        )
        for row in rows
    ]


def get_model_scoring_run_detail_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    scoring_run_id: int,
) -> dict[str, Any] | None:
    scoring_run = next(
        (
            entry
            for entry in list_model_scoring_runs_in_memory(repository)
            if entry.id == scoring_run_id
        ),
        None,
    )
    return _serialize_model_scoring_run(scoring_run)


def get_model_scoring_run_detail_postgres(
    connection: Any,
    *,
    scoring_run_id: int,
) -> dict[str, Any] | None:
    scoring_run = next(
        (
            entry
            for entry in list_model_scoring_runs_postgres(connection)
            if entry.id == scoring_run_id
        ),
        None,
    )
    return _serialize_model_scoring_run(scoring_run)


def get_model_scoring_history_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    model_market_board_id: int | None = None,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    scoring_runs = list_model_scoring_runs_in_memory(
        repository,
        model_market_board_id=model_market_board_id,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
    )
    return _summarize_model_scoring_history(scoring_runs, recent_limit=recent_limit)


def get_model_scoring_history_postgres(
    connection: Any,
    *,
    model_market_board_id: int | None = None,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    scoring_runs = list_model_scoring_runs_postgres(
        connection,
        model_market_board_id=model_market_board_id,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
    )
    return _summarize_model_scoring_history(scoring_runs, recent_limit=recent_limit)


def _summarize_model_scoring_history(
    scoring_runs: list[ModelScoringRunRecord],
    *,
    recent_limit: int,
) -> dict[str, Any]:
    policy_counts: dict[str, int] = {}
    daily_buckets: dict[str, dict[str, Any]] = {}
    total_prediction_count = 0
    surfaced_run_count = 0
    opportunity_status_counts = {
        "candidate_signal": 0,
        "review_manually": 0,
        "discarded": 0,
    }
    for scoring_run in scoring_runs:
        if scoring_run.policy_name is not None:
            policy_counts[scoring_run.policy_name] = (
                policy_counts.get(scoring_run.policy_name, 0) + 1
            )
        total_prediction_count += scoring_run.prediction_count
        surfaced_count = (
            scoring_run.candidate_opportunity_count + scoring_run.review_opportunity_count
        )
        if surfaced_count > 0:
            surfaced_run_count += 1
        opportunity_status_counts["candidate_signal"] += scoring_run.candidate_opportunity_count
        opportunity_status_counts["review_manually"] += scoring_run.review_opportunity_count
        opportunity_status_counts["discarded"] += scoring_run.discarded_opportunity_count
        if scoring_run.created_at is None:
            continue
        day_key = scoring_run.created_at.date().isoformat()
        bucket = daily_buckets.setdefault(
            day_key,
            {
                "date": day_key,
                "scoring_run_count": 0,
                "prediction_count": 0,
                "surfaced_run_count": 0,
                "opportunity_status_counts": {
                    "candidate_signal": 0,
                    "review_manually": 0,
                    "discarded": 0,
                },
            },
        )
        bucket["scoring_run_count"] += 1
        bucket["prediction_count"] += scoring_run.prediction_count
        if surfaced_count > 0:
            bucket["surfaced_run_count"] += 1
        bucket_status_counts = bucket["opportunity_status_counts"]
        bucket_status_counts["candidate_signal"] += scoring_run.candidate_opportunity_count
        bucket_status_counts["review_manually"] += scoring_run.review_opportunity_count
        bucket_status_counts["discarded"] += scoring_run.discarded_opportunity_count
    return {
        "overview": {
            "scoring_run_count": len(scoring_runs),
            "policy_counts": policy_counts,
            "prediction_count": total_prediction_count,
            "surfaced_run_count": surfaced_run_count,
            "opportunity_status_counts": opportunity_status_counts,
            "latest_scoring_run": _serialize_model_scoring_run(
                scoring_runs[0] if scoring_runs else None
            ),
        },
        "daily_buckets": [daily_buckets[key] for key in sorted(daily_buckets.keys())],
        "recent_scoring_runs": [
            _serialize_model_scoring_run(entry) for entry in scoring_runs[:recent_limit]
        ],
    }
