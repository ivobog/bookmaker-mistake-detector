from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from bookmaker_detector_api.repositories import ModelOpportunityStore
from bookmaker_detector_api.repositories.ingestion_json import _json_dumps
from bookmaker_detector_api.services import model_future_scenarios
from bookmaker_detector_api.services.model_market_board_views import _serialize_model_opportunity
from bookmaker_detector_api.services.model_records import ModelOpportunityRecord


def materialize_model_opportunities(
    *,
    scoring_preview: dict[str, Any],
    target_task: str,
    build_opportunities,
    save_opportunities,
) -> dict[str, Any]:
    opportunities = build_opportunities(
        scoring_preview=scoring_preview,
        target_task=target_task,
    )
    persisted = save_opportunities(opportunities)
    return {
        **scoring_preview,
        "materialized_count": len(persisted),
        "opportunity_count": len(persisted),
        "opportunities": [_serialize_model_opportunity(entry) for entry in persisted],
    }


def build_model_opportunities(
    *,
    scoring_preview: dict[str, Any],
    target_task: str,
    policy: dict[str, Any] | None,
    model_scoring_run_id: int | None = None,
    allow_best_effort_review: bool = False,
) -> list[ModelOpportunityRecord]:
    active_selection = scoring_preview.get("active_selection")
    active_snapshot = scoring_preview.get("active_evaluation_snapshot")
    feature_version = scoring_preview.get("feature_version")
    scenario = scoring_preview.get("scenario")
    if active_snapshot is None or active_selection is None or feature_version is None:
        return []
    if policy is None:
        raise ValueError(f"Unsupported opportunity policy target_task: {target_task}")
    opportunities = []
    predictions = list(scoring_preview.get("predictions", []))
    for prediction in predictions:
        status = evaluate_opportunity_status(prediction=prediction, policy=policy)
        if status == "discarded":
            continue
        canonical_game_id = positive_int_or_none(prediction.get("canonical_game_id"))
        source_kind = "future_scenario" if scenario is not None else "historical_game"
        scenario_key = str(scenario["scenario_key"]) if scenario is not None else None
        payload = {
            "prediction": prediction,
            "policy": policy,
            "active_selection": active_selection,
            "active_evaluation_snapshot": active_snapshot,
            "scenario": scenario,
        }
        opportunities.append(
            ModelOpportunityRecord(
                id=0,
                model_scoring_run_id=model_scoring_run_id,
                model_selection_snapshot_id=active_selection.get("id"),
                model_evaluation_snapshot_id=active_snapshot.get("id"),
                feature_version_id=int(feature_version["id"]),
                target_task=target_task,
                source_kind=source_kind,
                scenario_key=scenario_key,
                opportunity_key=build_model_opportunity_key(
                    target_task=target_task,
                    canonical_game_id=canonical_game_id,
                    scenario_key=scenario_key,
                    team_code=str(prediction["team_code"]),
                    policy_name=str(policy["policy_name"]),
                ),
                team_code=str(prediction["team_code"]),
                opponent_code=str(prediction["opponent_code"]),
                season_label=str(prediction["season_label"]),
                canonical_game_id=canonical_game_id,
                game_date=model_future_scenarios.coerce_date(prediction["game_date"]),
                policy_name=str(policy["policy_name"]),
                status=status,
                prediction_value=float(prediction["prediction_value"]),
                signal_strength=float(prediction["signal_strength"]),
                evidence_rating=nested_get(prediction, "evidence", "strength", "rating"),
                recommendation_status=nested_get(
                    prediction,
                    "evidence",
                    "recommendation",
                    "status",
                ),
                payload=payload,
            )
        )
    if not opportunities and allow_best_effort_review and predictions:
        strongest_prediction = max(
            predictions,
            key=lambda entry: float(entry.get("signal_strength", 0.0)),
        )
        canonical_game_id = positive_int_or_none(strongest_prediction.get("canonical_game_id"))
        source_kind = "future_scenario" if scenario is not None else "historical_game"
        scenario_key = str(scenario["scenario_key"]) if scenario is not None else None
        payload = {
            "prediction": strongest_prediction,
            "policy": policy,
            "active_selection": active_selection,
            "active_evaluation_snapshot": active_snapshot,
            "scenario": scenario,
            "policy_override_reason": "future_scenario_best_effort_review",
        }
        opportunities.append(
            ModelOpportunityRecord(
                id=0,
                model_scoring_run_id=model_scoring_run_id,
                model_selection_snapshot_id=active_selection.get("id"),
                model_evaluation_snapshot_id=active_snapshot.get("id"),
                feature_version_id=int(feature_version["id"]),
                target_task=target_task,
                source_kind=source_kind,
                scenario_key=scenario_key,
                opportunity_key=build_model_opportunity_key(
                    target_task=target_task,
                    canonical_game_id=canonical_game_id,
                    scenario_key=scenario_key,
                    team_code=str(strongest_prediction["team_code"]),
                    policy_name=str(policy["policy_name"]),
                ),
                team_code=str(strongest_prediction["team_code"]),
                opponent_code=str(strongest_prediction["opponent_code"]),
                season_label=str(strongest_prediction["season_label"]),
                canonical_game_id=canonical_game_id,
                game_date=model_future_scenarios.coerce_date(strongest_prediction["game_date"]),
                policy_name=str(policy["policy_name"]),
                status="review_manually",
                prediction_value=float(strongest_prediction["prediction_value"]),
                signal_strength=float(strongest_prediction["signal_strength"]),
                evidence_rating=nested_get(
                    strongest_prediction,
                    "evidence",
                    "strength",
                    "rating",
                ),
                recommendation_status=nested_get(
                    strongest_prediction,
                    "evidence",
                    "recommendation",
                    "status",
                ),
                payload=payload,
            )
        )
    return opportunities


def evaluate_opportunity_status(
    *,
    prediction: dict[str, Any],
    policy: dict[str, Any],
) -> str:
    signal_strength = float(prediction.get("signal_strength", 0.0))
    evidence_rating = nested_get(prediction, "evidence", "strength", "rating")
    recommendation_status = nested_get(prediction, "evidence", "recommendation", "status")
    if (
        signal_strength >= float(policy["candidate_min_signal_strength"])
        and evidence_rating in policy["candidate_evidence_ratings"]
        and recommendation_status in policy["candidate_recommendation_statuses"]
    ):
        return "candidate_signal"
    if (
        signal_strength >= float(policy["review_min_signal_strength"])
        and evidence_rating in policy["review_evidence_ratings"]
        and recommendation_status in policy["review_recommendation_statuses"]
    ):
        return "review_manually"
    return "discarded"


def build_model_opportunity_key(
    *,
    target_task: str,
    canonical_game_id: int | None,
    scenario_key: str | None,
    team_code: str,
    policy_name: str,
) -> str:
    subject_key = scenario_key if scenario_key is not None else f"game:{canonical_game_id}"
    return f"{target_task}:{subject_key}:{team_code}:{policy_name}"


def nested_get(payload: dict[str, Any] | None, *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def save_model_opportunities_in_memory(
    repository: ModelOpportunityStore,
    opportunities: list[ModelOpportunityRecord],
) -> list[ModelOpportunityRecord]:
    persisted: list[ModelOpportunityRecord] = []
    for opportunity in opportunities:
        existing = next(
            (
                entry
                for entry in repository.model_opportunities
                if entry["opportunity_key"] == opportunity.opportunity_key
            ),
            None,
        )
        now = datetime.now(timezone.utc)
        payload = {
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
            "game_date": opportunity.game_date,
            "policy_name": opportunity.policy_name,
            "status": opportunity.status,
            "prediction_value": opportunity.prediction_value,
            "signal_strength": opportunity.signal_strength,
            "evidence_rating": opportunity.evidence_rating,
            "recommendation_status": opportunity.recommendation_status,
            "payload": opportunity.payload,
            "updated_at": now,
        }
        if existing is None:
            payload["id"] = len(repository.model_opportunities) + 1
            payload["created_at"] = now
            repository.model_opportunities.append(payload)
            persisted.append(ModelOpportunityRecord(**payload))
        else:
            existing.update(payload)
            persisted.append(ModelOpportunityRecord(**existing))
    return persisted


def save_model_opportunities_postgres(
    connection: Any,
    opportunities: list[ModelOpportunityRecord],
) -> list[ModelOpportunityRecord]:
    persisted: list[ModelOpportunityRecord] = []
    with connection.cursor() as cursor:
        for opportunity in opportunities:
            cursor.execute(
                """
                INSERT INTO model_opportunity (
                    model_scoring_run_id,
                    model_selection_snapshot_id,
                    model_evaluation_snapshot_id,
                    feature_version_id,
                    target_task,
                    source_kind,
                    scenario_key,
                    opportunity_key,
                    team_code,
                    opponent_code,
                    season_label,
                    canonical_game_id,
                    game_date,
                    policy_name,
                    status,
                    prediction_value,
                    signal_strength,
                    evidence_rating,
                    recommendation_status,
                    payload_json
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
                )
                ON CONFLICT (opportunity_key)
                DO UPDATE SET
                    model_scoring_run_id = EXCLUDED.model_scoring_run_id,
                    model_selection_snapshot_id = EXCLUDED.model_selection_snapshot_id,
                    model_evaluation_snapshot_id = EXCLUDED.model_evaluation_snapshot_id,
                    feature_version_id = EXCLUDED.feature_version_id,
                    target_task = EXCLUDED.target_task,
                    source_kind = EXCLUDED.source_kind,
                    scenario_key = EXCLUDED.scenario_key,
                    team_code = EXCLUDED.team_code,
                    opponent_code = EXCLUDED.opponent_code,
                    season_label = EXCLUDED.season_label,
                    canonical_game_id = EXCLUDED.canonical_game_id,
                    game_date = EXCLUDED.game_date,
                    policy_name = EXCLUDED.policy_name,
                    status = EXCLUDED.status,
                    prediction_value = EXCLUDED.prediction_value,
                    signal_strength = EXCLUDED.signal_strength,
                    evidence_rating = EXCLUDED.evidence_rating,
                    recommendation_status = EXCLUDED.recommendation_status,
                    payload_json = EXCLUDED.payload_json,
                    updated_at = NOW()
                RETURNING id, created_at, updated_at
                """,
                (
                    opportunity.model_scoring_run_id,
                    opportunity.model_selection_snapshot_id,
                    opportunity.model_evaluation_snapshot_id,
                    opportunity.feature_version_id,
                    opportunity.target_task,
                    opportunity.source_kind,
                    opportunity.scenario_key,
                    opportunity.opportunity_key,
                    opportunity.team_code,
                    opportunity.opponent_code,
                    opportunity.season_label,
                    opportunity.canonical_game_id,
                    opportunity.game_date,
                    opportunity.policy_name,
                    opportunity.status,
                    opportunity.prediction_value,
                    opportunity.signal_strength,
                    opportunity.evidence_rating,
                    opportunity.recommendation_status,
                    _json_dumps(opportunity.payload),
                ),
            )
            row = cursor.fetchone()
            persisted.append(
                ModelOpportunityRecord(
                    id=int(row[0]),
                    model_scoring_run_id=opportunity.model_scoring_run_id,
                    model_selection_snapshot_id=opportunity.model_selection_snapshot_id,
                    model_evaluation_snapshot_id=opportunity.model_evaluation_snapshot_id,
                    feature_version_id=opportunity.feature_version_id,
                    target_task=opportunity.target_task,
                    source_kind=opportunity.source_kind,
                    scenario_key=opportunity.scenario_key,
                    opportunity_key=opportunity.opportunity_key,
                    team_code=opportunity.team_code,
                    opponent_code=opportunity.opponent_code,
                    season_label=opportunity.season_label,
                    canonical_game_id=opportunity.canonical_game_id,
                    game_date=opportunity.game_date,
                    policy_name=opportunity.policy_name,
                    status=opportunity.status,
                    prediction_value=opportunity.prediction_value,
                    signal_strength=opportunity.signal_strength,
                    evidence_rating=opportunity.evidence_rating,
                    recommendation_status=opportunity.recommendation_status,
                    payload=opportunity.payload,
                    created_at=row[1],
                    updated_at=row[2],
                )
            )
    connection.commit()
    return persisted


def list_model_opportunities_in_memory(
    repository: ModelOpportunityStore,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    status: str | None = None,
    season_label: str | None = None,
    source_kind: str | None = None,
    scenario_key: str | None = None,
) -> list[ModelOpportunityRecord]:
    selected = [
        ModelOpportunityRecord(**entry)
        for entry in repository.model_opportunities
        if (target_task is None or entry["target_task"] == target_task)
        and (
            team_code is None
            or entry["team_code"] == team_code
            or entry["opponent_code"] == team_code
        )
        and (status is None or entry["status"] == status)
        and (season_label is None or entry["season_label"] == season_label)
        and (source_kind is None or entry["source_kind"] == source_kind)
        and (scenario_key is None or entry.get("scenario_key") == scenario_key)
    ]
    return sorted(
        selected,
        key=lambda entry: (
            entry.created_at or datetime.min.replace(tzinfo=timezone.utc),
            entry.id,
        ),
        reverse=True,
    )


def list_model_opportunities_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    status: str | None = None,
    season_label: str | None = None,
    source_kind: str | None = None,
    scenario_key: str | None = None,
) -> list[ModelOpportunityRecord]:
    query = """
        SELECT
            id,
            model_scoring_run_id,
            model_selection_snapshot_id,
            model_evaluation_snapshot_id,
            feature_version_id,
            target_task,
            source_kind,
            scenario_key,
            opportunity_key,
            team_code,
            opponent_code,
            season_label,
            canonical_game_id,
            game_date,
            policy_name,
            status,
            prediction_value,
            signal_strength,
            evidence_rating,
            recommendation_status,
            payload_json,
            created_at,
            updated_at
        FROM model_opportunity
        WHERE 1=1
    """
    params: list[Any] = []
    if target_task is not None:
        query += " AND target_task = %s"
        params.append(target_task)
    if team_code is not None:
        query += " AND (team_code = %s OR opponent_code = %s)"
        params.extend([team_code, team_code])
    if status is not None:
        query += " AND status = %s"
        params.append(status)
    if season_label is not None:
        query += " AND season_label = %s"
        params.append(season_label)
    if source_kind is not None:
        query += " AND source_kind = %s"
        params.append(source_kind)
    if scenario_key is not None:
        query += " AND scenario_key = %s"
        params.append(scenario_key)
    query += " ORDER BY created_at DESC, id DESC"
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        ModelOpportunityRecord(
            id=int(row[0]),
            model_scoring_run_id=int(row[1]) if row[1] is not None else None,
            model_selection_snapshot_id=int(row[2]) if row[2] is not None else None,
            model_evaluation_snapshot_id=int(row[3]) if row[3] is not None else None,
            feature_version_id=int(row[4]),
            target_task=row[5],
            source_kind=row[6],
            scenario_key=row[7],
            opportunity_key=row[8],
            team_code=row[9],
            opponent_code=row[10],
            season_label=row[11],
            canonical_game_id=int(row[12]) if row[12] is not None else None,
            game_date=row[13],
            policy_name=row[14],
            status=row[15],
            prediction_value=float(row[16]),
            signal_strength=float(row[17]),
            evidence_rating=row[18],
            recommendation_status=row[19],
            payload=row[20],
            created_at=row[21],
            updated_at=row[22],
        )
        for row in rows
    ]


def get_model_opportunity_detail_in_memory(
    repository: ModelOpportunityStore,
    *,
    opportunity_id: int,
) -> dict[str, Any] | None:
    opportunity = next(
        (
            entry
            for entry in list_model_opportunities_in_memory(repository)
            if entry.id == opportunity_id
        ),
        None,
    )
    return _serialize_model_opportunity(opportunity)


def get_model_opportunity_detail_postgres(
    connection: Any,
    *,
    opportunity_id: int,
) -> dict[str, Any] | None:
    opportunity = next(
        (
            entry
            for entry in list_model_opportunities_postgres(connection)
            if entry.id == opportunity_id
        ),
        None,
    )
    return _serialize_model_opportunity(opportunity)


def get_model_opportunity_history_in_memory(
    repository: ModelOpportunityStore,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    source_kind: str | None = None,
    scenario_key: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    opportunities = list_model_opportunities_in_memory(
        repository,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        source_kind=source_kind,
        scenario_key=scenario_key,
    )
    return summarize_model_opportunity_history(opportunities, recent_limit=recent_limit)


def get_model_opportunity_history_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    source_kind: str | None = None,
    scenario_key: str | None = None,
    recent_limit: int = 10,
) -> dict[str, Any]:
    opportunities = list_model_opportunities_postgres(
        connection,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        source_kind=source_kind,
        scenario_key=scenario_key,
    )
    return summarize_model_opportunity_history(opportunities, recent_limit=recent_limit)


def summarize_model_opportunity_history(
    opportunities: list[ModelOpportunityRecord],
    *,
    recent_limit: int,
) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    policy_counts: dict[str, int] = {}
    rating_counts: dict[str, int] = {}
    source_kind_counts: dict[str, int] = {}
    daily_buckets: dict[str, dict[str, Any]] = {}
    for opportunity in opportunities:
        status_counts[opportunity.status] = status_counts.get(opportunity.status, 0) + 1
        policy_counts[opportunity.policy_name] = policy_counts.get(opportunity.policy_name, 0) + 1
        source_kind_counts[opportunity.source_kind] = (
            source_kind_counts.get(opportunity.source_kind, 0) + 1
        )
        if opportunity.evidence_rating is not None:
            rating_counts[opportunity.evidence_rating] = (
                rating_counts.get(opportunity.evidence_rating, 0) + 1
            )
        created_at = opportunity.created_at or opportunity.updated_at
        if created_at is None:
            continue
        day_key = created_at.date().isoformat()
        bucket = daily_buckets.setdefault(
            day_key,
            {
                "date": day_key,
                "opportunity_count": 0,
                "status_counts": {},
                "max_signal_strength": None,
            },
        )
        bucket["opportunity_count"] += 1
        bucket_status_counts = bucket["status_counts"]
        bucket_status_counts[opportunity.status] = (
            bucket_status_counts.get(opportunity.status, 0) + 1
        )
        current_max = bucket["max_signal_strength"]
        if current_max is None or float(opportunity.signal_strength) > float(current_max):
            bucket["max_signal_strength"] = opportunity.signal_strength
    return {
        "overview": {
            "opportunity_count": len(opportunities),
            "status_counts": status_counts,
            "policy_counts": policy_counts,
            "source_kind_counts": source_kind_counts,
            "evidence_rating_counts": rating_counts,
            "latest_opportunity": _serialize_model_opportunity(
                opportunities[0] if opportunities else None
            ),
        },
        "daily_buckets": [daily_buckets[key] for key in sorted(daily_buckets.keys())],
        "recent_opportunities": [
            _serialize_model_opportunity(entry) for entry in opportunities[:recent_limit]
        ],
    }


def positive_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    integer_value = int(value)
    return integer_value if integer_value > 0 else None
