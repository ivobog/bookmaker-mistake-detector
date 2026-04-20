from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

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
    materialization_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    opportunities = build_opportunities(
        scoring_preview=scoring_preview,
        target_task=target_task,
        materialization_context=materialization_context,
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
    materialization_context: dict[str, Any] | None = None,
) -> list[ModelOpportunityRecord]:
    active_selection = scoring_preview.get("active_selection")
    active_snapshot = scoring_preview.get("active_evaluation_snapshot")
    feature_version = scoring_preview.get("feature_version")
    scenario = scoring_preview.get("scenario")
    if active_snapshot is None or active_selection is None or feature_version is None:
        return []
    if policy is None:
        raise ValueError(f"Unsupported opportunity policy target_task: {target_task}")
    resolved_materialization_context = materialization_context or build_materialization_context()
    opportunities = []
    predictions = list(scoring_preview.get("predictions", []))
    materializable_predictions = [
        (prediction, evaluate_opportunity_status(prediction=prediction, policy=policy))
        for prediction in predictions
    ]
    for prediction, status in _select_materialized_predictions(
        materializable_predictions,
        scenario=scenario,
    ):
        canonical_game_id = positive_int_or_none(prediction.get("canonical_game_id"))
        source_kind = "future_scenario" if scenario is not None else "historical_game"
        scenario_key = str(scenario["scenario_key"]) if scenario is not None else None
        canonical_team_code, canonical_opponent_code = _resolve_opportunity_matchup(
            prediction=prediction,
            scenario=scenario,
        )
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
                    policy_name=str(policy["policy_name"]),
                ),
                team_code=canonical_team_code,
                opponent_code=canonical_opponent_code,
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
                materialization_batch_id=str(
                    resolved_materialization_context["materialization_batch_id"]
                ),
                materialized_at=resolved_materialization_context["materialized_at"],
                materialization_scope_team_code=resolved_materialization_context[
                    "materialization_scope_team_code"
                ],
                materialization_scope_season_label=resolved_materialization_context[
                    "materialization_scope_season_label"
                ],
                materialization_scope_canonical_game_id=resolved_materialization_context[
                    "materialization_scope_canonical_game_id"
                ],
                materialization_scope_source=str(
                    resolved_materialization_context["materialization_scope_source"]
                ),
                materialization_scope_key=str(
                    resolved_materialization_context["materialization_scope_key"]
                ),
                payload=payload,
            )
        )
    if not opportunities and allow_best_effort_review and predictions:
        strongest_prediction = max(
            predictions,
            key=_prediction_preference_key,
        )
        canonical_game_id = positive_int_or_none(strongest_prediction.get("canonical_game_id"))
        source_kind = "future_scenario" if scenario is not None else "historical_game"
        scenario_key = str(scenario["scenario_key"]) if scenario is not None else None
        canonical_team_code, canonical_opponent_code = _resolve_opportunity_matchup(
            prediction=strongest_prediction,
            scenario=scenario,
        )
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
                    policy_name=str(policy["policy_name"]),
                ),
                team_code=canonical_team_code,
                opponent_code=canonical_opponent_code,
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
                materialization_batch_id=str(
                    resolved_materialization_context["materialization_batch_id"]
                ),
                materialized_at=resolved_materialization_context["materialized_at"],
                materialization_scope_team_code=resolved_materialization_context[
                    "materialization_scope_team_code"
                ],
                materialization_scope_season_label=resolved_materialization_context[
                    "materialization_scope_season_label"
                ],
                materialization_scope_canonical_game_id=resolved_materialization_context[
                    "materialization_scope_canonical_game_id"
                ],
                materialization_scope_source=str(
                    resolved_materialization_context["materialization_scope_source"]
                ),
                materialization_scope_key=str(
                    resolved_materialization_context["materialization_scope_key"]
                ),
                payload=payload,
            )
        )
    return opportunities


def build_materialization_context(
    *,
    team_code: str | None = None,
    season_label: str | None = None,
    canonical_game_id: int | None = None,
    scope_source: str | None = None,
    scope_key: str | None = None,
    batch_id: str | None = None,
    materialized_at: datetime | None = None,
) -> dict[str, Any]:
    resolved_source = _resolve_materialization_scope_source(
        team_code=team_code,
        canonical_game_id=canonical_game_id,
        scope_source=scope_source,
    )
    return {
        "materialization_batch_id": batch_id or uuid4().hex,
        "materialized_at": materialized_at or datetime.now(timezone.utc),
        "materialization_scope_team_code": team_code,
        "materialization_scope_season_label": season_label,
        "materialization_scope_canonical_game_id": canonical_game_id,
        "materialization_scope_source": resolved_source,
        "materialization_scope_key": scope_key
        or _build_materialization_scope_key(
            team_code=team_code,
            season_label=season_label,
            canonical_game_id=canonical_game_id,
            scope_source=resolved_source,
        ),
    }


def _resolve_materialization_scope_source(
    *,
    team_code: str | None,
    canonical_game_id: int | None,
    scope_source: str | None,
) -> str:
    if scope_source is not None:
        return scope_source
    if canonical_game_id is not None:
        return "game_scoped"
    if team_code is not None:
        return "team_scoped"
    return "operator"


def _build_materialization_scope_key(
    *,
    team_code: str | None,
    season_label: str | None,
    canonical_game_id: int | None,
    scope_source: str,
) -> str:
    if scope_source == "operator" and team_code is None and canonical_game_id is None:
        return "operator-wide"
    parts: list[str] = []
    if team_code is not None:
        parts.append(f"team={team_code}")
    if season_label is not None:
        parts.append(f"season={season_label}")
    if canonical_game_id is not None:
        parts.append(f"game={canonical_game_id}")
    if parts:
        return "|".join(parts)
    return scope_source


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
    policy_name: str,
) -> str:
    subject_key = _build_opportunity_subject_key(
        canonical_game_id=canonical_game_id,
        scenario_key=scenario_key,
    )
    return f"{target_task}:{subject_key}:{policy_name}"


def nested_get(payload: dict[str, Any] | None, *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


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
                    materialization_batch_id,
                    materialized_at,
                    materialization_scope_team_code,
                    materialization_scope_season_label,
                    materialization_scope_canonical_game_id,
                    materialization_scope_source,
                    materialization_scope_key,
                    payload_json
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s::jsonb
                )
                ON CONFLICT (materialization_batch_id, opportunity_key)
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
                    materialized_at = EXCLUDED.materialized_at,
                    materialization_scope_team_code =
                        EXCLUDED.materialization_scope_team_code,
                    materialization_scope_season_label =
                        EXCLUDED.materialization_scope_season_label,
                    materialization_scope_canonical_game_id =
                        EXCLUDED.materialization_scope_canonical_game_id,
                    materialization_scope_source = EXCLUDED.materialization_scope_source,
                    materialization_scope_key = EXCLUDED.materialization_scope_key,
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
                    opportunity.materialization_batch_id,
                    opportunity.materialized_at,
                    opportunity.materialization_scope_team_code,
                    opportunity.materialization_scope_season_label,
                    opportunity.materialization_scope_canonical_game_id,
                    opportunity.materialization_scope_source,
                    opportunity.materialization_scope_key,
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
                    materialization_batch_id=opportunity.materialization_batch_id,
                    materialized_at=opportunity.materialized_at,
                    materialization_scope_team_code=opportunity.materialization_scope_team_code,
                    materialization_scope_season_label=(
                        opportunity.materialization_scope_season_label
                    ),
                    materialization_scope_canonical_game_id=(
                        opportunity.materialization_scope_canonical_game_id
                    ),
                    materialization_scope_source=opportunity.materialization_scope_source,
                    materialization_scope_key=opportunity.materialization_scope_key,
                    payload=opportunity.payload,
                    created_at=row[1],
                    updated_at=row[2],
                )
            )
    connection.commit()
    return persisted


def list_model_opportunities_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    status: str | None = None,
    season_label: str | None = None,
    source_kind: str | None = None,
    scenario_key: str | None = None,
    materialization_batch_id: str | None = None,
    latest_batch_only: bool = False,
) -> list[ModelOpportunityRecord]:
    resolved_batch_id = materialization_batch_id
    if latest_batch_only and resolved_batch_id is None:
        batch_anchor = _resolve_latest_opportunity_batch_postgres(
            connection,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
        )
        if batch_anchor is None:
            return []
        resolved_batch_id = str(batch_anchor["materialization_batch_id"])
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
            materialization_batch_id,
            materialized_at,
            materialization_scope_team_code,
            materialization_scope_season_label,
            materialization_scope_canonical_game_id,
            materialization_scope_source,
            materialization_scope_key,
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
    if resolved_batch_id is not None:
        query += " AND materialization_batch_id = %s"
        params.append(resolved_batch_id)
    query += " ORDER BY created_at DESC, id DESC"
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    opportunities = [
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
            materialization_batch_id=row[20],
            materialized_at=row[21],
            materialization_scope_team_code=row[22],
            materialization_scope_season_label=row[23],
            materialization_scope_canonical_game_id=(
                int(row[24]) if row[24] is not None else None
            ),
            materialization_scope_source=row[25],
            materialization_scope_key=row[26],
            payload=row[27],
            created_at=row[28],
            updated_at=row[29],
        )
        for row in rows
    ]
    return _dedupe_materialized_opportunities(opportunities)


def get_model_opportunity_queue_postgres(
    connection: Any,
    *,
    target_task: str | None = None,
    team_code: str | None = None,
    status: str | None = None,
    season_label: str | None = None,
    source_kind: str | None = None,
    scenario_key: str | None = None,
) -> dict[str, Any]:
    batch_anchor = _resolve_latest_opportunity_batch_postgres(
        connection,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
    )
    if batch_anchor is None:
        return _build_model_opportunity_queue_result(None, [])
    opportunities = list_model_opportunities_postgres(
        connection,
        target_task=target_task,
        team_code=team_code,
        status=status,
        season_label=season_label,
        source_kind=source_kind,
        scenario_key=scenario_key,
        materialization_batch_id=str(batch_anchor["materialization_batch_id"]),
    )
    return _build_model_opportunity_queue_result(batch_anchor, opportunities)


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


def _matches_opportunity_filters(
    entry: dict[str, Any],
    *,
    target_task: str | None,
    team_code: str | None,
    status: str | None,
    season_label: str | None,
    source_kind: str | None,
    scenario_key: str | None,
    materialization_batch_id: str | None,
) -> bool:
    if target_task is not None and entry["target_task"] != target_task:
        return False
    if (
        team_code is not None
        and entry["team_code"] != team_code
        and entry["opponent_code"] != team_code
    ):
        return False
    if status is not None and entry["status"] != status:
        return False
    if season_label is not None and entry["season_label"] != season_label:
        return False
    if source_kind is not None and entry["source_kind"] != source_kind:
        return False
    if scenario_key is not None and entry.get("scenario_key") != scenario_key:
        return False
    if (
        materialization_batch_id is not None
        and entry.get("materialization_batch_id") != materialization_batch_id
    ):
        return False
    return True


def _resolve_latest_opportunity_batch_postgres(
    connection: Any,
    *,
    target_task: str | None,
    team_code: str | None,
    season_label: str | None,
) -> dict[str, Any] | None:
    query = """
        SELECT
            materialization_batch_id,
            materialized_at,
            materialization_scope_team_code,
            materialization_scope_season_label,
            materialization_scope_canonical_game_id,
            materialization_scope_source,
            materialization_scope_key,
            MAX(id) AS max_id
        FROM model_opportunity
        WHERE 1=1
    """
    params: list[Any] = []
    if target_task is not None:
        query += " AND target_task = %s"
        params.append(target_task)
    if team_code is None:
        query += " AND materialization_scope_source = %s"
        params.append("operator")
    else:
        query += " AND materialization_scope_team_code = %s"
        params.append(team_code)
        query += " AND materialization_scope_source IN (%s, %s)"
        params.extend(["team_scoped", "game_scoped"])
        if season_label is not None:
            query += " AND materialization_scope_season_label = %s"
            params.append(season_label)
    query += """
        GROUP BY
            materialization_batch_id,
            materialized_at,
            materialization_scope_team_code,
            materialization_scope_season_label,
            materialization_scope_canonical_game_id,
            materialization_scope_source,
            materialization_scope_key
        ORDER BY materialized_at DESC, max_id DESC
        LIMIT 1
    """
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        row = cursor.fetchone()
    if row is None:
        return None
    return {
        "materialization_batch_id": row[0],
        "materialized_at": row[1],
        "materialization_scope_team_code": row[2],
        "materialization_scope_season_label": row[3],
        "materialization_scope_canonical_game_id": (
            int(row[4]) if row[4] is not None else None
        ),
        "materialization_scope_source": row[5],
        "materialization_scope_key": row[6],
        "id": int(row[7]),
    }


def _matches_materialization_scope(
    entry: dict[str, Any],
    *,
    target_task: str | None,
    team_code: str | None,
    season_label: str | None,
) -> bool:
    if target_task is not None and entry["target_task"] != target_task:
        return False
    scope_source = entry.get("materialization_scope_source")
    if team_code is None:
        return scope_source == "operator"
    if entry.get("materialization_scope_team_code") != team_code:
        return False
    if scope_source not in {"team_scoped", "game_scoped"}:
        return False
    if (
        season_label is not None
        and entry.get("materialization_scope_season_label") != season_label
    ):
        return False
    return True


def _build_model_opportunity_queue_result(
    batch_anchor: dict[str, Any] | None,
    opportunities: list[ModelOpportunityRecord],
) -> dict[str, Any]:
    scope_payload = (
        {
            "team_code": batch_anchor.get("materialization_scope_team_code"),
            "season_label": batch_anchor.get("materialization_scope_season_label"),
            "canonical_game_id": batch_anchor.get("materialization_scope_canonical_game_id"),
            "source": batch_anchor.get("materialization_scope_source"),
            "scope_key": batch_anchor.get("materialization_scope_key"),
        }
        if batch_anchor is not None
        else {
            "team_code": None,
            "season_label": None,
            "canonical_game_id": None,
            "source": None,
            "scope_key": None,
        }
    )
    return {
        "queue_batch_id": batch_anchor.get("materialization_batch_id") if batch_anchor else None,
        "queue_materialized_at": (
            batch_anchor.get("materialized_at").isoformat()
            if batch_anchor is not None and batch_anchor.get("materialized_at") is not None
            else None
        ),
        "queue_scope": scope_payload,
        "queue_scope_label": build_materialization_scope_label(scope_payload),
        "queue_scope_is_scoped": bool(
            batch_anchor is not None
            and batch_anchor.get("materialization_scope_source") != "operator"
        ),
        "opportunities": opportunities,
    }


def build_materialization_scope_label(scope_payload: dict[str, Any] | None) -> str | None:
    if scope_payload is None:
        return None
    scope_source = scope_payload.get("source")
    if scope_source == "operator":
        return "Operator-wide queue"
    if scope_source is None:
        return None
    parts: list[str] = []
    if scope_payload.get("team_code") is not None:
        parts.append(f"team={scope_payload['team_code']}")
    if scope_payload.get("season_label") is not None:
        parts.append(f"season={scope_payload['season_label']}")
    if scope_payload.get("canonical_game_id") is not None:
        parts.append(f"game={scope_payload['canonical_game_id']}")
    if not parts and scope_payload.get("scope_key") is not None:
        parts.append(str(scope_payload["scope_key"]))
    return "Scoped queue: " + ", ".join(parts) if parts else "Scoped queue"


def _opportunity_sort_key(entry: ModelOpportunityRecord) -> tuple[datetime, datetime, int]:
    minimum = datetime.min.replace(tzinfo=timezone.utc)
    return (
        entry.materialized_at or minimum,
        entry.created_at or minimum,
        entry.id,
    )


def _select_materialized_predictions(
    predictions_with_status: list[tuple[dict[str, Any], str]],
    *,
    scenario: dict[str, Any] | None,
) -> list[tuple[dict[str, Any], str]]:
    selected_by_subject: dict[str, tuple[dict[str, Any], str]] = {}
    for prediction, status in predictions_with_status:
        if status == "discarded":
            continue
        subject_key = _build_prediction_subject_key(
            prediction=prediction,
            scenario=scenario,
        )
        current = selected_by_subject.get(subject_key)
        if current is None or _prediction_with_status_preference_key(
            prediction,
            status,
        ) > _prediction_with_status_preference_key(*current):
            selected_by_subject[subject_key] = (prediction, status)
    return list(selected_by_subject.values())


def _build_prediction_subject_key(
    *,
    prediction: dict[str, Any],
    scenario: dict[str, Any] | None,
) -> str:
    return _build_opportunity_subject_key(
        canonical_game_id=positive_int_or_none(prediction.get("canonical_game_id")),
        scenario_key=(str(scenario["scenario_key"]) if scenario is not None else None),
        prediction=prediction,
    )


def _build_record_subject_key(entry: ModelOpportunityRecord) -> str:
    return _build_opportunity_subject_key(
        canonical_game_id=entry.canonical_game_id,
        scenario_key=entry.scenario_key,
        prediction={
            "game_date": entry.game_date.isoformat(),
            "team_code": entry.team_code,
            "opponent_code": entry.opponent_code,
        },
    )


def _build_opportunity_subject_key(
    *,
    canonical_game_id: int | None,
    scenario_key: str | None,
    prediction: dict[str, Any] | None = None,
) -> str:
    if scenario_key is not None:
        return f"scenario:{scenario_key}"
    if canonical_game_id is not None:
        return f"game:{canonical_game_id}"
    if prediction is None:
        return "unknown"
    team_code = str(prediction.get("team_code") or "")
    opponent_code = str(prediction.get("opponent_code") or "")
    ordered_matchup = ":".join(sorted((team_code, opponent_code)))
    return f"matchup:{prediction.get('game_date')}:{ordered_matchup}"


def _dedupe_materialized_opportunities(
    opportunities: list[ModelOpportunityRecord],
) -> list[ModelOpportunityRecord]:
    selected_by_subject: dict[tuple[str, str], ModelOpportunityRecord] = {}
    for opportunity in opportunities:
        normalized = _normalize_materialized_opportunity_matchup(opportunity)
        subject_key = (
            normalized.materialization_batch_id,
            _build_record_subject_key(normalized),
        )
        current = selected_by_subject.get(subject_key)
        if current is None or _record_preference_key(normalized) > _record_preference_key(
            current
        ):
            selected_by_subject[subject_key] = normalized
    return list(selected_by_subject.values())


def _normalize_materialized_opportunity_matchup(
    opportunity: ModelOpportunityRecord,
) -> ModelOpportunityRecord:
    canonical_team_code, canonical_opponent_code = _resolve_opportunity_matchup(
        prediction=_extract_prediction_payload(opportunity),
        scenario=_extract_scenario_payload(opportunity),
        fallback_team_code=opportunity.team_code,
        fallback_opponent_code=opportunity.opponent_code,
    )
    if (
        canonical_team_code == opportunity.team_code
        and canonical_opponent_code == opportunity.opponent_code
    ):
        return opportunity
    return replace(
        opportunity,
        team_code=canonical_team_code,
        opponent_code=canonical_opponent_code,
    )


def _extract_prediction_payload(opportunity: ModelOpportunityRecord) -> dict[str, Any]:
    payload = opportunity.payload if isinstance(opportunity.payload, dict) else {}
    prediction = payload.get("prediction")
    return prediction if isinstance(prediction, dict) else {}


def _extract_scenario_payload(
    opportunity: ModelOpportunityRecord,
) -> dict[str, Any] | None:
    payload = opportunity.payload if isinstance(opportunity.payload, dict) else {}
    scenario = payload.get("scenario")
    return scenario if isinstance(scenario, dict) else None


def _resolve_opportunity_matchup(
    *,
    prediction: dict[str, Any],
    scenario: dict[str, Any] | None,
    fallback_team_code: str | None = None,
    fallback_opponent_code: str | None = None,
) -> tuple[str, str]:
    if scenario is not None:
        home_team_code = scenario.get("home_team_code")
        away_team_code = scenario.get("away_team_code")
        if home_team_code is not None and away_team_code is not None:
            return str(home_team_code), str(away_team_code)
    team_code = str(prediction.get("team_code") or fallback_team_code or "")
    opponent_code = str(prediction.get("opponent_code") or fallback_opponent_code or "")
    venue = prediction.get("venue")
    if venue == "home":
        return team_code, opponent_code
    if venue == "away":
        return opponent_code, team_code
    return team_code, opponent_code


def _prediction_with_status_preference_key(
    prediction: dict[str, Any],
    status: str,
) -> tuple[int, float, int, str]:
    return (
        _status_priority(status),
        _prediction_preference_key(prediction),
        1 if prediction.get("venue") == "home" else 0,
        str(prediction.get("team_code") or ""),
    )


def _prediction_preference_key(prediction: dict[str, Any]) -> float:
    return float(prediction.get("signal_strength", 0.0))


def _record_preference_key(entry: ModelOpportunityRecord) -> tuple[int, float, int, int]:
    created_at_rank = int(entry.created_at.timestamp()) if entry.created_at is not None else -1
    return (
        _status_priority(entry.status),
        float(entry.signal_strength),
        created_at_rank,
        entry.id,
    )


def _status_priority(status: str | None) -> int:
    if status == "candidate_signal":
        return 2
    if status == "review_manually":
        return 1
    return 0


def _opportunity_entry_sort_key(entry: dict[str, Any]) -> tuple[datetime, datetime, int]:
    minimum = datetime.min.replace(tzinfo=timezone.utc)
    materialized_at = entry.get("materialized_at") or minimum
    created_at = entry.get("created_at") or minimum
    return (materialized_at, created_at, int(entry.get("id", 0)))
