from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timezone
from statistics import mean, median, pstdev
from typing import Any

from bookmaker_detector_api.repositories import FeatureDatasetStore
from bookmaker_detector_api.repositories.ingestion_json import _json_dumps
from bookmaker_detector_api.services.feature_evidence_scoring import (
    build_evidence_recommendation,
    build_evidence_strength_summary,
)
from bookmaker_detector_api.services.feature_records import (
    CanonicalGameMetricRecord,
    FeatureAnalysisArtifactRecord,
    FeatureSnapshotRecord,
    FeatureVersionRecord,
    TeamPerspectiveGame,
)

DEFAULT_FEATURE_KEY = "baseline_team_features_v1"
DEFAULT_FEATURE_WINDOWS = (3, 5, 10)
FEATURE_DATASET_LABEL_COLUMNS = (
    "point_margin_actual",
    "spread_error_actual",
    "covered_actual",
    "total_error_actual",
    "went_over_actual",
    "total_points_actual",
)
FEATURE_TRAINING_METADATA_COLUMNS = (
    "canonical_game_id",
    "season_label",
    "game_date",
    "team_code",
    "opponent_code",
    "venue",
)
FEATURE_TRAINING_TASKS = {
    "point_margin_regression": {
        "task_type": "regression",
        "target_column": "point_margin_actual",
    },
    "spread_error_regression": {
        "task_type": "regression",
        "target_column": "spread_error_actual",
    },
    "total_error_regression": {
        "task_type": "regression",
        "target_column": "total_error_actual",
    },
    "total_points_regression": {
        "task_type": "regression",
        "target_column": "total_points_actual",
    },
    "cover_classification": {
        "task_type": "classification",
        "target_column": "covered_actual",
    },
    "over_classification": {
        "task_type": "classification",
        "target_column": "went_over_actual",
    },
}
FEATURE_EVIDENCE_RECOMMENDATION_POLICIES = {
    "point_margin_regression": {
        "policy_name": "regression_margin_policy_v1",
        "candidate_min_overall_score": 0.72,
        "review_min_overall_score": 0.48,
        "candidate_min_pattern_sample": 5,
        "review_min_pattern_sample": 3,
        "candidate_min_comparables": 3,
        "review_min_comparables": 1,
        "candidate_min_benchmark_stability": 0.65,
    },
    "spread_error_regression": {
        "policy_name": "regression_error_policy_v1",
        "candidate_min_overall_score": 0.7,
        "review_min_overall_score": 0.45,
        "candidate_min_pattern_sample": 4,
        "review_min_pattern_sample": 3,
        "candidate_min_comparables": 2,
        "review_min_comparables": 1,
        "candidate_min_benchmark_stability": 0.6,
    },
    "total_error_regression": {
        "policy_name": "regression_error_policy_v1",
        "candidate_min_overall_score": 0.7,
        "review_min_overall_score": 0.45,
        "candidate_min_pattern_sample": 4,
        "review_min_pattern_sample": 3,
        "candidate_min_comparables": 2,
        "review_min_comparables": 1,
        "candidate_min_benchmark_stability": 0.6,
    },
    "total_points_regression": {
        "policy_name": "regression_totals_policy_v1",
        "candidate_min_overall_score": 0.72,
        "review_min_overall_score": 0.48,
        "candidate_min_pattern_sample": 5,
        "review_min_pattern_sample": 3,
        "candidate_min_comparables": 2,
        "review_min_comparables": 1,
        "candidate_min_benchmark_stability": 0.65,
    },
    "cover_classification": {
        "policy_name": "classification_cover_policy_v1",
        "candidate_min_overall_score": 0.68,
        "review_min_overall_score": 0.42,
        "candidate_min_pattern_sample": 4,
        "review_min_pattern_sample": 2,
        "candidate_min_comparables": 2,
        "review_min_comparables": 1,
        "candidate_min_benchmark_stability": 0.55,
    },
    "over_classification": {
        "policy_name": "classification_total_policy_v1",
        "candidate_min_overall_score": 0.68,
        "review_min_overall_score": 0.42,
        "candidate_min_pattern_sample": 4,
        "review_min_pattern_sample": 2,
        "candidate_min_comparables": 2,
        "review_min_comparables": 1,
        "candidate_min_benchmark_stability": 0.55,
    },
}
FEATURE_REGRESSION_BASELINE_FEATURES = {
    "point_margin_regression": {
        "rolling_3_feature_baseline": "rolling_3_avg_point_margin",
        "rolling_10_feature_baseline": "rolling_10_avg_point_margin",
    },
    "spread_error_regression": {
        "rolling_3_feature_baseline": "rolling_3_avg_spread_error",
        "rolling_10_feature_baseline": "rolling_10_avg_spread_error",
    },
    "total_error_regression": {
        "rolling_3_feature_baseline": "rolling_3_avg_total_error",
        "rolling_10_feature_baseline": "rolling_10_avg_total_error",
    },
    "total_points_regression": {
        "rolling_3_feature_baseline": "rolling_3_avg_total_points",
        "rolling_10_feature_baseline": "rolling_10_avg_total_points",
    },
}
FEATURE_CLASSIFICATION_BASELINE_FEATURES = {
    "cover_classification": {
        "rolling_3_rate_baseline": "rolling_3_cover_rate",
        "rolling_10_rate_baseline": "rolling_10_cover_rate",
    },
    "over_classification": {
        "rolling_3_rate_baseline": "rolling_3_over_rate",
        "rolling_10_rate_baseline": "rolling_10_over_rate",
    },
}
FEATURE_DATASET_PROFILE_COLUMNS = (
    "games_played_prior",
    "days_rest",
    "prior_matchup_count",
    "rolling_3_avg_point_margin",
    "rolling_3_avg_total_points",
    "rolling_3_avg_spread_error",
    "rolling_3_cover_rate",
    "rolling_5_avg_point_margin",
    "rolling_10_avg_point_margin",
    "point_margin_stddev",
    "spread_error_stddev",
    "current_cover_streak",
    "recent_point_margin_delta_3_vs_10",
)
FEATURE_COMPARABLE_DISTANCE_COLUMNS = (
    "days_rest",
    "games_played_prior",
    "prior_matchup_count",
    "rolling_3_avg_point_margin",
    "rolling_3_avg_total_points",
    "rolling_3_avg_spread_error",
    "rolling_3_avg_total_error",
    "point_margin_stddev",
    "spread_error_stddev",
)
FEATURE_PATTERN_DIMENSIONS = {
    "venue",
    "days_rest_bucket",
    "games_played_bucket",
    "prior_matchup_bucket",
    "rolling_3_spread_error_bucket",
    "rolling_3_total_error_bucket",
    "rolling_3_cover_rate_bucket",
    "rolling_3_over_rate_bucket",
}


def materialize_baseline_feature_snapshots_for_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    version_label: str = "Baseline Team Features v1",
    description: str = "Time-safe rolling team and matchup features for canonical games.",
    windows: tuple[int, ...] = DEFAULT_FEATURE_WINDOWS,
) -> dict[str, Any]:
    feature_version = ensure_feature_version_postgres(
        connection,
        feature_key=feature_key,
        version_label=version_label,
        description=description,
        config={"windows": list(windows)},
    )
    canonical_games = list_canonical_game_metric_records_postgres(connection)
    snapshots = build_feature_snapshots(
        canonical_games,
        feature_version_id=feature_version.id,
        windows=windows,
    )
    saved_count = save_feature_snapshots_postgres(connection, snapshots)
    persisted_snapshots = list_feature_snapshots_postgres(
        connection,
        feature_version_id=feature_version.id,
    )
    return {
        "feature_version": asdict(feature_version),
        "canonical_game_count": len(canonical_games),
        "snapshots_saved": saved_count,
        "feature_snapshots": [asdict(snapshot) for snapshot in persisted_snapshots],
    }


def build_feature_snapshots(
    canonical_games: list[CanonicalGameMetricRecord],
    *,
    feature_version_id: int,
    windows: tuple[int, ...] = DEFAULT_FEATURE_WINDOWS,
) -> list[FeatureSnapshotRecord]:
    sorted_games = sorted(
        canonical_games,
        key=lambda game: (game.game_date, game.canonical_game_id),
    )
    team_history: dict[str, list[TeamPerspectiveGame]] = {}
    matchup_history: dict[tuple[str, str], list[CanonicalGameMetricRecord]] = {}
    snapshots: list[FeatureSnapshotRecord] = []

    for game in sorted_games:
        matchup_key = tuple(sorted((game.home_team_code, game.away_team_code)))
        prior_home_games = team_history.get(game.home_team_code, [])
        prior_away_games = team_history.get(game.away_team_code, [])
        prior_matchups = matchup_history.get(matchup_key, [])

        home_payload = _build_team_feature_payload(
            team_code=game.home_team_code,
            prior_games=prior_home_games,
            current_game_date=game.game_date,
            current_season_label=game.season_label,
            windows=windows,
        )
        away_payload = _build_team_feature_payload(
            team_code=game.away_team_code,
            prior_games=prior_away_games,
            current_game_date=game.game_date,
            current_season_label=game.season_label,
            windows=windows,
        )
        feature_payload = {
            "feature_key": DEFAULT_FEATURE_KEY,
            "home_team": home_payload,
            "away_team": away_payload,
            "matchup": {
                "prior_matchup_count": len(prior_matchups),
                "season_prior_matchup_count": sum(
                    1 for prior in prior_matchups if prior.season_label == game.season_label
                ),
            },
        }
        snapshots.append(
            FeatureSnapshotRecord(
                id=0,
                canonical_game_id=game.canonical_game_id,
                feature_version_id=feature_version_id,
                season_label=game.season_label,
                game_date=game.game_date,
                home_team_code=game.home_team_code,
                away_team_code=game.away_team_code,
                feature_payload=feature_payload,
            )
        )

        team_history.setdefault(game.home_team_code, []).append(
            _to_team_perspective_game(game, team_code=game.home_team_code)
        )
        team_history.setdefault(game.away_team_code, []).append(
            _to_team_perspective_game(game, team_code=game.away_team_code)
        )
        matchup_history.setdefault(matchup_key, []).append(game)

    return snapshots


def build_future_feature_dataset_rows(
    canonical_games: list[CanonicalGameMetricRecord],
    *,
    feature_version_id: int,
    season_label: str,
    game_date: date,
    home_team_code: str,
    away_team_code: str,
    windows: tuple[int, ...] = DEFAULT_FEATURE_WINDOWS,
    home_spread_line: float | None = None,
    total_line: float | None = None,
) -> list[dict[str, Any]]:
    prior_games = sorted(
        [game for game in canonical_games if game.game_date < game_date],
        key=lambda game: (game.game_date, game.canonical_game_id),
    )
    team_history: dict[str, list[TeamPerspectiveGame]] = {}
    matchup_history: dict[tuple[str, str], list[CanonicalGameMetricRecord]] = {}
    for game in prior_games:
        matchup_key = tuple(sorted((game.home_team_code, game.away_team_code)))
        team_history.setdefault(game.home_team_code, []).append(
            _to_team_perspective_game(game, team_code=game.home_team_code)
        )
        team_history.setdefault(game.away_team_code, []).append(
            _to_team_perspective_game(game, team_code=game.away_team_code)
        )
        matchup_history.setdefault(matchup_key, []).append(game)

    matchup_key = tuple(sorted((home_team_code, away_team_code)))
    prior_matchups = matchup_history.get(matchup_key, [])
    home_payload = _build_team_feature_payload(
        team_code=home_team_code,
        prior_games=team_history.get(home_team_code, []),
        current_game_date=game_date,
        current_season_label=season_label,
        windows=windows,
    )
    away_payload = _build_team_feature_payload(
        team_code=away_team_code,
        prior_games=team_history.get(away_team_code, []),
        current_game_date=game_date,
        current_season_label=season_label,
        windows=windows,
    )
    away_spread_line = round(-float(home_spread_line), 4) if home_spread_line is not None else None
    scenario_key = f"{season_label}:{game_date.isoformat()}:{home_team_code}:{away_team_code}"
    return [
        _build_future_feature_dataset_row(
            feature_version_id=feature_version_id,
            scenario_key=scenario_key,
            season_label=season_label,
            game_date=game_date,
            team_code=home_team_code,
            opponent_code=away_team_code,
            venue="home",
            payload=home_payload,
            prior_matchups=prior_matchups,
            team_spread_line=home_spread_line,
            opponent_spread_line=away_spread_line,
            total_line=total_line,
        ),
        _build_future_feature_dataset_row(
            feature_version_id=feature_version_id,
            scenario_key=scenario_key,
            season_label=season_label,
            game_date=game_date,
            team_code=away_team_code,
            opponent_code=home_team_code,
            venue="away",
            payload=away_payload,
            prior_matchups=prior_matchups,
            team_spread_line=away_spread_line,
            opponent_spread_line=home_spread_line,
            total_line=total_line,
        ),
    ]


def ensure_feature_version_in_memory(
    repository: FeatureDatasetStore,
    *,
    feature_key: str,
    version_label: str,
    description: str,
    config: dict[str, Any],
) -> FeatureVersionRecord:
    for entry in repository.feature_versions:
        if entry["feature_key"] == feature_key:
            return FeatureVersionRecord(**entry)

    record = {
        "id": len(repository.feature_versions) + 1,
        "feature_key": feature_key,
        "version_label": version_label,
        "description": description,
        "config": config,
        "created_at": datetime.now(timezone.utc),
    }
    repository.feature_versions.append(record)
    return FeatureVersionRecord(**record)


def list_canonical_game_metric_records_in_memory(
    repository: FeatureDatasetStore,
) -> list[CanonicalGameMetricRecord]:
    metrics_by_game_id = {entry["canonical_game_id"]: entry for entry in repository.metrics}
    return [
        CanonicalGameMetricRecord(
            canonical_game_id=entry["id"],
            season_label=entry["season_label"],
            game_date=entry["game_date"],
            home_team_code=entry["home_team_code"],
            away_team_code=entry["away_team_code"],
            home_score=entry["home_score"],
            away_score=entry["away_score"],
            final_home_margin=entry["final_home_margin"],
            final_total_points=entry["final_total_points"],
            total_line=entry["total_line"],
            home_spread_line=entry["home_spread_line"],
            away_spread_line=entry["away_spread_line"],
            reconciliation_status=entry["reconciliation_status"],
            source_row_indexes=entry["source_row_indexes"],
            warnings=entry["warnings"],
            spread_error_home=metrics_by_game_id.get(entry["id"], {}).get("spread_error_home"),
            spread_error_away=metrics_by_game_id.get(entry["id"], {}).get("spread_error_away"),
            total_error=metrics_by_game_id.get(entry["id"], {}).get("total_error"),
            home_covered=metrics_by_game_id.get(entry["id"], {}).get("home_covered"),
            away_covered=metrics_by_game_id.get(entry["id"], {}).get("away_covered"),
            went_over=metrics_by_game_id.get(entry["id"], {}).get("went_over"),
            went_under=metrics_by_game_id.get(entry["id"], {}).get("went_under"),
        )
        for entry in repository.canonical_games
    ]


def save_feature_snapshots_in_memory(
    repository: FeatureDatasetStore,
    snapshots: list[FeatureSnapshotRecord],
) -> int:
    saved_count = 0
    for snapshot in snapshots:
        existing = next(
            (
                entry
                for entry in repository.feature_snapshots
                if entry["feature_version_id"] == snapshot.feature_version_id
                and entry["canonical_game_id"] == snapshot.canonical_game_id
            ),
            None,
        )
        payload = asdict(snapshot)
        if existing is None:
            payload["id"] = len(repository.feature_snapshots) + 1
            payload["created_at"] = datetime.now(timezone.utc)
            repository.feature_snapshots.append(payload)
        else:
            payload["id"] = existing["id"]
            payload["created_at"] = existing.get("created_at")
            existing.update(payload)
        saved_count += 1
    return saved_count


def list_feature_snapshots_in_memory(
    repository: FeatureDatasetStore,
    *,
    feature_version_id: int | None = None,
    feature_key: str = DEFAULT_FEATURE_KEY,
    team_code: str | None = None,
    season_label: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> list[FeatureSnapshotRecord]:
    if feature_version_id is None:
        feature_version = get_feature_version_in_memory(
            repository,
            feature_key=feature_key,
        )
        if feature_version is None:
            return []
        feature_version_id = feature_version.id

    selected = [
        FeatureSnapshotRecord(**entry)
        for entry in repository.feature_snapshots
        if entry["feature_version_id"] == feature_version_id
        and (season_label is None or entry["season_label"] == season_label)
        and (
            team_code is None
            or entry["home_team_code"] == team_code
            or entry["away_team_code"] == team_code
        )
    ]
    sorted_selected = sorted(
        selected,
        key=lambda entry: (entry.game_date, entry.canonical_game_id),
    )
    if limit is None:
        return sorted_selected[offset:]
    return sorted_selected[offset : offset + limit]


def count_feature_snapshots_in_memory(
    repository: FeatureDatasetStore,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    team_code: str | None = None,
    season_label: str | None = None,
) -> int:
    return len(
        list_feature_snapshots_in_memory(
            repository,
            feature_key=feature_key,
            team_code=team_code,
            season_label=season_label,
            limit=None,
        )
    )


def get_feature_version_in_memory(
    repository: FeatureDatasetStore,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
) -> FeatureVersionRecord | None:
    for entry in repository.feature_versions:
        if entry["feature_key"] == feature_key:
            return FeatureVersionRecord(**entry)
    return None


def ensure_feature_version_postgres(
    connection: Any,
    *,
    feature_key: str,
    version_label: str,
    description: str,
    config: dict[str, Any],
) -> FeatureVersionRecord:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO feature_version (feature_key, version_label, description, config_json)
            VALUES (%s, %s, %s, %s::jsonb)
            ON CONFLICT (feature_key)
            DO UPDATE SET
                version_label = EXCLUDED.version_label,
                description = EXCLUDED.description,
                config_json = EXCLUDED.config_json
            RETURNING id, feature_key, version_label, description, config_json, created_at
            """,
            (feature_key, version_label, description, _json_dumps(config)),
        )
        row = cursor.fetchone()
    connection.commit()
    return FeatureVersionRecord(
        id=int(row[0]),
        feature_key=row[1],
        version_label=row[2],
        description=row[3] or "",
        config=row[4],
        created_at=row[5],
    )


def list_canonical_game_metric_records_postgres(connection: Any) -> list[CanonicalGameMetricRecord]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                cg.id,
                s.label,
                cg.game_date,
                th.code,
                ta.code,
                cg.home_score,
                cg.away_score,
                cg.final_home_margin,
                cg.final_total_points,
                cg.total_line,
                cg.home_spread_line,
                cg.away_spread_line,
                cg.reconciliation_status,
                cg.source_row_indexes_json,
                cg.warning_codes_json,
                gm.spread_error_home,
                gm.spread_error_away,
                gm.total_error,
                gm.home_covered,
                gm.away_covered,
                gm.went_over,
                gm.went_under
            FROM canonical_game cg
            JOIN season s ON s.id = cg.season_id
            JOIN team th ON th.id = cg.home_team_id
            JOIN team ta ON ta.id = cg.away_team_id
            LEFT JOIN game_metric gm ON gm.canonical_game_id = cg.id
            ORDER BY cg.game_date ASC, cg.id ASC
            """
        )
        rows = cursor.fetchall()
    return [
        CanonicalGameMetricRecord(
            canonical_game_id=int(row[0]),
            season_label=row[1],
            game_date=row[2],
            home_team_code=row[3],
            away_team_code=row[4],
            home_score=row[5],
            away_score=row[6],
            final_home_margin=row[7],
            final_total_points=row[8],
            total_line=float(row[9]) if row[9] is not None else None,
            home_spread_line=float(row[10]) if row[10] is not None else None,
            away_spread_line=float(row[11]) if row[11] is not None else None,
            reconciliation_status=row[12],
            source_row_indexes=row[13],
            warnings=row[14],
            spread_error_home=float(row[15]) if row[15] is not None else None,
            spread_error_away=float(row[16]) if row[16] is not None else None,
            total_error=float(row[17]) if row[17] is not None else None,
            home_covered=row[18],
            away_covered=row[19],
            went_over=row[20],
            went_under=row[21],
        )
        for row in rows
    ]


def save_feature_snapshots_postgres(
    connection: Any,
    snapshots: list[FeatureSnapshotRecord],
) -> int:
    if not snapshots:
        return 0
    with connection.cursor() as cursor:
        for snapshot in snapshots:
            cursor.execute(
                """
                INSERT INTO game_feature_snapshot (
                    canonical_game_id,
                    feature_version_id,
                    season_id,
                    game_date,
                    home_team_id,
                    away_team_id,
                    feature_payload_json
                )
                VALUES (
                    %s,
                    %s,
                    (SELECT id FROM season WHERE label = %s),
                    %s,
                    (SELECT id FROM team WHERE code = %s),
                    (SELECT id FROM team WHERE code = %s),
                    %s::jsonb
                )
                ON CONFLICT (feature_version_id, canonical_game_id)
                DO UPDATE SET
                    season_id = EXCLUDED.season_id,
                    game_date = EXCLUDED.game_date,
                    home_team_id = EXCLUDED.home_team_id,
                    away_team_id = EXCLUDED.away_team_id,
                    feature_payload_json = EXCLUDED.feature_payload_json,
                    updated_at = NOW()
                """,
                (
                    snapshot.canonical_game_id,
                    snapshot.feature_version_id,
                    snapshot.season_label,
                    snapshot.game_date,
                    snapshot.home_team_code,
                    snapshot.away_team_code,
                    _json_dumps(snapshot.feature_payload),
                ),
            )
    connection.commit()
    return len(snapshots)


def list_feature_snapshots_postgres(
    connection: Any,
    *,
    feature_version_id: int | None = None,
    feature_key: str = DEFAULT_FEATURE_KEY,
    team_code: str | None = None,
    season_label: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> list[FeatureSnapshotRecord]:
    if feature_version_id is None:
        feature_version = get_feature_version_postgres(
            connection,
            feature_key=feature_key,
        )
        if feature_version is None:
            return []
        feature_version_id = feature_version.id

    select_query = """
        SELECT
            gfs.id,
            gfs.canonical_game_id,
            gfs.feature_version_id,
            s.label,
            gfs.game_date,
            th.code,
            ta.code,
            gfs.feature_payload_json,
            gfs.created_at
        FROM game_feature_snapshot gfs
        JOIN season s ON s.id = gfs.season_id
        JOIN team th ON th.id = gfs.home_team_id
        JOIN team ta ON ta.id = gfs.away_team_id
        WHERE gfs.feature_version_id = %s
    """
    params: list[Any] = [feature_version_id]
    if season_label is not None:
        select_query += " AND s.label = %s"
        params.append(season_label)
    if team_code is not None:
        select_query += " AND (th.code = %s OR ta.code = %s)"
        params.extend([team_code, team_code])
    select_query += " ORDER BY gfs.game_date ASC, gfs.canonical_game_id ASC"
    if limit is not None:
        select_query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])

    with connection.cursor() as cursor:
        cursor.execute(select_query, params)
        rows = cursor.fetchall()
    return [
        FeatureSnapshotRecord(
            id=int(row[0]),
            canonical_game_id=int(row[1]),
            feature_version_id=int(row[2]),
            season_label=row[3],
            game_date=row[4],
            home_team_code=row[5],
            away_team_code=row[6],
            feature_payload=row[7],
            created_at=row[8],
        )
        for row in rows
    ]


def count_feature_snapshots_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    team_code: str | None = None,
    season_label: str | None = None,
) -> int:
    feature_version = get_feature_version_postgres(
        connection,
        feature_key=feature_key,
    )
    if feature_version is None:
        return 0

    query = """
        SELECT COUNT(*)
        FROM game_feature_snapshot gfs
        JOIN season s ON s.id = gfs.season_id
        JOIN team th ON th.id = gfs.home_team_id
        JOIN team ta ON ta.id = gfs.away_team_id
        WHERE gfs.feature_version_id = %s
    """
    params: list[Any] = [feature_version.id]
    if season_label is not None:
        query += " AND s.label = %s"
        params.append(season_label)
    if team_code is not None:
        query += " AND (th.code = %s OR ta.code = %s)"
        params.extend([team_code, team_code])

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        row = cursor.fetchone()
    return int(row[0]) if row is not None else 0


def save_feature_analysis_artifacts_postgres(
    connection: Any,
    artifacts: list[FeatureAnalysisArtifactRecord],
) -> int:
    if not artifacts:
        return 0
    with connection.cursor() as cursor:
        for artifact in artifacts:
            cursor.execute(
                """
                INSERT INTO feature_analysis_artifact (
                    feature_version_id,
                    artifact_type,
                    target_task,
                    scope_team_code,
                    scope_season_label,
                    artifact_key,
                    dimensions_json,
                    payload_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
                ON CONFLICT (
                    feature_version_id,
                    artifact_type,
                    target_task,
                    scope_team_code,
                    scope_season_label,
                    artifact_key
                )
                DO UPDATE SET
                    dimensions_json = EXCLUDED.dimensions_json,
                    payload_json = EXCLUDED.payload_json,
                    updated_at = NOW()
                """,
                (
                    artifact.feature_version_id,
                    artifact.artifact_type,
                    artifact.target_task,
                    artifact.team_code or "",
                    artifact.season_label or "",
                    artifact.artifact_key,
                    _json_dumps(artifact.dimensions),
                    _json_dumps(artifact.payload),
                ),
            )
    connection.commit()
    return len(artifacts)


def list_feature_analysis_artifacts_postgres(
    connection: Any,
    *,
    feature_version_id: int | None = None,
    feature_key: str = DEFAULT_FEATURE_KEY,
    artifact_type: str | None = None,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> list[FeatureAnalysisArtifactRecord]:
    if feature_version_id is None:
        feature_version = get_feature_version_postgres(connection, feature_key=feature_key)
        if feature_version is None:
            return []
        feature_version_id = feature_version.id

    query = """
        SELECT
            id,
            feature_version_id,
            artifact_type,
            target_task,
            scope_team_code,
            scope_season_label,
            artifact_key,
            dimensions_json,
            payload_json,
            created_at,
            updated_at
        FROM feature_analysis_artifact
        WHERE feature_version_id = %s
    """
    params: list[Any] = [feature_version_id]
    if artifact_type is not None:
        query += " AND artifact_type = %s"
        params.append(artifact_type)
    if target_task is not None:
        query += " AND target_task = %s"
        params.append(target_task)
    if team_code is not None:
        query += " AND scope_team_code = %s"
        params.append(team_code)
    if season_label is not None:
        query += " AND scope_season_label = %s"
        params.append(season_label)
    query += " ORDER BY artifact_type ASC, target_task ASC, artifact_key ASC"
    if limit is not None:
        query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
    return [
        FeatureAnalysisArtifactRecord(
            id=int(row[0]),
            feature_version_id=int(row[1]),
            artifact_type=row[2],
            target_task=row[3],
            team_code=row[4] or None,
            season_label=row[5] or None,
            artifact_key=row[6],
            dimensions=row[7],
            payload=row[8],
            created_at=row[9],
            updated_at=row[10],
        )
        for row in rows
    ]


def get_feature_version_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
) -> FeatureVersionRecord | None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT id, feature_key, version_label, description, config_json, created_at
            FROM feature_version
            WHERE feature_key = %s
            """,
            (feature_key,),
        )
        row = cursor.fetchone()
    if row is None:
        return None
    return FeatureVersionRecord(
        id=int(row[0]),
        feature_key=row[1],
        version_label=row[2],
        description=row[3] or "",
        config=row[4],
        created_at=row[5],
    )


def get_feature_snapshot_catalog_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    team_code: str | None = None,
    season_label: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict[str, Any]:
    feature_version = get_feature_version_postgres(
        connection,
        feature_key=feature_key,
    )
    if feature_version is None:
        return {
            "feature_version": None,
            "snapshot_count": 0,
            "feature_snapshots": [],
        }

    snapshots = list_feature_snapshots_postgres(
        connection,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
        limit=limit,
        offset=offset,
    )
    snapshot_count = count_feature_snapshots_postgres(
        connection,
        feature_key=feature_key,
        team_code=team_code,
        season_label=season_label,
    )
    return {
        "feature_version": asdict(feature_version),
        "snapshot_count": snapshot_count,
        "feature_snapshots": [asdict(snapshot) for snapshot in snapshots],
    }


def get_feature_snapshot_summary_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    team_code: str | None = None,
    season_label: str | None = None,
) -> dict[str, Any]:
    feature_version = get_feature_version_postgres(
        connection,
        feature_key=feature_key,
    )
    if feature_version is None:
        return {
            "feature_version": None,
            "snapshot_count": 0,
            "perspective_count": 0,
            "summary": {},
            "latest_perspective": None,
        }

    snapshots = list_feature_snapshots_postgres(
        connection,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    summary_result = summarize_feature_snapshots(
        snapshots,
        team_code=team_code,
    )
    return {
        "feature_version": asdict(feature_version),
        "snapshot_count": len(snapshots),
        **summary_result,
    }


def get_feature_dataset_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    team_code: str | None = None,
    season_label: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    feature_version = get_feature_version_postgres(
        connection,
        feature_key=feature_key,
    )
    if feature_version is None:
        return {
            "feature_version": None,
            "row_count": 0,
            "feature_rows": [],
        }

    snapshots = list_feature_snapshots_postgres(
        connection,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    canonical_games = list_canonical_game_metric_records_postgres(connection)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code=team_code,
    )
    return {
        "feature_version": asdict(feature_version),
        "row_count": len(dataset_rows),
        "feature_rows": dataset_rows[offset : offset + limit],
    }


def get_feature_dataset_profile_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    team_code: str | None = None,
    season_label: str | None = None,
) -> dict[str, Any]:
    feature_version = get_feature_version_postgres(
        connection,
        feature_key=feature_key,
    )
    if feature_version is None:
        return {
            "feature_version": None,
            "row_count": 0,
            "profile": {},
        }

    snapshots = list_feature_snapshots_postgres(
        connection,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    canonical_games = list_canonical_game_metric_records_postgres(connection)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code=team_code,
    )
    return {
        "feature_version": asdict(feature_version),
        "row_count": len(dataset_rows),
        "profile": profile_feature_dataset_rows(dataset_rows),
    }


def get_feature_training_view_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    team_code: str | None = None,
    season_label: str | None = None,
    drop_null_targets: bool = True,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    feature_version = get_feature_version_postgres(
        connection,
        feature_key=feature_key,
    )
    if feature_version is None:
        return {
            "feature_version": None,
            "row_count": 0,
            "task": None,
            "training_rows": [],
        }

    snapshots = list_feature_snapshots_postgres(
        connection,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    canonical_games = list_canonical_game_metric_records_postgres(connection)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code=team_code,
    )
    training_view = build_feature_training_view(
        dataset_rows,
        target_task=target_task,
        drop_null_targets=drop_null_targets,
    )
    return {
        "feature_version": asdict(feature_version),
        "row_count": training_view["row_count"],
        "task": training_view["task"],
        "training_manifest": training_view["training_manifest"],
        "training_rows": training_view["training_rows"][offset : offset + limit],
    }


def get_feature_training_manifest_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    team_code: str | None = None,
    season_label: str | None = None,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    feature_version = get_feature_version_postgres(
        connection,
        feature_key=feature_key,
    )
    if feature_version is None:
        return {
            "feature_version": None,
            "row_count": 0,
            "task": None,
            "training_manifest": {},
        }

    snapshots = list_feature_snapshots_postgres(
        connection,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    canonical_games = list_canonical_game_metric_records_postgres(connection)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code=team_code,
    )
    training_view = build_feature_training_view(
        dataset_rows,
        target_task=target_task,
        drop_null_targets=drop_null_targets,
    )
    return {
        "feature_version": asdict(feature_version),
        "row_count": training_view["row_count"],
        "task": training_view["task"],
        "training_manifest": training_view["training_manifest"],
    }


def get_feature_training_task_matrix_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    team_code: str | None = None,
    season_label: str | None = None,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    feature_version = get_feature_version_postgres(
        connection,
        feature_key=feature_key,
    )
    if feature_version is None:
        return {
            "feature_version": None,
            "dataset_row_count": 0,
            "task_matrix": {},
        }

    snapshots = list_feature_snapshots_postgres(
        connection,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    canonical_games = list_canonical_game_metric_records_postgres(connection)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code=team_code,
    )
    return {
        "feature_version": asdict(feature_version),
        "dataset_row_count": len(dataset_rows),
        "task_matrix": build_feature_training_task_matrix(
            dataset_rows,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
        ),
    }


def get_feature_training_benchmark_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    team_code: str | None = None,
    season_label: str | None = None,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    feature_version = get_feature_version_postgres(
        connection,
        feature_key=feature_key,
    )
    if feature_version is None:
        return {
            "feature_version": None,
            "row_count": 0,
            "task": None,
            "benchmark_summary": {},
            "benchmark_rankings": [],
        }

    snapshots = list_feature_snapshots_postgres(
        connection,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    canonical_games = list_canonical_game_metric_records_postgres(connection)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code=team_code,
    )
    benchmark = build_feature_training_benchmark(
        dataset_rows,
        target_task=target_task,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
    )
    return {
        "feature_version": asdict(feature_version),
        "row_count": len(dataset_rows),
        **benchmark,
    }


def get_feature_pattern_catalog_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    team_code: str | None = None,
    season_label: str | None = None,
    dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    min_sample_size: int = 2,
    limit: int = 50,
) -> dict[str, Any]:
    feature_version = get_feature_version_postgres(
        connection,
        feature_key=feature_key,
    )
    if feature_version is None:
        return {
            "feature_version": None,
            "row_count": 0,
            "task": None,
            "pattern_count": 0,
            "patterns": [],
        }

    snapshots = list_feature_snapshots_postgres(
        connection,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    canonical_games = list_canonical_game_metric_records_postgres(connection)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code=team_code,
    )
    pattern_result = build_feature_pattern_catalog(
        dataset_rows,
        target_task=target_task,
        dimensions=dimensions,
        min_sample_size=min_sample_size,
        limit=limit,
    )
    return {
        "feature_version": asdict(feature_version),
        "row_count": len(dataset_rows),
        **pattern_result,
    }


def get_feature_comparable_cases_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    team_code: str | None = None,
    season_label: str | None = None,
    dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    canonical_game_id: int | None = None,
    condition_values: tuple[str, ...] | None = None,
    pattern_key: str | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    feature_version = get_feature_version_postgres(
        connection,
        feature_key=feature_key,
    )
    if feature_version is None:
        return {
            "feature_version": None,
            "row_count": 0,
            "task": None,
            "anchor_case": None,
            "comparable_count": 0,
            "comparables": [],
        }

    snapshots = list_feature_snapshots_postgres(
        connection,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    canonical_games = list_canonical_game_metric_records_postgres(connection)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code=team_code,
    )
    comparable_result = build_feature_comparable_cases(
        dataset_rows,
        target_task=target_task,
        dimensions=dimensions,
        canonical_game_id=canonical_game_id,
        team_code=team_code,
        condition_values=condition_values,
        pattern_key=pattern_key,
        limit=limit,
    )
    return {
        "feature_version": asdict(feature_version),
        "row_count": len(dataset_rows),
        **comparable_result,
    }


def get_feature_evidence_bundle_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    team_code: str | None = None,
    season_label: str | None = None,
    dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    canonical_game_id: int | None = None,
    condition_values: tuple[str, ...] | None = None,
    pattern_key: str | None = None,
    comparable_limit: int = 10,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    feature_version = get_feature_version_postgres(
        connection,
        feature_key=feature_key,
    )
    if feature_version is None:
        return {
            "feature_version": None,
            "row_count": 0,
            "task": None,
            "evidence": {},
        }

    snapshots = list_feature_snapshots_postgres(
        connection,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    canonical_games = list_canonical_game_metric_records_postgres(connection)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code=team_code,
    )
    evidence_bundle = build_feature_evidence_bundle(
        dataset_rows,
        target_task=target_task,
        dimensions=dimensions,
        canonical_game_id=canonical_game_id,
        team_code=team_code,
        condition_values=condition_values,
        pattern_key=pattern_key,
        comparable_limit=comparable_limit,
        min_pattern_sample_size=min_pattern_sample_size,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
    )
    return {
        "feature_version": asdict(feature_version),
        "row_count": len(dataset_rows),
        **evidence_bundle,
    }


def materialize_feature_analysis_artifacts_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    team_code: str | None = None,
    season_label: str | None = None,
    dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    min_sample_size: int = 2,
    canonical_game_id: int | None = None,
    condition_values: tuple[str, ...] | None = None,
    pattern_key: str | None = None,
    comparable_limit: int = 10,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    feature_version = get_feature_version_postgres(connection, feature_key=feature_key)
    if feature_version is None:
        return {
            "feature_version": None,
            "row_count": 0,
            "materialized_count": 0,
            "artifact_counts": {},
            "artifacts": [],
        }
    snapshots = list_feature_snapshots_postgres(
        connection,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    canonical_games = list_canonical_game_metric_records_postgres(connection)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code=team_code,
    )
    return _materialize_feature_analysis_artifacts(
        dataset_rows=dataset_rows,
        feature_version=feature_version,
        save_artifacts=lambda artifacts: save_feature_analysis_artifacts_postgres(
            connection,
            artifacts,
        ),
        list_artifacts=lambda: list_feature_analysis_artifacts_postgres(
            connection,
            feature_version_id=feature_version.id,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            limit=200,
        ),
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        dimensions=dimensions,
        min_sample_size=min_sample_size,
        canonical_game_id=canonical_game_id,
        condition_values=condition_values,
        pattern_key=pattern_key,
        comparable_limit=comparable_limit,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
    )


def get_feature_analysis_artifact_catalog_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    artifact_type: str | None = None,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    feature_version = get_feature_version_postgres(connection, feature_key=feature_key)
    if feature_version is None:
        return {
            "feature_version": None,
            "artifact_count": 0,
            "artifacts": [],
        }
    artifacts = list_feature_analysis_artifacts_postgres(
        connection,
        feature_version_id=feature_version.id,
        artifact_type=artifact_type,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        limit=limit,
        offset=offset,
    )
    full_count = len(
        list_feature_analysis_artifacts_postgres(
            connection,
            feature_version_id=feature_version.id,
            artifact_type=artifact_type,
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            limit=None,
        )
    )
    return {
        "feature_version": asdict(feature_version),
        "artifact_count": full_count,
        "artifacts": [asdict(artifact) for artifact in artifacts],
    }


def get_feature_analysis_artifact_history_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    artifact_type: str | None = None,
    target_task: str | None = None,
    team_code: str | None = None,
    season_label: str | None = None,
    latest_limit: int = 20,
) -> dict[str, Any]:
    feature_version = get_feature_version_postgres(connection, feature_key=feature_key)
    if feature_version is None:
        return {
            "feature_version": None,
            "overview": {},
            "daily_buckets": [],
            "latest_evidence_artifacts": [],
        }
    artifacts = list_feature_analysis_artifacts_postgres(
        connection,
        feature_version_id=feature_version.id,
        artifact_type=artifact_type,
        target_task=target_task,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    return {
        "feature_version": asdict(feature_version),
        **_build_feature_analysis_artifact_history_payload(
            artifacts,
            latest_limit=latest_limit,
        ),
    }


def get_feature_training_bundle_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    team_code: str | None = None,
    season_label: str | None = None,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
    preview_limit: int = 5,
) -> dict[str, Any]:
    feature_version = get_feature_version_postgres(
        connection,
        feature_key=feature_key,
    )
    if feature_version is None:
        return {
            "feature_version": None,
            "row_count": 0,
            "task": None,
            "training_manifest": {},
            "bundle_summary": {},
            "split_previews": {},
        }

    snapshots = list_feature_snapshots_postgres(
        connection,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    canonical_games = list_canonical_game_metric_records_postgres(connection)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code=team_code,
    )
    training_bundle = build_feature_training_bundle(
        dataset_rows,
        target_task=target_task,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
        preview_limit=preview_limit,
    )
    return {
        "feature_version": asdict(feature_version),
        "row_count": len(dataset_rows),
        **training_bundle,
    }


def get_feature_dataset_splits_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    team_code: str | None = None,
    season_label: str | None = None,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    preview_limit: int = 5,
) -> dict[str, Any]:
    feature_version = get_feature_version_postgres(
        connection,
        feature_key=feature_key,
    )
    if feature_version is None:
        return {
            "feature_version": None,
            "row_count": 0,
            "split_summary": {},
            "split_previews": {},
        }

    snapshots = list_feature_snapshots_postgres(
        connection,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    canonical_games = list_canonical_game_metric_records_postgres(connection)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code=team_code,
    )
    split_result = split_feature_dataset_rows(
        dataset_rows,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        preview_limit=preview_limit,
    )
    return {
        "feature_version": asdict(feature_version),
        "row_count": len(dataset_rows),
        **split_result,
    }


def build_feature_dataset_rows(
    *,
    snapshots: list[FeatureSnapshotRecord],
    canonical_games: list[CanonicalGameMetricRecord],
    team_code: str | None = None,
) -> list[dict[str, Any]]:
    canonical_games_by_id = {game.canonical_game_id: game for game in canonical_games}
    dataset_rows: list[dict[str, Any]] = []

    for snapshot in snapshots:
        game = canonical_games_by_id.get(snapshot.canonical_game_id)
        if game is None:
            continue
        home_payload = snapshot.feature_payload["home_team"]
        away_payload = snapshot.feature_payload["away_team"]

        if team_code is None or snapshot.home_team_code == team_code:
            dataset_rows.append(
                _build_feature_dataset_row(
                    snapshot=snapshot,
                    game=game,
                    team_code=snapshot.home_team_code,
                    opponent_code=snapshot.away_team_code,
                    venue="home",
                    payload=home_payload,
                    point_margin_actual=float(game.final_home_margin),
                    spread_error_actual=game.spread_error_home,
                    covered_actual=game.home_covered,
                )
            )
        if team_code is None or snapshot.away_team_code == team_code:
            dataset_rows.append(
                _build_feature_dataset_row(
                    snapshot=snapshot,
                    game=game,
                    team_code=snapshot.away_team_code,
                    opponent_code=snapshot.home_team_code,
                    venue="away",
                    payload=away_payload,
                    point_margin_actual=float(-game.final_home_margin),
                    spread_error_actual=game.spread_error_away,
                    covered_actual=game.away_covered,
                )
            )

    return sorted(
        dataset_rows,
        key=lambda row: (
            row["game_date"],
            row["canonical_game_id"],
            row["team_code"],
        ),
    )


def profile_feature_dataset_rows(dataset_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not dataset_rows:
        return {
            "date_range": None,
            "season_count": 0,
            "team_count": 0,
            "opponent_count": 0,
            "venue_counts": {},
            "label_balance": {},
            "feature_coverage": {},
        }

    game_dates = [row["game_date"] for row in dataset_rows]
    venue_counts: dict[str, int] = {}
    for row in dataset_rows:
        venue = row["venue"]
        venue_counts[venue] = venue_counts.get(venue, 0) + 1

    return {
        "date_range": {
            "min_game_date": min(game_dates),
            "max_game_date": max(game_dates),
        },
        "season_count": len({row["season_label"] for row in dataset_rows}),
        "team_count": len({row["team_code"] for row in dataset_rows}),
        "opponent_count": len({row["opponent_code"] for row in dataset_rows}),
        "venue_counts": venue_counts,
        "label_balance": {
            "covered_actual": _boolean_value_counts(row["covered_actual"] for row in dataset_rows),
            "went_over_actual": _boolean_value_counts(
                row["went_over_actual"] for row in dataset_rows
            ),
        },
        "feature_coverage": {
            column: _coverage_summary(row.get(column) for row in dataset_rows)
            for column in FEATURE_DATASET_PROFILE_COLUMNS
        },
    }


def split_feature_dataset_rows(
    dataset_rows: list[dict[str, Any]],
    *,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    preview_limit: int = 5,
) -> dict[str, Any]:
    split_rows = _partition_feature_dataset_rows(
        dataset_rows,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
    )
    return {
        "split_summary": {
            split_name: {
                "row_count": len(rows),
                "game_count": len({int(row["canonical_game_id"]) for row in rows}),
                "profile": profile_feature_dataset_rows(rows),
            }
            for split_name, rows in split_rows.items()
        },
        "split_previews": {
            split_name: rows[:preview_limit] for split_name, rows in split_rows.items()
        },
    }


def build_feature_training_bundle(
    dataset_rows: list[dict[str, Any]],
    *,
    target_task: str,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
    preview_limit: int = 5,
) -> dict[str, Any]:
    split_rows = _partition_feature_dataset_rows(
        dataset_rows,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
    )
    task_config = FEATURE_TRAINING_TASKS.get(target_task)
    if task_config is None:
        raise ValueError(f"Unsupported target_task: {target_task}")

    overall_training_view = build_feature_training_view(
        dataset_rows,
        target_task=target_task,
        drop_null_targets=drop_null_targets,
    )

    bundle_summary: dict[str, Any] = {}
    split_previews: dict[str, list[dict[str, Any]]] = {}
    for split_name, rows in split_rows.items():
        training_view = build_feature_training_view(
            rows,
            target_task=target_task,
            drop_null_targets=drop_null_targets,
        )
        bundle_summary[split_name] = {
            "dataset_row_count": len(rows),
            "game_count": len({int(row["canonical_game_id"]) for row in rows}),
            "training_row_count": training_view["row_count"],
            "profile": profile_feature_dataset_rows(rows),
            "target_summary": _build_training_target_summary(training_view["training_rows"]),
            "training_manifest": training_view["training_manifest"],
        }
        split_previews[split_name] = training_view["training_rows"][:preview_limit]

    return {
        "task": {
            "name": target_task,
            "task_type": task_config["task_type"],
            "target_column": task_config["target_column"],
            "drop_null_targets": drop_null_targets,
        },
        "training_manifest": overall_training_view["training_manifest"],
        "bundle_summary": bundle_summary,
        "split_previews": split_previews,
    }


def build_feature_training_task_matrix(
    dataset_rows: list[dict[str, Any]],
    *,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    task_matrix: dict[str, Any] = {}
    for task_name in FEATURE_TRAINING_TASKS:
        training_view = build_feature_training_view(
            dataset_rows,
            target_task=task_name,
            drop_null_targets=drop_null_targets,
        )
        training_bundle = build_feature_training_bundle(
            dataset_rows,
            target_task=task_name,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
            preview_limit=0,
        )
        task_matrix[task_name] = {
            "task": training_view["task"],
            "training_row_count": training_view["row_count"],
            "training_manifest": training_view["training_manifest"],
            "bundle_summary": training_bundle["bundle_summary"],
        }
    return task_matrix


def build_feature_training_benchmark(
    dataset_rows: list[dict[str, Any]],
    *,
    target_task: str,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    task_config = FEATURE_TRAINING_TASKS.get(target_task)
    if task_config is None:
        raise ValueError(f"Unsupported target_task: {target_task}")

    split_rows = _partition_feature_dataset_rows(
        dataset_rows,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
    )
    split_training_rows = {
        split_name: build_feature_training_view(
            rows,
            target_task=target_task,
            drop_null_targets=drop_null_targets,
        )["training_rows"]
        for split_name, rows in split_rows.items()
    }
    train_rows = split_training_rows["train"]
    train_target_values = [
        row["target_value"] for row in train_rows if row["target_value"] is not None
    ]
    train_target_mean = _mean_or_none(train_target_values)
    train_positive_rate = _mean_or_none(
        1.0 if row["target_value"] else 0.0 for row in train_rows if row["target_value"] is not None
    )

    baseline_specs = _get_training_benchmark_specs(
        target_task=target_task,
        task_type=task_config["task_type"],
        train_target_mean=train_target_mean,
        train_positive_rate=train_positive_rate,
    )
    benchmark_summary: dict[str, Any] = {}
    for split_name, training_rows in split_training_rows.items():
        benchmark_summary[split_name] = {
            "row_count": len(training_rows),
            "benchmarks": {
                baseline_name: _score_training_baseline(
                    training_rows,
                    predictor=predictor,
                    task_type=task_config["task_type"],
                )
                for baseline_name, predictor in baseline_specs.items()
            },
        }

    return {
        "task": {
            "name": target_task,
            "task_type": task_config["task_type"],
            "target_column": task_config["target_column"],
            "drop_null_targets": drop_null_targets,
        },
        "benchmark_summary": benchmark_summary,
        "benchmark_rankings": _rank_training_benchmarks(
            benchmark_summary,
            task_type=task_config["task_type"],
        ),
    }


def build_feature_evidence_bundle(
    dataset_rows: list[dict[str, Any]],
    *,
    target_task: str,
    dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    canonical_game_id: int | None = None,
    team_code: str | None = None,
    condition_values: tuple[str, ...] | None = None,
    pattern_key: str | None = None,
    comparable_limit: int = 10,
    min_pattern_sample_size: int = 1,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    comparable_result = build_feature_comparable_cases(
        dataset_rows,
        target_task=target_task,
        dimensions=dimensions,
        canonical_game_id=canonical_game_id,
        team_code=team_code,
        condition_values=condition_values,
        pattern_key=pattern_key,
        limit=comparable_limit,
    )
    resolved_dimensions = tuple(comparable_result["dimensions"])
    resolved_condition_values = tuple(comparable_result["condition_values"])
    resolved_pattern_key = comparable_result["pattern_key"]
    pattern_catalog = build_feature_pattern_catalog(
        dataset_rows,
        target_task=target_task,
        dimensions=resolved_dimensions,
        min_sample_size=min_pattern_sample_size,
        limit=max(len(dataset_rows), 1),
    )
    selected_pattern = next(
        (
            pattern
            for pattern in pattern_catalog["patterns"]
            if pattern.get("pattern_key") == resolved_pattern_key
        ),
        None,
    )
    benchmark_result = build_feature_training_benchmark(
        dataset_rows,
        target_task=target_task,
        train_ratio=train_ratio,
        validation_ratio=validation_ratio,
        drop_null_targets=drop_null_targets,
    )
    evidence_summary = {
        "pattern_key": resolved_pattern_key,
        "condition_count": len(resolved_dimensions),
        "comparable_count": comparable_result["comparable_count"],
        "top_comparable_similarity_score": (
            comparable_result["comparables"][0]["similarity_score"]
            if comparable_result["comparables"]
            else None
        ),
        "pattern_sample_size": (
            selected_pattern["sample_size"] if selected_pattern is not None else None
        ),
        "best_benchmark": (
            benchmark_result["benchmark_rankings"][0]
            if benchmark_result["benchmark_rankings"]
            else None
        ),
    }
    evidence_strength = build_evidence_strength_summary(
        task_type=comparable_result["task"]["task_type"],
        selected_pattern=selected_pattern,
        comparables=comparable_result["comparables"],
        benchmark_rankings=benchmark_result["benchmark_rankings"],
    )
    evidence_recommendation = build_evidence_recommendation(
        target_task=target_task,
        task_type=comparable_result["task"]["task_type"],
        evidence_strength=evidence_strength,
        selected_pattern=selected_pattern,
        comparables=comparable_result["comparables"],
        benchmark_rankings=benchmark_result["benchmark_rankings"],
        evidence_recommendation_policies=FEATURE_EVIDENCE_RECOMMENDATION_POLICIES,
    )
    return {
        "task": comparable_result["task"],
        "evidence": {
            "summary": evidence_summary,
            "strength": evidence_strength,
            "recommendation": evidence_recommendation,
            "pattern": {
                "dimensions": list(resolved_dimensions),
                "condition_values": list(resolved_condition_values),
                "selected_pattern": selected_pattern,
            },
            "comparables": {
                "anchor_case": comparable_result["anchor_case"],
                "pattern_key": comparable_result["pattern_key"],
                "summary": comparable_result["comparable_summary"],
                "cases": comparable_result["comparables"],
            },
            "benchmark_context": {
                "task": benchmark_result["task"],
                "benchmark_rankings": benchmark_result["benchmark_rankings"],
                "benchmark_summary": benchmark_result["benchmark_summary"],
            },
        },
    }


def resolve_feature_condition_values_for_row(
    row: dict[str, Any],
    *,
    dimensions: tuple[str, ...],
) -> tuple[str, ...]:
    return tuple(str(_pattern_dimension_value(row, dimension)) for dimension in dimensions)


def _materialize_feature_analysis_artifacts(
    *,
    dataset_rows: list[dict[str, Any]],
    feature_version: FeatureVersionRecord,
    save_artifacts,
    list_artifacts,
    target_task: str,
    team_code: str | None,
    season_label: str | None,
    dimensions: tuple[str, ...],
    min_sample_size: int,
    canonical_game_id: int | None,
    condition_values: tuple[str, ...] | None,
    pattern_key: str | None,
    comparable_limit: int,
    train_ratio: float,
    validation_ratio: float,
    drop_null_targets: bool,
) -> dict[str, Any]:
    pattern_result = build_feature_pattern_catalog(
        dataset_rows,
        target_task=target_task,
        dimensions=dimensions,
        min_sample_size=min_sample_size,
        limit=max(len(dataset_rows), 1),
    )
    artifacts = [
        FeatureAnalysisArtifactRecord(
            id=0,
            feature_version_id=feature_version.id,
            artifact_type="pattern_summary",
            target_task=target_task,
            team_code=team_code,
            season_label=season_label,
            artifact_key=pattern["pattern_key"],
            dimensions=list(dimensions),
            payload=pattern,
        )
        for pattern in pattern_result["patterns"]
    ]

    evidence_result = None
    if canonical_game_id is not None or condition_values is not None or pattern_key is not None:
        evidence_result = build_feature_evidence_bundle(
            dataset_rows,
            target_task=target_task,
            dimensions=dimensions,
            canonical_game_id=canonical_game_id,
            team_code=team_code,
            condition_values=condition_values,
            pattern_key=pattern_key,
            comparable_limit=comparable_limit,
            min_pattern_sample_size=min_sample_size,
            train_ratio=train_ratio,
            validation_ratio=validation_ratio,
            drop_null_targets=drop_null_targets,
        )
        resolved_pattern_key = evidence_result["evidence"]["summary"]["pattern_key"]
        artifacts.append(
            FeatureAnalysisArtifactRecord(
                id=0,
                feature_version_id=feature_version.id,
                artifact_type="evidence_bundle",
                target_task=target_task,
                team_code=team_code,
                season_label=season_label,
                artifact_key=_build_evidence_artifact_key(
                    canonical_game_id=canonical_game_id,
                    team_code=team_code,
                    pattern_key=resolved_pattern_key,
                ),
                dimensions=list(dimensions),
                payload=evidence_result["evidence"],
            )
        )

    materialized_count = save_artifacts(artifacts)
    artifact_counts = {
        "pattern_summary": len(
            [artifact for artifact in artifacts if artifact.artifact_type == "pattern_summary"]
        ),
        "evidence_bundle": len(
            [artifact for artifact in artifacts if artifact.artifact_type == "evidence_bundle"]
        ),
    }
    persisted_artifacts = [asdict(artifact) for artifact in list_artifacts()]
    return {
        "feature_version": asdict(feature_version),
        "row_count": len(dataset_rows),
        "materialized_count": materialized_count,
        "artifact_counts": artifact_counts,
        "artifacts": persisted_artifacts,
        "evidence": evidence_result["evidence"] if evidence_result is not None else None,
    }


def _build_feature_analysis_artifact_history_payload(
    artifacts: list[FeatureAnalysisArtifactRecord],
    *,
    latest_limit: int,
) -> dict[str, Any]:
    artifact_type_counts: dict[str, int] = {}
    evidence_status_counts: dict[str, int] = {}
    evidence_rating_counts: dict[str, int] = {}
    daily_buckets: dict[str, dict[str, Any]] = {}

    for artifact in artifacts:
        artifact_type_counts[artifact.artifact_type] = (
            artifact_type_counts.get(artifact.artifact_type, 0) + 1
        )
        bucket_key = _feature_artifact_bucket_date(artifact)
        bucket = daily_buckets.setdefault(
            bucket_key,
            {
                "bucket_date": bucket_key,
                "artifact_count": 0,
                "artifact_type_counts": {},
                "evidence_status_counts": {},
            },
        )
        bucket["artifact_count"] = int(bucket["artifact_count"]) + 1
        bucket["artifact_type_counts"][artifact.artifact_type] = (
            int(bucket["artifact_type_counts"].get(artifact.artifact_type, 0)) + 1
        )

        if artifact.artifact_type != "evidence_bundle":
            continue
        recommendation = artifact.payload.get("recommendation", {})
        strength = artifact.payload.get("strength", {})
        status = recommendation.get("status")
        rating = strength.get("rating")
        if status is not None:
            evidence_status_counts[status] = evidence_status_counts.get(status, 0) + 1
            bucket["evidence_status_counts"][status] = (
                int(bucket["evidence_status_counts"].get(status, 0)) + 1
            )
        if rating is not None:
            evidence_rating_counts[rating] = evidence_rating_counts.get(rating, 0) + 1

    latest_evidence_artifacts = sorted(
        [artifact for artifact in artifacts if artifact.artifact_type == "evidence_bundle"],
        key=lambda artifact: (
            artifact.updated_at or artifact.created_at or datetime.min.replace(tzinfo=timezone.utc),
            artifact.artifact_key,
        ),
        reverse=True,
    )[:latest_limit]

    return {
        "overview": {
            "artifact_count": len(artifacts),
            "artifact_type_counts": artifact_type_counts,
            "evidence_status_counts": evidence_status_counts,
            "evidence_strength_rating_counts": evidence_rating_counts,
        },
        "daily_buckets": sorted(
            daily_buckets.values(),
            key=lambda bucket: bucket["bucket_date"],
        ),
        "latest_evidence_artifacts": [
            {
                "artifact_key": artifact.artifact_key,
                "target_task": artifact.target_task,
                "team_code": artifact.team_code,
                "season_label": artifact.season_label,
                "status": artifact.payload.get("recommendation", {}).get("status"),
                "rating": artifact.payload.get("strength", {}).get("rating"),
                "overall_score": artifact.payload.get("strength", {}).get("overall_score"),
                "updated_at": artifact.updated_at,
            }
            for artifact in latest_evidence_artifacts
        ],
    }


def _feature_artifact_bucket_date(artifact: FeatureAnalysisArtifactRecord) -> str:
    reference = artifact.updated_at or artifact.created_at
    if reference is None:
        return "unknown"
    return reference.date().isoformat()


def _build_evidence_artifact_key(
    *,
    canonical_game_id: int | None,
    team_code: str | None,
    pattern_key: str,
) -> str:
    key_parts = [f"pattern={pattern_key}"]
    if canonical_game_id is not None:
        key_parts.insert(0, f"canonical_game_id={canonical_game_id}")
    if team_code is not None:
        key_parts.insert(1 if canonical_game_id is not None else 0, f"team_code={team_code}")
    return "|".join(key_parts)


def build_feature_pattern_catalog(
    dataset_rows: list[dict[str, Any]],
    *,
    target_task: str,
    dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    min_sample_size: int = 2,
    limit: int = 50,
) -> dict[str, Any]:
    task_config = FEATURE_TRAINING_TASKS.get(target_task)
    if task_config is None:
        raise ValueError(f"Unsupported target_task: {target_task}")
    if not dimensions:
        raise ValueError("Expected at least one pattern dimension.")

    normalized_dimensions = tuple(dimensions)
    invalid_dimensions = [
        dimension
        for dimension in normalized_dimensions
        if dimension not in FEATURE_PATTERN_DIMENSIONS
    ]
    if invalid_dimensions:
        raise ValueError(f"Unsupported pattern dimensions: {', '.join(invalid_dimensions)}")

    target_column = task_config["target_column"]
    rows_with_targets = [row for row in dataset_rows if row.get(target_column) is not None]
    grouped_rows: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for row in rows_with_targets:
        condition_key = tuple(
            str(_pattern_dimension_value(row, dimension)) for dimension in normalized_dimensions
        )
        grouped_rows.setdefault(condition_key, []).append(row)

    patterns = []
    for condition_key, grouped in grouped_rows.items():
        if len(grouped) < min_sample_size:
            continue
        pattern_key = _build_pattern_key(normalized_dimensions, condition_key)
        patterns.append(
            {
                "pattern_key": pattern_key,
                "conditions": {
                    dimension: value
                    for dimension, value in zip(normalized_dimensions, condition_key)
                },
                "comparable_lookup": {
                    "dimensions": list(normalized_dimensions),
                    "condition_values": list(condition_key),
                    "pattern_key": pattern_key,
                },
                **_build_pattern_metrics(grouped, task_config=task_config),
            }
        )

    ranked_patterns = sorted(
        patterns,
        key=lambda pattern: (
            -int(pattern["sample_size"]),
            -float(pattern["signal_strength"] or 0.0),
        ),
    )
    return {
        "task": {
            "name": target_task,
            "task_type": task_config["task_type"],
            "target_column": target_column,
        },
        "dimensions": list(normalized_dimensions),
        "min_sample_size": min_sample_size,
        "pattern_count": len(ranked_patterns),
        "patterns": ranked_patterns[:limit],
    }


def build_feature_comparable_cases(
    dataset_rows: list[dict[str, Any]],
    *,
    target_task: str,
    dimensions: tuple[str, ...] = ("venue", "days_rest_bucket"),
    canonical_game_id: int | None = None,
    team_code: str | None = None,
    condition_values: tuple[str, ...] | None = None,
    pattern_key: str | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    task_config = FEATURE_TRAINING_TASKS.get(target_task)
    if task_config is None:
        raise ValueError(f"Unsupported target_task: {target_task}")
    if not dimensions:
        raise ValueError("Expected at least one comparable dimension.")

    normalized_dimensions = tuple(dimensions)
    if pattern_key is not None:
        parsed_dimensions, parsed_condition_values = _parse_pattern_key(pattern_key)
        normalized_dimensions = parsed_dimensions
        condition_values = parsed_condition_values

    invalid_dimensions = [
        dimension
        for dimension in normalized_dimensions
        if dimension not in FEATURE_PATTERN_DIMENSIONS
    ]
    if invalid_dimensions:
        raise ValueError(f"Unsupported comparable dimensions: {', '.join(invalid_dimensions)}")

    anchor_row = None
    if canonical_game_id is not None:
        anchor_row = _find_dataset_anchor_row(
            dataset_rows,
            canonical_game_id=canonical_game_id,
            team_code=team_code,
        )

    resolved_condition_values = _resolve_comparable_condition_values(
        anchor_row=anchor_row,
        condition_values=condition_values,
        dimensions=normalized_dimensions,
    )
    target_column = task_config["target_column"]
    comparable_rows = [
        row
        for row in dataset_rows
        if row.get(target_column) is not None
        and all(
            str(_pattern_dimension_value(row, dimension)) == value
            for dimension, value in zip(normalized_dimensions, resolved_condition_values)
        )
        and not _is_same_anchor_case(row, anchor_row)
    ]
    scored_comparables = [
        _score_comparable_candidate(anchor_row=anchor_row, candidate_row=row)
        for row in comparable_rows
    ]
    scored_comparables = sorted(
        scored_comparables,
        key=lambda entry: (
            float(entry["similarity_score"])
            if entry["similarity_score"] is not None
            else float("-inf"),
            entry["row"]["game_date"],
            entry["row"]["canonical_game_id"],
        ),
        reverse=True,
    )
    comparable_summary = _build_pattern_metrics(
        comparable_rows,
        task_config=task_config,
    )
    return {
        "task": {
            "name": target_task,
            "task_type": task_config["task_type"],
            "target_column": target_column,
        },
        "dimensions": list(normalized_dimensions),
        "anchor_case": _serialize_anchor_case(anchor_row, normalized_dimensions),
        "pattern_key": _build_pattern_key(normalized_dimensions, resolved_condition_values),
        "condition_values": list(resolved_condition_values),
        "comparable_count": len(scored_comparables),
        "comparable_summary": {
            **comparable_summary,
            "ranking_mode": "anchor_similarity" if anchor_row is not None else "recency_only",
        },
        "comparables": [
            _serialize_comparable_case(
                entry["row"],
                normalized_dimensions,
                similarity_score=entry["similarity_score"],
                similarity_breakdown=entry["similarity_breakdown"],
            )
            for entry in scored_comparables[:limit]
        ],
    }


def _partition_feature_dataset_rows(
    dataset_rows: list[dict[str, Any]],
    *,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
) -> dict[str, list[dict[str, Any]]]:
    if train_ratio <= 0 or validation_ratio < 0 or train_ratio + validation_ratio >= 1:
        raise ValueError("Expected train_ratio > 0, validation_ratio >= 0, and sum < 1.")

    if not dataset_rows:
        return {"train": [], "validation": [], "test": []}

    rows_by_game: dict[int, list[dict[str, Any]]] = {}
    game_order: list[int] = []
    for row in dataset_rows:
        canonical_game_id = int(row["canonical_game_id"])
        if canonical_game_id not in rows_by_game:
            rows_by_game[canonical_game_id] = []
            game_order.append(canonical_game_id)
        rows_by_game[canonical_game_id].append(row)

    game_count = len(game_order)
    train_game_count = int(game_count * train_ratio)
    validation_game_count = int(game_count * validation_ratio)
    test_game_count = game_count - train_game_count - validation_game_count

    if game_count >= 3:
        if train_game_count == 0:
            train_game_count = 1
        if validation_game_count == 0:
            validation_game_count = 1
        test_game_count = game_count - train_game_count - validation_game_count
        if test_game_count <= 0:
            validation_game_count = max(1, validation_game_count - 1)
            test_game_count = game_count - train_game_count - validation_game_count
        if test_game_count <= 0:
            train_game_count = max(1, train_game_count - 1)
            test_game_count = game_count - train_game_count - validation_game_count

    train_games = set(game_order[:train_game_count])
    validation_games = set(game_order[train_game_count : train_game_count + validation_game_count])
    test_games = set(game_order[train_game_count + validation_game_count :])

    return {
        "train": [
            row for game_id in game_order for row in rows_by_game[game_id] if game_id in train_games
        ],
        "validation": [
            row
            for game_id in game_order
            for row in rows_by_game[game_id]
            if game_id in validation_games
        ],
        "test": [
            row for game_id in game_order for row in rows_by_game[game_id] if game_id in test_games
        ],
    }


def build_feature_training_view(
    dataset_rows: list[dict[str, Any]],
    *,
    target_task: str,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    task_config = FEATURE_TRAINING_TASKS.get(target_task)
    if task_config is None:
        raise ValueError(f"Unsupported target_task: {target_task}")

    target_column = task_config["target_column"]
    training_rows = []
    for row in dataset_rows:
        target_value = row.get(target_column)
        if drop_null_targets and target_value is None:
            continue
        training_rows.append(
            {
                "canonical_game_id": row["canonical_game_id"],
                "season_label": row["season_label"],
                "game_date": row["game_date"],
                "team_code": row["team_code"],
                "opponent_code": row["opponent_code"],
                "venue": row["venue"],
                "target_value": target_value,
                "features": {
                    column: value
                    for column, value in row.items()
                    if column not in FEATURE_DATASET_LABEL_COLUMNS
                    and column
                    not in {
                        "canonical_game_id",
                        "season_label",
                        "game_date",
                        "team_code",
                        "opponent_code",
                        "venue",
                        "feature_version_id",
                    }
                    for value in [row[column]]
                },
            }
        )

    return {
        "row_count": len(training_rows),
        "task": {
            "name": target_task,
            "task_type": task_config["task_type"],
            "target_column": target_column,
            "drop_null_targets": drop_null_targets,
            "excluded_label_columns": [
                column for column in FEATURE_DATASET_LABEL_COLUMNS if column != target_column
            ],
        },
        "training_manifest": profile_feature_training_rows(training_rows),
        "training_rows": training_rows,
    }


def _build_training_target_summary(training_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not training_rows:
        return {
            "row_count": 0,
            "target_non_null_count": 0,
            "target_mean": None,
            "target_stddev": None,
            "target_value_counts": {},
        }

    target_values = [
        row["target_value"] for row in training_rows if row["target_value"] is not None
    ]
    return {
        "row_count": len(training_rows),
        "target_non_null_count": len(target_values),
        "target_mean": _mean_or_none(target_values),
        "target_stddev": _pstdev_or_none(target_values),
        "target_value_counts": _training_target_value_counts(target_values),
    }


def profile_feature_training_rows(training_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not training_rows:
        return {
            "row_count": 0,
            "metadata_columns": list(FEATURE_TRAINING_METADATA_COLUMNS),
            "feature_column_count": 0,
            "feature_columns": [],
            "numeric_feature_columns": [],
            "boolean_feature_columns": [],
            "feature_coverage": {},
            "target_summary": _build_training_target_summary(training_rows),
        }

    feature_columns = sorted(
        {feature_name for row in training_rows for feature_name in row["features"].keys()}
    )
    numeric_feature_columns: list[str] = []
    boolean_feature_columns: list[str] = []
    for column in feature_columns:
        sample_value = _first_non_null_feature_value(training_rows, column)
        if isinstance(sample_value, bool):
            boolean_feature_columns.append(column)
        elif isinstance(sample_value, (int, float)):
            numeric_feature_columns.append(column)

    return {
        "row_count": len(training_rows),
        "metadata_columns": list(FEATURE_TRAINING_METADATA_COLUMNS),
        "feature_column_count": len(feature_columns),
        "feature_columns": feature_columns,
        "numeric_feature_columns": numeric_feature_columns,
        "boolean_feature_columns": boolean_feature_columns,
        "feature_coverage": {
            column: _coverage_summary(row["features"].get(column) for row in training_rows)
            for column in feature_columns
        },
        "target_summary": _build_training_target_summary(training_rows),
    }


def summarize_feature_snapshots(
    snapshots: list[FeatureSnapshotRecord],
    *,
    team_code: str | None = None,
) -> dict[str, Any]:
    perspectives = _extract_team_perspectives(
        snapshots,
        team_code=team_code,
    )
    if not perspectives:
        return {
            "perspective_count": 0,
            "summary": {},
            "latest_perspective": None,
        }

    home_perspectives = [entry for entry in perspectives if entry["venue"] == "home"]
    away_perspectives = [entry for entry in perspectives if entry["venue"] == "away"]
    latest_perspective = max(
        perspectives,
        key=lambda entry: (entry["game_date"], entry["canonical_game_id"]),
    )
    return {
        "perspective_count": len(perspectives),
        "summary": {
            "team_count": len({entry["team_code"] for entry in perspectives}),
            "home_perspective_count": len(home_perspectives),
            "away_perspective_count": len(away_perspectives),
            "avg_games_played_prior": _mean_or_none(
                entry["payload"]["games_played_prior"] for entry in perspectives
            ),
            "avg_days_rest": _mean_or_none(
                entry["payload"]["days_rest"]
                for entry in perspectives
                if entry["payload"]["days_rest"] is not None
            ),
            "back_to_back_rate": _mean_or_none(
                1.0 if entry["payload"]["is_back_to_back"] else 0.0 for entry in perspectives
            ),
            "avg_cover_streak": _mean_or_none(
                entry["payload"]["trend_signals"]["current_cover_streak"] for entry in perspectives
            ),
            "avg_non_cover_streak": _mean_or_none(
                entry["payload"]["trend_signals"]["current_non_cover_streak"]
                for entry in perspectives
            ),
            "avg_over_streak": _mean_or_none(
                entry["payload"]["trend_signals"]["current_over_streak"] for entry in perspectives
            ),
            "avg_under_streak": _mean_or_none(
                entry["payload"]["trend_signals"]["current_under_streak"] for entry in perspectives
            ),
            "rolling_window_averages": {
                str(window): _build_window_average_summary(
                    perspectives,
                    window=str(window),
                )
                for window in DEFAULT_FEATURE_WINDOWS
            },
        },
        "latest_perspective": {
            "canonical_game_id": latest_perspective["canonical_game_id"],
            "game_date": latest_perspective["game_date"],
            "season_label": latest_perspective["season_label"],
            "team_code": latest_perspective["team_code"],
            "opponent_code": latest_perspective["opponent_code"],
            "venue": latest_perspective["venue"],
            "days_rest": latest_perspective["payload"]["days_rest"],
            "is_back_to_back": latest_perspective["payload"]["is_back_to_back"],
            "current_cover_streak": latest_perspective["payload"]["trend_signals"][
                "current_cover_streak"
            ],
            "current_over_streak": latest_perspective["payload"]["trend_signals"][
                "current_over_streak"
            ],
        },
    }


def _build_feature_dataset_row(
    *,
    snapshot: FeatureSnapshotRecord,
    game: CanonicalGameMetricRecord,
    team_code: str,
    opponent_code: str,
    venue: str,
    payload: dict[str, Any],
    point_margin_actual: float,
    spread_error_actual: float | None,
    covered_actual: bool | None,
) -> dict[str, Any]:
    rolling_3 = payload["rolling_windows"]["3"]
    rolling_5 = payload["rolling_windows"]["5"]
    rolling_10 = payload["rolling_windows"]["10"]
    return {
        "canonical_game_id": snapshot.canonical_game_id,
        "feature_version_id": snapshot.feature_version_id,
        "season_label": snapshot.season_label,
        "game_date": snapshot.game_date,
        "team_code": team_code,
        "opponent_code": opponent_code,
        "venue": venue,
        "games_played_prior": payload["games_played_prior"],
        "season_games_played_prior": payload["season_games_played_prior"],
        "home_games_played_prior": payload["home_games_played_prior"],
        "away_games_played_prior": payload["away_games_played_prior"],
        "days_rest": payload["days_rest"],
        "is_back_to_back": payload["is_back_to_back"],
        "prior_matchup_count": snapshot.feature_payload["matchup"]["prior_matchup_count"],
        "season_prior_matchup_count": snapshot.feature_payload["matchup"][
            "season_prior_matchup_count"
        ],
        "rolling_3_avg_point_margin": rolling_3["avg_point_margin"],
        "rolling_3_avg_total_points": rolling_3["avg_total_points"],
        "rolling_3_avg_spread_error": rolling_3["avg_spread_error"],
        "rolling_3_avg_total_error": rolling_3["avg_total_error"],
        "rolling_3_cover_rate": rolling_3["cover_rate"],
        "rolling_3_over_rate": rolling_3["over_rate"],
        "rolling_5_avg_point_margin": rolling_5["avg_point_margin"],
        "rolling_5_avg_total_points": rolling_5["avg_total_points"],
        "rolling_5_avg_spread_error": rolling_5["avg_spread_error"],
        "rolling_5_avg_total_error": rolling_5["avg_total_error"],
        "rolling_5_cover_rate": rolling_5["cover_rate"],
        "rolling_5_over_rate": rolling_5["over_rate"],
        "rolling_10_avg_point_margin": rolling_10["avg_point_margin"],
        "rolling_10_avg_total_points": rolling_10["avg_total_points"],
        "rolling_10_avg_spread_error": rolling_10["avg_spread_error"],
        "rolling_10_avg_total_error": rolling_10["avg_total_error"],
        "rolling_10_cover_rate": rolling_10["cover_rate"],
        "rolling_10_over_rate": rolling_10["over_rate"],
        "point_margin_stddev": payload["volatility"]["point_margin_stddev"],
        "total_points_stddev": payload["volatility"]["total_points_stddev"],
        "spread_error_stddev": payload["volatility"]["spread_error_stddev"],
        "total_error_stddev": payload["volatility"]["total_error_stddev"],
        "current_cover_streak": payload["trend_signals"]["current_cover_streak"],
        "current_non_cover_streak": payload["trend_signals"]["current_non_cover_streak"],
        "current_over_streak": payload["trend_signals"]["current_over_streak"],
        "current_under_streak": payload["trend_signals"]["current_under_streak"],
        "recent_point_margin_delta_3_vs_10": payload["trend_signals"][
            "recent_point_margin_delta_3_vs_10"
        ],
        "recent_total_points_delta_3_vs_10": payload["trend_signals"][
            "recent_total_points_delta_3_vs_10"
        ],
        "recent_spread_error_delta_3_vs_10": payload["trend_signals"][
            "recent_spread_error_delta_3_vs_10"
        ],
        "recent_total_error_delta_3_vs_10": payload["trend_signals"][
            "recent_total_error_delta_3_vs_10"
        ],
        "point_margin_actual": round(point_margin_actual, 4),
        "spread_error_actual": round(spread_error_actual, 4)
        if spread_error_actual is not None
        else None,
        "covered_actual": covered_actual,
        "total_error_actual": round(game.total_error, 4) if game.total_error is not None else None,
        "went_over_actual": game.went_over,
        "total_points_actual": float(game.final_total_points),
    }


def _build_future_feature_dataset_row(
    *,
    feature_version_id: int,
    scenario_key: str,
    season_label: str,
    game_date: date,
    team_code: str,
    opponent_code: str,
    venue: str,
    payload: dict[str, Any],
    prior_matchups: list[CanonicalGameMetricRecord],
    team_spread_line: float | None,
    opponent_spread_line: float | None,
    total_line: float | None,
) -> dict[str, Any]:
    rolling_3 = payload["rolling_windows"]["3"]
    rolling_5 = payload["rolling_windows"]["5"]
    rolling_10 = payload["rolling_windows"]["10"]
    return {
        "canonical_game_id": 0,
        "feature_version_id": feature_version_id,
        "scenario_key": scenario_key,
        "is_future_scenario": True,
        "season_label": season_label,
        "game_date": game_date,
        "team_code": team_code,
        "opponent_code": opponent_code,
        "venue": venue,
        "games_played_prior": payload["games_played_prior"],
        "season_games_played_prior": payload["season_games_played_prior"],
        "home_games_played_prior": payload["home_games_played_prior"],
        "away_games_played_prior": payload["away_games_played_prior"],
        "days_rest": payload["days_rest"],
        "is_back_to_back": payload["is_back_to_back"],
        "prior_matchup_count": len(prior_matchups),
        "season_prior_matchup_count": sum(
            1 for prior in prior_matchups if prior.season_label == season_label
        ),
        "rolling_3_avg_point_margin": rolling_3["avg_point_margin"],
        "rolling_3_avg_total_points": rolling_3["avg_total_points"],
        "rolling_3_avg_spread_error": rolling_3["avg_spread_error"],
        "rolling_3_avg_total_error": rolling_3["avg_total_error"],
        "rolling_3_cover_rate": rolling_3["cover_rate"],
        "rolling_3_over_rate": rolling_3["over_rate"],
        "rolling_5_avg_point_margin": rolling_5["avg_point_margin"],
        "rolling_5_avg_total_points": rolling_5["avg_total_points"],
        "rolling_5_avg_spread_error": rolling_5["avg_spread_error"],
        "rolling_5_avg_total_error": rolling_5["avg_total_error"],
        "rolling_5_cover_rate": rolling_5["cover_rate"],
        "rolling_5_over_rate": rolling_5["over_rate"],
        "rolling_10_avg_point_margin": rolling_10["avg_point_margin"],
        "rolling_10_avg_total_points": rolling_10["avg_total_points"],
        "rolling_10_avg_spread_error": rolling_10["avg_spread_error"],
        "rolling_10_avg_total_error": rolling_10["avg_total_error"],
        "rolling_10_cover_rate": rolling_10["cover_rate"],
        "rolling_10_over_rate": rolling_10["over_rate"],
        "point_margin_stddev": payload["volatility"]["point_margin_stddev"],
        "total_points_stddev": payload["volatility"]["total_points_stddev"],
        "spread_error_stddev": payload["volatility"]["spread_error_stddev"],
        "total_error_stddev": payload["volatility"]["total_error_stddev"],
        "current_cover_streak": payload["trend_signals"]["current_cover_streak"],
        "current_non_cover_streak": payload["trend_signals"]["current_non_cover_streak"],
        "current_over_streak": payload["trend_signals"]["current_over_streak"],
        "current_under_streak": payload["trend_signals"]["current_under_streak"],
        "recent_point_margin_delta_3_vs_10": payload["trend_signals"][
            "recent_point_margin_delta_3_vs_10"
        ],
        "recent_total_points_delta_3_vs_10": payload["trend_signals"][
            "recent_total_points_delta_3_vs_10"
        ],
        "recent_spread_error_delta_3_vs_10": payload["trend_signals"][
            "recent_spread_error_delta_3_vs_10"
        ],
        "recent_total_error_delta_3_vs_10": payload["trend_signals"][
            "recent_total_error_delta_3_vs_10"
        ],
        "team_spread_line": round(team_spread_line, 4) if team_spread_line is not None else None,
        "opponent_spread_line": round(opponent_spread_line, 4)
        if opponent_spread_line is not None
        else None,
        "total_line": round(total_line, 4) if total_line is not None else None,
        "point_margin_actual": None,
        "spread_error_actual": None,
        "covered_actual": None,
        "total_error_actual": None,
        "went_over_actual": None,
        "total_points_actual": None,
    }


def _build_team_feature_payload(
    *,
    team_code: str,
    prior_games: list[TeamPerspectiveGame],
    current_game_date: date,
    current_season_label: str,
    windows: tuple[int, ...],
) -> dict[str, Any]:
    last_game_date = prior_games[-1].game_date if prior_games else None
    days_rest = (current_game_date - last_game_date).days if last_game_date is not None else None
    season_games_played_prior = sum(
        1 for game in prior_games if game.season_label == current_season_label
    )
    return {
        "team_code": team_code,
        "games_played_prior": len(prior_games),
        "season_games_played_prior": season_games_played_prior,
        "home_games_played_prior": sum(1 for game in prior_games if game.is_home),
        "away_games_played_prior": sum(1 for game in prior_games if not game.is_home),
        "days_rest": days_rest,
        "is_back_to_back": days_rest == 1 if days_rest is not None else False,
        "rolling_windows": {
            str(window): _build_window_summary(prior_games[-window:]) for window in windows
        },
        "rolling_home_windows": {
            str(window): _build_window_summary(
                [game for game in prior_games if game.is_home][-window:]
            )
            for window in windows
        },
        "rolling_away_windows": {
            str(window): _build_window_summary(
                [game for game in prior_games if not game.is_home][-window:]
            )
            for window in windows
        },
        "volatility": _build_volatility_summary(prior_games),
        "trend_signals": _build_trend_signal_summary(prior_games),
    }


def _extract_team_perspectives(
    snapshots: list[FeatureSnapshotRecord],
    *,
    team_code: str | None = None,
) -> list[dict[str, Any]]:
    perspectives: list[dict[str, Any]] = []
    for snapshot in snapshots:
        if team_code is None or snapshot.home_team_code == team_code:
            perspectives.append(
                {
                    "canonical_game_id": snapshot.canonical_game_id,
                    "game_date": snapshot.game_date,
                    "season_label": snapshot.season_label,
                    "team_code": snapshot.home_team_code,
                    "opponent_code": snapshot.away_team_code,
                    "venue": "home",
                    "payload": snapshot.feature_payload["home_team"],
                }
            )
        if team_code is None or snapshot.away_team_code == team_code:
            perspectives.append(
                {
                    "canonical_game_id": snapshot.canonical_game_id,
                    "game_date": snapshot.game_date,
                    "season_label": snapshot.season_label,
                    "team_code": snapshot.away_team_code,
                    "opponent_code": snapshot.home_team_code,
                    "venue": "away",
                    "payload": snapshot.feature_payload["away_team"],
                }
            )
    return perspectives


def _build_window_average_summary(
    perspectives: list[dict[str, Any]],
    *,
    window: str,
) -> dict[str, Any]:
    window_payloads = [
        entry["payload"]["rolling_windows"][window]
        for entry in perspectives
        if window in entry["payload"]["rolling_windows"]
    ]
    return {
        "avg_sample_size": _mean_or_none(payload["sample_size"] for payload in window_payloads),
        "avg_point_margin": _mean_or_none(
            payload["avg_point_margin"]
            for payload in window_payloads
            if payload["avg_point_margin"] is not None
        ),
        "avg_total_points": _mean_or_none(
            payload["avg_total_points"]
            for payload in window_payloads
            if payload["avg_total_points"] is not None
        ),
        "avg_spread_error": _mean_or_none(
            payload["avg_spread_error"]
            for payload in window_payloads
            if payload["avg_spread_error"] is not None
        ),
        "avg_total_error": _mean_or_none(
            payload["avg_total_error"]
            for payload in window_payloads
            if payload["avg_total_error"] is not None
        ),
        "avg_cover_rate": _mean_or_none(
            payload["cover_rate"]
            for payload in window_payloads
            if payload["cover_rate"] is not None
        ),
        "avg_over_rate": _mean_or_none(
            payload["over_rate"] for payload in window_payloads if payload["over_rate"] is not None
        ),
        "avg_point_margin_volatility": _mean_or_none(
            payload["point_margin_volatility"]
            for payload in window_payloads
            if payload["point_margin_volatility"] is not None
        ),
        "avg_spread_error_volatility": _mean_or_none(
            payload["spread_error_volatility"]
            for payload in window_payloads
            if payload["spread_error_volatility"] is not None
        ),
    }


def _build_window_summary(prior_games: list[TeamPerspectiveGame]) -> dict[str, Any]:
    return {
        "sample_size": len(prior_games),
        "avg_point_margin": _mean_or_none(game.point_margin for game in prior_games),
        "avg_total_points": _mean_or_none(game.total_points for game in prior_games),
        "avg_spread_error": _mean_or_none(
            game.spread_error for game in prior_games if game.spread_error is not None
        ),
        "avg_total_error": _mean_or_none(
            game.total_error for game in prior_games if game.total_error is not None
        ),
        "point_margin_volatility": _pstdev_or_none(game.point_margin for game in prior_games),
        "total_points_volatility": _pstdev_or_none(game.total_points for game in prior_games),
        "spread_error_volatility": _pstdev_or_none(
            game.spread_error for game in prior_games if game.spread_error is not None
        ),
        "total_error_volatility": _pstdev_or_none(
            game.total_error for game in prior_games if game.total_error is not None
        ),
        "cover_rate": _mean_or_none(
            1.0 if game.covered else 0.0 for game in prior_games if game.covered is not None
        ),
        "over_rate": _mean_or_none(
            1.0 if game.went_over else 0.0 for game in prior_games if game.went_over is not None
        ),
    }


def _build_volatility_summary(prior_games: list[TeamPerspectiveGame]) -> dict[str, Any]:
    return {
        "point_margin_stddev": _pstdev_or_none(game.point_margin for game in prior_games),
        "total_points_stddev": _pstdev_or_none(game.total_points for game in prior_games),
        "spread_error_stddev": _pstdev_or_none(
            game.spread_error for game in prior_games if game.spread_error is not None
        ),
        "total_error_stddev": _pstdev_or_none(
            game.total_error for game in prior_games if game.total_error is not None
        ),
    }


def _build_trend_signal_summary(prior_games: list[TeamPerspectiveGame]) -> dict[str, Any]:
    recent_three = prior_games[-3:]
    recent_ten = prior_games[-10:]
    return {
        "current_cover_streak": _boolean_streak(
            prior_games,
            value_getter=lambda game: game.covered,
            target_value=True,
        ),
        "current_non_cover_streak": _boolean_streak(
            prior_games,
            value_getter=lambda game: game.covered,
            target_value=False,
        ),
        "current_over_streak": _boolean_streak(
            prior_games,
            value_getter=lambda game: game.went_over,
            target_value=True,
        ),
        "current_under_streak": _boolean_streak(
            prior_games,
            value_getter=lambda game: game.went_over,
            target_value=False,
        ),
        "recent_point_margin_delta_3_vs_10": _delta_between_windows(
            recent_three,
            recent_ten,
            value_getter=lambda game: game.point_margin,
        ),
        "recent_total_points_delta_3_vs_10": _delta_between_windows(
            recent_three,
            recent_ten,
            value_getter=lambda game: game.total_points,
        ),
        "recent_spread_error_delta_3_vs_10": _delta_between_windows(
            recent_three,
            recent_ten,
            value_getter=lambda game: game.spread_error,
        ),
        "recent_total_error_delta_3_vs_10": _delta_between_windows(
            recent_three,
            recent_ten,
            value_getter=lambda game: game.total_error,
        ),
    }


def _to_team_perspective_game(
    game: CanonicalGameMetricRecord,
    *,
    team_code: str,
) -> TeamPerspectiveGame:
    is_home = team_code == game.home_team_code
    return TeamPerspectiveGame(
        season_label=game.season_label,
        game_date=game.game_date,
        is_home=is_home,
        point_margin=float(game.final_home_margin if is_home else -game.final_home_margin),
        total_points=float(game.final_total_points),
        spread_error=game.spread_error_home if is_home else game.spread_error_away,
        total_error=game.total_error,
        covered=game.home_covered if is_home else game.away_covered,
        went_over=game.went_over,
    )


def _mean_or_none(values) -> float | None:
    materialized = list(values)
    if not materialized:
        return None
    return round(float(mean(materialized)), 4)


def _pstdev_or_none(values) -> float | None:
    materialized = list(values)
    if not materialized:
        return None
    if len(materialized) == 1:
        return 0.0
    return round(float(pstdev(materialized)), 4)


def _boolean_streak(
    prior_games: list[TeamPerspectiveGame],
    *,
    value_getter,
    target_value: bool,
) -> int:
    streak = 0
    for game in reversed(prior_games):
        value = value_getter(game)
        if value is None:
            continue
        if value != target_value:
            break
        streak += 1
    return streak


def _delta_between_windows(recent_games, baseline_games, *, value_getter) -> float | None:
    recent_average = _mean_or_none(
        value_getter(game) for game in recent_games if value_getter(game) is not None
    )
    baseline_average = _mean_or_none(
        value_getter(game) for game in baseline_games if value_getter(game) is not None
    )
    if recent_average is None or baseline_average is None:
        return None
    return round(recent_average - baseline_average, 4)


def _boolean_value_counts(values) -> dict[str, int]:
    counts = {"true": 0, "false": 0, "null": 0}
    for value in values:
        if value is True:
            counts["true"] += 1
        elif value is False:
            counts["false"] += 1
        else:
            counts["null"] += 1
    return counts


def _coverage_summary(values) -> dict[str, float | int]:
    materialized = list(values)
    non_null_values = [value for value in materialized if value is not None]
    non_null_count = len(non_null_values)
    row_count = len(materialized)
    return {
        "non_null_count": non_null_count,
        "null_count": row_count - non_null_count,
        "coverage_rate": round(non_null_count / row_count, 4) if row_count else 0.0,
    }


def _training_target_value_counts(values) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value).lower() if isinstance(value, bool) else str(value)
        counts[key] = counts.get(key, 0) + 1
    return counts


def _first_non_null_feature_value(training_rows, column: str) -> Any:
    for row in training_rows:
        value = row["features"].get(column)
        if value is not None:
            return value
    return None


def _get_training_benchmark_specs(
    *,
    target_task: str,
    task_type: str,
    train_target_mean: float | None,
    train_positive_rate: float | None,
) -> dict[str, Any]:
    if task_type == "regression":
        feature_baselines = FEATURE_REGRESSION_BASELINE_FEATURES.get(target_task, {})
        return {
            "zero_baseline": lambda _row: 0.0,
            "train_mean_baseline": lambda _row, value=train_target_mean: value,
            **{
                baseline_name: (
                    lambda row, feature_column=feature_column: row["features"].get(feature_column)
                )
                for baseline_name, feature_column in feature_baselines.items()
            },
        }

    feature_baselines = FEATURE_CLASSIFICATION_BASELINE_FEATURES.get(target_task, {})
    return {
        "train_rate_baseline": lambda _row, value=train_positive_rate: value,
        **{
            baseline_name: (
                lambda row, feature_column=feature_column: row["features"].get(feature_column)
            )
            for baseline_name, feature_column in feature_baselines.items()
        },
    }


def _score_training_baseline(
    training_rows: list[dict[str, Any]],
    *,
    predictor,
    task_type: str,
) -> dict[str, Any]:
    if task_type == "regression":
        return _score_regression_baseline(training_rows, predictor=predictor)
    return _score_classification_baseline(training_rows, predictor=predictor)


def _score_regression_baseline(training_rows, *, predictor) -> dict[str, Any]:
    scored_rows = []
    for row in training_rows:
        target_value = row["target_value"]
        prediction = predictor(row)
        if target_value is None or prediction is None:
            continue
        error = float(prediction) - float(target_value)
        scored_rows.append(
            {
                "target_value": float(target_value),
                "prediction": round(float(prediction), 4),
                "absolute_error": round(abs(error), 4),
                "squared_error": round(error * error, 4),
            }
        )

    absolute_errors = [row["absolute_error"] for row in scored_rows]
    squared_errors = [row["squared_error"] for row in scored_rows]
    return {
        "prediction_count": len(scored_rows),
        "coverage_rate": round(len(scored_rows) / len(training_rows), 4) if training_rows else 0.0,
        "mae": _mean_or_none(absolute_errors),
        "rmse": round(float(mean(squared_errors) ** 0.5), 4) if squared_errors else None,
        "mean_prediction": _mean_or_none(row["prediction"] for row in scored_rows),
    }


def _score_classification_baseline(training_rows, *, predictor) -> dict[str, Any]:
    scored_rows = []
    for row in training_rows:
        target_value = row["target_value"]
        raw_prediction = predictor(row)
        probability = _normalize_probability(raw_prediction)
        if target_value is None or probability is None:
            continue
        predicted_label = probability >= 0.5
        actual_label = bool(target_value)
        scored_rows.append(
            {
                "actual_label": actual_label,
                "predicted_label": predicted_label,
                "probability": probability,
            }
        )

    true_positive = sum(
        1 for row in scored_rows if row["actual_label"] is True and row["predicted_label"] is True
    )
    true_negative = sum(
        1 for row in scored_rows if row["actual_label"] is False and row["predicted_label"] is False
    )
    false_positive = sum(
        1 for row in scored_rows if row["actual_label"] is False and row["predicted_label"] is True
    )
    false_negative = sum(
        1 for row in scored_rows if row["actual_label"] is True and row["predicted_label"] is False
    )
    brier_scores = [
        (row["probability"] - (1.0 if row["actual_label"] else 0.0)) ** 2 for row in scored_rows
    ]
    return {
        "prediction_count": len(scored_rows),
        "coverage_rate": round(len(scored_rows) / len(training_rows), 4) if training_rows else 0.0,
        "accuracy": round((true_positive + true_negative) / len(scored_rows), 4)
        if scored_rows
        else None,
        "brier_score": _mean_or_none(brier_scores),
        "positive_prediction_rate": _mean_or_none(
            1.0 if row["predicted_label"] else 0.0 for row in scored_rows
        ),
        "confusion_matrix": {
            "true_positive": true_positive,
            "true_negative": true_negative,
            "false_positive": false_positive,
            "false_negative": false_negative,
        },
    }


def _rank_training_benchmarks(
    benchmark_summary: dict[str, Any],
    *,
    task_type: str,
) -> list[dict[str, Any]]:
    validation_benchmarks = benchmark_summary.get("validation", {}).get("benchmarks", {})
    test_benchmarks = benchmark_summary.get("test", {}).get("benchmarks", {})
    if task_type == "regression":
        primary_metric = "mae"
        prefers_lower = True
    else:
        primary_metric = "brier_score"
        prefers_lower = True

    rankings = []
    baseline_names = sorted(set(validation_benchmarks) | set(test_benchmarks))
    for baseline_name in baseline_names:
        validation_metrics = validation_benchmarks.get(baseline_name, {})
        test_metrics = test_benchmarks.get(baseline_name, {})
        rankings.append(
            {
                "baseline_name": baseline_name,
                "primary_metric": primary_metric,
                "validation_primary_metric": validation_metrics.get(primary_metric),
                "test_primary_metric": test_metrics.get(primary_metric),
                "validation_prediction_count": validation_metrics.get("prediction_count", 0),
                "test_prediction_count": test_metrics.get("prediction_count", 0),
            }
        )

    def ranking_key(entry: dict[str, Any]) -> tuple[float, int, float]:
        test_value = entry["test_primary_metric"]
        validation_value = entry["validation_primary_metric"]
        test_prediction_count = entry["test_prediction_count"]
        sentinel = float("inf") if prefers_lower else float("-inf")
        comparable_test = test_value if test_value is not None else sentinel
        comparable_validation = validation_value if validation_value is not None else sentinel
        return (
            comparable_test,
            -test_prediction_count,
            comparable_validation,
        )

    return sorted(rankings, key=ranking_key)


def _normalize_probability(value: Any) -> float | None:
    if value is None:
        return None
    normalized = float(value)
    if normalized < 0:
        normalized = 0.0
    if normalized > 1:
        normalized = 1.0
    return round(normalized, 4)


def _pattern_dimension_value(row: dict[str, Any], dimension: str) -> str:
    if dimension == "venue":
        return str(row["venue"])
    if dimension == "days_rest_bucket":
        return _bucket_days_rest(row.get("days_rest"))
    if dimension == "games_played_bucket":
        return _bucket_games_played(row.get("games_played_prior"))
    if dimension == "prior_matchup_bucket":
        return _bucket_prior_matchup(row.get("prior_matchup_count"))
    if dimension == "rolling_3_spread_error_bucket":
        return _bucket_signed_metric(row.get("rolling_3_avg_spread_error"))
    if dimension == "rolling_3_total_error_bucket":
        return _bucket_signed_metric(row.get("rolling_3_avg_total_error"))
    if dimension == "rolling_3_cover_rate_bucket":
        return _bucket_rate(row.get("rolling_3_cover_rate"))
    if dimension == "rolling_3_over_rate_bucket":
        return _bucket_rate(row.get("rolling_3_over_rate"))
    raise ValueError(f"Unsupported pattern dimension: {dimension}")


def _find_dataset_anchor_row(
    dataset_rows: list[dict[str, Any]],
    *,
    canonical_game_id: int,
    team_code: str | None,
) -> dict[str, Any]:
    matches = [
        row
        for row in dataset_rows
        if int(row["canonical_game_id"]) == canonical_game_id
        and (team_code is None or row["team_code"] == team_code)
    ]
    if not matches:
        raise ValueError("No dataset row found for the requested comparable anchor.")
    if len(matches) > 1 and team_code is None:
        raise ValueError(
            "Comparable anchor requires team_code when a game has multiple perspectives."
        )
    return matches[0]


def _resolve_comparable_condition_values(
    *,
    anchor_row: dict[str, Any] | None,
    condition_values: tuple[str, ...] | None,
    dimensions: tuple[str, ...],
) -> tuple[str, ...]:
    if condition_values is not None:
        if len(condition_values) != len(dimensions):
            raise ValueError("condition_values must align 1:1 with dimensions.")
        return tuple(condition_values)
    if anchor_row is None:
        raise ValueError("Provide either canonical_game_id or explicit condition_values.")
    return tuple(str(_pattern_dimension_value(anchor_row, dimension)) for dimension in dimensions)


def _is_same_anchor_case(
    row: dict[str, Any],
    anchor_row: dict[str, Any] | None,
) -> bool:
    if anchor_row is None:
        return False
    return (
        int(row["canonical_game_id"]) == int(anchor_row["canonical_game_id"])
        and row["team_code"] == anchor_row["team_code"]
    )


def _serialize_anchor_case(
    anchor_row: dict[str, Any] | None,
    dimensions: tuple[str, ...],
) -> dict[str, Any] | None:
    if anchor_row is None:
        return None
    return {
        "canonical_game_id": anchor_row["canonical_game_id"],
        "game_date": anchor_row["game_date"],
        "season_label": anchor_row["season_label"],
        "team_code": anchor_row["team_code"],
        "opponent_code": anchor_row["opponent_code"],
        "venue": anchor_row["venue"],
        "matched_conditions": {
            dimension: _pattern_dimension_value(anchor_row, dimension) for dimension in dimensions
        },
        "target_values": {
            "point_margin_actual": anchor_row["point_margin_actual"],
            "spread_error_actual": anchor_row["spread_error_actual"],
            "total_error_actual": anchor_row["total_error_actual"],
            "covered_actual": anchor_row["covered_actual"],
            "went_over_actual": anchor_row["went_over_actual"],
        },
    }


def _serialize_comparable_case(
    row: dict[str, Any],
    dimensions: tuple[str, ...],
    *,
    similarity_score: float | None = None,
    similarity_breakdown: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "canonical_game_id": row["canonical_game_id"],
        "game_date": row["game_date"],
        "season_label": row["season_label"],
        "team_code": row["team_code"],
        "opponent_code": row["opponent_code"],
        "venue": row["venue"],
        "matched_conditions": {
            dimension: _pattern_dimension_value(row, dimension) for dimension in dimensions
        },
        "similarity_score": similarity_score,
        "similarity_breakdown": similarity_breakdown or {},
        "target_values": {
            "point_margin_actual": row["point_margin_actual"],
            "spread_error_actual": row["spread_error_actual"],
            "total_error_actual": row["total_error_actual"],
            "covered_actual": row["covered_actual"],
            "went_over_actual": row["went_over_actual"],
        },
    }


def _build_pattern_key(
    dimensions: tuple[str, ...],
    condition_values: tuple[str, ...],
) -> str:
    return "|".join(
        f"{dimension}={value}" for dimension, value in zip(dimensions, condition_values)
    )


def _parse_pattern_key(pattern_key: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    dimensions: list[str] = []
    condition_values: list[str] = []
    for segment in pattern_key.split("|"):
        if "=" not in segment:
            raise ValueError("Invalid pattern_key format.")
        dimension, value = segment.split("=", 1)
        dimension = dimension.strip()
        value = value.strip()
        if not dimension:
            raise ValueError("Invalid pattern_key format.")
        dimensions.append(dimension)
        condition_values.append(value)
    if not dimensions:
        raise ValueError("Invalid pattern_key format.")
    return tuple(dimensions), tuple(condition_values)


def _score_comparable_candidate(
    *,
    anchor_row: dict[str, Any] | None,
    candidate_row: dict[str, Any],
) -> dict[str, Any]:
    if anchor_row is None:
        return {
            "row": candidate_row,
            "similarity_score": None,
            "similarity_breakdown": {},
        }

    exact_match_columns = (
        "team_code",
        "opponent_code",
        "season_label",
    )
    breakdown: dict[str, Any] = {
        "exact_matches": {},
        "numeric_distances": {},
    }
    weighted_distance = 0.0
    weighted_components = 0.0

    for column in exact_match_columns:
        is_match = anchor_row.get(column) == candidate_row.get(column)
        breakdown["exact_matches"][column] = is_match
        weighted_distance += 0.0 if is_match else 1.0
        weighted_components += 1.0

    for column in FEATURE_COMPARABLE_DISTANCE_COLUMNS:
        anchor_value = anchor_row.get(column)
        candidate_value = candidate_row.get(column)
        if anchor_value is None and candidate_value is None:
            continue
        if anchor_value is None or candidate_value is None:
            breakdown["numeric_distances"][column] = None
            weighted_distance += 0.5
            weighted_components += 1.0
            continue
        difference = abs(float(anchor_value) - float(candidate_value))
        normalized_difference = difference / (1.0 + abs(float(anchor_value)))
        breakdown["numeric_distances"][column] = round(normalized_difference, 4)
        weighted_distance += normalized_difference
        weighted_components += 1.0

    similarity_score = (
        round(max(0.0, 1.0 - (weighted_distance / weighted_components)), 4)
        if weighted_components
        else None
    )
    breakdown["component_count"] = int(weighted_components)
    breakdown["weighted_distance"] = round(weighted_distance, 4)
    return {
        "row": candidate_row,
        "similarity_score": similarity_score,
        "similarity_breakdown": breakdown,
    }


def _build_pattern_metrics(
    rows: list[dict[str, Any]],
    *,
    task_config: dict[str, Any],
) -> dict[str, Any]:
    target_column = task_config["target_column"]
    target_values = [row[target_column] for row in rows if row.get(target_column) is not None]
    if task_config["task_type"] == "classification":
        boolean_values = [bool(value) for value in target_values]
        true_count = sum(1 for value in boolean_values if value is True)
        false_count = sum(1 for value in boolean_values if value is False)
        hit_rate = round(true_count / len(boolean_values), 4) if boolean_values else None
        variance = round(hit_rate * (1 - hit_rate), 4) if hit_rate is not None else None
        return {
            "sample_size": len(boolean_values),
            "target_mean": hit_rate,
            "target_median": hit_rate,
            "target_stddev": _pstdev_or_none(1.0 if value else 0.0 for value in boolean_values),
            "hit_rate": hit_rate,
            "variance": variance,
            "target_value_counts": {
                "true": true_count,
                "false": false_count,
            },
            "signal_strength": round(abs(hit_rate - 0.5), 4) if hit_rate is not None else None,
        }

    numeric_values = [float(value) for value in target_values]
    target_mean = _mean_or_none(numeric_values)
    target_median = round(float(median(numeric_values)), 4) if numeric_values else None
    target_stddev = _pstdev_or_none(numeric_values)
    return {
        "sample_size": len(numeric_values),
        "target_mean": target_mean,
        "target_median": target_median,
        "target_stddev": target_stddev,
        "hit_rate": None,
        "variance": round(target_stddev * target_stddev, 4) if target_stddev is not None else None,
        "target_value_counts": {},
        "signal_strength": round(abs(target_mean), 4) if target_mean is not None else None,
    }


def _bucket_days_rest(value: Any) -> str:
    if value is None:
        return "unknown_rest"
    numeric = int(value)
    if numeric <= 1:
        return "0_to_1_days"
    if numeric == 2:
        return "2_days"
    if numeric == 3:
        return "3_days"
    return "4_plus_days"


def _bucket_games_played(value: Any) -> str:
    if value is None:
        return "unknown_games_played"
    numeric = int(value)
    if numeric <= 4:
        return "0_to_4_games"
    if numeric <= 9:
        return "5_to_9_games"
    return "10_plus_games"


def _bucket_prior_matchup(value: Any) -> str:
    if value is None:
        return "unknown_matchups"
    numeric = int(value)
    if numeric == 0:
        return "0_prior_matchups"
    if numeric == 1:
        return "1_prior_matchup"
    return "2_plus_prior_matchups"


def _bucket_signed_metric(value: Any) -> str:
    if value is None:
        return "unknown"
    numeric = float(value)
    if numeric <= -1.0:
        return "negative"
    if numeric >= 1.0:
        return "positive"
    return "neutral"


def _bucket_rate(value: Any) -> str:
    if value is None:
        return "unknown_rate"
    numeric = float(value)
    if numeric < 0.4:
        return "low_rate"
    if numeric > 0.6:
        return "high_rate"
    return "balanced_rate"
