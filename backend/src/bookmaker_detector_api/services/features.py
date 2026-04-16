from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from statistics import mean, pstdev
from typing import Any

from bookmaker_detector_api.repositories import InMemoryIngestionRepository
from bookmaker_detector_api.repositories.ingestion import _json_dumps

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


@dataclass(slots=True)
class FeatureVersionRecord:
    id: int
    feature_key: str
    version_label: str
    description: str
    config: dict[str, Any]
    created_at: datetime | None = None


@dataclass(slots=True)
class CanonicalGameMetricRecord:
    canonical_game_id: int
    season_label: str
    game_date: date
    home_team_code: str
    away_team_code: str
    home_score: int
    away_score: int
    final_home_margin: int
    final_total_points: int
    total_line: float | None
    home_spread_line: float | None
    away_spread_line: float | None
    reconciliation_status: str
    source_row_indexes: list[int]
    warnings: list[str]
    spread_error_home: float | None
    spread_error_away: float | None
    total_error: float | None
    home_covered: bool | None
    away_covered: bool | None
    went_over: bool | None
    went_under: bool | None


@dataclass(slots=True)
class TeamPerspectiveGame:
    season_label: str
    game_date: date
    is_home: bool
    point_margin: float
    total_points: float
    spread_error: float | None
    total_error: float | None
    covered: bool | None
    went_over: bool | None


@dataclass(slots=True)
class FeatureSnapshotRecord:
    id: int
    canonical_game_id: int
    feature_version_id: int
    season_label: str
    game_date: date
    home_team_code: str
    away_team_code: str
    feature_payload: dict[str, Any]
    created_at: datetime | None = None


def materialize_baseline_feature_snapshots_for_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    version_label: str = "Baseline Team Features v1",
    description: str = "Time-safe rolling team and matchup features for canonical games.",
    windows: tuple[int, ...] = DEFAULT_FEATURE_WINDOWS,
) -> dict[str, Any]:
    feature_version = ensure_feature_version_in_memory(
        repository,
        feature_key=feature_key,
        version_label=version_label,
        description=description,
        config={"windows": list(windows)},
    )
    canonical_games = list_canonical_game_metric_records_in_memory(repository)
    snapshots = build_feature_snapshots(
        canonical_games,
        feature_version_id=feature_version.id,
        windows=windows,
    )
    saved_count = save_feature_snapshots_in_memory(repository, snapshots)
    persisted_snapshots = list_feature_snapshots_in_memory(
        repository,
        feature_version_id=feature_version.id,
    )
    return {
        "feature_version": asdict(feature_version),
        "canonical_game_count": len(canonical_games),
        "snapshots_saved": saved_count,
        "feature_snapshots": [asdict(snapshot) for snapshot in persisted_snapshots],
    }


def materialize_baseline_feature_snapshots_for_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    version_label: str = "Baseline Team Features v1",
    description: str = "Time-safe rolling team and matchup features for canonical games.",
    windows: tuple[int, ...] = DEFAULT_FEATURE_WINDOWS,
) -> dict[str, Any]:
    ensure_feature_tables(connection)
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


def ensure_feature_version_in_memory(
    repository: InMemoryIngestionRepository,
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
    repository: InMemoryIngestionRepository,
) -> list[CanonicalGameMetricRecord]:
    metrics_by_game_id = {
        entry["canonical_game_id"]: entry for entry in repository.metrics
    }
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
    repository: InMemoryIngestionRepository,
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
    repository: InMemoryIngestionRepository,
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
    repository: InMemoryIngestionRepository,
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
    repository: InMemoryIngestionRepository,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
) -> FeatureVersionRecord | None:
    for entry in repository.feature_versions:
        if entry["feature_key"] == feature_key:
            return FeatureVersionRecord(**entry)
    return None


def get_feature_snapshot_catalog_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    team_code: str | None = None,
    season_label: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> dict[str, Any]:
    feature_version = get_feature_version_in_memory(
        repository,
        feature_key=feature_key,
    )
    if feature_version is None:
        return {
            "feature_version": None,
            "snapshot_count": 0,
            "feature_snapshots": [],
        }

    snapshots = list_feature_snapshots_in_memory(
        repository,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
        limit=limit,
        offset=offset,
    )
    snapshot_count = count_feature_snapshots_in_memory(
        repository,
        feature_key=feature_key,
        team_code=team_code,
        season_label=season_label,
    )
    return {
        "feature_version": asdict(feature_version),
        "snapshot_count": snapshot_count,
        "feature_snapshots": [asdict(snapshot) for snapshot in snapshots],
    }


def get_feature_snapshot_summary_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    team_code: str | None = None,
    season_label: str | None = None,
) -> dict[str, Any]:
    feature_version = get_feature_version_in_memory(
        repository,
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

    snapshots = list_feature_snapshots_in_memory(
        repository,
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


def get_feature_dataset_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    team_code: str | None = None,
    season_label: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    feature_version = get_feature_version_in_memory(
        repository,
        feature_key=feature_key,
    )
    if feature_version is None:
        return {
            "feature_version": None,
            "row_count": 0,
            "feature_rows": [],
        }

    snapshots = list_feature_snapshots_in_memory(
        repository,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    canonical_games = list_canonical_game_metric_records_in_memory(repository)
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


def get_feature_dataset_profile_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    team_code: str | None = None,
    season_label: str | None = None,
) -> dict[str, Any]:
    feature_version = get_feature_version_in_memory(
        repository,
        feature_key=feature_key,
    )
    if feature_version is None:
        return {
            "feature_version": None,
            "row_count": 0,
            "profile": {},
        }

    snapshots = list_feature_snapshots_in_memory(
        repository,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    canonical_games = list_canonical_game_metric_records_in_memory(repository)
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


def get_feature_training_view_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    team_code: str | None = None,
    season_label: str | None = None,
    drop_null_targets: bool = True,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    feature_version = get_feature_version_in_memory(
        repository,
        feature_key=feature_key,
    )
    if feature_version is None:
        return {
            "feature_version": None,
            "row_count": 0,
            "task": None,
            "training_rows": [],
        }

    snapshots = list_feature_snapshots_in_memory(
        repository,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    canonical_games = list_canonical_game_metric_records_in_memory(repository)
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


def get_feature_training_manifest_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    target_task: str,
    team_code: str | None = None,
    season_label: str | None = None,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    feature_version = get_feature_version_in_memory(
        repository,
        feature_key=feature_key,
    )
    if feature_version is None:
        return {
            "feature_version": None,
            "row_count": 0,
            "task": None,
            "training_manifest": {},
        }

    snapshots = list_feature_snapshots_in_memory(
        repository,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    canonical_games = list_canonical_game_metric_records_in_memory(repository)
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


def get_feature_training_task_matrix_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    team_code: str | None = None,
    season_label: str | None = None,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    drop_null_targets: bool = True,
) -> dict[str, Any]:
    feature_version = get_feature_version_in_memory(
        repository,
        feature_key=feature_key,
    )
    if feature_version is None:
        return {
            "feature_version": None,
            "dataset_row_count": 0,
            "task_matrix": {},
        }

    snapshots = list_feature_snapshots_in_memory(
        repository,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    canonical_games = list_canonical_game_metric_records_in_memory(repository)
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


def get_feature_training_bundle_in_memory(
    repository: InMemoryIngestionRepository,
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
    feature_version = get_feature_version_in_memory(
        repository,
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

    snapshots = list_feature_snapshots_in_memory(
        repository,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    canonical_games = list_canonical_game_metric_records_in_memory(repository)
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


def get_feature_dataset_splits_in_memory(
    repository: InMemoryIngestionRepository,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
    team_code: str | None = None,
    season_label: str | None = None,
    train_ratio: float = 0.7,
    validation_ratio: float = 0.15,
    preview_limit: int = 5,
) -> dict[str, Any]:
    feature_version = get_feature_version_in_memory(
        repository,
        feature_key=feature_key,
    )
    if feature_version is None:
        return {
            "feature_version": None,
            "row_count": 0,
            "split_summary": {},
            "split_previews": {},
        }

    snapshots = list_feature_snapshots_in_memory(
        repository,
        feature_version_id=feature_version.id,
        team_code=team_code,
        season_label=season_label,
        limit=None,
    )
    canonical_games = list_canonical_game_metric_records_in_memory(repository)
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


def ensure_feature_tables(connection: Any) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS feature_version (
                id BIGSERIAL PRIMARY KEY,
                feature_key VARCHAR(64) NOT NULL UNIQUE,
                version_label VARCHAR(128) NOT NULL,
                description TEXT,
                config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS game_feature_snapshot (
                id BIGSERIAL PRIMARY KEY,
                canonical_game_id BIGINT NOT NULL REFERENCES canonical_game(id) ON DELETE CASCADE,
                feature_version_id BIGINT NOT NULL REFERENCES feature_version(id) ON DELETE CASCADE,
                season_id INTEGER NOT NULL REFERENCES season(id),
                game_date DATE NOT NULL,
                home_team_id INTEGER NOT NULL REFERENCES team(id),
                away_team_id INTEGER NOT NULL REFERENCES team(id),
                feature_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (feature_version_id, canonical_game_id)
            )
            """
        )
    connection.commit()


def ensure_feature_version_postgres(
    connection: Any,
    *,
    feature_key: str,
    version_label: str,
    description: str,
    config: dict[str, Any],
) -> FeatureVersionRecord:
    ensure_feature_tables(connection)
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
    ensure_feature_tables(connection)
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
    ensure_feature_tables(connection)
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
    ensure_feature_tables(connection)
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


def get_feature_version_postgres(
    connection: Any,
    *,
    feature_key: str = DEFAULT_FEATURE_KEY,
) -> FeatureVersionRecord | None:
    ensure_feature_tables(connection)
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
    canonical_games_by_id = {
        game.canonical_game_id: game for game in canonical_games
    }
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
            "covered_actual": _boolean_value_counts(
                row["covered_actual"] for row in dataset_rows
            ),
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
            split_name: rows[:preview_limit]
            for split_name, rows in split_rows.items()
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
    validation_games = set(
        game_order[train_game_count : train_game_count + validation_game_count]
    )
    test_games = set(game_order[train_game_count + validation_game_count :])

    return {
        "train": [
            row
            for game_id in game_order
            for row in rows_by_game[game_id]
            if game_id in train_games
        ],
        "validation": [
            row
            for game_id in game_order
            for row in rows_by_game[game_id]
            if game_id in validation_games
        ],
        "test": [
            row
            for game_id in game_order
            for row in rows_by_game[game_id]
            if game_id in test_games
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
                column
                for column in FEATURE_DATASET_LABEL_COLUMNS
                if column != target_column
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
        row["target_value"]
        for row in training_rows
        if row["target_value"] is not None
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
        {
            feature_name
            for row in training_rows
            for feature_name in row["features"].keys()
        }
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
            column: _coverage_summary(
                row["features"].get(column) for row in training_rows
            )
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
                1.0 if entry["payload"]["is_back_to_back"] else 0.0
                for entry in perspectives
            ),
            "avg_cover_streak": _mean_or_none(
                entry["payload"]["trend_signals"]["current_cover_streak"]
                for entry in perspectives
            ),
            "avg_non_cover_streak": _mean_or_none(
                entry["payload"]["trend_signals"]["current_non_cover_streak"]
                for entry in perspectives
            ),
            "avg_over_streak": _mean_or_none(
                entry["payload"]["trend_signals"]["current_over_streak"]
                for entry in perspectives
            ),
            "avg_under_streak": _mean_or_none(
                entry["payload"]["trend_signals"]["current_under_streak"]
                for entry in perspectives
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
        "rolling_10_avg_point_margin": rolling_10["avg_point_margin"],
        "rolling_10_avg_total_points": rolling_10["avg_total_points"],
        "rolling_10_avg_spread_error": rolling_10["avg_spread_error"],
        "rolling_10_avg_total_error": rolling_10["avg_total_error"],
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


def _build_team_feature_payload(
    *,
    team_code: str,
    prior_games: list[TeamPerspectiveGame],
    current_game_date: date,
    current_season_label: str,
    windows: tuple[int, ...],
) -> dict[str, Any]:
    last_game_date = prior_games[-1].game_date if prior_games else None
    days_rest = (
        (current_game_date - last_game_date).days
        if last_game_date is not None
        else None
    )
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
            str(window): _build_window_summary(prior_games[-window:])
            for window in windows
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
        "avg_sample_size": _mean_or_none(
            payload["sample_size"] for payload in window_payloads
        ),
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
            payload["over_rate"]
            for payload in window_payloads
            if payload["over_rate"] is not None
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
        "point_margin_volatility": _pstdev_or_none(
            game.point_margin for game in prior_games
        ),
        "total_points_volatility": _pstdev_or_none(
            game.total_points for game in prior_games
        ),
        "spread_error_volatility": _pstdev_or_none(
            game.spread_error for game in prior_games if game.spread_error is not None
        ),
        "total_error_volatility": _pstdev_or_none(
            game.total_error for game in prior_games if game.total_error is not None
        ),
        "cover_rate": _mean_or_none(
            1.0 if game.covered else 0.0
            for game in prior_games
            if game.covered is not None
        ),
        "over_rate": _mean_or_none(
            1.0 if game.went_over else 0.0
            for game in prior_games
            if game.went_over is not None
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
        value_getter(game)
        for game in recent_games
        if value_getter(game) is not None
    )
    baseline_average = _mean_or_none(
        value_getter(game)
        for game in baseline_games
        if value_getter(game) is not None
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
