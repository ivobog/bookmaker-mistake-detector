from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any


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


@dataclass(slots=True)
class FeatureAnalysisArtifactRecord:
    id: int
    feature_version_id: int
    artifact_type: str
    target_task: str
    team_code: str | None
    season_label: str | None
    artifact_key: str
    dimensions: list[str]
    payload: dict[str, Any]
    created_at: datetime | None = None
    updated_at: datetime | None = None
