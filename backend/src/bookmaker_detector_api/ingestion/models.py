from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date
from enum import StrEnum
from typing import Any


class ParseStatus(StrEnum):
    VALID = "VALID"
    VALID_WITH_WARNINGS = "VALID_WITH_WARNINGS"
    INVALID = "INVALID"


class ReconciliationStatus(StrEnum):
    FULL_MATCH = "FULL_MATCH"
    PARTIAL_SINGLE_ROW = "PARTIAL_SINGLE_ROW"
    CONFLICT_SCORE = "CONFLICT_SCORE"
    CONFLICT_TOTAL_LINE = "CONFLICT_TOTAL_LINE"
    CONFLICT_SPREAD_LINE = "CONFLICT_SPREAD_LINE"


@dataclass(slots=True)
class RawGameRow:
    provider_name: str
    team_code: str
    season_label: str
    source_url: str
    source_section: str
    source_row_index: int
    game_date: date
    opponent_code: str
    is_away: bool
    result_flag: str
    team_score: int
    opponent_score: int
    ats_result: str | None
    ats_line: float | None
    ou_result: str | None
    total_line: float | None
    parse_status: ParseStatus
    warnings: list[str] = field(default_factory=list)
    source_page_url: str | None = None
    source_page_season_label: str | None = None
    parser_provenance: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["parse_status"] = self.parse_status.value
        return payload


@dataclass(slots=True)
class CanonicalGame:
    season_label: str
    game_date: date
    home_team_code: str
    away_team_code: str
    home_score: int
    away_score: int
    total_line: float | None
    home_spread_line: float | None
    away_spread_line: float | None
    reconciliation_status: ReconciliationStatus
    source_row_indexes: list[int]
    warnings: list[str] = field(default_factory=list)

    @property
    def final_home_margin(self) -> int:
        return self.home_score - self.away_score

    @property
    def final_total_points(self) -> int:
        return self.home_score + self.away_score

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["reconciliation_status"] = self.reconciliation_status.value
        payload["final_home_margin"] = self.final_home_margin
        payload["final_total_points"] = self.final_total_points
        return payload


@dataclass(slots=True)
class GameMetric:
    spread_error_home: float | None
    spread_error_away: float | None
    total_error: float | None
    home_covered: bool | None
    away_covered: bool | None
    went_over: bool | None
    went_under: bool | None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
