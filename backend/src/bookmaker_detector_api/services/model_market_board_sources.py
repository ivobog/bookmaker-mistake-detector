from __future__ import annotations

import csv
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from bookmaker_detector_api.config import settings
from bookmaker_detector_api.repositories.ingestion import _json_dumps
from bookmaker_detector_api.services.model_records import (
    MarketBoardSourceProvider,
    ModelMarketBoardRecord,
)

MARKET_BOARD_SOURCE_CONFIGS = {
    "demo_daily_lines_v1": {
        "description": (
            "Deterministic built-in upcoming slate source for local scoring "
            "and refresh flows."
        ),
        "default_game_count": 2,
        "supports_refresh": True,
    },
    "demo_source_failure_v1": {
        "description": (
            "Intentional failing source provider for refresh diagnostics and "
            "error-path testing."
        ),
        "default_game_count": 2,
        "supports_refresh": True,
        "demo_failure": True,
    },
    "demo_partial_lines_v1": {
        "description": (
            "Mixed-quality source provider with valid and invalid rows for "
            "normalization and validation-path testing."
        ),
        "default_game_count": 3,
        "supports_refresh": True,
        "mixed_quality_demo": True,
    },
    "file_market_board_v1": {
        "description": (
            "File-backed market board source that loads upcoming games from a "
            "JSON or CSV file path."
        ),
        "default_game_count": 0,
        "supports_refresh": True,
        "requires_source_path": True,
        "supported_formats": ["json", "csv"],
    },
    "the_odds_api_v4_nba": {
        "description": (
            "External NBA odds source backed by The Odds API v4 upcoming odds "
            "endpoint."
        ),
        "default_game_count": 0,
        "supports_refresh": True,
        "requires_api_key": True,
        "requires_network": True,
    },
}

DEMO_MARKET_BOARD_GAME_TEMPLATES = [
    {
        "home_team_code": "LAL",
        "away_team_code": "BOS",
        "home_spread_line": -3.5,
        "total_line": 228.5,
    },
    {
        "home_team_code": "NYK",
        "away_team_code": "MIA",
        "home_spread_line": -1.5,
        "total_line": 219.5,
    },
    {
        "home_team_code": "GSW",
        "away_team_code": "DEN",
        "home_spread_line": 2.0,
        "total_line": 231.0,
    },
    {
        "home_team_code": "PHX",
        "away_team_code": "DAL",
        "home_spread_line": 1.5,
        "total_line": 226.5,
    },
]


def list_model_market_board_sources() -> dict[str, Any]:
    return {
        "sources": [
            {
                "source_name": source_name,
                **config,
            }
            for source_name, config in MARKET_BOARD_SOURCE_CONFIGS.items()
        ]
    }


def _build_demo_daily_lines_source_games(
    *,
    season_label: str,
    game_date: date,
    game_count: int | None = None,
    source_path: str | None = None,
) -> list[dict[str, Any]]:
    del source_path
    resolved_count = game_count or int(
        MARKET_BOARD_SOURCE_CONFIGS["demo_daily_lines_v1"]["default_game_count"]
    )
    template_count = len(DEMO_MARKET_BOARD_GAME_TEMPLATES)
    start_index = game_date.toordinal() % template_count
    games = []
    for offset in range(resolved_count):
        template = DEMO_MARKET_BOARD_GAME_TEMPLATES[(start_index + offset) % template_count]
        games.append(
            {
                "season_label": season_label,
                "game_date": game_date,
                "home_team_code": template["home_team_code"],
                "away_team_code": template["away_team_code"],
                "home_spread_line": template["home_spread_line"],
                "total_line": template["total_line"],
            }
        )
    return games


def _build_demo_failing_source_games(
    *,
    season_label: str,
    game_date: date,
    game_count: int | None = None,
    source_path: str | None = None,
) -> list[dict[str, Any]]:
    del season_label, game_date, game_count, source_path
    raise RuntimeError(
        "Demo market-board source failure triggered intentionally for diagnostics."
    )


def _build_demo_partial_source_games(
    *,
    season_label: str,
    game_date: date,
    game_count: int | None = None,
    source_path: str | None = None,
) -> list[dict[str, Any]]:
    del game_count, source_path
    return [
        {
            "season_label": season_label,
            "game_date": game_date,
            "home_team_code": "lal",
            "away_team_code": "bos",
            "home_spread_line": "-3.5",
            "total_line": "228.5",
        },
        {
            "season_label": season_label,
            "game_date": game_date,
            "home_team_code": "NYK",
            "away_team_code": "NYK",
            "home_spread_line": "-1.5",
            "total_line": "219.5",
        },
        {
            "season_label": season_label,
            "game_date": "not-a-date",
            "home_team_code": "GSW",
            "away_team_code": "DEN",
            "home_spread_line": "bad-number",
            "total_line": "231.0",
        },
    ]


def _resolve_market_board_source_path(source_path: str) -> Path:
    if source_path.startswith("fixture://"):
        fixture_name = source_path.removeprefix("fixture://")
        return (
            Path(__file__).resolve().parents[1] / "fixtures" / fixture_name
        ).resolve()
    return Path(source_path).expanduser().resolve()


def _build_file_market_board_source_games(
    *,
    season_label: str,
    game_date: date,
    game_count: int | None = None,
    source_path: str | None = None,
) -> list[dict[str, Any]]:
    del game_count
    if source_path is None:
        raise ValueError("file_market_board_v1 requires source_path.")
    resolved_path = _resolve_market_board_source_path(source_path)
    if not resolved_path.exists():
        raise FileNotFoundError(f"Market board source file not found: {resolved_path}")
    suffix = resolved_path.suffix.lower()
    if suffix == ".json":
        payload = json.loads(resolved_path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError("JSON market board source must contain a list of games.")
        rows = payload
    elif suffix == ".csv":
        with resolved_path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
    else:
        raise ValueError(
            "Unsupported market board source file format. Expected .json or .csv."
        )
    games = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Each market board source row must be an object.")
        payload = dict(row)
        payload.setdefault("season_label", season_label)
        payload.setdefault("game_date", game_date)
        games.append(payload)
    return games


def _fetch_the_odds_api_games() -> list[dict[str, Any]]:
    if not settings.the_odds_api_key:
        raise ValueError("the_odds_api_v4_nba requires THE_ODDS_API_KEY.")
    params = {
        "apiKey": settings.the_odds_api_key,
        "regions": settings.the_odds_api_regions,
        "markets": settings.the_odds_api_markets,
        "oddsFormat": settings.the_odds_api_odds_format,
    }
    if settings.the_odds_api_bookmakers:
        params["bookmakers"] = settings.the_odds_api_bookmakers
    base_url = settings.the_odds_api_base_url.rstrip("/")
    request_url = (
        f"{base_url}/sports/{settings.the_odds_api_sport_key}/odds?{urlencode(params)}"
    )
    request = Request(
        request_url,
        headers={
            "Accept": "application/json",
            "User-Agent": "bookmaker-mistake-detector/0.1.0",
        },
    )
    try:
        with urlopen(request, timeout=settings.the_odds_api_timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(
            f"The Odds API request failed with status {exc.code}: {body or exc.reason}"
        ) from exc
    except URLError as exc:
        raise RuntimeError(f"The Odds API request failed: {exc.reason}") from exc
    if not isinstance(payload, list):
        raise ValueError("The Odds API response must be a list of event objects.")
    return payload


def _extract_the_odds_api_game(
    *,
    season_label: str,
    event: dict[str, Any],
) -> dict[str, Any]:
    home_team = str(event.get("home_team") or "").strip().upper()
    away_team = str(event.get("away_team") or "").strip().upper()
    commence_time_raw = event.get("commence_time")
    if not commence_time_raw:
        raise ValueError("The Odds API event is missing commence_time.")
    commence_time = datetime.fromisoformat(
        str(commence_time_raw).replace("Z", "+00:00")
    ).astimezone(timezone.utc)
    spread_points: list[float] = []
    total_points: list[float] = []
    for bookmaker in event.get("bookmakers", []):
        if not isinstance(bookmaker, dict):
            continue
        for market in bookmaker.get("markets", []):
            if not isinstance(market, dict):
                continue
            market_key = str(market.get("key") or "")
            outcomes = market.get("outcomes", [])
            if market_key == "spreads":
                for outcome in outcomes:
                    if not isinstance(outcome, dict):
                        continue
                    if str(outcome.get("name") or "").strip().upper() != home_team:
                        continue
                    point = _float_or_none_value(outcome.get("point"))
                    if point is not None:
                        spread_points.append(point)
            if market_key == "totals":
                for outcome in outcomes:
                    if not isinstance(outcome, dict):
                        continue
                    if str(outcome.get("name") or "").strip().lower() != "over":
                        continue
                    point = _float_or_none_value(outcome.get("point"))
                    if point is not None:
                        total_points.append(point)
    return {
        "season_label": season_label,
        "game_date": commence_time.date(),
        "home_team_code": home_team,
        "away_team_code": away_team,
        "home_spread_line": median(spread_points) if spread_points else None,
        "total_line": median(total_points) if total_points else None,
    }


def _build_the_odds_api_source_games(
    *,
    season_label: str,
    game_date: date,
    game_count: int | None = None,
    source_path: str | None = None,
) -> list[dict[str, Any]]:
    del game_date, source_path
    events = _fetch_the_odds_api_games()
    games = [
        _extract_the_odds_api_game(season_label=season_label, event=event)
        for event in events
        if isinstance(event, dict)
    ]
    if game_count is not None and game_count > 0:
        return games[:game_count]
    return games


MARKET_BOARD_SOURCE_PROVIDERS: dict[str, MarketBoardSourceProvider] = {
    "demo_daily_lines_v1": _build_demo_daily_lines_source_games,
    "demo_source_failure_v1": _build_demo_failing_source_games,
    "demo_partial_lines_v1": _build_demo_partial_source_games,
    "file_market_board_v1": _build_file_market_board_source_games,
    "the_odds_api_v4_nba": _build_the_odds_api_source_games,
}


def _resolve_market_board_source_provider(source_name: str) -> MarketBoardSourceProvider:
    if source_name not in MARKET_BOARD_SOURCE_CONFIGS:
        raise ValueError(f"Unsupported market board source: {source_name}")
    provider = MARKET_BOARD_SOURCE_PROVIDERS.get(source_name)
    if provider is None:
        raise ValueError(f"No provider registered for market board source: {source_name}")
    return provider


def _run_market_board_source_provider(
    *,
    source_name: str,
    season_label: str,
    game_date: date,
    game_count: int | None = None,
    source_path: str | None = None,
) -> list[dict[str, Any]]:
    provider = _resolve_market_board_source_provider(source_name)
    return provider(
        season_label=season_label,
        game_date=game_date,
        game_count=game_count,
        source_path=source_path,
    )


def build_model_market_board_source_games(
    *,
    source_name: str,
    season_label: str,
    game_date: date,
    game_count: int | None = None,
    source_path: str | None = None,
) -> list[dict[str, Any]]:
    return _run_market_board_source_provider(
        source_name=source_name,
        season_label=season_label,
        game_date=game_date,
        game_count=game_count,
        source_path=source_path,
    )


def _normalize_market_board_source_games(
    *,
    source_name: str,
    season_label: str,
    game_date: date,
    raw_games: list[dict[str, Any]],
) -> dict[str, Any]:
    normalized_games: list[dict[str, Any]] = []
    invalid_entries: list[dict[str, Any]] = []
    warning_entries: list[dict[str, Any]] = []
    for row_index, raw_game in enumerate(raw_games):
        issues: list[str] = []
        raw_payload = dict(raw_game)
        resolved_season_label = str(raw_game.get("season_label") or season_label).strip()
        resolved_home_team = str(raw_game.get("home_team_code") or "").strip().upper()
        resolved_away_team = str(raw_game.get("away_team_code") or "").strip().upper()
        try:
            resolved_game_date = _coerce_date_value(raw_game.get("game_date") or game_date)
        except Exception:
            issues.append("invalid_game_date")
            resolved_game_date = None
        if not resolved_season_label:
            issues.append("missing_season_label")
        if not resolved_home_team:
            issues.append("missing_home_team_code")
        if not resolved_away_team:
            issues.append("missing_away_team_code")
        if resolved_home_team and resolved_away_team and resolved_home_team == resolved_away_team:
            issues.append("duplicate_team_codes")

        normalized_home_spread_line = None
        if raw_game.get("home_spread_line") is not None:
            try:
                normalized_home_spread_line = _float_or_none_value(
                    raw_game.get("home_spread_line")
                )
            except Exception:
                issues.append("invalid_home_spread_line")
        normalized_total_line = None
        if raw_game.get("total_line") is not None:
            try:
                normalized_total_line = _float_or_none_value(raw_game.get("total_line"))
            except Exception:
                issues.append("invalid_total_line")

        if issues:
            invalid_entries.append(
                {
                    "row_index": row_index,
                    "issues": issues,
                    "raw_game": raw_payload,
                }
            )
            continue

        normalized_game = {
            "season_label": resolved_season_label,
            "game_date": resolved_game_date,
            "home_team_code": resolved_home_team,
            "away_team_code": resolved_away_team,
            "home_spread_line": normalized_home_spread_line,
            "total_line": normalized_total_line,
        }
        serialized_normalized_game = _serialize_future_game_input_value(normalized_game)
        if _normalize_source_payload_value(raw_payload) != _normalize_source_payload_value(
            serialized_normalized_game
        ):
            warning_entries.append(
                {
                    "row_index": row_index,
                    "warning_type": "normalized_fields",
                    "raw_game": raw_payload,
                    "normalized_game": serialized_normalized_game,
                }
            )
        normalized_games.append(normalized_game)

    validation_summary = {
        "source_name": source_name,
        "raw_row_count": len(raw_games),
        "valid_row_count": len(normalized_games),
        "invalid_row_count": len(invalid_entries),
        "warning_count": len(warning_entries),
        "invalid_entries": invalid_entries,
        "warnings": warning_entries,
    }
    return {
        "normalized_games": normalized_games,
        "validation_summary": validation_summary,
    }


def _normalize_source_payload_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _normalize_source_payload_value(value[key])
            for key in sorted(value.keys(), key=str)
        }
    if isinstance(value, list):
        return [_normalize_source_payload_value(entry) for entry in value]
    if isinstance(value, date):
        return value.isoformat()
    return value


def _build_market_board_source_payload_fingerprints(
    *,
    raw_games: list[dict[str, Any]],
    normalized_games: list[dict[str, Any]],
) -> dict[str, Any]:
    raw_payload = _normalize_source_payload_value(raw_games)
    normalized_payload = _normalize_source_payload_value(
        [_serialize_future_game_input_value(game) for game in normalized_games]
    )
    raw_serialized = _json_dumps(raw_payload)
    normalized_serialized = _json_dumps(normalized_payload)
    return {
        "raw_game_count": len(raw_games),
        "normalized_game_count": len(normalized_games),
        "raw_payload_sha256": hashlib.sha256(raw_serialized.encode("utf-8")).hexdigest(),
        "normalized_payload_sha256": hashlib.sha256(
            normalized_serialized.encode("utf-8")
        ).hexdigest(),
    }


def _build_market_board_source_request_context(
    *,
    source_name: str,
    source_path: str | None,
) -> dict[str, Any] | None:
    if source_name == "file_market_board_v1":
        return {"source_path": source_path}
    if source_name == "the_odds_api_v4_nba":
        return {
            "base_url": settings.the_odds_api_base_url,
            "sport_key": settings.the_odds_api_sport_key,
            "regions": settings.the_odds_api_regions,
            "markets": settings.the_odds_api_markets,
            "odds_format": settings.the_odds_api_odds_format,
            "bookmakers": settings.the_odds_api_bookmakers,
        }
    return None


def _build_market_board_source_fingerprint_comparison(
    *,
    existing_board: ModelMarketBoardRecord | None,
    current_fingerprints: dict[str, Any],
) -> dict[str, Any]:
    source_payload = existing_board.payload.get("source", {}) if existing_board else {}
    previous_fingerprints = source_payload.get("source_payload_fingerprints")
    if not isinstance(previous_fingerprints, dict):
        return {
            "previous_fingerprints_available": False,
            "raw_payload_changed": None,
            "normalized_payload_changed": None,
            "raw_changed_but_normalized_same": None,
        }
    previous_raw_hash = previous_fingerprints.get("raw_payload_sha256")
    previous_normalized_hash = previous_fingerprints.get("normalized_payload_sha256")
    current_raw_hash = current_fingerprints.get("raw_payload_sha256")
    current_normalized_hash = current_fingerprints.get("normalized_payload_sha256")
    raw_payload_changed = previous_raw_hash != current_raw_hash
    normalized_payload_changed = previous_normalized_hash != current_normalized_hash
    return {
        "previous_fingerprints_available": True,
        "previous_raw_payload_sha256": previous_raw_hash,
        "previous_normalized_payload_sha256": previous_normalized_hash,
        "current_raw_payload_sha256": current_raw_hash,
        "current_normalized_payload_sha256": current_normalized_hash,
        "raw_payload_changed": raw_payload_changed,
        "normalized_payload_changed": normalized_payload_changed,
        "raw_changed_but_normalized_same": raw_payload_changed and not normalized_payload_changed,
    }


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
