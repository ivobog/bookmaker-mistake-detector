from __future__ import annotations

from datetime import date
from typing import Any, Callable


def serialize_future_game_input(
    game: dict[str, Any],
    *,
    float_or_none: Callable[[Any], float | None],
) -> dict[str, Any]:
    game_date = coerce_date(game["game_date"])
    return {
        "season_label": str(game["season_label"]),
        "game_date": game_date.isoformat(),
        "home_team_code": str(game["home_team_code"]),
        "away_team_code": str(game["away_team_code"]),
        "home_spread_line": float_or_none(game.get("home_spread_line")),
        "total_line": float_or_none(game.get("total_line")),
    }


def coerce_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def build_future_slate_response(
    *,
    target_task: str,
    slate_label: str | None,
    game_inputs: list[dict[str, Any]],
    games: list[dict[str, Any]],
    serialize_input: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    serialized_inputs = [serialize_input(game) for game in game_inputs]
    game_dates = [entry["game_date"] for entry in serialized_inputs]
    scenario_keys = [
        str(game["scenario"]["scenario_key"])
        for game in games
        if isinstance(game.get("scenario"), dict)
        and game["scenario"].get("scenario_key") is not None
    ]
    prediction_count = sum(int(game.get("scored_prediction_count", 0)) for game in games)
    status_counts: dict[str, int] = {}
    for game in games:
        for preview in game.get("opportunity_preview", []):
            status = str(preview.get("status", "discarded"))
            status_counts[status] = status_counts.get(status, 0) + 1
    slate_key = build_future_slate_key(
        target_task=target_task,
        slate_label=slate_label,
        serialized_inputs=serialized_inputs,
    )
    return {
        "slate": {
            "slate_key": slate_key,
            "slate_label": slate_label,
            "target_task": target_task,
            "game_count": len(serialized_inputs),
            "game_dates": sorted(set(game_dates)),
            "scenario_keys": scenario_keys,
            "games": serialized_inputs,
        },
        "game_preview_count": len(games),
        "scored_prediction_count": prediction_count,
        "opportunity_preview_status_counts": status_counts,
        "games": games,
    }


def build_future_slate_key(
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
    return f"{target_task}:{first['game_date']}:{last['game_date']}:{len(serialized_inputs)}-games"


def materialize_future_game_preview(
    *,
    preview: dict[str, Any],
    target_task: str,
    default_policy_name: str | None,
    model_market_board_id: int | None,
    build_scoring_run: Callable[..., Any],
    save_scoring_run: Callable[[Any], Any],
    serialize_scoring_run: Callable[[Any], Any],
) -> dict[str, Any]:
    scoring_run = build_scoring_run(
        preview=preview,
        target_task=target_task,
        default_policy_name=default_policy_name,
        model_market_board_id=model_market_board_id,
    )
    persisted = save_scoring_run(scoring_run)
    return {
        **preview,
        "materialized_count": 1 if persisted is not None else 0,
        "scoring_run": serialize_scoring_run(persisted),
    }


def materialize_future_opportunities(
    *,
    materialized_preview: dict[str, Any],
    target_task: str,
    build_opportunities: Callable[..., list[Any]],
    save_opportunities: Callable[[list[Any]], list[Any]],
    serialize_opportunity: Callable[[Any], Any],
) -> dict[str, Any]:
    scoring_run = materialized_preview.get("scoring_run")
    opportunities = build_opportunities(
        scoring_preview=materialized_preview,
        target_task=target_task,
        model_scoring_run_id=(int(scoring_run["id"]) if scoring_run is not None else None),
        allow_best_effort_review=True,
    )
    persisted = save_opportunities(opportunities)
    return {
        **materialized_preview,
        "opportunity_count": len(persisted),
        "opportunities": [serialize_opportunity(entry) for entry in persisted],
    }


def get_future_slate_preview(
    *,
    games: list[dict[str, Any]],
    target_task: str,
    slate_label: str | None,
    preview_loader: Callable[[dict[str, Any]], dict[str, Any]],
    serialize_input: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    previews = [
        {
            "input": serialize_input(game),
            **preview_loader(game),
        }
        for game in games
    ]
    return build_future_slate_response(
        target_task=target_task,
        slate_label=slate_label,
        game_inputs=games,
        games=previews,
        serialize_input=serialize_input,
    )


def materialize_future_slate(
    *,
    games: list[dict[str, Any]],
    target_task: str,
    slate_label: str | None,
    materialize_preview_loader: Callable[[dict[str, Any]], dict[str, Any]],
    build_opportunities: Callable[..., list[Any]],
    save_opportunities: Callable[[list[Any]], list[Any]],
    serialize_opportunity: Callable[[Any], Any],
    serialize_input: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    materialized_games = []
    materialized_scoring_runs = []
    materialized_opportunities = []
    for game in games:
        materialized_preview = materialize_preview_loader(game)
        scoring_run = materialized_preview.get("scoring_run")
        opportunities = build_opportunities(
            scoring_preview=materialized_preview,
            target_task=target_task,
            model_scoring_run_id=(int(scoring_run["id"]) if scoring_run is not None else None),
            allow_best_effort_review=True,
        )
        persisted_opportunities = save_opportunities(opportunities)
        serialized_opportunities = [
            serialize_opportunity(entry) for entry in persisted_opportunities
        ]
        materialized_games.append(
            {
                "input": serialize_input(game),
                **materialized_preview,
                "opportunity_count": len(serialized_opportunities),
                "opportunities": serialized_opportunities,
            }
        )
        if scoring_run is not None:
            materialized_scoring_runs.append(scoring_run)
        materialized_opportunities.extend(serialized_opportunities)
    return {
        **build_future_slate_response(
            target_task=target_task,
            slate_label=slate_label,
            game_inputs=games,
            games=materialized_games,
            serialize_input=serialize_input,
        ),
        "materialized_scoring_run_count": len(materialized_scoring_runs),
        "materialized_opportunity_count": len(materialized_opportunities),
        "scoring_runs": materialized_scoring_runs,
        "opportunities": materialized_opportunities,
    }
