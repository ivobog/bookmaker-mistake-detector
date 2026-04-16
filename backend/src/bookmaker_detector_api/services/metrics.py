from __future__ import annotations

from bookmaker_detector_api.ingestion.models import CanonicalGame, GameMetric


def calculate_game_metric(game: CanonicalGame) -> GameMetric:
    total_error = (
        game.final_total_points - game.total_line if game.total_line is not None else None
    )
    spread_error_home = (
        game.final_home_margin + game.home_spread_line
        if game.home_spread_line is not None
        else None
    )
    spread_error_away = (
        -game.final_home_margin + game.away_spread_line
        if game.away_spread_line is not None
        else None
    )

    home_covered = spread_error_home > 0 if spread_error_home is not None else None
    away_covered = spread_error_away > 0 if spread_error_away is not None else None
    went_over = total_error > 0 if total_error is not None else None
    went_under = total_error < 0 if total_error is not None else None

    return GameMetric(
        spread_error_home=spread_error_home,
        spread_error_away=spread_error_away,
        total_error=total_error,
        home_covered=home_covered,
        away_covered=away_covered,
        went_over=went_over,
        went_under=went_under,
    )

