from __future__ import annotations

from collections import defaultdict

from bookmaker_detector_api.ingestion.models import CanonicalGame, RawGameRow, ReconciliationStatus


def canonicalize_rows(rows: list[RawGameRow]) -> list[CanonicalGame]:
    grouped: dict[tuple[str, str, str, str], list[RawGameRow]] = defaultdict(list)
    for row in rows:
        normalized_team_code = _normalize_team_code(row.team_code)
        normalized_opponent_code = _normalize_team_code(row.opponent_code)
        home_team = normalized_opponent_code if row.is_away else normalized_team_code
        away_team = normalized_team_code if row.is_away else normalized_opponent_code
        key = (
            row.season_label,
            row.game_date.isoformat(),
            home_team,
            away_team,
        )
        grouped[key].append(row)

    canonical_games: list[CanonicalGame] = []
    for key, matches in grouped.items():
        season_label, game_date_iso, home_team, away_team = key
        home_rows = [row for row in matches if row.team_code == home_team]
        away_rows = [row for row in matches if row.team_code == away_team]
        warnings: list[str] = []

        home_row = home_rows[0] if home_rows else next(
            (row for row in matches if row.team_code != away_team),
            matches[0],
        )
        away_row = away_rows[0] if away_rows else next(
            (row for row in matches if row.team_code != home_team),
            matches[0],
        )

        if home_rows and away_rows:
            reconciliation_status = ReconciliationStatus.FULL_MATCH
            if (
                home_row.team_score != away_row.opponent_score
                or home_row.opponent_score != away_row.team_score
            ):
                reconciliation_status = ReconciliationStatus.CONFLICT_SCORE
                warnings.append("canonical.score_mismatch")
            elif (
                home_row.total_line is not None
                and away_row.total_line is not None
                and home_row.total_line != away_row.total_line
            ):
                reconciliation_status = ReconciliationStatus.CONFLICT_TOTAL_LINE
                warnings.append("canonical.total_line_mismatch")
            elif (
                home_row.ats_line is not None
                and away_row.ats_line is not None
                and home_row.ats_line != -away_row.ats_line
            ):
                reconciliation_status = ReconciliationStatus.CONFLICT_SPREAD_LINE
                warnings.append("canonical.spread_line_mismatch")

            total_line = _pick_value(home_row.total_line, away_row.total_line)
            home_spread_line = home_row.ats_line
            away_spread_line = away_row.ats_line
            home_score = home_row.team_score
            away_score = away_row.team_score
            row_indexes = sorted({home_row.source_row_index, away_row.source_row_index})
        else:
            single_row = matches[0]
            reconciliation_status = ReconciliationStatus.PARTIAL_SINGLE_ROW
            warnings.append("canonical.single_team_perspective_only")
            total_line = single_row.total_line
            if single_row.is_away:
                home_score = single_row.opponent_score
                away_score = single_row.team_score
                home_spread_line = -single_row.ats_line if single_row.ats_line is not None else None
                away_spread_line = single_row.ats_line
            else:
                home_score = single_row.team_score
                away_score = single_row.opponent_score
                home_spread_line = single_row.ats_line
                away_spread_line = -single_row.ats_line if single_row.ats_line is not None else None
            row_indexes = [single_row.source_row_index]

        canonical_games.append(
            CanonicalGame(
                season_label=season_label,
                game_date=home_row.game_date if home_rows else matches[0].game_date,
                home_team_code=home_team,
                away_team_code=away_team,
                home_score=home_score,
                away_score=away_score,
                total_line=total_line,
                home_spread_line=home_spread_line,
                away_spread_line=away_spread_line,
                reconciliation_status=reconciliation_status,
                source_row_indexes=row_indexes,
                warnings=warnings,
            )
        )

    return sorted(
        canonical_games,
        key=lambda game: (game.game_date.isoformat(), game.home_team_code, game.away_team_code),
    )


def _pick_value(primary: float | None, secondary: float | None) -> float | None:
    if primary is not None:
        return primary
    return secondary


def _normalize_team_code(team_code: str) -> str:
    return team_code.removeprefix("@").strip().upper()
