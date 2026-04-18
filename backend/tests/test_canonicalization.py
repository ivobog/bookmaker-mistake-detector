from datetime import date

from bookmaker_detector_api.ingestion.models import ParseStatus, RawGameRow, ReconciliationStatus
from bookmaker_detector_api.services.canonical import canonicalize_rows
from bookmaker_detector_api.services.metrics import calculate_game_metric


def test_canonicalization_matches_two_team_perspectives() -> None:
    rows = [
        RawGameRow(
            provider_name="covers",
            team_code="LAL",
            season_label="2024-2025",
            source_url="https://example.com/lal",
            source_section="Regular Season",
            source_row_index=1,
            game_date=date(2024, 11, 1),
            opponent_code="BOS",
            is_away=False,
            result_flag="W",
            team_score=112,
            opponent_score=104,
            ats_result="W",
            ats_line=-3.5,
            ou_result="O",
            total_line=214.5,
            parse_status=ParseStatus.VALID,
        ),
        RawGameRow(
            provider_name="covers",
            team_code="BOS",
            season_label="2024-2025",
            source_url="https://example.com/bos",
            source_section="Regular Season",
            source_row_index=18,
            game_date=date(2024, 11, 1),
            opponent_code="@LAL",
            is_away=True,
            result_flag="L",
            team_score=104,
            opponent_score=112,
            ats_result="L",
            ats_line=3.5,
            ou_result="O",
            total_line=214.5,
            parse_status=ParseStatus.VALID,
        ),
    ]

    canonical_games = canonicalize_rows(rows)

    assert len(canonical_games) == 1
    game = canonical_games[0]
    assert game.reconciliation_status == ReconciliationStatus.FULL_MATCH
    assert game.home_team_code == "LAL"
    assert game.away_team_code == "BOS"
    assert game.final_home_margin == 8
    assert game.final_total_points == 216

    metrics = calculate_game_metric(game)
    assert metrics.spread_error_home == 4.5
    assert metrics.spread_error_away == -4.5
    assert metrics.total_error == 1.5
    assert metrics.home_covered is True
    assert metrics.away_covered is False
    assert metrics.went_over is True
    assert metrics.went_under is False


def test_canonicalization_handles_single_team_perspective() -> None:
    rows = [
        RawGameRow(
            provider_name="covers",
            team_code="LAL",
            season_label="2024-2025",
            source_url="https://example.com/lal",
            source_section="Regular Season",
            source_row_index=1,
            game_date=date(2024, 11, 3),
            opponent_code="NYK",
            is_away=True,
            result_flag="L",
            team_score=101,
            opponent_score=107,
            ats_result="L",
            ats_line=2.5,
            ou_result="U",
            total_line=216.0,
            parse_status=ParseStatus.VALID,
        )
    ]

    canonical_games = canonicalize_rows(rows)

    assert len(canonical_games) == 1
    game = canonical_games[0]
    assert game.reconciliation_status == ReconciliationStatus.PARTIAL_SINGLE_ROW
    assert game.home_team_code == "NYK"
    assert game.away_team_code == "LAL"
    assert game.home_score == 107
    assert game.away_score == 101
    assert game.home_spread_line == -2.5
    assert game.away_spread_line == 2.5


def test_canonicalization_normalizes_team_alias_codes() -> None:
    rows = [
        RawGameRow(
            provider_name="covers",
            team_code="BOS",
            season_label="2024-2025",
            source_url="https://example.com/bos",
            source_section="Regular Season",
            source_row_index=1,
            game_date=date(2024, 10, 22),
            opponent_code="NY",
            is_away=False,
            result_flag="W",
            team_score=132,
            opponent_score=109,
            ats_result="W",
            ats_line=-6.0,
            ou_result="O",
            total_line=221.5,
            parse_status=ParseStatus.VALID,
        )
    ]

    canonical_games = canonicalize_rows(rows)

    assert len(canonical_games) == 1
    game = canonical_games[0]
    assert game.reconciliation_status == ReconciliationStatus.PARTIAL_SINGLE_ROW
    assert game.home_team_code == "BOS"
    assert game.away_team_code == "NYK"


def test_canonicalization_normalizes_historical_pho_alias_code() -> None:
    rows = [
        RawGameRow(
            provider_name="covers",
            team_code="MIL",
            season_label="2024-2025",
            source_url="https://example.com/mil",
            source_section="Regular Season",
            source_row_index=1,
            game_date=date(2025, 3, 24),
            opponent_code="PHO",
            is_away=False,
            result_flag="L",
            team_score=106,
            opponent_score=108,
            ats_result="L",
            ats_line=2.5,
            ou_result="U",
            total_line=223.5,
            parse_status=ParseStatus.VALID,
        )
    ]

    canonical_games = canonicalize_rows(rows)

    assert len(canonical_games) == 1
    game = canonical_games[0]
    assert game.reconciliation_status == ReconciliationStatus.PARTIAL_SINGLE_ROW
    assert game.home_team_code == "MIL"
    assert game.away_team_code == "PHX"


def test_canonicalization_marks_score_conflict() -> None:
    rows = [
        RawGameRow(
            provider_name="covers",
            team_code="LAL",
            season_label="2024-2025",
            source_url="https://example.com/lal",
            source_section="Regular Season",
            source_row_index=1,
            game_date=date(2024, 11, 1),
            opponent_code="BOS",
            is_away=False,
            result_flag="W",
            team_score=112,
            opponent_score=104,
            ats_result="W",
            ats_line=-3.5,
            ou_result="O",
            total_line=214.5,
            parse_status=ParseStatus.VALID,
        ),
        RawGameRow(
            provider_name="covers",
            team_code="BOS",
            season_label="2024-2025",
            source_url="https://example.com/bos",
            source_section="Regular Season",
            source_row_index=18,
            game_date=date(2024, 11, 1),
            opponent_code="@LAL",
            is_away=True,
            result_flag="L",
            team_score=103,
            opponent_score=112,
            ats_result="L",
            ats_line=3.5,
            ou_result="O",
            total_line=214.5,
            parse_status=ParseStatus.VALID,
        ),
    ]

    canonical_games = canonicalize_rows(rows)

    assert len(canonical_games) == 1
    game = canonical_games[0]
    assert game.reconciliation_status == ReconciliationStatus.CONFLICT_SCORE
    assert "canonical.score_mismatch" in game.warnings
