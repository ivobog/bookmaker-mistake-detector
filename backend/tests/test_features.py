from datetime import date

from bookmaker_detector_api.demo import seed_phase_two_feature_in_memory
from bookmaker_detector_api.services.features import (
    CanonicalGameMetricRecord,
    build_feature_comparable_cases,
    build_feature_dataset_rows,
    build_feature_evidence_bundle,
    build_feature_pattern_catalog,
    build_feature_snapshots,
    build_feature_training_benchmark,
    build_feature_training_bundle,
    build_feature_training_task_matrix,
    build_feature_training_view,
    get_feature_analysis_artifact_catalog_in_memory,
    get_feature_analysis_artifact_history_in_memory,
    materialize_feature_analysis_artifacts_in_memory,
    profile_feature_dataset_rows,
    profile_feature_training_rows,
    split_feature_dataset_rows,
    summarize_feature_snapshots,
)


def test_build_feature_snapshots_uses_only_prior_games() -> None:
    canonical_games = [
        CanonicalGameMetricRecord(
            canonical_game_id=1,
            season_label="2024-2025",
            game_date=date(2024, 11, 1),
            home_team_code="LAL",
            away_team_code="BOS",
            home_score=110,
            away_score=100,
            final_home_margin=10,
            final_total_points=210,
            total_line=205.5,
            home_spread_line=-4.5,
            away_spread_line=4.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[1],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=5.5,
            spread_error_away=-5.5,
            total_error=4.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=2,
            season_label="2024-2025",
            game_date=date(2024, 11, 3),
            home_team_code="NYK",
            away_team_code="LAL",
            home_score=101,
            away_score=105,
            final_home_margin=-4,
            final_total_points=206,
            total_line=208.5,
            home_spread_line=1.5,
            away_spread_line=-1.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[2],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=-2.5,
            spread_error_away=2.5,
            total_error=-2.5,
            home_covered=False,
            away_covered=True,
            went_over=False,
            went_under=True,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=3,
            season_label="2024-2025",
            game_date=date(2024, 11, 4),
            home_team_code="LAL",
            away_team_code="MIA",
            home_score=112,
            away_score=106,
            final_home_margin=6,
            final_total_points=218,
            total_line=214.5,
            home_spread_line=-3.5,
            away_spread_line=3.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[3],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=2.5,
            spread_error_away=-2.5,
            total_error=3.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
    ]

    snapshots = build_feature_snapshots(canonical_games, feature_version_id=1)

    assert len(snapshots) == 3
    first_snapshot = snapshots[0]
    assert first_snapshot.feature_payload["home_team"]["games_played_prior"] == 0
    assert first_snapshot.feature_payload["away_team"]["games_played_prior"] == 0

    third_snapshot = snapshots[2]
    home_payload = third_snapshot.feature_payload["home_team"]
    away_payload = third_snapshot.feature_payload["away_team"]

    assert home_payload["team_code"] == "LAL"
    assert home_payload["games_played_prior"] == 2
    assert home_payload["season_games_played_prior"] == 2
    assert home_payload["days_rest"] == 1
    assert home_payload["is_back_to_back"] is True
    assert home_payload["rolling_windows"]["3"]["sample_size"] == 2
    assert home_payload["rolling_windows"]["3"]["avg_point_margin"] == 7.0
    assert home_payload["rolling_windows"]["3"]["avg_total_points"] == 208.0
    assert home_payload["rolling_windows"]["3"]["avg_spread_error"] == 4.0
    assert home_payload["rolling_windows"]["3"]["avg_total_error"] == 1.0
    assert home_payload["rolling_windows"]["3"]["cover_rate"] == 1.0
    assert home_payload["rolling_windows"]["3"]["over_rate"] == 0.5

    assert away_payload["team_code"] == "MIA"
    assert away_payload["games_played_prior"] == 0
    assert away_payload["days_rest"] is None
    assert away_payload["rolling_windows"]["3"]["sample_size"] == 0
    assert away_payload["rolling_windows"]["3"]["avg_point_margin"] is None

    assert third_snapshot.feature_payload["matchup"]["prior_matchup_count"] == 0
    assert third_snapshot.feature_payload["matchup"]["season_prior_matchup_count"] == 0


def test_build_feature_snapshots_adds_split_volatility_and_trend_features() -> None:
    canonical_games = [
        CanonicalGameMetricRecord(
            canonical_game_id=1,
            season_label="2024-2025",
            game_date=date(2024, 11, 1),
            home_team_code="LAL",
            away_team_code="BOS",
            home_score=110,
            away_score=100,
            final_home_margin=10,
            final_total_points=210,
            total_line=205.5,
            home_spread_line=-4.5,
            away_spread_line=4.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[1],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=5.5,
            spread_error_away=-5.5,
            total_error=4.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=2,
            season_label="2024-2025",
            game_date=date(2024, 11, 3),
            home_team_code="NYK",
            away_team_code="LAL",
            home_score=101,
            away_score=105,
            final_home_margin=-4,
            final_total_points=206,
            total_line=208.5,
            home_spread_line=1.5,
            away_spread_line=-1.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[2],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=-2.5,
            spread_error_away=2.5,
            total_error=-2.5,
            home_covered=False,
            away_covered=True,
            went_over=False,
            went_under=True,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=3,
            season_label="2024-2025",
            game_date=date(2024, 11, 4),
            home_team_code="LAL",
            away_team_code="MIA",
            home_score=112,
            away_score=106,
            final_home_margin=6,
            final_total_points=218,
            total_line=214.5,
            home_spread_line=-3.5,
            away_spread_line=3.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[3],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=2.5,
            spread_error_away=-2.5,
            total_error=3.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
    ]

    snapshots = build_feature_snapshots(canonical_games, feature_version_id=1)

    home_payload = snapshots[2].feature_payload["home_team"]
    assert home_payload["home_games_played_prior"] == 1
    assert home_payload["away_games_played_prior"] == 1
    assert home_payload["rolling_home_windows"]["3"]["sample_size"] == 1
    assert home_payload["rolling_home_windows"]["3"]["avg_point_margin"] == 10.0
    assert home_payload["rolling_away_windows"]["3"]["sample_size"] == 1
    assert home_payload["rolling_away_windows"]["3"]["avg_point_margin"] == 4.0
    assert home_payload["volatility"] == {
        "point_margin_stddev": 3.0,
        "total_points_stddev": 2.0,
        "spread_error_stddev": 1.5,
        "total_error_stddev": 3.5,
    }
    assert home_payload["trend_signals"]["current_cover_streak"] == 2
    assert home_payload["trend_signals"]["current_non_cover_streak"] == 0
    assert home_payload["trend_signals"]["current_over_streak"] == 0
    assert home_payload["trend_signals"]["current_under_streak"] == 1
    assert home_payload["trend_signals"]["recent_point_margin_delta_3_vs_10"] == 0.0
    assert home_payload["trend_signals"]["recent_total_points_delta_3_vs_10"] == 0.0


def test_summarize_feature_snapshots_returns_team_level_rollup() -> None:
    canonical_games = [
        CanonicalGameMetricRecord(
            canonical_game_id=1,
            season_label="2024-2025",
            game_date=date(2024, 11, 1),
            home_team_code="LAL",
            away_team_code="BOS",
            home_score=110,
            away_score=100,
            final_home_margin=10,
            final_total_points=210,
            total_line=205.5,
            home_spread_line=-4.5,
            away_spread_line=4.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[1],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=5.5,
            spread_error_away=-5.5,
            total_error=4.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=2,
            season_label="2024-2025",
            game_date=date(2024, 11, 3),
            home_team_code="NYK",
            away_team_code="LAL",
            home_score=101,
            away_score=105,
            final_home_margin=-4,
            final_total_points=206,
            total_line=208.5,
            home_spread_line=1.5,
            away_spread_line=-1.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[2],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=-2.5,
            spread_error_away=2.5,
            total_error=-2.5,
            home_covered=False,
            away_covered=True,
            went_over=False,
            went_under=True,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=3,
            season_label="2024-2025",
            game_date=date(2024, 11, 4),
            home_team_code="LAL",
            away_team_code="MIA",
            home_score=112,
            away_score=106,
            final_home_margin=6,
            final_total_points=218,
            total_line=214.5,
            home_spread_line=-3.5,
            away_spread_line=3.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[3],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=2.5,
            spread_error_away=-2.5,
            total_error=3.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
    ]

    snapshots = build_feature_snapshots(canonical_games, feature_version_id=1)
    summary = summarize_feature_snapshots(snapshots, team_code="LAL")

    assert summary["perspective_count"] == 3
    assert summary["summary"]["team_count"] == 1
    assert summary["summary"]["home_perspective_count"] == 2
    assert summary["summary"]["away_perspective_count"] == 1
    assert summary["summary"]["avg_games_played_prior"] == 1.0
    assert summary["summary"]["avg_days_rest"] == 1.5
    assert summary["summary"]["back_to_back_rate"] == 0.3333
    assert summary["summary"]["avg_cover_streak"] == 1.0
    assert summary["summary"]["rolling_window_averages"]["3"]["avg_sample_size"] == 1.0
    assert summary["latest_perspective"]["team_code"] == "LAL"
    assert summary["latest_perspective"]["opponent_code"] == "MIA"
    assert summary["latest_perspective"]["venue"] == "home"


def test_build_feature_dataset_rows_returns_model_ready_team_rows() -> None:
    canonical_games = [
        CanonicalGameMetricRecord(
            canonical_game_id=1,
            season_label="2024-2025",
            game_date=date(2024, 11, 1),
            home_team_code="LAL",
            away_team_code="BOS",
            home_score=110,
            away_score=100,
            final_home_margin=10,
            final_total_points=210,
            total_line=205.5,
            home_spread_line=-4.5,
            away_spread_line=4.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[1],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=5.5,
            spread_error_away=-5.5,
            total_error=4.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=2,
            season_label="2024-2025",
            game_date=date(2024, 11, 3),
            home_team_code="NYK",
            away_team_code="LAL",
            home_score=101,
            away_score=105,
            final_home_margin=-4,
            final_total_points=206,
            total_line=208.5,
            home_spread_line=1.5,
            away_spread_line=-1.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[2],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=-2.5,
            spread_error_away=2.5,
            total_error=-2.5,
            home_covered=False,
            away_covered=True,
            went_over=False,
            went_under=True,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=3,
            season_label="2024-2025",
            game_date=date(2024, 11, 4),
            home_team_code="LAL",
            away_team_code="MIA",
            home_score=112,
            away_score=106,
            final_home_margin=6,
            final_total_points=218,
            total_line=214.5,
            home_spread_line=-3.5,
            away_spread_line=3.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[3],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=2.5,
            spread_error_away=-2.5,
            total_error=3.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
    ]

    snapshots = build_feature_snapshots(canonical_games, feature_version_id=1)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code="LAL",
    )

    assert len(dataset_rows) == 3
    first_row = dataset_rows[0]
    assert first_row["team_code"] == "LAL"
    assert first_row["venue"] == "home"
    assert first_row["games_played_prior"] == 0
    assert first_row["rolling_3_avg_point_margin"] is None
    assert first_row["point_margin_actual"] == 10.0
    assert first_row["spread_error_actual"] == 5.5
    assert first_row["covered_actual"] is True

    last_row = dataset_rows[-1]
    assert last_row["team_code"] == "LAL"
    assert last_row["opponent_code"] == "MIA"
    assert last_row["venue"] == "home"
    assert last_row["games_played_prior"] == 2
    assert last_row["rolling_3_avg_point_margin"] == 7.0
    assert last_row["current_cover_streak"] == 2
    assert last_row["point_margin_actual"] == 6.0
    assert last_row["went_over_actual"] is True


def test_profile_feature_dataset_rows_returns_coverage_and_label_balance() -> None:
    canonical_games = [
        CanonicalGameMetricRecord(
            canonical_game_id=1,
            season_label="2024-2025",
            game_date=date(2024, 11, 1),
            home_team_code="LAL",
            away_team_code="BOS",
            home_score=110,
            away_score=100,
            final_home_margin=10,
            final_total_points=210,
            total_line=205.5,
            home_spread_line=-4.5,
            away_spread_line=4.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[1],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=5.5,
            spread_error_away=-5.5,
            total_error=4.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=2,
            season_label="2024-2025",
            game_date=date(2024, 11, 3),
            home_team_code="NYK",
            away_team_code="LAL",
            home_score=101,
            away_score=105,
            final_home_margin=-4,
            final_total_points=206,
            total_line=208.5,
            home_spread_line=1.5,
            away_spread_line=-1.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[2],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=-2.5,
            spread_error_away=2.5,
            total_error=-2.5,
            home_covered=False,
            away_covered=True,
            went_over=False,
            went_under=True,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=3,
            season_label="2024-2025",
            game_date=date(2024, 11, 4),
            home_team_code="LAL",
            away_team_code="MIA",
            home_score=112,
            away_score=106,
            final_home_margin=6,
            final_total_points=218,
            total_line=214.5,
            home_spread_line=-3.5,
            away_spread_line=3.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[3],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=2.5,
            spread_error_away=-2.5,
            total_error=3.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
    ]

    snapshots = build_feature_snapshots(canonical_games, feature_version_id=1)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code="LAL",
    )
    profile = profile_feature_dataset_rows(dataset_rows)

    assert profile["date_range"] == {
        "min_game_date": date(2024, 11, 1),
        "max_game_date": date(2024, 11, 4),
    }
    assert profile["season_count"] == 1
    assert profile["team_count"] == 1
    assert profile["opponent_count"] == 3
    assert profile["venue_counts"] == {"home": 2, "away": 1}
    assert profile["label_balance"]["covered_actual"] == {"true": 3, "false": 0, "null": 0}
    assert profile["label_balance"]["went_over_actual"] == {"true": 2, "false": 1, "null": 0}
    assert profile["feature_coverage"]["games_played_prior"]["coverage_rate"] == 1.0
    assert profile["feature_coverage"]["days_rest"]["coverage_rate"] == 0.6667
    assert profile["feature_coverage"]["rolling_3_avg_point_margin"]["non_null_count"] == 2


def test_build_feature_pattern_catalog_groups_bucketed_conditions() -> None:
    canonical_games = [
        CanonicalGameMetricRecord(
            canonical_game_id=1,
            season_label="2024-2025",
            game_date=date(2024, 11, 1),
            home_team_code="LAL",
            away_team_code="BOS",
            home_score=110,
            away_score=100,
            final_home_margin=10,
            final_total_points=210,
            total_line=205.5,
            home_spread_line=-4.5,
            away_spread_line=4.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[1],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=5.5,
            spread_error_away=-5.5,
            total_error=4.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=2,
            season_label="2024-2025",
            game_date=date(2024, 11, 3),
            home_team_code="NYK",
            away_team_code="LAL",
            home_score=101,
            away_score=105,
            final_home_margin=-4,
            final_total_points=206,
            total_line=208.5,
            home_spread_line=1.5,
            away_spread_line=-1.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[2],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=-2.5,
            spread_error_away=2.5,
            total_error=-2.5,
            home_covered=False,
            away_covered=True,
            went_over=False,
            went_under=True,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=3,
            season_label="2024-2025",
            game_date=date(2024, 11, 4),
            home_team_code="LAL",
            away_team_code="MIA",
            home_score=112,
            away_score=106,
            final_home_margin=6,
            final_total_points=218,
            total_line=214.5,
            home_spread_line=-3.5,
            away_spread_line=3.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[3],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=2.5,
            spread_error_away=-2.5,
            total_error=3.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
    ]

    snapshots = build_feature_snapshots(canonical_games, feature_version_id=1)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code="LAL",
    )
    pattern_catalog = build_feature_pattern_catalog(
        dataset_rows,
        target_task="spread_error_regression",
        dimensions=("venue", "days_rest_bucket"),
        min_sample_size=1,
        limit=10,
    )

    assert pattern_catalog["task"]["target_column"] == "spread_error_actual"
    assert pattern_catalog["dimensions"] == ["venue", "days_rest_bucket"]
    assert pattern_catalog["pattern_count"] >= 2
    first_pattern = pattern_catalog["patterns"][0]
    assert "pattern_key" in first_pattern
    assert "comparable_lookup" in first_pattern
    assert "conditions" in first_pattern
    assert "sample_size" in first_pattern
    assert "target_mean" in first_pattern
    assert set(first_pattern["conditions"]) == {"venue", "days_rest_bucket"}


def test_build_feature_comparable_cases_uses_anchor_row_conditions() -> None:
    canonical_games = [
        CanonicalGameMetricRecord(
            canonical_game_id=1,
            season_label="2024-2025",
            game_date=date(2024, 11, 1),
            home_team_code="LAL",
            away_team_code="BOS",
            home_score=110,
            away_score=100,
            final_home_margin=10,
            final_total_points=210,
            total_line=205.5,
            home_spread_line=-4.5,
            away_spread_line=4.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[1],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=5.5,
            spread_error_away=-5.5,
            total_error=4.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=2,
            season_label="2024-2025",
            game_date=date(2024, 11, 3),
            home_team_code="NYK",
            away_team_code="LAL",
            home_score=101,
            away_score=105,
            final_home_margin=-4,
            final_total_points=206,
            total_line=208.5,
            home_spread_line=1.5,
            away_spread_line=-1.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[2],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=-2.5,
            spread_error_away=2.5,
            total_error=-2.5,
            home_covered=False,
            away_covered=True,
            went_over=False,
            went_under=True,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=3,
            season_label="2024-2025",
            game_date=date(2024, 11, 5),
            home_team_code="LAL",
            away_team_code="MIA",
            home_score=112,
            away_score=106,
            final_home_margin=6,
            final_total_points=218,
            total_line=214.5,
            home_spread_line=-3.5,
            away_spread_line=3.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[3],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=2.5,
            spread_error_away=-2.5,
            total_error=3.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
    ]

    snapshots = build_feature_snapshots(canonical_games, feature_version_id=1)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code="LAL",
    )
    comparables = build_feature_comparable_cases(
        dataset_rows,
        target_task="spread_error_regression",
        dimensions=("venue", "days_rest_bucket"),
        canonical_game_id=3,
        team_code="LAL",
        limit=10,
    )

    assert comparables["task"]["target_column"] == "spread_error_actual"
    assert comparables["anchor_case"]["canonical_game_id"] == 3
    assert comparables["anchor_case"]["matched_conditions"] == {
        "venue": "home",
        "days_rest_bucket": "2_days",
    }
    assert comparables["pattern_key"] == "venue=home|days_rest_bucket=2_days"
    assert comparables["condition_values"] == ["home", "2_days"]
    assert comparables["comparable_count"] == 0
    assert comparables["comparables"] == []


def test_build_feature_comparable_cases_accepts_pattern_key() -> None:
    canonical_games = [
        CanonicalGameMetricRecord(
            canonical_game_id=1,
            season_label="2024-2025",
            game_date=date(2024, 11, 1),
            home_team_code="LAL",
            away_team_code="BOS",
            home_score=110,
            away_score=100,
            final_home_margin=10,
            final_total_points=210,
            total_line=205.5,
            home_spread_line=-4.5,
            away_spread_line=4.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[1],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=5.5,
            spread_error_away=-5.5,
            total_error=4.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=2,
            season_label="2024-2025",
            game_date=date(2024, 11, 3),
            home_team_code="NYK",
            away_team_code="LAL",
            home_score=101,
            away_score=105,
            final_home_margin=-4,
            final_total_points=206,
            total_line=208.5,
            home_spread_line=1.5,
            away_spread_line=-1.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[2],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=-2.5,
            spread_error_away=2.5,
            total_error=-2.5,
            home_covered=False,
            away_covered=True,
            went_over=False,
            went_under=True,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=3,
            season_label="2024-2025",
            game_date=date(2024, 11, 5),
            home_team_code="LAL",
            away_team_code="MIA",
            home_score=112,
            away_score=106,
            final_home_margin=6,
            final_total_points=218,
            total_line=214.5,
            home_spread_line=-3.5,
            away_spread_line=3.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[3],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=2.5,
            spread_error_away=-2.5,
            total_error=3.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
    ]

    snapshots = build_feature_snapshots(canonical_games, feature_version_id=1)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
    )
    comparables = build_feature_comparable_cases(
        dataset_rows,
        target_task="spread_error_regression",
        pattern_key="venue=home|days_rest_bucket=unknown_rest",
        limit=10,
    )

    assert comparables["pattern_key"] == "venue=home|days_rest_bucket=unknown_rest"
    assert comparables["comparable_count"] == 2


def test_build_feature_comparable_cases_ranks_anchor_matches_by_similarity() -> None:
    canonical_games = [
        CanonicalGameMetricRecord(
            canonical_game_id=1,
            season_label="2024-2025",
            game_date=date(2024, 11, 1),
            home_team_code="LAL",
            away_team_code="BOS",
            home_score=110,
            away_score=100,
            final_home_margin=10,
            final_total_points=210,
            total_line=205.5,
            home_spread_line=-4.5,
            away_spread_line=4.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[1],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=5.5,
            spread_error_away=-5.5,
            total_error=4.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=2,
            season_label="2024-2025",
            game_date=date(2024, 11, 3),
            home_team_code="LAL",
            away_team_code="CHI",
            home_score=108,
            away_score=102,
            final_home_margin=6,
            final_total_points=210,
            total_line=209.5,
            home_spread_line=-2.5,
            away_spread_line=2.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[2],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=3.5,
            spread_error_away=-3.5,
            total_error=0.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=3,
            season_label="2024-2025",
            game_date=date(2024, 11, 5),
            home_team_code="LAL",
            away_team_code="NYK",
            home_score=112,
            away_score=106,
            final_home_margin=6,
            final_total_points=218,
            total_line=214.5,
            home_spread_line=-3.5,
            away_spread_line=3.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[3],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=2.5,
            spread_error_away=-2.5,
            total_error=3.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
    ]

    snapshots = build_feature_snapshots(canonical_games, feature_version_id=1)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code="LAL",
    )
    comparables = build_feature_comparable_cases(
        dataset_rows,
        target_task="spread_error_regression",
        dimensions=("venue",),
        canonical_game_id=3,
        team_code="LAL",
        limit=10,
    )

    assert comparables["comparable_summary"]["ranking_mode"] == "anchor_similarity"
    assert comparables["comparable_count"] == 2
    assert comparables["comparables"][0]["canonical_game_id"] == 2
    assert comparables["comparables"][1]["canonical_game_id"] == 1
    assert comparables["comparables"][0]["similarity_score"] is not None
    assert (
        comparables["comparables"][0]["similarity_score"]
        >= comparables["comparables"][1]["similarity_score"]
    )


def test_build_feature_evidence_bundle_combines_pattern_comparables_and_benchmarks() -> None:
    canonical_games = [
        CanonicalGameMetricRecord(
            canonical_game_id=1,
            season_label="2024-2025",
            game_date=date(2024, 11, 1),
            home_team_code="LAL",
            away_team_code="BOS",
            home_score=110,
            away_score=100,
            final_home_margin=10,
            final_total_points=210,
            total_line=205.5,
            home_spread_line=-4.5,
            away_spread_line=4.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[1],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=5.5,
            spread_error_away=-5.5,
            total_error=4.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=2,
            season_label="2024-2025",
            game_date=date(2024, 11, 3),
            home_team_code="NYK",
            away_team_code="LAL",
            home_score=101,
            away_score=105,
            final_home_margin=-4,
            final_total_points=206,
            total_line=208.5,
            home_spread_line=1.5,
            away_spread_line=-1.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[2],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=-2.5,
            spread_error_away=2.5,
            total_error=-2.5,
            home_covered=False,
            away_covered=True,
            went_over=False,
            went_under=True,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=3,
            season_label="2024-2025",
            game_date=date(2024, 11, 5),
            home_team_code="LAL",
            away_team_code="MIA",
            home_score=112,
            away_score=106,
            final_home_margin=6,
            final_total_points=218,
            total_line=214.5,
            home_spread_line=-3.5,
            away_spread_line=3.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[3],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=2.5,
            spread_error_away=-2.5,
            total_error=3.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
    ]

    snapshots = build_feature_snapshots(canonical_games, feature_version_id=1)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code="LAL",
    )
    evidence_bundle = build_feature_evidence_bundle(
        dataset_rows,
        target_task="spread_error_regression",
        dimensions=("venue", "days_rest_bucket"),
        canonical_game_id=3,
        team_code="LAL",
        comparable_limit=5,
        min_pattern_sample_size=1,
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    assert evidence_bundle["task"]["target_column"] == "spread_error_actual"
    assert evidence_bundle["evidence"]["pattern"]["selected_pattern"] is not None
    assert evidence_bundle["evidence"]["comparables"]["anchor_case"]["canonical_game_id"] == 3
    assert "benchmark_rankings" in evidence_bundle["evidence"]["benchmark_context"]
    assert evidence_bundle["evidence"]["summary"]["pattern_key"] is not None
    assert evidence_bundle["evidence"]["strength"]["rating"] in {
        "weak",
        "moderate",
        "strong",
    }
    assert 0.0 <= evidence_bundle["evidence"]["strength"]["overall_score"] <= 1.0
    assert "pattern_support" in evidence_bundle["evidence"]["strength"]["components"]
    assert "comparable_support" in evidence_bundle["evidence"]["strength"]["components"]
    assert "benchmark_support" in evidence_bundle["evidence"]["strength"]["components"]
    assert evidence_bundle["evidence"]["recommendation"]["status"] in {
        "monitor_only",
        "review_manually",
        "candidate_signal",
    }
    assert evidence_bundle["evidence"]["recommendation"]["recommended_action"] in {
        "monitor_only",
        "review_manually",
        "promote_to_model_review",
    }
    assert (
        evidence_bundle["evidence"]["recommendation"]["policy_profile"]["target_task"]
        == "spread_error_regression"
    )
    assert evidence_bundle["evidence"]["recommendation"]["next_steps"]


def test_build_feature_evidence_bundle_uses_task_specific_recommendation_policy() -> None:
    canonical_games = [
        CanonicalGameMetricRecord(
            canonical_game_id=1,
            season_label="2024-2025",
            game_date=date(2024, 11, 1),
            home_team_code="LAL",
            away_team_code="BOS",
            home_score=110,
            away_score=100,
            final_home_margin=10,
            final_total_points=210,
            total_line=205.5,
            home_spread_line=-4.5,
            away_spread_line=4.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[1],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=5.5,
            spread_error_away=-5.5,
            total_error=4.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=2,
            season_label="2024-2025",
            game_date=date(2024, 11, 3),
            home_team_code="NYK",
            away_team_code="LAL",
            home_score=108,
            away_score=101,
            final_home_margin=7,
            final_total_points=209,
            total_line=211.5,
            home_spread_line=-1.5,
            away_spread_line=1.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[2],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=5.5,
            spread_error_away=-5.5,
            total_error=-2.5,
            home_covered=False,
            away_covered=True,
            went_over=False,
            went_under=True,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=3,
            season_label="2024-2025",
            game_date=date(2024, 11, 4),
            home_team_code="LAL",
            away_team_code="MIA",
            home_score=112,
            away_score=106,
            final_home_margin=6,
            final_total_points=218,
            total_line=214.5,
            home_spread_line=-3.5,
            away_spread_line=3.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[3],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=2.5,
            spread_error_away=-2.5,
            total_error=3.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
    ]

    snapshots = build_feature_snapshots(canonical_games, feature_version_id=1)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code="LAL",
    )
    evidence_bundle = build_feature_evidence_bundle(
        dataset_rows,
        target_task="cover_classification",
        dimensions=("venue", "days_rest_bucket"),
        canonical_game_id=3,
        team_code="LAL",
        comparable_limit=5,
        min_pattern_sample_size=1,
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    recommendation = evidence_bundle["evidence"]["recommendation"]
    assert recommendation["task_type"] == "classification"
    assert recommendation["policy_profile"]["target_task"] == "cover_classification"
    assert recommendation["policy_profile"]["policy_name"] == "classification_cover_policy_v1"
    assert recommendation["policy_profile"]["thresholds"]["review_min_pattern_sample"] == 2


def test_materialize_feature_analysis_artifacts_persists_patterns_and_evidence() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    materialized = materialize_feature_analysis_artifacts_in_memory(
        repository,
        target_task="spread_error_regression",
        team_code="LAL",
        season_label="2024-2025",
        dimensions=("venue", "days_rest_bucket"),
        min_sample_size=1,
        canonical_game_id=3,
        comparable_limit=5,
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    assert materialized["materialized_count"] >= 2
    assert materialized["artifact_counts"]["pattern_summary"] >= 1
    assert materialized["artifact_counts"]["evidence_bundle"] == 1
    catalog = get_feature_analysis_artifact_catalog_in_memory(
        repository,
        target_task="spread_error_regression",
        team_code="LAL",
        season_label="2024-2025",
        limit=50,
    )
    assert catalog["artifact_count"] >= 2
    assert any(artifact["artifact_type"] == "pattern_summary" for artifact in catalog["artifacts"])
    assert any(artifact["artifact_type"] == "evidence_bundle" for artifact in catalog["artifacts"])


def test_feature_analysis_artifact_history_summarizes_evidence_statuses() -> None:
    repository, _, _ = seed_phase_two_feature_in_memory()
    materialize_feature_analysis_artifacts_in_memory(
        repository,
        target_task="spread_error_regression",
        team_code="LAL",
        season_label="2024-2025",
        dimensions=("venue", "days_rest_bucket"),
        min_sample_size=1,
        canonical_game_id=3,
        comparable_limit=5,
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    history = get_feature_analysis_artifact_history_in_memory(
        repository,
        target_task="spread_error_regression",
        team_code="LAL",
        season_label="2024-2025",
        latest_limit=10,
    )

    assert history["overview"]["artifact_count"] >= 2
    assert history["overview"]["artifact_type_counts"]["evidence_bundle"] == 1
    assert history["overview"]["evidence_status_counts"]["monitor_only"] == 1
    assert history["daily_buckets"]
    assert history["latest_evidence_artifacts"][0]["status"] == "monitor_only"


def test_split_feature_dataset_rows_keeps_games_together_chronologically() -> None:
    canonical_games = [
        CanonicalGameMetricRecord(
            canonical_game_id=1,
            season_label="2024-2025",
            game_date=date(2024, 11, 1),
            home_team_code="LAL",
            away_team_code="BOS",
            home_score=110,
            away_score=100,
            final_home_margin=10,
            final_total_points=210,
            total_line=205.5,
            home_spread_line=-4.5,
            away_spread_line=4.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[1],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=5.5,
            spread_error_away=-5.5,
            total_error=4.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=2,
            season_label="2024-2025",
            game_date=date(2024, 11, 3),
            home_team_code="NYK",
            away_team_code="LAL",
            home_score=101,
            away_score=105,
            final_home_margin=-4,
            final_total_points=206,
            total_line=208.5,
            home_spread_line=1.5,
            away_spread_line=-1.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[2],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=-2.5,
            spread_error_away=2.5,
            total_error=-2.5,
            home_covered=False,
            away_covered=True,
            went_over=False,
            went_under=True,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=3,
            season_label="2024-2025",
            game_date=date(2024, 11, 4),
            home_team_code="LAL",
            away_team_code="MIA",
            home_score=112,
            away_score=106,
            final_home_margin=6,
            final_total_points=218,
            total_line=214.5,
            home_spread_line=-3.5,
            away_spread_line=3.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[3],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=2.5,
            spread_error_away=-2.5,
            total_error=3.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
    ]

    snapshots = build_feature_snapshots(canonical_games, feature_version_id=1)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
    )
    split_result = split_feature_dataset_rows(
        dataset_rows,
        train_ratio=0.5,
        validation_ratio=0.25,
        preview_limit=2,
    )

    assert split_result["split_summary"]["train"]["game_count"] == 1
    assert split_result["split_summary"]["validation"]["game_count"] == 1
    assert split_result["split_summary"]["test"]["game_count"] == 1
    assert split_result["split_summary"]["train"]["row_count"] == 2
    assert split_result["split_summary"]["validation"]["row_count"] == 2
    assert split_result["split_summary"]["test"]["row_count"] == 2
    assert (
        split_result["split_previews"]["train"][0]["canonical_game_id"]
        != split_result["split_previews"]["validation"][0]["canonical_game_id"]
    )


def test_build_feature_training_view_projects_target_without_leakage_columns() -> None:
    canonical_games = [
        CanonicalGameMetricRecord(
            canonical_game_id=1,
            season_label="2024-2025",
            game_date=date(2024, 11, 1),
            home_team_code="LAL",
            away_team_code="BOS",
            home_score=110,
            away_score=100,
            final_home_margin=10,
            final_total_points=210,
            total_line=205.5,
            home_spread_line=-4.5,
            away_spread_line=4.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[1],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=5.5,
            spread_error_away=-5.5,
            total_error=4.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=2,
            season_label="2024-2025",
            game_date=date(2024, 11, 3),
            home_team_code="NYK",
            away_team_code="LAL",
            home_score=101,
            away_score=105,
            final_home_margin=-4,
            final_total_points=206,
            total_line=208.5,
            home_spread_line=1.5,
            away_spread_line=-1.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[2],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=-2.5,
            spread_error_away=2.5,
            total_error=-2.5,
            home_covered=False,
            away_covered=True,
            went_over=False,
            went_under=True,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=3,
            season_label="2024-2025",
            game_date=date(2024, 11, 4),
            home_team_code="LAL",
            away_team_code="MIA",
            home_score=112,
            away_score=106,
            final_home_margin=6,
            final_total_points=218,
            total_line=214.5,
            home_spread_line=-3.5,
            away_spread_line=3.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[3],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=2.5,
            spread_error_away=-2.5,
            total_error=3.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
    ]

    snapshots = build_feature_snapshots(canonical_games, feature_version_id=1)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code="LAL",
    )
    training_view = build_feature_training_view(
        dataset_rows,
        target_task="spread_error_regression",
        drop_null_targets=True,
    )

    assert training_view["row_count"] == 3
    assert training_view["task"]["target_column"] == "spread_error_actual"
    first_row = training_view["training_rows"][0]
    assert first_row["team_code"] == "LAL"
    assert first_row["target_value"] == 5.5
    assert "games_played_prior" in first_row["features"]
    assert "spread_error_actual" not in first_row["features"]
    assert "covered_actual" not in first_row["features"]
    assert "went_over_actual" not in first_row["features"]


def test_build_feature_training_bundle_returns_split_target_summaries() -> None:
    canonical_games = [
        CanonicalGameMetricRecord(
            canonical_game_id=1,
            season_label="2024-2025",
            game_date=date(2024, 11, 1),
            home_team_code="LAL",
            away_team_code="BOS",
            home_score=110,
            away_score=100,
            final_home_margin=10,
            final_total_points=210,
            total_line=205.5,
            home_spread_line=-4.5,
            away_spread_line=4.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[1],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=5.5,
            spread_error_away=-5.5,
            total_error=4.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=2,
            season_label="2024-2025",
            game_date=date(2024, 11, 3),
            home_team_code="NYK",
            away_team_code="LAL",
            home_score=101,
            away_score=105,
            final_home_margin=-4,
            final_total_points=206,
            total_line=208.5,
            home_spread_line=1.5,
            away_spread_line=-1.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[2],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=-2.5,
            spread_error_away=2.5,
            total_error=-2.5,
            home_covered=False,
            away_covered=True,
            went_over=False,
            went_under=True,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=3,
            season_label="2024-2025",
            game_date=date(2024, 11, 4),
            home_team_code="LAL",
            away_team_code="MIA",
            home_score=112,
            away_score=106,
            final_home_margin=6,
            final_total_points=218,
            total_line=214.5,
            home_spread_line=-3.5,
            away_spread_line=3.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[3],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=2.5,
            spread_error_away=-2.5,
            total_error=3.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
    ]

    snapshots = build_feature_snapshots(canonical_games, feature_version_id=1)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code="LAL",
    )
    training_bundle = build_feature_training_bundle(
        dataset_rows,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
        preview_limit=1,
    )

    assert training_bundle["task"]["target_column"] == "spread_error_actual"
    assert training_bundle["bundle_summary"]["train"]["game_count"] == 1
    assert training_bundle["bundle_summary"]["validation"]["game_count"] == 1
    assert training_bundle["bundle_summary"]["test"]["game_count"] == 1
    assert training_bundle["bundle_summary"]["train"]["training_row_count"] == 1
    assert training_bundle["bundle_summary"]["train"]["target_summary"]["row_count"] == 1
    assert (
        training_bundle["bundle_summary"]["train"]["training_manifest"]["feature_column_count"] > 0
    )
    assert len(training_bundle["split_previews"]["train"]) == 1


def test_build_feature_training_benchmark_scores_naive_regression_baselines() -> None:
    canonical_games = [
        CanonicalGameMetricRecord(
            canonical_game_id=1,
            season_label="2024-2025",
            game_date=date(2024, 11, 1),
            home_team_code="LAL",
            away_team_code="BOS",
            home_score=110,
            away_score=100,
            final_home_margin=10,
            final_total_points=210,
            total_line=205.5,
            home_spread_line=-4.5,
            away_spread_line=4.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[1],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=5.5,
            spread_error_away=-5.5,
            total_error=4.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=2,
            season_label="2024-2025",
            game_date=date(2024, 11, 3),
            home_team_code="NYK",
            away_team_code="LAL",
            home_score=101,
            away_score=105,
            final_home_margin=-4,
            final_total_points=206,
            total_line=208.5,
            home_spread_line=1.5,
            away_spread_line=-1.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[2],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=-2.5,
            spread_error_away=2.5,
            total_error=-2.5,
            home_covered=False,
            away_covered=True,
            went_over=False,
            went_under=True,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=3,
            season_label="2024-2025",
            game_date=date(2024, 11, 4),
            home_team_code="LAL",
            away_team_code="MIA",
            home_score=112,
            away_score=106,
            final_home_margin=6,
            final_total_points=218,
            total_line=214.5,
            home_spread_line=-3.5,
            away_spread_line=3.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[3],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=2.5,
            spread_error_away=-2.5,
            total_error=3.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
    ]

    snapshots = build_feature_snapshots(canonical_games, feature_version_id=1)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code="LAL",
    )
    benchmark = build_feature_training_benchmark(
        dataset_rows,
        target_task="spread_error_regression",
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    assert benchmark["task"]["target_column"] == "spread_error_actual"
    assert benchmark["benchmark_summary"]["train"]["row_count"] == 1
    assert (
        benchmark["benchmark_summary"]["validation"]["benchmarks"]["train_mean_baseline"][
            "prediction_count"
        ]
        == 1
    )
    assert (
        benchmark["benchmark_summary"]["test"]["benchmarks"]["rolling_3_feature_baseline"][
            "prediction_count"
        ]
        == 1
    )
    assert benchmark["benchmark_rankings"][0]["primary_metric"] == "mae"


def test_build_feature_training_benchmark_scores_classification_rate_baselines() -> None:
    canonical_games = [
        CanonicalGameMetricRecord(
            canonical_game_id=1,
            season_label="2024-2025",
            game_date=date(2024, 11, 1),
            home_team_code="LAL",
            away_team_code="BOS",
            home_score=110,
            away_score=100,
            final_home_margin=10,
            final_total_points=210,
            total_line=205.5,
            home_spread_line=-4.5,
            away_spread_line=4.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[1],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=5.5,
            spread_error_away=-5.5,
            total_error=4.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=2,
            season_label="2024-2025",
            game_date=date(2024, 11, 3),
            home_team_code="NYK",
            away_team_code="LAL",
            home_score=101,
            away_score=105,
            final_home_margin=-4,
            final_total_points=206,
            total_line=208.5,
            home_spread_line=1.5,
            away_spread_line=-1.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[2],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=-2.5,
            spread_error_away=2.5,
            total_error=-2.5,
            home_covered=False,
            away_covered=True,
            went_over=False,
            went_under=True,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=3,
            season_label="2024-2025",
            game_date=date(2024, 11, 4),
            home_team_code="LAL",
            away_team_code="MIA",
            home_score=112,
            away_score=106,
            final_home_margin=6,
            final_total_points=218,
            total_line=214.5,
            home_spread_line=-3.5,
            away_spread_line=3.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[3],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=2.5,
            spread_error_away=-2.5,
            total_error=3.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
    ]

    snapshots = build_feature_snapshots(canonical_games, feature_version_id=1)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code="LAL",
    )
    benchmark = build_feature_training_benchmark(
        dataset_rows,
        target_task="cover_classification",
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    assert benchmark["task"]["task_type"] == "classification"
    assert benchmark["benchmark_rankings"][0]["primary_metric"] == "brier_score"
    assert (
        benchmark["benchmark_summary"]["validation"]["benchmarks"]["train_rate_baseline"][
            "accuracy"
        ]
        == 1.0
    )
    assert (
        benchmark["benchmark_summary"]["test"]["benchmarks"]["rolling_3_rate_baseline"][
            "prediction_count"
        ]
        == 1
    )


def test_profile_feature_training_rows_returns_feature_manifest() -> None:
    canonical_games = [
        CanonicalGameMetricRecord(
            canonical_game_id=1,
            season_label="2024-2025",
            game_date=date(2024, 11, 1),
            home_team_code="LAL",
            away_team_code="BOS",
            home_score=110,
            away_score=100,
            final_home_margin=10,
            final_total_points=210,
            total_line=205.5,
            home_spread_line=-4.5,
            away_spread_line=4.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[1],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=5.5,
            spread_error_away=-5.5,
            total_error=4.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=2,
            season_label="2024-2025",
            game_date=date(2024, 11, 3),
            home_team_code="NYK",
            away_team_code="LAL",
            home_score=101,
            away_score=105,
            final_home_margin=-4,
            final_total_points=206,
            total_line=208.5,
            home_spread_line=1.5,
            away_spread_line=-1.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[2],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=-2.5,
            spread_error_away=2.5,
            total_error=-2.5,
            home_covered=False,
            away_covered=True,
            went_over=False,
            went_under=True,
        ),
    ]

    snapshots = build_feature_snapshots(canonical_games, feature_version_id=1)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code="LAL",
    )
    training_view = build_feature_training_view(
        dataset_rows,
        target_task="spread_error_regression",
    )
    manifest = profile_feature_training_rows(training_view["training_rows"])

    assert manifest["row_count"] == 2
    assert manifest["feature_column_count"] > 0
    assert "games_played_prior" in manifest["feature_columns"]
    assert "is_back_to_back" in manifest["boolean_feature_columns"]
    assert "rolling_3_avg_point_margin" in manifest["numeric_feature_columns"]
    assert "games_played_prior" in manifest["feature_coverage"]


def test_build_feature_training_task_matrix_compares_supported_targets() -> None:
    canonical_games = [
        CanonicalGameMetricRecord(
            canonical_game_id=1,
            season_label="2024-2025",
            game_date=date(2024, 11, 1),
            home_team_code="LAL",
            away_team_code="BOS",
            home_score=110,
            away_score=100,
            final_home_margin=10,
            final_total_points=210,
            total_line=205.5,
            home_spread_line=-4.5,
            away_spread_line=4.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[1],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=5.5,
            spread_error_away=-5.5,
            total_error=4.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=2,
            season_label="2024-2025",
            game_date=date(2024, 11, 3),
            home_team_code="NYK",
            away_team_code="LAL",
            home_score=101,
            away_score=105,
            final_home_margin=-4,
            final_total_points=206,
            total_line=208.5,
            home_spread_line=1.5,
            away_spread_line=-1.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[2],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=-2.5,
            spread_error_away=2.5,
            total_error=-2.5,
            home_covered=False,
            away_covered=True,
            went_over=False,
            went_under=True,
        ),
        CanonicalGameMetricRecord(
            canonical_game_id=3,
            season_label="2024-2025",
            game_date=date(2024, 11, 4),
            home_team_code="LAL",
            away_team_code="MIA",
            home_score=112,
            away_score=106,
            final_home_margin=6,
            final_total_points=218,
            total_line=214.5,
            home_spread_line=-3.5,
            away_spread_line=3.5,
            reconciliation_status="PARTIAL_SINGLE_ROW",
            source_row_indexes=[3],
            warnings=["canonical.single_team_perspective_only"],
            spread_error_home=2.5,
            spread_error_away=-2.5,
            total_error=3.5,
            home_covered=True,
            away_covered=False,
            went_over=True,
            went_under=False,
        ),
    ]

    snapshots = build_feature_snapshots(canonical_games, feature_version_id=1)
    dataset_rows = build_feature_dataset_rows(
        snapshots=snapshots,
        canonical_games=canonical_games,
        team_code="LAL",
    )
    task_matrix = build_feature_training_task_matrix(
        dataset_rows,
        train_ratio=0.5,
        validation_ratio=0.25,
    )

    assert "spread_error_regression" in task_matrix
    assert "cover_classification" in task_matrix
    assert task_matrix["spread_error_regression"]["task"]["target_column"] == "spread_error_actual"
    assert task_matrix["spread_error_regression"]["training_row_count"] == 3
    assert task_matrix["cover_classification"]["bundle_summary"]["train"]["game_count"] == 1
