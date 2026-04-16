from datetime import date

from bookmaker_detector_api.services.features import (
    CanonicalGameMetricRecord,
    build_feature_dataset_rows,
    build_feature_snapshots,
    build_feature_training_bundle,
    build_feature_training_task_matrix,
    build_feature_training_view,
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
    assert (
        profile["feature_coverage"]["rolling_3_avg_point_margin"]["non_null_count"] == 2
    )


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
        training_bundle["bundle_summary"]["train"]["training_manifest"]["feature_column_count"]
        > 0
    )
    assert len(training_bundle["split_previews"]["train"]) == 1


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
