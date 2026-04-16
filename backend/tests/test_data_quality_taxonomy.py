from bookmaker_detector_api.data_quality_taxonomy import (
    canonical_issue_type,
    merge_issue_type_counts,
    normalize_issue_type_and_severity,
    severity_counts_from_issue_type_counts,
)
from bookmaker_detector_api.repositories import InMemoryIngestionRepository


def test_data_quality_taxonomy_helpers_normalize_legacy_issue_types() -> None:
    assert canonical_issue_type("single_team_perspective_only") == (
        "canonical.single_team_perspective_only"
    )
    assert normalize_issue_type_and_severity("score_mismatch", "warning") == (
        "canonical.score_mismatch",
        "error",
    )


def test_merge_issue_type_counts_combines_legacy_and_canonical_keys() -> None:
    merged_counts = merge_issue_type_counts(
        {
            "single_team_perspective_only": 2,
            "canonical.single_team_perspective_only": 3,
            "score_mismatch": 1,
        }
    )

    assert merged_counts == {
        "canonical.single_team_perspective_only": 5,
        "canonical.score_mismatch": 1,
    }
    assert severity_counts_from_issue_type_counts(merged_counts) == {"warning": 5, "error": 1}


def test_in_memory_repository_normalizes_legacy_quality_issues_on_read_and_backfill() -> None:
    repository = InMemoryIngestionRepository()
    repository.data_quality_issues.extend(
        [
            {
                "id": 1,
                "issue_type": "single_team_perspective_only",
                "severity": "warning",
                "raw_team_game_row_id": None,
                "canonical_game_id": None,
                "details": {"season_label": "2024-2025"},
            },
            {
                "id": 2,
                "issue_type": "score_mismatch",
                "severity": "warning",
                "raw_team_game_row_id": None,
                "canonical_game_id": None,
                "details": {"season_label": "2024-2025"},
            },
        ]
    )

    issues = repository.list_data_quality_issues(limit=10)

    assert [issue.issue_type for issue in issues] == [
        "canonical.score_mismatch",
        "canonical.single_team_perspective_only",
    ]
    assert [issue.severity for issue in issues] == ["error", "warning"]
    assert repository.get_data_quality_issue_type_counts() == {
        "canonical.single_team_perspective_only": 1,
        "canonical.score_mismatch": 1,
    }
    assert repository.get_data_quality_issue_severity_counts() == {"warning": 1, "error": 1}

    preview = repository.normalize_data_quality_issue_taxonomy(dry_run=True)

    assert preview == {
        "matched_rows": 2,
        "updated_rows": 2,
        "issue_type_updates": 2,
        "severity_updates": 1,
    }
    assert repository.data_quality_issues[0]["issue_type"] == "single_team_perspective_only"

    applied = repository.normalize_data_quality_issue_taxonomy(dry_run=False)

    assert applied == preview
    assert (
        repository.data_quality_issues[0]["issue_type"]
        == "canonical.single_team_perspective_only"
    )
    assert repository.data_quality_issues[1]["issue_type"] == "canonical.score_mismatch"
    assert repository.data_quality_issues[1]["severity"] == "error"
