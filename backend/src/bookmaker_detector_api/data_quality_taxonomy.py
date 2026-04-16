from __future__ import annotations

LEGACY_ISSUE_TYPE_ALIASES = {
    "single_team_perspective_only": "canonical.single_team_perspective_only",
    "score_mismatch": "canonical.score_mismatch",
    "total_line_mismatch": "canonical.total_line_mismatch",
    "spread_line_mismatch": "canonical.spread_line_mismatch",
}

CANONICAL_ERROR_ISSUE_TYPES = {
    "parse.invalid_game_date_format",
    "parse.invalid_score_format",
    "row_too_short",
    "canonical.score_mismatch",
    "canonical.total_line_mismatch",
    "canonical.spread_line_mismatch",
}


def canonical_issue_type(issue_type: str) -> str:
    return LEGACY_ISSUE_TYPE_ALIASES.get(issue_type, issue_type)


def canonical_severity_for_issue_type(issue_type: str, current_severity: str | None = None) -> str:
    normalized_issue_type = canonical_issue_type(issue_type)
    if normalized_issue_type in CANONICAL_ERROR_ISSUE_TYPES:
        return "error"
    if current_severity in {"warning", "error"}:
        return current_severity
    return "warning"


def normalize_issue_type_and_severity(
    issue_type: str,
    severity: str | None = None,
) -> tuple[str, str]:
    normalized_issue_type = canonical_issue_type(issue_type)
    normalized_severity = canonical_severity_for_issue_type(
        normalized_issue_type,
        current_severity=severity,
    )
    return normalized_issue_type, normalized_severity


def merge_issue_type_counts(counts: dict[str, int]) -> dict[str, int]:
    merged_counts: dict[str, int] = {}
    for issue_type, count in counts.items():
        normalized_issue_type = canonical_issue_type(issue_type)
        merged_counts[normalized_issue_type] = merged_counts.get(normalized_issue_type, 0) + count
    return merged_counts


def severity_counts_from_issue_type_counts(issue_type_counts: dict[str, int]) -> dict[str, int]:
    severity_counts: dict[str, int] = {}
    for issue_type, count in issue_type_counts.items():
        severity = canonical_severity_for_issue_type(issue_type)
        severity_counts[severity] = severity_counts.get(severity, 0) + count
    return severity_counts


def issue_type_filter_variants(issue_type: str) -> set[str]:
    normalized_issue_type = canonical_issue_type(issue_type)
    variants = {normalized_issue_type}
    variants.update(
        legacy_issue_type
        for legacy_issue_type, canonical_name in LEGACY_ISSUE_TYPE_ALIASES.items()
        if canonical_name == normalized_issue_type
    )
    return variants
