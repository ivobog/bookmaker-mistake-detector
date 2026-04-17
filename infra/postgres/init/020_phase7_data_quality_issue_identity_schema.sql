DELETE FROM data_quality_issue dqi
USING (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY
                issue_type,
                COALESCE(raw_team_game_row_id, 0),
                COALESCE(canonical_game_id, 0)
            ORDER BY id DESC
        ) AS duplicate_rank
    FROM data_quality_issue
) duplicates
WHERE dqi.id = duplicates.id
  AND duplicates.duplicate_rank > 1;

CREATE UNIQUE INDEX IF NOT EXISTS ux_data_quality_issue_identity
ON data_quality_issue (
    issue_type,
    COALESCE(raw_team_game_row_id, 0),
    COALESCE(canonical_game_id, 0)
);
