ALTER TABLE raw_team_game_row
ADD COLUMN IF NOT EXISTS source_page_url TEXT;

ALTER TABLE raw_team_game_row
ADD COLUMN IF NOT EXISTS source_page_season_label VARCHAR(32);

UPDATE raw_team_game_row rr
SET
    source_page_url = COALESCE(rr.source_page_url, rr.source_url),
    source_page_season_label = COALESCE(
        rr.source_page_season_label,
        s.label
    )
FROM season s
WHERE rr.season_id = s.id
  AND (
    rr.source_page_url IS NULL
    OR rr.source_page_season_label IS NULL
  );

CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_team_game_row_source_coordinates
ON raw_team_game_row (
    provider_id,
    team_id,
    season_id,
    source_page_url,
    source_page_season_label,
    source_section,
    source_row_index
);
