CREATE TABLE IF NOT EXISTS page_retrieval (
    id BIGSERIAL PRIMARY KEY,
    job_run_id BIGINT REFERENCES job_run(id),
    provider_id INTEGER NOT NULL REFERENCES provider(id),
    team_id INTEGER NOT NULL REFERENCES team(id),
    season_id INTEGER NOT NULL REFERENCES season(id),
    source_url TEXT NOT NULL,
    http_status INTEGER,
    payload_storage_path TEXT,
    status VARCHAR(32) NOT NULL,
    error_message TEXT,
    retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_team_game_row (
    id BIGSERIAL PRIMARY KEY,
    provider_id INTEGER NOT NULL REFERENCES provider(id),
    team_id INTEGER NOT NULL REFERENCES team(id),
    season_id INTEGER NOT NULL REFERENCES season(id),
    page_retrieval_id BIGINT REFERENCES page_retrieval(id),
    source_url TEXT NOT NULL,
    source_page_url TEXT NOT NULL,
    source_page_season_label VARCHAR(32) NOT NULL,
    source_section VARCHAR(64) NOT NULL,
    source_row_index INTEGER NOT NULL,
    game_date DATE,
    opponent_team_code VARCHAR(8),
    is_away BOOLEAN NOT NULL DEFAULT FALSE,
    result_flag VARCHAR(4),
    team_score INTEGER,
    opponent_score INTEGER,
    ats_result_flag VARCHAR(4),
    ats_line NUMERIC(6, 2),
    ou_result_flag VARCHAR(4),
    total_line NUMERIC(6, 2),
    parse_status VARCHAR(32) NOT NULL,
    parse_warning_codes_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    canonicalization_status VARCHAR(32),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (
        provider_id,
        team_id,
        season_id,
        source_page_url,
        source_page_season_label,
        source_section,
        source_row_index
    )
);

CREATE TABLE IF NOT EXISTS canonical_game (
    id BIGSERIAL PRIMARY KEY,
    season_id INTEGER NOT NULL REFERENCES season(id),
    game_date DATE NOT NULL,
    home_team_id INTEGER NOT NULL REFERENCES team(id),
    away_team_id INTEGER NOT NULL REFERENCES team(id),
    home_score INTEGER,
    away_score INTEGER,
    final_home_margin INTEGER,
    final_total_points INTEGER,
    total_line NUMERIC(6, 2),
    home_spread_line NUMERIC(6, 2),
    away_spread_line NUMERIC(6, 2),
    reconciliation_status VARCHAR(32) NOT NULL,
    source_row_indexes_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    warning_codes_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (season_id, game_date, home_team_id, away_team_id)
);

CREATE TABLE IF NOT EXISTS game_metric (
    id BIGSERIAL PRIMARY KEY,
    canonical_game_id BIGINT NOT NULL UNIQUE REFERENCES canonical_game(id),
    spread_error_home NUMERIC(8, 2),
    spread_error_away NUMERIC(8, 2),
    total_error NUMERIC(8, 2),
    home_covered BOOLEAN,
    away_covered BOOLEAN,
    went_over BOOLEAN,
    went_under BOOLEAN,
    metric_version VARCHAR(32) NOT NULL DEFAULT 'v1',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS data_quality_issue (
    id BIGSERIAL PRIMARY KEY,
    issue_type VARCHAR(64) NOT NULL,
    severity VARCHAR(16) NOT NULL DEFAULT 'warning',
    raw_team_game_row_id BIGINT REFERENCES raw_team_game_row(id),
    canonical_game_id BIGINT REFERENCES canonical_game(id),
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS job_run_reporting_snapshot (
    id BIGSERIAL PRIMARY KEY,
    job_run_id BIGINT NOT NULL UNIQUE REFERENCES job_run(id) ON DELETE CASCADE,
    job_name VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL,
    run_label VARCHAR(64),
    provider_name VARCHAR(64),
    team_code VARCHAR(8),
    season_label VARCHAR(32),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    raw_rows_saved INTEGER NOT NULL DEFAULT 0,
    canonical_games_saved INTEGER NOT NULL DEFAULT 0,
    metrics_saved INTEGER NOT NULL DEFAULT 0,
    quality_issues_saved INTEGER NOT NULL DEFAULT 0,
    warning_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS page_retrieval_reporting_snapshot (
    id BIGSERIAL PRIMARY KEY,
    page_retrieval_id BIGINT NOT NULL UNIQUE REFERENCES page_retrieval(id) ON DELETE CASCADE,
    job_run_id BIGINT REFERENCES job_run(id) ON DELETE CASCADE,
    run_label VARCHAR(64),
    provider_name VARCHAR(64),
    team_code VARCHAR(8),
    season_label VARCHAR(32),
    source_url TEXT,
    status VARCHAR(32) NOT NULL,
    http_status INTEGER,
    payload_storage_path TEXT,
    error_message TEXT,
    retrieved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS job_run_quality_snapshot (
    id BIGSERIAL PRIMARY KEY,
    job_run_id BIGINT NOT NULL UNIQUE REFERENCES job_run(id) ON DELETE CASCADE,
    job_name VARCHAR(128) NOT NULL,
    run_label VARCHAR(64),
    provider_name VARCHAR(64),
    team_code VARCHAR(8),
    season_label VARCHAR(32),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    parse_valid_count INTEGER NOT NULL DEFAULT 0,
    parse_invalid_count INTEGER NOT NULL DEFAULT 0,
    parse_warning_count INTEGER NOT NULL DEFAULT 0,
    reconciliation_full_match_count INTEGER NOT NULL DEFAULT 0,
    reconciliation_partial_single_row_count INTEGER NOT NULL DEFAULT 0,
    reconciliation_conflict_score_count INTEGER NOT NULL DEFAULT 0,
    reconciliation_conflict_total_line_count INTEGER NOT NULL DEFAULT 0,
    reconciliation_conflict_spread_line_count INTEGER NOT NULL DEFAULT 0,
    quality_issue_warning_count INTEGER NOT NULL DEFAULT 0,
    quality_issue_error_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
