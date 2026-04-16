CREATE TABLE IF NOT EXISTS feature_version (
    id BIGSERIAL PRIMARY KEY,
    feature_key VARCHAR(64) NOT NULL UNIQUE,
    version_label VARCHAR(128) NOT NULL,
    description TEXT,
    config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS game_feature_snapshot (
    id BIGSERIAL PRIMARY KEY,
    canonical_game_id BIGINT NOT NULL REFERENCES canonical_game(id) ON DELETE CASCADE,
    feature_version_id BIGINT NOT NULL REFERENCES feature_version(id) ON DELETE CASCADE,
    season_id INTEGER NOT NULL REFERENCES season(id),
    game_date DATE NOT NULL,
    home_team_id INTEGER NOT NULL REFERENCES team(id),
    away_team_id INTEGER NOT NULL REFERENCES team(id),
    feature_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (feature_version_id, canonical_game_id)
);
