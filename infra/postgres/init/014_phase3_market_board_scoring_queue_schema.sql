ALTER TABLE IF EXISTS model_scoring_run
ADD COLUMN IF NOT EXISTS model_market_board_id BIGINT
    REFERENCES model_market_board(id) ON DELETE SET NULL;
