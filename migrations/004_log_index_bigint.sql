ALTER TABLE IF EXISTS transfers
    ALTER COLUMN log_position TYPE BIGINT
    USING log_position::BIGINT;

ALTER TABLE IF EXISTS liquidity_deltas
    ALTER COLUMN log_position TYPE BIGINT
    USING log_position::BIGINT;

ALTER TABLE IF EXISTS _call_revert_log
    ALTER COLUMN log_position TYPE BIGINT
    USING log_position::BIGINT;
