ALTER TABLE IF EXISTS transfers
    ALTER COLUMN log_index TYPE BIGINT
    USING log_index::BIGINT;

ALTER TABLE IF EXISTS liquidity_deltas
    ALTER COLUMN log_index TYPE BIGINT
    USING log_index::BIGINT;

ALTER TABLE IF EXISTS _call_revert_log
    ALTER COLUMN log_index TYPE BIGINT
    USING log_index::BIGINT;
