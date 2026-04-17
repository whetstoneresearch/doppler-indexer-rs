CREATE TABLE IF NOT EXISTS _call_revert_log (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    block_height BIGINT NOT NULL,
    log_position BIGINT,
    source_name VARCHAR(255) NOT NULL,
    function_name VARCHAR(255) NOT NULL,
    target_address BYTEA NOT NULL,
    revert_reason TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_call_revert_log_lookup
    ON _call_revert_log (chain_id, block_height);
