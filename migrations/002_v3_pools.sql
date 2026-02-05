-- V3 Pools table - tracks pools created via UniswapV3Initializer Create events

CREATE TABLE IF NOT EXISTS v3_pools (
    id BIGSERIAL PRIMARY KEY,
    pool_address BYTEA NOT NULL,
    asset BYTEA NOT NULL,
    numeraire BYTEA NOT NULL,
    block_number BIGINT NOT NULL,
    log_index INT NOT NULL,
    tx_hash BYTEA NOT NULL,
    timestamp BIGINT NOT NULL,
    chain_id BIGINT NOT NULL,
    UNIQUE (pool_address, chain_id)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_v3_pools_block ON v3_pools (block_number DESC);
CREATE INDEX IF NOT EXISTS idx_v3_pools_asset ON v3_pools (asset);
CREATE INDEX IF NOT EXISTS idx_v3_pools_numeraire ON v3_pools (numeraire);
CREATE INDEX IF NOT EXISTS idx_v3_pools_chain ON v3_pools (chain_id);
CREATE INDEX IF NOT EXISTS idx_v3_pools_timestamp ON v3_pools (timestamp DESC);
