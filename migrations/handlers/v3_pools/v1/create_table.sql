-- V3 Pools table v1 - tracks pools created via UniswapV3Initializer Create events

CREATE TABLE IF NOT EXISTS v3_pools_v1 (
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

CREATE INDEX IF NOT EXISTS idx_v3_pools_v1_block ON v3_pools_v1 (block_number DESC);
CREATE INDEX IF NOT EXISTS idx_v3_pools_v1_asset ON v3_pools_v1 (asset);
CREATE INDEX IF NOT EXISTS idx_v3_pools_v1_numeraire ON v3_pools_v1 (numeraire);
CREATE INDEX IF NOT EXISTS idx_v3_pools_v1_chain ON v3_pools_v1 (chain_id);
CREATE INDEX IF NOT EXISTS idx_v3_pools_v1_timestamp ON v3_pools_v1 (timestamp DESC);
