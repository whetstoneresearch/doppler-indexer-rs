-- V4 Swaps table v1 - tracks swap events from UniswapV4PoolManager

CREATE TABLE IF NOT EXISTS v4_swaps_v1 (
    id BIGSERIAL PRIMARY KEY,
    pool_id BYTEA NOT NULL,
    block_number BIGINT NOT NULL,
    log_index INT NOT NULL,
    tx_hash BYTEA NOT NULL,
    sender BYTEA NOT NULL,
    amount0 NUMERIC NOT NULL,
    amount1 NUMERIC NOT NULL,
    sqrt_price_x96 NUMERIC NOT NULL,
    liquidity NUMERIC NOT NULL,
    tick INT NOT NULL,
    fee BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    chain_id BIGINT NOT NULL,
    UNIQUE (pool_id, block_number, log_index)
);

CREATE INDEX IF NOT EXISTS idx_v4_swaps_v1_pool_block ON v4_swaps_v1 (pool_id, block_number DESC);
CREATE INDEX IF NOT EXISTS idx_v4_swaps_v1_block ON v4_swaps_v1 (block_number DESC);
CREATE INDEX IF NOT EXISTS idx_v4_swaps_v1_chain ON v4_swaps_v1 (chain_id);
CREATE INDEX IF NOT EXISTS idx_v4_swaps_v1_timestamp ON v4_swaps_v1 (timestamp DESC);
