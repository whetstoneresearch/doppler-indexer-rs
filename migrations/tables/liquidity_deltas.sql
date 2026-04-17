CREATE TABLE IF NOT EXISTS liquidity_deltas (
    chain_id        BIGINT NOT NULL,
    pool_id         BYTEA NOT NULL,
    block_height    BIGINT NOT NULL,
    log_position       BIGINT NOT NULL,
    tick_lower      INT NOT NULL,
    tick_upper      INT NOT NULL,
    liquidity_delta NUMERIC NOT NULL,
    source          VARCHAR(255) NOT NULL,
    source_version  INT NOT NULL,
    UNIQUE (chain_id, pool_id, block_height, log_position, source, source_version)
);

CREATE INDEX IF NOT EXISTS idx_liq_deltas_pool ON liquidity_deltas (pool_id);
CREATE INDEX IF NOT EXISTS idx_liq_deltas_reorg ON liquidity_deltas (chain_id, block_height);
