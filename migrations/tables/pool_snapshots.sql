CREATE TABLE IF NOT EXISTS pool_snapshots (
    chain_id         BIGINT NOT NULL,
    pool_id          BYTEA NOT NULL,
    block_number     BIGINT NOT NULL,
    block_timestamp  BIGINT NOT NULL,
    price_open       NUMERIC NOT NULL,
    price_close      NUMERIC NOT NULL,
    price_high       NUMERIC NOT NULL,
    price_low        NUMERIC NOT NULL,
    active_liquidity NUMERIC NOT NULL,
    volume0          NUMERIC NOT NULL DEFAULT 0,
    volume1          NUMERIC NOT NULL DEFAULT 0,
    swap_count       INT NOT NULL DEFAULT 0,
    source           VARCHAR(255) NOT NULL,
    source_version   INT NOT NULL,
    UNIQUE (chain_id, pool_id, block_number, source, source_version)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_time ON pool_snapshots (pool_id, block_timestamp);
CREATE INDEX IF NOT EXISTS idx_snapshots_reorg ON pool_snapshots (chain_id, block_number);
