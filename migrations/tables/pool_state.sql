CREATE TABLE IF NOT EXISTS pool_state (
    chain_id         BIGINT NOT NULL,
    pool_id          BYTEA NOT NULL,
    block_number     BIGINT NOT NULL,
    block_timestamp  BIGINT NOT NULL,
    tick             INTEGER NOT NULL,
    sqrt_price_x96   NUMERIC NOT NULL,
    price            NUMERIC NOT NULL,
    active_liquidity NUMERIC NOT NULL,
    volume_24h_usd   NUMERIC,
    price_change_1h  NUMERIC,
    price_change_24h NUMERIC,
    swap_count_24h   INTEGER,
    source           VARCHAR(255) NOT NULL,
    source_version   INT NOT NULL,
    UNIQUE (chain_id, pool_id, source, source_version)
);

CREATE INDEX IF NOT EXISTS idx_pool_state_price ON pool_state (price DESC NULLS LAST);
