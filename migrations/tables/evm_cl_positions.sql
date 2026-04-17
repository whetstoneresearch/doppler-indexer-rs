CREATE TABLE IF NOT EXISTS evm_cl_positions (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    pool BYTEA NOT NULL,
    owner BYTEA NOT NULL,
    tick_lower INTEGER NOT NULL,
    tick_upper INTEGER NOT NULL,
    liquidity NUMERIC NOT NULL,
    created_at TIMESTAMPTZ,
    source VARCHAR(255) NOT NULL,
    source_version INT NOT NULL,
    UNIQUE (chain_id, pool, owner, tick_lower, tick_upper, source, source_version)
);
