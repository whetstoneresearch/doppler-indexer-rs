CREATE TABLE IF NOT EXISTS (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    pool BYTEA NOT NULL,
    owner BYTEA NOT NULL,
    pool BYTEA NOT NULL,
    tick_lower INTEGER NOT NULL,
    tick_upper INTEGER NOT NULL,
    liquidity INTEGER NOT NULL,
    created_at TIMESTAMPTZ,
    UNIQUE (chain_id, pool, owner, tick_lower, tick_upper, source, source_version)
)