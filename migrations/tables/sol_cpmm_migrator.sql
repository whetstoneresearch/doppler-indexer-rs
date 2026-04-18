CREATE TABLE IF NOT EXISTS sol_cpmm_migrator (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    block_height BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    launch BYTEA NOT NULL,
    state BYTEA,
    cpmm_config BYTEA,
    min_raise_quote BIGINT,
    pool BYTEA,
    quote_for_liquidity BIGINT,
    base_for_liquidity BIGINT,
    migrated BOOLEAN NOT NULL DEFAULT false,
    source VARCHAR(255) NOT NULL,
    source_version INT NOT NULL,
    UNIQUE (chain_id, launch, source, source_version)
);
