CREATE TABLE IF NOT EXISTS sol_prediction_markets (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    block_height BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    market BYTEA NOT NULL,
    oracle BYTEA NOT NULL,
    quote_mint BYTEA NOT NULL,
    winner_mint BYTEA,
    claimable_supply BIGINT,
    total_pot BIGINT,
    source VARCHAR(255) NOT NULL,
    source_version INT NOT NULL,
    UNIQUE (chain_id, market, source, source_version)
);
