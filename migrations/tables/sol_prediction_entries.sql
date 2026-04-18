CREATE TABLE IF NOT EXISTS sol_prediction_entries (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    block_height BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    market BYTEA NOT NULL,
    oracle BYTEA NOT NULL,
    entry_id BYTEA NOT NULL,
    base_mint BYTEA NOT NULL,
    contribution BIGINT,
    is_winner BOOLEAN,
    source VARCHAR(255) NOT NULL,
    source_version INT NOT NULL,
    UNIQUE (chain_id, market, entry_id, source, source_version)
);
