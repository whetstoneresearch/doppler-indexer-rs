CREATE TABLE IF NOT EXISTS _skipped_addresses (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    tx_hash BYTEA NOT NULL,
    asset_address BYTEA NOT NULL,
    numeraire_address BYTEA NOT NULL,
    reason TEXT NOT NULL,
    source VARCHAR(255) NOT NULL,
    source_version INT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (chain_id, tx_hash, asset_address, source, source_version)
);

CREATE INDEX IF NOT EXISTS idx_skipped_addresses_lookup
    ON _skipped_addresses (chain_id, block_number);
