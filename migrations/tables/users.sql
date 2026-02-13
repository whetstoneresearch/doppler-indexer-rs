CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    address BYTEA NOT NULL,
    first_seen TIMESTAMPTZ,
    last_seen TIMESTAMPTZ,
    source VARCHAR(255) NOT NULL,
    source_version INT NOT NULL,
    UNIQUE (chain_id, address, source, source_version)
);