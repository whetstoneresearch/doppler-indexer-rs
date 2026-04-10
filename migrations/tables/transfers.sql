CREATE TABLE IF NOT EXISTS transfers (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    log_index INT NOT NULL,
    "timestamp" TIMESTAMPTZ NOT NULL,
    token BYTEA NOT NULL,
    "from" BYTEA NOT NULL,
    "to" BYTEA NOT NULL,
    "value" NUMERIC NOT NULL,
    source VARCHAR(255) NOT NULL,
    source_version INT NOT NULL,
    UNIQUE (chain_id, block_number, log_index, source, source_version)
);