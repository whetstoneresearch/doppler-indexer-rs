CREATE TABLE IF NOT EXISTS prices (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ,
    block_height BIGINT NOT NULL,
    chain_id BIGINT NOT NULL,
    token BYTEA NOT NULL,
    quote_token BYTEA NOT NULL,
    price NUMERIC NOT NULL,
    source varchar(255) NOT NULL,
    source_version INT NOT NULL,
    UNIQUE (timestamp, chain_id, token, source, source_version)
)