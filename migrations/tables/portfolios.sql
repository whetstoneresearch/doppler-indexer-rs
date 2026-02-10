CREATE TABLE IF NOT EXISTS portfolios (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    user_id BIGSERIAL NOT NULL,
    token BYTEA NOT NULL,
    balance BIGINT NOT NULL,
    first_interacted TIMESTAMPTZ NOT NULL,
    last_interacted TIMESTAMPTZ NOT NULL,
    source VARCHAR(255) NOT NULL,
    source_version INT NOT NULL,
    UNIQUE (user_id, chain_id, token, source, source_version)
)