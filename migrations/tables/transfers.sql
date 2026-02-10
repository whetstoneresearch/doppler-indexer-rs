CREATE TABLE IF NOT EXISTS (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    token BYTEA NOT NULL,
    from BYTEA NOT NULL,
    to BYTEA NOT NULL,
    value BIGINT NOT NULL,
    holder_count_delta NOT NULL
)