CREATE TABLE IF NOT EXISTS sol_cpmm_positions (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    block_height BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    closed_at TIMESTAMPTZ,
    position BYTEA NOT NULL,
    pool BYTEA NOT NULL,
    owner BYTEA NOT NULL,
    position_id BIGINT NOT NULL,
    source VARCHAR(255) NOT NULL,
    source_version INT NOT NULL,
    UNIQUE (chain_id, position, source, source_version)
);
