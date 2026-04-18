CREATE TABLE IF NOT EXISTS sol_prediction_claims (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    block_height BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    tx_id BYTEA NOT NULL,
    log_position BIGINT NOT NULL,
    market BYTEA NOT NULL,
    claimer BYTEA NOT NULL,
    burned_amount BIGINT NOT NULL,
    reward_amount BIGINT NOT NULL,
    total_burned BIGINT NOT NULL,
    source VARCHAR(255) NOT NULL,
    source_version INT NOT NULL,
    UNIQUE (chain_id, tx_id, log_position, source, source_version)
);
