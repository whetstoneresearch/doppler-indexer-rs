CREATE TABLE IF NOT EXISTS cumulated_fees (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    pool_id BYTEA NOT NULL,
    beneficiary BYTEA NOT NULL,
    token0_fees NUMERIC NOT NULL,
    token1_fees NUMERIC NOT NULL,
    source VARCHAR(255) NOT NULL,
    source_version INT NOT NULL,
    UNIQUE (chain_id, pool_id, beneficiary, source, source_version)
)