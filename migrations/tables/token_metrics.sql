CREATE TABLE IF NOT EXISTS token_metrics (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,  
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    address BYTEA NOT NULL,
    last_seen TIMESTAMPTZ,
    holder_count INT NOT NULL,
    market_cap_usd BIGINT NOT NULL,
    liquidity_usd BIGINT NOT NULL,
    volume_usd BIGINT NOT NULL,
    source VARCHAR(255) NOT NULL,
    source_version INT NOT NULL,
    UNIQUE (chain_id, address, timestamp)
)