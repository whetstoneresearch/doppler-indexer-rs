CREATE TABLE IF NOT EXISTS (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    hook_address BYTEA NOT NULL,
    num_tokens_to_sell BIGINT NOT NULL,
    min_proceeds BIGINT NOT NULL,
    max_proceeds BIGINT NOT NULL,
    starting_time TIMESTAMPTZ NOT NULL,
    ending_time TIMESTAMPTZ NOT NULL,
    starting_tick INTEGER NOT NULL,
    ending_tick INTEGER NOT NULL,
    epoch_length  BIGINT NOT NULL,
    gamma INTEGER NOT NULL,
    is_token_0 BOOLEAN NOT NULL,
    num_pd_slugs  BIGINT NOT NULL,
    source VARCHAR(255) NOT NULL,
    source_version INT NOT NULL,
    UNIQUE (chain_id, hook_address, source, source_version)
)