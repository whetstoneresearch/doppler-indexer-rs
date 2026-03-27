CREATE TABLE IF NOT EXISTS dhook_pool_configs (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    pool_id BYTEA NOT NULL,
    hook_address BYTEA NOT NULL,
    numeraire BYTEA NOT NULL,
    total_tokens_on_bonding_curve NUMERIC NOT NULL,
    doppler_hook BYTEA NOT NULL,
    status SMALLINT NOT NULL,
    far_tick INTEGER NOT NULL,
    is_token_0 BOOLEAN NOT NULL,
    source VARCHAR(255) NOT NULL,
    source_version INT NOT NULL,
    UNIQUE (chain_id, hook_address, source, source_version)
);
