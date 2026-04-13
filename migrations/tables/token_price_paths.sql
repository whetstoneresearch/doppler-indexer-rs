-- Caches resolved price paths from tokens to anchor (USD-priceable) tokens.
-- Used by the frontier expansion algorithm to avoid repeated graph searches.
-- Entries are refreshed when stale (> 1 week) and on demand for new tokens.
CREATE TABLE IF NOT EXISTS token_price_paths (
    chain_id            BIGINT NOT NULL,
    token_address       BYTEA NOT NULL,
    path_pool_ids       BYTEA[] NOT NULL,
    anchor_token        BYTEA NOT NULL,
    path_liquidity_usd  NUMERIC,
    resolved_at_block   BIGINT NOT NULL,
    is_priceable        BOOLEAN NOT NULL DEFAULT true,
    PRIMARY KEY (chain_id, token_address)
);
