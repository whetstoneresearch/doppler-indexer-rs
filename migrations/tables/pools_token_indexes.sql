-- Indexes on pools.base_token and pools.quote_token for efficient
-- frontier expansion queries in token price path resolution.
CREATE INDEX IF NOT EXISTS idx_pools_base_token ON pools (chain_id, base_token);
CREATE INDEX IF NOT EXISTS idx_pools_quote_token ON pools (chain_id, quote_token);
