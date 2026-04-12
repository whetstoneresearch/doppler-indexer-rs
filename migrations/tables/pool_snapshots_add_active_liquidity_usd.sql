-- Phase 7 extension: USD value of in-range (active) positions at snapshot block.
-- Populated by the shared TVL compute module alongside tvl_usd.
ALTER TABLE pool_snapshots ADD COLUMN IF NOT EXISTS active_liquidity_usd NUMERIC;
