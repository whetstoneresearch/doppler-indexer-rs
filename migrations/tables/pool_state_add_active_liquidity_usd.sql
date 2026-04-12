-- Phase 7 extension: USD value of in-range (active) positions at the current
-- pool_state block. Populated by the shared TVL compute module alongside tvl_usd.
ALTER TABLE pool_state ADD COLUMN IF NOT EXISTS active_liquidity_usd NUMERIC;
