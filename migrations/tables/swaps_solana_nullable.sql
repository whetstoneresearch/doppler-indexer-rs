-- Make EVM-specific delta columns nullable so Solana handlers can write to the same table.
-- All ALTER COLUMN DROP NOT NULL ops are idempotent in PostgreSQL.
ALTER TABLE swaps ALTER COLUMN asset DROP NOT NULL;
ALTER TABLE swaps ALTER COLUMN fee0_delta DROP NOT NULL;
ALTER TABLE swaps ALTER COLUMN fee1_delta DROP NOT NULL;
ALTER TABLE swaps ALTER COLUMN tokens_sold_delta DROP NOT NULL;
ALTER TABLE swaps ALTER COLUMN graduation_balance_delta DROP NOT NULL;
ALTER TABLE swaps ALTER COLUMN max_threshold_delta DROP NOT NULL;
ALTER TABLE swaps ALTER COLUMN market_cap_usd_delta DROP NOT NULL;
ALTER TABLE swaps ALTER COLUMN liquidity_usd_delta DROP NOT NULL;
ALTER TABLE swaps ALTER COLUMN value_usd DROP NOT NULL;
