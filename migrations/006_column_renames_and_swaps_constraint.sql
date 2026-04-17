-- Migration 006: Rename block_number -> block_height and log_index -> log_position
-- across all tables for chain-agnostic naming (EVM + Solana), and add log_position
-- to the swaps uniqueness constraint to support multiple swap events per transaction.

-- Part A: Rename block_number -> block_height in all tables that have it.

ALTER TABLE IF EXISTS _call_revert_log RENAME COLUMN block_number TO block_height;
ALTER TABLE IF EXISTS liquidity_deltas RENAME COLUMN block_number TO block_height;
ALTER TABLE IF EXISTS pool_metrics RENAME COLUMN block_number TO block_height;
ALTER TABLE IF EXISTS pool_snapshots RENAME COLUMN block_number TO block_height;
ALTER TABLE IF EXISTS pools RENAME COLUMN block_number TO block_height;
ALTER TABLE IF EXISTS pool_state RENAME COLUMN block_number TO block_height;
ALTER TABLE IF EXISTS prices RENAME COLUMN block_number TO block_height;
ALTER TABLE IF EXISTS _skipped_addresses RENAME COLUMN block_number TO block_height;
ALTER TABLE IF EXISTS swaps RENAME COLUMN block_number TO block_height;
ALTER TABLE IF EXISTS token_metrics RENAME COLUMN block_number TO block_height;
ALTER TABLE IF EXISTS tokens RENAME COLUMN block_number TO block_height;
ALTER TABLE IF EXISTS transfers RENAME COLUMN block_number TO block_height;
ALTER TABLE IF EXISTS _live_progress RENAME COLUMN block_number TO block_height;

-- Part B: Rename log_index -> log_position in tables that have it.

ALTER TABLE IF EXISTS _call_revert_log RENAME COLUMN log_index TO log_position;
ALTER TABLE IF EXISTS liquidity_deltas RENAME COLUMN log_index TO log_position;
ALTER TABLE IF EXISTS transfers RENAME COLUMN log_index TO log_position;

-- Part C: Add log_position to swaps and fix uniqueness constraint.

ALTER TABLE IF EXISTS swaps ADD COLUMN IF NOT EXISTS log_position BIGINT;

-- Drop old constraint and create new one including log_position.
-- Constraint name comes from Postgres auto-naming: tablename_col1_col2_..._key
DO $$
BEGIN
  ALTER TABLE swaps DROP CONSTRAINT IF EXISTS swaps_chain_id_tx_id_source_source_version_key;
EXCEPTION WHEN undefined_object THEN NULL;
END $$;

ALTER TABLE IF EXISTS swaps
  ADD CONSTRAINT swaps_chain_id_tx_id_log_position_source_source_version_key
  UNIQUE (chain_id, tx_id, log_position, source, source_version);
