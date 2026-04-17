-- Normalize schema for dual EVM + Solana support.
--
-- tx_hash → tx_id: EVM tx hashes are 32 bytes; Solana signatures are 64 bytes.
-- BYTEA already holds either, but the column name should be chain-neutral.
--
-- sqrt_price_x96 → sqrt_price: removes the Q64.96 (Uniswap V3) encoding assumption.
-- Solana (Orca Whirlpool) uses Q64.64; keeping the name generic avoids confusion.
--
-- tick and sqrt_price made nullable: non-tick-based Solana AMMs have no tick concept.

ALTER TABLE IF EXISTS tokens
    RENAME COLUMN tx_hash TO tx_id;

ALTER TABLE IF EXISTS _skipped_addresses
    RENAME COLUMN tx_hash TO tx_id;

ALTER TABLE IF EXISTS pool_state
    RENAME COLUMN sqrt_price_x96 TO sqrt_price;

ALTER TABLE IF EXISTS pool_state
    ALTER COLUMN tick DROP NOT NULL;

ALTER TABLE IF EXISTS pool_state
    ALTER COLUMN sqrt_price DROP NOT NULL;
