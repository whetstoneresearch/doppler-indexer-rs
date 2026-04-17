-- Live mode per-block progress tracking.
-- Tracks which handlers have completed processing for each block.
-- Entries are migrated to _handler_progress during compaction.

CREATE TABLE IF NOT EXISTS _live_progress (
    chain_id BIGINT NOT NULL,
    handler_key TEXT NOT NULL,
    block_height BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, handler_key, block_height)
);

-- Index for efficient range queries during compaction
CREATE INDEX IF NOT EXISTS idx_live_progress_chain_block
    ON _live_progress (chain_id, block_height);

-- Index for finding all handlers for a specific block
CREATE INDEX IF NOT EXISTS idx_live_progress_block
    ON _live_progress (block_height);
