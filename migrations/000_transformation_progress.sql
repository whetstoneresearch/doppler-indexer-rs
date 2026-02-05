-- Tracks which block ranges have been transformed for each chain
-- This allows the transformation engine to resume from where it left off

CREATE TABLE IF NOT EXISTS _transformation_progress (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    range_start BIGINT NOT NULL,
    range_end BIGINT NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (chain_id, range_start)
);

CREATE INDEX IF NOT EXISTS idx_transformation_progress_chain
    ON _transformation_progress (chain_id, range_start);
