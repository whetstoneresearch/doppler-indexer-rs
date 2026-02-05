-- Replace global transformation progress with per-handler progress tracking.
-- Each handler (identified by handler_key like "V3CreateHandler_v1") tracks
-- its own progress independently, enabling reprocessing and versioning.

DROP TABLE IF EXISTS _transformation_progress;

CREATE TABLE IF NOT EXISTS _handler_progress (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    handler_key VARCHAR(255) NOT NULL,
    range_start BIGINT NOT NULL,
    range_end BIGINT NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (chain_id, handler_key, range_start)
);

CREATE INDEX IF NOT EXISTS idx_handler_progress_lookup
    ON _handler_progress (chain_id, handler_key, range_start);
