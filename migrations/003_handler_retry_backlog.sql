CREATE TABLE IF NOT EXISTS _handler_retry_backlog (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    handler_key VARCHAR(255) NOT NULL,
    range_start BIGINT NOT NULL,
    range_end BIGINT NOT NULL,
    failure_reason VARCHAR(64) NOT NULL,
    error_message TEXT NOT NULL,
    debug_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    failure_count INTEGER NOT NULL DEFAULT 1,
    first_failed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_failed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, handler_key, range_start)
);

CREATE INDEX IF NOT EXISTS idx_handler_retry_backlog_lookup
    ON _handler_retry_backlog (chain_id, handler_key, range_start);

CREATE INDEX IF NOT EXISTS idx_handler_retry_backlog_range
    ON _handler_retry_backlog (chain_id, range_start, range_end);
