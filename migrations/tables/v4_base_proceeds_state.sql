-- Cumulative proceeds checkpoint for V4BaseMetricsHandler.
--
-- Stores the latest totalProceeds and totalTokensSold per pool so the handler
-- can restore its cumulative delta state on restart without reprocessing all
-- historical blocks. One row per (chain, pool, source_version); upserted after
-- each committed batch.

CREATE TABLE IF NOT EXISTS v4_base_proceeds_state (
    chain_id           BIGINT NOT NULL,
    pool_id            BYTEA NOT NULL,
    total_proceeds     NUMERIC NOT NULL DEFAULT '0',
    total_tokens_sold  NUMERIC NOT NULL DEFAULT '0',
    source             VARCHAR(255) NOT NULL,
    source_version     INT NOT NULL,
    UNIQUE (chain_id, pool_id, source, source_version)
);
