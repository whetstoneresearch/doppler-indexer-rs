CREATE SEQUENCE IF NOT EXISTS pool_leaderboard_snapshot_id_seq;

CREATE TABLE IF NOT EXISTS pool_leaderboard_snapshot (
    snapshot_id   BIGINT      NOT NULL,
    chain_id      BIGINT      NOT NULL,
    sort_key      TEXT        NOT NULL,
    pool_id       BYTEA       NOT NULL,
    id            BIGINT      NOT NULL,
    sort_val      NUMERIC,
    block_height  BIGINT      NOT NULL,
    taken_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_id, chain_id, sort_key, pool_id)
);

CREATE INDEX IF NOT EXISTS idx_plsnap_keyset
    ON pool_leaderboard_snapshot
       (chain_id, sort_key, snapshot_id, sort_val DESC NULLS LAST, id DESC);

CREATE INDEX IF NOT EXISTS idx_plsnap_reorg
    ON pool_leaderboard_snapshot (chain_id, block_height);

CREATE INDEX IF NOT EXISTS idx_plsnap_gc
    ON pool_leaderboard_snapshot (taken_at);
