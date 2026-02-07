CREATE TABLE IF NOT EXISTS active_versions (
    source VARCHAR(255) PRIMARY KEY,
    active_version INT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
