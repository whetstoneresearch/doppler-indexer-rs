-- Rename guard: renames the old table name if it exists, no-op otherwise.
-- Run this before evm_cl_positions.sql.
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'positions')
    AND NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'evm_cl_positions')
    THEN
        ALTER TABLE positions RENAME TO evm_cl_positions;
    END IF;
END $$;
