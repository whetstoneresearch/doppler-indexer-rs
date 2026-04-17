-- Make EVM-specific columns nullable so Solana handlers can write to the same table.
-- All ALTER COLUMN DROP NOT NULL ops are idempotent in PostgreSQL.
ALTER TABLE pools ALTER COLUMN integrator DROP NOT NULL;
ALTER TABLE pools ALTER COLUMN initializer DROP NOT NULL;
ALTER TABLE pools ALTER COLUMN fee DROP NOT NULL;
ALTER TABLE pools ALTER COLUMN starting_time DROP NOT NULL;
ALTER TABLE pools ALTER COLUMN ending_time DROP NOT NULL;
