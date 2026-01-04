-- +goose Up
-- Add soft delete support to csv_uploads

-- Add deleted_at column for soft delete
ALTER TABLE csv_uploads ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

-- Create view for active (non-deleted) records
-- Used by queries that should only see active uploads
CREATE OR REPLACE VIEW csv_uploads_active AS
    SELECT id, name, action, file_name, rows_inserted, rows_skipped,
           duration_ms, status, csv_headers, uploaded_at
    FROM csv_uploads
    WHERE deleted_at IS NULL;

-- Partial index for efficient soft delete filtering
-- Only indexes non-deleted rows, making WHERE deleted_at IS NULL fast
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_csv_uploads_deleted_at
    ON csv_uploads(deleted_at) WHERE deleted_at IS NOT NULL;

-- Index for querying deleted records (for restore/audit purposes)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_csv_uploads_active
    ON csv_uploads(uploaded_at DESC) WHERE deleted_at IS NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_csv_uploads_active;
DROP INDEX IF EXISTS idx_csv_uploads_deleted_at;
DROP VIEW IF EXISTS csv_uploads_active;
ALTER TABLE csv_uploads DROP COLUMN IF EXISTS deleted_at;
