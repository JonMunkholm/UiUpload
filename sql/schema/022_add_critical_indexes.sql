-- +goose Up
-- +goose NO TRANSACTION
-- Critical indexes for hot query paths
-- Using CONCURRENTLY to avoid locking production tables

-- Audit log: partial index for upload_id lookups (rollback queries)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_audit_log_upload_id
    ON audit_log(upload_id) WHERE upload_id IS NOT NULL;

-- Audit log: partial index for self-referential FK lookups
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_audit_log_related_audit_id
    ON audit_log(related_audit_id) WHERE related_audit_id IS NOT NULL;

-- CSV uploads: index for history queries by table name
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_csv_uploads_name
    ON csv_uploads(name);

-- CSV uploads: index for sorting by upload time (most recent first)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_csv_uploads_uploaded_at
    ON csv_uploads(uploaded_at DESC);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS idx_csv_uploads_uploaded_at;
DROP INDEX CONCURRENTLY IF EXISTS idx_csv_uploads_name;
DROP INDEX CONCURRENTLY IF EXISTS idx_audit_log_related_audit_id;
DROP INDEX CONCURRENTLY IF EXISTS idx_audit_log_upload_id;
