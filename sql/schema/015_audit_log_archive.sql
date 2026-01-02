-- +goose Up
CREATE TABLE audit_log_archive (
    id UUID PRIMARY KEY,
    action TEXT NOT NULL,
    severity TEXT NOT NULL,
    table_key TEXT NOT NULL,
    user_id TEXT,
    user_email TEXT,
    user_name TEXT,
    ip_address INET,
    user_agent TEXT,
    row_key TEXT,
    column_name TEXT,
    old_value TEXT,
    new_value TEXT,
    row_data JSONB,
    rows_affected INT,
    upload_id UUID,
    batch_id UUID,
    related_audit_id UUID,
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Minimal indexes for cold storage
CREATE INDEX idx_audit_archive_table_time ON audit_log_archive(table_key, created_at DESC);
CREATE INDEX idx_audit_archive_time ON audit_log_archive(created_at DESC);

-- Archive function: moves old entries from hot to cold storage
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION archive_audit_log(days_to_keep INTEGER DEFAULT 90, batch_size INTEGER DEFAULT 10000)
RETURNS INTEGER AS $$
DECLARE
    archived_count INTEGER := 0;
    batch_count INTEGER;
BEGIN
    LOOP
        WITH moved AS (
            DELETE FROM audit_log
            WHERE id IN (
                SELECT id FROM audit_log
                WHERE created_at < NOW() - (days_to_keep || ' days')::INTERVAL
                ORDER BY created_at
                LIMIT batch_size
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *
        )
        INSERT INTO audit_log_archive (
            id, action, severity, table_key,
            user_id, user_email, user_name,
            ip_address, user_agent,
            row_key, column_name,
            old_value, new_value, row_data, rows_affected,
            upload_id, batch_id, related_audit_id, reason, created_at
        )
        SELECT
            id, action, severity, table_key,
            user_id, user_email, user_name,
            ip_address, user_agent,
            row_key, column_name,
            old_value, new_value, row_data, rows_affected,
            upload_id, batch_id, related_audit_id, reason, created_at
        FROM moved;

        GET DIAGNOSTICS batch_count = ROW_COUNT;
        archived_count := archived_count + batch_count;

        EXIT WHEN batch_count < batch_size;
    END LOOP;

    RETURN archived_count;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- +goose Down
DROP FUNCTION IF EXISTS archive_audit_log;
DROP TABLE IF EXISTS audit_log_archive;
