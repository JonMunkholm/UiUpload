-- +goose Up
-- Optimized archive function with better concurrency and date-boundary processing

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION archive_audit_log(days_to_keep INTEGER DEFAULT 90, batch_size INTEGER DEFAULT 10000)
RETURNS INTEGER AS $$
DECLARE
    archived_count INTEGER := 0;
    cutoff_date TIMESTAMPTZ;
    batch_count INTEGER;
BEGIN
    cutoff_date := NOW() - (days_to_keep || ' days')::INTERVAL;

    -- Process in batches using date boundaries with row-level locking
    LOOP
        -- Use CTE to select, insert, and delete in one transaction
        -- FOR UPDATE SKIP LOCKED allows concurrent operations
        WITH to_archive AS (
            SELECT id
            FROM audit_log
            WHERE created_at < cutoff_date
            ORDER BY created_at
            LIMIT batch_size
            FOR UPDATE SKIP LOCKED
        ),
        moved AS (
            INSERT INTO audit_log_archive (
                id, action, severity, table_key,
                user_id, user_email, user_name,
                ip_address, user_agent,
                row_key, column_name,
                old_value, new_value, row_data, rows_affected,
                upload_id, batch_id, related_audit_id, reason, created_at
            )
            SELECT
                al.id, al.action, al.severity, al.table_key,
                al.user_id, al.user_email, al.user_name,
                al.ip_address, al.user_agent,
                al.row_key, al.column_name,
                al.old_value, al.new_value, al.row_data, al.rows_affected,
                al.upload_id, al.batch_id, al.related_audit_id, al.reason, al.created_at
            FROM audit_log al
            JOIN to_archive ta ON al.id = ta.id
            RETURNING 1
        ),
        deleted AS (
            DELETE FROM audit_log
            WHERE id IN (SELECT id FROM to_archive)
            RETURNING 1
        )
        SELECT COUNT(*) INTO batch_count FROM moved;

        archived_count := archived_count + batch_count;

        -- Exit when we've processed all eligible rows
        EXIT WHEN batch_count < batch_size;

        -- Brief pause to yield to other transactions (100ms)
        PERFORM pg_sleep(0.1);
    END LOOP;

    RETURN archived_count;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- +goose Down
-- Restore the original function
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
