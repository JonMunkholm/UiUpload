-- +goose Up
-- Migrate existing cell_edit_history to audit_log
INSERT INTO audit_log (
    action,
    severity,
    table_key,
    row_key,
    column_name,
    old_value,
    new_value,
    row_data,
    rows_affected,
    created_at
)
SELECT
    CASE action
        WHEN 'edit' THEN 'cell_edit'
        WHEN 'delete' THEN 'row_delete'
        WHEN 'restore' THEN 'row_restore'
    END as action,
    CASE action
        WHEN 'edit' THEN 'medium'
        WHEN 'delete' THEN 'high'
        WHEN 'restore' THEN 'medium'
    END as severity,
    table_key,
    row_key,
    column_name,
    old_value,
    new_value,
    row_data,
    1 as rows_affected,
    created_at
FROM cell_edit_history;

-- Rename old table (keep as backup for safety)
ALTER TABLE cell_edit_history RENAME TO cell_edit_history_deprecated;

-- +goose Down
ALTER TABLE cell_edit_history_deprecated RENAME TO cell_edit_history;
DELETE FROM audit_log WHERE action IN ('cell_edit', 'row_delete', 'row_restore');
