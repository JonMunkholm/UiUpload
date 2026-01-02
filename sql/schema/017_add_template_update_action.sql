-- +goose Up
ALTER TABLE audit_log DROP CONSTRAINT IF EXISTS audit_log_action_check;
ALTER TABLE audit_log ADD CONSTRAINT audit_log_action_check
    CHECK (action IN (
        'upload', 'upload_rollback',
        'cell_edit', 'bulk_edit',
        'row_delete', 'row_restore',
        'table_reset',
        'template_create', 'template_update', 'template_delete'
    ));

-- +goose Down
ALTER TABLE audit_log DROP CONSTRAINT IF EXISTS audit_log_action_check;
ALTER TABLE audit_log ADD CONSTRAINT audit_log_action_check
    CHECK (action IN (
        'upload', 'upload_rollback',
        'cell_edit', 'bulk_edit',
        'row_delete', 'row_restore',
        'table_reset',
        'template_create', 'template_delete'
    ));
