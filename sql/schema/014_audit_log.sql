-- +goose Up
CREATE TABLE audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- What happened
    action TEXT NOT NULL CHECK (action IN (
        'upload', 'upload_rollback',
        'cell_edit', 'bulk_edit',
        'row_delete', 'row_restore',
        'table_reset',
        'template_create', 'template_delete'
    )),
    severity TEXT NOT NULL DEFAULT 'medium' CHECK (severity IN ('low', 'medium', 'high', 'critical')),

    -- Where
    table_key TEXT NOT NULL,

    -- Who (NULL until auth implemented)
    user_id TEXT,
    user_email TEXT,
    user_name TEXT,

    -- Request metadata
    ip_address INET,
    user_agent TEXT,

    -- Target identification
    row_key TEXT,
    column_name TEXT,

    -- Change data
    old_value TEXT,
    new_value TEXT,
    row_data JSONB,
    rows_affected INT,

    -- Linkage
    upload_id UUID REFERENCES csv_uploads(id) ON DELETE SET NULL,
    batch_id UUID,
    related_audit_id UUID,

    -- Metadata
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Self-referential FK added after table creation
ALTER TABLE audit_log ADD CONSTRAINT fk_audit_log_related
    FOREIGN KEY (related_audit_id) REFERENCES audit_log(id) ON DELETE SET NULL;

-- Indexes for hot queries
CREATE INDEX idx_audit_log_table_time ON audit_log(table_key, created_at DESC);
CREATE INDEX idx_audit_log_action ON audit_log(action);
CREATE INDEX idx_audit_log_time ON audit_log(created_at DESC);
CREATE INDEX idx_audit_log_severity ON audit_log(severity) WHERE severity IN ('high', 'critical');
CREATE INDEX idx_audit_log_batch ON audit_log(batch_id) WHERE batch_id IS NOT NULL;

-- +goose Down
DROP TABLE IF EXISTS audit_log;
