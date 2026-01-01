-- +goose Up
CREATE TABLE cell_edit_history (
    id SERIAL PRIMARY KEY,
    table_key TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('edit', 'delete', 'restore')),
    row_key TEXT NOT NULL,
    column_name TEXT,
    old_value TEXT,
    new_value TEXT,
    row_data JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_edit_history_table ON cell_edit_history(table_key, created_at DESC);

-- +goose Down
DROP INDEX IF EXISTS idx_edit_history_table;
DROP TABLE IF EXISTS cell_edit_history;
