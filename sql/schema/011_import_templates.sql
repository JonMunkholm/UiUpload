-- +goose Up
CREATE TABLE import_templates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_key TEXT NOT NULL,
    name TEXT NOT NULL,
    column_mapping JSONB NOT NULL,
    csv_headers JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT import_templates_table_name_unique UNIQUE (table_key, name)
);

CREATE INDEX idx_import_templates_table_key ON import_templates(table_key);

-- +goose Down
DROP INDEX IF EXISTS idx_import_templates_table_key;
DROP TABLE IF EXISTS import_templates;
