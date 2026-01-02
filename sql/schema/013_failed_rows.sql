-- +goose Up

-- Store CSV headers for reconstructing failed rows export
ALTER TABLE csv_uploads ADD COLUMN csv_headers TEXT[];

-- Store individual failed rows for download
CREATE TABLE upload_failed_rows (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    upload_id UUID NOT NULL REFERENCES csv_uploads(id) ON DELETE CASCADE,
    line_number INT NOT NULL,
    reason TEXT NOT NULL,
    row_data TEXT[] NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_upload_failed_rows_upload_id ON upload_failed_rows(upload_id);

-- +goose Down
DROP TABLE IF EXISTS upload_failed_rows;
ALTER TABLE csv_uploads DROP COLUMN IF EXISTS csv_headers;
