-- +goose Up
-- Fix timestamp type to include timezone information
-- This ensures consistent timezone handling across different clients

ALTER TABLE csv_uploads
    ALTER COLUMN uploaded_at TYPE TIMESTAMPTZ
    USING uploaded_at AT TIME ZONE 'UTC';

-- +goose Down
-- Revert to timestamp without timezone
ALTER TABLE csv_uploads
    ALTER COLUMN uploaded_at TYPE TIMESTAMP
    USING uploaded_at AT TIME ZONE 'UTC';
