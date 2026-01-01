-- +goose Up
-- Enhance csv_uploads table for richer upload history

-- Add ID column for proper tracking
ALTER TABLE csv_uploads ADD COLUMN IF NOT EXISTS id UUID DEFAULT gen_random_uuid();

-- Add columns for richer history
ALTER TABLE csv_uploads ADD COLUMN IF NOT EXISTS file_name TEXT;
ALTER TABLE csv_uploads ADD COLUMN IF NOT EXISTS rows_inserted INTEGER DEFAULT 0;
ALTER TABLE csv_uploads ADD COLUMN IF NOT EXISTS rows_skipped INTEGER DEFAULT 0;
ALTER TABLE csv_uploads ADD COLUMN IF NOT EXISTS duration_ms INTEGER DEFAULT 0;

-- Remove the unique constraint to allow multiple uploads per table
ALTER TABLE csv_uploads DROP CONSTRAINT IF EXISTS csv_uploads_name_action_unique;

-- +goose Down
ALTER TABLE csv_uploads ADD CONSTRAINT csv_uploads_name_action_unique UNIQUE (name, action);
ALTER TABLE csv_uploads DROP COLUMN IF EXISTS duration_ms;
ALTER TABLE csv_uploads DROP COLUMN IF EXISTS rows_skipped;
ALTER TABLE csv_uploads DROP COLUMN IF EXISTS rows_inserted;
ALTER TABLE csv_uploads DROP COLUMN IF EXISTS file_name;
ALTER TABLE csv_uploads DROP COLUMN IF EXISTS id;
