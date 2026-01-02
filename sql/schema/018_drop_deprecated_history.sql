-- +goose Up
-- Drop the deprecated cell_edit_history table now that all data
-- has been migrated to audit_log and the old Edit History UI has been removed.
DROP TABLE IF EXISTS cell_edit_history_deprecated;
DROP TABLE IF EXISTS cell_edit_history;

-- +goose Down
-- Cannot restore the original table - data was migrated to audit_log
-- and the table schema is no longer maintained.
-- To restore functionality, use the audit_log table which contains
-- all historical cell edits with richer metadata.
