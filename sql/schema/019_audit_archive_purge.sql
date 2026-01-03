-- +goose Up
-- Function to purge old archived audit entries
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION purge_old_archives(years_to_keep INTEGER DEFAULT 7)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM audit_log_archive
    WHERE created_at < NOW() - (years_to_keep || ' years')::INTERVAL;

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- +goose Down
DROP FUNCTION IF EXISTS purge_old_archives;
