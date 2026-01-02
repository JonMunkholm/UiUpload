-- name: CreateUploadRecord :one
-- Create an upload record BEFORE processing, returns ID for linking rows
INSERT INTO csv_uploads (name, action, file_name, rows_inserted, rows_skipped, duration_ms, status, uploaded_at)
VALUES ($1, $2, $3, 0, 0, 0, 'active', NOW())
RETURNING id;

-- name: UpdateUploadCounts :exec
-- Update final counts after processing completes
UPDATE csv_uploads
SET rows_inserted = $2, rows_skipped = $3, duration_ms = $4
WHERE id = $1;

-- name: UpdateUploadHeaders :exec
-- Store CSV headers for failed rows export
UPDATE csv_uploads
SET csv_headers = $2
WHERE id = $1;

-- name: GetUploadById :one
SELECT id, name, action, file_name, rows_inserted, rows_skipped, duration_ms, status, csv_headers, uploaded_at
FROM csv_uploads
WHERE id = $1;

-- name: MarkUploadRolledBack :exec
UPDATE csv_uploads
SET status = 'rolled_back'
WHERE id = $1;

-- name: InsertCsvUpload :exec
INSERT INTO csv_uploads (name, action, file_name, rows_inserted, rows_skipped, duration_ms, uploaded_at)
VALUES ($1, $2, $3, $4, $5, $6, NOW());

-- name: GetCsvUpload :many
SELECT *
FROM csv_uploads
WHERE name = $1;

-- name: GetLastUpload :one
SELECT id, name, action, file_name, rows_inserted, rows_skipped, duration_ms, uploaded_at
FROM csv_uploads
WHERE name = $1
ORDER BY uploaded_at DESC
LIMIT 1;

-- name: GetUploadHistory :many
SELECT id, name, action, file_name, rows_inserted, rows_skipped, duration_ms, status, uploaded_at
FROM csv_uploads
WHERE name = $1
ORDER BY uploaded_at DESC
LIMIT 5;

-- name: ResetCsvUploads :exec
DELETE FROM csv_uploads;
