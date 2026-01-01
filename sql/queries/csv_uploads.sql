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
SELECT id, name, action, file_name, rows_inserted, rows_skipped, duration_ms, uploaded_at
FROM csv_uploads
WHERE name = $1
ORDER BY uploaded_at DESC
LIMIT 5;

-- name: ResetCsvUploads :exec
DELETE FROM csv_uploads;
