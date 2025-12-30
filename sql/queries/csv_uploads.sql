-- name: InsertCsvUpload :exec
INSERT INTO csv_uploads (name, action, uploaded_at)
VALUES ($1, $2, NOW())
ON CONFLICT (name, action) DO NOTHING;

-- name: GetCsvUpload :many
SELECT *
FROM csv_uploads
WHERE name = $1;

-- name: ResetCsvUploads :exec
DELETE FROM csv_uploads;
