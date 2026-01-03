-- name: InsertFailedRow :exec
INSERT INTO upload_failed_rows (upload_id, line_number, reason, row_data)
VALUES ($1, $2, $3, $4);

-- name: GetFailedRowsByUploadId :many
SELECT id, upload_id, line_number, reason, row_data, created_at
FROM upload_failed_rows
WHERE upload_id = $1
ORDER BY line_number;

-- name: DeleteFailedRowsByUploadId :exec
DELETE FROM upload_failed_rows WHERE upload_id = $1;

-- name: CountFailedRowsByUploadId :one
SELECT COUNT(*) FROM upload_failed_rows WHERE upload_id = $1;

-- name: GetFailedRowsByUploadIdPaginated :many
SELECT id, upload_id, line_number, reason, row_data, created_at
FROM upload_failed_rows
WHERE upload_id = $1
ORDER BY line_number
LIMIT $2 OFFSET $3;
