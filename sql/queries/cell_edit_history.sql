-- name: InsertEditHistory :one
INSERT INTO cell_edit_history (table_key, action, row_key, column_name, old_value, new_value, row_data)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING id;

-- name: GetEditHistory :many
SELECT id, table_key, action, row_key, column_name, old_value, new_value, row_data, created_at
FROM cell_edit_history
WHERE table_key = $1
ORDER BY created_at DESC
LIMIT $2;

-- name: GetEditHistoryEntry :one
SELECT id, table_key, action, row_key, column_name, old_value, new_value, row_data, created_at
FROM cell_edit_history
WHERE id = $1;

-- name: DeleteOldHistory :exec
DELETE FROM cell_edit_history WHERE created_at < $1;
