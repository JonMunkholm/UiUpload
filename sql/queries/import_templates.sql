-- name: CreateImportTemplate :one
INSERT INTO import_templates (table_key, name, column_mapping, csv_headers)
VALUES ($1, $2, $3, $4)
RETURNING id, table_key, name, column_mapping, csv_headers, created_at, updated_at;

-- name: GetImportTemplate :one
SELECT id, table_key, name, column_mapping, csv_headers, created_at, updated_at
FROM import_templates
WHERE id = $1;

-- name: GetImportTemplateByName :one
SELECT id, table_key, name, column_mapping, csv_headers, created_at, updated_at
FROM import_templates
WHERE table_key = $1 AND name = $2;

-- name: ListImportTemplates :many
SELECT id, table_key, name, column_mapping, csv_headers, created_at, updated_at
FROM import_templates
WHERE table_key = $1
ORDER BY updated_at DESC;

-- name: UpdateImportTemplate :one
UPDATE import_templates
SET name = $2, column_mapping = $3, csv_headers = $4, updated_at = NOW()
WHERE id = $1
RETURNING id, table_key, name, column_mapping, csv_headers, created_at, updated_at;

-- name: DeleteImportTemplate :exec
DELETE FROM import_templates
WHERE id = $1;

-- name: GetAllImportTemplates :many
SELECT id, table_key, name, column_mapping, csv_headers, created_at, updated_at
FROM import_templates
ORDER BY table_key, updated_at DESC;
