-- name: InsertAuditLog :one
INSERT INTO audit_log (
    action, severity, table_key,
    user_id, user_email, user_name,
    ip_address, user_agent,
    row_key, column_name,
    old_value, new_value, row_data, rows_affected,
    upload_id, batch_id, related_audit_id, reason
) VALUES (
    $1, $2, $3,
    $4, $5, $6,
    $7, $8,
    $9, $10,
    $11, $12, $13, $14,
    $15, $16, $17, $18
) RETURNING *;

-- name: GetAuditLogByTable :many
SELECT * FROM audit_log
WHERE table_key = $1
  AND created_at >= $2
  AND created_at <= $3
ORDER BY created_at DESC
LIMIT $4 OFFSET $5;

-- name: GetAuditLogAll :many
SELECT * FROM audit_log
WHERE created_at >= $1
  AND created_at <= $2
ORDER BY created_at DESC
LIMIT $3 OFFSET $4;

-- name: GetAuditLogByAction :many
SELECT * FROM audit_log
WHERE action = $1
  AND created_at >= $2
  AND created_at <= $3
ORDER BY created_at DESC
LIMIT $4 OFFSET $5;

-- name: CountAuditLogByTable :one
SELECT COUNT(*) FROM audit_log
WHERE table_key = $1
  AND created_at >= $2
  AND created_at <= $3;

-- name: CountAuditLogAll :one
SELECT COUNT(*) FROM audit_log
WHERE created_at >= $1
  AND created_at <= $2;

-- name: GetAuditLogByID :one
SELECT * FROM audit_log WHERE id = $1;

-- name: GetAuditLogArchiveByTable :many
SELECT * FROM audit_log_archive
WHERE table_key = $1
  AND created_at >= $2
  AND created_at <= $3
ORDER BY created_at DESC
LIMIT $4 OFFSET $5;

-- name: GetAuditLogArchiveAll :many
SELECT * FROM audit_log_archive
WHERE created_at >= $1
  AND created_at <= $2
ORDER BY created_at DESC
LIMIT $3 OFFSET $4;

-- name: CountAuditLogArchiveByTable :one
SELECT COUNT(*) FROM audit_log_archive
WHERE table_key = $1
  AND created_at >= $2
  AND created_at <= $3;

-- name: CountAuditLogArchiveAll :one
SELECT COUNT(*) FROM audit_log_archive
WHERE created_at >= $1
  AND created_at <= $2;

-- name: CountAuditLogByAction :one
SELECT COUNT(*) FROM audit_log
WHERE action = $1
  AND created_at >= $2
  AND created_at <= $3;

-- name: GetAuditLogByActionAndTable :many
SELECT * FROM audit_log
WHERE action = $1
  AND table_key = $2
  AND created_at >= $3
  AND created_at <= $4
ORDER BY created_at DESC
LIMIT $5 OFFSET $6;

-- name: CountAuditLogByActionAndTable :one
SELECT COUNT(*) FROM audit_log
WHERE action = $1
  AND table_key = $2
  AND created_at >= $3
  AND created_at <= $4;
