package core

import (
	"context"
	"fmt"

	db "github.com/JonMunkholm/TUI/internal/database"
	"github.com/jackc/pgx/v5/pgtype"
)

// RollbackUpload deletes all rows that were inserted from a specific upload.
func (s *Service) RollbackUpload(ctx context.Context, uploadID string) (RollbackResult, error) {
	result := RollbackResult{
		UploadID: uploadID,
	}

	// Parse UUID
	var pgUUID pgtype.UUID
	if err := pgUUID.Scan(uploadID); err != nil {
		result.Error = fmt.Sprintf("invalid upload ID: %v", err)
		return result, fmt.Errorf("invalid upload ID: %w", err)
	}

	// Get upload info
	upload, err := db.New(s.pool).GetUploadById(ctx, pgUUID)
	if err != nil {
		result.Error = fmt.Sprintf("upload not found: %v", err)
		return result, fmt.Errorf("get upload: %w", err)
	}

	result.TableKey = upload.Name

	// Check if already rolled back
	if upload.Status.Valid && upload.Status.String == "rolled_back" {
		result.Error = "upload already rolled back"
		return result, fmt.Errorf("upload already rolled back")
	}

	// Get table definition
	def, ok := Get(upload.Name)
	if !ok {
		result.Error = fmt.Sprintf("unknown table: %s", upload.Name)
		return result, fmt.Errorf("unknown table: %s", upload.Name)
	}

	// Check if table supports rollback
	if def.DeleteByUploadID == nil {
		result.Error = "table does not support rollback"
		return result, fmt.Errorf("table does not support rollback")
	}

	// Delete the rows
	rowsDeleted, err := def.DeleteByUploadID(ctx, s.pool, pgUUID)
	if err != nil {
		result.Error = fmt.Sprintf("delete failed: %v", err)
		return result, fmt.Errorf("delete by upload ID: %w", err)
	}

	// Mark upload as rolled back
	if err := db.New(s.pool).MarkUploadRolledBack(ctx, pgUUID); err != nil {
		// Log but don't fail - rows are already deleted
		result.Error = fmt.Sprintf("warning: rows deleted but status update failed: %v", err)
	}

	result.RowsDeleted = rowsDeleted
	result.Success = true

	// Log audit entry for rollback
	s.LogAudit(ctx, AuditLogParams{
		Action:       ActionUploadRollback,
		TableKey:     result.TableKey,
		UploadID:     uploadID,
		RowsAffected: int(rowsDeleted),
		IPAddress:    GetIPAddressFromContext(ctx),
		UserAgent:    GetUserAgentFromContext(ctx),
	})

	return result, nil
}
