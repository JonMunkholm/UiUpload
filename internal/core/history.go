package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"
)

// EditHistoryEntry represents a single edit operation in the history.
// This type is kept for backwards compatibility with existing UI code.
type EditHistoryEntry struct {
	ID         string // Changed from int32 to string (UUID)
	TableKey   string
	Action     string // "cell_edit", "row_delete", "row_restore"
	RowKey     string
	ColumnName string
	OldValue   string
	NewValue   string
	RowData    map[string]interface{} // For delete operations
	CreatedAt  time.Time
}

// RecordCellEdit logs a cell edit to the audit log.
func (s *Service) RecordCellEdit(ctx context.Context, tableKey, rowKey, column, oldValue, newValue string) error {
	_, err := s.LogAudit(ctx, AuditLogParams{
		Action:       ActionCellEdit,
		TableKey:     tableKey,
		RowKey:       rowKey,
		ColumnName:   column,
		OldValue:     oldValue,
		NewValue:     newValue,
		RowsAffected: 1,
	})
	return err
}

// RecordRowDelete logs a row deletion to the audit log.
func (s *Service) RecordRowDelete(ctx context.Context, tableKey, rowKey string, rowData map[string]interface{}) error {
	_, err := s.LogAudit(ctx, AuditLogParams{
		Action:       ActionRowDelete,
		TableKey:     tableKey,
		RowKey:       rowKey,
		RowData:      rowData,
		RowsAffected: 1,
	})
	return err
}

// GetEditHistory returns recent edit history for a table.
// This wraps the new audit log system for backwards compatibility.
func (s *Service) GetEditHistory(ctx context.Context, tableKey string, limit int) ([]EditHistoryEntry, error) {
	auditEntries, err := s.GetAuditLog(ctx, AuditLogFilter{
		TableKey: tableKey,
		Limit:    limit,
	})
	if err != nil {
		return nil, err
	}

	entries := make([]EditHistoryEntry, 0, len(auditEntries))
	for _, ae := range auditEntries {
		// Only include edit/delete/restore actions
		if ae.Action != ActionCellEdit && ae.Action != ActionRowDelete && ae.Action != ActionRowRestore {
			continue
		}

		entry := EditHistoryEntry{
			ID:         ae.ID,
			TableKey:   ae.TableKey,
			Action:     mapAuditActionToHistoryAction(ae.Action),
			RowKey:     ae.RowKey,
			ColumnName: ae.ColumnName,
			OldValue:   ae.OldValue,
			NewValue:   ae.NewValue,
			RowData:    ae.RowData,
			CreatedAt:  ae.CreatedAt,
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// mapAuditActionToHistoryAction converts audit actions to legacy history actions.
func mapAuditActionToHistoryAction(action AuditAction) string {
	switch action {
	case ActionCellEdit:
		return "edit"
	case ActionRowDelete:
		return "delete"
	case ActionRowRestore:
		return "restore"
	default:
		return string(action)
	}
}

// GetEditHistoryEntry returns a single history entry by ID.
func (s *Service) GetEditHistoryEntry(ctx context.Context, id string) (*EditHistoryEntry, error) {
	ae, err := s.GetAuditLogByID(ctx, id)
	if err != nil {
		return nil, err
	}

	return &EditHistoryEntry{
		ID:         ae.ID,
		TableKey:   ae.TableKey,
		Action:     mapAuditActionToHistoryAction(ae.Action),
		RowKey:     ae.RowKey,
		ColumnName: ae.ColumnName,
		OldValue:   ae.OldValue,
		NewValue:   ae.NewValue,
		RowData:    ae.RowData,
		CreatedAt:  ae.CreatedAt,
	}, nil
}

// RevertResult contains the result of a revert operation.
type RevertResult struct {
	Success    bool   `json:"success"`
	RowDeleted bool   `json:"rowDeleted,omitempty"`
	Error      string `json:"error,omitempty"`
}

// RevertChange reverts a specific history entry.
func (s *Service) RevertChange(ctx context.Context, tableKey string, entryID string) (*RevertResult, error) {
	entry, err := s.GetEditHistoryEntry(ctx, entryID)
	if err != nil {
		return nil, fmt.Errorf("get history entry: %w", err)
	}

	if entry.TableKey != tableKey {
		return &RevertResult{
			Success: false,
			Error:   "entry does not belong to this table",
		}, nil
	}

	switch entry.Action {
	case "edit":
		// Revert the cell edit by setting it back to the old value
		// Check if row still exists
		exists, err := s.rowExists(ctx, tableKey, entry.RowKey)
		if err != nil {
			return nil, fmt.Errorf("check row exists: %w", err)
		}
		if !exists {
			return &RevertResult{
				Success:    false,
				RowDeleted: true,
			}, nil
		}

		// Perform the revert (this will create a new audit entry)
		_, err = s.UpdateCell(ctx, tableKey, UpdateCellRequest{
			RowKey: entry.RowKey,
			Column: entry.ColumnName,
			Value:  entry.OldValue,
		})
		if err != nil {
			return nil, fmt.Errorf("revert cell: %w", err)
		}

		return &RevertResult{Success: true}, nil

	case "delete":
		// Restore the deleted row
		if entry.RowData == nil {
			return &RevertResult{
				Success: false,
				Error:   "no row data available to restore",
			}, nil
		}

		err := s.restoreRow(ctx, tableKey, entry.RowData)
		if err != nil {
			return nil, fmt.Errorf("restore row: %w", err)
		}

		// Record the restore action
		s.recordRestore(ctx, tableKey, entry.RowKey)

		return &RevertResult{Success: true}, nil

	default:
		return &RevertResult{
			Success: false,
			Error:   "unknown action type",
		}, nil
	}
}

// rowExists checks if a row with the given key exists.
func (s *Service) rowExists(ctx context.Context, tableKey, rowKey string) (bool, error) {
	def, ok := Get(tableKey)
	if !ok {
		return false, fmt.Errorf("unknown table: %s", tableKey)
	}

	uniqueKey := def.Info.UniqueKey
	if len(uniqueKey) == 0 {
		return false, fmt.Errorf("table has no unique key")
	}

	keyParts := strings.Split(rowKey, "|")
	if len(keyParts) != len(uniqueKey) {
		return false, fmt.Errorf("invalid key format")
	}

	// Build DB column names using helper
	dbCols := resolveDBColumns(uniqueKey, def.FieldSpecs)

	// Build query
	conditions := make([]string, len(dbCols))
	args := make([]interface{}, len(keyParts))
	for i := range dbCols {
		conditions[i] = fmt.Sprintf("%s = $%d", quoteIdentifier(dbCols[i]), i+1)
		args[i] = keyParts[i]
	}

	query := fmt.Sprintf(
		"SELECT EXISTS(SELECT 1 FROM %s WHERE %s)",
		quoteIdentifier(tableKey),
		strings.Join(conditions, " AND "),
	)

	var exists bool
	err := s.pool.QueryRow(ctx, query, args...).Scan(&exists)
	return exists, err
}

// getCellValue fetches the current value of a cell.
func (s *Service) getCellValue(ctx context.Context, tableKey string, def TableDefinition, uniqueKey []string, rowKey, dbCol string) (string, error) {
	keyParts := strings.Split(rowKey, "|")
	if len(keyParts) != len(uniqueKey) {
		return "", fmt.Errorf("invalid key format")
	}

	// Build DB column names using helper
	dbKeyCols := resolveDBColumns(uniqueKey, def.FieldSpecs)

	// Build conditions
	conditions := make([]string, len(dbKeyCols))
	args := make([]interface{}, len(keyParts))
	for i := range dbKeyCols {
		conditions[i] = fmt.Sprintf("%s = $%d", quoteIdentifier(dbKeyCols[i]), i+1)
		args[i] = keyParts[i]
	}

	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s",
		quoteIdentifier(dbCol),
		quoteIdentifier(tableKey),
		strings.Join(conditions, " AND "),
	)

	var value interface{}
	err := s.pool.QueryRow(ctx, query, args...).Scan(&value)
	if err != nil {
		return "", err
	}

	// Convert to string
	if value == nil {
		return "", nil
	}
	return fmt.Sprintf("%v", value), nil
}

// getRowData fetches all column data for a row.
func (s *Service) getRowData(ctx context.Context, tableKey, rowKey string) (map[string]interface{}, error) {
	def, ok := Get(tableKey)
	if !ok {
		return nil, fmt.Errorf("unknown table: %s", tableKey)
	}

	uniqueKey := def.Info.UniqueKey
	keyParts := strings.Split(rowKey, "|")
	if len(keyParts) != len(uniqueKey) {
		return nil, fmt.Errorf("invalid key format")
	}

	// Build column list using helpers
	displayColumns := def.Info.Columns
	dbColumns := resolveDBColumns(displayColumns, def.FieldSpecs)
	quotedCols := quoteColumns(dbColumns)

	// Build DB column names for unique key using helper
	dbKeyCols := resolveDBColumns(uniqueKey, def.FieldSpecs)

	// Build conditions
	conditions := make([]string, len(dbKeyCols))
	args := make([]interface{}, len(keyParts))
	for i := range dbKeyCols {
		conditions[i] = fmt.Sprintf("%s = $%d", quoteIdentifier(dbKeyCols[i]), i+1)
		args[i] = keyParts[i]
	}

	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s",
		strings.Join(quotedCols, ", "),
		quoteIdentifier(tableKey),
		strings.Join(conditions, " AND "),
	)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, nil // Row not found
	}

	values, err := rows.Values()
	if err != nil {
		return nil, err
	}

	result := make(map[string]interface{})
	for i, col := range displayColumns {
		result[col] = values[i]
	}

	return result, nil
}

// restoreRow inserts a previously deleted row back into the table.
func (s *Service) restoreRow(ctx context.Context, tableKey string, rowData map[string]interface{}) error {
	def, ok := Get(tableKey)
	if !ok {
		return fmt.Errorf("unknown table: %s", tableKey)
	}

	// Build column list and values
	var columns []string
	var placeholders []string
	var args []interface{}
	argIdx := 1

	for _, col := range def.Info.Columns {
		val, exists := rowData[col]
		if !exists {
			continue
		}

		// Find DB column name using helper
		dbCol := resolveDBColumn(col, def.FieldSpecs)

		columns = append(columns, quoteIdentifier(dbCol))
		placeholders = append(placeholders, fmt.Sprintf("$%d", argIdx))
		args = append(args, val)
		argIdx++
	}

	if len(columns) == 0 {
		return fmt.Errorf("no columns to restore")
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		quoteIdentifier(tableKey),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	_, err := s.pool.Exec(ctx, query, args...)
	return err
}

// recordRestore logs a restore action to the audit log.
func (s *Service) recordRestore(ctx context.Context, tableKey, rowKey string) {
	_, err := s.LogAudit(ctx, AuditLogParams{
		Action:       ActionRowRestore,
		TableKey:     tableKey,
		RowKey:       rowKey,
		RowsAffected: 1,
	})
	if err != nil {
		log.Printf("failed to record restore action: %v", err)
	}
}

// recordRestoreWithRowData logs a restore action with the row data.
func (s *Service) recordRestoreWithRowData(ctx context.Context, tableKey, rowKey string, rowData map[string]interface{}) {
	var rowDataJSON []byte
	if rowData != nil {
		var err error
		rowDataJSON, err = json.Marshal(rowData)
		if err != nil {
			log.Printf("failed to marshal row data: %v", err)
		}
	}

	// Log audit with row data
	_, err := s.LogAudit(ctx, AuditLogParams{
		Action:       ActionRowRestore,
		TableKey:     tableKey,
		RowKey:       rowKey,
		RowData:      rowData,
		RowsAffected: 1,
	})
	if err != nil {
		log.Printf("failed to record restore action: %v", err)
	}

	// Suppress unused variable warning
	_ = rowDataJSON
}
