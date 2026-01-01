package core

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	db "github.com/JonMunkholm/TUI/internal/database"
	"github.com/jackc/pgx/v5/pgtype"
)

// EditHistoryEntry represents a single edit operation in the history.
type EditHistoryEntry struct {
	ID         int32
	TableKey   string
	Action     string // "edit", "delete", "restore"
	RowKey     string
	ColumnName string
	OldValue   string
	NewValue   string
	RowData    map[string]interface{} // For delete operations
	CreatedAt  time.Time
}

// RecordCellEdit logs a cell edit to the history.
func (s *Service) RecordCellEdit(ctx context.Context, tableKey, rowKey, column, oldValue, newValue string) error {
	params := db.InsertEditHistoryParams{
		TableKey:   tableKey,
		Action:     "edit",
		RowKey:     rowKey,
		ColumnName: pgtype.Text{String: column, Valid: true},
		OldValue:   pgtype.Text{String: oldValue, Valid: oldValue != ""},
		NewValue:   pgtype.Text{String: newValue, Valid: newValue != ""},
		RowData:    nil,
	}

	_, err := db.New(s.pool).InsertEditHistory(ctx, params)
	return err
}

// RecordRowDelete logs a row deletion to the history.
func (s *Service) RecordRowDelete(ctx context.Context, tableKey, rowKey string, rowData map[string]interface{}) error {
	var rowDataJSON []byte
	if rowData != nil {
		var err error
		rowDataJSON, err = json.Marshal(rowData)
		if err != nil {
			return fmt.Errorf("marshal row data: %w", err)
		}
	}

	params := db.InsertEditHistoryParams{
		TableKey:   tableKey,
		Action:     "delete",
		RowKey:     rowKey,
		ColumnName: pgtype.Text{Valid: false},
		OldValue:   pgtype.Text{Valid: false},
		NewValue:   pgtype.Text{Valid: false},
		RowData:    rowDataJSON,
	}

	_, err := db.New(s.pool).InsertEditHistory(ctx, params)
	return err
}

// GetEditHistory returns recent edit history for a table.
func (s *Service) GetEditHistory(ctx context.Context, tableKey string, limit int) ([]EditHistoryEntry, error) {
	rows, err := db.New(s.pool).GetEditHistory(ctx, db.GetEditHistoryParams{
		TableKey: tableKey,
		Limit:    int32(limit),
	})
	if err != nil {
		return nil, err
	}

	entries := make([]EditHistoryEntry, 0, len(rows))
	for _, row := range rows {
		entry := EditHistoryEntry{
			ID:        row.ID,
			TableKey:  row.TableKey,
			Action:    row.Action,
			RowKey:    row.RowKey,
			CreatedAt: row.CreatedAt.Time,
		}

		if row.ColumnName.Valid {
			entry.ColumnName = row.ColumnName.String
		}
		if row.OldValue.Valid {
			entry.OldValue = row.OldValue.String
		}
		if row.NewValue.Valid {
			entry.NewValue = row.NewValue.String
		}
		if row.RowData != nil {
			json.Unmarshal(row.RowData, &entry.RowData)
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

// GetEditHistoryEntry returns a single history entry by ID.
func (s *Service) GetEditHistoryEntry(ctx context.Context, id int32) (*EditHistoryEntry, error) {
	row, err := db.New(s.pool).GetEditHistoryEntry(ctx, id)
	if err != nil {
		return nil, err
	}

	entry := &EditHistoryEntry{
		ID:        row.ID,
		TableKey:  row.TableKey,
		Action:    row.Action,
		RowKey:    row.RowKey,
		CreatedAt: row.CreatedAt.Time,
	}

	if row.ColumnName.Valid {
		entry.ColumnName = row.ColumnName.String
	}
	if row.OldValue.Valid {
		entry.OldValue = row.OldValue.String
	}
	if row.NewValue.Valid {
		entry.NewValue = row.NewValue.String
	}
	if row.RowData != nil {
		json.Unmarshal(row.RowData, &entry.RowData)
	}

	return entry, nil
}

// RevertResult contains the result of a revert operation.
type RevertResult struct {
	Success    bool   `json:"success"`
	RowDeleted bool   `json:"rowDeleted,omitempty"`
	Error      string `json:"error,omitempty"`
}

// RevertChange reverts a specific history entry.
func (s *Service) RevertChange(ctx context.Context, tableKey string, entryID int32) (*RevertResult, error) {
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

		// Perform the revert (this will create a new history entry)
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

	// Build DB column names
	dbCols := make([]string, len(uniqueKey))
	for i, col := range uniqueKey {
		dbCol := ""
		for _, spec := range def.FieldSpecs {
			if strings.EqualFold(spec.Name, col) && spec.DBColumn != "" {
				dbCol = spec.DBColumn
				break
			}
		}
		if dbCol == "" {
			dbCol = toDBColumnName(col)
		}
		dbCols[i] = dbCol
	}

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

	// Build DB column names for unique key
	dbKeyCols := make([]string, len(uniqueKey))
	for i, col := range uniqueKey {
		dbKeyCol := ""
		for _, spec := range def.FieldSpecs {
			if strings.EqualFold(spec.Name, col) && spec.DBColumn != "" {
				dbKeyCol = spec.DBColumn
				break
			}
		}
		if dbKeyCol == "" {
			dbKeyCol = toDBColumnName(col)
		}
		dbKeyCols[i] = dbKeyCol
	}

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

	// Build column list
	displayColumns := def.Info.Columns
	quotedCols := make([]string, len(displayColumns))
	for i, col := range displayColumns {
		dbCol := ""
		for _, spec := range def.FieldSpecs {
			if spec.Name == col {
				dbCol = spec.DBColumn
				break
			}
		}
		if dbCol == "" {
			dbCol = toDBColumnName(col)
		}
		quotedCols[i] = quoteIdentifier(dbCol)
	}

	// Build DB column names for unique key
	dbKeyCols := make([]string, len(uniqueKey))
	for i, col := range uniqueKey {
		dbKeyCol := ""
		for _, spec := range def.FieldSpecs {
			if strings.EqualFold(spec.Name, col) && spec.DBColumn != "" {
				dbKeyCol = spec.DBColumn
				break
			}
		}
		if dbKeyCol == "" {
			dbKeyCol = toDBColumnName(col)
		}
		dbKeyCols[i] = dbKeyCol
	}

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

		// Find DB column name
		dbCol := ""
		for _, spec := range def.FieldSpecs {
			if spec.Name == col {
				dbCol = spec.DBColumn
				break
			}
		}
		if dbCol == "" {
			dbCol = toDBColumnName(col)
		}

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

// recordRestore logs a restore action to history.
func (s *Service) recordRestore(ctx context.Context, tableKey, rowKey string) {
	params := db.InsertEditHistoryParams{
		TableKey:   tableKey,
		Action:     "restore",
		RowKey:     rowKey,
		ColumnName: pgtype.Text{Valid: false},
		OldValue:   pgtype.Text{Valid: false},
		NewValue:   pgtype.Text{Valid: false},
		RowData:    nil,
	}

	db.New(s.pool).InsertEditHistory(ctx, params)
}

// CleanupOldHistory removes history entries older than maxAge.
func (s *Service) CleanupOldHistory(ctx context.Context, maxAge time.Duration) error {
	cutoff := time.Now().Add(-maxAge)
	return db.New(s.pool).DeleteOldHistory(ctx, pgtype.Timestamp{
		Time:  cutoff,
		Valid: true,
	})
}
