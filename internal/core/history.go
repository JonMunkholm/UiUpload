package core

import (
	"context"
	"fmt"
	"strings"
)

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
