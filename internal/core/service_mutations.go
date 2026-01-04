package core

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Reset deletes all data from a specific table.
func (s *Service) Reset(ctx context.Context, tableKey string) error {
	def, ok := Get(tableKey)
	if !ok {
		return fmt.Errorf("unknown table: %s", tableKey)
	}

	// Get row count before reset for audit logging
	rowCount, _ := countTable(ctx, s.pool, tableKey)

	resetCtx, cancel := context.WithTimeout(ctx, s.ResetTimeout())
	defer cancel()

	if err := def.Reset(resetCtx, s.pool); err != nil {
		return err
	}

	// Log audit entry for table reset
	s.LogAudit(ctx, AuditLogParams{
		Action:       ActionTableReset,
		TableKey:     tableKey,
		RowsAffected: int(rowCount),
		IPAddress:    GetIPAddressFromContext(ctx),
		UserAgent:    GetUserAgentFromContext(ctx),
	})

	return nil
}

// ResetAll deletes all data from all registered tables.
func (s *Service) ResetAll(ctx context.Context) error {
	resetCtx, cancel := context.WithTimeout(ctx, s.ResetTimeout())
	defer cancel()

	for _, def := range All() {
		// Get row count before reset for audit logging
		rowCount, _ := countTable(ctx, s.pool, def.Info.Key)

		if err := def.Reset(resetCtx, s.pool); err != nil {
			return fmt.Errorf("reset %s: %w", def.Info.Key, err)
		}

		// Log audit entry for each table reset
		s.LogAudit(ctx, AuditLogParams{
			Action:       ActionTableReset,
			TableKey:     def.Info.Key,
			RowsAffected: int(rowCount),
			IPAddress:    GetIPAddressFromContext(ctx),
			UserAgent:    GetUserAgentFromContext(ctx),
		})
	}

	return nil
}

// DeleteRows deletes rows by their unique key values.
// Keys are in format "val1|val2" for composite keys.
// Returns count of deleted rows.
func (s *Service) DeleteRows(ctx context.Context, tableKey string, keys []string) (int, error) {
	def, ok := Get(tableKey)
	if !ok {
		return 0, fmt.Errorf("unknown table: %s", tableKey)
	}

	uniqueKey := def.Info.UniqueKey
	if len(uniqueKey) == 0 {
		return 0, fmt.Errorf("table %s has no unique key defined", tableKey)
	}

	// Build DB column names for unique key columns
	dbCols := resolveDBColumns(uniqueKey, def.FieldSpecs)

	var totalDeleted int64

	// Record deletions in history before deleting
	for _, key := range keys {
		if rowData, err := s.getRowData(ctx, tableKey, key); err == nil && rowData != nil {
			s.RecordRowDelete(ctx, tableKey, key, rowData)
		}
	}

	if len(uniqueKey) == 1 {
		// Single column key - use ANY for batch efficiency
		query := fmt.Sprintf(
			"DELETE FROM %s WHERE %s = ANY($1)",
			quoteIdentifier(tableKey),
			quoteIdentifier(dbCols[0]),
		)
		result, err := s.pool.Exec(ctx, query, keys)
		if err != nil {
			return 0, fmt.Errorf("delete failed: %w", err)
		}
		totalDeleted = result.RowsAffected()
	} else {
		// Composite key - delete each key individually
		for _, key := range keys {
			parts := strings.Split(key, "|")
			if len(parts) != len(uniqueKey) {
				continue // Invalid key format, skip
			}

			conditions := make([]string, len(dbCols))
			args := make([]interface{}, len(parts))
			for i, part := range parts {
				conditions[i] = fmt.Sprintf("%s = $%d", quoteIdentifier(dbCols[i]), i+1)
				args[i] = part
			}

			query := fmt.Sprintf(
				"DELETE FROM %s WHERE %s",
				quoteIdentifier(tableKey),
				strings.Join(conditions, " AND "),
			)
			result, err := s.pool.Exec(ctx, query, args...)
			if err != nil {
				continue // Log but continue with other keys
			}
			totalDeleted += result.RowsAffected()
		}
	}

	return int(totalDeleted), nil
}

// UpdateCellRequest contains the data for updating a single cell.
type UpdateCellRequest struct {
	RowKey string // Composite key value (e.g., "val1|val2")
	Column string // Display column name
	Value  string // New value as string
}

// UpdateCellResult contains the result of a cell update.
type UpdateCellResult struct {
	Success         bool   `json:"success"`
	DuplicateKey    bool   `json:"duplicateKey,omitempty"`
	ConflictingKey  string `json:"conflictingKey,omitempty"`
	ValidationError string `json:"validationError,omitempty"`
}

// UpdateCell updates a single cell value.
func (s *Service) UpdateCell(ctx context.Context, tableKey string, req UpdateCellRequest) (*UpdateCellResult, error) {
	def, ok := Get(tableKey)
	if !ok {
		return nil, fmt.Errorf("unknown table: %s", tableKey)
	}

	uniqueKey := def.Info.UniqueKey
	if len(uniqueKey) == 0 {
		return nil, fmt.Errorf("table %s has no unique key defined", tableKey)
	}

	// Find the FieldSpec for this column
	var fieldSpec *FieldSpec
	for i := range def.FieldSpecs {
		if strings.EqualFold(def.FieldSpecs[i].Name, req.Column) {
			fieldSpec = &def.FieldSpecs[i]
			break
		}
	}

	// Determine DB column name
	dbCol := toDBColumnName(req.Column)
	if fieldSpec != nil && fieldSpec.DBColumn != "" {
		dbCol = fieldSpec.DBColumn
	}

	// Validate value against type
	if fieldSpec != nil {
		if err := validateCellValue(req.Value, *fieldSpec); err != nil {
			return &UpdateCellResult{
				Success:         false,
				ValidationError: err.Error(),
			}, nil
		}
	}

	// Check if column is part of unique key
	isUniqueKeyColumn := false
	for _, uk := range uniqueKey {
		if strings.EqualFold(uk, req.Column) {
			isUniqueKeyColumn = true
			break
		}
	}

	// If updating unique key column, check for duplicates
	if isUniqueKeyColumn && req.Value != "" {
		newKey := s.buildNewCompositeKey(uniqueKey, req.RowKey, req.Column, req.Value)

		// Check if new key exists (excluding current row)
		exists, err := s.keyExistsExcludingRow(ctx, tableKey, def, uniqueKey, newKey, req.RowKey)
		if err != nil {
			return nil, fmt.Errorf("duplicate check failed: %w", err)
		}

		if exists {
			return &UpdateCellResult{
				Success:        false,
				DuplicateKey:   true,
				ConflictingKey: newKey,
			}, nil
		}
	}

	// Fetch old value before update (for history)
	oldValue, _ := s.getCellValue(ctx, tableKey, def, uniqueKey, req.RowKey, dbCol)

	// Build and execute UPDATE query
	err := s.executeUpdateCell(ctx, tableKey, def, uniqueKey, req.RowKey, dbCol, req.Value, fieldSpec)
	if err != nil {
		return nil, fmt.Errorf("update failed: %w", err)
	}

	// Record in history (ignore errors - update already succeeded)
	s.RecordCellEdit(ctx, tableKey, req.RowKey, req.Column, oldValue, req.Value)

	return &UpdateCellResult{Success: true}, nil
}

// BulkEditRequest represents a request to edit multiple rows.
type BulkEditRequest struct {
	Keys   []string
	Column string
	Value  string
}

// BulkEditResult contains the result of a bulk edit operation.
type BulkEditResult struct {
	Updated int      `json:"updated"`
	Failed  int      `json:"failed"`
	Errors  []string `json:"errors,omitempty"`
}

// BulkEditRows updates a single column across multiple rows.
func (s *Service) BulkEditRows(ctx context.Context, tableKey string, req BulkEditRequest) (*BulkEditResult, error) {
	def, ok := Get(tableKey)
	if !ok {
		return nil, fmt.Errorf("unknown table: %s", tableKey)
	}

	uniqueKey := def.Info.UniqueKey
	if len(uniqueKey) == 0 {
		return nil, fmt.Errorf("table %s has no unique key defined", tableKey)
	}

	if len(req.Keys) == 0 {
		return nil, fmt.Errorf("no rows specified")
	}

	// Find the FieldSpec for this column
	var fieldSpec *FieldSpec
	for i := range def.FieldSpecs {
		if strings.EqualFold(def.FieldSpecs[i].Name, req.Column) {
			fieldSpec = &def.FieldSpecs[i]
			break
		}
	}
	if fieldSpec == nil {
		return nil, fmt.Errorf("column not found: %s", req.Column)
	}

	// Block editing unique key columns (would cause duplicates)
	for _, uk := range uniqueKey {
		if strings.EqualFold(uk, req.Column) {
			return nil, fmt.Errorf("cannot bulk edit unique key column: %s", req.Column)
		}
	}

	// Determine DB column name
	dbCol := toDBColumnName(req.Column)
	if fieldSpec.DBColumn != "" {
		dbCol = fieldSpec.DBColumn
	}

	// Validate value against type (once, not per row)
	if err := validateCellValue(req.Value, *fieldSpec); err != nil {
		return nil, fmt.Errorf("invalid value: %v", err)
	}

	result := &BulkEditResult{}

	// Update each row
	for _, key := range req.Keys {
		// Get old value for history
		oldValue, _ := s.getCellValue(ctx, tableKey, def, uniqueKey, key, dbCol)

		// Skip if value is unchanged
		if oldValue == req.Value {
			result.Updated++ // Count as success but no actual change
			continue
		}

		// Execute update
		err := s.executeUpdateCell(ctx, tableKey, def, uniqueKey, key, dbCol, req.Value, fieldSpec)
		if err != nil {
			result.Failed++
			result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", key, err))
			continue
		}

		// Record in history
		s.RecordCellEdit(ctx, tableKey, key, req.Column, oldValue, req.Value)
		result.Updated++
	}

	// Log audit entry for bulk edit (only if any rows were actually updated)
	if result.Updated > 0 {
		s.LogAudit(ctx, AuditLogParams{
			Action:       ActionBulkEdit,
			TableKey:     tableKey,
			ColumnName:   req.Column,
			NewValue:     req.Value,
			RowsAffected: result.Updated,
			IPAddress:    GetIPAddressFromContext(ctx),
			UserAgent:    GetUserAgentFromContext(ctx),
			Reason:       fmt.Sprintf("Bulk edited %d rows", result.Updated),
		})
	}

	return result, nil
}

// validateCellValue checks if a value is valid for the given field spec.
func validateCellValue(value string, spec FieldSpec) error {
	if value == "" {
		return nil // Empty values are allowed (will be NULL)
	}

	switch spec.Type {
	case FieldNumeric:
		result := ToPgNumeric(value)
		if !result.Valid {
			return fmt.Errorf("invalid number format")
		}
	case FieldDate:
		result := ToPgDate(value)
		if !result.Valid {
			return fmt.Errorf("invalid date format (use YYYY-MM-DD)")
		}
	case FieldBool:
		result := ToPgBool(value)
		if !result.Valid {
			return fmt.Errorf("must be yes/no, true/false, or 1/0")
		}
	case FieldEnum:
		if len(spec.EnumValues) > 0 {
			found := false
			for _, ev := range spec.EnumValues {
				if strings.EqualFold(ev, value) {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("value must be one of: %s", strings.Join(spec.EnumValues, ", "))
			}
		}
	}
	return nil
}

// buildNewCompositeKey builds the new composite key value after updating one column.
func (s *Service) buildNewCompositeKey(uniqueKey []string, oldKey, updatedColumn, newValue string) string {
	parts := strings.Split(oldKey, "|")
	if len(parts) != len(uniqueKey) {
		return newValue // Single key case
	}

	// Find which part to update
	for i, col := range uniqueKey {
		if strings.EqualFold(col, updatedColumn) {
			parts[i] = newValue
			break
		}
	}

	return strings.Join(parts, "|")
}

// keyExistsExcludingRow checks if a key exists in the table, excluding the current row.
func (s *Service) keyExistsExcludingRow(ctx context.Context, tableKey string, def TableDefinition, uniqueKey []string, newKey, currentKey string) (bool, error) {
	// Build DB column names
	dbCols := resolveDBColumns(uniqueKey, def.FieldSpecs)

	newParts := strings.Split(newKey, "|")
	currentParts := strings.Split(currentKey, "|")

	if len(newParts) != len(uniqueKey) || len(currentParts) != len(uniqueKey) {
		return false, fmt.Errorf("invalid key format")
	}

	// Build WHERE clause: new key matches AND NOT current key
	var conditions []string
	var args []interface{}
	argIdx := 1

	// Conditions for new key (must match)
	for i, dbCol := range dbCols {
		conditions = append(conditions, fmt.Sprintf("%s = $%d", quoteIdentifier(dbCol), argIdx))
		args = append(args, newParts[i])
		argIdx++
	}

	// Conditions for excluding current row (at least one must differ)
	var excludeConditions []string
	for i, dbCol := range dbCols {
		excludeConditions = append(excludeConditions, fmt.Sprintf("%s != $%d", quoteIdentifier(dbCol), argIdx))
		args = append(args, currentParts[i])
		argIdx++
	}

	query := fmt.Sprintf(
		"SELECT EXISTS(SELECT 1 FROM %s WHERE %s AND (%s))",
		quoteIdentifier(tableKey),
		strings.Join(conditions, " AND "),
		strings.Join(excludeConditions, " OR "),
	)

	var exists bool
	err := s.pool.QueryRow(ctx, query, args...).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

// executeUpdateCell performs the actual database update.
func (s *Service) executeUpdateCell(ctx context.Context, tableKey string, def TableDefinition, uniqueKey []string, rowKey, dbCol, value string, spec *FieldSpec) error {
	keyParts := strings.Split(rowKey, "|")
	if len(keyParts) != len(uniqueKey) {
		return fmt.Errorf("invalid row key format")
	}

	// Build DB column names for unique key
	dbKeyCols := resolveDBColumns(uniqueKey, def.FieldSpecs)

	// Convert value to appropriate type
	var dbValue interface{}
	if value == "" {
		dbValue = nil
	} else if spec != nil {
		switch spec.Type {
		case FieldNumeric:
			dbValue = ToPgNumeric(value)
		case FieldDate:
			dbValue = ToPgDate(value)
		case FieldBool:
			dbValue = ToPgBool(value)
		default:
			dbValue = ToPgText(value)
		}
	} else {
		dbValue = ToPgText(value)
	}

	// Build parameterized query
	conditions := make([]string, len(dbKeyCols))
	args := make([]interface{}, len(dbKeyCols)+1)

	args[0] = dbValue // Value to set is $1
	for i, keyCol := range dbKeyCols {
		conditions[i] = fmt.Sprintf("%s = $%d", quoteIdentifier(keyCol), i+2)
		args[i+1] = keyParts[i]
	}

	query := fmt.Sprintf(
		"UPDATE %s SET %s = $1 WHERE %s",
		quoteIdentifier(tableKey),
		quoteIdentifier(dbCol),
		strings.Join(conditions, " AND "),
	)

	_, err := s.pool.Exec(ctx, query, args...)
	return err
}

// ============================================================================
// Import Templates
// ============================================================================

// ImportTemplate represents a saved column mapping template.
type ImportTemplate struct {
	ID            string         `json:"id"`
	TableKey      string         `json:"tableKey"`
	Name          string         `json:"name"`
	ColumnMapping map[string]int `json:"columnMapping"`
	CSVHeaders    []string       `json:"csvHeaders"`
	CreatedAt     time.Time      `json:"createdAt"`
	UpdatedAt     time.Time      `json:"updatedAt"`
}

// TemplateMatch represents a template that matches CSV headers.
type TemplateMatch struct {
	Template   ImportTemplate `json:"template"`
	MatchScore float64        `json:"matchScore"`
}
