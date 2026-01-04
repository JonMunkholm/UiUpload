package core

// helpers.go provides utility functions and builders for common operations.
//
// Key utilities:
//   - WhereBuilder: Constructs parameterized WHERE clauses safely
//   - resolveDBColumn: Maps display column names to database column names
//   - Column constants for defaults and thresholds

import (
	"fmt"
	"strings"
)

// TemplateMatchThreshold is the minimum score (0.0-1.0) for a template to be
// considered a match during auto-detection. Columns are matched by Jaccard
// similarity of their normalized names.
const TemplateMatchThreshold = 0.7

// DefaultPageSize is the default number of rows per page in table views.
// Used when the client doesn't specify a page size.
const DefaultPageSize = 25

// DefaultHistoryLimit is the default number of history entries to retrieve.
// Applies to upload history, audit log, and similar paginated lists.
const DefaultHistoryLimit = 50

// resolveDBColumn returns the database column name for a given field name.
// It checks the FieldSpecs for a DBColumn mapping, falling back to snake_case conversion.
func resolveDBColumn(col string, specs []FieldSpec) string {
	for _, spec := range specs {
		if strings.EqualFold(spec.Name, col) && spec.DBColumn != "" {
			return spec.DBColumn
		}
	}
	return toDBColumnName(col)
}

// resolveDBColumns returns database column names for multiple field names.
func resolveDBColumns(cols []string, specs []FieldSpec) []string {
	result := make([]string, len(cols))
	for i, col := range cols {
		result[i] = resolveDBColumn(col, specs)
	}
	return result
}

// WhereBuilder constructs parameterized WHERE clauses for SQL queries.
//
// It manages parameter indexing automatically ($1, $2, etc.) and builds
// safe, parameterized queries to prevent SQL injection.
//
// Example usage:
//
//	wb := NewWhereBuilder()
//	wb.Add("table_key", "sfdc_customers")
//	wb.AddSearch("acme", specs)
//	wb.AddFilters(filters)
//	whereClause, args := wb.Build()
//	// whereClause: " WHERE table_key = $1 AND (name ILIKE $2 OR email ILIKE $2)"
//	// args: ["sfdc_customers", "%acme%"]
type WhereBuilder struct {
	conditions []string       // SQL condition fragments (e.g., "table_key = $1")
	args       []interface{}  // Corresponding parameter values
	argIndex   int            // Next parameter index to use
}

// NewWhereBuilder creates a new WhereBuilder starting at parameter $1.
func NewWhereBuilder() *WhereBuilder {
	return &WhereBuilder{argIndex: 1}
}

// AddSearch adds text search conditions (OR across searchable text columns).
// Uses ILIKE for case-insensitive partial matching.
func (w *WhereBuilder) AddSearch(query string, specs []FieldSpec) {
	if query == "" {
		return
	}

	var textConditions []string
	for _, spec := range specs {
		if spec.Type == FieldText {
			dbCol := spec.DBColumn
			if dbCol == "" {
				dbCol = toDBColumnName(spec.Name)
			}
			textConditions = append(textConditions, fmt.Sprintf("%s ILIKE $%d", quoteIdentifier(dbCol), w.argIndex))
		}
	}

	if len(textConditions) > 0 {
		w.conditions = append(w.conditions, "("+strings.Join(textConditions, " OR ")+")")
		w.args = append(w.args, "%"+query+"%")
		w.argIndex++
	}
}

// AddFilters adds column filter conditions (AND together).
func (w *WhereBuilder) AddFilters(filters FilterSet) {
	for _, f := range filters.Filters {
		condition, filterArgs, newArgIdx := buildSingleFilter(f, w.argIndex)
		if condition != "" {
			w.conditions = append(w.conditions, condition)
			w.args = append(w.args, filterArgs...)
			w.argIndex = newArgIdx
		}
	}
}

// Build returns the WHERE clause string and arguments.
// Returns empty string if no conditions were added.
func (w *WhereBuilder) Build() (string, []interface{}) {
	if len(w.conditions) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(w.conditions, " AND "), w.args
}

// NextArgIndex returns the next available parameter index.
func (w *WhereBuilder) NextArgIndex() int {
	return w.argIndex
}

// AddUploadID adds an upload_id = $N condition.
func (w *WhereBuilder) AddUploadID(uploadID string) {
	if uploadID == "" {
		return
	}
	w.conditions = append(w.conditions, fmt.Sprintf("upload_id = $%d", w.argIndex))
	w.args = append(w.args, uploadID)
	w.argIndex++
}

// Add adds a column = $N condition if the value is non-empty.
// The column name is used directly (should be a valid SQL identifier).
func (w *WhereBuilder) Add(column, value string) {
	if value == "" {
		return
	}
	w.conditions = append(w.conditions, fmt.Sprintf("%s = $%d", column, w.argIndex))
	w.args = append(w.args, value)
	w.argIndex++
}

// AddTimestampRange adds created_at BETWEEN conditions using $N placeholders.
func (w *WhereBuilder) AddTimestampRange(column string, start, end interface{}) {
	w.conditions = append(w.conditions, fmt.Sprintf("%s >= $%d AND %s <= $%d", column, w.argIndex, column, w.argIndex+1))
	w.args = append(w.args, start, end)
	w.argIndex += 2
}
