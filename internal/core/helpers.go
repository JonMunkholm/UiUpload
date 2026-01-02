package core

import (
	"fmt"
	"strings"
)

// TemplateMatchThreshold is the minimum score for a template to be considered a match.
const TemplateMatchThreshold = 0.7

// DefaultPageSize is the default number of rows per page in table views.
const DefaultPageSize = 50

// DefaultHistoryLimit is the default number of history entries to retrieve.
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
type WhereBuilder struct {
	conditions []string
	args       []interface{}
	argIndex   int
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

// Args returns the current arguments slice.
func (w *WhereBuilder) Args() []interface{} {
	return w.args
}

// NextArgIndex returns the next available parameter index.
func (w *WhereBuilder) NextArgIndex() int {
	return w.argIndex
}
