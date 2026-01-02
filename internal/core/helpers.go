package core

import "strings"

// TemplateMatchThreshold is the minimum score for a template to be considered a match.
const TemplateMatchThreshold = 0.7

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
