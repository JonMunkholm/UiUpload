// Package web provides HTTP handlers for the CSV import application.
// This file contains shared utilities and helper functions used across handlers.
package web

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/JonMunkholm/TUI/internal/core"
	"github.com/JonMunkholm/TUI/internal/web/templates"
	"github.com/jackc/pgx/v5/pgtype"
)

// MaxUploadSize is the maximum allowed file size (100MB).
const MaxUploadSize = 100 * 1024 * 1024

// parseIntParam parses an integer query parameter with a default value.
func parseIntParam(r *http.Request, name string, defaultVal int) int {
	val := r.URL.Query().Get(name)
	if val == "" {
		return defaultVal
	}
	i, err := strconv.Atoi(val)
	if err != nil || i < 1 {
		return defaultVal
	}
	return i
}

// parseSorts parses comma-separated sort parameters from URL.
func parseSorts(r *http.Request) []core.SortSpec {
	sortStr := r.URL.Query().Get("sort")
	dirStr := r.URL.Query().Get("dir")

	if sortStr == "" {
		return nil
	}

	cols := strings.Split(sortStr, ",")
	dirs := strings.Split(dirStr, ",")

	var sorts []core.SortSpec
	for i, col := range cols {
		col = strings.TrimSpace(col)
		if col == "" {
			continue
		}
		dir := "asc"
		if i < len(dirs) {
			d := strings.TrimSpace(dirs[i])
			if d == "desc" {
				dir = "desc"
			}
		}
		sorts = append(sorts, core.SortSpec{Column: col, Dir: dir})
		if len(sorts) >= 2 {
			break
		}
	}
	return sorts
}

// parseFilters extracts column filters from URL query parameters.
func parseFilters(r *http.Request, def core.TableDefinition) core.FilterSet {
	var filters []core.ColumnFilter

	specMap := make(map[string]core.FieldSpec)
	for _, spec := range def.FieldSpecs {
		specMap[strings.ToLower(spec.Name)] = spec
	}

	for key, values := range r.URL.Query() {
		if !strings.HasPrefix(key, "filter[") || !strings.HasSuffix(key, "]") {
			continue
		}

		colName := key[7 : len(key)-1]
		if colName == "" {
			continue
		}

		spec, ok := specMap[strings.ToLower(colName)]
		if !ok {
			continue
		}

		for _, val := range values {
			parts := strings.SplitN(val, ":", 2)
			if len(parts) != 2 {
				continue
			}

			op := core.FilterOperator(parts[0])
			filterVal := parts[1]

			if filterVal == "" {
				continue
			}

			if !isValidOperator(op, spec.Type) {
				continue
			}

			dbCol := spec.DBColumn
			if dbCol == "" {
				dbCol = strings.ToLower(strings.ReplaceAll(spec.Name, " ", "_"))
			}

			filters = append(filters, core.ColumnFilter{
				Column:   spec.Name,
				DBColumn: dbCol,
				Operator: op,
				Value:    filterVal,
				Type:     spec.Type,
			})
		}
	}

	return core.FilterSet{Filters: filters}
}

// isValidOperator checks if an operator is valid for a given field type.
func isValidOperator(op core.FilterOperator, ft core.FieldType) bool {
	switch ft {
	case core.FieldText:
		switch op {
		case core.OpContains, core.OpEquals, core.OpStartsWith, core.OpEndsWith:
			return true
		}
	case core.FieldNumeric:
		switch op {
		case core.OpEquals, core.OpGreaterEq, core.OpLessEq, core.OpGreater, core.OpLess:
			return true
		}
	case core.FieldDate:
		switch op {
		case core.OpEquals, core.OpGreaterEq, core.OpLessEq:
			return true
		}
	case core.FieldBool:
		return op == core.OpEquals
	case core.FieldEnum:
		switch op {
		case core.OpEquals, core.OpIn:
			return true
		}
	}
	return false
}

// formatCellForExport formats a cell value for CSV export.
func formatCellForExport(v interface{}) string {
	if v == nil {
		return ""
	}

	switch val := v.(type) {
	case pgtype.Numeric:
		if !val.Valid {
			return ""
		}
		f, err := val.Float64Value()
		if err != nil || !f.Valid {
			return ""
		}
		if f.Float64 == float64(int64(f.Float64)) {
			return fmt.Sprintf("%.0f", f.Float64)
		}
		return fmt.Sprintf("%.2f", f.Float64)

	case pgtype.Date:
		if !val.Valid {
			return ""
		}
		return val.Time.Format("2006-01-02")

	case pgtype.Text:
		if !val.Valid {
			return ""
		}
		return val.String

	case pgtype.Bool:
		if !val.Valid {
			return ""
		}
		if val.Bool {
			return "Yes"
		}
		return "No"

	case time.Time:
		if val.IsZero() {
			return ""
		}
		return val.Format("2006-01-02")

	case bool:
		if val {
			return "Yes"
		}
		return "No"

	case string:
		return val

	default:
		return fmt.Sprintf("%v", v)
	}
}

// buildColumnMeta builds column metadata from a table definition.
func buildColumnMeta(def core.TableDefinition) []templates.ColumnMeta {
	uniqueKeySet := make(map[string]bool)
	for _, col := range def.Info.UniqueKey {
		uniqueKeySet[col] = true
	}

	specMap := make(map[string]core.FieldSpec)
	for _, spec := range def.FieldSpecs {
		specMap[spec.Name] = spec
	}

	meta := make([]templates.ColumnMeta, len(def.Info.Columns))
	for i, col := range def.Info.Columns {
		cm := templates.ColumnMeta{
			Name:        col,
			IsUniqueKey: uniqueKeySet[col],
		}

		if spec, ok := specMap[col]; ok {
			cm.DBColumn = spec.DBColumn
			if cm.DBColumn == "" {
				cm.DBColumn = col
			}
			cm.Type = fieldTypeToString(spec.Type)
			cm.EnumValues = spec.EnumValues
			cm.AllowEmpty = spec.AllowEmpty
		} else {
			cm.DBColumn = col
			cm.Type = "text"
			cm.AllowEmpty = true
		}

		meta[i] = cm
	}

	return meta
}

// fieldTypeToString converts a FieldType to a string for JSON.
func fieldTypeToString(ft core.FieldType) string {
	switch ft {
	case core.FieldText:
		return "text"
	case core.FieldNumeric:
		return "numeric"
	case core.FieldDate:
		return "date"
	case core.FieldBool:
		return "bool"
	case core.FieldEnum:
		return "enum"
	default:
		return "text"
	}
}

// UploadResultResponse wraps the upload result for JSON encoding.
type UploadResultResponse struct {
	UploadID   string           `json:"upload_id"`
	TableKey   string           `json:"table_key"`
	FileName   string           `json:"file_name"`
	TotalRows  int              `json:"total_rows"`
	Inserted   int              `json:"inserted"`
	Skipped    int              `json:"skipped"`
	FailedRows []core.FailedRow `json:"failed_rows,omitempty"`
	Duration   string           `json:"duration"`
	Error      string           `json:"error,omitempty"`
}

// toResponse converts an UploadResult to a JSON-friendly format.
func toResponse(result *core.UploadResult) UploadResultResponse {
	return UploadResultResponse{
		UploadID:   result.UploadID,
		TableKey:   result.TableKey,
		FileName:   result.FileName,
		TotalRows:  result.TotalRows,
		Inserted:   result.Inserted,
		Skipped:    result.Skipped,
		FailedRows: result.FailedRows,
		Duration:   result.Duration.String(),
		Error:      result.Error,
	}
}

// handleUploadQueueStatus returns the current state of the upload limiter.
// Used for monitoring and to check if the system can accept more uploads.
func (s *Server) handleUploadQueueStatus(w http.ResponseWriter, r *http.Request) {
	status := s.service.UploadLimiterStatus()
	writeJSON(w, status)
}
