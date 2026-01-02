package core

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// PreviewSummary contains the summary counts for upload preview.
type PreviewSummary struct {
	TotalRows       int `json:"totalRows"`
	NewRows         int `json:"newRows"`
	UpdateRows      int `json:"updateRows"`
	ErrorRows       int `json:"errorRows"`
	DuplicateInFile int `json:"duplicateInFile"`
}

// RowPreview represents a single row for preview display.
type RowPreview struct {
	LineNumber int               `json:"lineNumber"`
	RowKey     string            `json:"rowKey"`
	Values     map[string]string `json:"values"`
}

// UpdateDiff represents a before/after diff for a row that will be updated.
type UpdateDiff struct {
	LineNumber int               `json:"lineNumber"`
	RowKey     string            `json:"rowKey"`
	Current    map[string]string `json:"current"`
	Incoming   map[string]string `json:"incoming"`
	Changed    []string          `json:"changed"`
}

// ErrorPreview represents a row with validation errors.
type ErrorPreview struct {
	LineNumber int               `json:"lineNumber"`
	RowKey     string            `json:"rowKey,omitempty"`
	Values     map[string]string `json:"values"`
	Errors     []string          `json:"errors"`
}

// DuplicatePreview represents keys that appear multiple times in the file.
type DuplicatePreview struct {
	RowKey      string `json:"rowKey"`
	LineNumbers []int  `json:"lineNumbers"`
}

// PreviewResponse is the complete response from upload preview analysis.
type PreviewResponse struct {
	Summary          PreviewSummary     `json:"summary"`
	NewRowSamples    []RowPreview       `json:"newRowSamples"`
	UpdateDiffs      []UpdateDiff       `json:"updateDiffs"`
	ErrorSamples     []ErrorPreview     `json:"errorSamples"`
	DuplicateSamples []DuplicatePreview `json:"duplicateSamples"`
	ProcessingTimeMs int64              `json:"processingTimeMs"`
}

// Sample limits
const (
	maxNewRowSamples    = 10
	maxUpdateDiffs      = 10
	maxErrorSamples     = 20
	maxDuplicateSamples = 10
	keyBatchSize        = 1000
)

// AnalyzeUpload performs read-only analysis of a CSV upload.
// It validates all rows, checks for duplicates, and returns a preview of what will happen.
func (s *Service) AnalyzeUpload(ctx context.Context, tableKey string, fileData []byte, mapping map[string]int) (*PreviewResponse, error) {
	startTime := time.Now()

	def, ok := Get(tableKey)
	if !ok {
		return nil, fmt.Errorf("unknown table: %s", tableKey)
	}

	// Sanitize and parse CSV
	fileData = sanitizeUTF8(fileData)
	records, err := parseCSV(fileData)
	if err != nil {
		return nil, fmt.Errorf("parse CSV: %w", err)
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("empty file")
	}

	// Find header and data rows
	var csvHeaderIdx HeaderIndex
	var dataRows [][]string
	var headerRowIndex int

	if len(mapping) > 0 {
		headerRow := records[0]
		dataRows = records[1:]
		headerRowIndex = 0
		csvHeaderIdx = buildMappedHeaderIndex(mapping, headerRow)
	} else {
		headerIdx := findHeaderInRecords(records, def.Info.Columns)
		if headerIdx < 0 {
			return nil, fmt.Errorf("header not found (expected: %v)", def.Info.Columns)
		}
		headerRowIndex = headerIdx
		headerRow := records[headerIdx]
		dataRows = records[headerIdx+1:]
		csvHeaderIdx = MakeHeaderIndex(headerRow)
	}

	if len(dataRows) == 0 {
		return nil, fmt.Errorf("no data rows after header")
	}

	// Initialize response
	resp := &PreviewResponse{
		Summary: PreviewSummary{
			TotalRows: len(dataRows),
		},
	}

	// Track duplicates within file
	seenKeys := make(map[string][]int) // rowKey -> line numbers
	uniqueKey := def.Info.UniqueKey

	// First pass: validate all rows and extract keys
	type analyzedRow struct {
		lineNumber int
		rowKey     string
		values     map[string]string
		errors     []string
		isEmpty    bool
	}

	analyzedRows := make([]analyzedRow, 0, len(dataRows))

	for i, row := range dataRows {
		lineNum := headerRowIndex + i + 2 // 1-indexed, after header

		// Skip empty rows
		if isEmptyRow(row) {
			analyzedRows = append(analyzedRows, analyzedRow{isEmpty: true})
			continue
		}

		// Extract values and validate
		values := extractRowValues(row, csvHeaderIdx, def)
		errors := validateRowComplete(row, csvHeaderIdx, def)

		// Extract unique key
		rowKey := ""
		if len(uniqueKey) > 0 && len(errors) == 0 {
			rowKey = extractUniqueKey(row, csvHeaderIdx, uniqueKey)
			if rowKey != "" {
				seenKeys[rowKey] = append(seenKeys[rowKey], lineNum)
			}
		}

		analyzedRows = append(analyzedRows, analyzedRow{
			lineNumber: lineNum,
			rowKey:     rowKey,
			values:     values,
			errors:     errors,
		})

		// Collect error samples
		if len(errors) > 0 {
			resp.Summary.ErrorRows++
			if len(resp.ErrorSamples) < maxErrorSamples {
				resp.ErrorSamples = append(resp.ErrorSamples, ErrorPreview{
					LineNumber: lineNum,
					RowKey:     rowKey,
					Values:     values,
					Errors:     errors,
				})
			}
		}
	}

	// Track file duplicates
	for key, lines := range seenKeys {
		if len(lines) > 1 {
			resp.Summary.DuplicateInFile += len(lines) - 1 // Count extra occurrences
			if len(resp.DuplicateSamples) < maxDuplicateSamples {
				resp.DuplicateSamples = append(resp.DuplicateSamples, DuplicatePreview{
					RowKey:      key,
					LineNumbers: lines,
				})
			}
		}
	}

	// Collect unique keys for DB lookup
	var uniqueKeys []string
	for key := range seenKeys {
		uniqueKeys = append(uniqueKeys, key)
	}

	// Batch check which keys exist in DB
	existingKeys := make(map[string]bool)
	if len(uniqueKeys) > 0 && len(uniqueKey) > 0 {
		existing, err := s.CheckDuplicates(ctx, tableKey, uniqueKeys)
		if err == nil {
			for _, k := range existing {
				existingKeys[k] = true
			}
		}
	}

	// Classify rows as new or update
	var newRows []analyzedRow
	var updateRows []analyzedRow

	for _, ar := range analyzedRows {
		if ar.isEmpty || len(ar.errors) > 0 {
			continue
		}

		if ar.rowKey == "" {
			// No key - treat as new
			newRows = append(newRows, ar)
		} else if existingKeys[ar.rowKey] {
			updateRows = append(updateRows, ar)
		} else {
			newRows = append(newRows, ar)
		}
	}

	resp.Summary.NewRows = len(newRows)
	resp.Summary.UpdateRows = len(updateRows)

	// Collect new row samples
	for i := 0; i < len(newRows) && i < maxNewRowSamples; i++ {
		resp.NewRowSamples = append(resp.NewRowSamples, RowPreview{
			LineNumber: newRows[i].lineNumber,
			RowKey:     newRows[i].rowKey,
			Values:     newRows[i].values,
		})
	}

	// Fetch current values for update diffs
	if len(updateRows) > 0 {
		// Limit to first N for diff display
		diffCount := len(updateRows)
		if diffCount > maxUpdateDiffs {
			diffCount = maxUpdateDiffs
		}

		for i := 0; i < diffCount; i++ {
			ar := updateRows[i]
			currentValues, err := s.getRowData(ctx, tableKey, ar.rowKey)
			if err != nil || currentValues == nil {
				continue
			}

			// Convert to string map
			currentMap := make(map[string]string)
			for k, v := range currentValues {
				currentMap[k] = formatValueForPreview(v)
			}

			// Find changed columns
			var changed []string
			for col, incomingVal := range ar.values {
				if currentVal, ok := currentMap[col]; ok {
					if currentVal != incomingVal {
						changed = append(changed, col)
					}
				}
			}

			resp.UpdateDiffs = append(resp.UpdateDiffs, UpdateDiff{
				LineNumber: ar.lineNumber,
				RowKey:     ar.rowKey,
				Current:    currentMap,
				Incoming:   ar.values,
				Changed:    changed,
			})
		}
	}

	resp.ProcessingTimeMs = time.Since(startTime).Milliseconds()
	return resp, nil
}

// validateRowComplete validates a row and returns ALL errors (not just the first).
func validateRowComplete(row []string, headerIdx HeaderIndex, def TableDefinition) []string {
	var errors []string
	expectedCols := len(def.Info.Columns)

	// Check column count
	if len(row) < expectedCols {
		errors = append(errors, fmt.Sprintf("expected %d columns, got %d", expectedCols, len(row)))
		return errors
	}

	// Validate each field
	for _, spec := range def.FieldSpecs {
		pos, ok := headerIdx[strings.ToLower(spec.Name)]
		if !ok || pos >= len(row) {
			if spec.Required {
				errors = append(errors, fmt.Sprintf("missing required column %q", spec.Name))
			}
			continue
		}

		raw := CleanCell(row[pos])

		if raw == "" && spec.Required && !spec.AllowEmpty {
			errors = append(errors, fmt.Sprintf("empty required field %q", spec.Name))
			continue
		}

		// Apply normalizer if present
		if spec.Normalizer != nil && raw != "" {
			raw = spec.Normalizer(raw)
		}

		// Type validation for required non-empty fields
		if raw != "" && spec.Required {
			switch spec.Type {
			case FieldEnum:
				valid := false
				for _, v := range spec.EnumValues {
					if strings.EqualFold(raw, v) {
						valid = true
						break
					}
				}
				if !valid {
					errors = append(errors, fmt.Sprintf("invalid enum for %q: %q", spec.Name, raw))
				}
			case FieldDate:
				if !ToPgDate(raw).Valid {
					errors = append(errors, fmt.Sprintf("invalid date for %q: %q", spec.Name, raw))
				}
			case FieldNumeric:
				if !ToPgNumeric(raw).Valid {
					errors = append(errors, fmt.Sprintf("invalid numeric for %q: %q", spec.Name, raw))
				}
			case FieldBool:
				if !ToPgBool(raw).Valid {
					errors = append(errors, fmt.Sprintf("invalid bool for %q: %q", spec.Name, raw))
				}
			}
		}
	}

	return errors
}

// extractRowValues extracts column values as a string map.
func extractRowValues(row []string, headerIdx HeaderIndex, def TableDefinition) map[string]string {
	values := make(map[string]string)

	for _, col := range def.Info.Columns {
		pos, ok := headerIdx[strings.ToLower(col)]
		if ok && pos < len(row) {
			values[col] = CleanCell(row[pos])
		}
	}

	return values
}

// extractUniqueKey extracts the unique key value from a row.
func extractUniqueKey(row []string, headerIdx HeaderIndex, uniqueKey []string) string {
	parts := make([]string, len(uniqueKey))

	for i, col := range uniqueKey {
		pos, ok := headerIdx[strings.ToLower(col)]
		if !ok || pos >= len(row) {
			return ""
		}
		val := CleanCell(row[pos])
		if val == "" {
			return ""
		}
		parts[i] = val
	}

	return strings.Join(parts, "|")
}

// formatValueForPreview formats a database value for display in preview.
func formatValueForPreview(v any) string {
	if v == nil {
		return ""
	}

	switch val := v.(type) {
	case string:
		return val
	case bool:
		if val {
			return "Yes"
		}
		return "No"
	case int, int32, int64, float32, float64:
		return fmt.Sprintf("%v", val)
	case pgtype.Numeric:
		if !val.Valid {
			return ""
		}
		f, _ := val.Float64Value()
		if f.Valid {
			// Format with appropriate precision
			s := fmt.Sprintf("%.2f", f.Float64)
			// Remove trailing zeros after decimal
			s = strings.TrimRight(s, "0")
			s = strings.TrimRight(s, ".")
			return s
		}
		return ""
	case pgtype.Date:
		if !val.Valid {
			return ""
		}
		return val.Time.Format("2006-01-02")
	case pgtype.Bool:
		if !val.Valid {
			return ""
		}
		if val.Bool {
			return "Yes"
		}
		return "No"
	case pgtype.Text:
		if !val.Valid {
			return ""
		}
		return val.String
	case time.Time:
		return val.Format("2006-01-02")
	default:
		s := fmt.Sprintf("%v", val)
		// Handle time.Time that comes as string representation
		if len(s) > 10 && strings.Contains(s, " ") && strings.Count(s[:10], "-") == 2 {
			return s[:10]
		}
		if s == "true" {
			return "Yes"
		}
		if s == "false" {
			return "No"
		}
		return s
	}
}
