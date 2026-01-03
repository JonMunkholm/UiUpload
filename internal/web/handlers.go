package web

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/JonMunkholm/TUI/internal/core"
	"github.com/JonMunkholm/TUI/internal/web/templates"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// MaxUploadSize is the maximum allowed file size (100MB).
const MaxUploadSize = 100 * 1024 * 1024

// handleDashboard renders the main dashboard page.
func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Build table groups from registry with stats
	var groups []templates.TableGroup
	for _, groupName := range core.Groups() {
		tables := core.ByGroup(groupName)
		tableData := make([]templates.TableCardData, len(tables))
		for i, def := range tables {
			data := templates.TableCardData{
				Info: def.Info,
			}

			// Fetch stats (don't fail if we can't get them)
			if stats, err := s.service.GetTableStats(ctx, def.Info.Key); err == nil {
				data.RowCount = stats.RowCount
				if stats.LastUpload != nil {
					data.LastUpload = &stats.LastUpload.UploadedAt
				}
			}

			tableData[i] = data
		}
		groups = append(groups, templates.TableGroup{
			Name:   groupName,
			Tables: tableData,
		})
	}

	templates.Dashboard(groups).Render(ctx, w)
}

// handleListTables returns all tables organized by group.
func (s *Server) handleListTables(w http.ResponseWriter, r *http.Request) {
	tables := s.service.ListTablesByGroup()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(tables); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to encode response")
	}
}

// handleUpload processes a CSV file upload.
func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	tableKey := chi.URLParam(r, "tableKey")
	if tableKey == "" {
		writeError(w, http.StatusBadRequest, "missing table key")
		return
	}

	// Limit request size
	r.Body = http.MaxBytesReader(w, r.Body, MaxUploadSize)

	// Parse multipart form
	if err := r.ParseMultipartForm(MaxUploadSize); err != nil {
		writeError(w, http.StatusBadRequest, "file too large or invalid form")
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "no file provided")
		return
	}
	defer file.Close()

	// Read file data
	data, err := io.ReadAll(file)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to read file")
		return
	}

	// Parse optional column mapping
	var mapping map[string]int
	if mappingJSON := r.FormValue("mapping"); mappingJSON != "" {
		if err := json.Unmarshal([]byte(mappingJSON), &mapping); err != nil {
			writeError(w, http.StatusBadRequest, "invalid mapping format")
			return
		}
	}

	// Start upload with request metadata for audit logging
	ctx := WithRequestMetadata(r.Context(), r)
	uploadID, err := s.service.StartUpload(ctx, tableKey, header.Filename, data, mapping)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, map[string]string{"upload_id": uploadID})
}

// handlePreview analyzes a CSV file and returns what would happen on upload.
func (s *Server) handlePreview(w http.ResponseWriter, r *http.Request) {
	tableKey := chi.URLParam(r, "tableKey")
	if tableKey == "" {
		writeError(w, http.StatusBadRequest, "missing table key")
		return
	}

	// Limit request size
	r.Body = http.MaxBytesReader(w, r.Body, MaxUploadSize)

	// Parse multipart form
	if err := r.ParseMultipartForm(MaxUploadSize); err != nil {
		writeError(w, http.StatusBadRequest, "file too large or invalid form")
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "no file provided")
		return
	}
	defer file.Close()

	// Read file data
	data, err := io.ReadAll(file)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to read file")
		return
	}

	// Parse optional column mapping
	var mapping map[string]int
	if mappingJSON := r.FormValue("mapping"); mappingJSON != "" {
		if err := json.Unmarshal([]byte(mappingJSON), &mapping); err != nil {
			writeError(w, http.StatusBadRequest, "invalid mapping format")
			return
		}
	}

	// Run analysis
	result, err := s.service.AnalyzeUpload(r.Context(), tableKey, data, mapping)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, result)
}

// handleUploadProgress streams upload progress via Server-Sent Events.
func (s *Server) handleUploadProgress(w http.ResponseWriter, r *http.Request) {
	uploadID := chi.URLParam(r, "uploadID")
	if uploadID == "" {
		writeError(w, http.StatusBadRequest, "missing upload ID")
		return
	}

	// Subscribe to progress updates
	progressCh, err := s.service.SubscribeProgress(uploadID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Set up SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // Disable nginx buffering

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	// Stream progress updates
	for {
		select {
		case progress, ok := <-progressCh:
			if !ok {
				// Channel closed, upload complete
				fmt.Fprintf(w, "event: complete\ndata: {}\n\n")
				flusher.Flush()
				return
			}

			data, _ := json.Marshal(progress)
			fmt.Fprintf(w, "event: progress\ndata: %s\n\n", data)
			flusher.Flush()

		case <-r.Context().Done():
			// Client disconnected
			return
		}
	}
}

// handleCancelUpload cancels an in-progress upload.
func (s *Server) handleCancelUpload(w http.ResponseWriter, r *http.Request) {
	uploadID := chi.URLParam(r, "uploadID")
	if uploadID == "" {
		writeError(w, http.StatusBadRequest, "missing upload ID")
		return
	}

	if err := s.service.CancelUpload(uploadID); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"cancelled"}`))
}

// handleUploadResult returns the final result of an upload.
func (s *Server) handleUploadResult(w http.ResponseWriter, r *http.Request) {
	uploadID := chi.URLParam(r, "uploadID")
	if uploadID == "" {
		writeError(w, http.StatusBadRequest, "missing upload ID")
		return
	}

	result, err := s.service.GetUploadResult(uploadID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeJSON(w, toResponse(result))
}

// handleReset deletes all data from a specific table.
func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	tableKey := chi.URLParam(r, "tableKey")
	if tableKey == "" {
		writeError(w, http.StatusBadRequest, "missing table key")
		return
	}

	ctx := WithRequestMetadata(r.Context(), r)
	if err := s.service.Reset(ctx, tableKey); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"reset"}`))
}

// handleResetAll deletes all data from all tables.
func (s *Server) handleResetAll(w http.ResponseWriter, r *http.Request) {
	ctx := WithRequestMetadata(r.Context(), r)
	if err := s.service.ResetAll(ctx); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"reset_all"}`))
}

// handleRollbackUpload deletes all rows from a specific upload.
func (s *Server) handleRollbackUpload(w http.ResponseWriter, r *http.Request) {
	uploadID := chi.URLParam(r, "uploadID")
	if uploadID == "" {
		writeError(w, http.StatusBadRequest, "missing upload ID")
		return
	}

	ctx := WithRequestMetadata(r.Context(), r)
	result, err := s.service.RollbackUpload(ctx, uploadID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, result.Error)
		return
	}

	writeJSON(w, result)
}

// handleUploadHistory returns the upload history for a table as HTML.
func (s *Server) handleUploadHistory(w http.ResponseWriter, r *http.Request) {
	tableKey := chi.URLParam(r, "tableKey")
	if tableKey == "" {
		writeError(w, http.StatusBadRequest, "missing table key")
		return
	}

	history, err := s.service.GetUploadHistory(r.Context(), tableKey)
	if err != nil {
		// Return empty history on error
		history = nil
	}

	templates.UploadHistory(history).Render(r.Context(), w)
}

// handleDownloadTemplate returns a CSV template with headers for a table.
func (s *Server) handleDownloadTemplate(w http.ResponseWriter, r *http.Request) {
	tableKey := chi.URLParam(r, "tableKey")
	if tableKey == "" {
		writeError(w, http.StatusBadRequest, "missing table key")
		return
	}

	def, ok := core.Get(tableKey)
	if !ok {
		writeError(w, http.StatusNotFound, "table not found")
		return
	}

	// Set headers for CSV download
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s_template.csv"`, tableKey))

	// Write CSV with just headers
	csvWriter := csv.NewWriter(w)
	csvWriter.Write(def.Info.Columns)
	csvWriter.Flush()
}

// handleExportData exports table data as a CSV file.
// Respects search and column filters if provided.
func (s *Server) handleExportData(w http.ResponseWriter, r *http.Request) {
	tableKey := chi.URLParam(r, "tableKey")
	if tableKey == "" {
		writeError(w, http.StatusBadRequest, "missing table key")
		return
	}

	def, ok := core.Get(tableKey)
	if !ok {
		writeError(w, http.StatusNotFound, "table not found")
		return
	}

	// Parse search filter (if any)
	search := r.URL.Query().Get("search")
	filters := parseFilters(r, def)

	// Fetch data (filtered if search or filters provided)
	data, err := s.service.GetAllTableData(r.Context(), tableKey, search, filters)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Set CSV download headers with timestamp
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("%s_%s.csv", tableKey, timestamp)
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))

	// Write CSV
	csvWriter := csv.NewWriter(w)

	// Header row (display names)
	csvWriter.Write(def.Info.Columns)

	// Data rows (formatted values)
	for _, row := range data.Rows {
		record := make([]string, len(def.Info.Columns))
		for i, col := range def.Info.Columns {
			record[i] = formatCellForExport(row[col])
		}
		csvWriter.Write(record)
	}

	csvWriter.Flush()
}

// handleExportFailedRows exports failed rows from an upload as CSV.
func (s *Server) handleExportFailedRows(w http.ResponseWriter, r *http.Request) {
	uploadID := chi.URLParam(r, "uploadID")
	if uploadID == "" {
		writeError(w, http.StatusBadRequest, "missing upload ID")
		return
	}

	// Get upload info with headers
	upload, err := s.service.GetUploadWithHeaders(r.Context(), uploadID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Check if headers are available (legacy uploads may not have them)
	if len(upload.CsvHeaders) == 0 {
		writeError(w, http.StatusNotFound, "export unavailable for legacy uploads")
		return
	}

	// Get failed rows
	failedRows, err := s.service.GetFailedRows(r.Context(), uploadID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Set CSV download headers
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("failed_rows_%s.csv", timestamp)
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))

	// Write CSV
	csvWriter := csv.NewWriter(w)

	// Header row: _line, _error, ...original columns
	header := append([]string{"_line", "_error"}, upload.CsvHeaders...)
	csvWriter.Write(header)

	// Data rows
	for _, row := range failedRows {
		record := append([]string{
			strconv.Itoa(int(row.LineNumber)),
			row.Reason,
		}, row.RowData...)
		csvWriter.Write(record)
	}

	csvWriter.Flush()
}

// formatCellForExport formats a cell value for CSV export.
// Uses same logic as table view for consistency.
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
		// Format with appropriate precision
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

// handleTableView renders the table data view page.
func (s *Server) handleTableView(w http.ResponseWriter, r *http.Request) {
	tableKey := chi.URLParam(r, "tableKey")
	if tableKey == "" {
		writeError(w, http.StatusBadRequest, "missing table key")
		return
	}

	def, ok := core.Get(tableKey)
	if !ok {
		writeError(w, http.StatusNotFound, "table not found")
		return
	}

	// Parse query parameters
	page := parseIntParam(r, "page", 1)
	sorts := parseSorts(r)
	search := r.URL.Query().Get("search")
	filters := parseFilters(r, def)

	// Fetch data
	data, err := s.service.GetTableData(r.Context(), tableKey, page, core.DefaultPageSize, sorts, search, filters)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Build column metadata for frontend inline editing
	columnMeta := buildColumnMeta(def)

	// Check if HTMX request (partial update) or full page load
	if r.Header.Get("HX-Request") == "true" {
		templates.TablePartial(tableKey, def.Info, data, columnMeta).Render(r.Context(), w)
	} else {
		templates.TableView(tableKey, def.Info, data, columnMeta).Render(r.Context(), w)
	}
}

// buildColumnMeta builds column metadata from a table definition.
func buildColumnMeta(def core.TableDefinition) []templates.ColumnMeta {
	// Create a set of unique key columns for quick lookup
	uniqueKeySet := make(map[string]bool)
	for _, col := range def.Info.UniqueKey {
		uniqueKeySet[col] = true
	}

	// Build a map from display name to field spec
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

		// Get field spec if exists
		if spec, ok := specMap[col]; ok {
			cm.DBColumn = spec.DBColumn
			if cm.DBColumn == "" {
				cm.DBColumn = col
			}
			cm.Type = fieldTypeToString(spec.Type)
			cm.EnumValues = spec.EnumValues
			cm.AllowEmpty = spec.AllowEmpty
		} else {
			// Default to text
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
// Format: ?sort=Column1,Column2&dir=asc,desc
// Returns up to 2 sort specifications.
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
		if len(sorts) >= 2 { // Max 2 sort levels
			break
		}
	}
	return sorts
}

// parseFilters extracts column filters from URL query parameters.
// Format: filter[column_name]=operator:value
// Example: filter[Amount]=gte:1000
func parseFilters(r *http.Request, def core.TableDefinition) core.FilterSet {
	var filters []core.ColumnFilter

	// Build a map from display name to field spec for quick lookup
	specMap := make(map[string]core.FieldSpec)
	for _, spec := range def.FieldSpecs {
		specMap[strings.ToLower(spec.Name)] = spec
	}

	for key, values := range r.URL.Query() {
		// Check for filter[column] format
		if !strings.HasPrefix(key, "filter[") || !strings.HasSuffix(key, "]") {
			continue
		}

		// Extract column name
		colName := key[7 : len(key)-1]
		if colName == "" {
			continue
		}

		// Find the column in the table definition
		spec, ok := specMap[strings.ToLower(colName)]
		if !ok {
			continue
		}

		// Parse each value (multiple filters on same column allowed for ranges)
		for _, val := range values {
			// Parse "operator:value" format
			parts := strings.SplitN(val, ":", 2)
			if len(parts) != 2 {
				continue
			}

			op := core.FilterOperator(parts[0])
			filterVal := parts[1]

			// Skip empty filter values
			if filterVal == "" {
				continue
			}

			// Validate operator for column type
			if !isValidOperator(op, spec.Type) {
				continue
			}

			// Determine DB column name
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

// handleCheckDuplicates checks if provided keys already exist in the database.
func (s *Server) handleCheckDuplicates(w http.ResponseWriter, r *http.Request) {
	tableKey := chi.URLParam(r, "tableKey")
	if tableKey == "" {
		writeError(w, http.StatusBadRequest, "missing table key")
		return
	}

	// Parse request body
	var req struct {
		Keys []string `json:"keys"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if len(req.Keys) == 0 {
		writeJSON(w, map[string]interface{}{
			"existing": []string{},
			"count":    0,
		})
		return
	}

	// Check for duplicates
	existing, err := s.service.CheckDuplicates(r.Context(), tableKey, req.Keys)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, map[string]interface{}{
		"existing": existing,
		"count":    len(existing),
	})
}

// handleDeleteRows deletes multiple rows by their unique key values.
func (s *Server) handleDeleteRows(w http.ResponseWriter, r *http.Request) {
	tableKey := chi.URLParam(r, "tableKey")
	if tableKey == "" {
		writeError(w, http.StatusBadRequest, "missing table key")
		return
	}

	// Parse request body
	var req struct {
		Keys []string `json:"keys"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if len(req.Keys) == 0 {
		writeError(w, http.StatusBadRequest, "no rows specified")
		return
	}

	// Delete rows
	deleted, err := s.service.DeleteRows(r.Context(), tableKey, req.Keys)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, map[string]int{"deleted": deleted})
}

// handleUpdateCell updates a single cell value.
func (s *Server) handleUpdateCell(w http.ResponseWriter, r *http.Request) {
	tableKey := chi.URLParam(r, "tableKey")
	if tableKey == "" {
		writeError(w, http.StatusBadRequest, "missing table key")
		return
	}

	// Parse request body
	var req struct {
		RowKey string `json:"rowKey"`
		Column string `json:"column"`
		Value  string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.RowKey == "" || req.Column == "" {
		writeError(w, http.StatusBadRequest, "rowKey and column are required")
		return
	}

	// Update cell
	result, err := s.service.UpdateCell(r.Context(), tableKey, core.UpdateCellRequest{
		RowKey: req.RowKey,
		Column: req.Column,
		Value:  req.Value,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, result)
}

// handleBulkEdit updates a single column across multiple selected rows.
func (s *Server) handleBulkEdit(w http.ResponseWriter, r *http.Request) {
	tableKey := chi.URLParam(r, "tableKey")
	if tableKey == "" {
		writeError(w, http.StatusBadRequest, "missing table key")
		return
	}

	var req struct {
		Keys   []string `json:"keys"`
		Column string   `json:"column"`
		Value  string   `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if len(req.Keys) == 0 {
		writeError(w, http.StatusBadRequest, "no rows specified")
		return
	}

	if req.Column == "" {
		writeError(w, http.StatusBadRequest, "column is required")
		return
	}

	ctx := WithRequestMetadata(r.Context(), r)
	result, err := s.service.BulkEditRows(ctx, tableKey, core.BulkEditRequest{
		Keys:   req.Keys,
		Column: req.Column,
		Value:  req.Value,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, result)
}

// handleListTemplates returns all import templates for a table.
func (s *Server) handleListTemplates(w http.ResponseWriter, r *http.Request) {
	tableKey := chi.URLParam(r, "tableKey")
	if tableKey == "" {
		writeError(w, http.StatusBadRequest, "missing table key")
		return
	}

	templates, err := s.service.ListTemplates(r.Context(), tableKey)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(templates)
}

// handleMatchTemplates finds templates matching the provided CSV headers.
func (s *Server) handleMatchTemplates(w http.ResponseWriter, r *http.Request) {
	tableKey := chi.URLParam(r, "tableKey")
	if tableKey == "" {
		writeError(w, http.StatusBadRequest, "missing table key")
		return
	}

	// Parse headers from query string (comma-separated)
	headersStr := r.URL.Query().Get("headers")
	if headersStr == "" {
		writeError(w, http.StatusBadRequest, "missing headers parameter")
		return
	}

	headers := strings.Split(headersStr, ",")
	for i := range headers {
		headers[i] = strings.TrimSpace(headers[i])
	}

	matches, err := s.service.MatchTemplates(r.Context(), tableKey, headers)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(matches)
}

// handleGetTemplate returns a single import template by ID.
func (s *Server) handleGetTemplate(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing template id")
		return
	}

	template, err := s.service.GetTemplate(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(template)
}

// handleCreateTemplate creates a new import template.
func (s *Server) handleCreateTemplate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TableKey      string         `json:"tableKey"`
		Name          string         `json:"name"`
		ColumnMapping map[string]int `json:"columnMapping"`
		CSVHeaders    []string       `json:"csvHeaders"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.TableKey == "" || req.Name == "" {
		writeError(w, http.StatusBadRequest, "tableKey and name are required")
		return
	}

	if len(req.ColumnMapping) == 0 {
		writeError(w, http.StatusBadRequest, "columnMapping is required")
		return
	}

	ctx := WithRequestMetadata(r.Context(), r)
	template, err := s.service.CreateTemplate(ctx, req.TableKey, req.Name, req.ColumnMapping, req.CSVHeaders)
	if err != nil {
		// Check for duplicate name error
		if strings.Contains(err.Error(), "duplicate") || strings.Contains(err.Error(), "unique") {
			writeError(w, http.StatusConflict, "template name already exists")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(template)
}

// handleUpdateTemplate updates an existing import template.
func (s *Server) handleUpdateTemplate(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing template id")
		return
	}

	var req struct {
		Name          string         `json:"name"`
		ColumnMapping map[string]int `json:"columnMapping"`
		CSVHeaders    []string       `json:"csvHeaders"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}

	ctx := WithRequestMetadata(r.Context(), r)
	template, err := s.service.UpdateTemplate(ctx, id, req.Name, req.ColumnMapping, req.CSVHeaders)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(template)
}

// handleDeleteTemplate deletes an import template.
func (s *Server) handleDeleteTemplate(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing template id")
		return
	}

	ctx := WithRequestMetadata(r.Context(), r)
	if err := s.service.DeleteTemplate(ctx, id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"deleted"}`))
}

// handleAuditLog renders the audit log page with filtering and pagination.
func (s *Server) handleAuditLog(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters
	page := parseIntParam(r, "page", 1)
	pageSize := 50

	// Parse filters
	filter := templates.AuditFilter{
		Action:    r.URL.Query().Get("action"),
		TableKey:  r.URL.Query().Get("table"),
		Severity:  r.URL.Query().Get("severity"),
		StartDate: r.URL.Query().Get("from"),
		EndDate:   r.URL.Query().Get("to"),
	}

	// Build core filter
	coreFilter := core.AuditLogFilter{
		TableKey: filter.TableKey,
		Action:   core.AuditAction(filter.Action),
		Severity: filter.Severity,
		Limit:    pageSize,
		Offset:   (page - 1) * pageSize,
	}

	// Parse date filters
	if filter.StartDate != "" {
		if t, err := time.Parse("2006-01-02", filter.StartDate); err == nil {
			coreFilter.StartTime = t
		}
	}
	if filter.EndDate != "" {
		if t, err := time.Parse("2006-01-02", filter.EndDate); err == nil {
			// End of day
			coreFilter.EndTime = t.Add(24*time.Hour - time.Second)
		}
	}

	// Fetch entries
	entries, err := s.service.GetAuditLog(r.Context(), coreFilter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Get total count
	totalCount, err := s.service.CountAuditLog(r.Context(), coreFilter)
	if err != nil {
		totalCount = int64(len(entries))
	}

	// Get table list for filter dropdown
	tables := make([]string, 0)
	for _, def := range core.All() {
		tables = append(tables, def.Info.Key)
	}

	// Build view params
	params := templates.AuditLogViewParams{
		Entries:    entries,
		TotalCount: totalCount,
		Page:       page,
		PageSize:   pageSize,
		TotalPages: int((totalCount + int64(pageSize) - 1) / int64(pageSize)),
		Filter:     filter,
		Tables:     tables,
	}

	// Render
	if r.Header.Get("HX-Request") == "true" {
		templates.AuditLogPartial(params).Render(r.Context(), w)
	} else {
		templates.AuditLogPage(params).Render(r.Context(), w)
	}
}

// handleAuditLogEntry returns the detail view for a single audit entry.
func (s *Server) handleAuditLogEntry(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing audit log id")
		return
	}

	entry, err := s.service.GetAuditLogByID(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "audit entry not found")
		return
	}

	templates.AuditEntryDetail(*entry).Render(r.Context(), w)
}

// handleAuditLogExport exports audit log entries as a CSV file.
func (s *Server) handleAuditLogExport(w http.ResponseWriter, r *http.Request) {
	// Parse filters (same as handleAuditLog)
	filter := core.AuditLogFilter{
		TableKey: r.URL.Query().Get("table"),
		Action:   core.AuditAction(r.URL.Query().Get("action")),
		Severity: r.URL.Query().Get("severity"),
	}

	// Parse date filters
	if from := r.URL.Query().Get("from"); from != "" {
		if t, err := time.Parse("2006-01-02", from); err == nil {
			filter.StartTime = t
		}
	}
	if to := r.URL.Query().Get("to"); to != "" {
		if t, err := time.Parse("2006-01-02", to); err == nil {
			filter.EndTime = t.Add(24*time.Hour - time.Second)
		}
	}

	// Fetch all matching entries
	entries, err := s.service.GetAuditLogForExport(r.Context(), filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Set CSV download headers
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("audit_log_%s.csv", timestamp)
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))

	// Write CSV
	csvWriter := csv.NewWriter(w)

	// Header row
	csvWriter.Write([]string{
		"ID", "Timestamp", "Action", "Severity", "Table",
		"User Email", "User Name", "IP Address",
		"Row Key", "Column", "Old Value", "New Value",
		"Rows Affected", "Upload ID", "Reason",
	})

	// Data rows
	for _, e := range entries {
		csvWriter.Write([]string{
			e.ID,
			e.CreatedAt.Format("2006-01-02 15:04:05"),
			string(e.Action),
			string(e.Severity),
			e.TableKey,
			e.UserEmail,
			e.UserName,
			e.IPAddress,
			e.RowKey,
			e.ColumnName,
			e.OldValue,
			e.NewValue,
			fmt.Sprintf("%d", e.RowsAffected),
			e.UploadID,
			e.Reason,
		})
	}

	csvWriter.Flush()
}

// handleUploadDetail renders the upload detail page showing inserted/skipped rows.
func (s *Server) handleUploadDetail(w http.ResponseWriter, r *http.Request) {
	uploadID := chi.URLParam(r, "uploadID")
	if uploadID == "" {
		writeError(w, http.StatusBadRequest, "missing upload ID")
		return
	}

	// Get upload details
	upload, err := s.service.GetUploadDetail(r.Context(), uploadID)
	if err != nil {
		writeError(w, http.StatusNotFound, "upload not found")
		return
	}

	// Get table definition for column names
	def, ok := core.Get(upload.TableKey)
	if !ok {
		writeError(w, http.StatusInternalServerError, "unknown table")
		return
	}

	// Parse query params
	status := r.URL.Query().Get("status") // "all", "inserted", "skipped"
	if status == "" {
		status = "inserted"
	}
	page := parseIntParam(r, "page", 1)
	pageSize := 50

	// Build view params
	params := templates.UploadDetailParams{
		Upload:     upload,
		Columns:    def.Info.Columns,
		CsvHeaders: upload.CsvHeaders,
		Status:     status,
		Page:       page,
		PageSize:   pageSize,
	}

	// Fetch rows based on status filter
	switch status {
	case "skipped":
		failedRows, totalFailed, err := s.service.GetUploadFailedRowsPaginated(r.Context(), uploadID, page, pageSize)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		params.FailedRows = failedRows
		params.TotalRows = totalFailed
		params.TotalPages = int((totalFailed + int64(pageSize) - 1) / int64(pageSize))
	default: // "inserted" or "all"
		result, err := s.service.GetUploadInsertedRows(r.Context(), uploadID, upload.TableKey, page, pageSize)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		params.Rows = result.Rows
		params.TotalRows = result.TotalRows
		params.TotalPages = result.TotalPages
	}

	// Render
	if r.Header.Get("HX-Request") == "true" {
		templates.UploadDetailPartial(params).Render(r.Context(), w)
	} else {
		templates.UploadDetailPage(params).Render(r.Context(), w)
	}
}
