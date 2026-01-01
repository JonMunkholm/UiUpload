package web

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
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

	// Start upload
	uploadID, err := s.service.StartUpload(r.Context(), tableKey, header.Filename, data)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"upload_id": uploadID,
	})
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

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(toResponse(result))
}

// handleReset deletes all data from a specific table.
func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	tableKey := chi.URLParam(r, "tableKey")
	if tableKey == "" {
		writeError(w, http.StatusBadRequest, "missing table key")
		return
	}

	if err := s.service.Reset(r.Context(), tableKey); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"reset"}`))
}

// handleResetAll deletes all data from all tables.
func (s *Server) handleResetAll(w http.ResponseWriter, r *http.Request) {
	if err := s.service.ResetAll(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"reset_all"}`))
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
// Respects search filter if provided.
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

	// Fetch data (filtered if search provided)
	data, err := s.service.GetAllTableData(r.Context(), tableKey, search)
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
	sort := r.URL.Query().Get("sort")
	dir := r.URL.Query().Get("dir")
	search := r.URL.Query().Get("search")

	// Fetch data
	data, err := s.service.GetTableData(r.Context(), tableKey, page, 50, sort, dir, search)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Check if HTMX request (partial update) or full page load
	if r.Header.Get("HX-Request") == "true" {
		templates.TablePartial(tableKey, def.Info, data).Render(r.Context(), w)
	} else {
		templates.TableView(tableKey, def.Info, data).Render(r.Context(), w)
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
