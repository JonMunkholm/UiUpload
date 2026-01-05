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
)

// handleUpload processes a CSV file upload using streaming.
// Memory usage is O(batch_size) constant regardless of file size.
func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	tableKey := chi.URLParam(r, "tableKey")
	if tableKey == "" {
		writeError(w, http.StatusBadRequest, "missing table key")
		return
	}

	maxSize := s.cfg.Upload.MaxFileSize
	r.Body = http.MaxBytesReader(w, r.Body, maxSize)

	if err := r.ParseMultipartForm(maxSize); err != nil {
		writeError(w, http.StatusBadRequest, "file too large or invalid form")
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "no file provided")
		return
	}
	defer file.Close()

	// Parse column mapping if provided
	var mapping map[string]int
	if mappingJSON := r.FormValue("mapping"); mappingJSON != "" {
		if err := json.Unmarshal([]byte(mappingJSON), &mapping); err != nil {
			writeError(w, http.StatusBadRequest, "invalid mapping format")
			return
		}
	}

	// Use streaming upload - pass file directly as io.Reader
	// No io.ReadAll! Memory stays constant at O(batch_size) ~10MB
	ctx := WithRequestMetadata(r.Context(), r)
	uploadID, err := s.service.StartUploadStreaming(ctx, tableKey, header.Filename, file, header.Size, mapping)
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

	maxSize := s.cfg.Upload.MaxFileSize
	r.Body = http.MaxBytesReader(w, r.Body, maxSize)

	if err := r.ParseMultipartForm(maxSize); err != nil {
		writeError(w, http.StatusBadRequest, "file too large or invalid form")
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "no file provided")
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to read file")
		return
	}

	var mapping map[string]int
	if mappingJSON := r.FormValue("mapping"); mappingJSON != "" {
		if err := json.Unmarshal([]byte(mappingJSON), &mapping); err != nil {
			writeError(w, http.StatusBadRequest, "invalid mapping format")
			return
		}
	}

	result, err := s.service.AnalyzeUpload(r.Context(), tableKey, data, mapping)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	writeJSON(w, result)
}

// handleUploadProgress streams upload progress via Server-Sent Events.
// Supports resumption via lastEventId query parameter for reconnection.
func (s *Server) handleUploadProgress(w http.ResponseWriter, r *http.Request) {
	uploadID := chi.URLParam(r, "uploadID")
	if uploadID == "" {
		writeError(w, http.StatusBadRequest, "missing upload ID")
		return
	}

	// Support resumption from last event ID
	// The event ID is the progress percentage, allowing clients to skip
	// already-received events after reconnection
	lastEventIDStr := r.URL.Query().Get("lastEventId")
	var lastEventID int
	if lastEventIDStr != "" {
		lastEventID, _ = strconv.Atoi(lastEventIDStr)
	}

	progressCh, err := s.service.SubscribeProgress(uploadID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	// Track event ID for resumption support
	// Using progress percentage as event ID provides natural deduplication
	eventID := lastEventID

	for {
		select {
		case progress, ok := <-progressCh:
			if !ok {
				// Channel closed - upload complete or cancelled
				fmt.Fprintf(w, "event: complete\ndata: {}\n\n")
				flusher.Flush()
				return
			}

			// Calculate current progress percentage for event ID
			currentPercent := progress.Percent()

			// Skip events that were already sent (for resumption)
			// Only skip if we have a lastEventId and current is not greater
			if lastEventIDStr != "" && currentPercent <= lastEventID {
				continue
			}

			eventID = currentPercent
			data, _ := json.Marshal(progress)

			// Include event ID for client-side tracking and resumption
			fmt.Fprintf(w, "id: %d\nevent: progress\ndata: %s\n\n", eventID, data)
			flusher.Flush()

		case <-r.Context().Done():
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

// handleUploadHistory returns the upload history for a table as HTML.
func (s *Server) handleUploadHistory(w http.ResponseWriter, r *http.Request) {
	tableKey := chi.URLParam(r, "tableKey")
	if tableKey == "" {
		writeError(w, http.StatusBadRequest, "missing table key")
		return
	}

	history, err := s.service.GetUploadHistory(r.Context(), tableKey)
	if err != nil {
		history = nil
	}

	templates.UploadHistory(history).Render(r.Context(), w)
}

// handleExportFailedRows exports failed rows from an upload as CSV.
func (s *Server) handleExportFailedRows(w http.ResponseWriter, r *http.Request) {
	uploadID := chi.URLParam(r, "uploadID")
	if uploadID == "" {
		writeError(w, http.StatusBadRequest, "missing upload ID")
		return
	}

	upload, err := s.service.GetUploadWithHeaders(r.Context(), uploadID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	if len(upload.CsvHeaders) == 0 {
		writeError(w, http.StatusNotFound, "export unavailable for legacy uploads")
		return
	}

	failedRows, err := s.service.GetFailedRows(r.Context(), uploadID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("failed_rows_%s.csv", timestamp)
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))

	csvWriter := csv.NewWriter(w)

	header := append([]string{"_line", "_error"}, upload.CsvHeaders...)
	csvWriter.Write(header)

	for _, row := range failedRows {
		record := append([]string{
			strconv.Itoa(int(row.LineNumber)),
			row.Reason,
		}, row.RowData...)
		csvWriter.Write(record)
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

	upload, err := s.service.GetUploadDetail(r.Context(), uploadID)
	if err != nil {
		writeError(w, http.StatusNotFound, "upload not found")
		return
	}

	def, ok := core.Get(upload.TableKey)
	if !ok {
		writeError(w, http.StatusInternalServerError, "unknown table")
		return
	}

	status := r.URL.Query().Get("status")
	if status == "" {
		status = "inserted"
	}
	page := parseIntParam(r, "page", 1)
	pageSize := 50

	params := templates.UploadDetailParams{
		Upload:     upload,
		Columns:    def.Info.Columns,
		CsvHeaders: upload.CsvHeaders,
		Status:     status,
		Page:       page,
		PageSize:   pageSize,
	}

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
	default:
		result, err := s.service.GetUploadInsertedRows(r.Context(), uploadID, upload.TableKey, page, pageSize)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		params.Rows = result.Rows
		params.TotalRows = result.TotalRows
		params.TotalPages = result.TotalPages
	}

	if r.Header.Get("HX-Request") == "true" {
		templates.UploadDetailPartial(params).Render(r.Context(), w)
	} else {
		sidebar := templates.SidebarParams{}
		templates.UploadDetailPage(sidebar, params).Render(r.Context(), w)
	}
}
