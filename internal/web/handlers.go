package web

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/JonMunkholm/TUI/internal/core"
	"github.com/JonMunkholm/TUI/internal/web/templates"
	"github.com/go-chi/chi/v5"
)

// MaxUploadSize is the maximum allowed file size (100MB).
const MaxUploadSize = 100 * 1024 * 1024

// handleDashboard renders the main dashboard page.
func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	// Build table groups from registry
	var groups []templates.TableGroup
	for _, groupName := range core.Groups() {
		tables := core.ByGroup(groupName)
		infos := make([]core.TableInfo, len(tables))
		for i, def := range tables {
			infos[i] = def.Info
		}
		groups = append(groups, templates.TableGroup{
			Name:   groupName,
			Tables: infos,
		})
	}

	templates.Dashboard(groups).Render(r.Context(), w)
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
