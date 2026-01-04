package web

import (
	"encoding/json"
	"net/http"

	"github.com/JonMunkholm/TUI/internal/core"
	"github.com/go-chi/chi/v5"
)

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

// handleCheckDuplicates checks if provided keys already exist in the database.
func (s *Server) handleCheckDuplicates(w http.ResponseWriter, r *http.Request) {
	tableKey := chi.URLParam(r, "tableKey")
	if tableKey == "" {
		writeError(w, http.StatusBadRequest, "missing table key")
		return
	}

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
