package web

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
)

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
