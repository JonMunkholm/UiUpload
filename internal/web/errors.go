package web

// errors.go provides unified error response handling for the web layer.
//
// It ensures all errors are:
//   - Logged with full technical details for debugging (server-side)
//   - Returned to clients as user-friendly messages with action suggestions
//   - Formatted appropriately based on request type (HTMX, JSON, or HTML)
//
// The error flow:
//  1. Handler encounters an error
//  2. Calls respondError(w, r, err, statusCode)
//  3. Error is mapped via core.MapError to get user-friendly message
//  4. Technical error + context is logged with request ID for correlation
//  5. User message is rendered in appropriate format for the client

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/JonMunkholm/TUI/internal/core"
	"github.com/JonMunkholm/TUI/internal/web/templates"
	"github.com/go-chi/chi/v5/middleware"
)

// ErrorResponse represents the JSON structure for API error responses.
// Includes both machine-readable (Code) and human-readable (Message, Action) fields.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	Action  string `json:"action,omitempty"`
	Code    string `json:"code"`
}

// respondError handles error responses with user-friendly messages.
// It logs the technical error server-side and returns an appropriate response
// based on the request type (HTMX, JSON, or HTML).
func (s *Server) respondError(w http.ResponseWriter, r *http.Request, err error, statusCode int) {
	userMsg := core.MapError(err)

	// Get request ID for correlation
	requestID := middleware.GetReqID(r.Context())

	// Log the technical error with context
	slog.Error("request error",
		"path", r.URL.Path,
		"method", r.Method,
		"status", statusCode,
		"error", err.Error(),
		"code", userMsg.Code,
		"request_id", requestID,
	)

	// Return user-friendly error based on request type
	if isHTMX(r) {
		s.renderErrorPartial(w, userMsg, statusCode)
	} else if wantsJSON(r) {
		respondErrorJSON(w, userMsg, statusCode)
	} else {
		respondErrorHTML(w, userMsg, statusCode)
	}
}

// respondErrorJSON writes a JSON error response.
func respondErrorJSON(w http.ResponseWriter, msg core.UserMessage, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{
		Error:   msg.Message,
		Message: msg.Message,
		Action:  msg.Action,
		Code:    msg.Code,
	})
}

// respondErrorHTML writes a plain HTML error response.
func respondErrorHTML(w http.ResponseWriter, msg core.UserMessage, statusCode int) {
	http.Error(w, msg.Message+" ("+msg.Code+")", statusCode)
}

// renderErrorPartial renders an HTMX-compatible error fragment.
func (s *Server) renderErrorPartial(w http.ResponseWriter, msg core.UserMessage, statusCode int) {
	// Set appropriate status code and headers for HTMX
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(statusCode)

	// Render the error template partial
	templates.ErrorAlert(msg.Message, msg.Action, msg.Code).Render(nil, w)
}

// isHTMX checks if the request is an HTMX request.
func isHTMX(r *http.Request) bool {
	return r.Header.Get("HX-Request") == "true"
}

// wantsJSON checks if the client prefers JSON response.
func wantsJSON(r *http.Request) bool {
	accept := r.Header.Get("Accept")
	contentType := r.Header.Get("Content-Type")

	// Check Accept header
	if strings.Contains(accept, "application/json") {
		return true
	}

	// Check if request is sending JSON
	if strings.Contains(contentType, "application/json") {
		return true
	}

	// API routes default to JSON
	if strings.HasPrefix(r.URL.Path, "/api/") {
		return true
	}

	return false
}
