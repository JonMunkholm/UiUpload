// Package web provides the HTTP server and handlers for the CSV import UI.
package web

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/JonMunkholm/TUI/internal/config"
	"github.com/JonMunkholm/TUI/internal/core"
	"github.com/JonMunkholm/TUI/internal/logging"
	mw "github.com/JonMunkholm/TUI/internal/web/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

//go:embed static
var staticFiles embed.FS

// Server is the HTTP server for the CSV import application.
type Server struct {
	service *core.Service
	cfg     *config.Config
	router  *chi.Mux
	server  *http.Server
}

// NewServer creates a new Server instance with the given configuration.
func NewServer(service *core.Service, cfg *config.Config) *Server {
	s := &Server{
		service: service,
		cfg:     cfg,
		router:  chi.NewRouter(),
	}
	s.setupMiddleware()
	s.setupRoutes()
	return s
}

// setupMiddleware configures middleware for all routes.
// Note: Timeout middleware is applied per-route to avoid killing SSE/streaming endpoints.
func (s *Server) setupMiddleware() {
	s.router.Use(middleware.RequestID)

	// Only trust X-Real-IP/X-Forwarded-For headers from configured trusted proxies.
	// This prevents IP spoofing by untrusted clients sending fake headers.
	if len(s.cfg.Security.TrustedProxies) > 0 {
		s.router.Use(mw.TrustedRealIP(s.cfg.Security.TrustedProxies))
	}
	// If no trusted proxies configured, RemoteAddr is used as-is (direct connection)

	s.router.Use(mw.Logger) // Structured logging with request ID
	s.router.Use(middleware.Recoverer)
	s.router.Use(middleware.Compress(5))
	// Note: Timeout is applied per-route in setupRoutes() to avoid killing SSE streams

	// Security hardening
	if s.cfg.Security.EnableCSP {
		s.router.Use(securityHeaders)
	}

	// Rate limiting (configurable)
	if s.cfg.Rate.Enabled {
		limiter := newRateLimiter(s.cfg.Rate.RequestsPerMinute, time.Minute)
		s.router.Use(limiter.middleware)
	}
}

// setupRoutes configures all HTTP routes.
//
// =============================================================================
// API DOCUMENTATION
// =============================================================================
//
// Page Routes (HTML)
// ------------------
// These endpoints return full HTML pages or HTMX partials (based on HX-Request header).
//
//   GET  /                         Dashboard showing all tables grouped by category
//                                  Response: HTML page with table cards showing row counts and last upload times
//
//   GET  /table/{tableKey}         View table data with pagination, sorting, filtering
//                                  Query params:
//                                    - page         (int)    Page number, default 1
//                                    - sort         (string) Column name(s) to sort by, comma-separated (max 2)
//                                    - dir          (string) Sort direction(s): "asc" or "desc", comma-separated
//                                    - search       (string) Full-text search across all columns
//                                    - filter[col]  (string) Column filter in format "operator:value"
//                                                   Operators by type:
//                                                     text:    contains, equals, starts_with, ends_with
//                                                     numeric: equals, gte, lte, gt, lt
//                                                     date:    equals, gte, lte
//                                                     bool:    equals
//                                                     enum:    equals, in
//                                  Response: HTML page (full) or table partial (HTMX)
//
//   GET  /upload/{uploadID}        View upload detail page showing inserted/skipped rows
//                                  Query params:
//                                    - status   (string) "inserted" (default) or "skipped"
//                                    - page     (int)    Page number, default 1
//                                  Response: HTML page with row traceability
//
//   GET  /audit-log                View audit log with filtering and pagination
//                                  Query params:
//                                    - page     (int)    Page number, default 1
//                                    - action   (string) Filter by action type (upload, edit, delete, etc.)
//                                    - table    (string) Filter by table key
//                                    - severity (string) Filter by severity: info, warning, error
//                                    - from     (string) Start date (YYYY-MM-DD)
//                                    - to       (string) End date (YYYY-MM-DD)
//                                  Response: HTML page (full) or audit log partial (HTMX)
//
// Static Files
// ------------
//   GET  /static/*                 Embedded static assets (HTMX, Tailwind CSS, JS)
//
// =============================================================================
// System API
// =============================================================================
//
//   GET  /api/upload-queue-status  Get current upload queue/limiter status
//                                  Response: { "active": int, "max": int, "queued": int }
//
// =============================================================================
// Table API
// =============================================================================
//
//   GET  /api/tables               List all tables organized by group
//                                  Response: { "groupName": [{ table definitions }], ... }
//
//   GET  /api/template/{tableKey}  Download empty CSV template with correct headers
//                                  Response: CSV file attachment with column headers only
//
//   GET  /api/export/{tableKey}    Export table data as streaming CSV
//                                  Query params:
//                                    - search       (string) Full-text search filter
//                                    - filter[col]  (string) Column filters (same format as table view)
//                                  Response: Streaming CSV file attachment
//                                  Note: Uses chunked transfer encoding for large datasets
//
// =============================================================================
// Upload API
// =============================================================================
//
//   GET  /api/history/{tableKey}   Get upload history for a table
//                                  Response: HTML partial showing recent uploads
//
//   POST /api/upload/{tableKey}    Upload CSV file for import
//                                  Content-Type: multipart/form-data
//                                  Form fields:
//                                    - file     (file)   CSV file (max 100MB)
//                                    - mapping  (string) Optional JSON column mapping: { "dbColumn": csvIndex }
//                                  Response: { "upload_id": "uuid" }
//                                  Note: Returns immediately; use progress endpoint to track
//
//   GET  /api/upload/{uploadID}/progress
//                                  SSE stream for real-time upload progress
//                                  Query params:
//                                    - lastEventId (int) Resume from this progress percentage
//                                  Response: Server-Sent Events stream
//                                    - event: progress, data: { "processed": int, "total": int, "inserted": int, "skipped": int }
//                                    - event: complete, data: {}
//                                  Headers: Content-Type: text/event-stream
//
//   GET  /api/upload/{uploadID}/result
//                                  Get final upload result after completion
//                                  Response: {
//                                    "upload_id": "uuid",
//                                    "table_key": "string",
//                                    "file_name": "string",
//                                    "total_rows": int,
//                                    "inserted": int,
//                                    "skipped": int,
//                                    "failed_rows": [{ "line": int, "reason": "string", "data": [...] }],
//                                    "duration": "1.5s",
//                                    "error": "string" (optional)
//                                  }
//
//   POST /api/upload/{uploadID}/cancel
//                                  Cancel an in-progress upload
//                                  Response: { "status": "cancelled" }
//
//   GET  /api/upload/{uploadID}/failed-rows
//                                  Export failed rows from an upload as CSV
//                                  Response: CSV file with columns: _line, _error, [original columns...]
//                                  Note: Only available for uploads with stored CSV headers
//
// =============================================================================
// Preview API
// =============================================================================
//
//   POST /api/preview/{tableKey}   Analyze CSV file before upload
//                                  Content-Type: multipart/form-data
//                                  Form fields:
//                                    - file     (file)   CSV file to analyze
//                                    - mapping  (string) Optional JSON column mapping
//                                  Response: {
//                                    "total_rows": int,
//                                    "valid_rows": int,
//                                    "duplicate_rows": int,
//                                    "invalid_rows": int,
//                                    "column_mapping": { "dbColumn": csvIndex },
//                                    "unmapped_columns": ["col1", "col2"],
//                                    "sample_errors": [{ "line": int, "reason": "string" }]
//                                  }
//
// =============================================================================
// Duplicate Check API
// =============================================================================
//
//   POST /api/check-duplicates/{tableKey}
//                                  Check if keys already exist in the database
//                                  Request body: { "keys": ["key1", "key2", ...] }
//                                  Response: { "existing": ["key1"], "count": 1 }
//
// =============================================================================
// Mutation API
// =============================================================================
//
//   POST /api/delete/{tableKey}    Delete multiple rows by unique key
//                                  Request body: { "keys": ["key1", "key2", ...] }
//                                  Response: { "deleted": int }
//
//   POST /api/update/{tableKey}    Update a single cell value
//                                  Request body: {
//                                    "rowKey": "string",   // Unique key value identifying the row
//                                    "column": "string",   // Column name to update
//                                    "value": "string"     // New value
//                                  }
//                                  Response: {
//                                    "success": bool,
//                                    "old_value": "string",
//                                    "new_value": "string"
//                                  }
//
//   POST /api/bulk-edit/{tableKey} Update a column across multiple rows
//                                  Request body: {
//                                    "keys": ["key1", "key2"],  // Row keys to update
//                                    "column": "string",        // Column to update
//                                    "value": "string"          // New value for all rows
//                                  }
//                                  Response: { "updated": int, "errors": [...] }
//
// =============================================================================
// Reset API
// =============================================================================
//
//   POST /api/reset/{tableKey}     Delete all data from a specific table
//                                  Response: { "status": "reset" }
//                                  Note: Creates audit log entry
//
//   POST /api/reset                Delete all data from ALL tables
//                                  Response: { "status": "reset_all" }
//                                  Note: Creates audit log entries for each table
//
//   POST /api/rollback/{uploadID}  Rollback an upload (delete all rows from that upload)
//                                  Response: {
//                                    "success": bool,
//                                    "deleted": int,
//                                    "error": "string" (optional)
//                                  }
//
// =============================================================================
// Audit API
// =============================================================================
//
//   GET  /api/audit-log/export     Export audit log as streaming CSV
//                                  Query params:
//                                    - action   (string) Filter by action type
//                                    - table    (string) Filter by table key
//                                    - severity (string) Filter by severity
//                                    - from     (string) Start date (YYYY-MM-DD)
//                                    - to       (string) End date (YYYY-MM-DD)
//                                  Response: Streaming CSV file with columns:
//                                    ID, Timestamp, Action, Severity, Table, User Email,
//                                    User Name, IP Address, Row Key, Column, Old Value,
//                                    New Value, Rows Affected, Upload ID, Reason
//
//   GET  /api/audit-log/{id}       Get detail view for a single audit entry
//                                  Response: HTML partial with entry details
//
// =============================================================================
// Import Template API
// =============================================================================
// Templates save column mappings for reuse across uploads with similar CSV formats.
//
//   GET  /api/import-templates/{tableKey}
//                                  List all import templates for a table
//                                  Response: [{ "id": "uuid", "name": "string", "columnMapping": {...}, "csvHeaders": [...] }]
//
//   GET  /api/import-templates/{tableKey}/match
//                                  Find templates matching the provided CSV headers
//                                  Query params:
//                                    - headers (string) Comma-separated list of CSV column headers
//                                  Response: [{ template with match score }]
//
//   GET  /api/import-template/{id} Get a single template by ID
//                                  Response: { "id": "uuid", "tableKey": "string", "name": "string", "columnMapping": {...}, "csvHeaders": [...] }
//
//   POST /api/import-template      Create a new import template
//                                  Request body: {
//                                    "tableKey": "string",
//                                    "name": "string",
//                                    "columnMapping": { "dbColumn": csvIndex },
//                                    "csvHeaders": ["header1", "header2"]
//                                  }
//                                  Response: { created template } (201 Created)
//
//   PUT  /api/import-template/{id} Update an existing template
//                                  Request body: {
//                                    "name": "string",
//                                    "columnMapping": { "dbColumn": csvIndex },
//                                    "csvHeaders": ["header1", "header2"]
//                                  }
//                                  Response: { updated template }
//
//   DELETE /api/import-template/{id}
//                                  Delete an import template
//                                  Response: { "status": "deleted" }
//
// =============================================================================
// Error Response Format
// =============================================================================
// All endpoints return errors in a consistent JSON format:
//
//   {
//     "error": "User-friendly error message",
//     "message": "User-friendly error message",
//     "action": "Suggested action to resolve" (optional),
//     "code": "ERROR_CODE"
//   }
//
// Common HTTP status codes:
//   - 400 Bad Request: Invalid input, missing required fields
//   - 404 Not Found: Resource not found (table, upload, template)
//   - 409 Conflict: Duplicate resource (e.g., template name)
//   - 429 Too Many Requests: Rate limit exceeded (includes Retry-After header)
//   - 500 Internal Server Error: Server-side error
//
// =============================================================================
func (s *Server) setupRoutes() {
	// Static files (HTMX, Tailwind CSS)
	staticFS, err := fs.Sub(staticFiles, "static")
	if err != nil {
		slog.Error("failed to load static files", "error", err)
		os.Exit(1)
	}
	s.router.Handle("/static/*", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS))))

	// Pages (with timeout)
	s.router.Group(func(r chi.Router) {
		r.Use(middleware.Timeout(s.cfg.Server.RequestTimeout))
		r.Get("/", s.handleDashboard)
		r.Get("/table/{tableKey}", s.handleTableView)
		r.Get("/upload/{uploadID}", s.handleUploadDetail)
		r.Get("/audit-log", s.handleAuditLog)
		r.Get("/settings", s.handleSettings)
	})

	// API routes
	s.router.Route("/api", func(r chi.Router) {
		// =================================================================
		// Streaming routes (NO timeout - these can run indefinitely)
		// =================================================================
		// SSE progress stream - stays open until upload completes
		r.Get("/upload/{uploadID}/progress", s.handleUploadProgress)
		// CSV exports - may take time for large datasets
		r.Get("/export/{tableKey}", s.handleExportData)
		r.Get("/audit-log/export", s.handleAuditLogExport)
		r.Get("/upload/{uploadID}/failed-rows", s.handleExportFailedRows)

		// =================================================================
		// Standard API routes (WITH timeout)
		// =================================================================
		r.Group(func(r chi.Router) {
			r.Use(middleware.Timeout(s.cfg.Server.RequestTimeout))

			// System status
			r.Get("/upload-queue-status", s.handleUploadQueueStatus)

			// Table listing
			r.Get("/tables", s.handleListTables)

			// Template download
			r.Get("/template/{tableKey}", s.handleDownloadTemplate)

			// Upload history
			r.Get("/history/{tableKey}", s.handleUploadHistory)

			// Upload operations (with stricter rate limit if configured)
			r.Group(func(r chi.Router) {
				if s.cfg.Rate.Enabled && s.cfg.Rate.UploadLimit > 0 {
					uploadLimiter := newRateLimiter(s.cfg.Rate.UploadLimit, time.Minute)
					r.Use(uploadLimiter.middleware)
				}
				r.Post("/upload/{tableKey}", s.handleUpload)
				r.Post("/preview/{tableKey}", s.handlePreview)
			})

			// Upload read operations (no stricter rate limit)
			r.Get("/upload/{uploadID}/result", s.handleUploadResult)
			r.Post("/upload/{uploadID}/cancel", s.handleCancelUpload)

			// Duplicate check
			r.Post("/check-duplicates/{tableKey}", s.handleCheckDuplicates)

			// Audit log entry detail
			r.Get("/audit-log/{id}", s.handleAuditLogEntry)

			// Import templates (read operations)
			r.Get("/import-templates/{tableKey}", s.handleListTemplates)
			r.Get("/import-templates/{tableKey}/match", s.handleMatchTemplates)
			r.Get("/import-template/{id}", s.handleGetTemplate)
			r.Post("/import-template", s.handleCreateTemplate)

			// =============================================================
			// Destructive operations (protected by API key when enabled)
			// =============================================================
			r.Group(func(r chi.Router) {
				r.Use(mw.APIKeyAuth(&s.cfg.Security))

				// Delete rows
				r.Post("/delete/{tableKey}", s.handleDeleteRows)

				// Update cell
				r.Post("/update/{tableKey}", s.handleUpdateCell)

				// Bulk edit
				r.Post("/bulk-edit/{tableKey}", s.handleBulkEdit)

				// Import template mutations
				r.Put("/import-template/{id}", s.handleUpdateTemplate)
				r.Delete("/import-template/{id}", s.handleDeleteTemplate)

				// Reset operations
				r.Post("/reset/{tableKey}", s.handleReset)
				r.Post("/reset", s.handleResetAll)

				// Rollback operation
				r.Post("/rollback/{uploadID}", s.handleRollbackUpload)
			})
		})
	})
}

// Start begins listening for HTTP requests using configured address.
func (s *Server) Start() error {
	addr := s.cfg.Server.Addr()
	s.server = &http.Server{
		Addr:         addr,
		Handler:      s.router,
		ReadTimeout:  s.cfg.Server.ReadTimeout,
		WriteTimeout: s.cfg.Server.WriteTimeout, // 0 disables for SSE
		IdleTimeout:  s.cfg.Server.IdleTimeout,
	}

	slog.Info("server starting", "addr", addr)
	return s.server.ListenAndServe()
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

// Router returns the underlying chi router for testing.
func (s *Server) Router() *chi.Mux {
	return s.router
}

// securityHeaders adds security headers to all responses.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Prevent MIME type sniffing
		w.Header().Set("X-Content-Type-Options", "nosniff")

		// Prevent clickjacking
		w.Header().Set("X-Frame-Options", "DENY")

		// XSS protection (legacy but still useful for older browsers)
		w.Header().Set("X-XSS-Protection", "1; mode=block")

		// Content Security Policy - restrict resource loading
		// Allow inline styles for Tailwind, scripts from self
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self'")

		// Control referrer information
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")

		next.ServeHTTP(w, r)
	})
}

// rateLimiter implements a simple token bucket rate limiter per IP.
type rateLimiter struct {
	mu       sync.Mutex
	visitors map[string]*visitor
	rate     int           // requests per window
	window   time.Duration // time window
}

type visitor struct {
	tokens    int
	lastReset time.Time
}

// newRateLimiter creates a rate limiter with the specified rate per window.
func newRateLimiter(rate int, window time.Duration) *rateLimiter {
	rl := &rateLimiter{
		visitors: make(map[string]*visitor),
		rate:     rate,
		window:   window,
	}
	// Start cleanup goroutine
	go rl.cleanup()
	return rl
}

// cleanup removes stale visitor entries every minute.
func (rl *rateLimiter) cleanup() {
	for {
		time.Sleep(time.Minute)
		rl.mu.Lock()
		for ip, v := range rl.visitors {
			if time.Since(v.lastReset) > rl.window*2 {
				delete(rl.visitors, ip)
			}
		}
		rl.mu.Unlock()
	}
}

// allow checks if the request should be allowed and consumes a token if so.
func (rl *rateLimiter) allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	v, exists := rl.visitors[ip]
	if !exists {
		rl.visitors[ip] = &visitor{
			tokens:    rl.rate - 1, // consume one token
			lastReset: time.Now(),
		}
		return true
	}

	// Reset tokens if window has passed
	if time.Since(v.lastReset) > rl.window {
		v.tokens = rl.rate - 1
		v.lastReset = time.Now()
		return true
	}

	// Check if we have tokens left
	if v.tokens <= 0 {
		return false
	}

	v.tokens--
	return true
}

// middleware returns an HTTP middleware that rate limits by IP.
func (rl *rateLimiter) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// RemoteAddr is already set by TrustedRealIP middleware (if trusted proxy)
		// or contains the direct connection IP (if no proxy configured)
		if !rl.allow(r.RemoteAddr) {
			w.Header().Set("Retry-After", "60")
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}

		next.ServeHTTP(w, r)
	})
}

// writeError writes a JSON error response with user-friendly messages.
// Logs the full error server-side but returns a mapped user message to the client.
func writeError(w http.ResponseWriter, status int, message string) {
	// Map the technical error to a user-friendly message
	userMsg := core.MapError(fmt.Errorf("%s", message))

	// Log full error for debugging/audit with error code
	slog.Warn("http error",
		"status", status,
		"message", message,
		"code", userMsg.Code,
	)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	// Return user-friendly error with code and action
	resp := struct {
		Error   string `json:"error"`
		Message string `json:"message"`
		Action  string `json:"action,omitempty"`
		Code    string `json:"code"`
	}{
		Error:   userMsg.Message,
		Message: userMsg.Message,
		Action:  userMsg.Action,
		Code:    userMsg.Code,
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("json encode failed", "error", err)
	}
}

// writeJSON encodes v as JSON and writes it to w.
// Logs encoding errors since headers are already sent.
func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("json encode failed", "error", err)
	}
}

// Ensure logging package is used (for side effects in some contexts)
var _ = logging.FromContext
