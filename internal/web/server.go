// Package web provides the HTTP server and handlers for the CSV import UI.
package web

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/JonMunkholm/TUI/internal/core"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

//go:embed static
var staticFiles embed.FS

// Server is the HTTP server for the CSV import application.
type Server struct {
	service *core.Service
	router  *chi.Mux
	server  *http.Server
}

// NewServer creates a new Server instance.
func NewServer(service *core.Service) *Server {
	s := &Server{
		service: service,
		router:  chi.NewRouter(),
	}
	s.setupMiddleware()
	s.setupRoutes()
	return s
}

// setupMiddleware configures middleware for all routes.
func (s *Server) setupMiddleware() {
	s.router.Use(middleware.RequestID)
	s.router.Use(middleware.RealIP)
	s.router.Use(middleware.Logger)
	s.router.Use(middleware.Recoverer)
	s.router.Use(middleware.Compress(5))
	s.router.Use(middleware.Timeout(60 * time.Second))

	// Security hardening
	s.router.Use(securityHeaders)

	// Rate limiting: 100 requests per minute per IP
	limiter := newRateLimiter(100, time.Minute)
	s.router.Use(limiter.middleware)
}

// setupRoutes configures all HTTP routes.
func (s *Server) setupRoutes() {
	// Static files (HTMX, Tailwind CSS)
	staticFS, err := fs.Sub(staticFiles, "static")
	if err != nil {
		log.Fatal(err)
	}
	s.router.Handle("/static/*", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS))))

	// Pages
	s.router.Get("/", s.handleDashboard)
	s.router.Get("/table/{tableKey}", s.handleTableView)
	s.router.Get("/upload/{uploadID}", s.handleUploadDetail)
	s.router.Get("/audit-log", s.handleAuditLog)

	// API routes
	s.router.Route("/api", func(r chi.Router) {
		// Table listing
		r.Get("/tables", s.handleListTables)

		// Template download
		r.Get("/template/{tableKey}", s.handleDownloadTemplate)

		// Data export
		r.Get("/export/{tableKey}", s.handleExportData)

		// Upload history
		r.Get("/history/{tableKey}", s.handleUploadHistory)

		// Upload operations
		r.Post("/upload/{tableKey}", s.handleUpload)
		r.Get("/upload/{uploadID}/progress", s.handleUploadProgress)
		r.Get("/upload/{uploadID}/result", s.handleUploadResult)
		r.Post("/upload/{uploadID}/cancel", s.handleCancelUpload)
		r.Get("/upload/{uploadID}/failed-rows", s.handleExportFailedRows)

		// Preview analysis
		r.Post("/preview/{tableKey}", s.handlePreview)

		// Duplicate check
		r.Post("/check-duplicates/{tableKey}", s.handleCheckDuplicates)

		// Delete rows
		r.Post("/delete/{tableKey}", s.handleDeleteRows)

		// Update cell
		r.Post("/update/{tableKey}", s.handleUpdateCell)

		// Bulk edit
		r.Post("/bulk-edit/{tableKey}", s.handleBulkEdit)

		// Audit log
		r.Get("/audit-log/export", s.handleAuditLogExport)
		r.Get("/audit-log/{id}", s.handleAuditLogEntry)

		// Import templates
		r.Get("/import-templates/{tableKey}", s.handleListTemplates)
		r.Get("/import-templates/{tableKey}/match", s.handleMatchTemplates)
		r.Get("/import-template/{id}", s.handleGetTemplate)
		r.Post("/import-template", s.handleCreateTemplate)
		r.Put("/import-template/{id}", s.handleUpdateTemplate)
		r.Delete("/import-template/{id}", s.handleDeleteTemplate)

		// Reset operations
		r.Post("/reset/{tableKey}", s.handleReset)
		r.Post("/reset", s.handleResetAll)

		// Rollback operation
		r.Post("/rollback/{uploadID}", s.handleRollbackUpload)
	})
}

// Start begins listening for HTTP requests.
func (s *Server) Start(addr string) error {
	s.server = &http.Server{
		Addr:         addr,
		Handler:      s.router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 0, // Disabled for SSE
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("Starting server on %s", addr)
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
		ip := r.RemoteAddr
		// Use X-Real-IP if set (by RealIP middleware)
		if realIP := r.Header.Get("X-Real-IP"); realIP != "" {
			ip = realIP
		}

		if !rl.allow(ip) {
			w.Header().Set("Retry-After", "60")
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}

		next.ServeHTTP(w, r)
	})
}

// writeError writes a JSON error response.
// Logs the full error server-side but returns a sanitized message to the client.
func writeError(w http.ResponseWriter, status int, message string) {
	// Log full error for debugging/audit
	log.Printf("HTTP %d: %s", status, message)

	// Sanitize before sending to client
	safeMessage := sanitizeErrorMessage(message)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	fmt.Fprintf(w, `{"error":%q}`, safeMessage)
}

// writeJSON encodes v as JSON and writes it to w.
// Logs encoding errors since headers are already sent.
func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("json encode error: %v", err)
	}
}
