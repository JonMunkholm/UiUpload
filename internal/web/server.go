// Package web provides the HTTP server and handlers for the CSV import UI.
package web

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log"
	"net/http"
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

	// API routes
	s.router.Route("/api", func(r chi.Router) {
		// Table listing
		r.Get("/tables", s.handleListTables)

		// Template download
		r.Get("/template/{tableKey}", s.handleDownloadTemplate)

		// Upload history
		r.Get("/history/{tableKey}", s.handleUploadHistory)

		// Upload operations
		r.Post("/upload/{tableKey}", s.handleUpload)
		r.Get("/upload/{uploadID}/progress", s.handleUploadProgress)
		r.Get("/upload/{uploadID}/result", s.handleUploadResult)
		r.Post("/upload/{uploadID}/cancel", s.handleCancelUpload)

		// Reset operations
		r.Post("/reset/{tableKey}", s.handleReset)
		r.Post("/reset", s.handleResetAll)
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

// writeError writes a JSON error response.
func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	fmt.Fprintf(w, `{"error":%q}`, message)
}
