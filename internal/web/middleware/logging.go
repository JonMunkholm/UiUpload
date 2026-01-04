// Package middleware provides HTTP middleware for the web server.
package middleware

import (
	"net/http"
	"time"

	"github.com/JonMunkholm/TUI/internal/logging"
)

// Logger is an HTTP middleware that logs request details using structured logging.
//
// It captures request timing, status code, and key metadata for observability.
// The middleware integrates with chi's RequestID to ensure all log entries
// include the request ID for tracing.
//
// Log fields:
//   - method: HTTP method (GET, POST, etc.)
//   - path: Request URL path
//   - status: HTTP response status code
//   - duration_ms: Request processing time in milliseconds
//   - ip: Client IP address (via X-Real-IP or RemoteAddr)
//   - user_agent: Client user agent string
func Logger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Wrap response writer to capture status code
		ww := &responseWriter{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(ww, r)

		duration := time.Since(start)
		logger := logging.FromContext(r.Context())

		// Determine client IP (prefer X-Real-IP set by RealIP middleware)
		ip := r.RemoteAddr
		if realIP := r.Header.Get("X-Real-IP"); realIP != "" {
			ip = realIP
		}

		logger.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", ww.status,
			"duration_ms", duration.Milliseconds(),
			"ip", ip,
			"user_agent", r.UserAgent(),
		)
	})
}

// responseWriter wraps http.ResponseWriter to capture the status code.
type responseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *responseWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.status = status
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

// Unwrap provides access to the underlying ResponseWriter for middleware
// that need to inspect it (e.g., http.Flusher for SSE).
func (w *responseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
