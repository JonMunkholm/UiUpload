// Package logging provides structured logging configuration using log/slog.
//
// This package integrates with chi's RequestID middleware to propagate
// request IDs through structured log entries, enabling request tracing
// across the entire request lifecycle.
package logging

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"github.com/go-chi/chi/v5/middleware"
)

// Setup configures the global slog logger based on level and format.
//
// Level values: "debug", "info", "warn", "error" (default: "info")
// Format values: "text", "json" (default: "text")
//
// Use "json" format in production for machine parsing (ELK, CloudWatch, etc.)
// Use "text" format in development for human readability.
func Setup(level, format string) {
	opts := &slog.HandlerOptions{
		Level: parseLevel(level),
	}

	var handler slog.Handler
	if strings.ToLower(format) == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	slog.SetDefault(slog.New(handler))
}

// parseLevel converts a string log level to slog.Level.
func parseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// FromContext returns a logger enriched with request context.
//
// When called with a request context that contains a chi RequestID,
// the returned logger automatically includes request_id in all log entries.
// This enables correlation of all log entries for a single request.
//
// Usage:
//
//	func handleRequest(w http.ResponseWriter, r *http.Request) {
//	    logger := logging.FromContext(r.Context())
//	    logger.Info("processing request", "table", tableKey)
//	}
func FromContext(ctx context.Context) *slog.Logger {
	logger := slog.Default()

	// Chi's RequestID middleware stores the ID in context
	if reqID := middleware.GetReqID(ctx); reqID != "" {
		logger = logger.With("request_id", reqID)
	}

	return logger
}

// WithFields returns a logger with additional structured fields.
//
// This is useful for creating operation-specific loggers that carry
// consistent context through a multi-step process.
//
// Usage:
//
//	uploadLogger := logging.WithFields(ctx,
//	    "upload_id", uploadID,
//	    "table", tableKey,
//	)
//	uploadLogger.Info("upload started")
//	// ... later ...
//	uploadLogger.Info("upload completed", "rows", inserted)
func WithFields(ctx context.Context, args ...any) *slog.Logger {
	return FromContext(ctx).With(args...)
}
