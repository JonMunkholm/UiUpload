package middleware

import (
	"crypto/subtle"
	"log/slog"
	"net/http"

	"github.com/JonMunkholm/TUI/internal/config"
)

// APIKeyAuth returns middleware that validates X-API-Key header against configured keys.
// If RequireAPIKey is false, all requests pass through.
// If RequireAPIKey is true but no keys are configured, all requests are rejected.
func APIKeyAuth(cfg *config.SecurityConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip validation if auth is disabled
			if !cfg.RequireAPIKey {
				next.ServeHTTP(w, r)
				return
			}

			// Get API key from header
			apiKey := r.Header.Get("X-API-Key")
			if apiKey == "" {
				slog.Warn("auth: missing API key",
					"path", r.URL.Path,
					"method", r.Method,
					"remote_addr", r.RemoteAddr,
				)
				http.Error(w, `{"error":"missing API key","code":"AUTH_MISSING_KEY"}`, http.StatusUnauthorized)
				return
			}

			// Validate against configured keys
			if !isValidAPIKey(apiKey, cfg.APIKeys) {
				slog.Warn("auth: invalid API key",
					"path", r.URL.Path,
					"method", r.Method,
					"remote_addr", r.RemoteAddr,
				)
				http.Error(w, `{"error":"invalid API key","code":"AUTH_INVALID_KEY"}`, http.StatusForbidden)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// isValidAPIKey checks if the provided key matches any configured key.
// Uses constant-time comparison and checks ALL keys to prevent timing attacks.
// The comparison time is constant regardless of which key matches (or none).
func isValidAPIKey(key string, validKeys []string) bool {
	valid := 0
	for _, validKey := range validKeys {
		valid |= subtle.ConstantTimeCompare([]byte(key), []byte(validKey))
	}
	return valid == 1
}
