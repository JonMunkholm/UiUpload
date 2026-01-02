package web

import (
	"context"
	"net/http"

	"github.com/JonMunkholm/TUI/internal/core"
)

// WithRequestMetadata adds IP and User-Agent to context for audit logging.
func WithRequestMetadata(ctx context.Context, r *http.Request) context.Context {
	ip := r.RemoteAddr // Already processed by chi middleware.RealIP
	ua := r.Header.Get("User-Agent")
	ctx = core.ContextWithIPAddress(ctx, ip)
	ctx = core.ContextWithUserAgent(ctx, ua)
	return ctx
}
