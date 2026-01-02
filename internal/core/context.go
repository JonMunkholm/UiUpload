package core

import "context"

type contextKey string

const (
	ctxKeyIPAddress contextKey = "audit_ip"
	ctxKeyUserAgent contextKey = "audit_ua"
)

// ContextWithIPAddress adds IP address to context for audit logging.
func ContextWithIPAddress(ctx context.Context, ip string) context.Context {
	return context.WithValue(ctx, ctxKeyIPAddress, ip)
}

// ContextWithUserAgent adds User-Agent to context for audit logging.
func ContextWithUserAgent(ctx context.Context, ua string) context.Context {
	return context.WithValue(ctx, ctxKeyUserAgent, ua)
}

// GetIPAddressFromContext extracts IP address from context.
func GetIPAddressFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyIPAddress).(string); ok {
		return v
	}
	return ""
}

// GetUserAgentFromContext extracts User-Agent from context.
func GetUserAgentFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKeyUserAgent).(string); ok {
		return v
	}
	return ""
}
