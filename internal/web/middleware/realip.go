package middleware

import (
	"log/slog"
	"net"
	"net/http"
	"strings"
)

// TrustedRealIP extracts the real client IP from X-Real-IP or X-Forwarded-For
// headers, but ONLY if the request comes from a trusted proxy CIDR.
// If no trusted proxies are configured or the request is not from a trusted
// proxy, the original RemoteAddr is used.
//
// This prevents IP spoofing attacks where untrusted clients send fake
// X-Real-IP headers to bypass rate limiting or audit logging.
func TrustedRealIP(trustedCIDRs []string) func(http.Handler) http.Handler {
	// Parse trusted CIDRs once at startup
	var trustedNets []*net.IPNet
	for _, cidr := range trustedCIDRs {
		cidr = strings.TrimSpace(cidr)
		if cidr == "" {
			continue
		}

		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			// Try parsing as single IP (e.g., "127.0.0.1" instead of "127.0.0.1/32")
			if ip := net.ParseIP(cidr); ip != nil {
				mask := net.CIDRMask(128, 128)
				if ip.To4() != nil {
					mask = net.CIDRMask(32, 32)
				}
				trustedNets = append(trustedNets, &net.IPNet{IP: ip, Mask: mask})
			} else {
				slog.Warn("realip: invalid trusted proxy CIDR, skipping",
					"cidr", cidr,
					"error", err,
				)
			}
			continue
		}
		trustedNets = append(trustedNets, network)
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Get immediate remote IP (the connection source)
			remoteIP := extractIP(r.RemoteAddr)

			// Only trust headers if request is from a trusted proxy
			if isTrusted(remoteIP, trustedNets) {
				// Check X-Real-IP first (set by nginx/proxies)
				if rip := r.Header.Get("X-Real-IP"); rip != "" {
					// Validate X-Real-IP is a valid IP before accepting
					if ip := net.ParseIP(strings.TrimSpace(rip)); ip != nil {
						r.RemoteAddr = ip.String()
					}
				} else if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
					// Take first IP in X-Forwarded-For chain (original client)
					var candidate string
					if idx := strings.Index(xff, ","); idx > 0 {
						candidate = strings.TrimSpace(xff[:idx])
					} else {
						candidate = strings.TrimSpace(xff)
					}
					// Validate candidate is a valid IP before accepting
					if ip := net.ParseIP(candidate); ip != nil {
						r.RemoteAddr = ip.String()
					}
				}
				// If no valid headers, keep original RemoteAddr
			}
			// If not from trusted proxy, keep original RemoteAddr (don't trust headers)

			next.ServeHTTP(w, r)
		})
	}
}

// extractIP parses an IP address from a host:port string or plain IP.
func extractIP(addr string) net.IP {
	// Handle "host:port" format
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return net.ParseIP(host)
	}
	return net.ParseIP(addr)
}

// isTrusted checks if an IP is within any of the trusted networks.
func isTrusted(ip net.IP, trusted []*net.IPNet) bool {
	if ip == nil {
		return false
	}
	for _, network := range trusted {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}
