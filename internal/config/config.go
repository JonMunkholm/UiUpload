// Package config provides centralized configuration management for the application.
// It loads configuration from environment variables with sensible defaults and
// validates all settings on startup to fail fast on misconfiguration.
package config

import "time"

// Config holds all application configuration.
// All settings can be configured via environment variables.
type Config struct {
	Server   ServerConfig
	Database DatabaseConfig
	Upload   UploadConfig
	Rate     RateLimitConfig
	Security SecurityConfig
	Logging  LoggingConfig
	Archive  ArchiveConfig
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	// Host is the interface to bind to (default: 0.0.0.0)
	Host string `env:"SERVER_HOST" default:"0.0.0.0"`

	// Port is the port to listen on (default: 8080)
	Port int `env:"SERVER_PORT" default:"8080"`

	// ReadTimeout is the maximum duration for reading request body (default: 15s)
	ReadTimeout time.Duration `env:"SERVER_READ_TIMEOUT" default:"15s"`

	// WriteTimeout is the maximum duration for writing response (default: 0 for SSE)
	WriteTimeout time.Duration `env:"SERVER_WRITE_TIMEOUT" default:"0s"`

	// IdleTimeout is the keep-alive timeout (default: 60s)
	IdleTimeout time.Duration `env:"SERVER_IDLE_TIMEOUT" default:"60s"`

	// ShutdownTimeout is the maximum duration to wait for graceful shutdown (default: 30s)
	ShutdownTimeout time.Duration `env:"SERVER_SHUTDOWN_TIMEOUT" default:"30s"`

	// RequestTimeout is the middleware timeout for requests (default: 60s)
	RequestTimeout time.Duration `env:"SERVER_REQUEST_TIMEOUT" default:"60s"`
}

// DatabaseConfig holds database connection settings.
type DatabaseConfig struct {
	// URL is the PostgreSQL connection string (required)
	// Supports both DATABASE_URL and DB_URL env vars for compatibility
	URL string `env:"DATABASE_URL" envAlt:"DB_URL" required:"true"`

	// MaxConns is the maximum number of connections in the pool (default: 20)
	MaxConns int `env:"DB_MAX_CONNS" default:"20"`

	// MinConns is the minimum number of connections to keep open (default: 4)
	MinConns int `env:"DB_MIN_CONNS" default:"4"`

	// MaxConnLifetime is the maximum lifetime of a connection (default: 1h)
	MaxConnLifetime time.Duration `env:"DB_MAX_CONN_LIFETIME" default:"1h"`

	// MaxConnIdleTime is the maximum idle time before a connection is closed (default: 30m)
	MaxConnIdleTime time.Duration `env:"DB_MAX_CONN_IDLE_TIME" default:"30m"`
}

// UploadConfig holds CSV upload processing settings.
type UploadConfig struct {
	// MaxFileSize is the maximum allowed file size in bytes (default: 100MB)
	MaxFileSize int64 `env:"UPLOAD_MAX_FILE_SIZE" default:"104857600"`

	// MaxConcurrent is the maximum number of parallel uploads (default: 5)
	MaxConcurrent int `env:"UPLOAD_MAX_CONCURRENT" default:"5"`

	// MaxWaitTime is how long to wait for an upload slot (default: 30s)
	MaxWaitTime time.Duration `env:"UPLOAD_MAX_WAIT_TIME" default:"30s"`

	// BatchSize is the number of rows to insert per batch (default: 1000)
	BatchSize int `env:"UPLOAD_BATCH_SIZE" default:"1000"`

	// Timeout is the maximum duration for a single upload operation (default: 10m)
	Timeout time.Duration `env:"UPLOAD_TIMEOUT" default:"10m"`

	// ResetTimeout is the maximum duration for a reset operation (default: 30s)
	ResetTimeout time.Duration `env:"UPLOAD_RESET_TIMEOUT" default:"30s"`
}

// RateLimitConfig holds rate limiting settings per time window.
type RateLimitConfig struct {
	// Enabled controls whether rate limiting is active (default: true)
	Enabled bool `env:"RATE_LIMIT_ENABLED" default:"true"`

	// RequestsPerMinute is the default rate limit per IP (default: 100)
	RequestsPerMinute int `env:"RATE_LIMIT_REQUESTS_PER_MINUTE" default:"100"`

	// UploadLimit is requests per minute for upload endpoints (default: 10)
	UploadLimit int `env:"RATE_LIMIT_UPLOAD" default:"10"`
}

// SecurityConfig holds security-related settings.
type SecurityConfig struct {
	// TrustedProxies is a comma-separated list of trusted proxy CIDRs
	TrustedProxies []string `env:"TRUSTED_PROXIES"`

	// EnableCSP enables Content-Security-Policy headers (default: true)
	EnableCSP bool `env:"SECURITY_ENABLE_CSP" default:"true"`
}

// LoggingConfig holds logging settings.
type LoggingConfig struct {
	// Level is the minimum log level: debug, info, warn, error (default: info)
	Level string `env:"LOG_LEVEL" default:"info"`

	// Format is the log format: text or json (default: text)
	Format string `env:"LOG_FORMAT" default:"text"`
}

// ArchiveConfig holds audit log archiving settings.
type ArchiveConfig struct {
	// HotRetentionDays is days to keep entries in the hot table (default: 90)
	HotRetentionDays int `env:"ARCHIVE_HOT_RETENTION_DAYS" default:"90"`

	// ArchiveRetentionYears is years to keep archived entries (default: 7)
	ArchiveRetentionYears int `env:"ARCHIVE_RETENTION_YEARS" default:"7"`

	// BatchSize is rows to process per archive batch (default: 5000)
	BatchSize int `env:"ARCHIVE_BATCH_SIZE" default:"5000"`

	// CheckInterval is how often to run the archive job (default: 24h)
	CheckInterval time.Duration `env:"ARCHIVE_CHECK_INTERVAL" default:"24h"`
}

// Addr returns the server listen address in host:port format.
func (c *ServerConfig) Addr() string {
	if c.Host == "" {
		return ":" + itoa(c.Port)
	}
	return c.Host + ":" + itoa(c.Port)
}

// itoa converts an int to string without importing strconv in this file.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	n := len(b)
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		n--
		b[n] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		n--
		b[n] = '-'
	}
	return string(b[n:])
}
