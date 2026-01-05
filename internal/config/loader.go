package config

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Load reads configuration from environment variables.
// It applies defaults for unset values and validates the result.
// Returns an error if required values are missing or validation fails.
func Load() (*Config, error) {
	cfg := &Config{}

	if err := loadStruct(reflect.ValueOf(cfg).Elem()); err != nil {
		return nil, fmt.Errorf("config load: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation: %w", err)
	}

	return cfg, nil
}

// MustLoad loads configuration and panics on error.
// Use this only in main() where early termination is desired.
func MustLoad() *Config {
	cfg, err := Load()
	if err != nil {
		panic(fmt.Sprintf("failed to load configuration: %v", err))
	}
	return cfg
}

// loadStruct recursively populates struct fields from environment variables.
func loadStruct(v reflect.Value) error {
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldVal := v.Field(i)

		// Skip unexported fields
		if !fieldVal.CanSet() {
			continue
		}

		// Recurse into nested structs
		if field.Type.Kind() == reflect.Struct && field.Type != reflect.TypeOf(time.Time{}) {
			if err := loadStruct(fieldVal); err != nil {
				return err
			}
			continue
		}

		// Get tags
		envName := field.Tag.Get("env")
		envAlt := field.Tag.Get("envAlt")
		defaultVal := field.Tag.Get("default")
		required := field.Tag.Get("required") == "true"

		if envName == "" {
			continue
		}

		// Try primary env var, then alternate
		value := os.Getenv(envName)
		if value == "" && envAlt != "" {
			value = os.Getenv(envAlt)
		}

		// Apply default if not set
		if value == "" {
			if required {
				return fmt.Errorf("required environment variable %s is not set", envName)
			}
			value = defaultVal
		}

		if value == "" {
			continue
		}

		// Set the field value
		if err := setField(fieldVal, value); err != nil {
			return fmt.Errorf("invalid value for %s=%q: %w", envName, value, err)
		}
	}

	return nil
}

// setField sets a reflect.Value from a string based on its type.
func setField(field reflect.Value, value string) error {
	switch field.Kind() {
	case reflect.String:
		field.SetString(value)

	case reflect.Int, reflect.Int64:
		// Handle time.Duration specially
		if field.Type() == reflect.TypeOf(time.Duration(0)) {
			d, err := time.ParseDuration(value)
			if err != nil {
				return fmt.Errorf("invalid duration: %w", err)
			}
			field.Set(reflect.ValueOf(d))
		} else {
			i, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return fmt.Errorf("invalid integer: %w", err)
			}
			field.SetInt(i)
		}

	case reflect.Bool:
		b, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("invalid boolean: %w", err)
		}
		field.SetBool(b)

	case reflect.Slice:
		if field.Type().Elem().Kind() == reflect.String {
			// Split comma-separated values, trim whitespace
			parts := strings.Split(value, ",")
			result := make([]string, 0, len(parts))
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p != "" {
					result = append(result, p)
				}
			}
			field.Set(reflect.ValueOf(result))
		} else {
			return fmt.Errorf("unsupported slice type: %s", field.Type().Elem().Kind())
		}

	default:
		return fmt.Errorf("unsupported field type: %s", field.Kind())
	}

	return nil
}

// Validate checks that the configuration is valid.
// Returns an error describing all validation failures.
func (c *Config) Validate() error {
	var errs []string

	// Database validation
	if c.Database.URL == "" {
		errs = append(errs, "DATABASE_URL is required")
	}
	if c.Database.MaxConns < c.Database.MinConns {
		errs = append(errs, fmt.Sprintf("DB_MAX_CONNS (%d) must be >= DB_MIN_CONNS (%d)",
			c.Database.MaxConns, c.Database.MinConns))
	}
	if c.Database.MaxConns <= 0 {
		errs = append(errs, "DB_MAX_CONNS must be positive")
	}
	if c.Database.MinConns < 0 {
		errs = append(errs, "DB_MIN_CONNS must be non-negative")
	}

	// Server validation
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		errs = append(errs, fmt.Sprintf("SERVER_PORT (%d) must be 1-65535", c.Server.Port))
	}
	if c.Server.ReadTimeout < 0 {
		errs = append(errs, "SERVER_READ_TIMEOUT must be non-negative")
	}
	if c.Server.ShutdownTimeout <= 0 {
		errs = append(errs, "SERVER_SHUTDOWN_TIMEOUT must be positive")
	}

	// Upload validation
	if c.Upload.MaxFileSize <= 0 {
		errs = append(errs, "UPLOAD_MAX_FILE_SIZE must be positive")
	}
	if c.Upload.MaxConcurrent <= 0 {
		errs = append(errs, "UPLOAD_MAX_CONCURRENT must be positive")
	}
	if c.Upload.BatchSize <= 0 {
		errs = append(errs, "UPLOAD_BATCH_SIZE must be positive")
	}
	if c.Upload.MaxWaitTime <= 0 {
		errs = append(errs, "UPLOAD_MAX_WAIT_TIME must be positive")
	}
	if c.Upload.Timeout <= 0 {
		errs = append(errs, "UPLOAD_TIMEOUT must be positive")
	}

	// Rate limit validation
	if c.Rate.Enabled && c.Rate.RequestsPerMinute <= 0 {
		errs = append(errs, "RATE_LIMIT_REQUESTS_PER_MINUTE must be positive when rate limiting is enabled")
	}

	// Archive validation
	if c.Archive.HotRetentionDays <= 0 {
		errs = append(errs, "ARCHIVE_HOT_RETENTION_DAYS must be positive")
	}
	if c.Archive.ArchiveRetentionYears <= 0 {
		errs = append(errs, "ARCHIVE_RETENTION_YEARS must be positive")
	}
	if c.Archive.BatchSize <= 0 {
		errs = append(errs, "ARCHIVE_BATCH_SIZE must be positive")
	}
	if c.Archive.CheckInterval <= 0 {
		errs = append(errs, "ARCHIVE_CHECK_INTERVAL must be positive")
	}

	// Security validation
	if c.Security.RequireAPIKey && len(c.Security.APIKeys) == 0 {
		errs = append(errs, "REQUIRE_API_KEY is true but API_KEYS is empty; configure at least one API key or disable auth")
	}

	// Logging validation
	validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLevels[strings.ToLower(c.Logging.Level)] {
		errs = append(errs, fmt.Sprintf("LOG_LEVEL (%q) must be one of: debug, info, warn, error", c.Logging.Level))
	}

	validFormats := map[string]bool{"text": true, "json": true}
	if !validFormats[strings.ToLower(c.Logging.Format)] {
		errs = append(errs, fmt.Sprintf("LOG_FORMAT (%q) must be one of: text, json", c.Logging.Format))
	}

	if len(errs) > 0 {
		return fmt.Errorf("validation failed:\n  - %s", strings.Join(errs, "\n  - "))
	}

	return nil
}

// String returns a safe string representation of the config for logging.
// Sensitive values like database URLs are masked.
func (c *Config) String() string {
	var b strings.Builder
	b.WriteString("Config{")
	b.WriteString(fmt.Sprintf("Server: {Host: %q, Port: %d}, ", c.Server.Host, c.Server.Port))
	b.WriteString(fmt.Sprintf("Database: {URL: [MASKED], MaxConns: %d, MinConns: %d}, ",
		c.Database.MaxConns, c.Database.MinConns))
	b.WriteString(fmt.Sprintf("Upload: {MaxFileSize: %d, MaxConcurrent: %d, BatchSize: %d}, ",
		c.Upload.MaxFileSize, c.Upload.MaxConcurrent, c.Upload.BatchSize))
	b.WriteString(fmt.Sprintf("Rate: {Enabled: %v, RequestsPerMinute: %d}, ",
		c.Rate.Enabled, c.Rate.RequestsPerMinute))
	b.WriteString(fmt.Sprintf("Logging: {Level: %q, Format: %q}",
		c.Logging.Level, c.Logging.Format))
	b.WriteString("}")
	return b.String()
}
