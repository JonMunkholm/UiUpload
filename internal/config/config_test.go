package config

import (
	"os"
	"testing"
	"time"
)

func TestLoad_Defaults(t *testing.T) {
	// Set only required env var
	os.Setenv("DATABASE_URL", "postgres://localhost/test")
	defer os.Unsetenv("DATABASE_URL")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify defaults
	if cfg.Server.Host != "0.0.0.0" {
		t.Errorf("Server.Host = %q, want %q", cfg.Server.Host, "0.0.0.0")
	}
	if cfg.Server.Port != 8080 {
		t.Errorf("Server.Port = %d, want %d", cfg.Server.Port, 8080)
	}
	if cfg.Upload.MaxConcurrent != 5 {
		t.Errorf("Upload.MaxConcurrent = %d, want %d", cfg.Upload.MaxConcurrent, 5)
	}
	if cfg.Upload.MaxFileSize != 104857600 {
		t.Errorf("Upload.MaxFileSize = %d, want %d", cfg.Upload.MaxFileSize, 104857600)
	}
	if cfg.Rate.RequestsPerMinute != 100 {
		t.Errorf("Rate.RequestsPerMinute = %d, want %d", cfg.Rate.RequestsPerMinute, 100)
	}
}

func TestLoad_OverrideDefaults(t *testing.T) {
	os.Setenv("DATABASE_URL", "postgres://localhost/test")
	os.Setenv("SERVER_PORT", "9090")
	os.Setenv("UPLOAD_MAX_CONCURRENT", "10")
	os.Setenv("LOG_LEVEL", "debug")
	defer func() {
		os.Unsetenv("DATABASE_URL")
		os.Unsetenv("SERVER_PORT")
		os.Unsetenv("UPLOAD_MAX_CONCURRENT")
		os.Unsetenv("LOG_LEVEL")
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Server.Port != 9090 {
		t.Errorf("Server.Port = %d, want %d", cfg.Server.Port, 9090)
	}
	if cfg.Upload.MaxConcurrent != 10 {
		t.Errorf("Upload.MaxConcurrent = %d, want %d", cfg.Upload.MaxConcurrent, 10)
	}
	if cfg.Logging.Level != "debug" {
		t.Errorf("Logging.Level = %q, want %q", cfg.Logging.Level, "debug")
	}
}

func TestLoad_AltEnvVar(t *testing.T) {
	// Test that DB_URL works as fallback
	os.Setenv("DB_URL", "postgres://localhost/alttest")
	defer os.Unsetenv("DB_URL")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Database.URL != "postgres://localhost/alttest" {
		t.Errorf("Database.URL = %q, want %q", cfg.Database.URL, "postgres://localhost/alttest")
	}
}

func TestLoad_MissingRequired(t *testing.T) {
	// Ensure DATABASE_URL is not set
	os.Unsetenv("DATABASE_URL")
	os.Unsetenv("DB_URL")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() expected error for missing DATABASE_URL")
	}
}

func TestLoad_Duration(t *testing.T) {
	os.Setenv("DATABASE_URL", "postgres://localhost/test")
	os.Setenv("SERVER_READ_TIMEOUT", "45s")
	os.Setenv("UPLOAD_MAX_WAIT_TIME", "1m30s")
	defer func() {
		os.Unsetenv("DATABASE_URL")
		os.Unsetenv("SERVER_READ_TIMEOUT")
		os.Unsetenv("UPLOAD_MAX_WAIT_TIME")
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Server.ReadTimeout != 45*time.Second {
		t.Errorf("Server.ReadTimeout = %v, want %v", cfg.Server.ReadTimeout, 45*time.Second)
	}
	if cfg.Upload.MaxWaitTime != 90*time.Second {
		t.Errorf("Upload.MaxWaitTime = %v, want %v", cfg.Upload.MaxWaitTime, 90*time.Second)
	}
}

func TestLoad_CommaSeparatedSlice(t *testing.T) {
	os.Setenv("DATABASE_URL", "postgres://localhost/test")
	os.Setenv("TRUSTED_PROXIES", "10.0.0.0/8, 172.16.0.0/12 , 192.168.0.0/16")
	defer func() {
		os.Unsetenv("DATABASE_URL")
		os.Unsetenv("TRUSTED_PROXIES")
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	expected := []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}
	if len(cfg.Security.TrustedProxies) != len(expected) {
		t.Fatalf("TrustedProxies length = %d, want %d", len(cfg.Security.TrustedProxies), len(expected))
	}
	for i, v := range expected {
		if cfg.Security.TrustedProxies[i] != v {
			t.Errorf("TrustedProxies[%d] = %q, want %q", i, cfg.Security.TrustedProxies[i], v)
		}
	}
}

func TestValidate_InvalidPort(t *testing.T) {
	cfg := &Config{
		Database: DatabaseConfig{URL: "postgres://localhost/test", MaxConns: 20, MinConns: 4},
		Server:   ServerConfig{Port: 99999, ShutdownTimeout: time.Second},
		Upload:   UploadConfig{MaxFileSize: 1, MaxConcurrent: 1, BatchSize: 1, MaxWaitTime: time.Second, Timeout: time.Minute},
		Rate:     RateLimitConfig{Enabled: true, RequestsPerMinute: 100},
		Archive:  ArchiveConfig{HotRetentionDays: 90, ArchiveRetentionYears: 7, BatchSize: 1000, CheckInterval: time.Hour},
		Logging:  LoggingConfig{Level: "info", Format: "text"},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() expected error for invalid port")
	}
	if !contains(err.Error(), "SERVER_PORT") {
		t.Errorf("error should mention SERVER_PORT: %v", err)
	}
}

func TestValidate_MaxConnsLessThanMinConns(t *testing.T) {
	cfg := &Config{
		Database: DatabaseConfig{URL: "postgres://localhost/test", MaxConns: 2, MinConns: 5},
		Server:   ServerConfig{Port: 8080, ShutdownTimeout: time.Second},
		Upload:   UploadConfig{MaxFileSize: 1, MaxConcurrent: 1, BatchSize: 1, MaxWaitTime: time.Second, Timeout: time.Minute},
		Rate:     RateLimitConfig{Enabled: true, RequestsPerMinute: 100},
		Archive:  ArchiveConfig{HotRetentionDays: 90, ArchiveRetentionYears: 7, BatchSize: 1000, CheckInterval: time.Hour},
		Logging:  LoggingConfig{Level: "info", Format: "text"},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() expected error for MaxConns < MinConns")
	}
	if !contains(err.Error(), "DB_MAX_CONNS") {
		t.Errorf("error should mention DB_MAX_CONNS: %v", err)
	}
}

func TestValidate_InvalidLogLevel(t *testing.T) {
	cfg := &Config{
		Database: DatabaseConfig{URL: "postgres://localhost/test", MaxConns: 20, MinConns: 4},
		Server:   ServerConfig{Port: 8080, ShutdownTimeout: time.Second},
		Upload:   UploadConfig{MaxFileSize: 1, MaxConcurrent: 1, BatchSize: 1, MaxWaitTime: time.Second, Timeout: time.Minute},
		Rate:     RateLimitConfig{Enabled: true, RequestsPerMinute: 100},
		Archive:  ArchiveConfig{HotRetentionDays: 90, ArchiveRetentionYears: 7, BatchSize: 1000, CheckInterval: time.Hour},
		Logging:  LoggingConfig{Level: "verbose", Format: "text"},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() expected error for invalid log level")
	}
	if !contains(err.Error(), "LOG_LEVEL") {
		t.Errorf("error should mention LOG_LEVEL: %v", err)
	}
}

func TestServerAddr(t *testing.T) {
	tests := []struct {
		host string
		port int
		want string
	}{
		{"", 8080, ":8080"},
		{"0.0.0.0", 8080, "0.0.0.0:8080"},
		{"127.0.0.1", 3000, "127.0.0.1:3000"},
		{"localhost", 443, "localhost:443"},
	}

	for _, tt := range tests {
		cfg := &ServerConfig{Host: tt.host, Port: tt.port}
		got := cfg.Addr()
		if got != tt.want {
			t.Errorf("Addr() with host=%q, port=%d = %q, want %q", tt.host, tt.port, got, tt.want)
		}
	}
}

func TestConfigString_MasksURL(t *testing.T) {
	cfg := &Config{
		Database: DatabaseConfig{URL: "postgres://secret:password@host/db"},
	}
	str := cfg.String()
	if contains(str, "secret") || contains(str, "password") {
		t.Error("String() should mask database URL")
	}
	if !contains(str, "MASKED") {
		t.Error("String() should contain MASKED placeholder")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
