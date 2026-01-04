package main

import (
	"context"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/JonMunkholm/TUI/internal/config"
	"github.com/JonMunkholm/TUI/internal/core"
	_ "github.com/JonMunkholm/TUI/internal/core/tables" // Register all tables
	"github.com/JonMunkholm/TUI/internal/logging"
	"github.com/JonMunkholm/TUI/internal/web"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func main() {
	// Load .env file if it exists (Overload overwrites existing env vars)
	if err := godotenv.Overload(); err != nil {
		slog.Info("no .env file found, using environment variables")
	} else {
		slog.Info("loaded .env file (overwriting existing env vars)")
	}

	// Load and validate configuration
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	// Setup structured logging based on config
	logging.Setup(cfg.Logging.Level, cfg.Logging.Format)

	slog.Info("configuration loaded",
		"port", cfg.Server.Port,
		"db_max_conns", cfg.Database.MaxConns,
		"upload_max_concurrent", cfg.Upload.MaxConcurrent,
		"rate_limit_enabled", cfg.Rate.Enabled,
	)

	// Parse and configure connection pool
	poolConfig, err := pgxpool.ParseConfig(cfg.Database.URL)
	if err != nil {
		slog.Error("failed to parse database URL", "error", err)
		os.Exit(1)
	}

	// Apply pool configuration from config
	poolConfig.MaxConns = int32(cfg.Database.MaxConns)
	poolConfig.MinConns = int32(cfg.Database.MinConns)
	poolConfig.MaxConnLifetime = cfg.Database.MaxConnLifetime
	poolConfig.MaxConnIdleTime = cfg.Database.MaxConnIdleTime

	// Connect to database
	ctx := context.Background()
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		slog.Error("failed to ping database", "error", err)
		os.Exit(1)
	}

	// Log which database we connected to
	if u, err := url.Parse(cfg.Database.URL); err == nil {
		dbName := strings.TrimPrefix(u.Path, "/")
		slog.Info("connected to database", "name", dbName)
	} else {
		slog.Info("connected to database")
	}

	// Create service with config
	service, err := core.NewService(pool, cfg)
	if err != nil {
		slog.Error("failed to create service", "error", err)
		os.Exit(1)
	}

	// Log registered tables
	slog.Info("tables registered",
		"count", core.TableCount(),
		"groups", len(core.Groups()),
	)
	for _, group := range core.Groups() {
		tables := core.ByGroup(group)
		slog.Debug("table group", "group", group, "tables", len(tables))
	}

	// Create server with config
	server := web.NewServer(service, cfg)

	// Create cancellable context for background jobs
	jobCtx, cancelJobs := context.WithCancel(context.Background())

	// Start archive scheduler with config values
	go service.StartArchiveScheduler(jobCtx, core.ArchiveConfig{
		HotRetentionDays:      cfg.Archive.HotRetentionDays,
		ArchiveRetentionYears: cfg.Archive.ArchiveRetentionYears,
		BatchSize:             cfg.Archive.BatchSize,
		CheckInterval:         cfg.Archive.CheckInterval,
	})

	// Graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh

		slog.Info("shutting down...")

		// Stop background jobs
		cancelJobs()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
		defer cancel()

		// Wait for active uploads to complete (with timeout)
		uploadStatus := service.UploadLimiterStatus()
		if uploadStatus.Active > 0 {
			slog.Info("waiting for uploads to complete", "active", uploadStatus.Active)
			if err := service.WaitForUploads(shutdownCtx); err != nil {
				slog.Warn("uploads did not complete in time", "error", err)
			} else {
				slog.Info("all uploads completed")
			}
		}

		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.Error("shutdown error", "error", err)
		}
	}()

	// Start server (uses addr from config internally)
	slog.Info("server starting", "addr", cfg.Server.Addr())
	if err := server.Start(); err != nil {
		slog.Info("server stopped", "error", err)
	}
}
