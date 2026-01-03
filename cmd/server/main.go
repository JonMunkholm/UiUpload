package main

import (
	"context"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/JonMunkholm/TUI/internal/core"
	_ "github.com/JonMunkholm/TUI/internal/core/tables" // Register all tables
	"github.com/JonMunkholm/TUI/internal/web"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func main() {
	// Load .env file if it exists (Overload overwrites existing env vars)
	if err := godotenv.Overload(); err != nil {
		log.Println("No .env file found, using environment variables")
	} else {
		log.Println("Loaded .env file (overwriting existing env vars)")
	}

	// Get database URL from environment (check both DATABASE_URL and DB_URL)
	dbURL := os.Getenv("DATABASE_URL")
	dbSource := "DATABASE_URL"
	if dbURL == "" {
		dbURL = os.Getenv("DB_URL")
		dbSource = "DB_URL"
	}
	if dbURL == "" {
		dbURL = "postgres://localhost:5432/csvimporter?sslmode=disable"
		dbSource = "default"
	}
	log.Printf("Using database URL from: %s", dbSource)

	// Get server address from environment
	addr := os.Getenv("SERVER_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	// Connect to database
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	// Log which database we connected to
	if u, err := url.Parse(dbURL); err == nil {
		dbName := strings.TrimPrefix(u.Path, "/")
		log.Printf("Connected to database: %s", dbName)
	} else {
		log.Printf("Connected to database")
	}

	// Create service
	service, err := core.NewService(pool)
	if err != nil {
		log.Fatalf("Failed to create service: %v", err)
	}

	// Log registered tables
	log.Printf("Registered %d tables in %d groups", core.TableCount(), len(core.Groups()))
	for _, group := range core.Groups() {
		tables := core.ByGroup(group)
		log.Printf("  %s: %d tables", group, len(tables))
	}

	// Create and start server
	server := web.NewServer(service)

	// Create cancellable context for background jobs
	jobCtx, cancelJobs := context.WithCancel(context.Background())

	// Start archive scheduler
	go service.StartArchiveScheduler(jobCtx, core.ArchiveConfig{
		HotRetentionDays:      90,
		ArchiveRetentionYears: 7,
		BatchSize:             5000,
		CheckInterval:         24 * time.Hour,
	})

	// Graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh

		log.Println("Shutting down...")

		// Stop background jobs
		cancelJobs()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("Shutdown error: %v", err)
		}
	}()

	// Start server
	log.Printf("Server starting on %s", addr)
	if err := server.Start(addr); err != nil {
		log.Printf("Server stopped: %v", err)
	}
}
