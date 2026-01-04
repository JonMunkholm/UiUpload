package core

// scheduler.go provides background job scheduling for maintenance tasks.
//
// Currently implements audit log archiving, which runs periodically to:
//  1. Move old entries from audit_log to audit_log_archive (hot -> cold)
//  2. Purge very old entries from the archive based on retention policy
//
// The scheduler is designed to be long-running and context-aware for graceful
// shutdown. It logs progress and errors but does not fail the application
// if individual archive operations fail.

import (
	"context"
	"log/slog"
	"time"

	db "github.com/JonMunkholm/TUI/internal/database"
)

// ArchiveConfig holds configuration for the archive scheduler.
// All fields have sensible defaults if zero values are provided.
type ArchiveConfig struct {
	HotRetentionDays      int           // Days to keep in audit_log (default: 90)
	ArchiveRetentionYears int           // Years to keep in archive (default: 7)
	BatchSize             int           // Rows per batch (default: 5000)
	CheckInterval         time.Duration // How often to run (default: 24h)
}

// StartArchiveScheduler starts a background goroutine that periodically
// archives old audit log entries and purges very old archives.
// It runs immediately on start, then every CheckInterval.
// The scheduler stops when the context is cancelled.
func (s *Service) StartArchiveScheduler(ctx context.Context, cfg ArchiveConfig) {
	slog.Info("archive scheduler started",
		"hot_retention_days", cfg.HotRetentionDays,
		"archive_retention_years", cfg.ArchiveRetentionYears,
		"batch_size", cfg.BatchSize,
	)

	// Run immediately on startup
	s.runArchiveJob(ctx, cfg)

	// Then run periodically
	ticker := time.NewTicker(cfg.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("archive scheduler stopped")
			return
		case <-ticker.C:
			s.runArchiveJob(ctx, cfg)
		}
	}
}

// runArchiveJob performs one archive + purge cycle.
func (s *Service) runArchiveJob(ctx context.Context, cfg ArchiveConfig) {
	slog.Debug("archive job started")
	start := time.Now()

	// Archive old entries from hot to cold storage
	archiveStart := time.Now()
	archived, err := s.archiveOldAuditLogs(ctx, cfg.HotRetentionDays, cfg.BatchSize)
	if err != nil {
		slog.Error("archive failed", "error", err)
	} else {
		slog.Info("archived audit log entries",
			"entries_archived", archived,
			"duration_ms", time.Since(archiveStart).Milliseconds(),
		)
	}

	// Purge very old archives
	purgeStart := time.Now()
	purged, err := s.purgeOldArchives(ctx, cfg.ArchiveRetentionYears)
	if err != nil {
		slog.Error("purge failed", "error", err)
	} else {
		slog.Info("purged old archive entries",
			"entries_purged", purged,
			"duration_ms", time.Since(purgeStart).Milliseconds(),
		)
	}

	slog.Info("archive job completed", "duration_ms", time.Since(start).Milliseconds())
}

// archiveOldAuditLogs moves audit entries older than daysToKeep to cold storage.
func (s *Service) archiveOldAuditLogs(ctx context.Context, daysToKeep, batchSize int) (int64, error) {
	result, err := db.New(s.pool).ArchiveOldAuditLogs(ctx, db.ArchiveOldAuditLogsParams{
		Column1: int32(daysToKeep),
		Column2: int32(batchSize),
	})
	if err != nil {
		return 0, err
	}
	return int64(result), nil
}

// purgeOldArchives deletes archived entries older than yearsToKeep.
func (s *Service) purgeOldArchives(ctx context.Context, yearsToKeep int) (int64, error) {
	result, err := db.New(s.pool).PurgeOldArchives(ctx, int32(yearsToKeep))
	if err != nil {
		return 0, err
	}
	return int64(result), nil
}
