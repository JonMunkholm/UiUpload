package core

import (
	"context"
	"log"
	"time"

	db "github.com/JonMunkholm/TUI/internal/database"
)

// ArchiveConfig holds configuration for the archive scheduler.
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
	log.Printf("Archive scheduler started (retention: %dd hot, %dy archive, batch: %d)",
		cfg.HotRetentionDays, cfg.ArchiveRetentionYears, cfg.BatchSize)

	// Run immediately on startup
	s.runArchiveJob(ctx, cfg)

	// Then run periodically
	ticker := time.NewTicker(cfg.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Archive scheduler stopped")
			return
		case <-ticker.C:
			s.runArchiveJob(ctx, cfg)
		}
	}
}

// runArchiveJob performs one archive + purge cycle.
func (s *Service) runArchiveJob(ctx context.Context, cfg ArchiveConfig) {
	log.Println("Archive job started")
	start := time.Now()

	// Archive old entries from hot to cold storage
	archiveStart := time.Now()
	archived, err := s.archiveOldAuditLogs(ctx, cfg.HotRetentionDays, cfg.BatchSize)
	if err != nil {
		log.Printf("Archive error: %v", err)
	} else {
		log.Printf("Archived %d entries from audit_log (%dms)",
			archived, time.Since(archiveStart).Milliseconds())
	}

	// Purge very old archives
	purgeStart := time.Now()
	purged, err := s.purgeOldArchives(ctx, cfg.ArchiveRetentionYears)
	if err != nil {
		log.Printf("Purge error: %v", err)
	} else {
		log.Printf("Purged %d entries from audit_log_archive (%dms)",
			purged, time.Since(purgeStart).Milliseconds())
	}

	log.Printf("Archive job completed (%dms)", time.Since(start).Milliseconds())
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
