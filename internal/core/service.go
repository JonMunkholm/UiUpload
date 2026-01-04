package core

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/JonMunkholm/TUI/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Service provides the core business logic for CSV import operations.
type Service struct {
	pool       *pgxpool.Pool
	cfg        *config.Config
	uploadsDir string

	// Audit provides dedicated audit log functionality.
	Audit *AuditService

	// uploadLimiter controls concurrent upload processing.
	uploadLimiter *UploadLimiter

	mu      sync.RWMutex
	uploads map[string]*activeUpload
}

// UploadTimeout returns the configured upload timeout.
func (s *Service) UploadTimeout() time.Duration {
	return s.cfg.Upload.Timeout
}

// ResetTimeout returns the configured reset timeout.
func (s *Service) ResetTimeout() time.Duration {
	return s.cfg.Upload.ResetTimeout
}

type activeUpload struct {
	ID         string
	TableKey   string
	FileName   string
	Cancel     context.CancelFunc
	Progress   UploadProgress
	Result     *UploadResult
	Done       chan struct{}
	Listeners  []chan UploadProgress
	ListenerMu sync.Mutex
	Mapping    map[string]int // User-provided column mapping: expected column -> CSV index
}

// NewService creates a new Service instance with the given configuration.
func NewService(pool *pgxpool.Pool, cfg *config.Config) (*Service, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("get working directory: %w", err)
	}

	uploadsDir := filepath.Join(wd, "accounting", "uploads")

	return &Service{
		pool:          pool,
		cfg:           cfg,
		uploadsDir:    uploadsDir,
		Audit:         NewAuditService(pool),
		uploadLimiter: NewUploadLimiter(cfg.Upload.MaxConcurrent, cfg.Upload.MaxWaitTime),
		uploads:       make(map[string]*activeUpload),
	}, nil
}

// Config returns the service configuration.
func (s *Service) Config() *config.Config {
	return s.cfg
}

// UploadLimiterStatus returns the current state of the upload limiter.
// Used for monitoring and the /api/upload-status endpoint.
func (s *Service) UploadLimiterStatus() UploadLimiterStatus {
	return s.uploadLimiter.Status()
}

// WaitForUploads blocks until all active uploads complete or context is cancelled.
// Used during graceful shutdown to ensure uploads finish before termination.
func (s *Service) WaitForUploads(ctx context.Context) error {
	return s.uploadLimiter.WaitForDrain(ctx)
}

// notifyProgress sends progress updates to all listeners.
func (upload *activeUpload) notifyProgress() {
	upload.ListenerMu.Lock()
	defer upload.ListenerMu.Unlock()

	for _, ch := range upload.Listeners {
		select {
		case ch <- upload.Progress:
		default:
			// Listener is slow, skip this update
		}
	}
}

// closeListeners closes all listener channels.
func (upload *activeUpload) closeListeners() {
	upload.ListenerMu.Lock()
	defer upload.ListenerMu.Unlock()

	for _, ch := range upload.Listeners {
		close(ch)
	}
	upload.Listeners = nil
}

// cleanup removes the upload from tracking after a delay.
func (s *Service) cleanup(uploadID string, delay time.Duration) {
	time.AfterFunc(delay, func() {
		s.mu.Lock()
		delete(s.uploads, uploadID)
		s.mu.Unlock()
	})
}
