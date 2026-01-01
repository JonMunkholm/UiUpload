package core

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	db "github.com/JonMunkholm/TUI/internal/database"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// UploadTimeout is the maximum duration for an upload operation.
var UploadTimeout = 10 * time.Minute

// ResetTimeout is the maximum duration for a reset operation.
var ResetTimeout = 30 * time.Second

// Service provides the core business logic for CSV import operations.
type Service struct {
	pool       *pgxpool.Pool
	uploadsDir string

	mu      sync.RWMutex
	uploads map[string]*activeUpload
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
}

// NewService creates a new Service instance.
func NewService(pool *pgxpool.Pool) (*Service, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("get working directory: %w", err)
	}

	uploadsDir := filepath.Join(wd, "accounting", "uploads")

	return &Service{
		pool:       pool,
		uploadsDir: uploadsDir,
		uploads:    make(map[string]*activeUpload),
	}, nil
}

// ListTables returns information about all registered tables.
func (s *Service) ListTables() []TableInfo {
	defs := All()
	infos := make([]TableInfo, len(defs))
	for i, def := range defs {
		infos[i] = def.Info
	}
	return infos
}

// ListTablesByGroup returns tables organized by group.
func (s *Service) ListTablesByGroup() map[string][]TableInfo {
	result := make(map[string][]TableInfo)
	for _, group := range Groups() {
		for _, def := range ByGroup(group) {
			result[group] = append(result[group], def.Info)
		}
	}
	return result
}

// StartUpload begins an asynchronous upload operation.
// Returns the upload ID immediately. Use SubscribeProgress to get updates.
func (s *Service) StartUpload(ctx context.Context, tableKey string, fileName string, fileData []byte) (string, error) {
	def, ok := Get(tableKey)
	if !ok {
		return "", fmt.Errorf("unknown table: %s", tableKey)
	}

	uploadID := uuid.New().String()

	// Create cancellable context
	uploadCtx, cancel := context.WithTimeout(context.Background(), UploadTimeout)

	upload := &activeUpload{
		ID:       uploadID,
		TableKey: tableKey,
		FileName: fileName,
		Cancel:   cancel,
		Progress: UploadProgress{
			UploadID: uploadID,
			TableKey: tableKey,
			Phase:    PhaseStarting,
			FileName: fileName,
		},
		Done:      make(chan struct{}),
		Listeners: make([]chan UploadProgress, 0),
	}

	s.mu.Lock()
	s.uploads[uploadID] = upload
	s.mu.Unlock()

	// Process in background
	go s.processUpload(uploadCtx, upload, def, fileData)

	return uploadID, nil
}

// StartUploadFromDir begins an upload from the standard directory structure.
// Processes all CSV files in the table's upload directory.
func (s *Service) StartUploadFromDir(ctx context.Context, tableKey string) (string, error) {
	def, ok := Get(tableKey)
	if !ok {
		return "", fmt.Errorf("unknown table: %s", tableKey)
	}

	dir := filepath.Join(s.uploadsDir, def.Info.Group, def.Info.Directory)

	uploadID := uuid.New().String()
	uploadCtx, cancel := context.WithTimeout(context.Background(), UploadTimeout)

	upload := &activeUpload{
		ID:       uploadID,
		TableKey: tableKey,
		Cancel:   cancel,
		Progress: UploadProgress{
			UploadID: uploadID,
			TableKey: tableKey,
			Phase:    PhaseStarting,
		},
		Done:      make(chan struct{}),
		Listeners: make([]chan UploadProgress, 0),
	}

	s.mu.Lock()
	s.uploads[uploadID] = upload
	s.mu.Unlock()

	go s.processUploadDir(uploadCtx, upload, def, dir)

	return uploadID, nil
}

// SubscribeProgress returns a channel that receives progress updates.
// The channel is closed when the upload completes.
func (s *Service) SubscribeProgress(uploadID string) (<-chan UploadProgress, error) {
	s.mu.RLock()
	upload, ok := s.uploads[uploadID]
	s.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("upload not found: %s", uploadID)
	}

	ch := make(chan UploadProgress, 10)

	upload.ListenerMu.Lock()
	upload.Listeners = append(upload.Listeners, ch)
	// Send current progress immediately
	select {
	case ch <- upload.Progress:
	default:
	}
	upload.ListenerMu.Unlock()

	return ch, nil
}

// CancelUpload cancels an in-progress upload.
func (s *Service) CancelUpload(uploadID string) error {
	s.mu.RLock()
	upload, ok := s.uploads[uploadID]
	s.mu.RUnlock()

	if !ok {
		return fmt.Errorf("upload not found: %s", uploadID)
	}

	upload.Cancel()
	return nil
}

// GetUploadResult returns the result of a completed upload.
// Blocks until the upload completes if still in progress.
func (s *Service) GetUploadResult(uploadID string) (*UploadResult, error) {
	s.mu.RLock()
	upload, ok := s.uploads[uploadID]
	s.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("upload not found: %s", uploadID)
	}

	// Wait for completion
	<-upload.Done

	return upload.Result, nil
}

// GetUploadProgress returns the current progress without blocking.
func (s *Service) GetUploadProgress(uploadID string) (UploadProgress, error) {
	s.mu.RLock()
	upload, ok := s.uploads[uploadID]
	s.mu.RUnlock()

	if !ok {
		return UploadProgress{}, fmt.Errorf("upload not found: %s", uploadID)
	}

	return upload.Progress, nil
}

// Reset deletes all data from a specific table.
func (s *Service) Reset(ctx context.Context, tableKey string) error {
	def, ok := Get(tableKey)
	if !ok {
		return fmt.Errorf("unknown table: %s", tableKey)
	}

	ctx, cancel := context.WithTimeout(ctx, ResetTimeout)
	defer cancel()

	return def.Reset(ctx, s.pool)
}

// ResetAll deletes all data from all registered tables.
func (s *Service) ResetAll(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, ResetTimeout)
	defer cancel()

	for _, def := range All() {
		if err := def.Reset(ctx, s.pool); err != nil {
			return fmt.Errorf("reset %s: %w", def.Info.Key, err)
		}
	}

	return nil
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

// TableStats contains statistics for a single table.
type TableStats struct {
	RowCount   int64
	LastUpload *LastUploadInfo
}

// LastUploadInfo contains information about the last upload.
type LastUploadInfo struct {
	FileName     string
	RowsInserted int32
	RowsSkipped  int32
	UploadedAt   time.Time
}

// GetTableRowCount returns the row count for a specific table.
func (s *Service) GetTableRowCount(ctx context.Context, tableKey string) (int64, error) {
	return countTable(ctx, s.pool, tableKey)
}

// GetTableStats returns row count and last upload info for a table.
func (s *Service) GetTableStats(ctx context.Context, tableKey string) (*TableStats, error) {
	count, err := countTable(ctx, s.pool, tableKey)
	if err != nil {
		return nil, err
	}

	stats := &TableStats{
		RowCount: count,
	}

	// Get last upload info
	lastUpload, err := s.GetLastUpload(ctx, tableKey)
	if err == nil && lastUpload != nil {
		stats.LastUpload = lastUpload
	}

	return stats, nil
}

// GetLastUpload returns info about the last upload for a table.
func (s *Service) GetLastUpload(ctx context.Context, tableKey string) (*LastUploadInfo, error) {
	row, err := db.New(s.pool).GetLastUpload(ctx, tableKey)
	if err != nil {
		return nil, err
	}

	if !row.UploadedAt.Valid {
		return nil, nil
	}

	return &LastUploadInfo{
		FileName:     row.FileName.String,
		RowsInserted: row.RowsInserted.Int32,
		RowsSkipped:  row.RowsSkipped.Int32,
		UploadedAt:   row.UploadedAt.Time,
	}, nil
}

// UploadHistoryEntry represents a single upload history entry.
type UploadHistoryEntry struct {
	FileName     string
	RowsInserted int32
	RowsSkipped  int32
	DurationMs   int32
	UploadedAt   time.Time
}

// GetUploadHistory returns the upload history for a table.
func (s *Service) GetUploadHistory(ctx context.Context, tableKey string) ([]UploadHistoryEntry, error) {
	rows, err := db.New(s.pool).GetUploadHistory(ctx, tableKey)
	if err != nil {
		return nil, err
	}

	entries := make([]UploadHistoryEntry, 0, len(rows))
	for _, row := range rows {
		entries = append(entries, UploadHistoryEntry{
			FileName:     row.FileName.String,
			RowsInserted: row.RowsInserted.Int32,
			RowsSkipped:  row.RowsSkipped.Int32,
			DurationMs:   row.DurationMs.Int32,
			UploadedAt:   row.UploadedAt.Time,
		})
	}

	return entries, nil
}

// countTable returns the row count for a specific table.
func countTable(ctx context.Context, pool *pgxpool.Pool, tableKey string) (int64, error) {
	q := db.New(pool)

	switch tableKey {
	case "sfdc_customers":
		return q.CountSfdcCustomers(ctx)
	case "sfdc_price_book":
		return q.CountSfdcPriceBook(ctx)
	case "sfdc_opp_detail":
		return q.CountSfdcOppDetail(ctx)
	case "ns_customers":
		return q.CountNsCustomers(ctx)
	case "ns_invoice_detail":
		return q.CountNsInvoiceDetail(ctx)
	case "ns_so_detail":
		return q.CountNsSoDetail(ctx)
	case "anrok_transactions":
		return q.CountAnrokTransactions(ctx)
	default:
		return 0, fmt.Errorf("unknown table: %s", tableKey)
	}
}
