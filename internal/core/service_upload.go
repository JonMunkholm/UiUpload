package core

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	db "github.com/JonMunkholm/TUI/internal/database"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// StartUpload begins an asynchronous upload operation.
// Returns the upload ID immediately. Use SubscribeProgress to get updates.
// If mapping is non-nil, it maps expected column names to CSV column indices.
//
// Returns ErrTooManyUploads if the concurrent upload limit is reached and
// no slot becomes available within the timeout period.
func (s *Service) StartUpload(ctx context.Context, tableKey string, fileName string, fileData []byte, mapping map[string]int) (string, error) {
	def, ok := Get(tableKey)
	if !ok {
		return "", fmt.Errorf("unknown table: %s", tableKey)
	}

	// Acquire upload slot (blocks until available or timeout)
	if err := s.uploadLimiter.Acquire(ctx); err != nil {
		return "", err
	}

	uploadID := uuid.New().String()

	// Create cancellable context
	uploadCtx, cancel := context.WithTimeout(context.Background(), s.UploadTimeout())

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
		Mapping:   mapping,
	}

	s.mu.Lock()
	s.uploads[uploadID] = upload
	s.mu.Unlock()

	// Process in background with panic recovery to ensure limiter release
	go func() {
		defer s.uploadLimiter.Release()
		defer func() {
			if r := recover(); r != nil {
				slog.Error("panic in upload",
					"upload_id", uploadID,
					"table", tableKey,
					"panic", r,
				)
				upload.Progress.Phase = PhaseFailed
				upload.Progress.Error = fmt.Sprintf("internal error: %v", r)
				upload.notifyProgress()
				upload.closeListeners()
				close(upload.Done)
				s.cleanup(uploadID, 5*time.Minute)
			}
		}()
		s.processUpload(uploadCtx, upload, def, fileData)
	}()

	return uploadID, nil
}

// StartUploadStreaming begins an asynchronous upload operation with streaming.
// This maintains O(batch_size) constant memory usage regardless of file size.
//
// Parameters:
//   - reader: The CSV file data as an io.Reader (typically http.Request.FormFile)
//   - fileSize: Total file size in bytes for progress tracking (0 if unknown)
//
// The reader is wrapped with:
//   - BOM detection/skipping (handles Windows UTF-8 files)
//   - UTF-8 sanitization (replaces invalid sequences)
//   - Byte counting (for progress reporting)
//
// Returns ErrTooManyUploads if the concurrent upload limit is reached and
// no slot becomes available within the timeout period.
func (s *Service) StartUploadStreaming(ctx context.Context, tableKey string, fileName string, reader io.Reader, fileSize int64, mapping map[string]int) (string, error) {
	def, ok := Get(tableKey)
	if !ok {
		return "", fmt.Errorf("unknown table: %s", tableKey)
	}

	// Acquire upload slot (blocks until available or timeout)
	if err := s.uploadLimiter.Acquire(ctx); err != nil {
		return "", err
	}

	uploadID := uuid.New().String()

	// Create cancellable context
	uploadCtx, cancel := context.WithTimeout(context.Background(), s.UploadTimeout())

	upload := &activeUpload{
		ID:       uploadID,
		TableKey: tableKey,
		FileName: fileName,
		Cancel:   cancel,
		Progress: UploadProgress{
			UploadID:   uploadID,
			TableKey:   tableKey,
			Phase:      PhaseStarting,
			FileName:   fileName,
			BytesTotal: fileSize,
		},
		Done:      make(chan struct{}),
		Listeners: make([]chan UploadProgress, 0),
		Mapping:   mapping,
	}

	s.mu.Lock()
	s.uploads[uploadID] = upload
	s.mu.Unlock()

	// Wrap reader with streaming processors (BOM skip, UTF-8 sanitize, byte counting)
	streamingReader := WrapForStreaming(reader, fileSize)

	// Process in background with panic recovery to ensure limiter release
	go func() {
		defer s.uploadLimiter.Release()
		defer func() {
			if r := recover(); r != nil {
				slog.Error("panic in streaming upload",
					"upload_id", uploadID,
					"table", tableKey,
					"panic", r,
				)
				upload.Progress.Phase = PhaseFailed
				upload.Progress.Error = fmt.Sprintf("internal error: %v", r)
				upload.notifyProgress()
				upload.closeListeners()
				close(upload.Done)
				s.cleanup(uploadID, 5*time.Minute)
			}
		}()
		s.processUploadStreaming(uploadCtx, upload, def, streamingReader, fileName)
	}()

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

// GetAllTableStats returns stats for all registered tables in a single optimized query.
// This reduces N+1 queries (14 for 7 tables) to just 2 queries total.
func (s *Service) GetAllTableStats(ctx context.Context) (map[string]*TableStats, error) {
	// Get all registered table keys
	allDefs := All()
	if len(allDefs) == 0 {
		return make(map[string]*TableStats), nil
	}

	result := make(map[string]*TableStats, len(allDefs))
	for _, def := range allDefs {
		result[def.Info.Key] = &TableStats{RowCount: 0}
	}

	// Query 1: Get all row counts in a single UNION ALL query
	// Build: SELECT 'table_key' as table_key, COUNT(*) as cnt FROM table_key UNION ALL ...
	var unionParts []string
	for _, def := range allDefs {
		unionParts = append(unionParts, fmt.Sprintf(
			"SELECT '%s' as table_key, COUNT(*) as cnt FROM %s",
			def.Info.Key,
			quoteIdentifier(def.Info.Key),
		))
	}
	countQuery := strings.Join(unionParts, " UNION ALL ")

	rows, err := s.pool.Query(ctx, countQuery)
	if err != nil {
		return nil, fmt.Errorf("query table counts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableKey string
		var count int64
		if err := rows.Scan(&tableKey, &count); err != nil {
			return nil, fmt.Errorf("scan count row: %w", err)
		}
		if stats, ok := result[tableKey]; ok {
			stats.RowCount = count
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	// Query 2: Get last upload for all tables using DISTINCT ON
	// This replaces N individual GetLastUpload queries with 1 query
	lastUploadQuery := `
		SELECT DISTINCT ON (name)
			name, file_name, rows_inserted, rows_skipped, uploaded_at
		FROM csv_uploads
		WHERE status = 'active'
		ORDER BY name, uploaded_at DESC`

	uploadRows, err := s.pool.Query(ctx, lastUploadQuery)
	if err != nil {
		return nil, fmt.Errorf("query last uploads: %w", err)
	}
	defer uploadRows.Close()

	for uploadRows.Next() {
		var name string
		var fileName *string
		var rowsInserted, rowsSkipped *int32
		var uploadedAt *time.Time

		if err := uploadRows.Scan(&name, &fileName, &rowsInserted, &rowsSkipped, &uploadedAt); err != nil {
			return nil, fmt.Errorf("scan upload row: %w", err)
		}

		if stats, ok := result[name]; ok && uploadedAt != nil {
			stats.LastUpload = &LastUploadInfo{
				UploadedAt: *uploadedAt,
			}
			if fileName != nil {
				stats.LastUpload.FileName = *fileName
			}
			if rowsInserted != nil {
				stats.LastUpload.RowsInserted = *rowsInserted
			}
			if rowsSkipped != nil {
				stats.LastUpload.RowsSkipped = *rowsSkipped
			}
		}
	}
	if err := uploadRows.Err(); err != nil {
		return nil, fmt.Errorf("upload rows error: %w", err)
	}

	return result, nil
}

// UploadHistoryEntry represents a single upload history entry.
type UploadHistoryEntry struct {
	ID           string // UUID for rollback
	FileName     string
	RowsInserted int32
	RowsSkipped  int32
	DurationMs   int32
	Status       string // "active" or "rolled_back"
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
		// Convert UUID to string
		var id string
		if row.ID.Valid {
			id = fmt.Sprintf("%x-%x-%x-%x-%x",
				row.ID.Bytes[0:4], row.ID.Bytes[4:6], row.ID.Bytes[6:8],
				row.ID.Bytes[8:10], row.ID.Bytes[10:16])
		}
		entries = append(entries, UploadHistoryEntry{
			ID:           id,
			FileName:     row.FileName.String,
			RowsInserted: row.RowsInserted.Int32,
			RowsSkipped:  row.RowsSkipped.Int32,
			DurationMs:   row.DurationMs.Int32,
			Status:       row.Status.String,
			UploadedAt:   row.UploadedAt.Time,
		})
	}

	return entries, nil
}

// FailedRowExport contains data for exporting a failed row.
type FailedRowExport struct {
	LineNumber int32
	Reason     string
	RowData    []string
}

// UploadWithHeaders contains upload info including CSV headers for export.
type UploadWithHeaders struct {
	ID         string
	TableKey   string
	FileName   string
	CsvHeaders []string
}

// GetUploadWithHeaders returns upload info including CSV headers.
func (s *Service) GetUploadWithHeaders(ctx context.Context, uploadID string) (*UploadWithHeaders, error) {
	var pgUUID pgtype.UUID
	if err := pgUUID.Scan(uploadID); err != nil {
		return nil, fmt.Errorf("invalid upload ID: %w", err)
	}

	upload, err := db.New(s.pool).GetUploadById(ctx, pgUUID)
	if err != nil {
		return nil, fmt.Errorf("upload not found: %w", err)
	}

	return &UploadWithHeaders{
		ID:         uploadID,
		TableKey:   upload.Name,
		FileName:   upload.FileName.String,
		CsvHeaders: upload.CsvHeaders,
	}, nil
}

// GetFailedRows returns all failed rows for an upload.
func (s *Service) GetFailedRows(ctx context.Context, uploadID string) ([]FailedRowExport, error) {
	var pgUUID pgtype.UUID
	if err := pgUUID.Scan(uploadID); err != nil {
		return nil, fmt.Errorf("invalid upload ID: %w", err)
	}

	rows, err := db.New(s.pool).GetFailedRowsByUploadId(ctx, pgUUID)
	if err != nil {
		return nil, err
	}

	result := make([]FailedRowExport, 0, len(rows))
	for _, row := range rows {
		result = append(result, FailedRowExport{
			LineNumber: row.LineNumber,
			Reason:     row.Reason,
			RowData:    row.RowData,
		})
	}

	return result, nil
}

// UploadDetail contains full details about an upload.
type UploadDetail struct {
	ID           string
	TableKey     string
	FileName     string
	RowsInserted int
	RowsSkipped  int
	DurationMs   int
	Status       string
	CsvHeaders   []string
	UploadedAt   time.Time
}

// GetUploadDetail returns full details about an upload.
func (s *Service) GetUploadDetail(ctx context.Context, uploadID string) (*UploadDetail, error) {
	var pgUUID pgtype.UUID
	if err := pgUUID.Scan(uploadID); err != nil {
		return nil, fmt.Errorf("invalid upload ID: %w", err)
	}

	upload, err := db.New(s.pool).GetUploadById(ctx, pgUUID)
	if err != nil {
		return nil, fmt.Errorf("upload not found: %w", err)
	}

	return &UploadDetail{
		ID:           uploadID,
		TableKey:     upload.Name,
		FileName:     upload.FileName.String,
		RowsInserted: int(upload.RowsInserted.Int32),
		RowsSkipped:  int(upload.RowsSkipped.Int32),
		DurationMs:   int(upload.DurationMs.Int32),
		Status:       upload.Status.String,
		CsvHeaders:   upload.CsvHeaders,
		UploadedAt:   upload.UploadedAt.Time,
	}, nil
}

// UploadRowsResult contains paginated rows for an upload.
type UploadRowsResult struct {
	Rows       []map[string]interface{}
	TotalRows  int64
	Page       int
	PageSize   int
	TotalPages int
}

// GetUploadInsertedRows returns paginated rows inserted by a specific upload.
func (s *Service) GetUploadInsertedRows(ctx context.Context, uploadID, tableKey string, page, pageSize int) (*UploadRowsResult, error) {
	def, ok := Get(tableKey)
	if !ok {
		return nil, fmt.Errorf("unknown table: %s", tableKey)
	}

	// Build column mappings
	displayColumns := def.Info.Columns
	dbColumns := resolveDBColumns(displayColumns, def.FieldSpecs)
	quotedCols := quoteColumns(dbColumns)

	// Build WHERE clause for upload_id
	wb := NewWhereBuilder()
	wb.AddUploadID(uploadID)
	whereClause, queryArgs := wb.Build()

	// Get total count
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s%s", quoteIdentifier(tableKey), whereClause)
	var totalRows int64
	err := s.pool.QueryRow(ctx, countQuery, queryArgs...).Scan(&totalRows)
	if err != nil {
		return nil, fmt.Errorf("count rows: %w", err)
	}

	// Calculate pagination
	if page < 1 {
		page = 1
	}
	totalPages := int((totalRows + int64(pageSize) - 1) / int64(pageSize))
	if totalPages < 1 {
		totalPages = 1
	}
	if page > totalPages {
		page = totalPages
	}
	offset := (page - 1) * pageSize

	// Build SELECT query
	argIndex := wb.NextArgIndex()
	query := fmt.Sprintf(
		"SELECT %s FROM %s%s ORDER BY %s LIMIT $%d OFFSET $%d",
		strings.Join(quotedCols, ", "),
		quoteIdentifier(tableKey),
		whereClause,
		quoteIdentifier(dbColumns[0]),
		argIndex,
		argIndex+1,
	)
	queryArgs = append(queryArgs, pageSize, offset)

	// Execute query
	rows, err := s.pool.Query(ctx, query, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("query rows: %w", err)
	}
	defer rows.Close()

	// Process rows
	result := &UploadRowsResult{
		Rows:       make([]map[string]interface{}, 0),
		TotalRows:  totalRows,
		Page:       page,
		PageSize:   pageSize,
		TotalPages: totalPages,
	}

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		rowMap := make(map[string]interface{})
		for i, col := range displayColumns {
			if i < len(values) {
				rowMap[col] = values[i]
			}
		}
		result.Rows = append(result.Rows, rowMap)
	}

	return result, nil
}

// FailedRowDetail contains details about a failed row.
type FailedRowDetail struct {
	LineNumber int
	Reason     string
	RowData    []string
}

// GetUploadFailedRowsPaginated returns paginated failed rows for an upload.
func (s *Service) GetUploadFailedRowsPaginated(ctx context.Context, uploadID string, page, pageSize int) ([]FailedRowDetail, int64, error) {
	var pgUUID pgtype.UUID
	if err := pgUUID.Scan(uploadID); err != nil {
		return nil, 0, fmt.Errorf("invalid upload ID: %w", err)
	}

	// Get total count
	count, err := db.New(s.pool).CountFailedRowsByUploadId(ctx, pgUUID)
	if err != nil {
		return nil, 0, err
	}

	// Get paginated rows
	offset := (page - 1) * pageSize
	rows, err := db.New(s.pool).GetFailedRowsByUploadIdPaginated(ctx, db.GetFailedRowsByUploadIdPaginatedParams{
		UploadID: pgUUID,
		Limit:    int32(pageSize),
		Offset:   int32(offset),
	})
	if err != nil {
		return nil, 0, err
	}

	result := make([]FailedRowDetail, 0, len(rows))
	for _, row := range rows {
		result = append(result, FailedRowDetail{
			LineNumber: int(row.LineNumber),
			Reason:     row.Reason,
			RowData:    row.RowData,
		})
	}

	return result, count, nil
}
