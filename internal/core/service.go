package core

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	Mapping    map[string]int // User-provided column mapping: expected column -> CSV index
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
// If mapping is non-nil, it maps expected column names to CSV column indices.
func (s *Service) StartUpload(ctx context.Context, tableKey string, fileName string, fileData []byte, mapping map[string]int) (string, error) {
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
		Mapping:   mapping,
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

// TableRow represents a single row of data as key-value pairs.
type TableRow map[string]interface{}

// TableDataResult contains paginated table data.
type TableDataResult struct {
	Rows        []TableRow
	TotalRows   int64
	Page        int
	PageSize    int
	TotalPages  int
	SortColumn  string
	SortDir     string
	SearchQuery string // Current search term, if any
}

// GetTableData fetches paginated, sorted, and optionally filtered data from any table.
func (s *Service) GetTableData(ctx context.Context, tableKey string, page, pageSize int, sortColumn, sortDir, searchQuery string) (*TableDataResult, error) {
	def, ok := Get(tableKey)
	if !ok {
		return nil, fmt.Errorf("unknown table: %s", tableKey)
	}

	// Build column mappings (display name -> DB column)
	displayColumns := def.Info.Columns
	dbColumns := make([]string, len(displayColumns))
	quotedCols := make([]string, len(displayColumns))
	for i, col := range displayColumns {
		dbCol := ""
		for _, spec := range def.FieldSpecs {
			if spec.Name == col {
				dbCol = spec.DBColumn
				break
			}
		}
		if dbCol == "" {
			dbCol = toDBColumnName(col)
		}
		dbColumns[i] = dbCol
		quotedCols[i] = quoteIdentifier(dbCol)
	}

	// Build WHERE clause for search (only text columns)
	var whereClause string
	var queryArgs []interface{}
	argIndex := 1

	if searchQuery != "" {
		var textConditions []string
		for _, spec := range def.FieldSpecs {
			if spec.Type == FieldText {
				dbCol := spec.DBColumn
				if dbCol == "" {
					dbCol = toDBColumnName(spec.Name)
				}
				textConditions = append(textConditions, fmt.Sprintf("%s ILIKE $1", quoteIdentifier(dbCol)))
			}
		}
		if len(textConditions) > 0 {
			whereClause = " WHERE " + strings.Join(textConditions, " OR ")
			queryArgs = append(queryArgs, "%"+searchQuery+"%")
			argIndex = 2
		}
	}

	// Get total count (with search filter)
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

	// Validate sort column
	if sortColumn == "" || !containsColumn(displayColumns, sortColumn) {
		sortColumn = displayColumns[0]
	}
	if sortDir != "asc" && sortDir != "desc" {
		sortDir = "asc"
	}

	// Find DB column name for sort column
	sortDBColumn := toDBColumnName(sortColumn)
	for _, spec := range def.FieldSpecs {
		if spec.Name == sortColumn && spec.DBColumn != "" {
			sortDBColumn = spec.DBColumn
			break
		}
	}

	// Build SELECT query with WHERE clause
	query := fmt.Sprintf(
		"SELECT %s FROM %s%s ORDER BY %s %s LIMIT $%d OFFSET $%d",
		strings.Join(quotedCols, ", "),
		quoteIdentifier(tableKey),
		whereClause,
		quoteIdentifier(sortDBColumn),
		sortDir,
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

	// Collect results
	var resultRows []TableRow
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("read row values: %w", err)
		}

		row := make(TableRow)
		for i, col := range displayColumns {
			row[col] = values[i]
		}
		resultRows = append(resultRows, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return &TableDataResult{
		Rows:        resultRows,
		TotalRows:   totalRows,
		Page:        page,
		PageSize:    pageSize,
		TotalPages:  totalPages,
		SortColumn:  sortColumn,
		SortDir:     sortDir,
		SearchQuery: searchQuery,
	}, nil
}

// GetAllTableData fetches all data from a table without pagination.
// Used for CSV export. Optionally filters by search query.
func (s *Service) GetAllTableData(ctx context.Context, tableKey, searchQuery string) (*TableDataResult, error) {
	def, ok := Get(tableKey)
	if !ok {
		return nil, fmt.Errorf("unknown table: %s", tableKey)
	}

	// Build column names
	displayColumns := def.Info.Columns
	quotedCols := make([]string, len(displayColumns))
	for i, col := range displayColumns {
		dbCol := ""
		for _, spec := range def.FieldSpecs {
			if spec.Name == col {
				dbCol = spec.DBColumn
				break
			}
		}
		if dbCol == "" {
			dbCol = toDBColumnName(col)
		}
		quotedCols[i] = quoteIdentifier(dbCol)
	}

	// Build WHERE clause for search (only text columns)
	var whereClause string
	var queryArgs []interface{}

	if searchQuery != "" {
		var textConditions []string
		for _, spec := range def.FieldSpecs {
			if spec.Type == FieldText {
				dbCol := spec.DBColumn
				if dbCol == "" {
					dbCol = toDBColumnName(spec.Name)
				}
				textConditions = append(textConditions, fmt.Sprintf("%s ILIKE $1", quoteIdentifier(dbCol)))
			}
		}
		if len(textConditions) > 0 {
			whereClause = " WHERE " + strings.Join(textConditions, " OR ")
			queryArgs = append(queryArgs, "%"+searchQuery+"%")
		}
	}

	// Get total count (with search filter)
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s%s", quoteIdentifier(tableKey), whereClause)
	var totalRows int64
	err := s.pool.QueryRow(ctx, countQuery, queryArgs...).Scan(&totalRows)
	if err != nil {
		return nil, fmt.Errorf("count rows: %w", err)
	}

	if totalRows == 0 {
		return &TableDataResult{
			Rows:        []TableRow{},
			TotalRows:   0,
			SearchQuery: searchQuery,
		}, nil
	}

	// Query ALL rows (no LIMIT/OFFSET), sorted by first column
	query := fmt.Sprintf(
		"SELECT %s FROM %s%s ORDER BY %s ASC",
		strings.Join(quotedCols, ", "),
		quoteIdentifier(tableKey),
		whereClause,
		quotedCols[0],
	)

	rows, err := s.pool.Query(ctx, query, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("query rows: %w", err)
	}
	defer rows.Close()

	var resultRows []TableRow
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("read row values: %w", err)
		}

		row := make(TableRow)
		for i, col := range displayColumns {
			row[col] = values[i]
		}
		resultRows = append(resultRows, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return &TableDataResult{
		Rows:        resultRows,
		TotalRows:   totalRows,
		SearchQuery: searchQuery,
	}, nil
}

// containsColumn checks if a column name exists in the list.
func containsColumn(columns []string, target string) bool {
	for _, col := range columns {
		if strings.EqualFold(col, target) {
			return true
		}
	}
	return false
}

// quoteIdentifier quotes a SQL identifier to prevent injection.
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// toDBColumnName converts a display column name to a database column name.
// "Transaction ID" -> "transaction_id"
// "account_name" -> "account_name" (no change if already snake_case)
func toDBColumnName(name string) string {
	// Replace spaces with underscores and convert to lowercase
	return strings.ToLower(strings.ReplaceAll(name, " ", "_"))
}

// CheckDuplicates checks if any of the provided keys already exist in the database.
// Keys are expected to be in the format "val1|val2" for composite keys.
// Returns the list of keys that already exist in the database.
func (s *Service) CheckDuplicates(ctx context.Context, tableKey string, keys []string) ([]string, error) {
	def, ok := Get(tableKey)
	if !ok {
		return nil, fmt.Errorf("unknown table: %s", tableKey)
	}

	uniqueKey := def.Info.UniqueKey
	if len(uniqueKey) == 0 {
		return []string{}, nil // No unique key defined, can't check duplicates
	}

	// Build the DB column names for unique key columns
	dbCols := make([]string, len(uniqueKey))
	for i, col := range uniqueKey {
		// First check FieldSpecs for explicit DBColumn
		dbCol := ""
		for _, spec := range def.FieldSpecs {
			if strings.EqualFold(spec.Name, col) && spec.DBColumn != "" {
				dbCol = spec.DBColumn
				break
			}
		}
		if dbCol == "" {
			dbCol = toDBColumnName(col)
		}
		dbCols[i] = dbCol
	}

	// Build query based on single vs composite key
	var query string
	var args []interface{}

	if len(uniqueKey) == 1 {
		// Single column unique key - use ANY for efficiency
		query = fmt.Sprintf(
			"SELECT DISTINCT %s FROM %s WHERE %s = ANY($1)",
			quoteIdentifier(dbCols[0]),
			quoteIdentifier(tableKey),
			quoteIdentifier(dbCols[0]),
		)
		args = []interface{}{keys}
	} else {
		// Composite key - need to parse keys and use tuple comparison
		// Keys are in format "val1|val2"
		var conditions []string
		argIndex := 1

		for _, key := range keys {
			parts := strings.Split(key, "|")
			if len(parts) != len(uniqueKey) {
				continue // Invalid key format
			}

			placeholders := make([]string, len(parts))
			for i, part := range parts {
				placeholders[i] = fmt.Sprintf("$%d", argIndex)
				args = append(args, part)
				argIndex++
			}

			condition := fmt.Sprintf("(%s) = (%s)",
				strings.Join(quoteColumns(dbCols), ", "),
				strings.Join(placeholders, ", "),
			)
			conditions = append(conditions, condition)
		}

		if len(conditions) == 0 {
			return []string{}, nil // No valid keys to check
		}

		// Select concatenated key columns
		concatExpr := make([]string, len(dbCols))
		for i, col := range dbCols {
			concatExpr[i] = fmt.Sprintf("COALESCE(%s::text, '')", quoteIdentifier(col))
		}

		query = fmt.Sprintf(
			"SELECT DISTINCT %s FROM %s WHERE %s",
			strings.Join(concatExpr, " || '|' || "),
			quoteIdentifier(tableKey),
			strings.Join(conditions, " OR "),
		)
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("check duplicates: %w", err)
	}
	defer rows.Close()

	var existing []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("scan key: %w", err)
		}
		existing = append(existing, key)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return existing, nil
}

// quoteColumns quotes each column name in the slice.
func quoteColumns(cols []string) []string {
	quoted := make([]string, len(cols))
	for i, col := range cols {
		quoted[i] = quoteIdentifier(col)
	}
	return quoted
}
