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
	"github.com/jackc/pgx/v5/pgtype"
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

	// Get row count before reset for audit logging
	rowCount, _ := countTable(ctx, s.pool, tableKey)

	resetCtx, cancel := context.WithTimeout(ctx, ResetTimeout)
	defer cancel()

	if err := def.Reset(resetCtx, s.pool); err != nil {
		return err
	}

	// Log audit entry for table reset
	s.LogAudit(ctx, AuditLogParams{
		Action:       ActionTableReset,
		TableKey:     tableKey,
		RowsAffected: int(rowCount),
		IPAddress:    GetIPAddressFromContext(ctx),
		UserAgent:    GetUserAgentFromContext(ctx),
	})

	return nil
}

// ResetAll deletes all data from all registered tables.
func (s *Service) ResetAll(ctx context.Context) error {
	resetCtx, cancel := context.WithTimeout(ctx, ResetTimeout)
	defer cancel()

	for _, def := range All() {
		// Get row count before reset for audit logging
		rowCount, _ := countTable(ctx, s.pool, def.Info.Key)

		if err := def.Reset(resetCtx, s.pool); err != nil {
			return fmt.Errorf("reset %s: %w", def.Info.Key, err)
		}

		// Log audit entry for each table reset
		s.LogAudit(ctx, AuditLogParams{
			Action:       ActionTableReset,
			TableKey:     def.Info.Key,
			RowsAffected: int(rowCount),
			IPAddress:    GetIPAddressFromContext(ctx),
			UserAgent:    GetUserAgentFromContext(ctx),
		})
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

// RollbackUpload deletes all rows that were inserted from a specific upload.
func (s *Service) RollbackUpload(ctx context.Context, uploadID string) (RollbackResult, error) {
	result := RollbackResult{
		UploadID: uploadID,
	}

	// Parse UUID
	var pgUUID pgtype.UUID
	if err := pgUUID.Scan(uploadID); err != nil {
		result.Error = fmt.Sprintf("invalid upload ID: %v", err)
		return result, fmt.Errorf("invalid upload ID: %w", err)
	}

	// Get upload info
	upload, err := db.New(s.pool).GetUploadById(ctx, pgUUID)
	if err != nil {
		result.Error = fmt.Sprintf("upload not found: %v", err)
		return result, fmt.Errorf("get upload: %w", err)
	}

	result.TableKey = upload.Name

	// Check if already rolled back
	if upload.Status.Valid && upload.Status.String == "rolled_back" {
		result.Error = "upload already rolled back"
		return result, fmt.Errorf("upload already rolled back")
	}

	// Get table definition
	def, ok := Get(upload.Name)
	if !ok {
		result.Error = fmt.Sprintf("unknown table: %s", upload.Name)
		return result, fmt.Errorf("unknown table: %s", upload.Name)
	}

	// Check if table supports rollback
	if def.DeleteByUploadID == nil {
		result.Error = "table does not support rollback"
		return result, fmt.Errorf("table does not support rollback")
	}

	// Delete the rows
	rowsDeleted, err := def.DeleteByUploadID(ctx, s.pool, pgUUID)
	if err != nil {
		result.Error = fmt.Sprintf("delete failed: %v", err)
		return result, fmt.Errorf("delete by upload ID: %w", err)
	}

	// Mark upload as rolled back
	if err := db.New(s.pool).MarkUploadRolledBack(ctx, pgUUID); err != nil {
		// Log but don't fail - rows are already deleted
		result.Error = fmt.Sprintf("warning: rows deleted but status update failed: %v", err)
	}

	result.RowsDeleted = rowsDeleted
	result.Success = true

	// Log audit entry for rollback
	s.LogAudit(ctx, AuditLogParams{
		Action:       ActionUploadRollback,
		TableKey:     result.TableKey,
		UploadID:     uploadID,
		RowsAffected: int(rowsDeleted),
		IPAddress:    GetIPAddressFromContext(ctx),
		UserAgent:    GetUserAgentFromContext(ctx),
	})

	return result, nil
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
	Rows          []TableRow
	TotalRows     int64
	Page          int
	PageSize      int
	TotalPages    int
	Sorts         []SortSpec        // Ordered list of sort specifications (max 2)
	SortColumn    string            // Primary sort column (first in Sorts) - kept for backwards compat
	SortDir       string            // Primary sort direction - kept for backwards compat
	SearchQuery   string            // Current search term, if any
	ActiveFilters map[string]string // Active column filters: column -> "op:value"
	Aggregations  Aggregations      // Column aggregations for numeric columns
}

// buildFilterConditions generates WHERE clause conditions from filters.
// Returns conditions, args, and next arg index.
func buildFilterConditions(filters FilterSet, startArgIndex int) ([]string, []interface{}, int) {
	var conditions []string
	var args []interface{}
	argIdx := startArgIndex

	for _, f := range filters.Filters {
		condition, filterArgs, newArgIdx := buildSingleFilter(f, argIdx)
		if condition != "" {
			conditions = append(conditions, condition)
			args = append(args, filterArgs...)
			argIdx = newArgIdx
		}
	}

	return conditions, args, argIdx
}

// buildSingleFilter generates SQL for a single filter.
func buildSingleFilter(f ColumnFilter, argIdx int) (string, []interface{}, int) {
	col := quoteIdentifier(f.DBColumn)

	switch f.Operator {
	case OpContains:
		return fmt.Sprintf("%s ILIKE $%d", col, argIdx),
			[]interface{}{"%" + f.Value + "%"}, argIdx + 1

	case OpEquals:
		return fmt.Sprintf("%s = $%d", col, argIdx),
			[]interface{}{f.Value}, argIdx + 1

	case OpStartsWith:
		return fmt.Sprintf("%s ILIKE $%d", col, argIdx),
			[]interface{}{f.Value + "%"}, argIdx + 1

	case OpEndsWith:
		return fmt.Sprintf("%s ILIKE $%d", col, argIdx),
			[]interface{}{"%" + f.Value}, argIdx + 1

	case OpGreaterEq:
		return fmt.Sprintf("%s >= $%d", col, argIdx),
			[]interface{}{f.Value}, argIdx + 1

	case OpLessEq:
		return fmt.Sprintf("%s <= $%d", col, argIdx),
			[]interface{}{f.Value}, argIdx + 1

	case OpGreater:
		return fmt.Sprintf("%s > $%d", col, argIdx),
			[]interface{}{f.Value}, argIdx + 1

	case OpLess:
		return fmt.Sprintf("%s < $%d", col, argIdx),
			[]interface{}{f.Value}, argIdx + 1

	case OpIn:
		values := strings.Split(f.Value, ",")
		if len(values) == 0 {
			return "", nil, argIdx
		}
		placeholders := make([]string, len(values))
		filterArgs := make([]interface{}, len(values))
		for i, v := range values {
			placeholders[i] = fmt.Sprintf("$%d", argIdx+i)
			filterArgs[i] = strings.TrimSpace(v)
		}
		return fmt.Sprintf("%s IN (%s)", col, strings.Join(placeholders, ", ")),
			filterArgs, argIdx + len(values)

	default:
		return "", nil, argIdx
	}
}

// GetTableData fetches paginated, sorted, and optionally filtered data from any table.
func (s *Service) GetTableData(ctx context.Context, tableKey string, page, pageSize int, sorts []SortSpec, searchQuery string, filters FilterSet) (*TableDataResult, error) {
	def, ok := Get(tableKey)
	if !ok {
		return nil, fmt.Errorf("unknown table: %s", tableKey)
	}

	// Build column mappings using helper
	displayColumns := def.Info.Columns
	dbColumns := resolveDBColumns(displayColumns, def.FieldSpecs)
	quotedCols := quoteColumns(dbColumns)

	// Build WHERE clause using WhereBuilder
	wb := NewWhereBuilder()
	wb.AddSearch(searchQuery, def.FieldSpecs)
	wb.AddFilters(filters)
	whereClause, queryArgs := wb.Build()

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

	// Build ORDER BY clause from sorts (max 2 levels)
	var validSorts []SortSpec
	var orderParts []string
	for _, sort := range sorts {
		if sort.Column == "" || !containsColumn(displayColumns, sort.Column) {
			continue
		}
		dir := strings.ToLower(sort.Dir)
		if dir != "asc" && dir != "desc" {
			dir = "asc"
		}
		// Find DB column name
		sortDBColumn := toDBColumnName(sort.Column)
		for _, spec := range def.FieldSpecs {
			if spec.Name == sort.Column && spec.DBColumn != "" {
				sortDBColumn = spec.DBColumn
				break
			}
		}
		orderParts = append(orderParts, fmt.Sprintf("%s %s", quoteIdentifier(sortDBColumn), dir))
		validSorts = append(validSorts, SortSpec{Column: sort.Column, Dir: dir})
		if len(validSorts) >= 2 { // Max 2 sort levels
			break
		}
	}

	// Default to first column if no valid sorts
	if len(orderParts) == 0 {
		defaultCol := displayColumns[0]
		defaultDBCol := toDBColumnName(defaultCol)
		for _, spec := range def.FieldSpecs {
			if spec.Name == defaultCol && spec.DBColumn != "" {
				defaultDBCol = spec.DBColumn
				break
			}
		}
		orderParts = append(orderParts, fmt.Sprintf("%s asc", quoteIdentifier(defaultDBCol)))
		validSorts = append(validSorts, SortSpec{Column: defaultCol, Dir: "asc"})
	}

	// Build SELECT query with WHERE and ORDER BY clauses
	argIndex := wb.NextArgIndex()
	query := fmt.Sprintf(
		"SELECT %s FROM %s%s ORDER BY %s LIMIT $%d OFFSET $%d",
		strings.Join(quotedCols, ", "),
		quoteIdentifier(tableKey),
		whereClause,
		strings.Join(orderParts, ", "),
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

	// Build ActiveFilters map for UI state
	activeFilters := make(map[string]string)
	for _, f := range filters.Filters {
		activeFilters[f.Column] = string(f.Operator) + ":" + f.Value
	}

	// Extract primary sort for backwards compatibility
	primarySortCol := ""
	primarySortDir := ""
	if len(validSorts) > 0 {
		primarySortCol = validSorts[0].Column
		primarySortDir = validSorts[0].Dir
	}

	result := &TableDataResult{
		Rows:          resultRows,
		TotalRows:     totalRows,
		Page:          page,
		PageSize:      pageSize,
		TotalPages:    totalPages,
		Sorts:         validSorts,
		SortColumn:    primarySortCol,
		SortDir:       primarySortDir,
		SearchQuery:   searchQuery,
		ActiveFilters: activeFilters,
	}

	// Fetch aggregations for numeric columns
	if aggs, err := s.GetColumnAggregations(ctx, tableKey, searchQuery, filters); err == nil {
		result.Aggregations = aggs
	}

	return result, nil
}

// GetColumnAggregations calculates Sum, Avg, Min, Max for numeric columns.
// Uses the same WHERE clause as GetTableData to aggregate filtered data.
func (s *Service) GetColumnAggregations(ctx context.Context, tableKey string, searchQuery string, filters FilterSet) (Aggregations, error) {
	def, ok := Get(tableKey)
	if !ok {
		return nil, fmt.Errorf("unknown table: %s", tableKey)
	}

	// Identify numeric columns from FieldSpecs
	type numericCol struct {
		name     string
		dbColumn string
	}
	var numericCols []numericCol

	for _, spec := range def.FieldSpecs {
		if spec.Type == FieldNumeric {
			dbCol := spec.DBColumn
			if dbCol == "" {
				dbCol = toDBColumnName(spec.Name)
			}
			numericCols = append(numericCols, numericCol{spec.Name, dbCol})
		}
	}

	// Early return if no numeric columns
	if len(numericCols) == 0 {
		return Aggregations{}, nil
	}

	// Build WHERE clause using WhereBuilder
	wb := NewWhereBuilder()
	wb.AddSearch(searchQuery, def.FieldSpecs)
	wb.AddFilters(filters)
	whereClause, queryArgs := wb.Build()

	// Build aggregation SELECT expressions: SUM, AVG, MIN, MAX, COUNT per column
	var selectExprs []string
	for _, col := range numericCols {
		quoted := quoteIdentifier(col.dbColumn)
		selectExprs = append(selectExprs,
			fmt.Sprintf("SUM(%s)", quoted),
			fmt.Sprintf("AVG(%s)", quoted),
			fmt.Sprintf("MIN(%s)", quoted),
			fmt.Sprintf("MAX(%s)", quoted),
			fmt.Sprintf("COUNT(%s)", quoted),
		)
	}

	query := fmt.Sprintf("SELECT %s FROM %s%s",
		strings.Join(selectExprs, ", "),
		quoteIdentifier(tableKey),
		whereClause,
	)

	row := s.pool.QueryRow(ctx, query, queryArgs...)

	// Scan results - 5 values per column (sum, avg, min, max, count)
	scanDest := make([]interface{}, len(numericCols)*5)
	for i := range numericCols {
		base := i * 5
		scanDest[base] = new(*float64)   // Sum
		scanDest[base+1] = new(*float64) // Avg
		scanDest[base+2] = new(*float64) // Min
		scanDest[base+3] = new(*float64) // Max
		scanDest[base+4] = new(int64)    // Count
	}

	if err := row.Scan(scanDest...); err != nil {
		return nil, fmt.Errorf("scan aggregations: %w", err)
	}

	// Build result map
	result := make(Aggregations)
	for i, col := range numericCols {
		base := i * 5
		agg := &ColumnAggregation{
			Column: col.name,
		}

		// Extract values (handling NULL as nil)
		if v := scanDest[base].(**float64); *v != nil {
			agg.Sum = *v
		}
		if v := scanDest[base+1].(**float64); *v != nil {
			agg.Avg = *v
		}
		if v := scanDest[base+2].(**float64); *v != nil {
			agg.Min = *v
		}
		if v := scanDest[base+3].(**float64); *v != nil {
			agg.Max = *v
		}
		if v := scanDest[base+4].(*int64); v != nil {
			agg.Count = *v
		}

		result[col.name] = agg
	}

	return result, nil
}

// GetAllTableData fetches all data from a table without pagination.
// Used for CSV export. Optionally filters by search query and column filters.
func (s *Service) GetAllTableData(ctx context.Context, tableKey, searchQuery string, filters FilterSet) (*TableDataResult, error) {
	def, ok := Get(tableKey)
	if !ok {
		return nil, fmt.Errorf("unknown table: %s", tableKey)
	}

	// Build column names using helper
	displayColumns := def.Info.Columns
	dbColumns := resolveDBColumns(displayColumns, def.FieldSpecs)
	quotedCols := quoteColumns(dbColumns)

	// Build WHERE clause using WhereBuilder
	wb := NewWhereBuilder()
	wb.AddSearch(searchQuery, def.FieldSpecs)
	wb.AddFilters(filters)
	whereClause, queryArgs := wb.Build()

	// Get total count (with search and filter)
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s%s", quoteIdentifier(tableKey), whereClause)
	var totalRows int64
	err := s.pool.QueryRow(ctx, countQuery, queryArgs...).Scan(&totalRows)
	if err != nil {
		return nil, fmt.Errorf("count rows: %w", err)
	}

	// Build ActiveFilters map for UI state
	activeFilters := make(map[string]string)
	for _, f := range filters.Filters {
		activeFilters[f.Column] = string(f.Operator) + ":" + f.Value
	}

	if totalRows == 0 {
		return &TableDataResult{
			Rows:          []TableRow{},
			TotalRows:     0,
			SearchQuery:   searchQuery,
			ActiveFilters: activeFilters,
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
		Rows:          resultRows,
		TotalRows:     totalRows,
		SearchQuery:   searchQuery,
		ActiveFilters: activeFilters,
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
	dbCols := resolveDBColumns(uniqueKey, def.FieldSpecs)

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

// DeleteRows deletes rows by their unique key values.
// Keys are in format "val1|val2" for composite keys.
// Returns count of deleted rows.
func (s *Service) DeleteRows(ctx context.Context, tableKey string, keys []string) (int, error) {
	def, ok := Get(tableKey)
	if !ok {
		return 0, fmt.Errorf("unknown table: %s", tableKey)
	}

	uniqueKey := def.Info.UniqueKey
	if len(uniqueKey) == 0 {
		return 0, fmt.Errorf("table %s has no unique key defined", tableKey)
	}

	// Build DB column names for unique key columns
	dbCols := resolveDBColumns(uniqueKey, def.FieldSpecs)

	var totalDeleted int64

	// Record deletions in history before deleting
	for _, key := range keys {
		if rowData, err := s.getRowData(ctx, tableKey, key); err == nil && rowData != nil {
			s.RecordRowDelete(ctx, tableKey, key, rowData)
		}
	}

	if len(uniqueKey) == 1 {
		// Single column key - use ANY for batch efficiency
		query := fmt.Sprintf(
			"DELETE FROM %s WHERE %s = ANY($1)",
			quoteIdentifier(tableKey),
			quoteIdentifier(dbCols[0]),
		)
		result, err := s.pool.Exec(ctx, query, keys)
		if err != nil {
			return 0, fmt.Errorf("delete failed: %w", err)
		}
		totalDeleted = result.RowsAffected()
	} else {
		// Composite key - delete each key individually
		for _, key := range keys {
			parts := strings.Split(key, "|")
			if len(parts) != len(uniqueKey) {
				continue // Invalid key format, skip
			}

			conditions := make([]string, len(dbCols))
			args := make([]interface{}, len(parts))
			for i, part := range parts {
				conditions[i] = fmt.Sprintf("%s = $%d", quoteIdentifier(dbCols[i]), i+1)
				args[i] = part
			}

			query := fmt.Sprintf(
				"DELETE FROM %s WHERE %s",
				quoteIdentifier(tableKey),
				strings.Join(conditions, " AND "),
			)
			result, err := s.pool.Exec(ctx, query, args...)
			if err != nil {
				continue // Log but continue with other keys
			}
			totalDeleted += result.RowsAffected()
		}
	}

	return int(totalDeleted), nil
}

// UpdateCellRequest contains the data for updating a single cell.
type UpdateCellRequest struct {
	RowKey string // Composite key value (e.g., "val1|val2")
	Column string // Display column name
	Value  string // New value as string
}

// UpdateCellResult contains the result of a cell update.
type UpdateCellResult struct {
	Success         bool   `json:"success"`
	DuplicateKey    bool   `json:"duplicateKey,omitempty"`
	ConflictingKey  string `json:"conflictingKey,omitempty"`
	ValidationError string `json:"validationError,omitempty"`
}

// UpdateCell updates a single cell value.
func (s *Service) UpdateCell(ctx context.Context, tableKey string, req UpdateCellRequest) (*UpdateCellResult, error) {
	def, ok := Get(tableKey)
	if !ok {
		return nil, fmt.Errorf("unknown table: %s", tableKey)
	}

	uniqueKey := def.Info.UniqueKey
	if len(uniqueKey) == 0 {
		return nil, fmt.Errorf("table %s has no unique key defined", tableKey)
	}

	// Find the FieldSpec for this column
	var fieldSpec *FieldSpec
	for i := range def.FieldSpecs {
		if strings.EqualFold(def.FieldSpecs[i].Name, req.Column) {
			fieldSpec = &def.FieldSpecs[i]
			break
		}
	}

	// Determine DB column name
	dbCol := toDBColumnName(req.Column)
	if fieldSpec != nil && fieldSpec.DBColumn != "" {
		dbCol = fieldSpec.DBColumn
	}

	// Validate value against type
	if fieldSpec != nil {
		if err := validateCellValue(req.Value, *fieldSpec); err != nil {
			return &UpdateCellResult{
				Success:         false,
				ValidationError: err.Error(),
			}, nil
		}
	}

	// Check if column is part of unique key
	isUniqueKeyColumn := false
	for _, uk := range uniqueKey {
		if strings.EqualFold(uk, req.Column) {
			isUniqueKeyColumn = true
			break
		}
	}

	// If updating unique key column, check for duplicates
	if isUniqueKeyColumn && req.Value != "" {
		newKey := s.buildNewCompositeKey(uniqueKey, req.RowKey, req.Column, req.Value)

		// Check if new key exists (excluding current row)
		exists, err := s.keyExistsExcludingRow(ctx, tableKey, def, uniqueKey, newKey, req.RowKey)
		if err != nil {
			return nil, fmt.Errorf("duplicate check failed: %w", err)
		}

		if exists {
			return &UpdateCellResult{
				Success:        false,
				DuplicateKey:   true,
				ConflictingKey: newKey,
			}, nil
		}
	}

	// Fetch old value before update (for history)
	oldValue, _ := s.getCellValue(ctx, tableKey, def, uniqueKey, req.RowKey, dbCol)

	// Build and execute UPDATE query
	err := s.executeUpdateCell(ctx, tableKey, def, uniqueKey, req.RowKey, dbCol, req.Value, fieldSpec)
	if err != nil {
		return nil, fmt.Errorf("update failed: %w", err)
	}

	// Record in history (ignore errors - update already succeeded)
	s.RecordCellEdit(ctx, tableKey, req.RowKey, req.Column, oldValue, req.Value)

	return &UpdateCellResult{Success: true}, nil
}

// BulkEditRequest represents a request to edit multiple rows.
type BulkEditRequest struct {
	Keys   []string
	Column string
	Value  string
}

// BulkEditResult contains the result of a bulk edit operation.
type BulkEditResult struct {
	Updated int      `json:"updated"`
	Failed  int      `json:"failed"`
	Errors  []string `json:"errors,omitempty"`
}

// BulkEditRows updates a single column across multiple rows.
func (s *Service) BulkEditRows(ctx context.Context, tableKey string, req BulkEditRequest) (*BulkEditResult, error) {
	def, ok := Get(tableKey)
	if !ok {
		return nil, fmt.Errorf("unknown table: %s", tableKey)
	}

	uniqueKey := def.Info.UniqueKey
	if len(uniqueKey) == 0 {
		return nil, fmt.Errorf("table %s has no unique key defined", tableKey)
	}

	if len(req.Keys) == 0 {
		return nil, fmt.Errorf("no rows specified")
	}

	// Find the FieldSpec for this column
	var fieldSpec *FieldSpec
	for i := range def.FieldSpecs {
		if strings.EqualFold(def.FieldSpecs[i].Name, req.Column) {
			fieldSpec = &def.FieldSpecs[i]
			break
		}
	}
	if fieldSpec == nil {
		return nil, fmt.Errorf("column not found: %s", req.Column)
	}

	// Block editing unique key columns (would cause duplicates)
	for _, uk := range uniqueKey {
		if strings.EqualFold(uk, req.Column) {
			return nil, fmt.Errorf("cannot bulk edit unique key column: %s", req.Column)
		}
	}

	// Determine DB column name
	dbCol := toDBColumnName(req.Column)
	if fieldSpec.DBColumn != "" {
		dbCol = fieldSpec.DBColumn
	}

	// Validate value against type (once, not per row)
	if err := validateCellValue(req.Value, *fieldSpec); err != nil {
		return nil, fmt.Errorf("invalid value: %v", err)
	}

	result := &BulkEditResult{}

	// Update each row
	for _, key := range req.Keys {
		// Get old value for history
		oldValue, _ := s.getCellValue(ctx, tableKey, def, uniqueKey, key, dbCol)

		// Skip if value is unchanged
		if oldValue == req.Value {
			result.Updated++ // Count as success but no actual change
			continue
		}

		// Execute update
		err := s.executeUpdateCell(ctx, tableKey, def, uniqueKey, key, dbCol, req.Value, fieldSpec)
		if err != nil {
			result.Failed++
			result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", key, err))
			continue
		}

		// Record in history
		s.RecordCellEdit(ctx, tableKey, key, req.Column, oldValue, req.Value)
		result.Updated++
	}

	// Log audit entry for bulk edit (only if any rows were actually updated)
	if result.Updated > 0 {
		s.LogAudit(ctx, AuditLogParams{
			Action:       ActionBulkEdit,
			TableKey:     tableKey,
			ColumnName:   req.Column,
			NewValue:     req.Value,
			RowsAffected: result.Updated,
			IPAddress:    GetIPAddressFromContext(ctx),
			UserAgent:    GetUserAgentFromContext(ctx),
			Reason:       fmt.Sprintf("Bulk edited %d rows", result.Updated),
		})
	}

	return result, nil
}

// validateCellValue checks if a value is valid for the given field spec.
func validateCellValue(value string, spec FieldSpec) error {
	if value == "" {
		return nil // Empty values are allowed (will be NULL)
	}

	switch spec.Type {
	case FieldNumeric:
		result := ToPgNumeric(value)
		if !result.Valid {
			return fmt.Errorf("invalid number format")
		}
	case FieldDate:
		result := ToPgDate(value)
		if !result.Valid {
			return fmt.Errorf("invalid date format (use YYYY-MM-DD)")
		}
	case FieldBool:
		result := ToPgBool(value)
		if !result.Valid {
			return fmt.Errorf("must be yes/no, true/false, or 1/0")
		}
	case FieldEnum:
		if len(spec.EnumValues) > 0 {
			found := false
			for _, ev := range spec.EnumValues {
				if strings.EqualFold(ev, value) {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("value must be one of: %s", strings.Join(spec.EnumValues, ", "))
			}
		}
	}
	return nil
}

// buildNewCompositeKey builds the new composite key value after updating one column.
func (s *Service) buildNewCompositeKey(uniqueKey []string, oldKey, updatedColumn, newValue string) string {
	parts := strings.Split(oldKey, "|")
	if len(parts) != len(uniqueKey) {
		return newValue // Single key case
	}

	// Find which part to update
	for i, col := range uniqueKey {
		if strings.EqualFold(col, updatedColumn) {
			parts[i] = newValue
			break
		}
	}

	return strings.Join(parts, "|")
}

// keyExistsExcludingRow checks if a key exists in the table, excluding the current row.
func (s *Service) keyExistsExcludingRow(ctx context.Context, tableKey string, def TableDefinition, uniqueKey []string, newKey, currentKey string) (bool, error) {
	// Build DB column names
	dbCols := resolveDBColumns(uniqueKey, def.FieldSpecs)

	newParts := strings.Split(newKey, "|")
	currentParts := strings.Split(currentKey, "|")

	if len(newParts) != len(uniqueKey) || len(currentParts) != len(uniqueKey) {
		return false, fmt.Errorf("invalid key format")
	}

	// Build WHERE clause: new key matches AND NOT current key
	var conditions []string
	var args []interface{}
	argIdx := 1

	// Conditions for new key (must match)
	for i, dbCol := range dbCols {
		conditions = append(conditions, fmt.Sprintf("%s = $%d", quoteIdentifier(dbCol), argIdx))
		args = append(args, newParts[i])
		argIdx++
	}

	// Conditions for excluding current row (at least one must differ)
	var excludeConditions []string
	for i, dbCol := range dbCols {
		excludeConditions = append(excludeConditions, fmt.Sprintf("%s != $%d", quoteIdentifier(dbCol), argIdx))
		args = append(args, currentParts[i])
		argIdx++
	}

	query := fmt.Sprintf(
		"SELECT EXISTS(SELECT 1 FROM %s WHERE %s AND (%s))",
		quoteIdentifier(tableKey),
		strings.Join(conditions, " AND "),
		strings.Join(excludeConditions, " OR "),
	)

	var exists bool
	err := s.pool.QueryRow(ctx, query, args...).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

// executeUpdateCell performs the actual database update.
func (s *Service) executeUpdateCell(ctx context.Context, tableKey string, def TableDefinition, uniqueKey []string, rowKey, dbCol, value string, spec *FieldSpec) error {
	keyParts := strings.Split(rowKey, "|")
	if len(keyParts) != len(uniqueKey) {
		return fmt.Errorf("invalid row key format")
	}

	// Build DB column names for unique key
	dbKeyCols := resolveDBColumns(uniqueKey, def.FieldSpecs)

	// Convert value to appropriate type
	var dbValue interface{}
	if value == "" {
		dbValue = nil
	} else if spec != nil {
		switch spec.Type {
		case FieldNumeric:
			dbValue = ToPgNumeric(value)
		case FieldDate:
			dbValue = ToPgDate(value)
		case FieldBool:
			dbValue = ToPgBool(value)
		default:
			dbValue = ToPgText(value)
		}
	} else {
		dbValue = ToPgText(value)
	}

	// Build parameterized query
	conditions := make([]string, len(dbKeyCols))
	args := make([]interface{}, len(dbKeyCols)+1)

	args[0] = dbValue // Value to set is $1
	for i, keyCol := range dbKeyCols {
		conditions[i] = fmt.Sprintf("%s = $%d", quoteIdentifier(keyCol), i+2)
		args[i+1] = keyParts[i]
	}

	query := fmt.Sprintf(
		"UPDATE %s SET %s = $1 WHERE %s",
		quoteIdentifier(tableKey),
		quoteIdentifier(dbCol),
		strings.Join(conditions, " AND "),
	)

	_, err := s.pool.Exec(ctx, query, args...)
	return err
}

// ============================================================================
// Import Templates
// ============================================================================

// ImportTemplate represents a saved column mapping template.
type ImportTemplate struct {
	ID            string         `json:"id"`
	TableKey      string         `json:"tableKey"`
	Name          string         `json:"name"`
	ColumnMapping map[string]int `json:"columnMapping"`
	CSVHeaders    []string       `json:"csvHeaders"`
	CreatedAt     time.Time      `json:"createdAt"`
	UpdatedAt     time.Time      `json:"updatedAt"`
}

// TemplateMatch represents a template that matches CSV headers.
type TemplateMatch struct {
	Template   ImportTemplate `json:"template"`
	MatchScore float64        `json:"matchScore"`
}
