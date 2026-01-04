package core

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strings"
	"time"

	db "github.com/JonMunkholm/TUI/internal/database"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// AuditService handles audit log operations.
// It provides a dedicated interface for logging, querying, and managing audit entries.
type AuditService struct {
	pool *pgxpool.Pool
}

// NewAuditService creates a new audit service.
func NewAuditService(pool *pgxpool.Pool) *AuditService {
	return &AuditService{pool: pool}
}

// ----------------------------------------------------------------------------
// Typed Audit Entry Structures
// ----------------------------------------------------------------------------

// BaseAuditEntry contains common fields for all audit entries.
type BaseAuditEntry struct {
	TableKey  string
	UserID    string
	UserEmail string
	UserName  string
	IPAddress string
	UserAgent string
}

// UploadAuditEntry for upload actions.
type UploadAuditEntry struct {
	BaseAuditEntry
	UploadID     string
	FileName     string
	RowsInserted int
	RowsSkipped  int
}

// EditAuditEntry for cell edit actions.
type EditAuditEntry struct {
	BaseAuditEntry
	RowKey   string
	Column   string
	OldValue string
	NewValue string
}

// BulkEditAuditEntry for bulk edit actions.
type BulkEditAuditEntry struct {
	BaseAuditEntry
	Column       string
	NewValue     string
	RowsAffected int
	Reason       string
}

// DeleteAuditEntry for delete actions.
type DeleteAuditEntry struct {
	BaseAuditEntry
	RowKey  string
	RowData map[string]interface{}
}

// RollbackAuditEntry for rollback actions.
type RollbackAuditEntry struct {
	BaseAuditEntry
	UploadID     string
	RowsAffected int
}

// ResetAuditEntry for table reset actions.
type ResetAuditEntry struct {
	BaseAuditEntry
	RowsAffected int
}

// TemplateAuditEntry for template create/update/delete actions.
type TemplateAuditEntry struct {
	BaseAuditEntry
	TemplateName string
	Reason       string
}

// ----------------------------------------------------------------------------
// Logging Methods
// ----------------------------------------------------------------------------

// LogUpload logs an upload action.
func (a *AuditService) LogUpload(ctx context.Context, entry UploadAuditEntry) (*AuditEntry, error) {
	return a.Log(ctx, AuditLogParams{
		Action:       ActionUpload,
		TableKey:     entry.TableKey,
		UserID:       entry.UserID,
		UserEmail:    entry.UserEmail,
		UserName:     entry.UserName,
		IPAddress:    entry.IPAddress,
		UserAgent:    entry.UserAgent,
		UploadID:     entry.UploadID,
		RowsAffected: entry.RowsInserted,
		Reason:       fmt.Sprintf("Uploaded %s: %d inserted, %d skipped", entry.FileName, entry.RowsInserted, entry.RowsSkipped),
	})
}

// LogEdit logs a cell edit action.
func (a *AuditService) LogEdit(ctx context.Context, entry EditAuditEntry) (*AuditEntry, error) {
	return a.Log(ctx, AuditLogParams{
		Action:       ActionCellEdit,
		TableKey:     entry.TableKey,
		UserID:       entry.UserID,
		UserEmail:    entry.UserEmail,
		UserName:     entry.UserName,
		IPAddress:    entry.IPAddress,
		UserAgent:    entry.UserAgent,
		RowKey:       entry.RowKey,
		ColumnName:   entry.Column,
		OldValue:     entry.OldValue,
		NewValue:     entry.NewValue,
		RowsAffected: 1,
	})
}

// LogBulkEdit logs a bulk edit action.
func (a *AuditService) LogBulkEdit(ctx context.Context, entry BulkEditAuditEntry) (*AuditEntry, error) {
	return a.Log(ctx, AuditLogParams{
		Action:       ActionBulkEdit,
		TableKey:     entry.TableKey,
		UserID:       entry.UserID,
		UserEmail:    entry.UserEmail,
		UserName:     entry.UserName,
		IPAddress:    entry.IPAddress,
		UserAgent:    entry.UserAgent,
		ColumnName:   entry.Column,
		NewValue:     entry.NewValue,
		RowsAffected: entry.RowsAffected,
		Reason:       entry.Reason,
	})
}

// LogDelete logs a row delete action.
func (a *AuditService) LogDelete(ctx context.Context, entry DeleteAuditEntry) (*AuditEntry, error) {
	return a.Log(ctx, AuditLogParams{
		Action:       ActionRowDelete,
		TableKey:     entry.TableKey,
		UserID:       entry.UserID,
		UserEmail:    entry.UserEmail,
		UserName:     entry.UserName,
		IPAddress:    entry.IPAddress,
		UserAgent:    entry.UserAgent,
		RowKey:       entry.RowKey,
		RowData:      entry.RowData,
		RowsAffected: 1,
	})
}

// LogRollback logs a rollback action.
func (a *AuditService) LogRollback(ctx context.Context, entry RollbackAuditEntry) (*AuditEntry, error) {
	return a.Log(ctx, AuditLogParams{
		Action:       ActionUploadRollback,
		TableKey:     entry.TableKey,
		UserID:       entry.UserID,
		UserEmail:    entry.UserEmail,
		UserName:     entry.UserName,
		IPAddress:    entry.IPAddress,
		UserAgent:    entry.UserAgent,
		UploadID:     entry.UploadID,
		RowsAffected: entry.RowsAffected,
	})
}

// LogReset logs a table reset action.
func (a *AuditService) LogReset(ctx context.Context, entry ResetAuditEntry) (*AuditEntry, error) {
	return a.Log(ctx, AuditLogParams{
		Action:       ActionTableReset,
		TableKey:     entry.TableKey,
		UserID:       entry.UserID,
		UserEmail:    entry.UserEmail,
		UserName:     entry.UserName,
		IPAddress:    entry.IPAddress,
		UserAgent:    entry.UserAgent,
		RowsAffected: entry.RowsAffected,
	})
}

// LogTemplateCreate logs a template creation action.
func (a *AuditService) LogTemplateCreate(ctx context.Context, entry TemplateAuditEntry) (*AuditEntry, error) {
	return a.Log(ctx, AuditLogParams{
		Action:    ActionTemplateCreate,
		TableKey:  entry.TableKey,
		UserID:    entry.UserID,
		UserEmail: entry.UserEmail,
		UserName:  entry.UserName,
		IPAddress: entry.IPAddress,
		UserAgent: entry.UserAgent,
		Reason:    fmt.Sprintf("Created template: %s", entry.TemplateName),
	})
}

// LogTemplateUpdate logs a template update action.
func (a *AuditService) LogTemplateUpdate(ctx context.Context, entry TemplateAuditEntry) (*AuditEntry, error) {
	return a.Log(ctx, AuditLogParams{
		Action:    ActionTemplateUpdate,
		TableKey:  entry.TableKey,
		UserID:    entry.UserID,
		UserEmail: entry.UserEmail,
		UserName:  entry.UserName,
		IPAddress: entry.IPAddress,
		UserAgent: entry.UserAgent,
		Reason:    fmt.Sprintf("Updated template: %s", entry.TemplateName),
	})
}

// LogTemplateDelete logs a template deletion action.
func (a *AuditService) LogTemplateDelete(ctx context.Context, entry TemplateAuditEntry) (*AuditEntry, error) {
	return a.Log(ctx, AuditLogParams{
		Action:    ActionTemplateDelete,
		TableKey:  entry.TableKey,
		UserID:    entry.UserID,
		UserEmail: entry.UserEmail,
		UserName:  entry.UserName,
		IPAddress: entry.IPAddress,
		UserAgent: entry.UserAgent,
		Reason:    fmt.Sprintf("Deleted template: %s", entry.TemplateName),
	})
}

// Log creates a new audit log entry using the generic params structure.
func (a *AuditService) Log(ctx context.Context, params AuditLogParams) (*AuditEntry, error) {
	severity := auditSeverity(params.Action)

	var rowDataJSON []byte
	if params.RowData != nil {
		var err error
		rowDataJSON, err = json.Marshal(params.RowData)
		if err != nil {
			rowDataJSON = nil // Fall back to nil if marshaling fails
		}
	}

	// Build insert params using helpers from convert.go
	insertParams := db.InsertAuditLogParams{
		Action:         string(params.Action),
		Severity:       string(severity),
		TableKey:       params.TableKey,
		UserID:         ToPgText(params.UserID),
		UserEmail:      ToPgText(params.UserEmail),
		UserName:       ToPgText(params.UserName),
		UserAgent:      ToPgText(params.UserAgent),
		RowKey:         ToPgText(params.RowKey),
		ColumnName:     ToPgText(params.ColumnName),
		OldValue:       ToPgText(params.OldValue),
		NewValue:       ToPgText(params.NewValue),
		RowData:        rowDataJSON,
		RowsAffected:   ToPgInt4(params.RowsAffected),
		UploadID:       ToPgUUID(params.UploadID),
		BatchID:        ToPgUUID(params.BatchID),
		RelatedAuditID: ToPgUUID(params.RelatedAuditID),
		Reason:         ToPgText(params.Reason),
	}

	// Handle IP address - strip port if present and parse
	if params.IPAddress != "" {
		host := params.IPAddress
		if h, _, err := net.SplitHostPort(params.IPAddress); err == nil {
			host = h
		}
		if addr, err := netip.ParseAddr(host); err == nil {
			insertParams.IpAddress = &addr
		}
	}

	row, err := db.New(a.pool).InsertAuditLog(ctx, insertParams)
	if err != nil {
		return nil, err
	}

	return auditRowToEntry(row), nil
}

// ----------------------------------------------------------------------------
// Query Methods
// ----------------------------------------------------------------------------

// AuditLogOptions contains options for querying audit logs.
type AuditLogOptions struct {
	TableKey  string
	Action    AuditAction
	Severity  string
	StartTime time.Time
	EndTime   time.Time
	Limit     int
	Offset    int
}

// AuditLogResult contains the result of an audit log query.
type AuditLogResult struct {
	Entries    []AuditEntry
	TotalCount int64
	Page       int
	PageSize   int
	TotalPages int
}

// GetAuditLog retrieves audit log entries with optional filtering.
func (a *AuditService) GetAuditLog(ctx context.Context, opts AuditLogOptions) (*AuditLogResult, error) {
	if opts.Limit <= 0 {
		opts.Limit = DefaultHistoryLimit
	}

	// Build WHERE clause dynamically
	wb := NewWhereBuilder()
	wb.Add("action", string(opts.Action))
	wb.Add("table_key", opts.TableKey)
	wb.Add("severity", opts.Severity)

	// Add time range (always applied)
	startTime := opts.StartTime
	if startTime.IsZero() {
		startTime = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	endTime := opts.EndTime
	if endTime.IsZero() {
		endTime = time.Now().Add(24 * time.Hour)
	}
	wb.AddTimestampRange("created_at", startTime, endTime)

	whereClause, args := wb.Build()

	// Get total count
	countQuery := "SELECT COUNT(*) FROM audit_log" + whereClause
	var totalCount int64
	if err := a.pool.QueryRow(ctx, countQuery, args...).Scan(&totalCount); err != nil {
		return nil, err
	}

	// Build complete query
	query := `SELECT id, action, severity, table_key, user_id, user_email, user_name,
		ip_address, user_agent, row_key, column_name, old_value, new_value,
		row_data, rows_affected, upload_id, batch_id, related_audit_id, reason, created_at
		FROM audit_log` + whereClause + ` ORDER BY created_at DESC LIMIT $` +
		fmt.Sprintf("%d OFFSET $%d", wb.NextArgIndex(), wb.NextArgIndex()+1)
	args = append(args, opts.Limit, opts.Offset)

	rows, err := a.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := make([]AuditEntry, 0)
	for rows.Next() {
		entry, err := scanAuditRow(rows)
		if err != nil {
			return nil, err
		}
		entries = append(entries, *entry)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Calculate pagination
	page := (opts.Offset / opts.Limit) + 1
	totalPages := int((totalCount + int64(opts.Limit) - 1) / int64(opts.Limit))
	if totalPages < 1 {
		totalPages = 1
	}

	return &AuditLogResult{
		Entries:    entries,
		TotalCount: totalCount,
		Page:       page,
		PageSize:   opts.Limit,
		TotalPages: totalPages,
	}, nil
}

// GetByID retrieves a single audit log entry by ID.
func (a *AuditService) GetByID(ctx context.Context, id string) (*AuditEntry, error) {
	pgUUID := ToPgUUID(id)
	row, err := db.New(a.pool).GetAuditLogByID(ctx, pgUUID)
	if err != nil {
		return nil, err
	}
	return auditRowToEntry(row), nil
}

// Count returns the total count of audit log entries matching the options.
func (a *AuditService) Count(ctx context.Context, opts AuditLogOptions) (int64, error) {
	// Build WHERE clause dynamically (same logic as GetAuditLog)
	wb := NewWhereBuilder()
	wb.Add("action", string(opts.Action))
	wb.Add("table_key", opts.TableKey)
	wb.Add("severity", opts.Severity)

	startTime := opts.StartTime
	if startTime.IsZero() {
		startTime = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	endTime := opts.EndTime
	if endTime.IsZero() {
		endTime = time.Now().Add(24 * time.Hour)
	}
	wb.AddTimestampRange("created_at", startTime, endTime)

	whereClause, args := wb.Build()
	query := "SELECT COUNT(*) FROM audit_log" + whereClause

	var count int64
	err := a.pool.QueryRow(ctx, query, args...).Scan(&count)
	return count, err
}

// ----------------------------------------------------------------------------
// Export Methods
// ----------------------------------------------------------------------------

// AuditExportOptions contains options for exporting audit logs.
type AuditExportOptions struct {
	TableKey  string
	Action    AuditAction
	Severity  string
	StartTime time.Time
	EndTime   time.Time
}

// ExportAuditLog exports audit log entries as CSV data.
func (a *AuditService) ExportAuditLog(ctx context.Context, opts AuditExportOptions) (io.Reader, error) {
	result, err := a.GetAuditLog(ctx, AuditLogOptions{
		TableKey:  opts.TableKey,
		Action:    opts.Action,
		Severity:  opts.Severity,
		StartTime: opts.StartTime,
		EndTime:   opts.EndTime,
		Limit:     ExportLimit,
		Offset:    0,
	})
	if err != nil {
		return nil, err
	}

	// Build CSV content
	var sb strings.Builder

	// Header row
	sb.WriteString("ID,Timestamp,Action,Severity,Table,User Email,User Name,IP Address,Row Key,Column,Old Value,New Value,Rows Affected,Upload ID,Reason\n")

	// Data rows
	for _, e := range result.Entries {
		sb.WriteString(fmt.Sprintf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%s\n",
			csvEscapeField(e.ID),
			e.CreatedAt.Format("2006-01-02 15:04:05"),
			csvEscapeField(string(e.Action)),
			csvEscapeField(string(e.Severity)),
			csvEscapeField(e.TableKey),
			csvEscapeField(e.UserEmail),
			csvEscapeField(e.UserName),
			csvEscapeField(e.IPAddress),
			csvEscapeField(e.RowKey),
			csvEscapeField(e.ColumnName),
			csvEscapeField(e.OldValue),
			csvEscapeField(e.NewValue),
			e.RowsAffected,
			csvEscapeField(e.UploadID),
			csvEscapeField(e.Reason),
		))
	}

	return strings.NewReader(sb.String()), nil
}

// GetForExport retrieves audit log entries for CSV export (no pagination).
func (a *AuditService) GetForExport(ctx context.Context, opts AuditLogOptions) ([]AuditEntry, error) {
	opts.Limit = ExportLimit
	opts.Offset = 0
	result, err := a.GetAuditLog(ctx, opts)
	if err != nil {
		return nil, err
	}
	return result.Entries, nil
}

// csvEscapeField escapes a string for CSV output.
func csvEscapeField(s string) string {
	if strings.ContainsAny(s, ",\"\n\r") {
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}
	return s
}

// ----------------------------------------------------------------------------
// Archive Methods
// ----------------------------------------------------------------------------

// ArchiveOldEntries archives entries older than retention period.
// Returns the number of entries archived.
func (a *AuditService) ArchiveOldEntries(ctx context.Context, retentionDays int) (int, error) {
	var archivedCount int
	err := a.pool.QueryRow(ctx, "SELECT archive_audit_log($1)", retentionDays).Scan(&archivedCount)
	if err != nil {
		return 0, err
	}
	return archivedCount, nil
}

// GetArchive retrieves archived audit log entries.
func (a *AuditService) GetArchive(ctx context.Context, opts AuditLogOptions) ([]AuditEntry, error) {
	if opts.Limit <= 0 {
		opts.Limit = DefaultHistoryLimit
	}

	startTime := pgtype.Timestamptz{Time: opts.StartTime, Valid: true}
	endTime := pgtype.Timestamptz{Time: opts.EndTime, Valid: true}

	if opts.StartTime.IsZero() {
		startTime = pgtype.Timestamptz{Time: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true}
	}
	if opts.EndTime.IsZero() {
		endTime = pgtype.Timestamptz{Time: time.Now().Add(24 * time.Hour), Valid: true}
	}

	var rows []db.AuditLogArchive
	var err error

	if opts.TableKey != "" {
		rows, err = db.New(a.pool).GetAuditLogArchiveByTable(ctx, db.GetAuditLogArchiveByTableParams{
			TableKey:    opts.TableKey,
			CreatedAt:   startTime,
			CreatedAt_2: endTime,
			Limit:       int32(opts.Limit),
			Offset:      int32(opts.Offset),
		})
	} else {
		rows, err = db.New(a.pool).GetAuditLogArchiveAll(ctx, db.GetAuditLogArchiveAllParams{
			CreatedAt:   startTime,
			CreatedAt_2: endTime,
			Limit:       int32(opts.Limit),
			Offset:      int32(opts.Offset),
		})
	}

	if err != nil {
		return nil, err
	}

	entries := make([]AuditEntry, 0, len(rows))
	for _, row := range rows {
		entries = append(entries, *auditArchiveRowToEntry(row))
	}

	return entries, nil
}

// PurgeOldArchives permanently deletes archived entries older than the specified days.
func (a *AuditService) PurgeOldArchives(ctx context.Context, daysOld int) (int, error) {
	count, err := db.New(a.pool).PurgeOldArchives(ctx, int32(daysOld))
	return int(count), err
}

// ----------------------------------------------------------------------------
// Internal helper functions
// ----------------------------------------------------------------------------

// scanAuditRow scans a single row from audit_log into an AuditEntry.
func scanAuditRow(rows pgx.Rows) (*AuditEntry, error) {
	var (
		id             pgtype.UUID
		action         string
		severity       string
		tableKey       string
		userID         pgtype.Text
		userEmail      pgtype.Text
		userName       pgtype.Text
		ipAddress      *netip.Addr
		userAgent      pgtype.Text
		rowKey         pgtype.Text
		columnName     pgtype.Text
		oldValue       pgtype.Text
		newValue       pgtype.Text
		rowData        []byte
		rowsAffected   pgtype.Int4
		uploadID       pgtype.UUID
		batchID        pgtype.UUID
		relatedAuditID pgtype.UUID
		reason         pgtype.Text
		createdAt      pgtype.Timestamptz
	)

	err := rows.Scan(
		&id, &action, &severity, &tableKey,
		&userID, &userEmail, &userName, &ipAddress, &userAgent,
		&rowKey, &columnName, &oldValue, &newValue, &rowData, &rowsAffected,
		&uploadID, &batchID, &relatedAuditID, &reason, &createdAt,
	)
	if err != nil {
		return nil, err
	}

	entry := &AuditEntry{
		ID:        PgUUIDToString(id),
		Action:    AuditAction(action),
		Severity:  AuditSeverity(severity),
		TableKey:  tableKey,
		CreatedAt: createdAt.Time,
	}

	if userID.Valid {
		entry.UserID = userID.String
	}
	if userEmail.Valid {
		entry.UserEmail = userEmail.String
	}
	if userName.Valid {
		entry.UserName = userName.String
	}
	if ipAddress != nil {
		entry.IPAddress = ipAddress.String()
	}
	if userAgent.Valid {
		entry.UserAgent = userAgent.String
	}
	if rowKey.Valid {
		entry.RowKey = rowKey.String
	}
	if columnName.Valid {
		entry.ColumnName = columnName.String
	}
	if oldValue.Valid {
		entry.OldValue = oldValue.String
	}
	if newValue.Valid {
		entry.NewValue = newValue.String
	}
	if rowData != nil {
		_ = json.Unmarshal(rowData, &entry.RowData)
	}
	if rowsAffected.Valid {
		entry.RowsAffected = int(rowsAffected.Int32)
	}
	entry.UploadID = PgUUIDToString(uploadID)
	entry.BatchID = PgUUIDToString(batchID)
	entry.RelatedAuditID = PgUUIDToString(relatedAuditID)
	if reason.Valid {
		entry.Reason = reason.String
	}

	return entry, nil
}

// auditRowToEntry converts a db.AuditLog to an AuditEntry.
func auditRowToEntry(row db.AuditLog) *AuditEntry {
	entry := &AuditEntry{
		ID:        PgUUIDToString(row.ID),
		Action:    AuditAction(row.Action),
		Severity:  AuditSeverity(row.Severity),
		TableKey:  row.TableKey,
		CreatedAt: row.CreatedAt.Time,
	}
	fillAuditOptionalFields(entry, row.UserID, row.UserEmail, row.UserName, row.IpAddress,
		row.UserAgent, row.RowKey, row.ColumnName, row.OldValue, row.NewValue,
		row.RowData, row.RowsAffected, row.UploadID, row.BatchID, row.RelatedAuditID, row.Reason)
	return entry
}

// auditArchiveRowToEntry converts a db.AuditLogArchive to an AuditEntry.
func auditArchiveRowToEntry(row db.AuditLogArchive) *AuditEntry {
	entry := &AuditEntry{
		ID:        PgUUIDToString(row.ID),
		Action:    AuditAction(row.Action),
		Severity:  AuditSeverity(row.Severity),
		TableKey:  row.TableKey,
		CreatedAt: row.CreatedAt.Time,
	}
	fillAuditOptionalFields(entry, row.UserID, row.UserEmail, row.UserName, row.IpAddress,
		row.UserAgent, row.RowKey, row.ColumnName, row.OldValue, row.NewValue,
		row.RowData, row.RowsAffected, row.UploadID, row.BatchID, row.RelatedAuditID, row.Reason)
	return entry
}

// fillAuditOptionalFields fills in optional fields on an AuditEntry.
func fillAuditOptionalFields(entry *AuditEntry,
	userID, userEmail, userName pgtype.Text,
	ipAddress *netip.Addr,
	userAgent, rowKey, columnName, oldValue, newValue pgtype.Text,
	rowData []byte,
	rowsAffected pgtype.Int4,
	uploadID, batchID, relatedAuditID pgtype.UUID,
	reason pgtype.Text,
) {
	if userID.Valid {
		entry.UserID = userID.String
	}
	if userEmail.Valid {
		entry.UserEmail = userEmail.String
	}
	if userName.Valid {
		entry.UserName = userName.String
	}
	if ipAddress != nil {
		entry.IPAddress = ipAddress.String()
	}
	if userAgent.Valid {
		entry.UserAgent = userAgent.String
	}
	if rowKey.Valid {
		entry.RowKey = rowKey.String
	}
	if columnName.Valid {
		entry.ColumnName = columnName.String
	}
	if oldValue.Valid {
		entry.OldValue = oldValue.String
	}
	if newValue.Valid {
		entry.NewValue = newValue.String
	}
	if rowData != nil {
		_ = json.Unmarshal(rowData, &entry.RowData)
	}
	if rowsAffected.Valid {
		entry.RowsAffected = int(rowsAffected.Int32)
	}
	entry.UploadID = PgUUIDToString(uploadID)
	entry.BatchID = PgUUIDToString(batchID)
	entry.RelatedAuditID = PgUUIDToString(relatedAuditID)
	if reason.Valid {
		entry.Reason = reason.String
	}
}

// auditSeverity returns the appropriate severity for an action.
func auditSeverity(action AuditAction) AuditSeverity {
	switch action {
	case ActionUpload, ActionUploadRollback, ActionBulkEdit, ActionRowDelete:
		return SeverityHigh
	case ActionTableReset:
		return SeverityCritical
	case ActionTemplateCreate, ActionTemplateUpdate, ActionTemplateDelete:
		return SeverityLow
	default:
		return SeverityMedium
	}
}
