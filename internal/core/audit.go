package core

// audit.go provides comprehensive audit logging for all data modifications.
//
// Every change to data is recorded with:
//   - Who: User ID, email, name, IP address, user agent
//   - What: Action type, affected table, row key, old/new values
//   - When: Timestamp with timezone
//   - Context: Upload ID, batch ID, related audit entries
//
// Audit entries are assigned severity levels (low/medium/high/critical) based
// on the impact of the action. Old entries are automatically archived to cold
// storage based on the configured retention policy.
//
// The audit log supports:
//   - Filtering by table, action, severity, time range
//   - Streaming export for large datasets
//   - CSV export for compliance and reporting

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/netip"
	"time"

	db "github.com/JonMunkholm/TUI/internal/database"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// AuditAction represents the type of action being audited.
type AuditAction string

const (
	ActionUpload         AuditAction = "upload"
	ActionUploadRollback AuditAction = "upload_rollback"
	ActionCellEdit       AuditAction = "cell_edit"
	ActionBulkEdit       AuditAction = "bulk_edit"
	ActionRowDelete      AuditAction = "row_delete"
	ActionRowRestore     AuditAction = "row_restore"
	ActionTableReset     AuditAction = "table_reset"
	ActionTemplateCreate AuditAction = "template_create"
	ActionTemplateUpdate AuditAction = "template_update"
	ActionTemplateDelete AuditAction = "template_delete"
)

// AuditSeverity represents the severity level of an audit entry.
type AuditSeverity string

const (
	SeverityLow      AuditSeverity = "low"
	SeverityMedium   AuditSeverity = "medium"
	SeverityHigh     AuditSeverity = "high"
	SeverityCritical AuditSeverity = "critical"
)

// AuditEntry represents a single audit log entry.
type AuditEntry struct {
	ID             string                 `json:"id"`
	Action         AuditAction            `json:"action"`
	Severity       AuditSeverity          `json:"severity"`
	TableKey       string                 `json:"tableKey"`
	UserID         string                 `json:"userId,omitempty"`
	UserEmail      string                 `json:"userEmail,omitempty"`
	UserName       string                 `json:"userName,omitempty"`
	IPAddress      string                 `json:"ipAddress,omitempty"`
	UserAgent      string                 `json:"userAgent,omitempty"`
	RowKey         string                 `json:"rowKey,omitempty"`
	ColumnName     string                 `json:"columnName,omitempty"`
	OldValue       string                 `json:"oldValue,omitempty"`
	NewValue       string                 `json:"newValue,omitempty"`
	RowData        map[string]interface{} `json:"rowData,omitempty"`
	RowsAffected   int                    `json:"rowsAffected,omitempty"`
	UploadID       string                 `json:"uploadId,omitempty"`
	BatchID        string                 `json:"batchId,omitempty"`
	RelatedAuditID string                 `json:"relatedAuditId,omitempty"`
	Reason         string                 `json:"reason,omitempty"`
	CreatedAt      time.Time              `json:"createdAt"`
}

// AuditLogParams contains parameters for creating an audit log entry.
type AuditLogParams struct {
	Action         AuditAction
	TableKey       string
	UserID         string
	UserEmail      string
	UserName       string
	IPAddress      string
	UserAgent      string
	RowKey         string
	ColumnName     string
	OldValue       string
	NewValue       string
	RowData        map[string]interface{}
	RowsAffected   int
	UploadID       string
	BatchID        string
	RelatedAuditID string
	Reason         string
}

// determineSeverity returns the appropriate severity for an action.
func determineSeverity(action AuditAction) AuditSeverity {
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

// LogAudit creates a new audit log entry.
func (s *Service) LogAudit(ctx context.Context, params AuditLogParams) (*AuditEntry, error) {
	severity := determineSeverity(params.Action)

	var rowDataJSON []byte
	if params.RowData != nil {
		var err error
		rowDataJSON, err = json.Marshal(params.RowData)
		if err != nil {
			rowDataJSON = nil // Fall back to nil if marshaling fails
		}
	}

	// Build insert params
	insertParams := db.InsertAuditLogParams{
		Action:   string(params.Action),
		Severity: string(severity),
		TableKey: params.TableKey,
		UserID:   ToPgText(params.UserID),
		UserEmail: ToPgText(params.UserEmail),
		UserName: ToPgText(params.UserName),
		UserAgent: ToPgText(params.UserAgent),
		RowKey:    ToPgText(params.RowKey),
		ColumnName: ToPgText(params.ColumnName),
		OldValue:  ToPgText(params.OldValue),
		NewValue:  ToPgText(params.NewValue),
		RowData:   rowDataJSON,
		RowsAffected: ToPgInt4(params.RowsAffected),
		UploadID:  ToPgUUID(params.UploadID),
		BatchID:   ToPgUUID(params.BatchID),
		RelatedAuditID: ToPgUUID(params.RelatedAuditID),
		Reason:    ToPgText(params.Reason),
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

	row, err := db.New(s.pool).InsertAuditLog(ctx, insertParams)
	if err != nil {
		return nil, err
	}

	return dbAuditLogToEntry(row), nil
}

// AuditLogFilter contains filtering options for querying audit logs.
type AuditLogFilter struct {
	TableKey  string
	Action    AuditAction
	Severity  string
	StartTime time.Time
	EndTime   time.Time
	Limit     int
	Offset    int
}

// GetAuditLog retrieves audit log entries with optional filtering.
func (s *Service) GetAuditLog(ctx context.Context, filter AuditLogFilter) ([]AuditEntry, error) {
	if filter.Limit <= 0 {
		filter.Limit = DefaultHistoryLimit
	}

	// Build WHERE clause dynamically
	wb := NewWhereBuilder()
	wb.Add("action", string(filter.Action))
	wb.Add("table_key", filter.TableKey)
	wb.Add("severity", filter.Severity)

	// Add time range (always applied)
	startTime := filter.StartTime
	if startTime.IsZero() {
		startTime = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	endTime := filter.EndTime
	if endTime.IsZero() {
		endTime = time.Now().Add(24 * time.Hour)
	}
	wb.AddTimestampRange("created_at", startTime, endTime)

	whereClause, args := wb.Build()

	// Build complete query
	query := `SELECT id, action, severity, table_key, user_id, user_email, user_name,
		ip_address, user_agent, row_key, column_name, old_value, new_value,
		row_data, rows_affected, upload_id, batch_id, related_audit_id, reason, created_at
		FROM audit_log` + whereClause + ` ORDER BY created_at DESC LIMIT $` +
		fmt.Sprintf("%d OFFSET $%d", wb.NextArgIndex(), wb.NextArgIndex()+1)
	args = append(args, filter.Limit, filter.Offset)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := make([]AuditEntry, 0)
	for rows.Next() {
		entry, err := scanAuditLogRow(rows)
		if err != nil {
			return nil, err
		}
		entries = append(entries, *entry)
	}

	return entries, rows.Err()
}

// ExportLimit is the maximum number of entries to export.
const ExportLimit = 100000

// GetAuditLogForExport retrieves audit log entries for CSV export (no pagination).
func (s *Service) GetAuditLogForExport(ctx context.Context, filter AuditLogFilter) ([]AuditEntry, error) {
	filter.Limit = ExportLimit
	filter.Offset = 0
	return s.GetAuditLog(ctx, filter)
}

// ExportAuditLog returns audit entries as CSV data.
// Implements the AuditLogger interface for exporting to external systems.
func (s *Service) ExportAuditLog(ctx context.Context, opts AuditLogFilter) (io.Reader, error) {
	entries, err := s.GetAuditLogForExport(ctx, opts)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	w := csv.NewWriter(&buf)

	// Write header row
	header := []string{
		"ID", "Action", "Severity", "Table", "User Email", "User Name",
		"IP Address", "Row Key", "Column", "Old Value", "New Value",
		"Rows Affected", "Upload ID", "Reason", "Created At",
	}
	if err := w.Write(header); err != nil {
		return nil, fmt.Errorf("write CSV header: %w", err)
	}

	// Write data rows
	for _, e := range entries {
		row := []string{
			e.ID,
			string(e.Action),
			string(e.Severity),
			e.TableKey,
			e.UserEmail,
			e.UserName,
			e.IPAddress,
			e.RowKey,
			e.ColumnName,
			e.OldValue,
			e.NewValue,
			fmt.Sprintf("%d", e.RowsAffected),
			e.UploadID,
			e.Reason,
			e.CreatedAt.Format(time.RFC3339),
		}
		if err := w.Write(row); err != nil {
			return nil, fmt.Errorf("write CSV row: %w", err)
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return nil, fmt.Errorf("flush CSV: %w", err)
	}

	return bytes.NewReader(buf.Bytes()), nil
}

// GetAuditLogByID retrieves a single audit log entry by ID.
func (s *Service) GetAuditLogByID(ctx context.Context, id string) (*AuditEntry, error) {
	pgUUID := ToPgUUID(id)
	row, err := db.New(s.pool).GetAuditLogByID(ctx, pgUUID)
	if err != nil {
		return nil, err
	}
	return dbAuditLogToEntry(row), nil
}

// CountAuditLog returns the total count of audit log entries matching the filter.
func (s *Service) CountAuditLog(ctx context.Context, filter AuditLogFilter) (int64, error) {
	// Build WHERE clause dynamically (same logic as GetAuditLog)
	wb := NewWhereBuilder()
	wb.Add("action", string(filter.Action))
	wb.Add("table_key", filter.TableKey)
	wb.Add("severity", filter.Severity)

	startTime := filter.StartTime
	if startTime.IsZero() {
		startTime = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	endTime := filter.EndTime
	if endTime.IsZero() {
		endTime = time.Now().Add(24 * time.Hour)
	}
	wb.AddTimestampRange("created_at", startTime, endTime)

	whereClause, args := wb.Build()
	query := "SELECT COUNT(*) FROM audit_log" + whereClause

	var count int64
	err := s.pool.QueryRow(ctx, query, args...).Scan(&count)
	return count, err
}

// StreamAuditLog streams audit log entries row by row via callback, avoiding memory accumulation.
// Used for large CSV exports. Returns after all rows are processed or on first error.
func (s *Service) StreamAuditLog(ctx context.Context, filter AuditLogFilter, callback func(entry AuditEntry) error) error {
	// Build WHERE clause dynamically
	wb := NewWhereBuilder()
	wb.Add("action", string(filter.Action))
	wb.Add("table_key", filter.TableKey)
	wb.Add("severity", filter.Severity)

	// Add time range (always applied)
	startTime := filter.StartTime
	if startTime.IsZero() {
		startTime = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	endTime := filter.EndTime
	if endTime.IsZero() {
		endTime = time.Now().Add(24 * time.Hour)
	}
	wb.AddTimestampRange("created_at", startTime, endTime)

	whereClause, args := wb.Build()

	// Build complete query without LIMIT for streaming
	query := `SELECT id, action, severity, table_key, user_id, user_email, user_name,
		ip_address, user_agent, row_key, column_name, old_value, new_value,
		row_data, rows_affected, upload_id, batch_id, related_audit_id, reason, created_at
		FROM audit_log` + whereClause + ` ORDER BY created_at DESC`

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		// Check for context cancellation (user closed browser)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		entry, err := scanAuditLogRow(rows)
		if err != nil {
			return err
		}

		if err := callback(*entry); err != nil {
			return err
		}
	}

	return rows.Err()
}

// GetAuditLogArchive retrieves archived audit log entries.
func (s *Service) GetAuditLogArchive(ctx context.Context, filter AuditLogFilter) ([]AuditEntry, error) {
	if filter.Limit <= 0 {
		filter.Limit = DefaultHistoryLimit
	}

	startTime := pgtype.Timestamptz{Time: filter.StartTime, Valid: true}
	endTime := pgtype.Timestamptz{Time: filter.EndTime, Valid: true}

	if filter.StartTime.IsZero() {
		startTime = pgtype.Timestamptz{Time: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true}
	}
	if filter.EndTime.IsZero() {
		endTime = pgtype.Timestamptz{Time: time.Now().Add(24 * time.Hour), Valid: true}
	}

	var rows []db.AuditLogArchive
	var err error

	if filter.TableKey != "" {
		rows, err = db.New(s.pool).GetAuditLogArchiveByTable(ctx, db.GetAuditLogArchiveByTableParams{
			TableKey:    filter.TableKey,
			CreatedAt:   startTime,
			CreatedAt_2: endTime,
			Limit:       int32(filter.Limit),
			Offset:      int32(filter.Offset),
		})
	} else {
		rows, err = db.New(s.pool).GetAuditLogArchiveAll(ctx, db.GetAuditLogArchiveAllParams{
			CreatedAt:   startTime,
			CreatedAt_2: endTime,
			Limit:       int32(filter.Limit),
			Offset:      int32(filter.Offset),
		})
	}

	if err != nil {
		return nil, err
	}

	entries := make([]AuditEntry, 0, len(rows))
	for _, row := range rows {
		entries = append(entries, *dbAuditLogArchiveToEntry(row))
	}

	return entries, nil
}

// ArchiveOldAuditLogs triggers the archive function to move old entries to cold storage.
func (s *Service) ArchiveOldAuditLogs(ctx context.Context, daysToKeep int) (int, error) {
	var archivedCount int
	err := s.pool.QueryRow(ctx, "SELECT archive_audit_log($1)", daysToKeep).Scan(&archivedCount)
	if err != nil {
		return 0, err
	}
	return archivedCount, nil
}

// scanAuditLogRow scans a single row from audit_log or audit_log_archive into an AuditEntry.
// Column order must match: id, action, severity, table_key, user_id, user_email, user_name,
// ip_address, user_agent, row_key, column_name, old_value, new_value, row_data, rows_affected,
// upload_id, batch_id, related_audit_id, reason, created_at
func scanAuditLogRow(rows pgx.Rows) (*AuditEntry, error) {
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

// dbAuditLogToEntry converts a db.AuditLog to an AuditEntry.
// Used by sqlc-generated queries that return db.AuditLog.
func dbAuditLogToEntry(row db.AuditLog) *AuditEntry {
	entry := &AuditEntry{
		ID:        PgUUIDToString(row.ID),
		Action:    AuditAction(row.Action),
		Severity:  AuditSeverity(row.Severity),
		TableKey:  row.TableKey,
		CreatedAt: row.CreatedAt.Time,
	}
	populateOptionalFields(entry, row.UserID, row.UserEmail, row.UserName, row.IpAddress,
		row.UserAgent, row.RowKey, row.ColumnName, row.OldValue, row.NewValue,
		row.RowData, row.RowsAffected, row.UploadID, row.BatchID, row.RelatedAuditID, row.Reason)
	return entry
}

// dbAuditLogArchiveToEntry converts a db.AuditLogArchive to an AuditEntry.
// Used by sqlc-generated queries that return db.AuditLogArchive.
func dbAuditLogArchiveToEntry(row db.AuditLogArchive) *AuditEntry {
	entry := &AuditEntry{
		ID:        PgUUIDToString(row.ID),
		Action:    AuditAction(row.Action),
		Severity:  AuditSeverity(row.Severity),
		TableKey:  row.TableKey,
		CreatedAt: row.CreatedAt.Time,
	}
	populateOptionalFields(entry, row.UserID, row.UserEmail, row.UserName, row.IpAddress,
		row.UserAgent, row.RowKey, row.ColumnName, row.OldValue, row.NewValue,
		row.RowData, row.RowsAffected, row.UploadID, row.BatchID, row.RelatedAuditID, row.Reason)
	return entry
}

// populateOptionalFields fills in optional fields on an AuditEntry.
// This extracts common logic from dbAuditLogToEntry and dbAuditLogArchiveToEntry.
func populateOptionalFields(entry *AuditEntry,
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
