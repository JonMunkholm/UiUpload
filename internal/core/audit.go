package core

import (
	"context"
	"encoding/json"
	"time"

	db "github.com/JonMunkholm/TUI/internal/database"
	"github.com/google/uuid"
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
	case ActionTemplateCreate, ActionTemplateDelete:
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
		UserID:   toPgText(params.UserID),
		UserEmail: toPgText(params.UserEmail),
		UserName: toPgText(params.UserName),
		UserAgent: toPgText(params.UserAgent),
		RowKey:    toPgText(params.RowKey),
		ColumnName: toPgText(params.ColumnName),
		OldValue:  toPgText(params.OldValue),
		NewValue:  toPgText(params.NewValue),
		RowData:   rowDataJSON,
		RowsAffected: toPgInt4(params.RowsAffected),
		UploadID:  toPgUUID(params.UploadID),
		BatchID:   toPgUUID(params.BatchID),
		RelatedAuditID: toPgUUID(params.RelatedAuditID),
		Reason:    toPgText(params.Reason),
	}

	// Handle IP address
	if params.IPAddress != "" {
		// Parse and set IP address - for now, just skip if invalid
		// We'll leave IpAddress as nil if not provided
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

	startTime := pgtype.Timestamptz{Time: filter.StartTime, Valid: true}
	endTime := pgtype.Timestamptz{Time: filter.EndTime, Valid: true}

	// Use far past/future if not specified
	if filter.StartTime.IsZero() {
		startTime = pgtype.Timestamptz{Time: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true}
	}
	if filter.EndTime.IsZero() {
		endTime = pgtype.Timestamptz{Time: time.Now().Add(24 * time.Hour), Valid: true}
	}

	var rows []db.AuditLog
	var err error

	if filter.TableKey != "" {
		rows, err = db.New(s.pool).GetAuditLogByTable(ctx, db.GetAuditLogByTableParams{
			TableKey:    filter.TableKey,
			CreatedAt:   startTime,
			CreatedAt_2: endTime,
			Limit:       int32(filter.Limit),
			Offset:      int32(filter.Offset),
		})
	} else {
		rows, err = db.New(s.pool).GetAuditLogAll(ctx, db.GetAuditLogAllParams{
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
		entries = append(entries, *dbAuditLogToEntry(row))
	}

	return entries, nil
}

// GetAuditLogByID retrieves a single audit log entry by ID.
func (s *Service) GetAuditLogByID(ctx context.Context, id string) (*AuditEntry, error) {
	pgUUID := toPgUUID(id)
	row, err := db.New(s.pool).GetAuditLogByID(ctx, pgUUID)
	if err != nil {
		return nil, err
	}
	return dbAuditLogToEntry(row), nil
}

// CountAuditLog returns the total count of audit log entries matching the filter.
func (s *Service) CountAuditLog(ctx context.Context, filter AuditLogFilter) (int64, error) {
	startTime := pgtype.Timestamptz{Time: filter.StartTime, Valid: true}
	endTime := pgtype.Timestamptz{Time: filter.EndTime, Valid: true}

	if filter.StartTime.IsZero() {
		startTime = pgtype.Timestamptz{Time: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true}
	}
	if filter.EndTime.IsZero() {
		endTime = pgtype.Timestamptz{Time: time.Now().Add(24 * time.Hour), Valid: true}
	}

	if filter.TableKey != "" {
		return db.New(s.pool).CountAuditLogByTable(ctx, db.CountAuditLogByTableParams{
			TableKey:    filter.TableKey,
			CreatedAt:   startTime,
			CreatedAt_2: endTime,
		})
	}

	return db.New(s.pool).CountAuditLogAll(ctx, db.CountAuditLogAllParams{
		CreatedAt:   startTime,
		CreatedAt_2: endTime,
	})
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

// Helper functions for type conversion

func toPgText(s string) pgtype.Text {
	if s == "" {
		return pgtype.Text{Valid: false}
	}
	return pgtype.Text{String: s, Valid: true}
}

func toPgInt4(i int) pgtype.Int4 {
	if i == 0 {
		return pgtype.Int4{Valid: false}
	}
	return pgtype.Int4{Int32: int32(i), Valid: true}
}

func toPgUUID(s string) pgtype.UUID {
	if s == "" {
		return pgtype.UUID{Valid: false}
	}
	parsed, err := uuid.Parse(s)
	if err != nil {
		return pgtype.UUID{Valid: false}
	}
	return pgtype.UUID{Bytes: parsed, Valid: true}
}

func uuidToString(u pgtype.UUID) string {
	if !u.Valid {
		return ""
	}
	return uuid.UUID(u.Bytes).String()
}

func dbAuditLogToEntry(row db.AuditLog) *AuditEntry {
	entry := &AuditEntry{
		ID:        uuidToString(row.ID),
		Action:    AuditAction(row.Action),
		Severity:  AuditSeverity(row.Severity),
		TableKey:  row.TableKey,
		CreatedAt: row.CreatedAt.Time,
	}

	if row.UserID.Valid {
		entry.UserID = row.UserID.String
	}
	if row.UserEmail.Valid {
		entry.UserEmail = row.UserEmail.String
	}
	if row.UserName.Valid {
		entry.UserName = row.UserName.String
	}
	if row.IpAddress != nil {
		entry.IPAddress = row.IpAddress.String()
	}
	if row.UserAgent.Valid {
		entry.UserAgent = row.UserAgent.String
	}
	if row.RowKey.Valid {
		entry.RowKey = row.RowKey.String
	}
	if row.ColumnName.Valid {
		entry.ColumnName = row.ColumnName.String
	}
	if row.OldValue.Valid {
		entry.OldValue = row.OldValue.String
	}
	if row.NewValue.Valid {
		entry.NewValue = row.NewValue.String
	}
	if row.RowData != nil {
		_ = json.Unmarshal(row.RowData, &entry.RowData)
	}
	if row.RowsAffected.Valid {
		entry.RowsAffected = int(row.RowsAffected.Int32)
	}
	entry.UploadID = uuidToString(row.UploadID)
	entry.BatchID = uuidToString(row.BatchID)
	entry.RelatedAuditID = uuidToString(row.RelatedAuditID)
	if row.Reason.Valid {
		entry.Reason = row.Reason.String
	}

	return entry
}

func dbAuditLogArchiveToEntry(row db.AuditLogArchive) *AuditEntry {
	entry := &AuditEntry{
		ID:        uuidToString(row.ID),
		Action:    AuditAction(row.Action),
		Severity:  AuditSeverity(row.Severity),
		TableKey:  row.TableKey,
		CreatedAt: row.CreatedAt.Time,
	}

	if row.UserID.Valid {
		entry.UserID = row.UserID.String
	}
	if row.UserEmail.Valid {
		entry.UserEmail = row.UserEmail.String
	}
	if row.UserName.Valid {
		entry.UserName = row.UserName.String
	}
	if row.IpAddress != nil {
		entry.IPAddress = row.IpAddress.String()
	}
	if row.UserAgent.Valid {
		entry.UserAgent = row.UserAgent.String
	}
	if row.RowKey.Valid {
		entry.RowKey = row.RowKey.String
	}
	if row.ColumnName.Valid {
		entry.ColumnName = row.ColumnName.String
	}
	if row.OldValue.Valid {
		entry.OldValue = row.OldValue.String
	}
	if row.NewValue.Valid {
		entry.NewValue = row.NewValue.String
	}
	if row.RowData != nil {
		_ = json.Unmarshal(row.RowData, &entry.RowData)
	}
	if row.RowsAffected.Valid {
		entry.RowsAffected = int(row.RowsAffected.Int32)
	}
	entry.UploadID = uuidToString(row.UploadID)
	entry.BatchID = uuidToString(row.BatchID)
	entry.RelatedAuditID = uuidToString(row.RelatedAuditID)
	if row.Reason.Valid {
		entry.Reason = row.Reason.String
	}

	return entry
}
