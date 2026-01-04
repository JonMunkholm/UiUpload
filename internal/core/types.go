// Package core provides the business logic for CSV import operations.
// This package has no UI dependencies and can be used by any frontend.
package core

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// DBTX is the interface for database operations.
// Satisfied by both *pgxpool.Pool and pgx.Tx.
type DBTX interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
	QueryRow(context.Context, string, ...interface{}) pgx.Row
}

// FieldType represents the expected data type for a CSV field.
type FieldType int

const (
	FieldText FieldType = iota
	FieldEnum
	FieldDate
	FieldNumeric
	FieldBool
)

// FieldSpec defines validation rules for a single CSV column.
type FieldSpec struct {
	Name       string            // Column header name (must match CSV exactly)
	DBColumn   string            // Database column name (if different from Name, otherwise derived)
	Type       FieldType         // Expected data type
	Required   bool              // Column must exist in CSV header
	AllowEmpty bool              // If true, empty values are allowed even when Required
	EnumValues []string          // Valid values for FieldEnum type
	Normalizer func(string) string // Optional transformation function
}

// TableInfo contains display information about a table.
type TableInfo struct {
	Key       string   // Unique identifier: "sfdc_customers"
	Group     string   // Data source: "SFDC", "NS", "Anrok"
	Label     string   // Display name: "Customers"
	Directory string   // Upload folder: "Customers"
	Columns   []string // Header column names
	UniqueKey []string // Column(s) that form the unique key for duplicate detection
}

// HeaderIndex maps column names (lowercase) to their position in the CSV row.
type HeaderIndex map[string]int

// BuildParamsFunc builds database insert parameters from a CSV row.
// uploadID is optional - pass pgtype.UUID{} if not tracking uploads.
type BuildParamsFunc func(row []string, headerIdx HeaderIndex, uploadID pgtype.UUID) (any, error)

// InsertFunc inserts a row into the database.
type InsertFunc func(ctx context.Context, db DBTX, params any) error

// ResetFunc deletes all data from a table.
type ResetFunc func(ctx context.Context, db DBTX) error

// DeleteByUploadIDFunc deletes rows by upload ID and returns the count deleted.
type DeleteByUploadIDFunc func(ctx context.Context, db DBTX, uploadID pgtype.UUID) (int64, error)

// CopyRowFunc converts params to a row of values for COPY protocol.
// The returned slice must contain values in the same order as CopyColumns.
// Each value should be a native Go type or pgtype (e.g., pgtype.Text, pgtype.Numeric).
type CopyRowFunc func(params any) []any

// TableDefinition contains everything needed to process a table.
type TableDefinition struct {
	Info             TableInfo
	FieldSpecs       []FieldSpec
	BuildParams      BuildParamsFunc
	Insert           InsertFunc
	Reset            ResetFunc
	DeleteByUploadID DeleteByUploadIDFunc // Deletes rows by upload_id for rollback

	// Optional: PostgreSQL COPY protocol support (~10-100x faster than INSERT).
	// If both CopyColumns and CopyRow are set, bulk inserts will use COPY.
	// Falls back to Insert if either is nil or if COPY fails.
	//
	// CopyColumns must list database column names in the order that CopyRow
	// returns values. Include upload_id if the table tracks uploads.
	CopyColumns []string

	// CopyRow converts the params struct (from BuildParams) to a row slice.
	// Values must match the order of CopyColumns exactly.
	CopyRow CopyRowFunc
}

// SupportsCopy returns true if the table has COPY protocol support configured.
// When true, bulk inserts can use PostgreSQL COPY for ~10-100x faster performance.
func (t TableDefinition) SupportsCopy() bool {
	return len(t.CopyColumns) > 0 && t.CopyRow != nil
}

// UploadPhase indicates the current stage of upload processing.
type UploadPhase string

const (
	PhaseStarting   UploadPhase = "starting"
	PhaseReading    UploadPhase = "reading"
	PhaseValidating UploadPhase = "validating"
	PhaseInserting  UploadPhase = "inserting"
	PhaseComplete   UploadPhase = "complete"
	PhaseFailed     UploadPhase = "failed"
	PhaseCancelled  UploadPhase = "cancelled"
)

// UploadProgress represents the current state of an upload operation.
type UploadProgress struct {
	UploadID    string
	TableKey    string
	Phase       UploadPhase
	FileName    string
	TotalRows   int
	CurrentRow  int
	Inserted    int
	Skipped     int
	Error       string // Non-empty if Phase is PhaseFailed
	// Byte-based progress for streaming (used when TotalRows is unknown).
	// When streaming, TotalRows may be 0 and progress is calculated from bytes.
	BytesRead  int64
	BytesTotal int64
}

// Percent returns the progress as a percentage (0-100).
// Uses row-based progress if TotalRows is known, otherwise falls back to byte-based.
func (p UploadProgress) Percent() int {
	// Prefer row-based progress when available
	if p.TotalRows > 0 {
		return (p.CurrentRow * 100) / p.TotalRows
	}
	// Fall back to byte-based progress for streaming
	if p.BytesTotal > 0 {
		return int((p.BytesRead * 100) / p.BytesTotal)
	}
	return 0
}

// FailedRow contains information about a row that failed to insert.
type FailedRow struct {
	FileName   string
	LineNumber int
	Reason     string
	Data       []string
}

// UploadResult contains the final result of an upload operation.
type UploadResult struct {
	UploadID   string
	TableKey   string
	FileName   string
	TotalRows  int
	Inserted   int
	Skipped    int
	FailedRows []FailedRow
	Duration   time.Duration
	Error      string // Non-empty if upload failed
}

// ProgressCallback is called periodically during upload processing.
type ProgressCallback func(UploadProgress)

// FilterOperator represents a comparison operator for column filters.
type FilterOperator string

const (
	OpContains   FilterOperator = "contains"
	OpEquals     FilterOperator = "eq"
	OpStartsWith FilterOperator = "starts"
	OpEndsWith   FilterOperator = "ends"
	OpGreaterEq  FilterOperator = "gte"
	OpLessEq     FilterOperator = "lte"
	OpGreater    FilterOperator = "gt"
	OpLess       FilterOperator = "lt"
	OpIn         FilterOperator = "in"
)

// ColumnFilter represents a single filter condition on a column.
type ColumnFilter struct {
	Column   string         // Display column name
	DBColumn string         // Database column name
	Operator FilterOperator // Comparison operator
	Value    string         // Filter value (comma-separated for OpIn)
	Type     FieldType      // Column type for proper SQL generation
}

// FilterSet represents all active filters (combined with AND logic).
type FilterSet struct {
	Filters []ColumnFilter
}

// ColumnAggregation holds aggregated values for a single numeric column.
type ColumnAggregation struct {
	Column string   // Display column name
	Sum    *float64 // nil if no valid values
	Avg    *float64
	Min    *float64
	Max    *float64
	Count  int64 // Count of non-NULL values
}

// Aggregations maps column names to their aggregation results.
type Aggregations map[string]*ColumnAggregation

// SortSpec represents a single sort column and direction.
type SortSpec struct {
	Column string // Display column name
	Dir    string // "asc" or "desc"
}

// RollbackResult contains the result of a rollback operation.
type RollbackResult struct {
	UploadID    string `json:"uploadId"`
	TableKey    string `json:"tableKey"`
	RowsDeleted int64  `json:"rowsDeleted"`
	Success     bool   `json:"success"`
	Error       string `json:"error,omitempty"`
}

// RollbackPreview contains information about what would be rolled back.
// Used for confirmation dialogs before performing a rollback.
type RollbackPreview struct {
	UploadID     string `json:"uploadId"`
	TableKey     string `json:"tableKey"`
	FileName     string `json:"fileName,omitempty"`
	UploadedAt   string `json:"uploadedAt,omitempty"`
	RowsToDelete int64  `json:"rowsToDelete"`
	CanRollback  bool   `json:"canRollback"`
	Reason       string `json:"reason,omitempty"`
}
