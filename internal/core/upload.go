package core

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"strings"
	"time"
	"unicode/utf8"

	db "github.com/JonMunkholm/TUI/internal/database"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// MaxFileSize is the maximum allowed CSV file size (100MB).
var MaxFileSize int64 = 100 * 1024 * 1024

// MaxHeaderSearchRows is the maximum number of rows to scan for the header.
var MaxHeaderSearchRows = 20

// ContextCheckInterval is how often to check for context cancellation.
var ContextCheckInterval = 100

// processUpload handles direct file upload (from web form).
// Uses streaming CSV parsing to minimize memory usage: O(batch_size) instead of O(file_size).
func (s *Service) processUpload(ctx context.Context, upload *activeUpload, def TableDefinition, fileData []byte) {
	startTime := time.Now()

	defer func() {
		upload.closeListeners()
		close(upload.Done)
		s.cleanup(upload.ID, 5*time.Minute)
	}()

	// Sanitize UTF-8 (streaming sanitization would add complexity for minimal gain)
	fileData = sanitizeUTF8(fileData)

	// Process using streaming parser (reads CSV row-by-row instead of loading all into memory)
	result := s.processStreamingRecords(ctx, upload, def, fileData, upload.FileName, startTime)
	upload.Result = result
}

// BatchSize is the number of rows to insert in a single batch.
// 500 is a good balance between network efficiency and memory usage.
var BatchSize = 500

// validatedRow holds a validated row ready for insertion.
type validatedRow struct {
	index   int      // Position in dataRows
	lineNum int      // CSV line number for error reporting
	params  any      // Built params for insertion
	row     []string // Original data for error reporting
}

// insertBatch attempts to insert a batch of rows.
// Uses a single savepoint per batch instead of per row (3x fewer round-trips).
// Returns the number of rows that failed to insert.
// On batch failure, falls back to row-by-row insertion to identify bad rows.
func (s *Service) insertBatch(ctx context.Context, tx pgx.Tx, def TableDefinition, batch []validatedRow, failedRows *[]FailedRow, fileName string) int {
	if len(batch) == 0 {
		return 0
	}

	// Try COPY if the table supports it (10-100x faster than INSERT)
	if def.SupportsCopy() {
		failed := s.insertWithCopy(ctx, tx, def, batch, failedRows, fileName)
		if failed == 0 {
			return 0
		}
		// COPY failed - fall through to savepoint-based insert
		log.Printf("COPY failed for %s, falling back to savepoint insert", def.Info.Key)
	}

	// Create savepoint for the entire batch
	_, err := tx.Exec(ctx, "SAVEPOINT batch_sp")
	if err != nil {
		// Savepoint failed - fall back to row-by-row
		return s.insertRowByRow(ctx, tx, def, batch, failedRows, fileName)
	}

	// Try inserting all rows in the batch without per-row savepoints
	allSucceeded := true
	for _, vr := range batch {
		if err := def.Insert(ctx, tx, vr.params); err != nil {
			allSucceeded = false
			break
		}
	}

	if allSucceeded {
		// Release savepoint - batch succeeded
		_, _ = tx.Exec(ctx, "RELEASE SAVEPOINT batch_sp")
		return 0
	}

	// Batch had failures - rollback and retry row-by-row to identify bad rows
	_, _ = tx.Exec(ctx, "ROLLBACK TO SAVEPOINT batch_sp")
	_, _ = tx.Exec(ctx, "RELEASE SAVEPOINT batch_sp")

	return s.insertRowByRow(ctx, tx, def, batch, failedRows, fileName)
}

// insertWithCopy uses PostgreSQL COPY protocol for bulk insertion.
// Returns the number of failed rows (0 = all succeeded).
// COPY is atomic per batch - if it fails, all rows are rejected.
func (s *Service) insertWithCopy(ctx context.Context, tx pgx.Tx, def TableDefinition, batch []validatedRow, failedRows *[]FailedRow, fileName string) int {
	// Create savepoint so we can rollback if COPY fails
	_, err := tx.Exec(ctx, "SAVEPOINT copy_sp")
	if err != nil {
		return len(batch) // Signal failure
	}

	// Convert validated rows to COPY format
	copyRows := make([][]any, len(batch))
	for i, vr := range batch {
		copyRows[i] = def.CopyRow(vr.params)
	}

	// Execute COPY
	_, err = tx.CopyFrom(ctx,
		pgx.Identifier{def.Info.Key},
		def.CopyColumns,
		pgx.CopyFromRows(copyRows),
	)

	if err != nil {
		// COPY failed - rollback and signal failure
		_, _ = tx.Exec(ctx, "ROLLBACK TO SAVEPOINT copy_sp")
		_, _ = tx.Exec(ctx, "RELEASE SAVEPOINT copy_sp")
		return len(batch) // Signal that caller should try row-by-row
	}

	// Success - release savepoint
	_, _ = tx.Exec(ctx, "RELEASE SAVEPOINT copy_sp")
	return 0
}

// insertRowByRow inserts rows one at a time with individual savepoints.
// Used as fallback when batch insert fails.
func (s *Service) insertRowByRow(ctx context.Context, tx pgx.Tx, def TableDefinition, batch []validatedRow, failedRows *[]FailedRow, fileName string) int {
	failed := 0

	for i, vr := range batch {
		savepointName := fmt.Sprintf("row_sp_%d", i)
		_, err := tx.Exec(ctx, fmt.Sprintf("SAVEPOINT %s", savepointName))
		if err != nil {
			// If we can't create savepoint, mark row as failed
			*failedRows = append(*failedRows, FailedRow{
				FileName:   fileName,
				LineNumber: vr.lineNum,
				Reason:     fmt.Sprintf("savepoint: %v", err),
				Data:       vr.row,
			})
			failed++
			continue
		}

		if err := def.Insert(ctx, tx, vr.params); err != nil {
			// Rollback and mark as failed
			_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", savepointName))
			*failedRows = append(*failedRows, FailedRow{
				FileName:   fileName,
				LineNumber: vr.lineNum,
				Reason:     fmt.Sprintf("insert: %v", err),
				Data:       vr.row,
			})
			failed++
		}

		// Release savepoint (works for both success and rollback cases)
		_, _ = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", savepointName))
	}

	return failed
}

// buildAndValidate validates a row and builds insert parameters.
func buildAndValidate(row []string, headerIdx HeaderIndex, def TableDefinition, uploadID pgtype.UUID) (any, error) {
	// Validate required fields
	for _, spec := range def.FieldSpecs {
		pos, ok := headerIdx[strings.ToLower(spec.Name)]
		if !ok || pos >= len(row) {
			if spec.Required {
				return nil, fmt.Errorf("missing required column %q", spec.Name)
			}
			continue
		}

		raw := CleanCell(row[pos])

		if raw == "" && spec.Required && !spec.AllowEmpty {
			return nil, fmt.Errorf("empty required field %q", spec.Name)
		}

		// Apply normalizer if present
		if spec.Normalizer != nil && raw != "" {
			raw = spec.Normalizer(raw)
		}

		// Type validation for required non-empty fields
		if raw != "" && spec.Required {
			switch spec.Type {
			case FieldEnum:
				valid := false
				for _, v := range spec.EnumValues {
					if strings.EqualFold(raw, v) {
						valid = true
						break
					}
				}
				if !valid {
					return nil, fmt.Errorf("invalid enum for %q: %q", spec.Name, raw)
				}
			case FieldDate:
				if !ToPgDate(raw).Valid {
					return nil, fmt.Errorf("invalid date for %q: %q", spec.Name, raw)
				}
			case FieldNumeric:
				if !ToPgNumeric(raw).Valid {
					return nil, fmt.Errorf("invalid numeric for %q: %q", spec.Name, raw)
				}
			case FieldBool:
				if !ToPgBool(raw).Valid {
					return nil, fmt.Errorf("invalid bool for %q: %q", spec.Name, raw)
				}
			}
		}
	}

	// Build params using the table's build function with uploadID
	return def.BuildParams(row, headerIdx, uploadID)
}

// Helper functions

func sanitizeUTF8(data []byte) []byte {
	if utf8.Valid(data) {
		return data
	}

	var buf bytes.Buffer
	buf.Grow(len(data))

	for len(data) > 0 {
		r, size := utf8.DecodeRune(data)
		if r == utf8.RuneError && size == 1 {
			buf.WriteRune('\uFFFD')
			data = data[1:]
		} else {
			buf.WriteRune(r)
			data = data[size:]
		}
	}

	return buf.Bytes()
}

func parseCSV(data []byte) ([][]string, error) {
	r := csv.NewReader(bytes.NewReader(data))
	r.FieldsPerRecord = -1
	r.LazyQuotes = true
	return r.ReadAll()
}

// buildMappedHeaderIndex creates a HeaderIndex from user-provided column mapping.
// mapping: expected column name -> CSV column index
// csvHeader: actual CSV header row (used for bounds checking)
func buildMappedHeaderIndex(mapping map[string]int, csvHeader []string) HeaderIndex {
	idx := make(HeaderIndex, len(mapping))
	for expectedCol, csvIdx := range mapping {
		if csvIdx >= 0 && csvIdx < len(csvHeader) {
			idx[strings.ToLower(expectedCol)] = csvIdx
		}
	}
	return idx
}

func findHeaderInRecords(records [][]string, required []string) int {
	maxRows := MaxHeaderSearchRows
	if len(records) < maxRows {
		maxRows = len(records)
	}

	for i := 0; i < maxRows; i++ {
		if equalHeaders(records[i], required) {
			return i
		}
	}
	return -1
}

func equalHeaders(a, b []string) bool {
	if len(a) < len(b) {
		return false
	}

	for i := range b {
		aa := CleanCell(a[i])
		bb := CleanCell(b[i])
		if !strings.EqualFold(aa, bb) {
			return false
		}
	}
	return true
}

func isEmptyRow(row []string) bool {
	for _, v := range row {
		if strings.TrimSpace(v) != "" {
			return false
		}
	}
	return true
}

// countingReader wraps an io.Reader to track bytes read for progress reporting.
type countingReader struct {
	r     io.Reader
	read  int64
	total int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.read += int64(n)
	return n, err
}

// stripBOM removes UTF-8 BOM (Byte Order Mark) from the start of data if present.
// BOM is 0xEF 0xBB 0xBF and some Windows programs add it to UTF-8 files.
func stripBOM(data []byte) []byte {
	if len(data) >= 3 && data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF {
		return data[3:]
	}
	return data
}

// processStreamingRecords processes CSV data with streaming parsing.
// Memory usage is O(batch_size) instead of O(file_size).
//
// The streaming approach:
// 1. Buffer first MaxHeaderSearchRows for header detection
// 2. Stream remaining rows, accumulating batches of BatchSize
// 3. Validate and insert each batch before reading more
// 4. Report progress using bytes read / total bytes
func (s *Service) processStreamingRecords(ctx context.Context, upload *activeUpload, def TableDefinition, fileData []byte, fileName string, startTime time.Time) *UploadResult {
	result := &UploadResult{
		UploadID: upload.ID,
		TableKey: upload.TableKey,
		FileName: fileName,
	}

	// Strip BOM if present
	fileData = stripBOM(fileData)

	totalBytes := int64(len(fileData))
	if totalBytes == 0 {
		result.Error = "empty file"
		return result
	}

	// Create counting reader for byte-based progress
	cr := &countingReader{
		r:     bytes.NewReader(fileData),
		total: totalBytes,
	}

	// Initialize progress with byte-based tracking
	upload.Progress.Phase = PhaseReading
	upload.Progress.BytesTotal = totalBytes
	upload.Progress.BytesRead = 0
	upload.notifyProgress()

	// Create CSV reader
	csvReader := csv.NewReader(cr)
	csvReader.FieldsPerRecord = -1 // Allow variable field counts
	csvReader.LazyQuotes = true    // Be lenient with quoting

	// Phase 1: Buffer first N rows for header detection
	headerBuffer := make([][]string, 0, MaxHeaderSearchRows)
	for i := 0; i < MaxHeaderSearchRows; i++ {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			result.Error = fmt.Sprintf("read CSV: %v", err)
			upload.Progress.Phase = PhaseFailed
			upload.Progress.Error = result.Error
			upload.notifyProgress()
			return result
		}
		headerBuffer = append(headerBuffer, row)
	}

	if len(headerBuffer) == 0 {
		result.Error = "empty file"
		return result
	}

	// Find header row
	var csvHeaderIdx HeaderIndex
	var headerRowIndex int
	var csvHeaderRow []string

	if upload.Mapping != nil && len(upload.Mapping) > 0 {
		// User provided explicit column mapping
		headerRow := headerBuffer[0]
		csvHeaderRow = headerRow
		headerRowIndex = 0
		csvHeaderIdx = buildMappedHeaderIndex(upload.Mapping, headerRow)
	} else {
		// Auto-detect header row in buffered rows
		headerIdx := findHeaderInRecords(headerBuffer, def.Info.Columns)
		if headerIdx < 0 {
			result.Error = fmt.Sprintf("header not found (expected: %v)", def.Info.Columns)
			upload.Progress.Phase = PhaseFailed
			upload.Progress.Error = result.Error
			upload.notifyProgress()
			return result
		}
		headerRowIndex = headerIdx
		csvHeaderRow = headerBuffer[headerIdx]
		csvHeaderIdx = MakeHeaderIndex(csvHeaderRow)
	}

	expectedCols := len(def.Info.Columns)

	// Create upload record for tracking
	var uploadID pgtype.UUID
	createParams := db.CreateUploadRecordParams{
		Name:   upload.TableKey,
		Action: "upload",
	}
	if fileName != "" {
		createParams.FileName.String = fileName
		createParams.FileName.Valid = true
	}
	uploadID, err := db.New(s.pool).CreateUploadRecord(ctx, createParams)
	if err != nil {
		uploadID = pgtype.UUID{}
	}

	// Begin transaction
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		result.Error = fmt.Sprintf("begin transaction: %v", err)
		upload.Progress.Phase = PhaseFailed
		upload.Progress.Error = result.Error
		upload.notifyProgress()
		return result
	}
	defer tx.Rollback(ctx)

	upload.Progress.Phase = PhaseInserting
	upload.notifyProgress()

	var failedRows []FailedRow
	var totalProcessed int
	lineNum := headerRowIndex + 2 // 1-indexed, after header

	// Pre-allocate batch slice (reused across batches)
	batch := make([]validatedRow, 0, BatchSize)

	// Helper to process and insert a batch
	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		batchFailed := s.insertBatch(ctx, tx, def, batch, &failedRows, fileName)
		batchInserted := len(batch) - batchFailed
		result.Inserted += batchInserted

		// Update progress
		upload.Progress.BytesRead = cr.read
		upload.Progress.Inserted = result.Inserted
		upload.Progress.Skipped = len(failedRows)
		upload.notifyProgress()

		// Reset batch (reuse backing array)
		batch = batch[:0]
		return nil
	}

	// Helper to validate and add a row to the batch
	processRow := func(row []string) {
		totalProcessed++

		// Skip empty rows
		if isEmptyRow(row) {
			return
		}

		// Check column count
		if len(row) < expectedCols {
			failedRows = append(failedRows, FailedRow{
				FileName:   fileName,
				LineNumber: lineNum,
				Reason:     fmt.Sprintf("expected %d columns, got %d", expectedCols, len(row)),
				Data:       row,
			})
			return
		}

		// Validate and build params
		params, err := buildAndValidate(row, csvHeaderIdx, def, uploadID)
		if err != nil {
			failedRows = append(failedRows, FailedRow{
				FileName:   fileName,
				LineNumber: lineNum,
				Reason:     err.Error(),
				Data:       row,
			})
			return
		}

		batch = append(batch, validatedRow{
			index:   totalProcessed - 1,
			lineNum: lineNum,
			params:  params,
			row:     row,
		})
	}

	// Process data rows from header buffer (after header row)
	for i := headerRowIndex + 1; i < len(headerBuffer); i++ {
		processRow(headerBuffer[i])
		lineNum++

		// Flush batch if full
		if len(batch) >= BatchSize {
			if err := flushBatch(); err != nil {
				return result
			}
		}
	}

	// Stream remaining rows from CSV
	for {
		// Check for cancellation periodically
		if totalProcessed%ContextCheckInterval == 0 {
			if ctx.Err() != nil {
				upload.Progress.Phase = PhaseCancelled
				upload.notifyProgress()
				result.Error = "cancelled"
				return result
			}
		}

		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Log parse error and continue (lenient parsing)
			failedRows = append(failedRows, FailedRow{
				FileName:   fileName,
				LineNumber: lineNum,
				Reason:     fmt.Sprintf("CSV parse error: %v", err),
			})
			lineNum++
			continue
		}

		processRow(row)
		lineNum++

		// Flush batch if full
		if len(batch) >= BatchSize {
			if err := flushBatch(); err != nil {
				return result
			}
		}
	}

	// Flush any remaining rows in the batch
	if err := flushBatch(); err != nil {
		return result
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		result.Error = fmt.Sprintf("commit: %v", err)
		upload.Progress.Phase = PhaseFailed
		upload.Progress.Error = result.Error
		upload.notifyProgress()
		return result
	}

	// Log audit entry
	var uploadIDStr string
	if uploadID.Valid {
		uploadIDStr = pgUUIDToString(uploadID)
	}
	s.LogAudit(ctx, AuditLogParams{
		Action:       ActionUpload,
		TableKey:     upload.TableKey,
		UploadID:     uploadIDStr,
		RowsAffected: result.Inserted,
		IPAddress:    GetIPAddressFromContext(ctx),
		UserAgent:    GetUserAgentFromContext(ctx),
		Reason:       fmt.Sprintf("Uploaded %s", fileName),
	})

	// Update upload record with final counts
	if uploadID.Valid {
		updateParams := db.UpdateUploadCountsParams{
			ID: uploadID,
		}
		updateParams.RowsInserted.Int32 = int32(result.Inserted)
		updateParams.RowsInserted.Valid = true
		updateParams.RowsSkipped.Int32 = int32(len(failedRows))
		updateParams.RowsSkipped.Valid = true
		updateParams.DurationMs.Int32 = int32(time.Since(startTime).Milliseconds())
		updateParams.DurationMs.Valid = true
		if err := db.New(s.pool).UpdateUploadCounts(ctx, updateParams); err != nil {
			log.Printf("failed to update upload counts: %v", err)
		}

		// Store CSV headers for failed rows export
		if len(csvHeaderRow) > 0 {
			if err := db.New(s.pool).UpdateUploadHeaders(ctx, db.UpdateUploadHeadersParams{
				ID:         uploadID,
				CsvHeaders: csvHeaderRow,
			}); err != nil {
				log.Printf("failed to update upload headers: %v", err)
			}
		}

		// Persist failed rows for later download
		if len(failedRows) > 0 {
			for _, fr := range failedRows {
				if err := db.New(s.pool).InsertFailedRow(ctx, db.InsertFailedRowParams{
					UploadID:   uploadID,
					LineNumber: int32(fr.LineNumber),
					Reason:     fr.Reason,
					RowData:    fr.Data,
				}); err != nil {
					log.Printf("failed to insert failed row: %v", err)
				}
			}
		}
	}

	result.TotalRows = totalProcessed
	result.Skipped = len(failedRows)
	result.FailedRows = failedRows
	result.Duration = time.Since(startTime)

	upload.Progress.Phase = PhaseComplete
	upload.Progress.BytesRead = cr.total
	upload.Progress.Inserted = result.Inserted
	upload.Progress.Skipped = result.Skipped
	upload.notifyProgress()

	return result
}
