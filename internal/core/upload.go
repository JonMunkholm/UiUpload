package core

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	db "github.com/JonMunkholm/TUI/internal/database"
)

// MaxFileSize is the maximum allowed CSV file size (100MB).
var MaxFileSize int64 = 100 * 1024 * 1024

// MaxHeaderSearchRows is the maximum number of rows to scan for the header.
var MaxHeaderSearchRows = 20

// ContextCheckInterval is how often to check for context cancellation.
var ContextCheckInterval = 100

// processUpload handles direct file upload (from web form).
func (s *Service) processUpload(ctx context.Context, upload *activeUpload, def TableDefinition, fileData []byte) {
	startTime := time.Now()

	defer func() {
		upload.closeListeners()
		close(upload.Done)
		s.cleanup(upload.ID, 5*time.Minute)
	}()

	// Sanitize UTF-8
	fileData = sanitizeUTF8(fileData)

	// Parse CSV
	upload.Progress.Phase = PhaseReading
	upload.notifyProgress()

	records, err := parseCSV(fileData)
	if err != nil {
		upload.Progress.Phase = PhaseFailed
		upload.Progress.Error = err.Error()
		upload.notifyProgress()
		upload.Result = &UploadResult{
			UploadID: upload.ID,
			TableKey: upload.TableKey,
			FileName: upload.FileName,
			Error:    err.Error(),
			Duration: time.Since(startTime),
		}
		return
	}

	// Process the records
	result := s.processRecords(ctx, upload, def, records, upload.FileName, startTime)
	upload.Result = result
}

// processUploadDir handles upload from directory (for compatibility).
func (s *Service) processUploadDir(ctx context.Context, upload *activeUpload, def TableDefinition, dir string) {
	startTime := time.Now()

	defer func() {
		upload.closeListeners()
		close(upload.Done)
		s.cleanup(upload.ID, 5*time.Minute)
	}()

	// Check if directory exists
	entries, err := os.ReadDir(dir)
	if err != nil {
		upload.Progress.Phase = PhaseFailed
		upload.Progress.Error = fmt.Sprintf("read directory: %v", err)
		upload.notifyProgress()
		upload.Result = &UploadResult{
			UploadID: upload.ID,
			TableKey: upload.TableKey,
			Error:    upload.Progress.Error,
			Duration: time.Since(startTime),
		}
		return
	}

	// Find CSV files
	var csvFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(strings.ToLower(entry.Name()), ".csv") {
			csvFiles = append(csvFiles, filepath.Join(dir, entry.Name()))
		}
	}

	if len(csvFiles) == 0 {
		upload.Progress.Phase = PhaseComplete
		upload.notifyProgress()
		upload.Result = &UploadResult{
			UploadID: upload.ID,
			TableKey: upload.TableKey,
			Duration: time.Since(startTime),
		}
		return
	}

	// Process each file
	var totalInserted, totalSkipped int
	var allFailedRows []FailedRow

	for _, filePath := range csvFiles {
		if ctx.Err() != nil {
			upload.Progress.Phase = PhaseCancelled
			upload.notifyProgress()
			upload.Result = &UploadResult{
				UploadID:   upload.ID,
				TableKey:   upload.TableKey,
				Inserted:   totalInserted,
				Skipped:    totalSkipped,
				FailedRows: allFailedRows,
				Duration:   time.Since(startTime),
				Error:      "cancelled",
			}
			return
		}

		fileName := filepath.Base(filePath)
		upload.FileName = fileName
		upload.Progress.FileName = fileName
		upload.Progress.Phase = PhaseReading
		upload.notifyProgress()

		// Check if already processed
		existing, err := db.New(s.pool).GetCsvUpload(ctx, filePath)
		if err == nil && len(existing) > 0 {
			continue // Skip already processed
		}

		// Read file
		data, err := os.ReadFile(filePath)
		if err != nil {
			allFailedRows = append(allFailedRows, FailedRow{
				FileName: fileName,
				Reason:   fmt.Sprintf("read file: %v", err),
			})
			continue
		}

		if int64(len(data)) > MaxFileSize {
			allFailedRows = append(allFailedRows, FailedRow{
				FileName: fileName,
				Reason:   fmt.Sprintf("file exceeds %dMB limit", MaxFileSize/(1024*1024)),
			})
			continue
		}

		data = sanitizeUTF8(data)
		records, err := parseCSV(data)
		if err != nil {
			allFailedRows = append(allFailedRows, FailedRow{
				FileName: fileName,
				Reason:   fmt.Sprintf("parse CSV: %v", err),
			})
			continue
		}

		result := s.processRecords(ctx, upload, def, records, fileName, startTime)
		totalInserted += result.Inserted
		totalSkipped += result.Skipped
		allFailedRows = append(allFailedRows, result.FailedRows...)

		// Record successful upload
		if result.Error == "" {
			_ = db.New(s.pool).InsertCsvUpload(ctx, db.InsertCsvUploadParams{
				Name:   filePath,
				Action: "upload",
			})

			// Move to Uploaded directory
			uploadedDir := filepath.Join(dir, "Uploaded")
			_ = os.MkdirAll(uploadedDir, 0755)
			_ = os.Rename(filePath, filepath.Join(uploadedDir, fileName))
		}
	}

	upload.Progress.Phase = PhaseComplete
	upload.Progress.Inserted = totalInserted
	upload.Progress.Skipped = totalSkipped
	upload.notifyProgress()

	upload.Result = &UploadResult{
		UploadID:   upload.ID,
		TableKey:   upload.TableKey,
		Inserted:   totalInserted,
		Skipped:    totalSkipped,
		FailedRows: allFailedRows,
		Duration:   time.Since(startTime),
	}
}

// processRecords processes CSV records and inserts into database.
func (s *Service) processRecords(ctx context.Context, upload *activeUpload, def TableDefinition, records [][]string, fileName string, startTime time.Time) *UploadResult {
	result := &UploadResult{
		UploadID: upload.ID,
		TableKey: upload.TableKey,
		FileName: fileName,
	}

	if len(records) == 0 {
		result.Error = "empty file"
		return result
	}

	// Find header row
	headerIdx := findHeaderInRecords(records, def.Info.Columns)
	if headerIdx < 0 {
		result.Error = fmt.Sprintf("header not found (expected: %v)", def.Info.Columns)
		upload.Progress.Phase = PhaseFailed
		upload.Progress.Error = result.Error
		upload.notifyProgress()
		return result
	}

	headerRow := records[headerIdx]
	dataRows := records[headerIdx+1:]

	if len(dataRows) == 0 {
		result.Error = "no data rows after header"
		return result
	}

	// Build header index
	csvHeaderIdx := MakeHeaderIndex(headerRow)
	expectedCols := len(def.Info.Columns)

	upload.Progress.Phase = PhaseInserting
	upload.Progress.TotalRows = len(dataRows)
	upload.Progress.CurrentRow = 0
	upload.notifyProgress()

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

	var failedRows []FailedRow

	for i, row := range dataRows {
		lineNum := headerIdx + i + 2 // 1-indexed, after header

		// Check for cancellation periodically
		if i%ContextCheckInterval == 0 {
			if ctx.Err() != nil {
				upload.Progress.Phase = PhaseCancelled
				upload.notifyProgress()
				result.Error = "cancelled"
				return result
			}
		}

		// Skip empty rows
		if isEmptyRow(row) {
			continue
		}

		// Check column count
		if len(row) < expectedCols {
			failedRows = append(failedRows, FailedRow{
				FileName:   fileName,
				LineNumber: lineNum,
				Reason:     fmt.Sprintf("expected %d columns, got %d", expectedCols, len(row)),
				Data:       row,
			})
			continue
		}

		// Validate and build params
		params, err := buildAndValidate(row, csvHeaderIdx, def)
		if err != nil {
			failedRows = append(failedRows, FailedRow{
				FileName:   fileName,
				LineNumber: lineNum,
				Reason:     err.Error(),
				Data:       row,
			})
			continue
		}

		// Use savepoint for each insert
		savepointName := fmt.Sprintf("sp_%d", i)
		_, err = tx.Exec(ctx, fmt.Sprintf("SAVEPOINT %s", savepointName))
		if err != nil {
			result.Error = fmt.Sprintf("create savepoint: %v", err)
			upload.Progress.Phase = PhaseFailed
			upload.Progress.Error = result.Error
			upload.notifyProgress()
			return result
		}

		err = def.Insert(ctx, tx, params)
		if err != nil {
			// Rollback savepoint
			_, _ = tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", savepointName))
			failedRows = append(failedRows, FailedRow{
				FileName:   fileName,
				LineNumber: lineNum,
				Reason:     fmt.Sprintf("insert: %v", err),
				Data:       row,
			})
			continue
		}

		// Release savepoint
		_, _ = tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", savepointName))

		result.Inserted++
		upload.Progress.CurrentRow = i + 1
		upload.Progress.Inserted = result.Inserted
		upload.Progress.Skipped = len(failedRows)

		// Notify progress every 100 rows
		if i%100 == 0 {
			upload.notifyProgress()
		}
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		result.Error = fmt.Sprintf("commit: %v", err)
		upload.Progress.Phase = PhaseFailed
		upload.Progress.Error = result.Error
		upload.notifyProgress()
		return result
	}

	result.TotalRows = len(dataRows)
	result.Skipped = len(failedRows)
	result.FailedRows = failedRows
	result.Duration = time.Since(startTime)

	upload.Progress.Phase = PhaseComplete
	upload.Progress.CurrentRow = len(dataRows)
	upload.Progress.Inserted = result.Inserted
	upload.Progress.Skipped = result.Skipped
	upload.notifyProgress()

	return result
}

// buildAndValidate validates a row and builds insert parameters.
func buildAndValidate(row []string, headerIdx HeaderIndex, def TableDefinition) (any, error) {
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

	// Build params using the table's build function
	return def.BuildParams(row, headerIdx)
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
