package handler

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/JonMunkholm/TUI/internal/csv"
	db "github.com/JonMunkholm/TUI/internal/database"
	"github.com/JonMunkholm/TUI/internal/schema"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pre-compiled regex for numeric validation (avoids recompilation on each call)
var numericRegex = regexp.MustCompile(`^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$`)


// TwoDigitYearPivot defines how 2-digit years are interpreted.
// Years that would result in dates more than this many years in the future
// are assumed to be in the previous century.
// Example with pivot=20 in year 2025: "46" → 1946 (not 2046), "24" → 2024
var TwoDigitYearPivot = 20

// ContextCheckInterval is how often (in rows) to check for context cancellation.
// Checking every row would be expensive; checking periodically balances responsiveness
// with performance. 100 rows is typically sub-millisecond of processing.
var ContextCheckInterval = 100

// Date layouts split by year format for proper 2-digit year handling
var (
	// 2-digit year layouts - require pivot year adjustment
	twoDigitYearLayouts = []string{
		"1/2/06", "01/02/06", "1-2-06", "1.2.06", "01.02.06",
	}
	// 4-digit year layouts - no adjustment needed
	fourDigitYearLayouts = []string{
		"1/2/2006", "01/02/2006", "1-2-2006", "01-02-2006", "1.2.2006", "01.02.2006",
		"2006-01-02", "2006/01/02", "2006.01.02",
		"Jan 2, 2006", "2 Jan 2006",
		"20060102",
	}
)

type WdMsg string
type DoneMsg string
type ErrMsg struct{ Err error }


type CsvCheck func(ctx context.Context, file string) ([]db.CsvUpload, error)
type ActType func(ctx context.Context, fileName string) error



func getUploadsRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	uploadRoot := filepath.Join(wd, "accounting/uploads")

	return uploadRoot, nil
}


/* ----------------------------------------
	Main entry for uploading
---------------------------------------- */

func ProcessUpload(
	ctx context.Context,
	pool *pgxpool.Pool,
	root string,
	dir string,
	dirMap map[string]CsvProps,
	csvCheck CsvCheck,
	actType ActType,
) error {

	// Top level (NS) dir

	handler := dirMap[dir]
	if handler == nil {
		return fmt.Errorf("no CsvProps handler found for dir: %s", dir)
	}

	full := filepath.Join(root, dir)

	if len(handler.Header()) == 0 {
		return fmt.Errorf("directory Headers not defined: %s", dir)
	}

	entries, err := os.ReadDir(full)

	if err != nil {
		return fmt.Errorf("reading directory %s: %w", dir, err)
	}

	// DB dir level - dir containing upload file

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if filepath.Ext(entry.Name()) != ".csv" {
			continue
		}

		if err := processUploadFile(
			ctx, pool, full, entry.Name(), handler, csvCheck, actType,
		); err != nil {
			return err
		}
	}
	return nil
}


/* ----------------------------------------
	Process individual CSV file
---------------------------------------- */

func processUploadFile(
	ctx context.Context,
	pool *pgxpool.Pool,
	dir string,
	file string,
	handler CsvProps,
	csvCheck CsvCheck,
	actType ActType,
) error {

	path := filepath.Join(dir, file)

	// Check context before starting
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("operation cancelled: %w", err)
	}

	// 1. Skip already-uploaded files
	existing, err := csvCheck(ctx, path)
	if err != nil {
		return fmt.Errorf("csv check failed: %w", err)
	}

	if len(existing) != 0 {
		// File already processed - verify it still exists before removing
		if _, statErr := os.Stat(path); statErr == nil {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("failed to remove already-uploaded file %s: %w", filepath.Base(path), err)
			}
		}
		return nil
	}

	// 2. Header
	headerIdx, err := csv.FindHeaderRow(path, handler.Header())
	if err != nil {
		return fmt.Errorf("header match failed for %s: %w", file, err)
	}

	// 3. Read rows
	rows, err := csv.Read(path)
	if err != nil {
		return fmt.Errorf("error reading csv %s: %w", file, err)
	}

	if headerIdx < 0 || headerIdx >= len(rows) {
		return fmt.Errorf("csv file %s has fewer rows (%d) than header index (%d)", file, len(rows), headerIdx)
	}

	dataRows := rows[headerIdx+1:]
	if len(dataRows) == 0 {
		return fmt.Errorf("no rows found after header in csv %s", file)
	}

	// 4. BEGIN TRANSACTION - all row inserts are atomic per file
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) // No-op if already committed

	txQueries := db.New(tx) // Transaction-bound queries

	// 5. Build + Act on rows
	headerRow := rows[headerIdx]
	csvHeaderIdx := MakeHeaderIndex(headerRow) // Pre-compute once for all rows
	header := append([]string{"Status"}, headerRow...)
	failedRecords := [][]string{header}
	expectedCols := len(handler.Header())

	for i, row := range dataRows {
		// CSV line number (1-indexed for user-friendly display)
		csvLineNum := headerIdx + i + 2

		// Check context periodically to allow cancellation
		if i%ContextCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("operation cancelled at line %d: %w", csvLineNum, err)
			}
		}

		// Skip fully empty rows
		empty := true
		for _, v := range row {
			if strings.TrimSpace(v) != "" {
				empty = false
				break
			}
		}
		if empty {
			continue
		}

		if len(row) < expectedCols {
			failedRecords = append(failedRecords, rowFailed(
				fmt.Sprintf("line %d: row has %d columns, expected %d", csvLineNum, len(row), expectedCols),
				row,
			))
			continue
		}

		arg, err := handler.BuildParams(row, csvHeaderIdx)
		if err != nil {
			failedRecords = append(failedRecords, rowFailed(
				fmt.Sprintf("line %d: %s", csvLineNum, err.Error()),
				row,
			))
			continue
		}

		// Check context before each insert to allow prompt cancellation
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("operation cancelled at line %d: %w", csvLineNum, err)
		}

		// Use savepoint to isolate each insert - PostgreSQL aborts entire transaction on any error
		savepointName := fmt.Sprintf("sp_%d", i)
		if _, err := tx.Exec(ctx, fmt.Sprintf("SAVEPOINT %s", savepointName)); err != nil {
			return fmt.Errorf("failed to create savepoint at line %d: %w", csvLineNum, err)
		}

		ok, err := handler.Insert(ctx, txQueries, arg)
		if err != nil {
			// Rollback to savepoint to recover transaction state
			if _, rbErr := tx.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", savepointName)); rbErr != nil {
				return fmt.Errorf("failed to rollback savepoint at line %d: %w", csvLineNum, rbErr)
			}
			failedRecords = append(failedRecords, rowFailed(
				fmt.Sprintf("line %d: db error: %s", csvLineNum, err.Error()),
				row,
			))
			continue
		}

		// Release savepoint on success to free resources
		if _, err := tx.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", savepointName)); err != nil {
			return fmt.Errorf("failed to release savepoint at line %d: %w", csvLineNum, err)
		}

		if !ok {
			failedRecords = append(failedRecords, rowFailed(
				fmt.Sprintf("line %d: insert returned false, no rows affected", csvLineNum),
				row,
			))
			continue
		}
	}

	// 6. COMMIT TRANSACTION
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// 7. Log upload (after successful commit)
	if err = actType(ctx, path); err != nil {
		return err
	}

	// 8. Sanitize filename to prevent path traversal attacks
	safeFile := filepath.Base(file)
	if safeFile != file || strings.Contains(file, "..") {
		return fmt.Errorf("invalid filename: %q", file)
	}

	// 9. Write failed records file (if applicable)
	if len(failedRecords) > 1 {
		top := filepath.Dir(dir)
		fileName := fmt.Sprintf("%s - failed.csv", strings.TrimSuffix(safeFile, ".csv"))
		failedRecordsPath := filepath.Join(top, fileName)

		if err = csv.Write(failedRecordsPath, failedRecords); err != nil {
			return fmt.Errorf("failed writing failure file: %w", err)
		}
	}

	// 10. Move to Uploaded directory

	// Ensure Uploaded directory exists
	uploadedDir := filepath.Join(dir, "Uploaded")
	if err := os.MkdirAll(uploadedDir, 0755); err != nil {
		return fmt.Errorf("failed to create Uploaded directory: %w", err)
	}

	dest := filepath.Join(uploadedDir, safeFile)
	if err := os.Rename(path, dest); err != nil {
		return fmt.Errorf("failed moving file %s: %w", safeFile, err)
	}

	return nil

}

func rowFailed(reason string, row []string) []string {
	return append([]string{reason}, row...)
}


/* ----------------------------------------
	Validation Helpers
---------------------------------------- */

type ValidatedRow map[string]string

// MakeHeaderIndex creates a HeaderIndex from a CSV header row.
// This should be called once per file, then reused for all rows.
// Uses csv.CleanHeader to handle Excel formula prefixes and other artifacts.
func MakeHeaderIndex(header []string) HeaderIndex {
	idx := make(HeaderIndex, len(header))
	for i, h := range header {
		key := csv.CleanHeader(h)
		idx[key] = i
	}
	return idx
}

func validateRow(row []string, headerIdx HeaderIndex, specs []schema.FieldSpec) (ValidatedRow, error) {
	out := make(ValidatedRow, len(specs))

	for _, spec := range specs {
		pos, ok := headerIdx[strings.ToLower(spec.Name)]
		if !ok || pos >= len(row) {
			if spec.Required {
				return nil, fmt.Errorf("missing required column %q", spec.Name)
			}
			continue
		}
		raw := csv.CleanCell(row[pos])
		// If Required but not AllowEmpty, reject empty values
		// If Required and AllowEmpty, accept empty (will become NULL in DB)
		if raw == "" && spec.Required && !spec.AllowEmpty {
			return nil, fmt.Errorf("empty required field %q", spec.Name)
		}

		if spec.Normalizer != nil {
			raw = spec.Normalizer(raw)
		}

		// Skip type validation for empty values when AllowEmpty is true
		if raw == "" && spec.AllowEmpty {
			out[spec.Name] = raw
			continue
		}

		switch spec.Type {
		case schema.FieldEnum:
			valid := false
			for _, v := range spec.EnumValues {
				if strings.EqualFold(raw, v) {
					raw = v
					valid = true
					break
				}
			}
			if !valid && spec.Required {
				return nil, fmt.Errorf("invalid enum for %q: %q", spec.Name, raw)
			}
		case schema.FieldDate:
			if spec.Required && !ToPgDate(raw).Valid {
				return nil, fmt.Errorf("invalid date for %q: %q", spec.Name, raw)
			}
		case schema.FieldNumeric:
			if spec.Required && !ToPgNumeric(raw).Valid {
				return nil, fmt.Errorf("invalid numeric for %q: %q", spec.Name, raw)
			}
		case schema.FieldBool:
			if spec.Required && !ToPgBool(raw).Valid {
				return nil, fmt.Errorf("invalid bool for %q: %q", spec.Name, raw)
			}
		case schema.FieldText:
			// no-op
		}

		out[spec.Name] = raw
	}

	return out, nil
}


/* ----------------------------------------
	Pgx Helpers
---------------------------------------- */


func ToPgText(s string) pgtype.Text {
	s = strings.TrimSpace(s)
	if s == "" {
		return pgtype.Text{Valid: false}
	}
	return pgtype.Text{String: s, Valid: true}
}

func ToPgDate(s string) pgtype.Date {
	s = strings.TrimSpace(s)
	if s == "" {
		return pgtype.Date{Valid: false}
	}

	// Try 4-digit year layouts first (unambiguous)
	for _, layout := range fourDigitYearLayouts {
		t, err := time.Parse(layout, s)
		if err == nil {
			return pgtype.Date{Time: t, Valid: true}
		}
	}

	// Try 2-digit year layouts with pivot year adjustment
	currentYear := time.Now().Year()
	pivotYear := currentYear + TwoDigitYearPivot

	for _, layout := range twoDigitYearLayouts {
		t, err := time.Parse(layout, s)
		if err == nil {
			// Go's time.Parse interprets 2-digit years as:
			// 00-68 → 2000-2068, 69-99 → 1969-1999
			// We apply a consistent pivot: if year > currentYear + pivot, use previous century
			if t.Year() > pivotYear {
				t = t.AddDate(-100, 0, 0)
			}
			return pgtype.Date{Time: t, Valid: true}
		}
	}

	return pgtype.Date{Valid: false}
}


func ToPgNumeric(s string) pgtype.Numeric {
	s = strings.TrimSpace(s)
	if s == "" {
		return pgtype.Numeric{Valid: false}
	}

	// Detect negative accounting format "(123.45)"
	isNegative := false
	if strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		isNegative = true
		s = strings.TrimSpace(s[1 : len(s)-1])
	}

	// Remove common currency symbols and thousands separators
	s = strings.ReplaceAll(s, "$", "")
	s = strings.ReplaceAll(s, "\u20ac", "")
	s = strings.ReplaceAll(s, "\u00a3", "")
	s = strings.ReplaceAll(s, ",", "")
	s = strings.TrimSpace(s)

	// Apply negative sign if needed
	if isNegative {
		s = "-" + s
	}

	// Validate numeric format using pre-compiled regex
	if !numericRegex.MatchString(s) {
		return pgtype.Numeric{Valid: false}
	}

	// Scan into pgtype.Numeric
	var n pgtype.Numeric
	if err := n.Scan(s); err != nil {
		return pgtype.Numeric{Valid: false}
	}

	return n

}


func ToPgBool(s string) pgtype.Bool {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return pgtype.Bool{Valid: false}
	}

	switch s {
	case "true", "t", "yes", "y", "1":
		return pgtype.Bool{Bool: true, Valid: true}
	case "false", "f", "no", "n", "0":
		return pgtype.Bool{Bool: false, Valid: true}
	default:
		return pgtype.Bool{Valid: false}
	}
}
