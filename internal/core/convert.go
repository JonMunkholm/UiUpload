package core

// convert.go provides type conversion functions for CSV data to PostgreSQL types.
//
// These functions handle the messy reality of user-provided CSV data:
//   - Multiple date formats (US, EU, ISO, etc.)
//   - Currency symbols and thousand separators in numbers
//   - Various boolean representations (yes/no, true/false, 1/0)
//   - Excel formula prefixes (="value")
//   - Common CSV artifacts (BOM, weird quotes)
//
// All ToPg* functions return pgtype values with Valid=false for empty/invalid input,
// allowing the database to handle NULLs appropriately.

import (
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// numericRegex validates that a string is a valid numeric format after cleanup.
// Matches integers, decimals, and scientific notation.
var numericRegex = regexp.MustCompile(`^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$`)

// TwoDigitYearPivot defines how 2-digit years are interpreted.
// Years that would result in dates more than this many years in the future
// are assumed to be in the previous century.
var TwoDigitYearPivot = 20

// Date layouts split by year format for proper 2-digit year handling
var (
	twoDigitYearLayouts = []string{
		"1/2/06", "01/02/06", "1-2-06", "1.2.06", "01.02.06",
	}
	fourDigitYearLayouts = []string{
		"1/2/2006", "01/02/2006", "1-2-2006", "01-02-2006", "1.2.2006", "01.02.2006",
		"2006-01-02", "2006/01/02", "2006.01.02",
		"Jan 2, 2006", "2 Jan 2006",
		"20060102",
	}
)

// ToPgText converts a string to pgtype.Text.
// Returns invalid if the string is empty or only whitespace.
func ToPgText(s string) pgtype.Text {
	s = strings.TrimSpace(s)
	if s == "" {
		return pgtype.Text{Valid: false}
	}
	return pgtype.Text{String: s, Valid: true}
}

// ToPgDate converts a string to pgtype.Date.
// Supports multiple date formats and handles 2-digit years with pivot.
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
			if t.Year() > pivotYear {
				t = t.AddDate(-100, 0, 0)
			}
			return pgtype.Date{Time: t, Valid: true}
		}
	}

	return pgtype.Date{Valid: false}
}

// ToPgNumeric converts a string to pgtype.Numeric.
// Handles currency symbols, thousands separators, and accounting format (parentheses for negative).
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
	s = strings.ReplaceAll(s, "\u20ac", "") // Euro
	s = strings.ReplaceAll(s, "\u00a3", "") // Pound
	s = strings.ReplaceAll(s, ",", "")
	s = strings.TrimSpace(s)

	// Apply negative sign if needed
	if isNegative {
		s = "-" + s
	}

	// Validate numeric format
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

// ToPgBool converts a string to pgtype.Bool.
// Accepts various representations: true/false, yes/no, t/f, y/n, 1/0.
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

// ToPgInt4 converts an int to pgtype.Int4.
// Returns invalid if the value is zero.
func ToPgInt4(i int) pgtype.Int4 {
	if i == 0 {
		return pgtype.Int4{Valid: false}
	}
	return pgtype.Int4{Int32: int32(i), Valid: true}
}

// ToPgUUID converts a string to pgtype.UUID.
// Returns invalid if the string is empty or not a valid UUID.
func ToPgUUID(s string) pgtype.UUID {
	if s == "" {
		return pgtype.UUID{Valid: false}
	}
	parsed, err := uuid.Parse(s)
	if err != nil {
		return pgtype.UUID{Valid: false}
	}
	return pgtype.UUID{Bytes: parsed, Valid: true}
}

// PgUUIDToString converts a pgtype.UUID to its string representation.
// Returns empty string if the UUID is invalid.
func PgUUIDToString(u pgtype.UUID) string {
	if !u.Valid {
		return ""
	}
	return uuid.UUID(u.Bytes).String()
}

// MakeHeaderIndex creates a HeaderIndex from a CSV header row.
// Keys are lowercased for case-insensitive matching.
func MakeHeaderIndex(header []string) HeaderIndex {
	idx := make(HeaderIndex, len(header))
	for i, h := range header {
		key := strings.ToLower(CleanCell(h))
		idx[key] = i
	}
	return idx
}

// CleanCell removes common CSV artifacts from a cell value:
// - Trims whitespace
// - Removes Excel formula prefix (="...")
// - Removes surrounding quotes
// - Removes "netsuite:" prefix
func CleanCell(s string) string {
	s = strings.TrimSpace(s)

	// Remove leading '='
	if strings.HasPrefix(s, "=\"") && strings.HasSuffix(s, "\"") {
		s = s[2 : len(s)-1]
	} else if strings.HasPrefix(s, "=") {
		s = s[1:]
	}

	// Remove any surrounding quotes
	s = strings.Trim(s, `"'`)

	// Remove "netsuite:" prefix if present
	s = strings.TrimPrefix(s, "netsuite:")

	return s
}
