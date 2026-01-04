package core

import (
	"testing"
	"time"
)

// ----------------------------------------------------------------------------
// ToPgNumeric Tests
// ----------------------------------------------------------------------------

func TestToPgNumeric(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantValid bool
		wantValue string // String representation of expected numeric value
	}{
		// Valid: Basic integers
		{
			name:      "positive integer",
			input:     "123",
			wantValid: true,
			wantValue: "123",
		},
		{
			name:      "zero",
			input:     "0",
			wantValid: true,
			wantValue: "0",
		},
		{
			name:      "negative integer",
			input:     "-456",
			wantValid: true,
			wantValue: "-456",
		},

		// Valid: Decimals
		{
			name:      "decimal number",
			input:     "123.45",
			wantValid: true,
			wantValue: "123.45",
		},
		{
			name:      "leading decimal point",
			input:     ".99",
			wantValid: true,
			wantValue: "0.99",
		},
		{
			name:      "trailing decimal point",
			input:     "99.",
			wantValid: true,
			wantValue: "99",
		},

		// Valid: Currency symbols
		{
			name:      "dollar sign",
			input:     "$1,234.56",
			wantValid: true,
			wantValue: "1234.56",
		},
		{
			name:      "euro sign",
			input:     "\u20ac1234.56",
			wantValid: true,
			wantValue: "1234.56",
		},
		{
			name:      "pound sign",
			input:     "\u00a31234.56",
			wantValid: true,
			wantValue: "1234.56",
		},

		// Valid: Thousands separators
		{
			name:      "thousands separator",
			input:     "1,234,567.89",
			wantValid: true,
			wantValue: "1234567.89",
		},
		{
			name:      "millions with separators",
			input:     "1,000,000",
			wantValid: true,
			wantValue: "1000000",
		},

		// Valid: Accounting format (parentheses for negative)
		{
			name:      "accounting negative parentheses",
			input:     "(123.45)",
			wantValid: true,
			wantValue: "-123.45",
		},
		{
			name:      "accounting negative with currency",
			input:     "($1,234.56)",
			wantValid: true,
			wantValue: "-1234.56",
		},
		{
			name:      "accounting negative with spaces",
			input:     "( 999.99 )",
			wantValid: true,
			wantValue: "-999.99",
		},

		// Note: Scientific notation is NOT supported by pgtype.Numeric.Scan()
		// These test cases document current behavior (invalid)
		{
			name:      "scientific notation positive exponent not supported",
			input:     "1.5e10",
			wantValid: false,
		},
		{
			name:      "scientific notation negative exponent not supported",
			input:     "1.5e-3",
			wantValid: false,
		},
		{
			name:      "scientific notation uppercase E not supported",
			input:     "1.5E10",
			wantValid: false,
		},
		{
			name:      "scientific notation with plus not supported",
			input:     "1.5e+10",
			wantValid: false,
		},

		// Valid: Whitespace handling
		{
			name:      "leading whitespace",
			input:     "  123",
			wantValid: true,
			wantValue: "123",
		},
		{
			name:      "trailing whitespace",
			input:     "123  ",
			wantValid: true,
			wantValue: "123",
		},
		{
			name:      "surrounded by whitespace",
			input:     "  123.45  ",
			wantValid: true,
			wantValue: "123.45",
		},

		// Valid: Explicit positive sign
		{
			name:      "explicit positive sign",
			input:     "+123",
			wantValid: true,
			wantValue: "123",
		},

		// Invalid: Empty and whitespace
		{
			name:      "empty string",
			input:     "",
			wantValid: false,
		},
		{
			name:      "only whitespace",
			input:     "   ",
			wantValid: false,
		},

		// Invalid: Non-numeric content
		{
			name:      "alphabetic string",
			input:     "abc",
			wantValid: false,
		},
		{
			name:      "mixed alphanumeric",
			input:     "12abc34",
			wantValid: false,
		},
		{
			name:      "only currency symbol",
			input:     "$",
			wantValid: false,
		},
		{
			name:      "only currency and comma",
			input:     "$,",
			wantValid: false,
		},

		// Invalid: Malformed numbers
		{
			name:      "multiple decimal points",
			input:     "12.34.56",
			wantValid: false,
		},
		{
			name:      "double negative",
			input:     "--123",
			wantValid: false,
		},
		{
			name:      "negative after number",
			input:     "123-",
			wantValid: false,
		},

		// Invalid: Special values
		{
			name:      "NaN",
			input:     "NaN",
			wantValid: false,
		},
		{
			name:      "Infinity",
			input:     "Infinity",
			wantValid: false,
		},
		{
			name:      "negative infinity",
			input:     "-Infinity",
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToPgNumeric(tt.input)

			if result.Valid != tt.wantValid {
				t.Errorf("ToPgNumeric(%q).Valid = %v, want %v",
					tt.input, result.Valid, tt.wantValid)
				return
			}

			if tt.wantValid {
				// Verify the numeric is valid and can be converted to float64
				f, err := result.Float64Value()
				if err != nil {
					t.Errorf("ToPgNumeric(%q) Float64Value error: %v", tt.input, err)
				}
				if !f.Valid {
					t.Errorf("ToPgNumeric(%q) Float64Value returned invalid", tt.input)
				}
			}
		})
	}
}

// Note: We don't use formatNumericString as pgtype.Numeric comparison
// is complex due to internal representation. We verify validity instead.

// TestToPgNumeric_SpecificValues tests that specific inputs produce expected outputs
func TestToPgNumeric_SpecificValues(t *testing.T) {
	tests := []struct {
		input    string
		wantInt  int64
		wantPrec bool // true if we expect a decimal value
	}{
		{"123", 123, false},
		{"0", 0, false},
		{"-456", -456, false},
		{"1000", 1000, false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := ToPgNumeric(tt.input)
			if !result.Valid {
				t.Fatalf("ToPgNumeric(%q) returned invalid", tt.input)
			}

			f, err := result.Float64Value()
			if err != nil {
				t.Fatalf("Float64Value() error: %v", err)
			}
			if !f.Valid {
				t.Fatalf("Float64Value() returned invalid")
			}

			if !tt.wantPrec && int64(f.Float64) != tt.wantInt {
				t.Errorf("ToPgNumeric(%q) = %v, want %d", tt.input, f.Float64, tt.wantInt)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// ToPgDate Tests
// ----------------------------------------------------------------------------

func TestToPgDate(t *testing.T) {
	// Store original pivot for restoration
	originalPivot := TwoDigitYearPivot
	defer func() { TwoDigitYearPivot = originalPivot }()

	tests := []struct {
		name      string
		input     string
		wantValid bool
		wantYear  int
		wantMonth time.Month
		wantDay   int
	}{
		// Valid: ISO format (YYYY-MM-DD)
		{
			name:      "ISO format standard",
			input:     "2024-01-15",
			wantValid: true,
			wantYear:  2024,
			wantMonth: time.January,
			wantDay:   15,
		},
		{
			name:      "ISO format end of year",
			input:     "2024-12-31",
			wantValid: true,
			wantYear:  2024,
			wantMonth: time.December,
			wantDay:   31,
		},
		{
			name:      "ISO format leap year Feb 29",
			input:     "2024-02-29",
			wantValid: true,
			wantYear:  2024,
			wantMonth: time.February,
			wantDay:   29,
		},

		// Valid: US format (MM/DD/YYYY)
		{
			name:      "US format with slashes",
			input:     "01/15/2024",
			wantValid: true,
			wantYear:  2024,
			wantMonth: time.January,
			wantDay:   15,
		},
		{
			name:      "US format single digit month/day",
			input:     "1/5/2024",
			wantValid: true,
			wantYear:  2024,
			wantMonth: time.January,
			wantDay:   5,
		},

		// Valid: Other 4-digit year formats
		{
			name:      "dash separator MM-DD-YYYY",
			input:     "01-15-2024",
			wantValid: true,
			wantYear:  2024,
			wantMonth: time.January,
			wantDay:   15,
		},
		{
			name:      "dot separator MM.DD.YYYY",
			input:     "01.15.2024",
			wantValid: true,
			wantYear:  2024,
			wantMonth: time.January,
			wantDay:   15,
		},
		{
			name:      "year first with slash YYYY/MM/DD",
			input:     "2024/01/15",
			wantValid: true,
			wantYear:  2024,
			wantMonth: time.January,
			wantDay:   15,
		},
		{
			name:      "year first with dot YYYY.MM.DD",
			input:     "2024.01.15",
			wantValid: true,
			wantYear:  2024,
			wantMonth: time.January,
			wantDay:   15,
		},

		// Valid: Text month formats
		{
			name:      "text month Jan 15, 2024",
			input:     "Jan 15, 2024",
			wantValid: true,
			wantYear:  2024,
			wantMonth: time.January,
			wantDay:   15,
		},
		{
			name:      "text month 15 Jan 2024",
			input:     "15 Jan 2024",
			wantValid: true,
			wantYear:  2024,
			wantMonth: time.January,
			wantDay:   15,
		},

		// Valid: Compact format (YYYYMMDD)
		{
			name:      "compact format no separators",
			input:     "20240115",
			wantValid: true,
			wantYear:  2024,
			wantMonth: time.January,
			wantDay:   15,
		},

		// Valid: Whitespace handling
		{
			name:      "leading whitespace",
			input:     "  2024-01-15",
			wantValid: true,
			wantYear:  2024,
			wantMonth: time.January,
			wantDay:   15,
		},
		{
			name:      "trailing whitespace",
			input:     "2024-01-15  ",
			wantValid: true,
			wantYear:  2024,
			wantMonth: time.January,
			wantDay:   15,
		},

		// Invalid: Empty and whitespace
		{
			name:      "empty string",
			input:     "",
			wantValid: false,
		},
		{
			name:      "only whitespace",
			input:     "   ",
			wantValid: false,
		},

		// Invalid: Non-date content
		{
			name:      "not a date text",
			input:     "not-a-date",
			wantValid: false,
		},
		{
			name:      "random text",
			input:     "hello world",
			wantValid: false,
		},

		// Invalid: Out of range values
		{
			name:      "month greater than 12",
			input:     "2024-13-01",
			wantValid: false,
		},
		{
			name:      "day greater than 31",
			input:     "2024-01-32",
			wantValid: false,
		},
		{
			name:      "invalid Feb 29 non-leap year",
			input:     "2023-02-29",
			wantValid: false,
		},
		{
			name:      "month zero",
			input:     "2024-00-15",
			wantValid: false,
		},
		{
			name:      "day zero",
			input:     "2024-01-00",
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToPgDate(tt.input)

			if result.Valid != tt.wantValid {
				t.Errorf("ToPgDate(%q).Valid = %v, want %v",
					tt.input, result.Valid, tt.wantValid)
				return
			}

			if tt.wantValid {
				if result.Time.Year() != tt.wantYear {
					t.Errorf("ToPgDate(%q).Year = %d, want %d",
						tt.input, result.Time.Year(), tt.wantYear)
				}
				if result.Time.Month() != tt.wantMonth {
					t.Errorf("ToPgDate(%q).Month = %v, want %v",
						tt.input, result.Time.Month(), tt.wantMonth)
				}
				if result.Time.Day() != tt.wantDay {
					t.Errorf("ToPgDate(%q).Day = %d, want %d",
						tt.input, result.Time.Day(), tt.wantDay)
				}
			}
		})
	}
}

// TestToPgDate_TwoDigitYear tests 2-digit year handling with pivot year logic
func TestToPgDate_TwoDigitYear(t *testing.T) {
	// Save original and restore after test
	originalPivot := TwoDigitYearPivot
	defer func() { TwoDigitYearPivot = originalPivot }()

	// Set pivot to 20 years from now
	TwoDigitYearPivot = 20
	currentYear := time.Now().Year()
	pivotYear := currentYear + 20

	tests := []struct {
		name      string
		input     string
		wantValid bool
		wantYear  int
	}{
		// 2-digit years that should be in 2000s (current century)
		{
			name:      "2-digit year 25 as 2025",
			input:     "01/15/25",
			wantValid: true,
			wantYear:  2025,
		},
		{
			name:      "2-digit year 30 (within pivot)",
			input:     "01/15/30",
			wantValid: true,
			wantYear:  2030,
		},

		// 2-digit years that should be in 1900s (past century)
		{
			name:      "2-digit year 99 as 1999",
			input:     "01/15/99",
			wantValid: true,
			wantYear:  1999,
		},
		{
			name:      "2-digit year 85 as 1985",
			input:     "01/15/85",
			wantValid: true,
			wantYear:  1985,
		},

		// Different formats with 2-digit years
		{
			name:      "dash format 2-digit year",
			input:     "1-15-99",
			wantValid: true,
			wantYear:  1999,
		},
		{
			name:      "dot format 2-digit year",
			input:     "01.15.99",
			wantValid: true,
			wantYear:  1999,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToPgDate(tt.input)

			if result.Valid != tt.wantValid {
				t.Errorf("ToPgDate(%q).Valid = %v, want %v",
					tt.input, result.Valid, tt.wantValid)
				return
			}

			if tt.wantValid {
				// Check if year is as expected (accounting for the pivot calculation)
				gotYear := result.Time.Year()

				// For years that would be > pivotYear, they should be adjusted to 1900s
				// For years <= pivotYear, they should be in 2000s
				if gotYear != tt.wantYear {
					t.Errorf("ToPgDate(%q).Year = %d, want %d (pivot year: %d)",
						tt.input, gotYear, tt.wantYear, pivotYear)
				}
			}
		})
	}
}

// ----------------------------------------------------------------------------
// ToPgBool Tests
// ----------------------------------------------------------------------------

func TestToPgBool(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantValid bool
		wantBool  bool
	}{
		// Valid: True values - full words
		{
			name:      "true lowercase",
			input:     "true",
			wantValid: true,
			wantBool:  true,
		},
		{
			name:      "TRUE uppercase",
			input:     "TRUE",
			wantValid: true,
			wantBool:  true,
		},
		{
			name:      "True mixed case",
			input:     "True",
			wantValid: true,
			wantBool:  true,
		},
		{
			name:      "yes lowercase",
			input:     "yes",
			wantValid: true,
			wantBool:  true,
		},
		{
			name:      "YES uppercase",
			input:     "YES",
			wantValid: true,
			wantBool:  true,
		},
		{
			name:      "Yes mixed case",
			input:     "Yes",
			wantValid: true,
			wantBool:  true,
		},

		// Valid: True values - abbreviations
		{
			name:      "t lowercase",
			input:     "t",
			wantValid: true,
			wantBool:  true,
		},
		{
			name:      "T uppercase",
			input:     "T",
			wantValid: true,
			wantBool:  true,
		},
		{
			name:      "y lowercase",
			input:     "y",
			wantValid: true,
			wantBool:  true,
		},
		{
			name:      "Y uppercase",
			input:     "Y",
			wantValid: true,
			wantBool:  true,
		},

		// Valid: True values - numeric
		{
			name:      "1 as true",
			input:     "1",
			wantValid: true,
			wantBool:  true,
		},

		// Valid: False values - full words
		{
			name:      "false lowercase",
			input:     "false",
			wantValid: true,
			wantBool:  false,
		},
		{
			name:      "FALSE uppercase",
			input:     "FALSE",
			wantValid: true,
			wantBool:  false,
		},
		{
			name:      "False mixed case",
			input:     "False",
			wantValid: true,
			wantBool:  false,
		},
		{
			name:      "no lowercase",
			input:     "no",
			wantValid: true,
			wantBool:  false,
		},
		{
			name:      "NO uppercase",
			input:     "NO",
			wantValid: true,
			wantBool:  false,
		},
		{
			name:      "No mixed case",
			input:     "No",
			wantValid: true,
			wantBool:  false,
		},

		// Valid: False values - abbreviations
		{
			name:      "f lowercase",
			input:     "f",
			wantValid: true,
			wantBool:  false,
		},
		{
			name:      "F uppercase",
			input:     "F",
			wantValid: true,
			wantBool:  false,
		},
		{
			name:      "n lowercase",
			input:     "n",
			wantValid: true,
			wantBool:  false,
		},
		{
			name:      "N uppercase",
			input:     "N",
			wantValid: true,
			wantBool:  false,
		},

		// Valid: False values - numeric
		{
			name:      "0 as false",
			input:     "0",
			wantValid: true,
			wantBool:  false,
		},

		// Valid: With whitespace
		{
			name:      "true with leading whitespace",
			input:     "  true",
			wantValid: true,
			wantBool:  true,
		},
		{
			name:      "false with trailing whitespace",
			input:     "false  ",
			wantValid: true,
			wantBool:  false,
		},
		{
			name:      "yes surrounded by whitespace",
			input:     "  yes  ",
			wantValid: true,
			wantBool:  true,
		},

		// Invalid: Empty and whitespace
		{
			name:      "empty string",
			input:     "",
			wantValid: false,
		},
		{
			name:      "only whitespace",
			input:     "   ",
			wantValid: false,
		},

		// Invalid: Unrecognized values
		{
			name:      "maybe",
			input:     "maybe",
			wantValid: false,
		},
		{
			name:      "on",
			input:     "on",
			wantValid: false,
		},
		{
			name:      "off",
			input:     "off",
			wantValid: false,
		},
		{
			name:      "random text",
			input:     "hello",
			wantValid: false,
		},
		{
			name:      "number 2",
			input:     "2",
			wantValid: false,
		},
		{
			name:      "negative 1",
			input:     "-1",
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToPgBool(tt.input)

			if result.Valid != tt.wantValid {
				t.Errorf("ToPgBool(%q).Valid = %v, want %v",
					tt.input, result.Valid, tt.wantValid)
				return
			}

			if tt.wantValid && result.Bool != tt.wantBool {
				t.Errorf("ToPgBool(%q).Bool = %v, want %v",
					tt.input, result.Bool, tt.wantBool)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// ToPgText Tests
// ----------------------------------------------------------------------------

func TestToPgText(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantValid  bool
		wantString string
	}{
		// Valid: Normal strings
		{
			name:       "simple string",
			input:      "hello",
			wantValid:  true,
			wantString: "hello",
		},
		{
			name:       "string with spaces",
			input:      "hello world",
			wantValid:  true,
			wantString: "hello world",
		},
		{
			name:       "string with numbers",
			input:      "test123",
			wantValid:  true,
			wantString: "test123",
		},

		// Valid: Whitespace handling (should trim)
		{
			name:       "leading whitespace trimmed",
			input:      "  hello",
			wantValid:  true,
			wantString: "hello",
		},
		{
			name:       "trailing whitespace trimmed",
			input:      "hello  ",
			wantValid:  true,
			wantString: "hello",
		},
		{
			name:       "surrounded whitespace trimmed",
			input:      "  hello world  ",
			wantValid:  true,
			wantString: "hello world",
		},

		// Valid: Special characters preserved
		{
			name:       "unicode characters",
			input:      "caf\u00e9",
			wantValid:  true,
			wantString: "caf\u00e9",
		},
		{
			name:       "emoji",
			input:      "hello \U0001F44B",
			wantValid:  true,
			wantString: "hello \U0001F44B",
		},

		// Invalid: Empty and whitespace only
		{
			name:      "empty string",
			input:     "",
			wantValid: false,
		},
		{
			name:      "only spaces",
			input:     "   ",
			wantValid: false,
		},
		{
			name:      "only tabs",
			input:     "\t\t",
			wantValid: false,
		},
		{
			name:      "only newlines",
			input:     "\n\n",
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToPgText(tt.input)

			if result.Valid != tt.wantValid {
				t.Errorf("ToPgText(%q).Valid = %v, want %v",
					tt.input, result.Valid, tt.wantValid)
				return
			}

			if tt.wantValid && result.String != tt.wantString {
				t.Errorf("ToPgText(%q).String = %q, want %q",
					tt.input, result.String, tt.wantString)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// CleanCell Tests
// ----------------------------------------------------------------------------

func TestCleanCell(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		// Basic cleaning
		{
			name:  "simple string unchanged",
			input: "hello",
			want:  "hello",
		},
		{
			name:  "empty string",
			input: "",
			want:  "",
		},

		// Whitespace trimming
		{
			name:  "leading whitespace",
			input: "  hello",
			want:  "hello",
		},
		{
			name:  "trailing whitespace",
			input: "hello  ",
			want:  "hello",
		},
		{
			name:  "surrounded by whitespace",
			input: "  hello  ",
			want:  "hello",
		},

		// Excel formula prefix handling
		{
			name:  "Excel formula with quotes",
			input: `="hello"`,
			want:  "hello",
		},
		{
			name:  "Excel formula number as text",
			input: `="12345"`,
			want:  "12345",
		},
		{
			name:  "bare equals sign",
			input: "=SUM(A1)",
			want:  "SUM(A1)",
		},
		{
			name:  "equals at start only",
			input: "=hello",
			want:  "hello",
		},

		// Quote handling
		{
			name:  "double quotes removed",
			input: `"hello"`,
			want:  "hello",
		},
		{
			name:  "single quotes removed",
			input: "'hello'",
			want:  "hello",
		},
		{
			name:  "mixed quotes removed outer only",
			input: `"hello'`,
			want:  "hello",
		},
		{
			name:  "leading single quote (Excel text prefix)",
			input: "'12345",
			want:  "12345",
		},

		// NetSuite prefix handling
		{
			name:  "netsuite prefix removed",
			input: "netsuite:ABC123",
			want:  "ABC123",
		},
		{
			name:  "netsuite prefix case sensitive",
			input: "NETSUITE:ABC123",
			want:  "NETSUITE:ABC123", // Not removed - case sensitive
		},

		// Combined cleaning
		{
			name:  "whitespace and quotes",
			input: `  "hello"  `,
			want:  "hello",
		},
		{
			name:  "excel formula with whitespace",
			input: `  ="test"  `,
			want:  "test",
		},

		// Edge cases
		{
			name:  "only quotes",
			input: `""`,
			want:  "",
		},
		{
			name:  "only single quotes",
			input: "''",
			want:  "",
		},
		{
			name:  "equals with quoted number",
			input: `="0"`,
			want:  "0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CleanCell(tt.input)
			if got != tt.want {
				t.Errorf("CleanCell(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// MakeHeaderIndex Tests
// ----------------------------------------------------------------------------

func TestMakeHeaderIndex(t *testing.T) {
	tests := []struct {
		name   string
		header []string
		checks map[string]int // key -> expected index
	}{
		{
			name:   "simple headers",
			header: []string{"Name", "Email", "Phone"},
			checks: map[string]int{
				"name":  0,
				"email": 1,
				"phone": 2,
			},
		},
		{
			name:   "case insensitive lookup",
			header: []string{"NAME", "Email", "pHoNe"},
			checks: map[string]int{
				"name":  0,
				"email": 1,
				"phone": 2,
			},
		},
		{
			name:   "headers with quotes cleaned",
			header: []string{`"Name"`, `"Email"`, `"Phone"`},
			checks: map[string]int{
				"name":  0,
				"email": 1,
				"phone": 2,
			},
		},
		{
			name:   "headers with whitespace",
			header: []string{"  Name  ", " Email ", "Phone"},
			checks: map[string]int{
				"name":  0,
				"email": 1,
				"phone": 2,
			},
		},
		{
			name:   "headers with Excel formula",
			header: []string{`="Name"`, `="Email"`},
			checks: map[string]int{
				"name":  0,
				"email": 1,
			},
		},
		{
			name:   "empty header",
			header: []string{},
			checks: map[string]int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := MakeHeaderIndex(tt.header)

			for key, wantPos := range tt.checks {
				gotPos, ok := idx[key]
				if !ok {
					t.Errorf("MakeHeaderIndex(%v)[%q] not found, want index %d",
						tt.header, key, wantPos)
					continue
				}
				if gotPos != wantPos {
					t.Errorf("MakeHeaderIndex(%v)[%q] = %d, want %d",
						tt.header, key, gotPos, wantPos)
				}
			}
		})
	}
}

// TestMakeHeaderIndex_DuplicateHeaders verifies behavior with duplicate column names
func TestMakeHeaderIndex_DuplicateHeaders(t *testing.T) {
	// When duplicates exist, the last occurrence wins
	header := []string{"Name", "Email", "Name"}
	idx := MakeHeaderIndex(header)

	// "name" should map to index 2 (last occurrence)
	if gotPos, ok := idx["name"]; !ok || gotPos != 2 {
		t.Errorf("MakeHeaderIndex with duplicates: name index = %d, want 2", gotPos)
	}
}
