package core

import (
	"bytes"
	"testing"
)

// ============================================================================
// sanitizeUTF8 Tests
// ============================================================================

func TestSanitizeUTF8(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  []byte
	}{
		{
			name:  "valid UTF-8 unchanged",
			input: []byte("hello world"),
			want:  []byte("hello world"),
		},
		{
			name:  "empty input",
			input: []byte{},
			want:  []byte{},
		},
		{
			name:  "valid unicode",
			input: []byte("hello \xe4\xb8\x96\xe7\x95\x8c"), // hello 世界
			want:  []byte("hello \xe4\xb8\x96\xe7\x95\x8c"),
		},
		{
			name:  "emoji preserved",
			input: []byte("hello \xf0\x9f\x91\x8b"), // hello 👋
			want:  []byte("hello \xf0\x9f\x91\x8b"),
		},
		{
			name:  "invalid byte replaced with replacement char",
			input: []byte{0x80}, // Invalid UTF-8 start byte
			want:  []byte("\uFFFD"),
		},
		{
			name:  "truncated multibyte sequence",
			input: []byte{0xc3}, // Start of 2-byte sequence, missing continuation
			want:  []byte("\uFFFD"),
		},
		{
			name:  "mixed valid and invalid",
			input: []byte("hello\x80world"),
			want:  []byte("hello\uFFFDworld"),
		},
		{
			name:  "multiple invalid bytes",
			input: []byte{0x80, 0x81, 0x82},
			want:  []byte("\uFFFD\uFFFD\uFFFD"),
		},
		{
			name:  "invalid continuation byte without lead",
			input: []byte("abc\xbfdef"), // 0xBF is a continuation byte
			want:  []byte("abc\uFFFDdef"),
		},
		{
			name:  "overlong encoding (2 bytes for ASCII)",
			input: []byte{0xc0, 0x80}, // Overlong encoding for NULL
			want:  []byte("\uFFFD\uFFFD"),
		},
		{
			name:  "Windows-1252 characters replaced",
			input: []byte("hello\x93world\x94"), // Smart quotes in Windows-1252
			want:  []byte("hello\uFFFDworld\uFFFD"),
		},
		{
			name:  "Latin-1 high bytes replaced",
			input: []byte("caf\xe9"), // e9 is Latin-1 'e with acute'
			want:  []byte("caf\uFFFD"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeUTF8(tt.input)
			if !bytes.Equal(got, tt.want) {
				t.Errorf("sanitizeUTF8(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// TestSanitizeUTF8_PreservesValid ensures valid UTF-8 passes through unchanged
func TestSanitizeUTF8_PreservesValid(t *testing.T) {
	// Various valid UTF-8 strings
	inputs := []string{
		"",
		"ASCII only",
		"Cafe with accent: cafe",
		"Chinese: world",
		"Emoji: party popper",
		"Japanese: hello",
		"Mixed: Hello world party",
		"Numbers: 1234567890",
		"Punctuation: !@#$%^&*()",
		"Newlines: line1\nline2\r\nline3",
		"Tabs: col1\tcol2\tcol3",
	}

	for _, input := range inputs {
		t.Run(input[:min(20, len(input))], func(t *testing.T) {
			inputBytes := []byte(input)
			got := sanitizeUTF8(inputBytes)
			if !bytes.Equal(got, inputBytes) {
				t.Errorf("sanitizeUTF8(%q) modified valid UTF-8", input)
			}
		})
	}
}

// ============================================================================
// isEmptyRow Tests
// ============================================================================

func TestIsEmptyRow(t *testing.T) {
	tests := []struct {
		name string
		row  []string
		want bool
	}{
		{
			name: "empty slice",
			row:  []string{},
			want: true,
		},
		{
			name: "single empty string",
			row:  []string{""},
			want: true,
		},
		{
			name: "multiple empty strings",
			row:  []string{"", "", ""},
			want: true,
		},
		{
			name: "whitespace only cells",
			row:  []string{"   ", "\t", "  \t  "},
			want: true,
		},
		{
			name: "newlines only",
			row:  []string{"\n", "\r\n", "\r"},
			want: true,
		},
		{
			name: "single non-empty cell",
			row:  []string{"data"},
			want: false,
		},
		{
			name: "non-empty with empties",
			row:  []string{"", "data", ""},
			want: false,
		},
		{
			name: "non-empty last cell",
			row:  []string{"", "", "data"},
			want: false,
		},
		{
			name: "non-empty first cell",
			row:  []string{"data", "", ""},
			want: false,
		},
		{
			name: "all non-empty",
			row:  []string{"a", "b", "c"},
			want: false,
		},
		{
			name: "whitespace and data",
			row:  []string{"   ", "data", "\t"},
			want: false,
		},
		{
			name: "single space is not data",
			row:  []string{" "},
			want: true,
		},
		{
			name: "number zero is data",
			row:  []string{"0"},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isEmptyRow(tt.row)
			if got != tt.want {
				t.Errorf("isEmptyRow(%v) = %v, want %v", tt.row, got, tt.want)
			}
		})
	}
}

// ============================================================================
// stripBOM Tests
// ============================================================================

func TestStripBOM(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  []byte
	}{
		{
			name:  "no BOM",
			input: []byte("hello"),
			want:  []byte("hello"),
		},
		{
			name:  "with UTF-8 BOM",
			input: []byte{0xEF, 0xBB, 0xBF, 'h', 'e', 'l', 'l', 'o'},
			want:  []byte("hello"),
		},
		{
			name:  "empty with BOM",
			input: []byte{0xEF, 0xBB, 0xBF},
			want:  []byte{},
		},
		{
			name:  "empty without BOM",
			input: []byte{},
			want:  []byte{},
		},
		{
			name:  "partial BOM (1 byte)",
			input: []byte{0xEF, 'h', 'e', 'l', 'l', 'o'},
			want:  []byte{0xEF, 'h', 'e', 'l', 'l', 'o'},
		},
		{
			name:  "partial BOM (2 bytes)",
			input: []byte{0xEF, 0xBB, 'h', 'e', 'l', 'l', 'o'},
			want:  []byte{0xEF, 0xBB, 'h', 'e', 'l', 'l', 'o'},
		},
		{
			name:  "BOM-like bytes not at start",
			input: []byte{'h', 0xEF, 0xBB, 0xBF, 'i'},
			want:  []byte{'h', 0xEF, 0xBB, 0xBF, 'i'},
		},
		{
			name:  "BOM with CSV content",
			input: append([]byte{0xEF, 0xBB, 0xBF}, []byte("name,email\njohn,john@example.com")...),
			want:  []byte("name,email\njohn,john@example.com"),
		},
		{
			name:  "BOM only (short file)",
			input: []byte{0xEF, 0xBB, 0xBF},
			want:  []byte{},
		},
		{
			name:  "single byte file",
			input: []byte{0xEF},
			want:  []byte{0xEF},
		},
		{
			name:  "two byte file",
			input: []byte{0xEF, 0xBB},
			want:  []byte{0xEF, 0xBB},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripBOM(tt.input)
			if !bytes.Equal(got, tt.want) {
				t.Errorf("stripBOM(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// ============================================================================
// parseCSV Tests
// ============================================================================

func TestParseCSV(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    [][]string
		wantErr bool
	}{
		{
			name:  "simple CSV",
			input: "a,b,c\n1,2,3",
			want: [][]string{
				{"a", "b", "c"},
				{"1", "2", "3"},
			},
		},
		{
			name:  "quoted fields",
			input: `"name","email"` + "\n" + `"John Doe","john@example.com"`,
			want: [][]string{
				{"name", "email"},
				{"John Doe", "john@example.com"},
			},
		},
		{
			name:  "quoted field with comma",
			input: `name,address` + "\n" + `John,"123 Main St, Apt 4"`,
			want: [][]string{
				{"name", "address"},
				{"John", "123 Main St, Apt 4"},
			},
		},
		{
			name:  "variable field count allowed",
			input: "a,b,c\n1,2\nx,y,z,w",
			want: [][]string{
				{"a", "b", "c"},
				{"1", "2"},
				{"x", "y", "z", "w"},
			},
		},
		{
			name:  "empty file",
			input: "",
			want:  nil,
		},
		{
			name:  "single cell",
			input: "value",
			want: [][]string{
				{"value"},
			},
		},
		{
			name:  "CRLF line endings",
			input: "a,b\r\n1,2\r\n",
			want: [][]string{
				{"a", "b"},
				{"1", "2"},
			},
		},
		{
			name:  "quoted field with newline",
			input: `name,notes` + "\n" + `John,"Line 1` + "\n" + `Line 2"`,
			want: [][]string{
				{"name", "notes"},
				{"John", "Line 1\nLine 2"},
			},
		},
		{
			name:  "empty cells",
			input: "a,,c\n,2,",
			want: [][]string{
				{"a", "", "c"},
				{"", "2", ""},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseCSV([]byte(tt.input))

			if (err != nil) != tt.wantErr {
				t.Errorf("parseCSV() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				return
			}

			if len(got) != len(tt.want) {
				t.Errorf("parseCSV() got %d rows, want %d", len(got), len(tt.want))
				return
			}

			for i, wantRow := range tt.want {
				if len(got[i]) != len(wantRow) {
					t.Errorf("row %d: got %d cells, want %d", i, len(got[i]), len(wantRow))
					continue
				}
				for j, wantCell := range wantRow {
					if got[i][j] != wantCell {
						t.Errorf("cell [%d][%d]: got %q, want %q", i, j, got[i][j], wantCell)
					}
				}
			}
		})
	}
}

// ============================================================================
// equalHeaders Tests
// ============================================================================

func TestEqualHeaders(t *testing.T) {
	tests := []struct {
		name   string
		a      []string
		b      []string
		want   bool
	}{
		{
			name: "exact match",
			a:    []string{"Name", "Email", "Phone"},
			b:    []string{"Name", "Email", "Phone"},
			want: true,
		},
		{
			name: "case insensitive match",
			a:    []string{"NAME", "EMAIL", "PHONE"},
			b:    []string{"name", "email", "phone"},
			want: true,
		},
		{
			name: "with whitespace trimming",
			a:    []string{"  Name  ", " Email ", "Phone"},
			b:    []string{"Name", "Email", "Phone"},
			want: true,
		},
		{
			name: "CSV has extra columns (superset)",
			a:    []string{"Name", "Email", "Phone", "Extra"},
			b:    []string{"Name", "Email", "Phone"},
			want: true,
		},
		{
			name: "CSV missing required column",
			a:    []string{"Name", "Email"},
			b:    []string{"Name", "Email", "Phone"},
			want: false,
		},
		{
			name: "different values",
			a:    []string{"Name", "Address"},
			b:    []string{"Name", "Email"},
			want: false,
		},
		{
			name: "different order",
			a:    []string{"Email", "Name", "Phone"},
			b:    []string{"Name", "Email", "Phone"},
			want: false,
		},
		{
			name: "empty required",
			a:    []string{"Name", "Email"},
			b:    []string{},
			want: true, // No required columns means any header matches
		},
		{
			name: "both empty",
			a:    []string{},
			b:    []string{},
			want: true,
		},
		{
			name: "with quotes in CSV header",
			a:    []string{`"Name"`, `"Email"`},
			b:    []string{"Name", "Email"},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := equalHeaders(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("equalHeaders(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// ============================================================================
// buildMappedHeaderIndex Tests
// ============================================================================

func TestBuildMappedHeaderIndex(t *testing.T) {
	tests := []struct {
		name      string
		mapping   map[string]int
		csvHeader []string
		checks    map[string]int // expected lowercase key -> index
	}{
		{
			name: "simple mapping",
			mapping: map[string]int{
				"Name":  0,
				"Email": 1,
			},
			csvHeader: []string{"n", "e", "x"},
			checks: map[string]int{
				"name":  0,
				"email": 1,
			},
		},
		{
			name: "out of bounds index ignored",
			mapping: map[string]int{
				"Name":  0,
				"Email": 10, // Out of bounds
			},
			csvHeader: []string{"a", "b", "c"},
			checks: map[string]int{
				"name": 0,
				// email should not be present
			},
		},
		{
			name: "negative index ignored",
			mapping: map[string]int{
				"Name":  0,
				"Email": -1, // Negative
			},
			csvHeader: []string{"a", "b"},
			checks: map[string]int{
				"name": 0,
			},
		},
		{
			name:      "empty mapping",
			mapping:   map[string]int{},
			csvHeader: []string{"a", "b"},
			checks:    map[string]int{},
		},
		{
			name: "empty header",
			mapping: map[string]int{
				"Name": 0,
			},
			csvHeader: []string{},
			checks:    map[string]int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := buildMappedHeaderIndex(tt.mapping, tt.csvHeader)

			// Check expected entries exist
			for key, wantIdx := range tt.checks {
				gotIdx, ok := idx[key]
				if !ok {
					t.Errorf("buildMappedHeaderIndex: expected key %q not found", key)
					continue
				}
				if gotIdx != wantIdx {
					t.Errorf("buildMappedHeaderIndex[%q] = %d, want %d", key, gotIdx, wantIdx)
				}
			}

			// Check no unexpected entries
			for key := range idx {
				if _, expected := tt.checks[key]; !expected {
					t.Errorf("buildMappedHeaderIndex: unexpected key %q in result", key)
				}
			}
		})
	}
}

// ============================================================================
// findHeaderInRecords Tests
// ============================================================================

func TestFindHeaderInRecords(t *testing.T) {
	tests := []struct {
		name     string
		records  [][]string
		required []string
		want     int
	}{
		{
			name: "header in first row",
			records: [][]string{
				{"Name", "Email", "Phone"},
				{"John", "john@example.com", "555-1234"},
			},
			required: []string{"Name", "Email", "Phone"},
			want:     0,
		},
		{
			name: "header in second row",
			records: [][]string{
				{"Report Title"},
				{"Name", "Email", "Phone"},
				{"John", "john@example.com", "555-1234"},
			},
			required: []string{"Name", "Email", "Phone"},
			want:     1,
		},
		{
			name: "header with extra columns",
			records: [][]string{
				{"Name", "Email", "Phone", "Notes"},
				{"John", "john@example.com", "555-1234", "VIP"},
			},
			required: []string{"Name", "Email"},
			want:     0,
		},
		{
			name: "header not found",
			records: [][]string{
				{"First", "Last", "Age"},
				{"John", "Doe", "30"},
			},
			required: []string{"Name", "Email"},
			want:     -1,
		},
		{
			name:     "empty records",
			records:  [][]string{},
			required: []string{"Name"},
			want:     -1,
		},
		{
			name: "case insensitive match",
			records: [][]string{
				{"NAME", "EMAIL"},
				{"John", "john@example.com"},
			},
			required: []string{"name", "email"},
			want:     0,
		},
		{
			name:     "empty required matches anything",
			records:  [][]string{{"a", "b"}},
			required: []string{},
			want:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findHeaderInRecords(tt.records, tt.required)
			if got != tt.want {
				t.Errorf("findHeaderInRecords() = %d, want %d", got, tt.want)
			}
		})
	}
}

// ============================================================================
// Benchmark Tests
// ============================================================================

func BenchmarkSanitizeUTF8_ValidInput(b *testing.B) {
	input := []byte("Hello, world! This is a valid UTF-8 string with some unicode: cafe")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sanitizeUTF8(input)
	}
}

func BenchmarkSanitizeUTF8_InvalidInput(b *testing.B) {
	// Create input with invalid bytes
	input := make([]byte, 1000)
	for i := range input {
		if i%10 == 0 {
			input[i] = 0x80 // Invalid UTF-8
		} else {
			input[i] = 'a'
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sanitizeUTF8(input)
	}
}

func BenchmarkStripBOM(b *testing.B) {
	input := append([]byte{0xEF, 0xBB, 0xBF}, bytes.Repeat([]byte("data,"), 100)...)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = stripBOM(input)
	}
}

func BenchmarkIsEmptyRow(b *testing.B) {
	row := []string{"", "  ", "\t", "", "   "}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = isEmptyRow(row)
	}
}

// Helper function for min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
