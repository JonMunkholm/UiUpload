package core

import (
	"bytes"
	"encoding/csv"
	"io"
	"strings"
	"testing"
)

// ============================================================================
// Conversion Function Benchmarks
// ============================================================================

// BenchmarkToPgNumeric benchmarks numeric string conversion.
// This is a hot path during CSV import for any numeric columns.
func BenchmarkToPgNumeric(b *testing.B) {
	testCases := []string{
		"123",
		"-456.78",
		"$1,234.56",
		"(123.45)",      // Accounting negative
		"1,234,567.89",  // Thousands separators
		"  999.99  ",    // Whitespace
		"\u20ac1234.56", // Euro
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, tc := range testCases {
			ToPgNumeric(tc)
		}
	}
}

// BenchmarkToPgNumeric_Simple benchmarks the most common case: plain integers.
func BenchmarkToPgNumeric_Simple(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ToPgNumeric("12345")
	}
}

// BenchmarkToPgNumeric_Currency benchmarks currency string conversion.
func BenchmarkToPgNumeric_Currency(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ToPgNumeric("$1,234,567.89")
	}
}

// BenchmarkToPgDate benchmarks date string parsing.
// This is a hot path during CSV import for date columns.
func BenchmarkToPgDate(b *testing.B) {
	testCases := []string{
		"2024-01-15",   // ISO format
		"01/15/2024",   // US format
		"Jan 15, 2024", // Text month
		"20240115",     // Compact
		"1/5/24",       // 2-digit year
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, tc := range testCases {
			ToPgDate(tc)
		}
	}
}

// BenchmarkToPgDate_ISO benchmarks the most common date format (ISO 8601).
func BenchmarkToPgDate_ISO(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ToPgDate("2024-01-15")
	}
}

// BenchmarkToPgDate_US benchmarks US date format parsing.
func BenchmarkToPgDate_US(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ToPgDate("01/15/2024")
	}
}

// BenchmarkToPgDate_TwoDigitYear benchmarks 2-digit year parsing with pivot.
func BenchmarkToPgDate_TwoDigitYear(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ToPgDate("1/15/99")
	}
}

// BenchmarkToPgBool benchmarks boolean string conversion.
func BenchmarkToPgBool(b *testing.B) {
	testCases := []string{
		"true", "false",
		"yes", "no",
		"1", "0",
		"Y", "N",
		"  true  ", // with whitespace
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, tc := range testCases {
			ToPgBool(tc)
		}
	}
}

// BenchmarkToPgText benchmarks text conversion with trimming.
func BenchmarkToPgText(b *testing.B) {
	testCases := []string{
		"hello world",
		"  hello  ",
		"",
		"   ",
		"A longer string with multiple words and some content",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, tc := range testCases {
			ToPgText(tc)
		}
	}
}

// ============================================================================
// Cell Cleaning Benchmarks
// ============================================================================

// BenchmarkCleanCell benchmarks CSV cell cleaning.
// Called for every cell during import, so performance is critical.
func BenchmarkCleanCell(b *testing.B) {
	testCases := []string{
		"normal value",
		`="formula"`,       // Excel formula prefix
		`"quoted"`,         // Quoted
		"  whitespace  ",   // Whitespace
		"netsuite:ABC123",  // NetSuite prefix
		`="12345"`,         // Number as text in Excel
		"'single quoted'",  // Single quotes
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, tc := range testCases {
			CleanCell(tc)
		}
	}
}

// BenchmarkCleanCell_Simple benchmarks the common case: no cleaning needed.
func BenchmarkCleanCell_Simple(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CleanCell("simple value")
	}
}

// BenchmarkCleanCell_ExcelFormula benchmarks Excel formula prefix removal.
func BenchmarkCleanCell_ExcelFormula(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CleanCell(`="12345"`)
	}
}

// ============================================================================
// Header Index Benchmarks
// ============================================================================

// BenchmarkMakeHeaderIndex benchmarks header index creation.
// Called once per file upload to build the column lookup map.
func BenchmarkMakeHeaderIndex(b *testing.B) {
	headers := []string{
		"Transaction ID", "Date", "Customer Name", "Amount",
		"Status", "Description", "Account", "Reference",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MakeHeaderIndex(headers)
	}
}

// BenchmarkMakeHeaderIndex_Large benchmarks with many columns.
func BenchmarkMakeHeaderIndex_Large(b *testing.B) {
	headers := make([]string, 50)
	for i := range headers {
		headers[i] = strings.Repeat("Column_", i+1)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MakeHeaderIndex(headers)
	}
}

// ============================================================================
// WhereBuilder Benchmarks
// ============================================================================

// BenchmarkWhereBuilder benchmarks SQL WHERE clause construction.
// Used for every filtered table query in the UI.
func BenchmarkWhereBuilder(b *testing.B) {
	specs := []FieldSpec{
		{Name: "Name", DBColumn: "full_name", Type: FieldText},
		{Name: "Email", DBColumn: "email", Type: FieldText},
		{Name: "Status", DBColumn: "status", Type: FieldText},
	}
	filters := FilterSet{
		Filters: []ColumnFilter{
			{DBColumn: "status", Operator: OpEquals, Value: "active"},
			{DBColumn: "name", Operator: OpContains, Value: "john"},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wb := NewWhereBuilder()
		wb.AddSearch("test", specs)
		wb.AddFilters(filters)
		wb.AddUploadID("abc-123")
		wb.Build()
	}
}

// BenchmarkWhereBuilder_Simple benchmarks a simple single condition.
func BenchmarkWhereBuilder_Simple(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wb := NewWhereBuilder()
		wb.Add("status", "active")
		wb.Build()
	}
}

// BenchmarkBuildSingleFilter benchmarks individual filter building.
func BenchmarkBuildSingleFilter(b *testing.B) {
	filter := ColumnFilter{DBColumn: "status", Operator: OpContains, Value: "active"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buildSingleFilter(filter, 1)
	}
}

// BenchmarkBuildSingleFilter_In benchmarks IN operator with multiple values.
func BenchmarkBuildSingleFilter_In(b *testing.B) {
	filter := ColumnFilter{
		DBColumn: "status",
		Operator: OpIn,
		Value:    "active,pending,review,completed,cancelled",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buildSingleFilter(filter, 1)
	}
}

// ============================================================================
// Identifier Quoting Benchmarks
// ============================================================================

// BenchmarkQuoteIdentifier benchmarks SQL identifier quoting.
// Called frequently when building dynamic SQL queries.
func BenchmarkQuoteIdentifier(b *testing.B) {
	identifiers := []string{
		"simple",
		"with spaces",
		"with\"quotes",
		"MixedCase",
		"transaction_id",
		"users\"; DROP TABLE users; --", // SQL injection attempt
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, id := range identifiers {
			quoteIdentifier(id)
		}
	}
}

// BenchmarkQuoteIdentifier_Simple benchmarks the common case.
func BenchmarkQuoteIdentifier_Simple(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		quoteIdentifier("column_name")
	}
}

// BenchmarkToDBColumnName benchmarks column name conversion to snake_case.
func BenchmarkToDBColumnName(b *testing.B) {
	names := []string{
		"User Name",
		"Transaction ID",
		"already_snake_case",
		"MixedCase",
		"First Name Last Name",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, name := range names {
			toDBColumnName(name)
		}
	}
}

// ============================================================================
// UTF-8 Sanitization Benchmarks (Large dataset, complements upload_test.go)
// ============================================================================

// BenchmarkSanitizeUTF8_LargeDataset benchmarks a larger data set.
// Complements the smaller input benchmarks in upload_test.go.
func BenchmarkSanitizeUTF8_LargeDataset(b *testing.B) {
	// Generate 10KB of valid UTF-8
	data := bytes.Repeat([]byte("Valid UTF-8 line with numbers 12345\n"), 300)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sanitizeUTF8(data)
	}
}

// ============================================================================
// CSV Parsing Benchmarks
// ============================================================================

// BenchmarkParseCSV benchmarks CSV parsing memory usage.
func BenchmarkParseCSV(b *testing.B) {
	data := generateTestCSV(100)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		parseCSV(data)
	}
}

// BenchmarkParseCSV_Large benchmarks parsing a larger CSV.
func BenchmarkParseCSV_Large(b *testing.B) {
	data := generateTestCSV(1000)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		parseCSV(data)
	}
}

// BenchmarkCSVParsing_Comparison compares ReadAll vs streaming approaches.
func BenchmarkCSVParsing_Comparison(b *testing.B) {
	data := generateTestCSV(500)

	b.Run("ReadAll", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// Original approach: parse all at once
			csv.NewReader(bytes.NewReader(data)).ReadAll()
		}
	})

	b.Run("Streaming", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// Streaming approach: read row by row
			r := csv.NewReader(bytes.NewReader(data))
			r.FieldsPerRecord = -1
			for {
				_, err := r.Read()
				if err == io.EOF {
					break
				}
			}
		}
	})
}

// ============================================================================
// Row Processing Benchmarks (Extended, complements upload_test.go)
// ============================================================================

// BenchmarkIsEmptyRow_Extended benchmarks empty row detection with various inputs.
// Complements the basic benchmark in upload_test.go with subtests.
func BenchmarkIsEmptyRow_Extended(b *testing.B) {
	tests := []struct {
		name string
		row  []string
	}{
		{"large_empty", make([]string, 50)}, // 50 empty columns
		{"large_non_empty", func() []string {
			row := make([]string, 50)
			row[49] = "data" // Last column has data
			return row
		}()},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				isEmptyRow(tt.row)
			}
		})
	}
}

// BenchmarkStripBOM_LargeFile benchmarks BOM removal on larger data.
// Complements the basic benchmark in upload_test.go.
func BenchmarkStripBOM_LargeFile(b *testing.B) {
	// Large file with BOM
	data := append([]byte{0xEF, 0xBB, 0xBF}, bytes.Repeat([]byte("data line\n"), 1000)...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stripBOM(data)
	}
}

// ============================================================================
// Validation Benchmarks
// ============================================================================

// BenchmarkValidateCell benchmarks cell validation.
func BenchmarkValidateCell(b *testing.B) {
	specs := []struct {
		name  string
		spec  FieldSpec
		value string
	}{
		{"text", FieldSpec{Name: "Name", Type: FieldText}, "John Doe"},
		{"numeric_valid", FieldSpec{Name: "Amount", Type: FieldNumeric}, "1234.56"},
		{"numeric_invalid", FieldSpec{Name: "Amount", Type: FieldNumeric}, "not a number"},
		{"date_valid", FieldSpec{Name: "Date", Type: FieldDate}, "2024-01-15"},
		{"date_invalid", FieldSpec{Name: "Date", Type: FieldDate}, "invalid date"},
		{"bool_valid", FieldSpec{Name: "Active", Type: FieldBool}, "yes"},
		{"enum_valid", FieldSpec{Name: "Status", Type: FieldEnum, EnumValues: []string{"active", "pending"}}, "active"},
	}

	for _, s := range specs {
		b.Run(s.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				ValidateCell(s.value, s.spec)
			}
		})
	}
}

// BenchmarkRowValidator benchmarks full row validation.
func BenchmarkRowValidator(b *testing.B) {
	specs := []FieldSpec{
		{Name: "Name", Type: FieldText, Required: true},
		{Name: "Email", Type: FieldText, Required: true},
		{Name: "Date", Type: FieldDate, Required: true},
		{Name: "Amount", Type: FieldNumeric, Required: true},
		{Name: "Active", Type: FieldBool, Required: false},
	}
	headerIdx := map[string]int{
		"name":   0,
		"email":  1,
		"date":   2,
		"amount": 3,
		"active": 4,
	}
	validator := NewRowValidator(specs, headerIdx)
	row := []string{"John Doe", "john@example.com", "2024-01-15", "1234.56", "yes"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		validator.ValidateRow(row)
	}
}

// BenchmarkRowValidator_FirstError benchmarks first-error-only validation.
func BenchmarkRowValidator_FirstError(b *testing.B) {
	specs := []FieldSpec{
		{Name: "Name", Type: FieldText, Required: true},
		{Name: "Email", Type: FieldText, Required: true},
		{Name: "Date", Type: FieldDate, Required: true},
		{Name: "Amount", Type: FieldNumeric, Required: true},
	}
	headerIdx := map[string]int{
		"name":   0,
		"email":  1,
		"date":   2,
		"amount": 3,
	}
	validator := NewRowValidator(specs, headerIdx)
	row := []string{"John Doe", "john@example.com", "2024-01-15", "1234.56"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		validator.ValidateRowFirst(row)
	}
}

// BenchmarkValidateHeaders benchmarks header validation.
func BenchmarkValidateHeaders(b *testing.B) {
	specs := []FieldSpec{
		{Name: "Name", Required: true},
		{Name: "Email", Required: true},
		{Name: "Date", Required: true},
		{Name: "Amount", Required: true},
		{Name: "Status", Required: false},
		{Name: "Notes", Required: false},
	}
	headers := []string{"Name", "Email", "Date", "Amount", "Status", "Notes", "Extra Column"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ValidateHeaders(headers, specs)
	}
}

// ============================================================================
// Parallel Benchmarks
// ============================================================================

// BenchmarkToPgNumericParallel benchmarks parallel numeric conversion.
func BenchmarkToPgNumericParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ToPgNumeric("$1,234.56")
		}
	})
}

// BenchmarkToPgDateParallel benchmarks parallel date parsing.
func BenchmarkToPgDateParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ToPgDate("2024-01-15")
		}
	})
}

// BenchmarkCleanCellParallel benchmarks parallel cell cleaning.
func BenchmarkCleanCellParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			CleanCell(`="formula value"`)
		}
	})
}

// BenchmarkWhereBuilderParallel benchmarks parallel WHERE building.
func BenchmarkWhereBuilderParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			wb := NewWhereBuilder()
			wb.Add("status", "active")
			wb.Add("type", "user")
			wb.Build()
		}
	})
}

// ============================================================================
// Memory Allocation Benchmarks
// ============================================================================

// BenchmarkConversionsAllocs measures allocations in conversion functions.
func BenchmarkConversionsAllocs(b *testing.B) {
	b.Run("ToPgNumeric", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ToPgNumeric("$1,234.56")
		}
	})

	b.Run("ToPgDate", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ToPgDate("2024-01-15")
		}
	})

	b.Run("CleanCell", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			CleanCell(`="formula"`)
		}
	})

	b.Run("QuoteIdentifier", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			quoteIdentifier("column_name")
		}
	})
}

// ============================================================================
// Helper Functions
// ============================================================================

// generateTestCSV generates CSV data with the specified number of rows.
func generateTestCSV(rows int) []byte {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)

	// Header
	w.Write([]string{"ID", "Name", "Email", "Date", "Amount", "Status"})

	// Data rows
	for i := 0; i < rows; i++ {
		w.Write([]string{
			"1001",
			"John Doe",
			"john@example.com",
			"2024-01-15",
			"$1,234.56",
			"active",
		})
	}
	w.Flush()

	return buf.Bytes()
}
