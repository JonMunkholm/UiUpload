package core

import (
	"testing"
)

// ============================================================================
// WhereBuilder Tests
// ============================================================================

func TestNewWhereBuilder(t *testing.T) {
	wb := NewWhereBuilder()

	if wb == nil {
		t.Fatal("NewWhereBuilder returned nil")
	}

	if wb.argIndex != 1 {
		t.Errorf("expected argIndex to be 1, got %d", wb.argIndex)
	}

	if len(wb.conditions) != 0 {
		t.Errorf("expected empty conditions, got %d", len(wb.conditions))
	}

	if len(wb.args) != 0 {
		t.Errorf("expected empty args, got %d", len(wb.args))
	}
}

func TestWhereBuilder_Build_Empty(t *testing.T) {
	wb := NewWhereBuilder()
	whereClause, args := wb.Build()

	if whereClause != "" {
		t.Errorf("expected empty string for no conditions, got %q", whereClause)
	}

	if args != nil {
		t.Errorf("expected nil args for no conditions, got %v", args)
	}
}

func TestWhereBuilder_Add_SingleCondition(t *testing.T) {
	wb := NewWhereBuilder()
	wb.Add("status", "active")

	whereClause, args := wb.Build()

	expectedClause := " WHERE status = $1"
	if whereClause != expectedClause {
		t.Errorf("expected %q, got %q", expectedClause, whereClause)
	}

	if len(args) != 1 {
		t.Fatalf("expected 1 arg, got %d", len(args))
	}

	if args[0] != "active" {
		t.Errorf("expected arg 'active', got %v", args[0])
	}
}

func TestWhereBuilder_Add_MultipleConditions(t *testing.T) {
	wb := NewWhereBuilder()
	wb.Add("status", "active")
	wb.Add("type", "user")

	whereClause, args := wb.Build()

	expectedClause := " WHERE status = $1 AND type = $2"
	if whereClause != expectedClause {
		t.Errorf("expected %q, got %q", expectedClause, whereClause)
	}

	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}

	if args[0] != "active" || args[1] != "user" {
		t.Errorf("expected args ['active', 'user'], got %v", args)
	}
}

func TestWhereBuilder_Add_EmptyValue_Skipped(t *testing.T) {
	wb := NewWhereBuilder()
	wb.Add("status", "")
	wb.Add("type", "user")

	whereClause, args := wb.Build()

	// Empty value should be skipped, so only "type" condition should exist
	expectedClause := " WHERE type = $1"
	if whereClause != expectedClause {
		t.Errorf("expected %q, got %q", expectedClause, whereClause)
	}

	if len(args) != 1 {
		t.Fatalf("expected 1 arg, got %d", len(args))
	}
}

func TestWhereBuilder_AddUploadID(t *testing.T) {
	wb := NewWhereBuilder()
	wb.AddUploadID("abc-123-def")

	whereClause, args := wb.Build()

	expectedClause := " WHERE upload_id = $1"
	if whereClause != expectedClause {
		t.Errorf("expected %q, got %q", expectedClause, whereClause)
	}

	if len(args) != 1 {
		t.Fatalf("expected 1 arg, got %d", len(args))
	}

	if args[0] != "abc-123-def" {
		t.Errorf("expected arg 'abc-123-def', got %v", args[0])
	}
}

func TestWhereBuilder_AddUploadID_Empty_Skipped(t *testing.T) {
	wb := NewWhereBuilder()
	wb.AddUploadID("")

	whereClause, _ := wb.Build()

	if whereClause != "" {
		t.Errorf("expected empty where clause for empty upload ID, got %q", whereClause)
	}
}

func TestWhereBuilder_AddTimestampRange(t *testing.T) {
	wb := NewWhereBuilder()
	wb.AddTimestampRange("created_at", "2024-01-01", "2024-12-31")

	whereClause, args := wb.Build()

	expectedClause := " WHERE created_at >= $1 AND created_at <= $2"
	if whereClause != expectedClause {
		t.Errorf("expected %q, got %q", expectedClause, whereClause)
	}

	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}

	if args[0] != "2024-01-01" || args[1] != "2024-12-31" {
		t.Errorf("expected args ['2024-01-01', '2024-12-31'], got %v", args)
	}
}

func TestWhereBuilder_NextArgIndex(t *testing.T) {
	wb := NewWhereBuilder()

	if wb.NextArgIndex() != 1 {
		t.Errorf("expected initial NextArgIndex to be 1, got %d", wb.NextArgIndex())
	}

	wb.Add("col1", "val1")
	if wb.NextArgIndex() != 2 {
		t.Errorf("expected NextArgIndex after 1 add to be 2, got %d", wb.NextArgIndex())
	}

	wb.Add("col2", "val2")
	if wb.NextArgIndex() != 3 {
		t.Errorf("expected NextArgIndex after 2 adds to be 3, got %d", wb.NextArgIndex())
	}

	wb.AddTimestampRange("created_at", "start", "end")
	if wb.NextArgIndex() != 5 {
		t.Errorf("expected NextArgIndex after timestamp range to be 5, got %d", wb.NextArgIndex())
	}
}

func TestWhereBuilder_AddSearch(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		specs         []FieldSpec
		wantClause    string
		wantArgsCount int
	}{
		{
			name:          "empty query skipped",
			query:         "",
			specs:         []FieldSpec{{Name: "name", Type: FieldText}},
			wantClause:    "",
			wantArgsCount: 0,
		},
		{
			name:  "single text column",
			query: "search term",
			specs: []FieldSpec{
				{Name: "name", DBColumn: "name_col", Type: FieldText},
			},
			wantClause:    ` WHERE ("name_col" ILIKE $1)`,
			wantArgsCount: 1,
		},
		{
			name:  "multiple text columns",
			query: "test",
			specs: []FieldSpec{
				{Name: "name", DBColumn: "name", Type: FieldText},
				{Name: "email", DBColumn: "email", Type: FieldText},
				{Name: "age", DBColumn: "age", Type: FieldNumeric}, // Should be skipped
			},
			wantClause:    ` WHERE ("name" ILIKE $1 OR "email" ILIKE $1)`,
			wantArgsCount: 1,
		},
		{
			name:  "no text columns",
			query: "test",
			specs: []FieldSpec{
				{Name: "age", Type: FieldNumeric},
				{Name: "date", Type: FieldDate},
			},
			wantClause:    "",
			wantArgsCount: 0,
		},
		{
			name:  "derives db column from name",
			query: "test",
			specs: []FieldSpec{
				{Name: "User Name", Type: FieldText}, // Should become user_name
			},
			wantClause:    ` WHERE ("user_name" ILIKE $1)`,
			wantArgsCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wb := NewWhereBuilder()
			wb.AddSearch(tt.query, tt.specs)

			gotClause, gotArgs := wb.Build()

			if gotClause != tt.wantClause {
				t.Errorf("clause = %q, want %q", gotClause, tt.wantClause)
			}

			if len(gotArgs) != tt.wantArgsCount {
				t.Errorf("args count = %d, want %d", len(gotArgs), tt.wantArgsCount)
			}

			// Check wildcard wrapping for search
			if tt.wantArgsCount > 0 && tt.query != "" {
				expected := "%" + tt.query + "%"
				if gotArgs[0] != expected {
					t.Errorf("search arg = %q, want %q", gotArgs[0], expected)
				}
			}
		})
	}
}

func TestWhereBuilder_AddFilters(t *testing.T) {
	tests := []struct {
		name          string
		filters       FilterSet
		wantClause    string
		wantArgsCount int
		wantArgs      []interface{}
	}{
		{
			name:          "empty filter set",
			filters:       FilterSet{},
			wantClause:    "",
			wantArgsCount: 0,
		},
		{
			name: "single equals filter",
			filters: FilterSet{
				Filters: []ColumnFilter{
					{DBColumn: "status", Operator: OpEquals, Value: "active"},
				},
			},
			wantClause:    ` WHERE "status" = $1`,
			wantArgsCount: 1,
			wantArgs:      []interface{}{"active"},
		},
		{
			name: "contains filter",
			filters: FilterSet{
				Filters: []ColumnFilter{
					{DBColumn: "name", Operator: OpContains, Value: "john"},
				},
			},
			wantClause:    ` WHERE "name" ILIKE $1`,
			wantArgsCount: 1,
			wantArgs:      []interface{}{"%john%"},
		},
		{
			name: "starts with filter",
			filters: FilterSet{
				Filters: []ColumnFilter{
					{DBColumn: "email", Operator: OpStartsWith, Value: "admin"},
				},
			},
			wantClause:    ` WHERE "email" ILIKE $1`,
			wantArgsCount: 1,
			wantArgs:      []interface{}{"admin%"},
		},
		{
			name: "ends with filter",
			filters: FilterSet{
				Filters: []ColumnFilter{
					{DBColumn: "email", Operator: OpEndsWith, Value: ".com"},
				},
			},
			wantClause:    ` WHERE "email" ILIKE $1`,
			wantArgsCount: 1,
			wantArgs:      []interface{}{"%.com"},
		},
		{
			name: "greater than or equal filter",
			filters: FilterSet{
				Filters: []ColumnFilter{
					{DBColumn: "amount", Operator: OpGreaterEq, Value: "100"},
				},
			},
			wantClause:    ` WHERE "amount" >= $1`,
			wantArgsCount: 1,
			wantArgs:      []interface{}{"100"},
		},
		{
			name: "less than or equal filter",
			filters: FilterSet{
				Filters: []ColumnFilter{
					{DBColumn: "amount", Operator: OpLessEq, Value: "500"},
				},
			},
			wantClause:    ` WHERE "amount" <= $1`,
			wantArgsCount: 1,
			wantArgs:      []interface{}{"500"},
		},
		{
			name: "greater than filter",
			filters: FilterSet{
				Filters: []ColumnFilter{
					{DBColumn: "score", Operator: OpGreater, Value: "90"},
				},
			},
			wantClause:    ` WHERE "score" > $1`,
			wantArgsCount: 1,
			wantArgs:      []interface{}{"90"},
		},
		{
			name: "less than filter",
			filters: FilterSet{
				Filters: []ColumnFilter{
					{DBColumn: "score", Operator: OpLess, Value: "50"},
				},
			},
			wantClause:    ` WHERE "score" < $1`,
			wantArgsCount: 1,
			wantArgs:      []interface{}{"50"},
		},
		{
			name: "IN filter with multiple values",
			filters: FilterSet{
				Filters: []ColumnFilter{
					{DBColumn: "status", Operator: OpIn, Value: "active,pending,review"},
				},
			},
			wantClause:    ` WHERE "status" IN ($1, $2, $3)`,
			wantArgsCount: 3,
			wantArgs:      []interface{}{"active", "pending", "review"},
		},
		{
			name: "multiple filters combined with AND",
			filters: FilterSet{
				Filters: []ColumnFilter{
					{DBColumn: "status", Operator: OpEquals, Value: "active"},
					{DBColumn: "type", Operator: OpContains, Value: "user"},
				},
			},
			wantClause:    ` WHERE "status" = $1 AND "type" ILIKE $2`,
			wantArgsCount: 2,
			wantArgs:      []interface{}{"active", "%user%"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wb := NewWhereBuilder()
			wb.AddFilters(tt.filters)

			gotClause, gotArgs := wb.Build()

			if gotClause != tt.wantClause {
				t.Errorf("clause = %q, want %q", gotClause, tt.wantClause)
			}

			if len(gotArgs) != tt.wantArgsCount {
				t.Errorf("args count = %d, want %d", len(gotArgs), tt.wantArgsCount)
			}

			if tt.wantArgs != nil {
				for i, want := range tt.wantArgs {
					if i >= len(gotArgs) {
						t.Errorf("missing arg at index %d", i)
						continue
					}
					if gotArgs[i] != want {
						t.Errorf("arg[%d] = %v, want %v", i, gotArgs[i], want)
					}
				}
			}
		})
	}
}

// ============================================================================
// quoteIdentifier Tests
// ============================================================================

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "normal identifier",
			input: "users",
			want:  `"users"`,
		},
		{
			name:  "mixed case preserved",
			input: "UserName",
			want:  `"UserName"`,
		},
		{
			name:  "reserved word still quoted",
			input: "select",
			want:  `"select"`,
		},
		{
			name:  "contains space",
			input: "user name",
			want:  `"user name"`,
		},
		{
			name:  "contains double quote - escaped",
			input: `user"name`,
			want:  `"user""name"`,
		},
		{
			name:  "multiple double quotes",
			input: `user""name`,
			want:  `"user""""name"`,
		},
		{
			name:  "sql injection attempt safely quoted",
			input: `users"; DROP TABLE users; --`,
			want:  `"users""; DROP TABLE users; --"`,
		},
		{
			name:  "empty string",
			input: "",
			want:  `""`,
		},
		{
			name:  "unicode characters",
			input: "table_name",
			want:  `"table_name"`,
		},
		{
			name:  "snake_case",
			input: "transaction_id",
			want:  `"transaction_id"`,
		},
		{
			name:  "with special characters",
			input: "col@name!",
			want:  `"col@name!"`,
		},
		{
			name:  "with numbers",
			input: "column1",
			want:  `"column1"`,
		},
		{
			name:  "leading number",
			input: "1column",
			want:  `"1column"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := quoteIdentifier(tt.input)
			if got != tt.want {
				t.Errorf("quoteIdentifier(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// ============================================================================
// toDBColumnName Tests
// ============================================================================

func TestToDBColumnName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "already snake_case",
			input: "user_name",
			want:  "user_name",
		},
		{
			name:  "single word lowercase",
			input: "name",
			want:  "name",
		},
		{
			name:  "single word uppercase",
			input: "NAME",
			want:  "name",
		},
		{
			name:  "with spaces",
			input: "User Name",
			want:  "user_name",
		},
		{
			name:  "multiple spaces",
			input: "First Name Last Name",
			want:  "first_name_last_name",
		},
		{
			name:  "mixed case",
			input: "UserName",
			want:  "username",
		},
		{
			name:  "with numbers",
			input: "Column 1",
			want:  "column_1",
		},
		{
			name:  "empty string",
			input: "",
			want:  "",
		},
		{
			name:  "Transaction ID",
			input: "Transaction ID",
			want:  "transaction_id",
		},
		{
			name:  "account_name unchanged",
			input: "account_name",
			want:  "account_name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toDBColumnName(tt.input)
			if got != tt.want {
				t.Errorf("toDBColumnName(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// ============================================================================
// resolveDBColumn Tests
// ============================================================================

func TestResolveDBColumn(t *testing.T) {
	specs := []FieldSpec{
		{Name: "Transaction ID", DBColumn: "txn_id", Type: FieldText},
		{Name: "User Name", DBColumn: "", Type: FieldText}, // Should derive from name
		{Name: "Amount", DBColumn: "amount_cents", Type: FieldNumeric},
	}

	tests := []struct {
		name  string
		col   string
		want  string
	}{
		{
			name: "exact match with DBColumn",
			col:  "Transaction ID",
			want: "txn_id",
		},
		{
			name: "case insensitive match",
			col:  "TRANSACTION ID",
			want: "txn_id",
		},
		{
			name: "derives from name when DBColumn empty",
			col:  "User Name",
			want: "user_name",
		},
		{
			name: "not found - uses toDBColumnName",
			col:  "Unknown Column",
			want: "unknown_column",
		},
		{
			name: "amount with explicit DBColumn",
			col:  "Amount",
			want: "amount_cents",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveDBColumn(tt.col, specs)
			if got != tt.want {
				t.Errorf("resolveDBColumn(%q) = %q, want %q", tt.col, got, tt.want)
			}
		})
	}
}

func TestResolveDBColumns(t *testing.T) {
	specs := []FieldSpec{
		{Name: "Transaction ID", DBColumn: "txn_id", Type: FieldText},
		{Name: "Amount", DBColumn: "amount_cents", Type: FieldNumeric},
	}

	cols := []string{"Transaction ID", "Amount", "User Name"}
	got := resolveDBColumns(cols, specs)

	expected := []string{"txn_id", "amount_cents", "user_name"}

	if len(got) != len(expected) {
		t.Fatalf("expected %d columns, got %d", len(expected), len(got))
	}

	for i, want := range expected {
		if got[i] != want {
			t.Errorf("resolveDBColumns[%d] = %q, want %q", i, got[i], want)
		}
	}
}

// ============================================================================
// quoteColumns Tests
// ============================================================================

func TestQuoteColumns(t *testing.T) {
	tests := []struct {
		name  string
		cols  []string
		want  []string
	}{
		{
			name: "simple columns",
			cols: []string{"id", "name", "email"},
			want: []string{`"id"`, `"name"`, `"email"`},
		},
		{
			name: "with special characters",
			cols: []string{"user_id", `col"name`},
			want: []string{`"user_id"`, `"col""name"`},
		},
		{
			name: "empty slice",
			cols: []string{},
			want: []string{},
		},
		{
			name: "single column",
			cols: []string{"status"},
			want: []string{`"status"`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := quoteColumns(tt.cols)

			if len(got) != len(tt.want) {
				t.Fatalf("expected %d columns, got %d", len(tt.want), len(got))
			}

			for i, want := range tt.want {
				if got[i] != want {
					t.Errorf("quoteColumns[%d] = %q, want %q", i, got[i], want)
				}
			}
		})
	}
}

// ============================================================================
// buildSingleFilter Tests
// ============================================================================

func TestBuildSingleFilter(t *testing.T) {
	tests := []struct {
		name        string
		filter      ColumnFilter
		argIdx      int
		wantSQL     string
		wantArgs    []interface{}
		wantNextIdx int
	}{
		{
			name:        "equals operator",
			filter:      ColumnFilter{DBColumn: "status", Operator: OpEquals, Value: "active"},
			argIdx:      1,
			wantSQL:     `"status" = $1`,
			wantArgs:    []interface{}{"active"},
			wantNextIdx: 2,
		},
		{
			name:        "contains operator",
			filter:      ColumnFilter{DBColumn: "name", Operator: OpContains, Value: "john"},
			argIdx:      1,
			wantSQL:     `"name" ILIKE $1`,
			wantArgs:    []interface{}{"%john%"},
			wantNextIdx: 2,
		},
		{
			name:        "starts with operator",
			filter:      ColumnFilter{DBColumn: "email", Operator: OpStartsWith, Value: "admin"},
			argIdx:      3,
			wantSQL:     `"email" ILIKE $3`,
			wantArgs:    []interface{}{"admin%"},
			wantNextIdx: 4,
		},
		{
			name:        "ends with operator",
			filter:      ColumnFilter{DBColumn: "domain", Operator: OpEndsWith, Value: ".com"},
			argIdx:      1,
			wantSQL:     `"domain" ILIKE $1`,
			wantArgs:    []interface{}{"%.com"},
			wantNextIdx: 2,
		},
		{
			name:        "greater than or equal",
			filter:      ColumnFilter{DBColumn: "amount", Operator: OpGreaterEq, Value: "100"},
			argIdx:      1,
			wantSQL:     `"amount" >= $1`,
			wantArgs:    []interface{}{"100"},
			wantNextIdx: 2,
		},
		{
			name:        "less than or equal",
			filter:      ColumnFilter{DBColumn: "amount", Operator: OpLessEq, Value: "500"},
			argIdx:      2,
			wantSQL:     `"amount" <= $2`,
			wantArgs:    []interface{}{"500"},
			wantNextIdx: 3,
		},
		{
			name:        "greater than",
			filter:      ColumnFilter{DBColumn: "score", Operator: OpGreater, Value: "90"},
			argIdx:      1,
			wantSQL:     `"score" > $1`,
			wantArgs:    []interface{}{"90"},
			wantNextIdx: 2,
		},
		{
			name:        "less than",
			filter:      ColumnFilter{DBColumn: "score", Operator: OpLess, Value: "50"},
			argIdx:      1,
			wantSQL:     `"score" < $1`,
			wantArgs:    []interface{}{"50"},
			wantNextIdx: 2,
		},
		{
			name:        "IN operator with multiple values",
			filter:      ColumnFilter{DBColumn: "status", Operator: OpIn, Value: "active, pending, review"},
			argIdx:      1,
			wantSQL:     `"status" IN ($1, $2, $3)`,
			wantArgs:    []interface{}{"active", "pending", "review"},
			wantNextIdx: 4,
		},
		{
			name:        "IN operator with single value",
			filter:      ColumnFilter{DBColumn: "status", Operator: OpIn, Value: "active"},
			argIdx:      5,
			wantSQL:     `"status" IN ($5)`,
			wantArgs:    []interface{}{"active"},
			wantNextIdx: 6,
		},
		{
			name:        "unknown operator returns empty",
			filter:      ColumnFilter{DBColumn: "col", Operator: "unknown", Value: "val"},
			argIdx:      1,
			wantSQL:     "",
			wantArgs:    nil,
			wantNextIdx: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSQL, gotArgs, gotNextIdx := buildSingleFilter(tt.filter, tt.argIdx)

			if gotSQL != tt.wantSQL {
				t.Errorf("SQL = %q, want %q", gotSQL, tt.wantSQL)
			}

			if gotNextIdx != tt.wantNextIdx {
				t.Errorf("nextIdx = %d, want %d", gotNextIdx, tt.wantNextIdx)
			}

			if len(gotArgs) != len(tt.wantArgs) {
				t.Errorf("args length = %d, want %d", len(gotArgs), len(tt.wantArgs))
				return
			}

			for i, want := range tt.wantArgs {
				if gotArgs[i] != want {
					t.Errorf("args[%d] = %v, want %v", i, gotArgs[i], want)
				}
			}
		})
	}
}

// ============================================================================
// Integration Tests - Combined WhereBuilder Operations
// ============================================================================

func TestWhereBuilder_ComplexQuery(t *testing.T) {
	specs := []FieldSpec{
		{Name: "Name", DBColumn: "full_name", Type: FieldText},
		{Name: "Email", DBColumn: "email_addr", Type: FieldText},
		{Name: "Age", DBColumn: "age", Type: FieldNumeric},
	}

	wb := NewWhereBuilder()

	// Add search across text columns
	wb.AddSearch("john", specs)

	// Add specific filters
	wb.AddFilters(FilterSet{
		Filters: []ColumnFilter{
			{DBColumn: "status", Operator: OpEquals, Value: "active"},
			{DBColumn: "age", Operator: OpGreaterEq, Value: "18"},
		},
	})

	// Add upload ID
	wb.AddUploadID("upload-123")

	whereClause, args := wb.Build()

	// Verify structure (order may vary, but should have all conditions ANDed)
	expectedConditions := []string{
		`"full_name" ILIKE`,
		`"email_addr" ILIKE`,
		`"status" = `,
		`"age" >= `,
		"upload_id = ",
	}

	for _, cond := range expectedConditions {
		if !contains(whereClause, cond) {
			t.Errorf("expected whereClause to contain %q, got %q", cond, whereClause)
		}
	}

	// Verify arg count: 1 (search) + 2 (filters) + 1 (upload_id) = 4
	if len(args) != 4 {
		t.Errorf("expected 4 args, got %d: %v", len(args), args)
	}

	// First arg should be the search term with wildcards
	if args[0] != "%john%" {
		t.Errorf("expected first arg to be '%%john%%', got %v", args[0])
	}
}

// contains checks if s contains substr
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
