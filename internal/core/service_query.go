package core

import (
	"context"
	"fmt"
	"strings"

	db "github.com/JonMunkholm/TUI/internal/database"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ListTables returns information about all registered tables.
func (s *Service) ListTables() []TableInfo {
	defs := All()
	infos := make([]TableInfo, len(defs))
	for i, def := range defs {
		infos[i] = def.Info
	}
	return infos
}

// ListTablesByGroup returns tables organized by group.
func (s *Service) ListTablesByGroup() map[string][]TableInfo {
	result := make(map[string][]TableInfo)
	for _, group := range Groups() {
		for _, def := range ByGroup(group) {
			result[group] = append(result[group], def.Info)
		}
	}
	return result
}

// countTable returns the row count for a specific table.
func countTable(ctx context.Context, pool *pgxpool.Pool, tableKey string) (int64, error) {
	q := db.New(pool)

	switch tableKey {
	case "sfdc_customers":
		return q.CountSfdcCustomers(ctx)
	case "sfdc_price_book":
		return q.CountSfdcPriceBook(ctx)
	case "sfdc_opp_detail":
		return q.CountSfdcOppDetail(ctx)
	case "ns_customers":
		return q.CountNsCustomers(ctx)
	case "ns_invoice_detail":
		return q.CountNsInvoiceDetail(ctx)
	case "ns_so_detail":
		return q.CountNsSoDetail(ctx)
	case "anrok_transactions":
		return q.CountAnrokTransactions(ctx)
	default:
		return 0, fmt.Errorf("unknown table: %s", tableKey)
	}
}

// TableRow represents a single row of data as key-value pairs.
type TableRow map[string]interface{}

// TableDataResult contains paginated table data.
type TableDataResult struct {
	Rows          []TableRow
	TotalRows     int64
	Page          int
	PageSize      int
	TotalPages    int
	Sorts         []SortSpec        // Ordered list of sort specifications (max 2)
	SortColumn    string            // Primary sort column (first in Sorts) - kept for backwards compat
	SortDir       string            // Primary sort direction - kept for backwards compat
	SearchQuery   string            // Current search term, if any
	ActiveFilters map[string]string // Active column filters: column -> "op:value"
	Aggregations  Aggregations      // Column aggregations for numeric columns
}

// buildSingleFilter generates SQL for a single filter.
func buildSingleFilter(f ColumnFilter, argIdx int) (string, []interface{}, int) {
	col := quoteIdentifier(f.DBColumn)

	switch f.Operator {
	case OpContains:
		return fmt.Sprintf("%s ILIKE $%d", col, argIdx),
			[]interface{}{"%" + f.Value + "%"}, argIdx + 1

	case OpEquals:
		return fmt.Sprintf("%s = $%d", col, argIdx),
			[]interface{}{f.Value}, argIdx + 1

	case OpStartsWith:
		return fmt.Sprintf("%s ILIKE $%d", col, argIdx),
			[]interface{}{f.Value + "%"}, argIdx + 1

	case OpEndsWith:
		return fmt.Sprintf("%s ILIKE $%d", col, argIdx),
			[]interface{}{"%" + f.Value}, argIdx + 1

	case OpGreaterEq:
		return fmt.Sprintf("%s >= $%d", col, argIdx),
			[]interface{}{f.Value}, argIdx + 1

	case OpLessEq:
		return fmt.Sprintf("%s <= $%d", col, argIdx),
			[]interface{}{f.Value}, argIdx + 1

	case OpGreater:
		return fmt.Sprintf("%s > $%d", col, argIdx),
			[]interface{}{f.Value}, argIdx + 1

	case OpLess:
		return fmt.Sprintf("%s < $%d", col, argIdx),
			[]interface{}{f.Value}, argIdx + 1

	case OpIn:
		values := strings.Split(f.Value, ",")
		if len(values) == 0 {
			return "", nil, argIdx
		}
		placeholders := make([]string, len(values))
		filterArgs := make([]interface{}, len(values))
		for i, v := range values {
			placeholders[i] = fmt.Sprintf("$%d", argIdx+i)
			filterArgs[i] = strings.TrimSpace(v)
		}
		return fmt.Sprintf("%s IN (%s)", col, strings.Join(placeholders, ", ")),
			filterArgs, argIdx + len(values)

	default:
		return "", nil, argIdx
	}
}

// GetTableData fetches paginated, sorted, and optionally filtered data from any table.
func (s *Service) GetTableData(ctx context.Context, tableKey string, page, pageSize int, sorts []SortSpec, searchQuery string, filters FilterSet) (*TableDataResult, error) {
	def, ok := Get(tableKey)
	if !ok {
		return nil, fmt.Errorf("unknown table: %s", tableKey)
	}

	// Build column mappings using helper
	displayColumns := def.Info.Columns
	dbColumns := resolveDBColumns(displayColumns, def.FieldSpecs)
	quotedCols := quoteColumns(dbColumns)

	// Build WHERE clause using WhereBuilder
	wb := NewWhereBuilder()
	wb.AddSearch(searchQuery, def.FieldSpecs)
	wb.AddFilters(filters)
	whereClause, queryArgs := wb.Build()

	// Get total count (with search filter)
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s%s", quoteIdentifier(tableKey), whereClause)
	var totalRows int64
	err := s.pool.QueryRow(ctx, countQuery, queryArgs...).Scan(&totalRows)
	if err != nil {
		return nil, fmt.Errorf("count rows: %w", err)
	}

	// Calculate pagination
	if page < 1 {
		page = 1
	}
	totalPages := int((totalRows + int64(pageSize) - 1) / int64(pageSize))
	if totalPages < 1 {
		totalPages = 1
	}
	if page > totalPages {
		page = totalPages
	}
	offset := (page - 1) * pageSize

	// Build ORDER BY clause from sorts (max 2 levels)
	var validSorts []SortSpec
	var orderParts []string
	for _, sort := range sorts {
		if sort.Column == "" || !containsColumn(displayColumns, sort.Column) {
			continue
		}
		dir := strings.ToLower(sort.Dir)
		if dir != "asc" && dir != "desc" {
			dir = "asc"
		}
		// Find DB column name
		sortDBColumn := toDBColumnName(sort.Column)
		for _, spec := range def.FieldSpecs {
			if spec.Name == sort.Column && spec.DBColumn != "" {
				sortDBColumn = spec.DBColumn
				break
			}
		}
		orderParts = append(orderParts, fmt.Sprintf("%s %s", quoteIdentifier(sortDBColumn), dir))
		validSorts = append(validSorts, SortSpec{Column: sort.Column, Dir: dir})
		if len(validSorts) >= 2 { // Max 2 sort levels
			break
		}
	}

	// Default to first column if no valid sorts
	if len(orderParts) == 0 {
		defaultCol := displayColumns[0]
		defaultDBCol := toDBColumnName(defaultCol)
		for _, spec := range def.FieldSpecs {
			if spec.Name == defaultCol && spec.DBColumn != "" {
				defaultDBCol = spec.DBColumn
				break
			}
		}
		orderParts = append(orderParts, fmt.Sprintf("%s asc", quoteIdentifier(defaultDBCol)))
		validSorts = append(validSorts, SortSpec{Column: defaultCol, Dir: "asc"})
	}

	// Build SELECT query with WHERE and ORDER BY clauses
	argIndex := wb.NextArgIndex()
	query := fmt.Sprintf(
		"SELECT %s FROM %s%s ORDER BY %s LIMIT $%d OFFSET $%d",
		strings.Join(quotedCols, ", "),
		quoteIdentifier(tableKey),
		whereClause,
		strings.Join(orderParts, ", "),
		argIndex,
		argIndex+1,
	)
	queryArgs = append(queryArgs, pageSize, offset)

	// Execute query
	rows, err := s.pool.Query(ctx, query, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("query rows: %w", err)
	}
	defer rows.Close()

	// Collect results
	var resultRows []TableRow
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("read row values: %w", err)
		}

		row := make(TableRow)
		for i, col := range displayColumns {
			row[col] = values[i]
		}
		resultRows = append(resultRows, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	// Build ActiveFilters map for UI state
	activeFilters := make(map[string]string)
	for _, f := range filters.Filters {
		activeFilters[f.Column] = string(f.Operator) + ":" + f.Value
	}

	// Extract primary sort for backwards compatibility
	primarySortCol := ""
	primarySortDir := ""
	if len(validSorts) > 0 {
		primarySortCol = validSorts[0].Column
		primarySortDir = validSorts[0].Dir
	}

	result := &TableDataResult{
		Rows:          resultRows,
		TotalRows:     totalRows,
		Page:          page,
		PageSize:      pageSize,
		TotalPages:    totalPages,
		Sorts:         validSorts,
		SortColumn:    primarySortCol,
		SortDir:       primarySortDir,
		SearchQuery:   searchQuery,
		ActiveFilters: activeFilters,
	}

	// Fetch aggregations for numeric columns
	if aggs, err := s.GetColumnAggregations(ctx, tableKey, searchQuery, filters); err == nil {
		result.Aggregations = aggs
	}

	return result, nil
}

// GetColumnAggregations calculates Sum, Avg, Min, Max for numeric columns.
// Uses the same WHERE clause as GetTableData to aggregate filtered data.
func (s *Service) GetColumnAggregations(ctx context.Context, tableKey string, searchQuery string, filters FilterSet) (Aggregations, error) {
	def, ok := Get(tableKey)
	if !ok {
		return nil, fmt.Errorf("unknown table: %s", tableKey)
	}

	// Identify numeric columns from FieldSpecs
	type numericCol struct {
		name     string
		dbColumn string
	}
	var numericCols []numericCol

	for _, spec := range def.FieldSpecs {
		if spec.Type == FieldNumeric {
			dbCol := spec.DBColumn
			if dbCol == "" {
				dbCol = toDBColumnName(spec.Name)
			}
			numericCols = append(numericCols, numericCol{spec.Name, dbCol})
		}
	}

	// Early return if no numeric columns
	if len(numericCols) == 0 {
		return Aggregations{}, nil
	}

	// Build WHERE clause using WhereBuilder
	wb := NewWhereBuilder()
	wb.AddSearch(searchQuery, def.FieldSpecs)
	wb.AddFilters(filters)
	whereClause, queryArgs := wb.Build()

	// Build aggregation SELECT expressions: SUM, AVG, MIN, MAX, COUNT per column
	var selectExprs []string
	for _, col := range numericCols {
		quoted := quoteIdentifier(col.dbColumn)
		selectExprs = append(selectExprs,
			fmt.Sprintf("SUM(%s)", quoted),
			fmt.Sprintf("AVG(%s)", quoted),
			fmt.Sprintf("MIN(%s)", quoted),
			fmt.Sprintf("MAX(%s)", quoted),
			fmt.Sprintf("COUNT(%s)", quoted),
		)
	}

	query := fmt.Sprintf("SELECT %s FROM %s%s",
		strings.Join(selectExprs, ", "),
		quoteIdentifier(tableKey),
		whereClause,
	)

	row := s.pool.QueryRow(ctx, query, queryArgs...)

	// Scan results - 5 values per column (sum, avg, min, max, count)
	scanDest := make([]interface{}, len(numericCols)*5)
	for i := range numericCols {
		base := i * 5
		scanDest[base] = new(*float64)   // Sum
		scanDest[base+1] = new(*float64) // Avg
		scanDest[base+2] = new(*float64) // Min
		scanDest[base+3] = new(*float64) // Max
		scanDest[base+4] = new(int64)    // Count
	}

	if err := row.Scan(scanDest...); err != nil {
		return nil, fmt.Errorf("scan aggregations: %w", err)
	}

	// Build result map
	result := make(Aggregations)
	for i, col := range numericCols {
		base := i * 5
		agg := &ColumnAggregation{
			Column: col.name,
		}

		// Extract values (handling NULL as nil)
		if v := scanDest[base].(**float64); *v != nil {
			agg.Sum = *v
		}
		if v := scanDest[base+1].(**float64); *v != nil {
			agg.Avg = *v
		}
		if v := scanDest[base+2].(**float64); *v != nil {
			agg.Min = *v
		}
		if v := scanDest[base+3].(**float64); *v != nil {
			agg.Max = *v
		}
		if v := scanDest[base+4].(*int64); v != nil {
			agg.Count = *v
		}

		result[col.name] = agg
	}

	return result, nil
}

// GetAllTableData fetches all data from a table without pagination.
// Used for CSV export. Optionally filters by search query and column filters.
func (s *Service) GetAllTableData(ctx context.Context, tableKey, searchQuery string, filters FilterSet) (*TableDataResult, error) {
	def, ok := Get(tableKey)
	if !ok {
		return nil, fmt.Errorf("unknown table: %s", tableKey)
	}

	// Build column names using helper
	displayColumns := def.Info.Columns
	dbColumns := resolveDBColumns(displayColumns, def.FieldSpecs)
	quotedCols := quoteColumns(dbColumns)

	// Build WHERE clause using WhereBuilder
	wb := NewWhereBuilder()
	wb.AddSearch(searchQuery, def.FieldSpecs)
	wb.AddFilters(filters)
	whereClause, queryArgs := wb.Build()

	// Get total count (with search and filter)
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s%s", quoteIdentifier(tableKey), whereClause)
	var totalRows int64
	err := s.pool.QueryRow(ctx, countQuery, queryArgs...).Scan(&totalRows)
	if err != nil {
		return nil, fmt.Errorf("count rows: %w", err)
	}

	// Build ActiveFilters map for UI state
	activeFilters := make(map[string]string)
	for _, f := range filters.Filters {
		activeFilters[f.Column] = string(f.Operator) + ":" + f.Value
	}

	if totalRows == 0 {
		return &TableDataResult{
			Rows:          []TableRow{},
			TotalRows:     0,
			SearchQuery:   searchQuery,
			ActiveFilters: activeFilters,
		}, nil
	}

	// Query ALL rows (no LIMIT/OFFSET), sorted by first column
	query := fmt.Sprintf(
		"SELECT %s FROM %s%s ORDER BY %s ASC",
		strings.Join(quotedCols, ", "),
		quoteIdentifier(tableKey),
		whereClause,
		quotedCols[0],
	)

	rows, err := s.pool.Query(ctx, query, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("query rows: %w", err)
	}
	defer rows.Close()

	var resultRows []TableRow
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("read row values: %w", err)
		}

		row := make(TableRow)
		for i, col := range displayColumns {
			row[col] = values[i]
		}
		resultRows = append(resultRows, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return &TableDataResult{
		Rows:          resultRows,
		TotalRows:     totalRows,
		SearchQuery:   searchQuery,
		ActiveFilters: activeFilters,
	}, nil
}

// containsColumn checks if a column name exists in the list.
func containsColumn(columns []string, target string) bool {
	for _, col := range columns {
		if strings.EqualFold(col, target) {
			return true
		}
	}
	return false
}

// quoteIdentifier quotes a SQL identifier to prevent injection.
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// toDBColumnName converts a display column name to a database column name.
// "Transaction ID" -> "transaction_id"
// "account_name" -> "account_name" (no change if already snake_case)
func toDBColumnName(name string) string {
	// Replace spaces with underscores and convert to lowercase
	return strings.ToLower(strings.ReplaceAll(name, " ", "_"))
}

// CheckDuplicates checks if any of the provided keys already exist in the database.
// Keys are expected to be in the format "val1|val2" for composite keys.
// Returns the list of keys that already exist in the database.
func (s *Service) CheckDuplicates(ctx context.Context, tableKey string, keys []string) ([]string, error) {
	def, ok := Get(tableKey)
	if !ok {
		return nil, fmt.Errorf("unknown table: %s", tableKey)
	}

	uniqueKey := def.Info.UniqueKey
	if len(uniqueKey) == 0 {
		return []string{}, nil // No unique key defined, can't check duplicates
	}

	// Build the DB column names for unique key columns
	dbCols := resolveDBColumns(uniqueKey, def.FieldSpecs)

	// Build query based on single vs composite key
	var query string
	var args []interface{}

	if len(uniqueKey) == 1 {
		// Single column unique key - use ANY for efficiency
		query = fmt.Sprintf(
			"SELECT DISTINCT %s FROM %s WHERE %s = ANY($1)",
			quoteIdentifier(dbCols[0]),
			quoteIdentifier(tableKey),
			quoteIdentifier(dbCols[0]),
		)
		args = []interface{}{keys}
	} else {
		// Composite key - need to parse keys and use tuple comparison
		// Keys are in format "val1|val2"
		var conditions []string
		argIndex := 1

		for _, key := range keys {
			parts := strings.Split(key, "|")
			if len(parts) != len(uniqueKey) {
				continue // Invalid key format
			}

			placeholders := make([]string, len(parts))
			for i, part := range parts {
				placeholders[i] = fmt.Sprintf("$%d", argIndex)
				args = append(args, part)
				argIndex++
			}

			condition := fmt.Sprintf("(%s) = (%s)",
				strings.Join(quoteColumns(dbCols), ", "),
				strings.Join(placeholders, ", "),
			)
			conditions = append(conditions, condition)
		}

		if len(conditions) == 0 {
			return []string{}, nil // No valid keys to check
		}

		// Select concatenated key columns
		concatExpr := make([]string, len(dbCols))
		for i, col := range dbCols {
			concatExpr[i] = fmt.Sprintf("COALESCE(%s::text, '')", quoteIdentifier(col))
		}

		query = fmt.Sprintf(
			"SELECT DISTINCT %s FROM %s WHERE %s",
			strings.Join(concatExpr, " || '|' || "),
			quoteIdentifier(tableKey),
			strings.Join(conditions, " OR "),
		)
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("check duplicates: %w", err)
	}
	defer rows.Close()

	var existing []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("scan key: %w", err)
		}
		existing = append(existing, key)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return existing, nil
}

// quoteColumns quotes each column name in the slice.
func quoteColumns(cols []string) []string {
	quoted := make([]string, len(cols))
	for i, col := range cols {
		quoted[i] = quoteIdentifier(col)
	}
	return quoted
}

// StreamTableData streams table data row by row via callback, avoiding memory accumulation.
// Used for large CSV exports. The callback receives display column names as keys.
// Returns after all rows are processed or on first error.
func (s *Service) StreamTableData(ctx context.Context, tableKey, searchQuery string, filters FilterSet, callback func(row TableRow) error) error {
	def, ok := Get(tableKey)
	if !ok {
		return fmt.Errorf("unknown table: %s", tableKey)
	}

	// Build column names using helper
	displayColumns := def.Info.Columns
	dbColumns := resolveDBColumns(displayColumns, def.FieldSpecs)
	quotedCols := quoteColumns(dbColumns)

	// Build WHERE clause using WhereBuilder
	wb := NewWhereBuilder()
	wb.AddSearch(searchQuery, def.FieldSpecs)
	wb.AddFilters(filters)
	whereClause, queryArgs := wb.Build()

	// Query ALL rows (no LIMIT/OFFSET), sorted by first column
	query := fmt.Sprintf(
		"SELECT %s FROM %s%s ORDER BY %s ASC",
		strings.Join(quotedCols, ", "),
		quoteIdentifier(tableKey),
		whereClause,
		quotedCols[0],
	)

	rows, err := s.pool.Query(ctx, query, queryArgs...)
	if err != nil {
		return fmt.Errorf("query rows: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		// Check for context cancellation (user closed browser)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		values, err := rows.Values()
		if err != nil {
			return fmt.Errorf("read row values: %w", err)
		}

		row := make(TableRow)
		for i, col := range displayColumns {
			row[col] = values[i]
		}

		if err := callback(row); err != nil {
			return err
		}
	}

	return rows.Err()
}
