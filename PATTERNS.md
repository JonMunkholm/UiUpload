# Coding Patterns

This document codifies the established coding conventions for this Go codebase.
All new code should follow these patterns to maintain consistency and quality.

---

## 1. Error Handling

### Error Wrapping with Context

Always wrap errors with context that describes what operation failed.
Use `fmt.Errorf` with `%w` to preserve the error chain.

**Good:**
```go
func (s *Service) GetTableData(ctx context.Context, tableKey string) (*TableDataResult, error) {
    if err := s.pool.QueryRow(ctx, countQuery, queryArgs...).Scan(&totalRows); err != nil {
        return nil, fmt.Errorf("count rows: %w", err)
    }
    // ...
    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("rows error: %w", err)
    }
    return result, nil
}
```

**Bad:**
```go
func (s *Service) GetTableData(ctx context.Context, tableKey string) (*TableDataResult, error) {
    if err := s.pool.QueryRow(ctx, countQuery, queryArgs...).Scan(&totalRows); err != nil {
        return nil, err  // No context about what failed
    }
    return result, nil
}
```

**Rationale:** Wrapped errors create a trail that makes debugging easier. When an error surfaces, the chain shows exactly where it originated.

### User-Facing Error Messages

Never expose raw database or internal errors to users. Use `core.MapError()` to convert technical errors to user-friendly messages with action suggestions.

**Good:**
```go
// In handlers: Map technical errors to user-friendly messages
func writeError(w http.ResponseWriter, status int, message string) {
    userMsg := core.MapError(fmt.Errorf("%s", message))

    slog.Warn("http error",
        "status", status,
        "message", message,
        "code", userMsg.Code,
    )

    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(status)
    json.NewEncoder(w).Encode(struct {
        Error   string `json:"error"`
        Message string `json:"message"`
        Action  string `json:"action,omitempty"`
        Code    string `json:"code"`
    }{
        Error:   userMsg.Message,
        Message: userMsg.Message,
        Action:  userMsg.Action,
        Code:    userMsg.Code,
    })
}
```

**Bad:**
```go
func writeError(w http.ResponseWriter, status int, err error) {
    http.Error(w, err.Error(), status)  // Exposes internal details
}
```

**Rationale:** Users should receive helpful guidance, not stack traces. Error codes allow support staff to quickly diagnose issues.

### Error Code Categories

Error codes follow a consistent naming scheme in `error_messages.go`:

| Category | Code Range | Examples |
|----------|------------|----------|
| Database | DB001-DB099 | DB001 (duplicate key), DB004 (connection refused) |
| Validation | VAL001-VAL099 | VAL001 (invalid date), VAL004 (missing column) |
| File | FILE001-FILE099 | FILE001 (too large), FILE002 (invalid CSV) |
| Upload | UPL001-UPL099 | UPL002 (system busy), UPL003 (session expired) |
| Table | TBL001-TBL099 | TBL001 (not found), TBL002 (unknown type) |
| Rate Limit | RATE001-RATE099 | RATE001 (too many requests) |
| Default | ERR000 | Unknown/unexpected errors |

---

## 2. Naming Conventions

### Handler Functions

HTTP handlers are methods on `*Server` with the prefix `handle`.
Name describes the action, not the HTTP method.

**Good:**
```go
func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) { ... }
func (s *Server) handleTableView(w http.ResponseWriter, r *http.Request) { ... }
func (s *Server) handleAuditLogExport(w http.ResponseWriter, r *http.Request) { ... }
```

**Bad:**
```go
func UploadHandler(w http.ResponseWriter, r *http.Request) { ... }     // Not a method
func (s *Server) postUpload(w http.ResponseWriter, r *http.Request) { ... }  // HTTP method in name
```

### Service Methods

Service methods use verb prefixes that describe the operation:

| Prefix | Purpose | Example |
|--------|---------|---------|
| `Get` | Retrieve single item | `GetTableData`, `GetUploadResult` |
| `List` | Retrieve multiple items | `ListTables`, `ListTablesByGroup` |
| `Count` | Return count | `CountAuditLog` |
| `Create` | Create new resource | `CreateTemplate` |
| `Update` | Modify existing resource | `UpdateCell`, `UpdateTemplate` |
| `Delete` | Remove resource | `DeleteRows`, `DeleteTemplate` |
| `Check` | Validate/verify | `CheckDuplicates` |
| `Stream` | Row-by-row callback | `StreamTableData`, `StreamAuditLog` |
| `Start` | Begin async operation | `StartUploadStreaming` |
| `Cancel` | Abort operation | `CancelUpload` |
| `Subscribe` | Get update channel | `SubscribeProgress` |

**Good:**
```go
func (s *Service) GetTableData(ctx context.Context, tableKey string, ...) (*TableDataResult, error)
func (s *Service) StreamAuditLog(ctx context.Context, filter AuditLogFilter, callback func(AuditEntry) error) error
func (s *Service) CheckDuplicates(ctx context.Context, tableKey string, keys []string) ([]string, error)
```

### Test Functions

Tests use `Test` prefix with descriptive names. Use subtests for variations.

**Good:**
```go
func TestWhereBuilder_Add_SingleCondition(t *testing.T) { ... }
func TestQuoteIdentifier(t *testing.T) {
    tests := []struct {
        name  string
        input string
        want  string
    }{
        {"normal identifier", "users", `"users"`},
        {"with space", "user name", `"user name"`},
        {"sql injection attempt", `users"; DROP TABLE`, `"users""; DROP TABLE"`},
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) { ... })
    }
}
```

**Bad:**
```go
func Test1(t *testing.T) { ... }  // Non-descriptive
func TestAddCondition(t *testing.T) { ... }  // Missing context
```

### File Naming

| Pattern | Purpose | Examples |
|---------|---------|----------|
| `handlers_*.go` | HTTP handler groups | `handlers_upload.go`, `handlers_audit.go` |
| `service_*.go` | Service method groups | `service_query.go`, `service_mutations.go` |
| `*_test.go` | Test files | `helpers_test.go`, `streaming_test.go` |
| `types.go` | Type definitions | Core data structures |
| `helpers.go` | Utility functions | `WhereBuilder`, column helpers |

### Variable Naming

- Use `camelCase` for all variables and struct fields
- Acronyms follow Go conventions: `ID` not `Id`, `URL` not `Url`
- Context is always named `ctx`
- Errors are named `err`
- HTTP handlers: `w` for ResponseWriter, `r` for Request

```go
uploadID := chi.URLParam(r, "uploadID")  // Good: ID uppercase
tableKey := chi.URLParam(r, "tableKey")  // Good: camelCase
ctx := r.Context()                        // Good: standard name
```

---

## 3. SQL Safety

### Identifier Quoting

**Always** use `quoteIdentifier()` for table and column names in dynamic SQL.
This prevents SQL injection and handles special characters.

**Good:**
```go
func quoteIdentifier(name string) string {
    return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

query := fmt.Sprintf(
    "SELECT %s FROM %s WHERE %s = $1",
    quoteIdentifier(dbCol),
    quoteIdentifier(tableKey),
    quoteIdentifier(keyCol),
)
```

**Bad:**
```go
query := fmt.Sprintf(
    "SELECT %s FROM %s WHERE %s = $1",
    dbCol,       // Vulnerable to injection
    tableKey,    // Vulnerable to injection
    keyCol,
)
```

### Parameterized Queries with WhereBuilder

Use `WhereBuilder` for constructing WHERE clauses. It handles parameter indexing automatically.

**Good:**
```go
wb := NewWhereBuilder()
wb.AddSearch("john", specs)                    // Adds ILIKE conditions for text columns
wb.AddFilters(filters)                          // Adds column filter conditions
wb.Add("table_key", "sfdc_customers")          // Adds equality condition
wb.AddTimestampRange("created_at", start, end) // Adds range condition

whereClause, args := wb.Build()
// whereClause: " WHERE (...) AND ... = $1 AND created_at >= $2 AND created_at <= $3"
// args: properly ordered slice of values

query := fmt.Sprintf(
    "SELECT * FROM audit_log%s ORDER BY created_at LIMIT $%d",
    whereClause,
    wb.NextArgIndex(),
)
args = append(args, limit)
```

**Bad:**
```go
// Manual string concatenation is error-prone and verbose
where := ""
args := []interface{}{}
argIdx := 1
if tableKey != "" {
    where += fmt.Sprintf(" AND table_key = $%d", argIdx)
    args = append(args, tableKey)
    argIdx++
}
// ... repeat for every condition
```

**Rationale:** `WhereBuilder` eliminates off-by-one errors in parameter indexing and ensures consistent WHERE clause structure.

### Never Build SQL with User Input

**Good:**
```go
// User values go through parameters
query := "SELECT * FROM users WHERE name = $1"
rows, err := pool.Query(ctx, query, userName)
```

**Bad:**
```go
// NEVER concatenate user input into SQL
query := "SELECT * FROM users WHERE name = '" + userName + "'"  // SQL INJECTION
```

---

## 4. File Organization

### Package Structure

```
internal/
  config/       # Configuration loading and validation
    config.go       # Config struct definitions
    loader.go       # Environment parsing, defaults
    config_test.go

  core/         # Business logic (no HTTP dependencies)
    types.go        # Core type definitions
    service.go      # Service struct and constructor
    service_*.go    # Service method groups by domain
    helpers.go      # WhereBuilder, column utilities
    streaming.go    # Reader composition for I/O
    error_messages.go  # User error mapping
    audit.go        # Audit logging logic
    validation.go   # Row/cell validation
    *_test.go       # Unit tests
    benchmark_test.go  # Performance benchmarks

  database/     # Generated sqlc code (do not edit)
    db.go
    models.go
    *.sql.go

  logging/      # Structured logging
    logger.go

  web/          # HTTP layer
    server.go       # Server struct, routes, middleware
    context.go      # Request context helpers
    errors.go       # Error response handling
    handlers_*.go   # Handler groups
    middleware/     # Custom middleware
    templates/      # templ-generated templates
    static/         # Embedded static files
```

### What Goes Where

| File Type | Contents |
|-----------|----------|
| `types.go` | Struct definitions, constants, interfaces |
| `service.go` | Service constructor, core configuration |
| `service_query.go` | Read operations (Get, List, Count, Stream) |
| `service_mutations.go` | Write operations (Create, Update, Delete) |
| `helpers.go` | Pure functions with no side effects |
| `handlers_common.go` | Shared handler utilities |
| `handlers_{domain}.go` | Handlers grouped by domain (upload, audit, etc.) |

---

## 5. HTTP Handlers

### Standard Handler Structure

```go
// handleXxx processes [description of what it does].
func (s *Server) handleXxx(w http.ResponseWriter, r *http.Request) {
    // 1. Extract and validate URL parameters
    tableKey := chi.URLParam(r, "tableKey")
    if tableKey == "" {
        writeError(w, http.StatusBadRequest, "missing table key")
        return
    }

    // 2. Parse query parameters or request body
    page := parseIntParam(r, "page", 1)  // With defaults

    // 3. Decode JSON body if needed
    var req struct {
        Keys []string `json:"keys"`
    }
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        writeError(w, http.StatusBadRequest, "invalid request body")
        return
    }

    // 4. Validate business rules
    if len(req.Keys) == 0 {
        writeError(w, http.StatusBadRequest, "no rows specified")
        return
    }

    // 5. Call service layer
    result, err := s.service.DoOperation(r.Context(), tableKey, req.Keys)
    if err != nil {
        writeError(w, http.StatusInternalServerError, err.Error())
        return
    }

    // 6. Return response
    writeJSON(w, result)
}
```

### HTMX Considerations

Check `HX-Request` header to return partials vs full pages:

```go
if r.Header.Get("HX-Request") == "true" {
    templates.TablePartial(params).Render(r.Context(), w)
} else {
    templates.TableView(params).Render(r.Context(), w)
}
```

### Error Responses

Use the centralized error helpers:

```go
// JSON API errors
writeError(w, http.StatusBadRequest, "missing table key")

// JSON success
writeJSON(w, result)

// Content-aware error handling (HTMX, JSON, or HTML)
s.respondError(w, r, err, http.StatusInternalServerError)
```

### Context Propagation for Audit

Add request metadata to context for audit logging:

```go
ctx := WithRequestMetadata(r.Context(), r)
result, err := s.service.RollbackUpload(ctx, uploadID)
```

---

## 6. Testing

### Table-Driven Tests

Group related test cases with descriptive names:

```go
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
        // ... more cases
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            gotSQL, gotArgs, gotNextIdx := buildSingleFilter(tt.filter, tt.argIdx)

            if gotSQL != tt.wantSQL {
                t.Errorf("SQL = %q, want %q", gotSQL, tt.wantSQL)
            }
            // ... verify other fields
        })
    }
}
```

### Test Naming

```go
func TestComponentName(t *testing.T) { ... }           // Basic test
func TestComponentName_SpecificCase(t *testing.T) { ... }  // Specific scenario
func TestComponentName_Empty_ReturnsDefault(t *testing.T) { ... }  // Detailed behavior
```

### Benchmarks

Use `b.ReportAllocs()` for memory-critical paths:

```go
func BenchmarkToPgNumeric(b *testing.B) {
    testCases := []string{"123", "-456.78", "$1,234.56"}

    b.ResetTimer()
    b.ReportAllocs()
    for i := 0; i < b.N; i++ {
        for _, tc := range testCases {
            ToPgNumeric(tc)
        }
    }
}

// Parallel benchmarks for thread-safety verification
func BenchmarkToPgNumericParallel(b *testing.B) {
    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            ToPgNumeric("$1,234.56")
        }
    })
}
```

### Assertions

Use the standard testing package with clear error messages:

```go
if got != want {
    t.Errorf("functionName(%q) = %q, want %q", input, got, want)
}

if len(result) != expectedLen {
    t.Fatalf("expected %d items, got %d", expectedLen, len(result))
}
```

---

## 7. Concurrency

### Context Usage

Pass `context.Context` as the first parameter to any function that:
- Makes network calls (database, HTTP)
- Can be cancelled
- Needs timeout support

```go
func (s *Service) GetTableData(ctx context.Context, tableKey string, ...) (*TableDataResult, error) {
    // Check for cancellation before expensive operations
    if ctx.Err() != nil {
        return nil, ctx.Err()
    }

    rows, err := s.pool.Query(ctx, query, args...)
    // ...
}
```

### Mutex Patterns

Use `sync.RWMutex` for shared state with read-heavy access:

```go
type Service struct {
    mu      sync.RWMutex
    uploads map[string]*activeUpload
}

func (s *Service) getUpload(id string) (*activeUpload, bool) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    upload, ok := s.uploads[id]
    return upload, ok
}

func (s *Service) setUpload(id string, upload *activeUpload) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.uploads[id] = upload
}
```

### Channel Patterns for Progress

Use buffered channels with non-blocking sends for progress updates:

```go
type activeUpload struct {
    Listeners  []chan UploadProgress
    ListenerMu sync.Mutex
}

// Non-blocking send - skip if listener is slow
func (upload *activeUpload) notifyProgress() {
    upload.ListenerMu.Lock()
    defer upload.ListenerMu.Unlock()

    for _, ch := range upload.Listeners {
        select {
        case ch <- upload.Progress:
        default:
            // Listener is slow, skip this update
        }
    }
}
```

---

## 8. Configuration

### Environment Variables with Struct Tags

Use struct tags to document configuration:

```go
type UploadConfig struct {
    // MaxFileSize is the maximum allowed file size in bytes (default: 100MB)
    MaxFileSize int64 `env:"UPLOAD_MAX_FILE_SIZE" default:"104857600"`

    // MaxConcurrent is the maximum number of parallel uploads (default: 5)
    MaxConcurrent int `env:"UPLOAD_MAX_CONCURRENT" default:"5"`

    // Timeout is the maximum duration for a single upload (default: 10m)
    Timeout time.Duration `env:"UPLOAD_TIMEOUT" default:"10m"`
}
```

### Validation on Load

Fail fast with clear error messages:

```go
func Load() (*Config, error) {
    cfg := &Config{}

    if err := loadFromEnv(cfg); err != nil {
        return nil, fmt.Errorf("load config: %w", err)
    }

    if cfg.Database.URL == "" {
        return nil, fmt.Errorf("DATABASE_URL is required")
    }

    if cfg.Upload.BatchSize < 1 {
        return nil, fmt.Errorf("UPLOAD_BATCH_SIZE must be >= 1")
    }

    return cfg, nil
}
```

### Sensible Defaults

Provide defaults that work for development:

```go
type ServerConfig struct {
    Host string `env:"SERVER_HOST" default:"0.0.0.0"`
    Port int    `env:"SERVER_PORT" default:"8080"`
}
```

---

## 9. Logging

### Structured Logging with slog

Use `log/slog` for all logging with structured fields:

```go
import "log/slog"

slog.Info("upload completed",
    "upload_id", uploadID,
    "table", tableKey,
    "inserted", result.Inserted,
    "skipped", result.Skipped,
    "duration", duration,
)

slog.Error("database query failed",
    "query", query,
    "error", err,
)
```

### Request ID Correlation

Include request ID in all logs for a request:

```go
// Get logger with request ID from context
logger := logging.FromContext(r.Context())
logger.Info("processing upload", "table", tableKey)

// Or add multiple fields
uploadLogger := logging.WithFields(ctx,
    "upload_id", uploadID,
    "table", tableKey,
)
uploadLogger.Info("upload started")
// Later...
uploadLogger.Info("upload completed", "rows", inserted)
```

### Log Levels

| Level | Usage |
|-------|-------|
| `Debug` | Detailed debugging info (development only) |
| `Info` | Normal operational events |
| `Warn` | Unexpected but handled situations |
| `Error` | Errors that need attention |

```go
slog.Debug("parsing row", "line", lineNum, "data", row)  // Only in development
slog.Info("server starting", "addr", addr)               // Normal operations
slog.Warn("http error", "status", 400, "error", err)     // Expected errors
slog.Error("database connection failed", "error", err)  // Needs attention
```

---

## 10. Streaming/I/O

### Reader Composition

Compose readers for layered transformations:

```go
// WrapForStreaming applies BOM removal, UTF-8 sanitization, and byte counting
func WrapForStreaming(r io.Reader, totalSize int64) *StreamingCountingReader {
    bomReader := NewBOMSkippingReader(r)           // Layer 1: Remove BOM
    sanitizedReader := NewStreamingUTF8Sanitizer(bomReader)  // Layer 2: Fix UTF-8
    return NewStreamingCountingReader(sanitizedReader, totalSize)  // Layer 3: Count
}
```

### Memory-Efficient Callbacks

Use callbacks for row-by-row processing instead of loading everything into memory:

```go
// Good: Stream rows directly to CSV writer
func (s *Service) StreamTableData(ctx context.Context, tableKey string, callback func(row TableRow) error) error {
    rows, err := s.pool.Query(ctx, query, args...)
    if err != nil {
        return err
    }
    defer rows.Close()

    for rows.Next() {
        if ctx.Err() != nil {
            return ctx.Err()  // Check for cancellation
        }

        row := parseRow(rows)
        if err := callback(row); err != nil {
            return err
        }
    }
    return rows.Err()
}

// Usage in handler
err := s.service.StreamTableData(ctx, tableKey, func(row core.TableRow) error {
    record := formatRow(row)
    return csvWriter.Write(record)
})
```

**Bad: Loading everything into memory**
```go
// Avoid for large datasets
data, err := s.service.GetAllTableData(ctx, tableKey)  // Loads all into memory
for _, row := range data.Rows {
    csvWriter.Write(formatRow(row))
}
```

### HTTP Response Streaming

For large exports, stream directly to the response:

```go
func (s *Server) handleExportData(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "text/csv")
    w.Header().Set("Content-Disposition", `attachment; filename="export.csv"`)

    csvWriter := csv.NewWriter(w)
    rowCount := 0
    const flushInterval = 1000

    err := s.service.StreamTableData(r.Context(), tableKey, func(row core.TableRow) error {
        if err := csvWriter.Write(formatRow(row)); err != nil {
            return err
        }

        rowCount++
        if rowCount % flushInterval == 0 {
            csvWriter.Flush()
            if f, ok := w.(http.Flusher); ok {
                f.Flush()  // Flush HTTP response for chunked transfer
            }
        }
        return nil
    })

    csvWriter.Flush()
}
```

---

## Quick Reference

### Common Patterns Checklist

- [ ] Errors wrapped with context (`fmt.Errorf("operation: %w", err)`)
- [ ] User-facing errors use `core.MapError()` or `writeError()`
- [ ] SQL identifiers quoted with `quoteIdentifier()`
- [ ] Parameters use `WhereBuilder` or direct `$N` placeholders
- [ ] Context passed to all I/O operations
- [ ] Handler validates input before calling service
- [ ] Tests use table-driven format with subtests
- [ ] Benchmarks call `b.ReportAllocs()` for memory-sensitive code
- [ ] Logging uses `slog` with structured fields
- [ ] Large data uses streaming callbacks, not in-memory arrays

---

## Enforcement

To verify code follows these patterns, use the **patterns-enforcer** agent:

```
/run patterns-enforcer
```

The agent will:
1. Read this PATTERNS.md file to load current conventions
2. Check target files against each pattern category
3. Report violations with exact locations and suggested fixes
4. Classify findings by severity (Critical, Warning, Suggestion)

### When to Run

- **After implementing features** -- Before committing new code
- **During code review** -- Catch mechanical violations automatically
- **After refactoring** -- Ensure patterns weren't lost in the change
- **Onboarding** -- Learn conventions by seeing what matches and what doesn't

### Severity Levels

| Level | Examples | Action |
|-------|----------|--------|
| **Critical** | SQL injection risk, unwrapped errors in service layer | Fix before commit |
| **Warning** | Naming violations, handler structure deviations | Fix soon |
| **Suggestion** | Test naming, logging field naming | Nice to have |

### Integration with Other Agents

After patterns-enforcer passes:
- Use `code-reviewer` for design and maintainability review
- Use `security-auditor` if code handles sensitive data
- Use `performance-optimizer` for hot paths

The patterns-enforcer focuses on consistency. Trust the other agents for their domains.
