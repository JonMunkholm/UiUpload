# Code Restructuring Plan

**Status:** Completed
**Agents:** a007e14 (Plan)

## Implementation Notes

**Completed:**
- Handlers split into 6 focused files:
  - `handlers_common.go` - Shared utilities and dashboard
  - `handlers_upload.go` - File upload and progress SSE
  - `handlers_data.go` - Table data queries and export
  - `handlers_mutations.go` - Cell edits, row deletes, resets
  - `handlers_templates.go` - Import template CRUD
  - `handlers_audit.go` - Audit log and archive endpoints
- Service split into 5 files:
  - `service.go` - Core service initialization
  - `service_upload.go` - Upload processing
  - `service_query.go` - Table data queries
  - `service_mutations.go` - Cell/row mutations
  - `service_rollback.go` - Rollback operations
- Interfaces defined in `types.go` (DBTX interface)
- Error types in `internal/web/errors.go` and `internal/core/error_messages.go`
- Validation logic in `validation.go`
- All build and route tests pass

## Executive Summary

Decompose two monolithic files:
- `handlers.go` (1,350 lines) → 6 focused files
- `service.go` (1,690 lines) → 5 focused files

## Before/After Structure

```
Before:                          After:
internal/web/                    internal/web/
  handlers.go (1350)               handlers/
                                     common.go (~50)
                                     upload.go (~300)
                                     data.go (~250)
                                     mutations.go (~150)
                                     templates.go (~200)
                                     audit.go (~200)

internal/core/                   internal/core/
  service.go (1690)                service.go (~200)
                                   service_upload.go (~350)
                                   service_query.go (~400)
                                   service_mutations.go (~300)
                                   service_rollback.go (~150)
                                   interfaces.go (~80)
                                   errors.go (~60)
                                   validation.go (~100)
```

## Interface Definitions

```go
type Uploader interface {
    StartUpload(ctx, tableKey, fileName string, fileData []byte, mapping map[string]int) (string, error)
    SubscribeProgress(uploadID string) (<-chan UploadProgress, error)
    CancelUpload(uploadID string) error
    GetUploadResult(uploadID string) (*UploadResult, error)
}

type TableReader interface {
    ListTables() []TableInfo
    GetTableData(ctx, tableKey string, ...) (*TableDataResult, error)
    CheckDuplicates(ctx, tableKey string, keys []string) ([]string, error)
}

type TableMutator interface {
    UpdateCell(ctx, tableKey string, req UpdateCellRequest) (*UpdateCellResult, error)
    DeleteRows(ctx, tableKey string, keys []string) (int, error)
    Reset(ctx, tableKey string) error
}
```

## Consolidate Duplicates

1. Remove `toPgText` from `audit.go` (use `ToPgText` from `convert.go`)
2. Create `validation.go` with unified `ValidateRow()` function

## Dependency Order

1. `core/errors.go` (no deps)
2. `core/interfaces.go` (depends on types.go)
3. `core/validation.go` (uses convert.go)
4. `core/service*.go` files
5. `web/handlers/` files
6. Update `web/server.go`

## Test Criteria
- `go build ./...` passes
- All routes still work
- Interface compliance checks
