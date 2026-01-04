# Performance Optimization Plan

**Status:** Completed
**Agents:** a66ce67 (Plan)

## Implementation Notes

**Completed:**
1. **Dashboard N+1 Query** - Optimized in `service_query.go`
2. **Streaming File Upload** - `streaming.go` with StreamingUTF8Sanitizer
3. **Batch Failed Row Insertion** - Implemented in upload processing
4. **Streaming Export** - Cursor-based streaming in `service_query.go`
5. **Concurrent Upload Limits** - `upload_limiter.go` with semaphore pattern
6. **Connection Pool Tuning** - Configured in `internal/config/`:
   - MaxConns: 20 (5x default capacity)
   - MinConns: 4 (warm connections)
   - MaxConnLifetime: 1 hour
   - MaxConnIdleTime: 30 minutes

All 6 optimizations from the plan have been implemented.

## Executive Summary

Address 6 critical bottlenecks with expected 10x improvement.

## Optimizations

### 1. Dashboard N+1 Query
**Current:** 14 queries (2 per table × 7 tables)
**Fix:** Single CTE query with UNION ALL
**Impact:** 200ms → 20ms (10x)

### 2. Streaming File Upload
**Current:** `io.ReadAll(file)` - 100MB file = 300MB RAM
**Fix:** StreamingUTF8Sanitizer, pass io.Reader to CSV parser
**Impact:** O(file_size) → O(batch_size) constant memory

### 3. Batch Failed Row Insertion
**Current:** One INSERT per failed row
**Fix:** COPY protocol or multi-value INSERT
**Impact:** 10,000 failed rows: 20s → 200ms

### 4. Streaming Export
**Current:** Load all rows, then write CSV
**Fix:** Cursor-based streaming with chunked transfer
**Impact:** 1M rows: OOM → 10MB constant

### 5. Concurrent Upload Limits
**Current:** Unlimited parallel uploads
**Fix:** Semaphore (5 concurrent max)
**Impact:** DoS protection, predictable memory

### 6. Connection Pool Tuning
**Current:** 4 connections (default)
**Fix:** 20 connections, 4 min warm
**Impact:** 5x concurrent capacity

## Implementation Order (ROI)

| Priority | Optimization | Effort | Impact |
|----------|-------------|--------|--------|
| 1 | Connection pool | 1h | High |
| 2 | Upload limits | 2h | High |
| 3 | Batch failed rows | 3h | Medium |
| 4 | Dashboard query | 4h | High |
| 5 | Streaming export | 6h | Medium |
| 6 | Streaming upload | 8h | High |

## Key Code Patterns

```go
// Concurrent upload limiter
var uploadSemaphore = make(chan struct{}, 5)

// Streaming export
func StreamTableData(ctx, tableKey string, callback func(row) error) error
```
