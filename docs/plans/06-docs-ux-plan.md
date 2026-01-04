# Documentation and UX Plan

**Status:** Partially Implemented - Core UX done, OpenAPI pending
**Agents:** a2aacb9 (Plan)

## Implementation Notes

**Completed:**
- Error message mapping in `internal/core/error_messages.go`:
  - 25+ user-friendly error mappings with codes
  - MapError() function for translation
  - UserMessage struct with Message, Action, Code fields
- Structured logging with `log/slog`:
  - `internal/logging/logger.go` setup
  - Used in server, upload, scheduler, and error handling
- Loading states with HTMX:
  - `.htmx-request` spinner classes
  - Progress bars for uploads
- Configuration management:
  - `internal/config/config.go` - Centralized Config struct
  - `internal/config/loader.go` - Environment variable loading
  - Validation on startup
- Keyboard navigation in `internal/web/static/js/keyboard.js`
- SSE reconnection with backoff in `internal/web/static/js/sse.js`

**Remaining Work:**
- OpenAPI 3.0 spec for 25+ endpoints
- Swagger UI integration (optional)
- Godoc comments audit

## Executive Summary

Polish the application with documentation, better errors, and UX improvements.

## 1. API Documentation
- OpenAPI 3.0 spec for 25+ endpoints
- Swagger UI (optional)
- Godoc comments

## 2. Error Message Mapping

**Current:** "an internal error occurred" for everything

**Proposed:**
```go
var ErrorMapping = map[string]UserMessage{
    "duplicate key": {
        Message: "A record with this ID already exists",
        Action:  "Download failed rows to review duplicates",
        Code:    "DB001",
    },
    "invalid date": {
        Message: "Invalid date format detected",
        Action:  "Use YYYY-MM-DD, MM/DD/YYYY, or Jan 15, 2024",
        Code:    "VAL002",
    },
}
```

## 3. Structured Logging
- Migrate from `log.Printf` to `log/slog`
- Request ID propagation
- JSON format in production

## 4. Loading States
- HTMX `.htmx-request` class for spinners
- Skeleton loaders for tables
- Progress bars for uploads

## 5. UI Polish Items

| Item | Priority |
|------|----------|
| Cancel button during upload | P0 |
| File size limit display (100MB) | P0 |
| Empty state differentiation | P1 |
| SSE reconnection with backoff | P2 |
| Keyboard navigation | P2 |

## 6. Configuration Management
- Centralized `Config` struct
- Environment variable loading
- Validation on startup

## Implementation Priority

**Sprint 1:** Loading states, cancel button, file size display
**Sprint 2:** Error message mapping
**Sprint 3:** Structured logging, config management
**Sprint 4:** Keyboard nav, SSE reconnection
**Sprint 5:** OpenAPI spec, documentation
