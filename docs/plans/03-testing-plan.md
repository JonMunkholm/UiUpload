# Testing Strategy Plan

**Status:** Partially Implemented - Core tests done, web/integration pending
**Agents:** aad023b (Plan)

## Implementation Notes

**Completed:**
- 350+ test cases for pure functions (Phase 1 target exceeded)
- `convert_test.go` - ToPgNumeric, ToPgDate, ToPgBool, CleanCell tests
- `helpers_test.go` - WhereBuilder, quoteIdentifier tests
- `upload_test.go` - Upload processing tests
- `streaming_test.go` - Streaming functionality tests
- `upload_limiter_test.go` - Concurrent upload limiting tests
- `error_messages_test.go` - Error mapping tests
- `benchmark_test.go` - Performance benchmarks

**Remaining Work:**
- Web handler tests (Phase 2 with mocks)
- Integration tests with test database (Phase 3)
- CI integration with coverage reporting
- Target: reach 80% overall coverage

## Executive Summary

Build test coverage from 0% to 80% in phases:
- Phase 1: Pure functions (40% coverage)
- Phase 2: With mocks (60% coverage)
- Phase 3: Integration tests (80% coverage)

## Priority 1: Pure Functions (No Mocking)

### convert.go
| Function | Cases |
|----------|-------|
| ToPgNumeric | 15+ (currency, negatives, scientific notation) |
| ToPgDate | 15+ (ISO, US, 2-digit years, edge cases) |
| ToPgBool | 10+ (true/false/yes/no/1/0) |
| CleanCell | 12+ (Excel formulas, quotes, prefixes) |

### helpers.go
| Function | Cases |
|----------|-------|
| WhereBuilder.Build | 6+ (empty, single, multiple conditions) |
| WhereBuilder.AddSearch | 5+ |
| quoteIdentifier | 5+ (SQL injection edge cases) |

### Key Test Cases

**ToPgNumeric:**
```
"123"          → 123 (valid)
"$1,234.56"    → 1234.56 (valid)
"(123.45)"     → -123.45 (accounting format)
"1.5e10"       → 15000000000 (scientific)
"abc"          → invalid
```

**ToPgDate:**
```
"2024-01-15"   → 2024-01-15 (ISO)
"01/15/2024"   → 2024-01-15 (US)
"01/15/99"     → 1999-01-15 (2-digit year)
"not-a-date"   → invalid
```

## File Ownership

| Source | Test File |
|--------|-----------|
| convert.go | convert_test.go |
| helpers.go | helpers_test.go |
| upload.go | upload_test.go |
| service.go | service_test.go |

## CI Integration

```yaml
- name: Run tests
  run: go test -v -race -coverprofile=coverage.out ./internal/...
```

## Coverage Targets
- Week 2: 40% (pure functions)
- Week 4: 60% (with mocks)
- Ongoing: 80% (integration)
