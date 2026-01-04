# Security Implementation Plan

**Status:** Partially Implemented - Auth migrations created, handlers pending
**Agents:** a4a9f6a (Plan)

## Implementation Notes

**Completed:**
- Database migrations created: `020_auth_users.sql`, `021_auth_sessions.sql`
- Users table with role-based access (viewer, editor, admin)
- Sessions table with TTL support

**Remaining Work:**
- `internal/auth/` package not yet created
- Auth handlers not wired up to routes
- CSRF middleware not implemented
- RBAC middleware not implemented
- Rate limiter IP spoofing fix not done (trusted proxy validation)
- Per-user vs per-IP rate limiting not implemented

## Executive Summary

Implement comprehensive security for an application with **zero authentication**. Currently anyone can delete all data.

## Components

### 1. Authentication (Session-Based)
- New `internal/auth/` package
- Database: `users` and `sessions` tables (migrations 020, 021)
- Bcrypt password hashing (cost 12)
- Cookie-based sessions (HttpOnly, Secure, SameSite=Lax)
- 24h default TTL, 30d with "Remember Me"

### 2. Authorization (RBAC)
| Role | Permissions |
|------|-------------|
| viewer | View, export data |
| editor | + Upload, edit, delete data |
| admin | + Reset tables, rollback, manage users |

### 3. CSRF Protection
- Per-session tokens
- X-CSRF-Token header for HTMX
- Validate on all POST/PUT/DELETE

### 4. Rate Limiter Hardening
- Fix IP spoofing via X-Real-IP
- Trusted proxy CIDR validation
- Per-user limits (200/min) vs per-IP (100/min)

## File Ownership Matrix

| File | Status |
|------|--------|
| `internal/auth/session.go` | Create |
| `internal/auth/password.go` | Create |
| `internal/auth/csrf.go` | Create |
| `internal/web/middleware/auth.go` | Create |
| `internal/web/middleware/rbac.go` | Create |
| `internal/web/middleware/csrf.go` | Create |
| `internal/web/middleware/realip.go` | Create |
| `sql/schema/020_auth_users.sql` | Create |
| `sql/schema/021_auth_sessions.sql` | Create |

## Estimated Effort: ~10 days

## Dependencies
- None (foundation work)

## Outputs for Other Phases
- User context for audit logs
- Role-based UI customization
- Protected API for integrations
