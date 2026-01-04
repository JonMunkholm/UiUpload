package web

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"time"

	"github.com/JonMunkholm/TUI/internal/core"
	"github.com/JonMunkholm/TUI/internal/web/templates"
	"github.com/go-chi/chi/v5"
)

// handleAuditLog renders the audit log page with filtering and pagination.
func (s *Server) handleAuditLog(w http.ResponseWriter, r *http.Request) {
	page := parseIntParam(r, "page", 1)
	pageSize := 50

	filter := templates.AuditFilter{
		Action:    r.URL.Query().Get("action"),
		TableKey:  r.URL.Query().Get("table"),
		Severity:  r.URL.Query().Get("severity"),
		StartDate: r.URL.Query().Get("from"),
		EndDate:   r.URL.Query().Get("to"),
	}

	coreFilter := core.AuditLogFilter{
		TableKey: filter.TableKey,
		Action:   core.AuditAction(filter.Action),
		Severity: filter.Severity,
		Limit:    pageSize,
		Offset:   (page - 1) * pageSize,
	}

	if filter.StartDate != "" {
		if t, err := time.Parse("2006-01-02", filter.StartDate); err == nil {
			coreFilter.StartTime = t
		}
	}
	if filter.EndDate != "" {
		if t, err := time.Parse("2006-01-02", filter.EndDate); err == nil {
			coreFilter.EndTime = t.Add(24*time.Hour - time.Second)
		}
	}

	entries, err := s.service.GetAuditLog(r.Context(), coreFilter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	totalCount, err := s.service.CountAuditLog(r.Context(), coreFilter)
	if err != nil {
		totalCount = int64(len(entries))
	}

	tables := make([]string, 0)
	for _, def := range core.All() {
		tables = append(tables, def.Info.Key)
	}

	params := templates.AuditLogViewParams{
		Entries:    entries,
		TotalCount: totalCount,
		Page:       page,
		PageSize:   pageSize,
		TotalPages: int((totalCount + int64(pageSize) - 1) / int64(pageSize)),
		Filter:     filter,
		Tables:     tables,
	}

	if r.Header.Get("HX-Request") == "true" {
		templates.AuditLogPartial(params).Render(r.Context(), w)
	} else {
		sidebar := templates.SidebarParams{ActivePage: "audit"}
		templates.AuditLogPage(sidebar, params).Render(r.Context(), w)
	}
}

// handleAuditLogEntry returns the detail view for a single audit entry.
func (s *Server) handleAuditLogEntry(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing audit log id")
		return
	}

	entry, err := s.service.GetAuditLogByID(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "audit entry not found")
		return
	}

	templates.AuditEntryDetail(*entry).Render(r.Context(), w)
}

// handleAuditLogExport exports audit log entries as a streaming CSV file.
// Uses chunked transfer encoding to avoid loading all entries into memory.
func (s *Server) handleAuditLogExport(w http.ResponseWriter, r *http.Request) {
	filter := core.AuditLogFilter{
		TableKey: r.URL.Query().Get("table"),
		Action:   core.AuditAction(r.URL.Query().Get("action")),
		Severity: r.URL.Query().Get("severity"),
	}

	if from := r.URL.Query().Get("from"); from != "" {
		if t, err := time.Parse("2006-01-02", from); err == nil {
			filter.StartTime = t
		}
	}
	if to := r.URL.Query().Get("to"); to != "" {
		if t, err := time.Parse("2006-01-02", to); err == nil {
			filter.EndTime = t.Add(24*time.Hour - time.Second)
		}
	}

	// Set headers for streaming download
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("audit_log_%s.csv", timestamp)
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	w.Header().Set("X-Content-Type-Options", "nosniff")

	// Create CSV writer that writes directly to response
	csvWriter := csv.NewWriter(w)

	// Write header row first
	if err := csvWriter.Write([]string{
		"ID", "Timestamp", "Action", "Severity", "Table",
		"User Email", "User Name", "IP Address",
		"Row Key", "Column", "Old Value", "New Value",
		"Rows Affected", "Upload ID", "Reason",
	}); err != nil {
		return
	}

	// Batch flushing for performance: flush every N rows
	const flushInterval = 1000
	rowCount := 0

	// Stream entries directly from database to response
	err := s.service.StreamAuditLog(r.Context(), filter, func(e core.AuditEntry) error {
		if err := csvWriter.Write([]string{
			e.ID,
			e.CreatedAt.Format("2006-01-02 15:04:05"),
			string(e.Action),
			string(e.Severity),
			e.TableKey,
			e.UserEmail,
			e.UserName,
			e.IPAddress,
			e.RowKey,
			e.ColumnName,
			e.OldValue,
			e.NewValue,
			fmt.Sprintf("%d", e.RowsAffected),
			e.UploadID,
			e.Reason,
		}); err != nil {
			return err
		}

		rowCount++
		if rowCount%flushInterval == 0 {
			csvWriter.Flush()
			if err := csvWriter.Error(); err != nil {
				return err
			}
			// Flush HTTP response for chunked transfer
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}

		return nil
	})

	// Final flush
	csvWriter.Flush()

	// Log streaming errors (can't send to client after headers are written)
	if err != nil && err != r.Context().Err() {
		_ = err
	}
}
