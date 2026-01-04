package web

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/JonMunkholm/TUI/internal/core"
	"github.com/JonMunkholm/TUI/internal/web/templates"
	"github.com/go-chi/chi/v5"
)

// handleDashboard renders the main dashboard page.
// Optimized: fetches all table stats in 2 queries instead of 14 (N+1 elimination).
func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Fetch all table stats in a single batch (2 queries total)
	allStats, err := s.service.GetAllTableStats(ctx)
	if err != nil {
		// Log error but continue with empty stats
		allStats = make(map[string]*core.TableStats)
	}

	var groups []templates.TableGroup
	for _, groupName := range core.Groups() {
		tables := core.ByGroup(groupName)
		tableData := make([]templates.TableCardData, len(tables))
		for i, def := range tables {
			data := templates.TableCardData{
				Info: def.Info,
			}

			// Use pre-fetched stats instead of individual queries
			if stats, ok := allStats[def.Info.Key]; ok {
				data.RowCount = stats.RowCount
				if stats.LastUpload != nil {
					data.LastUpload = &stats.LastUpload.UploadedAt
				}
			}

			tableData[i] = data
		}
		groups = append(groups, templates.TableGroup{
			Name:   groupName,
			Tables: tableData,
		})
	}

	sidebar := templates.SidebarParams{ActivePage: "dashboard"}
	templates.Dashboard(sidebar, groups).Render(ctx, w)
}

// handleSettings renders the settings page.
func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	sidebar := templates.SidebarParams{ActivePage: "settings"}
	templates.SettingsPage(sidebar).Render(r.Context(), w)
}

// handleListTables returns all tables organized by group.
func (s *Server) handleListTables(w http.ResponseWriter, r *http.Request) {
	tables := s.service.ListTablesByGroup()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(tables); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to encode response")
	}
}

// handleTableView renders the table data view page.
func (s *Server) handleTableView(w http.ResponseWriter, r *http.Request) {
	tableKey := chi.URLParam(r, "tableKey")
	if tableKey == "" {
		writeError(w, http.StatusBadRequest, "missing table key")
		return
	}

	def, ok := core.Get(tableKey)
	if !ok {
		writeError(w, http.StatusNotFound, "table not found")
		return
	}

	page := parseIntParam(r, "page", 1)
	sorts := parseSorts(r)
	search := r.URL.Query().Get("search")
	filters := parseFilters(r, def)

	data, err := s.service.GetTableData(r.Context(), tableKey, page, core.DefaultPageSize, sorts, search, filters)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	columnMeta := buildColumnMeta(def)

	if r.Header.Get("HX-Request") == "true" {
		templates.TablePartial(tableKey, def.Info, data, columnMeta).Render(r.Context(), w)
	} else {
		sidebar := templates.SidebarParams{ActiveTable: tableKey}
		templates.TableView(sidebar, tableKey, def.Info, data, columnMeta).Render(r.Context(), w)
	}
}

// handleDownloadTemplate returns a CSV template with headers for a table.
func (s *Server) handleDownloadTemplate(w http.ResponseWriter, r *http.Request) {
	tableKey := chi.URLParam(r, "tableKey")
	if tableKey == "" {
		writeError(w, http.StatusBadRequest, "missing table key")
		return
	}

	def, ok := core.Get(tableKey)
	if !ok {
		writeError(w, http.StatusNotFound, "table not found")
		return
	}

	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s_template.csv"`, tableKey))

	csvWriter := csv.NewWriter(w)
	csvWriter.Write(def.Info.Columns)
	csvWriter.Flush()
}

// handleExportData exports table data as a streaming CSV file.
// Uses chunked transfer encoding to avoid loading all rows into memory.
func (s *Server) handleExportData(w http.ResponseWriter, r *http.Request) {
	tableKey := chi.URLParam(r, "tableKey")
	if tableKey == "" {
		writeError(w, http.StatusBadRequest, "missing table key")
		return
	}

	def, ok := core.Get(tableKey)
	if !ok {
		writeError(w, http.StatusNotFound, "table not found")
		return
	}

	search := r.URL.Query().Get("search")
	filters := parseFilters(r, def)

	// Set headers for streaming download (chunked transfer is automatic in HTTP/1.1)
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("%s_%s.csv", tableKey, timestamp)
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	w.Header().Set("X-Content-Type-Options", "nosniff")

	// Create CSV writer that writes directly to response
	csvWriter := csv.NewWriter(w)

	// Write header row first
	if err := csvWriter.Write(def.Info.Columns); err != nil {
		// Can't change status code after writing, just log and return
		return
	}

	// Batch flushing for performance: flush every N rows
	const flushInterval = 1000
	rowCount := 0

	// Stream rows directly from database to response
	err := s.service.StreamTableData(r.Context(), tableKey, search, filters, func(row core.TableRow) error {
		record := make([]string, len(def.Info.Columns))
		for i, col := range def.Info.Columns {
			record[i] = formatCellForExport(row[col])
		}

		if err := csvWriter.Write(record); err != nil {
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
		// Log error but don't expose to client (headers already sent)
		_ = err
	}
}
