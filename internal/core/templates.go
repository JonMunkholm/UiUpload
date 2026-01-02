package core

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	db "github.com/JonMunkholm/TUI/internal/database"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// CreateTemplate creates a new import template.
func (s *Service) CreateTemplate(ctx context.Context, tableKey, name string, mapping map[string]int, csvHeaders []string) (*ImportTemplate, error) {
	if name == "" {
		return nil, fmt.Errorf("template name is required")
	}

	mappingJSON, err := json.Marshal(mapping)
	if err != nil {
		return nil, fmt.Errorf("marshal mapping: %w", err)
	}

	headersJSON, err := json.Marshal(csvHeaders)
	if err != nil {
		return nil, fmt.Errorf("marshal headers: %w", err)
	}

	queries := db.New(s.pool)
	result, err := queries.CreateImportTemplate(ctx, db.CreateImportTemplateParams{
		TableKey:      tableKey,
		Name:          name,
		ColumnMapping: mappingJSON,
		CsvHeaders:    headersJSON,
	})
	if err != nil {
		if strings.Contains(err.Error(), "import_templates_table_name_unique") {
			return nil, fmt.Errorf("template '%s' already exists for this table", name)
		}
		return nil, fmt.Errorf("create template: %w", err)
	}

	return dbTemplateToTemplate(result)
}

// GetTemplate retrieves a template by ID.
func (s *Service) GetTemplate(ctx context.Context, id string) (*ImportTemplate, error) {
	uid, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("invalid template ID: %w", err)
	}

	queries := db.New(s.pool)
	result, err := queries.GetImportTemplate(ctx, pgtype.UUID{Bytes: uid, Valid: true})
	if err != nil {
		return nil, fmt.Errorf("get template: %w", err)
	}

	return dbTemplateToTemplate(result)
}

// ListTemplates returns all templates for a table.
func (s *Service) ListTemplates(ctx context.Context, tableKey string) ([]ImportTemplate, error) {
	queries := db.New(s.pool)
	results, err := queries.ListImportTemplates(ctx, tableKey)
	if err != nil {
		return nil, fmt.Errorf("list templates: %w", err)
	}

	templates := make([]ImportTemplate, 0, len(results))
	for _, r := range results {
		t, err := dbTemplateToTemplate(r)
		if err != nil {
			continue // Skip invalid templates
		}
		templates = append(templates, *t)
	}

	return templates, nil
}

// UpdateTemplate updates an existing template.
func (s *Service) UpdateTemplate(ctx context.Context, id, name string, mapping map[string]int, csvHeaders []string) (*ImportTemplate, error) {
	if name == "" {
		return nil, fmt.Errorf("template name is required")
	}

	uid, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("invalid template ID: %w", err)
	}

	mappingJSON, err := json.Marshal(mapping)
	if err != nil {
		return nil, fmt.Errorf("marshal mapping: %w", err)
	}

	headersJSON, err := json.Marshal(csvHeaders)
	if err != nil {
		return nil, fmt.Errorf("marshal headers: %w", err)
	}

	queries := db.New(s.pool)
	result, err := queries.UpdateImportTemplate(ctx, db.UpdateImportTemplateParams{
		ID:            pgtype.UUID{Bytes: uid, Valid: true},
		Name:          name,
		ColumnMapping: mappingJSON,
		CsvHeaders:    headersJSON,
	})
	if err != nil {
		return nil, fmt.Errorf("update template: %w", err)
	}

	return dbTemplateToTemplate(result)
}

// DeleteTemplate removes a template.
func (s *Service) DeleteTemplate(ctx context.Context, id string) error {
	uid, err := uuid.Parse(id)
	if err != nil {
		return fmt.Errorf("invalid template ID: %w", err)
	}

	queries := db.New(s.pool)
	return queries.DeleteImportTemplate(ctx, pgtype.UUID{Bytes: uid, Valid: true})
}

// MatchTemplates finds templates that match the given CSV headers.
func (s *Service) MatchTemplates(ctx context.Context, tableKey string, csvHeaders []string) ([]TemplateMatch, error) {
	templates, err := s.ListTemplates(ctx, tableKey)
	if err != nil {
		return nil, err
	}

	var matches []TemplateMatch
	for _, t := range templates {
		score := matchTemplateHeaders(csvHeaders, t.CSVHeaders)
		if score >= TemplateMatchThreshold {
			matches = append(matches, TemplateMatch{
				Template:   t,
				MatchScore: score,
			})
		}
	}

	// Sort by score descending
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].MatchScore > matches[j].MatchScore
	})

	return matches, nil
}

// matchTemplateHeaders calculates how well CSV headers match template headers.
func matchTemplateHeaders(csvHeaders, templateHeaders []string) float64 {
	if len(templateHeaders) == 0 {
		return 0
	}

	csvSet := make(map[string]bool)
	for _, h := range csvHeaders {
		csvSet[strings.ToLower(strings.TrimSpace(h))] = true
	}

	matched := 0
	for _, h := range templateHeaders {
		if csvSet[strings.ToLower(strings.TrimSpace(h))] {
			matched++
		}
	}

	return float64(matched) / float64(len(templateHeaders))
}

// dbTemplateToTemplate converts a database template to our API type.
func dbTemplateToTemplate(t db.ImportTemplate) (*ImportTemplate, error) {
	var mapping map[string]int
	if err := json.Unmarshal(t.ColumnMapping, &mapping); err != nil {
		return nil, fmt.Errorf("unmarshal mapping: %w", err)
	}

	var headers []string
	if err := json.Unmarshal(t.CsvHeaders, &headers); err != nil {
		return nil, fmt.Errorf("unmarshal headers: %w", err)
	}

	id := ""
	if t.ID.Valid {
		id = uuid.UUID(t.ID.Bytes).String()
	}

	createdAt := time.Time{}
	if t.CreatedAt.Valid {
		createdAt = t.CreatedAt.Time
	}

	updatedAt := time.Time{}
	if t.UpdatedAt.Valid {
		updatedAt = t.UpdatedAt.Time
	}

	return &ImportTemplate{
		ID:            id,
		TableKey:      t.TableKey,
		Name:          t.Name,
		ColumnMapping: mapping,
		CSVHeaders:    headers,
		CreatedAt:     createdAt,
		UpdatedAt:     updatedAt,
	}, nil
}
