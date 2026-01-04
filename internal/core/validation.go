package core

// validation.go provides row-level validation for CSV data before insertion.
//
// Validation happens at two levels:
//  1. Header validation: Ensures required columns are present
//  2. Row validation: Checks each cell against its FieldSpec (type, format, enum values)
//
// The RowValidator can return all errors (for preview UI) or just the first error
// (for efficient batch processing). Validation errors include the field name,
// invalid value, and a human-readable message.

import (
	"fmt"
	"strings"
)

// ValidationError represents a single validation error for a field.
type ValidationError struct {
	Field   string // Field/column name
	Value   string // The invalid value
	Message string // Human-readable error message
}

func (e ValidationError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("%s: %s", e.Field, e.Message)
	}
	return e.Message
}

// ValidationResult contains the result of validating a row.
type ValidationResult struct {
	Valid  bool              // True if all validations passed
	Errors []ValidationError // List of validation errors (empty if Valid)
}

// RowValidator validates rows against a table's field specifications.
type RowValidator struct {
	specs     []FieldSpec
	headerIdx HeaderIndex
}

// NewRowValidator creates a validator for the given table definition and header index.
func NewRowValidator(specs []FieldSpec, headerIdx HeaderIndex) *RowValidator {
	return &RowValidator{
		specs:     specs,
		headerIdx: headerIdx,
	}
}

// ValidateRow validates a single CSV row and returns all validation errors.
// This is useful for preview/validation UI that shows all problems at once.
func (v *RowValidator) ValidateRow(row []string) ValidationResult {
	result := ValidationResult{Valid: true}

	for _, spec := range v.specs {
		pos, ok := v.headerIdx[strings.ToLower(spec.Name)]
		if !ok || pos >= len(row) {
			if spec.Required {
				result.Valid = false
				result.Errors = append(result.Errors, ValidationError{
					Field:   spec.Name,
					Message: "missing required column",
				})
			}
			continue
		}

		raw := CleanCell(row[pos])

		// Check required fields
		if raw == "" && spec.Required && !spec.AllowEmpty {
			result.Valid = false
			result.Errors = append(result.Errors, ValidationError{
				Field:   spec.Name,
				Message: "required field is empty",
			})
			continue
		}

		// Apply normalizer if present
		if spec.Normalizer != nil && raw != "" {
			raw = spec.Normalizer(raw)
		}

		// Type validation
		if raw != "" {
			if err := ValidateCell(raw, spec); err != nil {
				result.Valid = false
				result.Errors = append(result.Errors, ValidationError{
					Field:   spec.Name,
					Value:   raw,
					Message: err.Error(),
				})
			}
		}
	}

	return result
}

// ValidateRowFirst validates a row and returns the first error only.
// This is more efficient for batch processing where you just need pass/fail.
func (v *RowValidator) ValidateRowFirst(row []string) error {
	for _, spec := range v.specs {
		pos, ok := v.headerIdx[strings.ToLower(spec.Name)]
		if !ok || pos >= len(row) {
			if spec.Required {
				return fmt.Errorf("missing required column %q", spec.Name)
			}
			continue
		}

		raw := CleanCell(row[pos])

		if raw == "" && spec.Required && !spec.AllowEmpty {
			return fmt.Errorf("empty required field %q", spec.Name)
		}

		// Apply normalizer if present
		if spec.Normalizer != nil && raw != "" {
			raw = spec.Normalizer(raw)
		}

		// Type validation
		if raw != "" && spec.Required {
			if err := ValidateCell(raw, spec); err != nil {
				return fmt.Errorf("invalid %s for %q: %q", fieldTypeName(spec.Type), spec.Name, raw)
			}
		}
	}
	return nil
}

// ValidateCell validates a single cell value against a field specification.
// Returns nil if valid, or an error describing the problem.
func ValidateCell(value string, spec FieldSpec) error {
	if value == "" {
		return nil // Empty values are allowed (will be NULL)
	}

	switch spec.Type {
	case FieldNumeric:
		if !ToPgNumeric(value).Valid {
			return fmt.Errorf("invalid number format")
		}
	case FieldDate:
		if !ToPgDate(value).Valid {
			return fmt.Errorf("invalid date format (use YYYY-MM-DD or similar)")
		}
	case FieldBool:
		if !ToPgBool(value).Valid {
			return fmt.Errorf("must be yes/no, true/false, or 1/0")
		}
	case FieldEnum:
		if len(spec.EnumValues) > 0 {
			for _, ev := range spec.EnumValues {
				if strings.EqualFold(ev, value) {
					return nil
				}
			}
			return fmt.Errorf("value must be one of: %s", strings.Join(spec.EnumValues, ", "))
		}
	}
	return nil
}

// ValidateHeaders validates that all required columns exist in the CSV headers.
// Returns a mapping from column name to index, or an error listing missing columns.
func ValidateHeaders(headers []string, specs []FieldSpec) (HeaderIndex, error) {
	idx := MakeHeaderIndex(headers)
	var missing []string

	for _, spec := range specs {
		if spec.Required {
			key := strings.ToLower(spec.Name)
			if _, ok := idx[key]; !ok {
				missing = append(missing, spec.Name)
			}
		}
	}

	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required columns: %s", strings.Join(missing, ", "))
	}

	return idx, nil
}

// fieldTypeName returns a human-readable name for a field type.
func fieldTypeName(ft FieldType) string {
	switch ft {
	case FieldText:
		return "text"
	case FieldEnum:
		return "enum"
	case FieldDate:
		return "date"
	case FieldNumeric:
		return "numeric"
	case FieldBool:
		return "bool"
	default:
		return "value"
	}
}
