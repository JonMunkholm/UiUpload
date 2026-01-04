// Package core provides the business logic for CSV import operations.
//
// # Error Codes Reference
//
// This file defines user-friendly error messages with codes for support reference.
// When users encounter errors, they can quote the error code to support staff
// for faster diagnosis.
//
// Error codes are grouped by category:
//
// # Database Errors (DB001-DB099)
//
// Errors related to database operations and constraints:
//
//	DB001 - Duplicate key: A record with this ID already exists
//	        Action: Download failed rows to review duplicates
//	        Patterns: "duplicate key"
//
//	DB002 - Unique constraint: This value must be unique but already exists
//	        Action: Check for duplicate entries in your CSV
//	        Patterns: "unique constraint", "violates unique"
//
//	DB003 - Foreign key: Referenced record does not exist
//	        Action: Ensure parent records are uploaded first
//	        Patterns: "foreign key constraint", "violates foreign key"
//
//	DB004 - Connection refused: Unable to connect to database
//	        Action: Please try again in a few moments
//	        Patterns: "connection refused"
//
//	DB005 - Connection reset: Database connection was interrupted
//	        Action: Please try again
//	        Patterns: "connection reset"
//
//	DB006 - Timeout: Operation timed out
//	        Action: Try uploading a smaller file or try again later
//	        Patterns: "timeout"
//
//	DB007 - Deadlock: Database was busy with conflicting operations
//	        Action: Please try again
//	        Patterns: "deadlock"
//
// # Validation Errors (VAL001-VAL099)
//
// Errors related to data validation and format checking:
//
//	VAL001 - Invalid date: Invalid date format detected
//	         Action: Use YYYY-MM-DD, MM/DD/YYYY, or Jan 15, 2024
//	         Patterns: "invalid date"
//
//	VAL002 - Invalid number: Invalid number format detected
//	         Action: Remove currency symbols and use standard decimal format
//	         Patterns: "invalid number"
//
//	VAL003 - Required field: Required field is empty
//	         Action: Ensure all required columns have values
//	         Patterns: "required field"
//
//	VAL004 - Missing column: Required column is missing from CSV
//	         Action: Check that all required columns are present in your file
//	         Patterns: "missing required column"
//
//	VAL005 - Column not found: Expected column not found in CSV
//	         Action: Verify column headers match the template exactly
//	         Patterns: "column not found"
//
//	VAL006 - Invalid enum: Value is not in the allowed list
//	         Action: Check the allowed values for this field
//	         Patterns: "invalid enum"
//
// # File Errors (FILE001-FILE099)
//
// Errors related to file handling and parsing:
//
//	FILE001 - File too large: File exceeds maximum size limit (100MB)
//	          Action: Split the file into smaller chunks
//	          Patterns: "file too large"
//
//	FILE002 - Invalid CSV: File is not a valid CSV
//	          Action: Ensure file is comma-separated with consistent columns
//	          Patterns: "invalid csv"
//
//	FILE003 - Encoding error: File contains invalid characters
//	          Action: Save file as UTF-8 encoding
//	          Patterns: "encoding error"
//
//	FILE004 - No file: No file was selected
//	          Action: Please select a CSV file to upload
//	          Patterns: "no file provided"
//
//	FILE005 - Empty file: The uploaded file is empty
//	          Action: Please upload a CSV file with data rows
//	          Patterns: "empty file"
//
// # Upload Errors (UPL001-UPL099)
//
// Errors related to the upload process and session management:
//
//	UPL001 - Upload cancelled: Upload was cancelled by user
//	         Action: Start a new upload when ready
//	         Patterns: "upload cancelled"
//
//	UPL002 - System busy: Too many uploads in progress
//	         Action: Please wait a moment and try again
//	         Patterns: "too many uploads"
//
//	UPL003 - Session expired: Upload session not found
//	         Action: The upload may have expired. Please start a new upload
//	         Patterns: "upload not found"
//
//	UPL004 - Request cancelled: Request was cancelled
//	         Action: Please try again
//	         Patterns: "context canceled"
//
//	UPL005 - Request timeout: Request timed out
//	         Action: Try uploading a smaller file or check your connection
//	         Patterns: "context deadline exceeded"
//
// # Table Errors (TBL001-TBL099)
//
// Errors related to table configuration and access:
//
//	TBL001 - Table not found: The specified table does not exist
//	         Action: Verify the table name is correct
//	         Patterns: "table not found"
//
//	TBL002 - Unknown table: Table type is not configured
//	         Action: This table type is not configured
//	         Patterns: "unknown table"
//
// # Rate Limiting (RATE001-RATE099)
//
// Errors related to request throttling:
//
//	RATE001 - Rate limited: Too many requests
//	          Action: Please wait a moment before trying again
//	          Patterns: "rate limit"
//
// # Default Error (ERR000)
//
// Fallback when no specific pattern matches:
//
//	ERR000 - Unknown error: An unexpected error occurred
//	         Action: Please try again or contact support
//
// # Pattern Matching
//
// Error patterns are matched case-insensitively using strings.Contains.
// The first matching pattern wins, so more specific patterns should be
// defined before general ones. Multiple patterns can map to the same code
// (e.g., DB002 matches both "unique constraint" and "violates unique").
//
// # For Support Staff
//
// When a user reports an error code:
//  1. Look up the code in this reference
//  2. Check the associated patterns to understand what triggered it
//  3. Review the suggested action to guide the user
//  4. If ERR000, check application logs for the original technical error
package core

import (
	"fmt"
	"strings"
)

// UserMessage provides user-friendly error information with actionable guidance.
type UserMessage struct {
	Message string // What happened (user-friendly)
	Action  string // What to do about it
	Code    string // Error code for support reference
}

// errorPattern defines a pattern to match and its corresponding user message.
type errorPattern struct {
	pattern string
	msg     UserMessage
}

// errorPatterns maps technical error patterns (case-insensitive) to user messages.
// Patterns are matched using strings.Contains, so partial matches work.
// The first matching pattern wins, so order matters:
//   - More specific patterns should come before general ones
//   - Multiple patterns can map to the same error code
//
// To add a new error pattern:
//  1. Choose the appropriate category and code range
//  2. Add the pattern in the correct position (specific before general)
//  3. Update the package documentation at the top of this file
var errorPatterns = []errorPattern{
	// =========================================================================
	// Database Constraint Errors (DB001-DB003)
	// These errors occur when data violates database constraints.
	// =========================================================================
	{
		pattern: "duplicate key",
		msg: UserMessage{
			Message: "A record with this ID already exists",
			Action:  "Download failed rows to review duplicates",
			Code:    "DB001",
		},
	},
	{
		pattern: "unique constraint",
		msg: UserMessage{
			Message: "This value must be unique but already exists",
			Action:  "Check for duplicate entries in your CSV",
			Code:    "DB002",
		},
	},
	{
		pattern: "violates unique",
		msg: UserMessage{
			Message: "A duplicate value was found",
			Action:  "Review your data for duplicate key values",
			Code:    "DB002",
		},
	},
	{
		pattern: "foreign key constraint",
		msg: UserMessage{
			Message: "Referenced record does not exist",
			Action:  "Ensure parent records are uploaded first",
			Code:    "DB003",
		},
	},
	{
		pattern: "violates foreign key",
		msg: UserMessage{
			Message: "Referenced record does not exist",
			Action:  "Ensure parent records are uploaded first",
			Code:    "DB003",
		},
	},

	// =========================================================================
	// Database Connection Errors (DB004-DB007)
	// These errors occur when database connectivity is disrupted.
	// =========================================================================
	{
		pattern: "connection refused",
		msg: UserMessage{
			Message: "Unable to connect to database",
			Action:  "Please try again in a few moments",
			Code:    "DB004",
		},
	},
	{
		pattern: "connection reset",
		msg: UserMessage{
			Message: "Database connection was interrupted",
			Action:  "Please try again",
			Code:    "DB005",
		},
	},
	{
		pattern: "timeout",
		msg: UserMessage{
			Message: "Operation timed out",
			Action:  "Try uploading a smaller file or try again later",
			Code:    "DB006",
		},
	},
	{
		pattern: "deadlock",
		msg: UserMessage{
			Message: "Database was busy with conflicting operations",
			Action:  "Please try again",
			Code:    "DB007",
		},
	},

	// =========================================================================
	// Validation Errors (VAL001-VAL006)
	// These errors occur when data doesn't match expected formats.
	// =========================================================================
	{
		pattern: "invalid date",
		msg: UserMessage{
			Message: "Invalid date format detected",
			Action:  "Use YYYY-MM-DD, MM/DD/YYYY, or Jan 15, 2024",
			Code:    "VAL001",
		},
	},
	{
		pattern: "invalid number",
		msg: UserMessage{
			Message: "Invalid number format detected",
			Action:  "Remove currency symbols and use standard decimal format",
			Code:    "VAL002",
		},
	},
	{
		pattern: "required field",
		msg: UserMessage{
			Message: "Required field is empty",
			Action:  "Ensure all required columns have values",
			Code:    "VAL003",
		},
	},
	{
		pattern: "missing required column",
		msg: UserMessage{
			Message: "Required column is missing from CSV",
			Action:  "Check that all required columns are present in your file",
			Code:    "VAL004",
		},
	},
	{
		pattern: "column not found",
		msg: UserMessage{
			Message: "Expected column not found in CSV",
			Action:  "Verify column headers match the template exactly",
			Code:    "VAL005",
		},
	},
	{
		pattern: "invalid enum",
		msg: UserMessage{
			Message: "Value is not in the allowed list",
			Action:  "Check the allowed values for this field",
			Code:    "VAL006",
		},
	},

	// =========================================================================
	// File Errors (FILE001-FILE005)
	// These errors occur when processing uploaded files.
	// =========================================================================
	{
		pattern: "file too large",
		msg: UserMessage{
			Message: "File exceeds maximum size limit (100MB)",
			Action:  "Split the file into smaller chunks",
			Code:    "FILE001",
		},
	},
	{
		pattern: "invalid csv",
		msg: UserMessage{
			Message: "File is not a valid CSV",
			Action:  "Ensure file is comma-separated with consistent columns",
			Code:    "FILE002",
		},
	},
	{
		pattern: "encoding error",
		msg: UserMessage{
			Message: "File contains invalid characters",
			Action:  "Save file as UTF-8 encoding",
			Code:    "FILE003",
		},
	},
	{
		pattern: "no file provided",
		msg: UserMessage{
			Message: "No file was selected",
			Action:  "Please select a CSV file to upload",
			Code:    "FILE004",
		},
	},
	{
		pattern: "empty file",
		msg: UserMessage{
			Message: "The uploaded file is empty",
			Action:  "Please upload a CSV file with data rows",
			Code:    "FILE005",
		},
	},

	// =========================================================================
	// Upload Errors (UPL001-UPL005)
	// These errors occur during the upload process and session management.
	// =========================================================================
	{
		pattern: "upload cancelled",
		msg: UserMessage{
			Message: "Upload was cancelled",
			Action:  "Start a new upload when ready",
			Code:    "UPL001",
		},
	},
	{
		pattern: "too many uploads",
		msg: UserMessage{
			Message: "System is busy processing other uploads",
			Action:  "Please wait a moment and try again",
			Code:    "UPL002",
		},
	},
	{
		pattern: "upload not found",
		msg: UserMessage{
			Message: "Upload session not found",
			Action:  "The upload may have expired. Please start a new upload",
			Code:    "UPL003",
		},
	},
	{
		pattern: "context canceled",
		msg: UserMessage{
			Message: "Request was cancelled",
			Action:  "Please try again",
			Code:    "UPL004",
		},
	},
	{
		pattern: "context deadline exceeded",
		msg: UserMessage{
			Message: "Request timed out",
			Action:  "Try uploading a smaller file or check your connection",
			Code:    "UPL005",
		},
	},

	// =========================================================================
	// Table Errors (TBL001-TBL002)
	// These errors occur when working with database tables.
	// =========================================================================
	{
		pattern: "table not found",
		msg: UserMessage{
			Message: "Table not found",
			Action:  "Verify the table name is correct",
			Code:    "TBL001",
		},
	},
	{
		pattern: "unknown table",
		msg: UserMessage{
			Message: "Unknown table type",
			Action:  "This table type is not configured",
			Code:    "TBL002",
		},
	},

	// =========================================================================
	// Rate Limiting (RATE001)
	// These errors occur when request limits are exceeded.
	// =========================================================================
	{
		pattern: "rate limit",
		msg: UserMessage{
			Message: "Too many requests",
			Action:  "Please wait a moment before trying again",
			Code:    "RATE001",
		},
	},
}

// defaultMessage is returned when no pattern matches (ERR000).
// This is the fallback for unexpected errors. Support staff should check
// application logs for the original technical error when users report ERR000.
var defaultMessage = UserMessage{
	Message: "An unexpected error occurred",
	Action:  "Please try again or contact support",
	Code:    "ERR000",
}

// MapError converts a technical error to a user-friendly message.
// It searches through known error patterns (case-insensitive) and returns
// the first match. If no pattern matches, a generic fallback message with
// code ERR000 is returned.
//
// Example:
//
//	err := errors.New("duplicate key violation")
//	msg := MapError(err)
//	// msg.Code == "DB001"
//	// msg.Message == "A record with this ID already exists"
func MapError(err error) UserMessage {
	if err == nil {
		return UserMessage{}
	}

	errStr := strings.ToLower(err.Error())

	for _, ep := range errorPatterns {
		if strings.Contains(errStr, ep.pattern) {
			return ep.msg
		}
	}

	return defaultMessage
}

// FormatUserError creates a formatted error string for display.
// The format is: "Message (Code: XXX). Action"
//
// Example output: "A record with this ID already exists (Code: DB001). Download failed rows to review duplicates"
//
// This is the primary function for displaying errors to end users.
func FormatUserError(err error) string {
	msg := MapError(err)
	if msg.Message == "" {
		return ""
	}
	return fmt.Sprintf("%s (Code: %s). %s", msg.Message, msg.Code, msg.Action)
}

// IsUserFacing checks if an error matches a known pattern and should be shown to users.
// Returns true if the error matches a specific pattern (not the generic ERR000 fallback).
// Use this to decide whether to show the raw error or the mapped user message.
//
// Example:
//
//	if IsUserFacing(err) {
//	    showToUser(FormatUserError(err))
//	} else {
//	    log.Error(err) // Log technical error
//	    showToUser("An error occurred. Please try again.")
//	}
func IsUserFacing(err error) bool {
	if err == nil {
		return false
	}
	msg := MapError(err)
	return msg.Code != defaultMessage.Code
}

// WrapWithUserMessage wraps a technical error with a user-friendly message.
// The original error is preserved for logging while providing a clean message for users.
type UserError struct {
	Technical error       // Original technical error for logging
	User      UserMessage // User-friendly message for display
}

func (e *UserError) Error() string {
	return e.User.Message
}

func (e *UserError) Unwrap() error {
	return e.Technical
}

// NewUserError creates a UserError by mapping a technical error to a user-friendly message.
// The returned UserError preserves the original technical error for logging via Unwrap(),
// while providing a clean user message via Error().
//
// Returns nil if err is nil.
//
// Example:
//
//	ue := NewUserError(dbErr)
//	log.Error(ue.Technical)          // Log original error
//	fmt.Println(ue.Error())           // Show "A record with this ID already exists"
//	fmt.Println(ue.User.Code)         // Show "DB001"
func NewUserError(err error) *UserError {
	if err == nil {
		return nil
	}
	return &UserError{
		Technical: err,
		User:      MapError(err),
	}
}
