package core

import (
	"errors"
	"testing"
)

func TestMapError(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		wantCode    string
		wantMessage string
	}{
		{
			name:        "nil error returns empty",
			err:         nil,
			wantCode:    "",
			wantMessage: "",
		},
		{
			name:        "duplicate key maps correctly",
			err:         errors.New("pq: duplicate key value violates unique constraint"),
			wantCode:    "DB001",
			wantMessage: "A record with this ID already exists",
		},
		{
			name:        "unique constraint maps correctly",
			err:         errors.New("ERROR: unique constraint violated"),
			wantCode:    "DB002",
			wantMessage: "This value must be unique but already exists",
		},
		{
			name:        "foreign key maps correctly",
			err:         errors.New("violates foreign key constraint"),
			wantCode:    "DB003",
			wantMessage: "Referenced record does not exist",
		},
		{
			name:        "connection refused maps correctly",
			err:         errors.New("dial tcp: connection refused"),
			wantCode:    "DB004",
			wantMessage: "Unable to connect to database",
		},
		{
			name:        "timeout maps correctly",
			err:         errors.New("context deadline exceeded (timeout)"),
			wantCode:    "DB006",
			wantMessage: "Operation timed out",
		},
		{
			name:        "file too large maps correctly",
			err:         errors.New("file too large: 200MB exceeds limit"),
			wantCode:    "FILE001",
			wantMessage: "File exceeds maximum size limit (100MB)",
		},
		{
			name:        "rate limit maps correctly",
			err:         errors.New("rate limit exceeded"),
			wantCode:    "RATE001",
			wantMessage: "Too many requests",
		},
		{
			name:        "unknown error returns default",
			err:         errors.New("some random internal error"),
			wantCode:    "ERR000",
			wantMessage: "An unexpected error occurred",
		},
		{
			name:        "case insensitive matching",
			err:         errors.New("DUPLICATE KEY value violates"),
			wantCode:    "DB001",
			wantMessage: "A record with this ID already exists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MapError(tt.err)
			if got.Code != tt.wantCode {
				t.Errorf("MapError() code = %q, want %q", got.Code, tt.wantCode)
			}
			if got.Message != tt.wantMessage {
				t.Errorf("MapError() message = %q, want %q", got.Message, tt.wantMessage)
			}
		})
	}
}

func TestFormatUserError(t *testing.T) {
	err := errors.New("duplicate key value violates")
	result := FormatUserError(err)

	expected := "A record with this ID already exists (Code: DB001). Download failed rows to review duplicates"
	if result != expected {
		t.Errorf("FormatUserError() = %q, want %q", result, expected)
	}
}

func TestIsUserFacing(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error is not user facing",
			err:  nil,
			want: false,
		},
		{
			name: "known error is user facing",
			err:  errors.New("duplicate key"),
			want: true,
		},
		{
			name: "unknown error is not user facing",
			err:  errors.New("random internal error xyz"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsUserFacing(tt.err)
			if got != tt.want {
				t.Errorf("IsUserFacing() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewUserError(t *testing.T) {
	t.Run("nil error returns nil", func(t *testing.T) {
		if got := NewUserError(nil); got != nil {
			t.Errorf("NewUserError(nil) = %v, want nil", got)
		}
	})

	t.Run("wraps technical error with user message", func(t *testing.T) {
		techErr := errors.New("pq: duplicate key value")
		userErr := NewUserError(techErr)

		if userErr.Error() != "A record with this ID already exists" {
			t.Errorf("Error() = %q, want user message", userErr.Error())
		}

		if !errors.Is(userErr, techErr) {
			t.Error("Unwrap() should return original error")
		}
	})
}
