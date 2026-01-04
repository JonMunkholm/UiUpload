package core

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestBOMSkippingReader(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "file with BOM",
			input:    append([]byte{0xEF, 0xBB, 0xBF}, []byte("hello,world")...),
			expected: "hello,world",
		},
		{
			name:     "file without BOM",
			input:    []byte("hello,world"),
			expected: "hello,world",
		},
		{
			name:     "empty file",
			input:    []byte{},
			expected: "",
		},
		{
			name:     "only BOM",
			input:    []byte{0xEF, 0xBB, 0xBF},
			expected: "",
		},
		{
			name:     "partial BOM at start",
			input:    []byte{0xEF, 0xBB, 'a', 'b', 'c'},
			expected: string([]byte{0xEF, 0xBB, 'a', 'b', 'c'}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewBOMSkippingReader(bytes.NewReader(tt.input))
			result, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if string(result) != tt.expected {
				t.Errorf("got %q, want %q", string(result), tt.expected)
			}
		})
	}
}

func TestStreamingUTF8Sanitizer(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "valid ASCII",
			input:    []byte("hello,world"),
			expected: "hello,world",
		},
		{
			name:     "valid UTF-8 with multibyte",
			input:    []byte("hello,welt"),
			expected: "hello,welt",
		},
		{
			name:     "invalid single byte replaced",
			input:    []byte{'h', 'e', 0x80, 'l', 'o'},
			expected: "he?lo", // Invalid byte replaced with ?
		},
		{
			name:     "empty input",
			input:    []byte{},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewStreamingUTF8Sanitizer(bytes.NewReader(tt.input))
			result, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if string(result) != tt.expected {
				t.Errorf("got %q, want %q", string(result), tt.expected)
			}
		})
	}
}

func TestStreamingCountingReader(t *testing.T) {
	input := strings.Repeat("x", 1000)
	reader := NewStreamingCountingReader(strings.NewReader(input), int64(len(input)))

	// Read in chunks
	buf := make([]byte, 100)
	totalRead := 0
	for {
		n, err := reader.Read(buf)
		totalRead += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if totalRead != len(input) {
		t.Errorf("total read = %d, want %d", totalRead, len(input))
	}

	if reader.BytesRead != int64(len(input)) {
		t.Errorf("BytesRead = %d, want %d", reader.BytesRead, len(input))
	}

	if reader.Progress() != 100 {
		t.Errorf("Progress = %d, want 100", reader.Progress())
	}
}

func TestWrapForStreaming(t *testing.T) {
	// Create a file with BOM and some invalid UTF-8
	input := append([]byte{0xEF, 0xBB, 0xBF}, []byte{'h', 'e', 0x80, 'l', 'o'}...)

	reader := WrapForStreaming(bytes.NewReader(input), int64(len(input)))
	result, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// BOM should be stripped, invalid byte replaced
	expected := "he?lo"
	if string(result) != expected {
		t.Errorf("got %q, want %q", string(result), expected)
	}

	// Verify bytes were tracked
	if reader.BytesRead == 0 {
		t.Error("BytesRead should be > 0")
	}
}
