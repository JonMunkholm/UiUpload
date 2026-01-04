package core

// streaming.go provides memory-efficient streaming readers for CSV processing.
//
// These readers wrap io.Reader to handle common CSV issues without loading
// the entire file into memory:
//
//   - StreamingUTF8Sanitizer: Replaces invalid UTF-8 sequences with '?'
//   - BOMSkippingReader: Removes UTF-8 BOM (0xEF 0xBB 0xBF) from Windows files
//   - StreamingCountingReader: Tracks bytes read for progress reporting
//
// Use WrapForStreaming to apply all transforms in the correct order.

import (
	"io"
	"unicode/utf8"
)

// StreamingUTF8Sanitizer wraps an io.Reader and replaces invalid UTF-8 sequences
// with the Unicode replacement character (U+FFFD) on the fly.
//
// This enables O(buffer_size) constant memory usage instead of loading the
// entire file for sanitization.
type StreamingUTF8Sanitizer struct {
	reader io.Reader

	// Leftover bytes from previous read that may form a multi-byte sequence
	pending []byte
}

// NewStreamingUTF8Sanitizer creates a new streaming UTF-8 sanitizer.
func NewStreamingUTF8Sanitizer(r io.Reader) *StreamingUTF8Sanitizer {
	return &StreamingUTF8Sanitizer{
		reader:  r,
		pending: make([]byte, 0, utf8.UTFMax),
	}
}

// Read implements io.Reader. It reads from the underlying reader and sanitizes
// invalid UTF-8 sequences in place.
func (s *StreamingUTF8Sanitizer) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	// If we have pending bytes from a previous incomplete sequence, prepend them
	offset := 0
	if len(s.pending) > 0 {
		offset = copy(p, s.pending)
		s.pending = s.pending[:0]
	}

	// Read from underlying reader
	n, err := s.reader.Read(p[offset:])
	n += offset

	if n == 0 {
		return 0, err
	}

	// Quick check: if all bytes are ASCII, no sanitization needed
	if isAllASCII(p[:n]) {
		return n, err
	}

	// Sanitize in place, handling incomplete sequences at the end
	sanitized := s.sanitizeUTF8(p[:n], err == io.EOF)
	return sanitized, err
}

// isAllASCII returns true if all bytes are ASCII (< 128).
// This is a fast path optimization since most CSV data is ASCII.
func isAllASCII(data []byte) bool {
	for _, b := range data {
		if b >= 0x80 {
			return false
		}
	}
	return true
}

// sanitizeUTF8 sanitizes the data in place, replacing invalid UTF-8 sequences
// with the replacement character. Returns the number of valid bytes.
//
// If atEOF is false, incomplete sequences at the end are saved to pending
// for the next read call.
func (s *StreamingUTF8Sanitizer) sanitizeUTF8(data []byte, atEOF bool) int {
	if utf8.Valid(data) {
		// Handle potential incomplete sequence at end
		if !atEOF {
			trailing := incompleteTrailingBytes(data)
			if trailing > 0 {
				s.pending = append(s.pending, data[len(data)-trailing:]...)
				return len(data) - trailing
			}
		}
		return len(data)
	}

	// Need to sanitize - process byte by byte
	write := 0
	for read := 0; read < len(data); {
		r, size := utf8.DecodeRune(data[read:])

		// Check for incomplete sequence at end (not at EOF)
		if !atEOF && read+size >= len(data) && isIncompleteRune(data[read:]) {
			s.pending = append(s.pending, data[read:]...)
			return write
		}

		if r == utf8.RuneError && size == 1 {
			// Invalid byte - replace with replacement character
			// Note: This can expand the data, but replacement char is 3 bytes
			// For simplicity in streaming, we replace with '?' (1 byte) to avoid expansion
			data[write] = '?'
			write++
			read++
		} else {
			// Valid rune - copy as-is
			copy(data[write:], data[read:read+size])
			write += size
			read += size
		}
	}

	return write
}

// incompleteTrailingBytes returns the number of bytes at the end of data
// that could be the start of an incomplete multi-byte UTF-8 sequence.
func incompleteTrailingBytes(data []byte) int {
	if len(data) == 0 {
		return 0
	}

	// Check last 1-3 bytes for incomplete sequences
	for i := 1; i <= 3 && i <= len(data); i++ {
		b := data[len(data)-i]
		// Check if this byte starts a multi-byte sequence
		if b >= 0xC0 {
			// This byte starts a sequence - check if complete
			expectedLen := runeLen(b)
			if i < expectedLen {
				return i
			}
			return 0
		}
		// Continuation byte (10xxxxxx) - keep checking
		if b&0xC0 != 0x80 {
			return 0
		}
	}
	return 0
}

// runeLen returns the expected length of a UTF-8 sequence starting with byte b.
func runeLen(b byte) int {
	if b < 0x80 {
		return 1
	}
	if b < 0xC0 {
		return 0 // continuation byte
	}
	if b < 0xE0 {
		return 2
	}
	if b < 0xF0 {
		return 3
	}
	return 4
}

// isIncompleteRune returns true if the data could be an incomplete multi-byte sequence.
func isIncompleteRune(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	expectedLen := runeLen(data[0])
	return expectedLen > len(data)
}

// BOMSkippingReader wraps an io.Reader and skips the UTF-8 BOM if present.
// The UTF-8 BOM is 0xEF 0xBB 0xBF and is commonly added by Windows programs.
type BOMSkippingReader struct {
	reader     io.Reader
	bomChecked bool
	buf        [3]byte   // Buffer for BOM detection
	bufData    []byte    // Remaining data after BOM check
	bufOffset  int       // Current read position in bufData
}

// NewBOMSkippingReader creates a new BOM-skipping reader.
func NewBOMSkippingReader(r io.Reader) *BOMSkippingReader {
	return &BOMSkippingReader{
		reader: r,
	}
}

// Read implements io.Reader. On the first read, it checks for and skips the BOM.
func (r *BOMSkippingReader) Read(p []byte) (int, error) {
	if !r.bomChecked {
		r.bomChecked = true

		// Read first 3 bytes to check for BOM
		n, err := io.ReadFull(r.reader, r.buf[:])
		if n == 0 {
			return 0, err
		}

		// Check for BOM
		if n >= 3 && r.buf[0] == 0xEF && r.buf[1] == 0xBB && r.buf[2] == 0xBF {
			// BOM found - skip it
			r.bufData = nil
		} else {
			// No BOM - preserve the bytes we read
			r.bufData = r.buf[:n]
			r.bufOffset = 0
		}

		// If we hit EOF during BOM check, handle it
		if err == io.ErrUnexpectedEOF {
			err = io.EOF
		}
		if err != nil && err != io.EOF {
			return 0, err
		}

		// If we have buffered data, return it first
		if len(r.bufData) > 0 {
			copied := copy(p, r.bufData[r.bufOffset:])
			r.bufOffset += copied
			if r.bufOffset >= len(r.bufData) {
				r.bufData = nil
			}
			if copied < len(p) && err != io.EOF {
				// Read more from underlying reader
				n, err2 := r.reader.Read(p[copied:])
				return copied + n, err2
			}
			return copied, err
		}
	}

	// Return any remaining buffered data first
	if len(r.bufData) > r.bufOffset {
		copied := copy(p, r.bufData[r.bufOffset:])
		r.bufOffset += copied
		if r.bufOffset >= len(r.bufData) {
			r.bufData = nil
		}
		return copied, nil
	}

	// Normal read from underlying reader
	return r.reader.Read(p)
}

// StreamingCountingReader wraps an io.Reader to track bytes read.
// Used for progress reporting during streaming uploads.
type StreamingCountingReader struct {
	reader    io.Reader
	BytesRead int64
	Total     int64  // If known (0 if unknown)
}

// NewStreamingCountingReader creates a counting reader with optional total size.
func NewStreamingCountingReader(r io.Reader, total int64) *StreamingCountingReader {
	return &StreamingCountingReader{
		reader: r,
		Total:  total,
	}
}

// Read implements io.Reader.
func (r *StreamingCountingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.BytesRead += int64(n)
	return n, err
}

// Progress returns the read progress as a percentage (0-100).
// Returns 0 if total is unknown.
func (r *StreamingCountingReader) Progress() int {
	if r.Total <= 0 {
		return 0
	}
	return int(r.BytesRead * 100 / r.Total)
}

// WrapForStreaming wraps a reader with BOM skipping, UTF-8 sanitization,
// and byte counting for progress tracking.
//
// The order matters:
// 1. BOM must be stripped first (before any processing)
// 2. UTF-8 sanitization happens next
// 3. Counting wraps everything for progress
func WrapForStreaming(r io.Reader, totalSize int64) *StreamingCountingReader {
	bomReader := NewBOMSkippingReader(r)
	sanitizedReader := NewStreamingUTF8Sanitizer(bomReader)
	return NewStreamingCountingReader(sanitizedReader, totalSize)
}
