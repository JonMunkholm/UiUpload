package core

// upload_limiter.go implements concurrency control for upload processing.
//
// The limiter uses a semaphore pattern to restrict parallel uploads to a
// configurable maximum, preventing resource exhaustion under load. When all
// slots are occupied, new requests wait up to maxWait before failing with
// ErrTooManyUploads.
//
// The limiter also supports graceful shutdown via WaitForDrain, which blocks
// until all active uploads complete.

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrTooManyUploads is returned when all upload slots are occupied and the
// wait timeout expires. Clients should retry after a short delay.
var ErrTooManyUploads = errors.New("too many concurrent uploads, please try again later")

// DefaultMaxConcurrentUploads is the default limit for parallel uploads.
const DefaultMaxConcurrentUploads = 5

// DefaultMaxWaitTime is how long to wait for a slot before rejecting.
const DefaultMaxWaitTime = 30 * time.Second

// UploadLimiter controls concurrent upload processing using a semaphore pattern.
// It prevents resource exhaustion by limiting parallel uploads to a configurable max.
type UploadLimiter struct {
	semaphore chan struct{}
	maxWait   time.Duration

	mu     sync.RWMutex
	active int
}

// NewUploadLimiter creates a limiter that allows at most maxConcurrent simultaneous uploads.
// Requests that cannot acquire a slot within maxWait will receive ErrTooManyUploads.
func NewUploadLimiter(maxConcurrent int, maxWait time.Duration) *UploadLimiter {
	if maxConcurrent <= 0 {
		maxConcurrent = DefaultMaxConcurrentUploads
	}
	if maxWait <= 0 {
		maxWait = DefaultMaxWaitTime
	}

	return &UploadLimiter{
		semaphore: make(chan struct{}, maxConcurrent),
		maxWait:   maxWait,
	}
}

// Acquire attempts to acquire an upload slot.
// Returns nil on success, ErrTooManyUploads if timeout expires.
// The caller MUST call Release() when the upload completes (use defer).
func (l *UploadLimiter) Acquire(ctx context.Context) error {
	// Create timeout context for waiting
	waitCtx, cancel := context.WithTimeout(ctx, l.maxWait)
	defer cancel()

	select {
	case l.semaphore <- struct{}{}:
		// Got a slot
		l.mu.Lock()
		l.active++
		l.mu.Unlock()
		return nil

	case <-waitCtx.Done():
		// Check if original context was cancelled vs timeout
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrTooManyUploads

	case <-ctx.Done():
		return ctx.Err()
	}
}

// TryAcquire attempts to acquire a slot without blocking.
// Returns true if a slot was acquired, false otherwise.
func (l *UploadLimiter) TryAcquire() bool {
	select {
	case l.semaphore <- struct{}{}:
		l.mu.Lock()
		l.active++
		l.mu.Unlock()
		return true
	default:
		return false
	}
}

// Release releases a previously acquired slot.
// Must be called exactly once for each successful Acquire/TryAcquire.
func (l *UploadLimiter) Release() {
	l.mu.Lock()
	l.active--
	l.mu.Unlock()

	// Release the semaphore slot
	<-l.semaphore
}

// ActiveCount returns the number of currently active uploads.
func (l *UploadLimiter) ActiveCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.active
}

// MaxConcurrent returns the maximum allowed concurrent uploads.
func (l *UploadLimiter) MaxConcurrent() int {
	return cap(l.semaphore)
}

// Available returns the number of available slots.
func (l *UploadLimiter) Available() int {
	return cap(l.semaphore) - len(l.semaphore)
}

// WaitForDrain blocks until all active uploads complete or context is cancelled.
// Used for graceful shutdown to ensure uploads finish before termination.
func (l *UploadLimiter) WaitForDrain(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if l.ActiveCount() == 0 {
				return nil
			}
		}
	}
}

// Status returns a snapshot of the limiter's current state.
type UploadLimiterStatus struct {
	Active       int `json:"active"`
	Available    int `json:"available"`
	MaxConcurrent int `json:"max_concurrent"`
}

// Status returns the current limiter state for monitoring/debugging.
func (l *UploadLimiter) Status() UploadLimiterStatus {
	l.mu.RLock()
	active := l.active
	l.mu.RUnlock()

	return UploadLimiterStatus{
		Active:       active,
		Available:    cap(l.semaphore) - len(l.semaphore),
		MaxConcurrent: cap(l.semaphore),
	}
}
