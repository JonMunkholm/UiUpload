package core

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestUploadLimiter_AcquireRelease(t *testing.T) {
	limiter := NewUploadLimiter(2, time.Second)

	// Initial state
	if got := limiter.ActiveCount(); got != 0 {
		t.Errorf("initial ActiveCount = %d, want 0", got)
	}
	if got := limiter.Available(); got != 2 {
		t.Errorf("initial Available = %d, want 2", got)
	}

	// Acquire first slot
	ctx := context.Background()
	if err := limiter.Acquire(ctx); err != nil {
		t.Fatalf("first Acquire failed: %v", err)
	}

	if got := limiter.ActiveCount(); got != 1 {
		t.Errorf("after first Acquire, ActiveCount = %d, want 1", got)
	}

	// Acquire second slot
	if err := limiter.Acquire(ctx); err != nil {
		t.Fatalf("second Acquire failed: %v", err)
	}

	if got := limiter.ActiveCount(); got != 2 {
		t.Errorf("after second Acquire, ActiveCount = %d, want 2", got)
	}
	if got := limiter.Available(); got != 0 {
		t.Errorf("after second Acquire, Available = %d, want 0", got)
	}

	// Release one
	limiter.Release()

	if got := limiter.ActiveCount(); got != 1 {
		t.Errorf("after Release, ActiveCount = %d, want 1", got)
	}
	if got := limiter.Available(); got != 1 {
		t.Errorf("after Release, Available = %d, want 1", got)
	}

	// Release the other
	limiter.Release()

	if got := limiter.ActiveCount(); got != 0 {
		t.Errorf("after second Release, ActiveCount = %d, want 0", got)
	}
}

func TestUploadLimiter_BlocksWhenFull(t *testing.T) {
	limiter := NewUploadLimiter(1, 100*time.Millisecond)

	ctx := context.Background()

	// Acquire the only slot
	if err := limiter.Acquire(ctx); err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	// Try to acquire again - should timeout
	start := time.Now()
	err := limiter.Acquire(ctx)
	elapsed := time.Since(start)

	if err != ErrTooManyUploads {
		t.Errorf("expected ErrTooManyUploads, got %v", err)
	}

	// Should have waited approximately the timeout duration
	if elapsed < 90*time.Millisecond {
		t.Errorf("timeout too fast: %v", elapsed)
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("timeout too slow: %v", elapsed)
	}

	// Clean up
	limiter.Release()
}

func TestUploadLimiter_ConcurrentAccess(t *testing.T) {
	const maxConcurrent = 3
	const totalRequests = 10

	limiter := NewUploadLimiter(maxConcurrent, time.Second)

	var wg sync.WaitGroup
	var mu sync.Mutex
	maxObserved := 0

	for i := 0; i < totalRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			ctx := context.Background()
			if err := limiter.Acquire(ctx); err != nil {
				t.Errorf("Acquire failed: %v", err)
				return
			}
			defer limiter.Release()

			// Record the observed active count
			mu.Lock()
			current := limiter.ActiveCount()
			if current > maxObserved {
				maxObserved = current
			}
			mu.Unlock()

			// Simulate some work
			time.Sleep(10 * time.Millisecond)
		}()
	}

	wg.Wait()

	// Should never have exceeded max concurrent
	if maxObserved > maxConcurrent {
		t.Errorf("exceeded max concurrent: observed %d, max %d", maxObserved, maxConcurrent)
	}

	// All should be released
	if got := limiter.ActiveCount(); got != 0 {
		t.Errorf("final ActiveCount = %d, want 0", got)
	}
}

func TestUploadLimiter_TryAcquire(t *testing.T) {
	limiter := NewUploadLimiter(1, time.Second)

	// First TryAcquire should succeed
	if !limiter.TryAcquire() {
		t.Error("first TryAcquire should succeed")
	}

	// Second TryAcquire should fail immediately (no blocking)
	start := time.Now()
	if limiter.TryAcquire() {
		t.Error("second TryAcquire should fail")
		limiter.Release()
	}
	elapsed := time.Since(start)

	// Should return immediately (not block)
	if elapsed > 10*time.Millisecond {
		t.Errorf("TryAcquire blocked for %v", elapsed)
	}

	// Release and try again
	limiter.Release()

	if !limiter.TryAcquire() {
		t.Error("TryAcquire after Release should succeed")
	}
	limiter.Release()
}

func TestUploadLimiter_ContextCancellation(t *testing.T) {
	limiter := NewUploadLimiter(1, 5*time.Second)

	ctx := context.Background()

	// Acquire the only slot
	if err := limiter.Acquire(ctx); err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	// Create a cancellable context
	cancelCtx, cancel := context.WithCancel(context.Background())

	// Start trying to acquire in a goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- limiter.Acquire(cancelCtx)
	}()

	// Cancel after a short delay
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Should receive context.Canceled
	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Error("Acquire did not return after context cancellation")
	}

	limiter.Release()
}

func TestUploadLimiter_WaitForDrain(t *testing.T) {
	limiter := NewUploadLimiter(2, time.Second)

	ctx := context.Background()

	// Acquire two slots
	limiter.Acquire(ctx)
	limiter.Acquire(ctx)

	// Start draining in a goroutine
	drainDone := make(chan error, 1)
	go func() {
		drainDone <- limiter.WaitForDrain(context.Background())
	}()

	// Ensure WaitForDrain is blocked
	select {
	case <-drainDone:
		t.Error("WaitForDrain returned too early")
	case <-time.After(50 * time.Millisecond):
		// Expected - still waiting
	}

	// Release one
	limiter.Release()

	// Still should be waiting (one active)
	select {
	case <-drainDone:
		t.Error("WaitForDrain returned with one active")
	case <-time.After(50 * time.Millisecond):
		// Expected
	}

	// Release the last one
	limiter.Release()

	// Now should complete
	select {
	case err := <-drainDone:
		if err != nil {
			t.Errorf("WaitForDrain returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Error("WaitForDrain did not complete after all released")
	}
}

func TestUploadLimiter_WaitForDrain_ContextCancelled(t *testing.T) {
	limiter := NewUploadLimiter(1, time.Second)

	ctx := context.Background()
	limiter.Acquire(ctx)

	cancelCtx, cancel := context.WithCancel(context.Background())

	drainDone := make(chan error, 1)
	go func() {
		drainDone <- limiter.WaitForDrain(cancelCtx)
	}()

	// Cancel the drain context
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-drainDone:
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Error("WaitForDrain did not return after context cancellation")
	}

	limiter.Release()
}

func TestUploadLimiter_Status(t *testing.T) {
	limiter := NewUploadLimiter(3, time.Second)

	status := limiter.Status()
	if status.Active != 0 {
		t.Errorf("initial Active = %d, want 0", status.Active)
	}
	if status.Available != 3 {
		t.Errorf("initial Available = %d, want 3", status.Available)
	}
	if status.MaxConcurrent != 3 {
		t.Errorf("MaxConcurrent = %d, want 3", status.MaxConcurrent)
	}

	ctx := context.Background()
	limiter.Acquire(ctx)
	limiter.Acquire(ctx)

	status = limiter.Status()
	if status.Active != 2 {
		t.Errorf("Active = %d, want 2", status.Active)
	}
	if status.Available != 1 {
		t.Errorf("Available = %d, want 1", status.Available)
	}

	limiter.Release()
	limiter.Release()
}

func TestUploadLimiter_DefaultValues(t *testing.T) {
	// Test with invalid values - should use defaults
	limiter := NewUploadLimiter(0, 0)

	if got := limiter.MaxConcurrent(); got != DefaultMaxConcurrentUploads {
		t.Errorf("MaxConcurrent = %d, want %d", got, DefaultMaxConcurrentUploads)
	}
}

func TestUploadLimiter_UnblocksWaiter(t *testing.T) {
	limiter := NewUploadLimiter(1, time.Second)

	ctx := context.Background()

	// Acquire the slot
	if err := limiter.Acquire(ctx); err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	// Start a waiter
	acquired := make(chan struct{})
	go func() {
		if err := limiter.Acquire(ctx); err != nil {
			t.Errorf("waiting Acquire failed: %v", err)
			return
		}
		close(acquired)
		limiter.Release()
	}()

	// Give the waiter time to block
	time.Sleep(50 * time.Millisecond)

	// Release - waiter should acquire
	limiter.Release()

	select {
	case <-acquired:
		// Success
	case <-time.After(500 * time.Millisecond):
		t.Error("waiter did not acquire after release")
	}
}
