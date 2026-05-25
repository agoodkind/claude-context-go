package mcpserver

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestWatchParentDeathExitsWhenReparented(t *testing.T) {
	t.Parallel()

	// Force a tight poll interval so this test stays fast.
	original := orphanPollInterval
	defer func() { _ = original }()

	var pollCount int32
	originalProbe := getppidFunc
	getppidFunc = func() int {
		count := atomic.AddInt32(&pollCount, 1)
		if count == 1 {
			return 12345 // starting parent is alive
		}
		return orphanInitPID
	}
	t.Cleanup(func() { getppidFunc = originalProbe })

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		watchParentDeath(runCtx, cancel)
		close(done)
	}()

	select {
	case <-runCtx.Done():
	case <-time.After(15 * time.Second):
		t.Fatal("watchParentDeath did not cancel run context when reparented to init")
	}
	<-done
}

func TestWatchParentDeathReturnsImmediatelyWhenAlreadyOrphan(t *testing.T) {
	t.Parallel()

	originalProbe := getppidFunc
	getppidFunc = func() int { return orphanInitPID }
	t.Cleanup(func() { getppidFunc = originalProbe })

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		watchParentDeath(runCtx, cancel)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watchParentDeath did not return when parent is already init")
	}

	// Parent was already init at start time, so we must NOT have cancelled
	// the run context (caller still owns lifecycle).
	if runCtx.Err() != nil {
		t.Fatal("watchParentDeath cancelled the run context when no transition occurred")
	}
}

func TestWatchParentDeathStopsOnContextCancel(t *testing.T) {
	t.Parallel()

	originalProbe := getppidFunc
	getppidFunc = func() int { return 12345 }
	t.Cleanup(func() { getppidFunc = originalProbe })

	runCtx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		watchParentDeath(runCtx, cancel)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("watchParentDeath did not return when context was cancelled")
	}
}
