package mcpserver

import (
	"context"
	"testing"
	"time"
)

// TestWatchParentDeathExitsWhenReparented proves that the guard cancels the
// run context as soon as its parent-death signal channel closes. The test
// substitutes a controllable channel for the native event source so the
// outcome is deterministic and not subject to scheduler load.
func TestWatchParentDeathExitsWhenReparented(t *testing.T) {
	// Not Parallel: these tests mutate package-level getppidFunc and
	// parentDeathSignalFunc, so they must run sequentially to avoid racing
	// each other on the swap.
	originalProbe := getppidFunc
	getppidFunc = func() int { return 12345 }
	t.Cleanup(func() { getppidFunc = originalProbe })

	deathSignal := make(chan struct{})
	originalSig := parentDeathSignalFunc
	parentDeathSignalFunc = func(_ context.Context, _ int) <-chan struct{} {
		return deathSignal
	}
	t.Cleanup(func() { parentDeathSignalFunc = originalSig })

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		watchParentDeath(runCtx, cancel)
		close(done)
	}()

	close(deathSignal)

	select {
	case <-runCtx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("watchParentDeath did not cancel run context after parent-death signal")
	}
	<-done
}

// TestWatchParentDeathReturnsImmediatelyWhenAlreadyOrphan proves the guard
// is a no-op when the process is already a child of init at startup. The
// caller, not the guard, owns lifecycle in that case.
func TestWatchParentDeathReturnsImmediatelyWhenAlreadyOrphan(t *testing.T) {
	// Not Parallel: these tests mutate package-level getppidFunc and
	// parentDeathSignalFunc, so they must run sequentially to avoid racing
	// each other on the swap.
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

	if runCtx.Err() != nil {
		t.Fatal("watchParentDeath cancelled the run context when no transition occurred")
	}
}

// TestWatchParentDeathStopsOnContextCancel proves the guard exits cleanly
// when the caller cancels the run context, without waiting for any signal
// from the parent-death source.
func TestWatchParentDeathStopsOnContextCancel(t *testing.T) {
	// Not Parallel: these tests mutate package-level getppidFunc and
	// parentDeathSignalFunc, so they must run sequentially to avoid racing
	// each other on the swap.
	originalProbe := getppidFunc
	getppidFunc = func() int { return 12345 }
	t.Cleanup(func() { getppidFunc = originalProbe })

	originalSig := parentDeathSignalFunc
	parentDeathSignalFunc = func(_ context.Context, _ int) <-chan struct{} {
		return make(chan struct{})
	}
	t.Cleanup(func() { parentDeathSignalFunc = originalSig })

	runCtx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		watchParentDeath(runCtx, cancel)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watchParentDeath did not return after context cancellation")
	}
}
