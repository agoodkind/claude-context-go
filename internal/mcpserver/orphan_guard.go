package mcpserver

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"
)

// orphanInitPID is the PID a process inherits when its parent dies on Unix.
const orphanInitPID = 1

// fallbackPollInterval is how often the polling fallback checks the parent
// PID. The native event source (pidfd_open on Linux, kqueue EVFILT_PROC on
// Darwin/BSD) is preferred and detects exit in sub-millisecond time; this
// interval only matters on platforms or kernels where the native source is
// unavailable.
const fallbackPollInterval = 200 * time.Millisecond

// getppidFunc is the parent-PID probe used at startup. Tests substitute a
// fake so they can control whether the guard enters its watch loop without
// spawning a real process.
var getppidFunc = os.Getppid

// parentDeathSignalFunc returns a channel that closes when parentPID exits.
// Production wires this to [parentDeathSignal], which uses the platform's
// native event source. Tests override it to inject a controllable channel so
// they do not depend on real syscalls or timing.
var parentDeathSignalFunc = parentDeathSignal

// watchParentDeath cancels ctx when the process is reparented to init.
//
// The watch is event-driven: pidfd_open + poll on Linux, kqueue with
// EVFILT_PROC NOTE_EXIT on Darwin/BSD. Detection is immediate and independent
// of the goroutine scheduler, so heavy parallel test load no longer races
// against a polling tick. On platforms without a native event source the guard
// falls back to a short-interval poll.
//
// The watcher returns either when the caller cancels ctx (graceful shutdown)
// or when the parent PID becomes init, which means the original parent
// (Claude, Cursor, the launching shell, etc.) exited and this process is now
// orphaned. Cancelling the context lets the stdio server unwind cleanly
// instead of lingering and accumulating into the kind of pile that pushed
// system load to 28 in the upstream TS adapter.
func watchParentDeath(ctx context.Context, cancel context.CancelFunc) {
	startingParent := getppidFunc()
	if startingParent == orphanInitPID {
		// Parent already gone (or we were launched directly by init/launchd).
		// The guard never fires in this case; let the caller manage lifetime.
		return
	}
	signal := parentDeathSignalFunc(ctx, startingParent)
	select {
	case <-ctx.Done():
		return
	case <-signal:
		slog.WarnContext(ctx, "parent process exited; shutting down to avoid orphan accumulation", "starting_parent_pid", startingParent)
		cancel()
	}
}

// goSafeOrphan launches run on a new goroutine that recovers from panics so a
// crash in the orphan-guard plumbing never tears down the host process. The
// helper is package-local because the daemon's goSafe lives in main.
func goSafeOrphan(ctx context.Context, run func()) {
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.ErrorContext(ctx, "orphan guard goroutine panic", "err", fmt.Errorf("panic: %v", recovered))
			}
		}()
		run()
	}()
}

// pollParentDeath polls the parent PID until it equals init, then closes ch.
// Used as the fallback when the native event source is unavailable, and as
// the implementation on platforms without a native source. The interval is
// short because the syscall is cheap and detection latency matters.
func pollParentDeath(ctx context.Context, ch chan struct{}) {
	defer close(ch)
	ticker := time.NewTicker(fallbackPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if getppidFunc() == orphanInitPID {
				return
			}
		}
	}
}
