package mcpserver

import (
	"context"
	"log/slog"
	"os"
	"time"
)

// orphanPollInterval is how often the watcher checks the parent PID.
const orphanPollInterval = 2 * time.Second

// orphanInitPID is the PID a process inherits when its parent dies on Unix.
const orphanInitPID = 1

// getppidFunc is the parent-PID probe used by the guard. It is var-typed so
// tests can substitute a fake.
var getppidFunc = os.Getppid

// watchParentDeath cancels the supplied context when the process is reparented
// to init. The watcher returns either when the context is cancelled by the
// caller (graceful shutdown) or when the parent PID becomes init, which means
// the original parent (Claude, Cursor, the launching shell, etc.) exited and
// this process is now orphaned. Cancelling the context lets the stdio server
// unwind cleanly instead of lingering and accumulating into the kind of pile
// that pushed system load to 28 in the upstream TS adapter.
func watchParentDeath(ctx context.Context, cancel context.CancelFunc) {
	startingParent := getppidFunc()
	if startingParent == orphanInitPID {
		// Parent already gone (or we were launched directly by init/launchd).
		// The guard never fires in this case; let the caller manage lifetime.
		return
	}

	ticker := time.NewTicker(orphanPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			currentParent := getppidFunc()
			if currentParent == orphanInitPID {
				slog.WarnContext(ctx, "parent process exited; shutting down to avoid orphan accumulation", "starting_parent_pid", startingParent)
				cancel()
				return
			}
		}
	}
}
