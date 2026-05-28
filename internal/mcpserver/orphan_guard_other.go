//go:build !linux && !darwin

package mcpserver

import "context"

// parentDeathSignal falls back to polling on platforms without a native
// process-exit event source. The polling interval is short enough that
// detection latency stays well under a second.
func parentDeathSignal(ctx context.Context, _ int) <-chan struct{} {
	ch := make(chan struct{})
	goSafeOrphan(ctx, func() { pollParentDeath(ctx, ch) })
	return ch
}
