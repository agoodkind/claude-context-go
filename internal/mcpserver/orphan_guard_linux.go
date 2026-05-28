//go:build linux

package mcpserver

import (
	"context"
	"errors"
	"log/slog"

	"golang.org/x/sys/unix"
)

// parentDeathSignal opens a pidfd for parentPID and waits for its exit by
// polling that fd for POLLIN. pidfd_open is event-driven at the kernel level:
// the fd becomes readable the instant the referenced process exits, so the
// wakeup is independent of any timer or scheduler.
//
// pidfd_open requires Linux 5.3+. On older kernels we fall back to a
// short-interval poll of Getppid.
func parentDeathSignal(ctx context.Context, parentPID int) <-chan struct{} {
	ch := make(chan struct{})
	pidfd, err := unix.PidfdOpen(parentPID, 0)
	if err != nil {
		slog.WarnContext(ctx, "pidfd_open unavailable; falling back to polling parent pid", "parent_pid", parentPID, "err", err)
		goSafeOrphan(ctx, func() { pollParentDeath(ctx, ch) })
		return ch
	}
	goSafeOrphan(ctx, func() { waitPidfdExit(ctx, pidfd, ch) })
	return ch
}

// waitPidfdExit blocks on Poll until the pidfd signals exit. Poll uses a 1
// second timeout solely to let ctx cancellation propagate; detection of the
// actual exit is immediate (Poll wakes on the kernel event, not the timeout).
func waitPidfdExit(ctx context.Context, pidfd int, ch chan struct{}) {
	defer close(ch)
	defer func() { _ = unix.Close(pidfd) }()
	fds := []unix.PollFd{{Fd: int32(pidfd), Events: unix.POLLIN, Revents: 0}}
	const pollTimeoutMillis = 1000
	for {
		n, err := unix.Poll(fds, pollTimeoutMillis)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			slog.ErrorContext(ctx, "pidfd poll failed", "err", err)
			return
		}
		if n > 0 {
			return
		}
	}
}
