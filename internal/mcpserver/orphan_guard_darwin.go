//go:build darwin

package mcpserver

import (
	"context"
	"errors"
	"log/slog"
	"math"

	"golang.org/x/sys/unix"
)

// parentDeathSignal registers a kqueue EVFILT_PROC/NOTE_EXIT watch on
// parentPID and blocks until the kernel delivers the exit event. kqueue is
// event-driven: the wakeup is immediate on process exit, with no timer or
// poll loop involved in detection.
func parentDeathSignal(ctx context.Context, parentPID int) <-chan struct{} {
	ch := make(chan struct{})
	if parentPID <= 0 || parentPID > math.MaxInt32 {
		slog.WarnContext(ctx, "parent pid out of range for kqueue; falling back to polling", "parent_pid", parentPID)
		goSafeOrphan(ctx, func() { pollParentDeath(ctx, ch) })
		return ch
	}
	kq, err := unix.Kqueue()
	if err != nil {
		slog.WarnContext(ctx, "kqueue unavailable; falling back to polling parent pid", "parent_pid", parentPID, "err", err)
		goSafeOrphan(ctx, func() { pollParentDeath(ctx, ch) })
		return ch
	}
	change := unix.Kevent_t{
		Ident:  uint64(parentPID),
		Filter: int16(unix.EVFILT_PROC),
		Flags:  uint16(unix.EV_ADD | unix.EV_ONESHOT),
		Fflags: uint32(unix.NOTE_EXIT),
		Data:   0,
		Udata:  nil,
	}
	zero := unix.Timespec{Sec: 0, Nsec: 0}
	if _, regErr := unix.Kevent(kq, []unix.Kevent_t{change}, nil, &zero); regErr != nil {
		slog.WarnContext(ctx, "kqueue EVFILT_PROC register failed; falling back to polling", "parent_pid", parentPID, "err", regErr)
		_ = unix.Close(kq)
		goSafeOrphan(ctx, func() { pollParentDeath(ctx, ch) })
		return ch
	}
	goSafeOrphan(ctx, func() { waitKqueueExit(ctx, kq, ch) })
	return ch
}

// waitKqueueExit blocks on Kevent until the registered EVFILT_PROC fires. A 1
// second wait timeout lets ctx cancellation propagate; the exit itself is
// detected immediately by the kernel event, not by polling.
func waitKqueueExit(ctx context.Context, kq int, ch chan struct{}) {
	defer close(ch)
	defer func() { _ = unix.Close(kq) }()
	events := make([]unix.Kevent_t, 1)
	timeout := unix.Timespec{Sec: 1, Nsec: 0}
	for {
		n, err := unix.Kevent(kq, nil, events, &timeout)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			slog.ErrorContext(ctx, "kqueue Kevent failed", "err", err)
			return
		}
		if n > 0 {
			return
		}
	}
}
