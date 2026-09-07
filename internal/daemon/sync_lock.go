package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"syscall"
	"time"

	"goodkind.io/lm-semantic-search/internal/store"
)

const syncLockRetryInterval = 500 * time.Millisecond

// syncLockOutcome classifies one acquire attempt so a caller can tell ordinary
// contention, which clears when the other holder finishes, from a failure
// waiting cannot fix.
type syncLockOutcome string

const (
	// syncLockAcquired means the caller holds a reference and must release the
	// returned lease exactly once.
	syncLockAcquired syncLockOutcome = "acquired"
	// syncLockBusy means another holder owns the lock right now, so a later
	// attempt may succeed.
	syncLockBusy syncLockOutcome = "busy"
	// syncLockCancelled means the caller's context ended before the lock could
	// be taken.
	syncLockCancelled syncLockOutcome = "cancelled"
	// syncLockFailed means waiting cannot make the current failure succeed.
	syncLockFailed syncLockOutcome = "failed"
)

// errSyncLockWaitCancelled reports a wait that ended because the caller's
// context ended.
var errSyncLockWaitCancelled = errors.New("sync lock wait cancelled")

// syncLock is a process-wide refcounted hold of the kernel file lock that
// serializes daemon embeds against any other process holding the same lock
// file. The kernel releases the lock when the holding process exits. The
// upstream TypeScript claude-context tool takes a different lock and is not
// excluded by this one, so the two tools do not coordinate in either direction.
type syncLock struct {
	lockPath string
	rootDir  string
	mu       sync.Mutex
	refcount int
	file     *os.File
}

// newSyncLock builds a refcounted hold of the kernel file lock at lockPath.
// rootDir is created before the first acquisition.
func newSyncLock(lockPath string, rootDir string) *syncLock {
	return &syncLock{
		lockPath: lockPath,
		rootDir:  rootDir,
		mu:       sync.Mutex{},
		refcount: 0,
		file:     nil,
	}
}

// syncLockLease is one acquisition's release handle. Every successful acquire
// returns a fresh lease whose release runs at most once, so a duplicate release
// cannot drop a reference that a later acquisition took.
type syncLockLease struct {
	lock *syncLock
	once *sync.Once
}

// release drops this acquisition's reference and releases the kernel lock when
// the last reference leaves.
func (lease syncLockLease) release(ctx context.Context) {
	if lease.lock == nil || lease.once == nil {
		return
	}
	lease.once.Do(func() {
		lease.lock.releaseReference(ctx)
	})
}

// acquire takes one reference to the kernel file lock. Ordinary contention
// returns syncLockBusy. A filesystem or lock failure returns syncLockFailed
// because waiting cannot repair it.
func (lock *syncLock) acquire(ctx context.Context) (syncLockLease, syncLockOutcome, error) {
	lock.mu.Lock()
	defer lock.mu.Unlock()

	if lock.refcount > 0 {
		lock.refcount++
		return newSyncLockLease(lock), syncLockAcquired, nil
	}

	if err := store.EnsureDir(lock.rootDir); err != nil {
		slog.ErrorContext(ctx, "ensure sync lock root failed", "path", lock.rootDir, "err", err)
		return syncLockLease{lock: nil, once: nil}, syncLockFailed,
			fmt.Errorf("ensure sync lock root %s: %w", lock.rootDir, err)
	}

	file, outcome, err := lock.lockFileLocked(ctx)
	if outcome != syncLockAcquired {
		return syncLockLease{lock: nil, once: nil}, outcome, err
	}

	lock.file = file
	lock.refcount = 1
	return newSyncLockLease(lock), syncLockAcquired, nil
}

// newSyncLockLease builds the release handle for one acquisition.
func newSyncLockLease(lock *syncLock) syncLockLease {
	return syncLockLease{lock: lock, once: &sync.Once{}}
}

// lockFileLocked opens the lock file and takes a non-blocking exclusive lock.
// The caller holds lock.mu.
func (lock *syncLock) lockFileLocked(
	ctx context.Context,
) (*os.File, syncLockOutcome, error) {
	file, err := os.OpenFile(lock.lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		slog.ErrorContext(ctx, "open sync lock failed", "path", lock.lockPath, "err", err)
		return nil, syncLockFailed, fmt.Errorf("open sync lock %s: %w", lock.lockPath, err)
	}

	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		_ = file.Close()
		return nil, syncLockBusy, nil
	}
	if err != nil {
		_ = file.Close()
		slog.ErrorContext(ctx, "acquire sync lock failed", "path", lock.lockPath, "err", err)
		return nil, syncLockFailed, fmt.Errorf("acquire sync lock %s: %w", lock.lockPath, err)
	}
	return file, syncLockAcquired, nil
}

// unlockFileLocked drops the kernel lock and closes the descriptor. The caller
// holds lock.mu.
func (lock *syncLock) unlockFileLocked(ctx context.Context) {
	if lock.file == nil {
		return
	}
	if err := syscall.Flock(int(lock.file.Fd()), syscall.LOCK_UN); err != nil {
		slog.ErrorContext(ctx, "release sync lock failed", "path", lock.lockPath, "err", err)
	}
	if err := lock.file.Close(); err != nil {
		slog.ErrorContext(ctx, "close sync lock failed", "path", lock.lockPath, "err", err)
	}
	lock.file = nil
}

// releaseReference drops one reference and releases the kernel lock when the
// last reference leaves.
func (lock *syncLock) releaseReference(ctx context.Context) {
	lock.mu.Lock()
	defer lock.mu.Unlock()

	if lock.refcount == 0 {
		return
	}
	lock.refcount--
	if lock.refcount > 0 {
		return
	}
	lock.unlockFileLocked(ctx)
}
