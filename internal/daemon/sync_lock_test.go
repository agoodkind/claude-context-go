package daemon

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func newTestSyncLock(t *testing.T) (*syncLock, string) {
	t.Helper()
	root := t.TempDir()
	lockPath := filepath.Join(root, "mcp-sync.lock")
	return newSyncLock(lockPath, root, 600000), lockPath
}

// TestSyncLockRefcountHoldsAcrossNestedAcquire proves the lock directory is
// created on the first acquire, survives while any reference is held, and is
// removed only after the last release. This is the contract that keeps the
// directory present for the whole window any daemon embed is active.
func TestSyncLockRefcountHoldsAcrossNestedAcquire(t *testing.T) {
	lock, lockPath := newTestSyncLock(t)
	ctx := context.Background()

	if !lock.acquire(ctx) {
		t.Fatal("first acquire should succeed")
	}
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("lock dir missing after first acquire: %v", err)
	}

	if !lock.acquire(ctx) {
		t.Fatal("nested acquire under the same daemon should succeed")
	}
	lock.release(ctx)
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("lock dir removed while a reference remains: %v", err)
	}

	lock.release(ctx)
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatalf("lock dir should be gone after the last release; stat err=%v", err)
	}
}

// TestSyncLockExternalHolderDefers proves a daemon embed defers when the lock
// directory already exists with a fresh timestamp and no local reference, which
// is how the daemon yields to the upstream TS adapter.
func TestSyncLockExternalHolderDefers(t *testing.T) {
	lock, lockPath := newTestSyncLock(t)
	ctx := context.Background()

	if err := os.Mkdir(lockPath, 0o755); err != nil {
		t.Fatalf("Mkdir returned error: %v", err)
	}
	if lock.acquire(ctx) {
		t.Fatal("acquire should defer while a fresh external lock is held")
	}
}

// TestSyncLockReclaimsStaleDir proves a lock directory left by a crashed holder
// is reclaimed once it is older than the stale window, so a dead holder cannot
// block embedding forever.
func TestSyncLockReclaimsStaleDir(t *testing.T) {
	lock, lockPath := newTestSyncLock(t)
	ctx := context.Background()

	if err := os.Mkdir(lockPath, 0o755); err != nil {
		t.Fatalf("Mkdir returned error: %v", err)
	}
	stale := time.Now().Add(-30 * time.Minute)
	if err := os.Chtimes(lockPath, stale, stale); err != nil {
		t.Fatalf("Chtimes returned error: %v", err)
	}
	if !lock.acquire(ctx) {
		t.Fatal("acquire should reclaim a stale lock dir")
	}
	lock.release(ctx)
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatalf("lock dir should be gone after release of a reclaimed lock; stat err=%v", err)
	}
}

// TestSyncLockReclaimsDeadOwnerImmediately proves a lock left by a crashed
// daemon (its recorded owner PID no longer alive) is reclaimed at once, even
// when the directory is fresh, so a restart resumes indexing without waiting
// out the stale window.
func TestSyncLockReclaimsDeadOwnerImmediately(t *testing.T) {
	lock, lockPath := newTestSyncLock(t)
	ctx := context.Background()

	if err := os.Mkdir(lockPath, 0o755); err != nil {
		t.Fatalf("Mkdir returned error: %v", err)
	}
	// A PID that is essentially certain not to be a live process.
	if err := os.WriteFile(filepath.Join(lockPath, ownerPidFileName), []byte(strconv.Itoa(2147483646)), 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if !lock.acquire(ctx) {
		t.Fatal("acquire should reclaim a fresh lock whose owner process is dead")
	}
	lock.release(ctx)
}

// TestSyncLockDefersLiveOwner proves a lock whose recorded owner is a live
// process is left alone, so the daemon never steals a lock another live owner
// holds.
func TestSyncLockDefersLiveOwner(t *testing.T) {
	lock, lockPath := newTestSyncLock(t)
	ctx := context.Background()

	if err := os.Mkdir(lockPath, 0o755); err != nil {
		t.Fatalf("Mkdir returned error: %v", err)
	}
	if err := os.WriteFile(filepath.Join(lockPath, ownerPidFileName), []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if lock.acquire(ctx) {
		t.Fatal("acquire should defer to a lock held by a live owner")
	}
}

// TestBeginConvergeSerializesPerCodebase proves the per-codebase guard admits
// one converge per codebase at a time while letting distinct codebases proceed
// concurrently, so a heavily-edited repository cannot starve the others and two
// converges of the same codebase never race on its snapshot.
func TestBeginConvergeSerializesPerCodebase(t *testing.T) {
	syncer := &BackgroundSync{converging: make(map[string]struct{})}

	if !syncer.beginConverge("cb1") {
		t.Fatal("first beginConverge for cb1 should succeed")
	}
	if syncer.beginConverge("cb1") {
		t.Fatal("second beginConverge for cb1 must fail while the first runs")
	}
	if !syncer.beginConverge("cb2") {
		t.Fatal("beginConverge for a distinct codebase should succeed concurrently")
	}

	syncer.endConverge("cb1")
	if !syncer.beginConverge("cb1") {
		t.Fatal("beginConverge for cb1 should succeed again after endConverge")
	}
}
