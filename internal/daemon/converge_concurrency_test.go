package daemon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"goodkind.io/claude-context-go/internal/merkle"
	"goodkind.io/claude-context-go/internal/metrics"
	"goodkind.io/claude-context-go/internal/model"
	"goodkind.io/claude-context-go/internal/semantic"
)

// fakeSemantic is a semanticIndex double for converge tests. reindex and
// copyChunks are the only behaviors a converge exercises; the rest return inert
// values so the manager treats the backend as available and empty.
type fakeSemantic struct {
	reindex    func(ctx context.Context, codebasePath string, chunks []model.StoredChunk, removed []string) error
	copyChunks func(ctx context.Context, codebasePath string, src string, dst string) (int, error)
}

func (f *fakeSemantic) Available() bool                { return true }
func (f *fakeSemantic) CollectionName(_ string) string { return "code_chunks_test" }
func (f *fakeSemantic) HasStaging(context.Context, string) (bool, error) {
	return false, nil
}

func (f *fakeSemantic) Search(context.Context, string, string, int32, []string) ([]model.StoredChunk, error) {
	return nil, nil
}
func (f *fakeSemantic) Count(context.Context, string) (int32, error) { return 0, nil }

// ListCollections returns the one collection CollectionName reports, keeping the
// fake self-consistent: the forward reconciler in GetIndex drops a registry
// entry whose collection is absent from this list, so it must include it.
func (f *fakeSemantic) ListCollections(context.Context) ([]string, error) {
	return []string{"code_chunks_test"}, nil
}

func (f *fakeSemantic) HasCollectionForPath(context.Context, string) (bool, error) {
	return true, nil
}

func (f *fakeSemantic) Reindex(ctx context.Context, codebasePath string, chunks []model.StoredChunk, removed []string, _ func(semantic.Progress)) error {
	if f.reindex != nil {
		return f.reindex(ctx, codebasePath, chunks, removed)
	}
	return nil
}

func (f *fakeSemantic) StageReindex(context.Context, string, []model.StoredChunk, []string, func(semantic.Progress)) error {
	return nil
}
func (f *fakeSemantic) PromoteStaging(context.Context, string) error { return nil }

func (f *fakeSemantic) CopyChunks(ctx context.Context, codebasePath string, src string, dst string) (int, error) {
	if f.copyChunks != nil {
		return f.copyChunks(ctx, codebasePath, src, dst)
	}
	return 0, nil
}

func (f *fakeSemantic) PruneToCurrent(context.Context, string, []string) error { return nil }
func (f *fakeSemantic) Drop(context.Context, string) error                     { return nil }
func (f *fakeSemantic) DropStaging(context.Context, string) error              { return nil }

// TestConvergeViaWatcherRunsCodebasesConcurrentlyUpToCap proves that several
// codebases converge at once up to the index-slot cap while another waits, that
// the shared lock exists while any converge runs, and that it is gone once all
// converges finish.
func TestConvergeViaWatcherRunsCodebasesConcurrentlyUpToCap(t *testing.T) {
	const cap = 2
	const codebases = 3

	manager, cfg := newTestManagerWithCap(t, cap)
	entered := make(chan struct{}, codebases)
	release := make(chan struct{})
	inFlight := atomic.Int32{}
	maxInFlight := atomic.Int32{}
	manager.semantic = &fakeSemantic{
		reindex: func(_ context.Context, _ string, _ []model.StoredChunk, _ []string) error {
			current := inFlight.Add(1)
			for {
				observed := maxInFlight.Load()
				if current <= observed || maxInFlight.CompareAndSwap(observed, current) {
					break
				}
			}
			entered <- struct{}{}
			<-release
			inFlight.Add(-1)
			return nil
		},
	}

	syncer := NewBackgroundSync(cfg, manager)
	syncer.queue = NewEventQueue(time.Hour, func(string, []string) {})

	ids := make([]string, 0, codebases)
	for i := range codebases {
		canonical := newCapTestRepo(t)
		id := fmt.Sprintf("cb-converge-%d", i)
		manager.mu.Lock()
		manager.codebases[id] = model.Codebase{
			ID:              id,
			CanonicalPath:   canonical,
			Status:          model.CodebaseStatusIndexed,
			EffectiveConfig: defaultIndexConfig(),
		}
		manager.mu.Unlock()
		ids = append(ids, id)
	}

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(codebaseID string) {
			defer wg.Done()
			syncer.convergeViaWatcher(context.Background(), codebaseID, []string{"main.go"})
		}(id)
	}

	// Exactly cap converges embed before any slot frees.
	for range cap {
		<-entered
	}
	waitForCondition(t, func() bool { return inFlight.Load() == int32(cap) })
	if got := maxInFlight.Load(); got > int32(cap) {
		t.Fatalf("max concurrent converges = %d, want <= %d", got, cap)
	}

	lockPath := filepath.Join(cfg.ContextRoot, "mcp-sync.lock")
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("sync lock should exist while converges run: %v", err)
	}

	close(release)
	for i := cap; i < codebases; i++ {
		<-entered
	}
	wg.Wait()

	if got := maxInFlight.Load(); got > int32(cap) {
		t.Fatalf("max concurrent converges over the run = %d, want <= %d", got, cap)
	}
	waitForCondition(t, func() bool {
		_, err := os.Stat(lockPath)
		return os.IsNotExist(err)
	})
}

// TestConvergeViaWatcherDefersToExternalLock proves a converge yields when the
// shared lock is held externally (a fresh directory with no daemon owner
// marker, as the upstream TS adapter leaves it) and requeues its paths instead
// of embedding.
func TestConvergeViaWatcherDefersToExternalLock(t *testing.T) {
	manager, cfg := newTestManagerWithCap(t, 2)
	var reindexCalls atomic.Int32
	manager.semantic = &fakeSemantic{
		reindex: func(_ context.Context, _ string, _ []model.StoredChunk, _ []string) error {
			reindexCalls.Add(1)
			return nil
		},
	}

	var requeued atomic.Int32
	syncer := NewBackgroundSync(cfg, manager)
	// A short debounce lets the requeued path drain promptly; the drain just
	// records the requeue rather than re-running the converge.
	syncer.queue = NewEventQueue(20*time.Millisecond, func(string, []string) { requeued.Add(1) })

	canonical := newCapTestRepo(t)
	codebaseID := "cb-external-lock"
	manager.mu.Lock()
	manager.codebases[codebaseID] = model.Codebase{
		ID:              codebaseID,
		CanonicalPath:   canonical,
		Status:          model.CodebaseStatusIndexed,
		EffectiveConfig: defaultIndexConfig(),
	}
	manager.mu.Unlock()

	// An external holder: a fresh lock directory with no owner marker.
	lockPath := filepath.Join(cfg.ContextRoot, "mcp-sync.lock")
	if err := os.MkdirAll(cfg.ContextRoot, 0o755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}
	if err := os.Mkdir(lockPath, 0o755); err != nil {
		t.Fatalf("Mkdir returned error: %v", err)
	}

	syncer.convergeViaWatcher(context.Background(), codebaseID, []string{"main.go"})

	if got := reindexCalls.Load(); got != 0 {
		t.Fatalf("converge embedded %d time(s) while the lock was held externally, want 0", got)
	}
	waitForCondition(t, func() bool { return requeued.Load() >= 1 })
}

// TestConvergeCopyChunksFiresOnRename proves a renamed file converges through
// the CopyChunks fast path rather than a re-embed, incrementing
// converge_copy_chunks_total.
func TestConvergeCopyChunksFiresOnRename(t *testing.T) {
	manager, _ := newTestManagerWithCap(t, 2)
	var copyCalls atomic.Int32
	manager.semantic = &fakeSemantic{
		copyChunks: func(_ context.Context, _ string, _ string, _ string) (int, error) {
			copyCalls.Add(1)
			return 5, nil
		},
		reindex: func(_ context.Context, _ string, _ []model.StoredChunk, _ []string) error {
			return nil
		},
	}

	canonical := newCapTestRepo(t)
	if err := os.WriteFile(filepath.Join(canonical, "src.go"), []byte("package main\nfunc Moved() {}\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	cfg := defaultIndexConfig()
	cfg.IgnoreDigest = "sha256:rename-test"
	codebaseID := "cb-rename"
	manager.mu.Lock()
	manager.codebases[codebaseID] = model.Codebase{
		ID:              codebaseID,
		CanonicalPath:   canonical,
		Status:          model.CodebaseStatusIndexed,
		EffectiveConfig: cfg,
	}
	manager.mu.Unlock()

	// Seed a checkpoint recording src.go with its real content hash and inode,
	// so the renamed file is recognized as a move of src.go.
	captured, err := merkle.Capture(context.Background(), canonical, cfg)
	if err != nil {
		t.Fatalf("Capture returned error: %v", err)
	}
	identity, err := statInode(filepath.Join(canonical, "src.go"))
	if err != nil {
		t.Fatalf("statInode returned error: %v", err)
	}
	checkpoint := merkle.Snapshot{
		ConfigDigest: cfg.IgnoreDigest,
		Files:        map[string]string{"src.go": captured.Files["src.go"]},
		Inodes:       map[string]merkle.InodeRef{"src.go": {Device: identity.device, Inode: identity.inode}},
	}
	if err := merkle.WriteSnapshot(manager.merklePath(codebaseID), checkpoint); err != nil {
		t.Fatalf("WriteSnapshot returned error: %v", err)
	}

	// Rename on the same filesystem preserves the inode, which is what the fast
	// path keys on.
	if err := os.Rename(filepath.Join(canonical, "src.go"), filepath.Join(canonical, "dst.go")); err != nil {
		t.Fatalf("Rename returned error: %v", err)
	}

	before := metrics.Read().ConvergeCopyChunksTotal
	if err := manager.ConvergePaths(context.Background(), codebaseID, []string{"src.go", "dst.go"}); err != nil {
		t.Fatalf("ConvergePaths returned error: %v", err)
	}

	if got := copyCalls.Load(); got != 1 {
		t.Fatalf("CopyChunks called %d time(s), want 1 (the rename fast path)", got)
	}
	if delta := metrics.Read().ConvergeCopyChunksTotal - before; delta != 1 {
		t.Fatalf("converge_copy_chunks_total moved by %d, want 1", delta)
	}

	snapshot, err := merkle.ReadSnapshot(manager.merklePath(codebaseID))
	if err != nil {
		t.Fatalf("ReadSnapshot returned error: %v", err)
	}
	if _, present := snapshot.Files["dst.go"]; !present {
		t.Fatalf("snapshot missing renamed destination dst.go; have %v", snapshot.Files)
	}
}
