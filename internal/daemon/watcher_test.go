package daemon

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rjeczalik/notify"
	"goodkind.io/claude-context-go/internal/discovery"
	"goodkind.io/claude-context-go/internal/model"
)

// stubNotifyEvent satisfies notify.EventInfo so the watcher's dispatch
// routine can be exercised without registering with the platform's native
// watch backend.
type stubNotifyEvent struct {
	path string
}

func (event stubNotifyEvent) Event() notify.Event { return notify.Write }
func (event stubNotifyEvent) Path() string        { return event.path }
func (event stubNotifyEvent) Sys() any            { return nil }

// TestWatcherAddCodebaseIsIdempotent confirms AddCodebase tolerates a
// second call for the same codebase id without duplicating the in-memory
// registration.
func TestWatcherAddCodebaseIsIdempotent(t *testing.T) {
	t.Parallel()
	manager, _, _ := newTestManager(t)
	queue := NewEventQueue(time.Millisecond, func(_ string, _ []string) {})
	watcher := NewWatcher(manager, queue)

	codebase := model.Codebase{
		ID:                  "cb_test_idempotent",
		CanonicalPath:       t.TempDir(),
		ResolvedIgnoreRules: discovery.IgnoreRules{Nodes: nil},
	}
	watcher.AddCodebase(context.Background(), codebase)
	watcher.AddCodebase(context.Background(), codebase)

	if got := watcher.activeRootCount(); got != 1 {
		t.Fatalf("activeRootCount=%d, want 1 after idempotent add", got)
	}
}

// TestWatcherRemoveCodebaseDropsRoot confirms RemoveCodebase clears the
// dispatch entry so events for that path stop enqueuing into the coalescer.
func TestWatcherRemoveCodebaseDropsRoot(t *testing.T) {
	t.Parallel()
	manager, _, _ := newTestManager(t)
	queue := NewEventQueue(time.Millisecond, func(_ string, _ []string) {})
	watcher := NewWatcher(manager, queue)

	codebase := model.Codebase{
		ID:                  "cb_test_remove",
		CanonicalPath:       t.TempDir(),
		ResolvedIgnoreRules: discovery.IgnoreRules{Nodes: nil},
	}
	watcher.AddCodebase(context.Background(), codebase)
	watcher.RemoveCodebase(context.Background(), codebase.ID)
	if got := watcher.activeRootCount(); got != 0 {
		t.Fatalf("activeRootCount=%d, want 0 after remove", got)
	}
	watcher.RemoveCodebase(context.Background(), codebase.ID)
}

// TestWatcherDispatchBroadcastsToAllCovering confirms that an event under
// a path covered by two overlapping codebases enqueues onto both ids, not
// just the longest-prefix match.
func TestWatcherDispatchBroadcastsToAllCovering(t *testing.T) {
	t.Parallel()
	manager, _, _ := newTestManager(t)

	var observedMu sync.Mutex
	observed := map[string][]string{}
	queue := NewEventQueue(5*time.Millisecond, func(codebaseID string, relativePaths []string) {
		observedMu.Lock()
		observed[codebaseID] = append(observed[codebaseID], relativePaths...)
		observedMu.Unlock()
	})
	watcher := NewWatcher(manager, queue)

	rootDir := t.TempDir()
	nestedDir := filepath.Join(rootDir, "child")
	if err := os.MkdirAll(nestedDir, 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}

	outer := model.Codebase{
		ID:                  "cb_outer",
		CanonicalPath:       rootDir,
		ResolvedIgnoreRules: discovery.IgnoreRules{Nodes: nil},
	}
	inner := model.Codebase{
		ID:                  "cb_inner",
		CanonicalPath:       nestedDir,
		ResolvedIgnoreRules: discovery.IgnoreRules{Nodes: nil},
	}
	watcher.AddCodebase(context.Background(), outer)
	watcher.AddCodebase(context.Background(), inner)

	leaf := filepath.Join(nestedDir, "file.go")
	if err := os.WriteFile(leaf, []byte{}, 0o644); err != nil {
		t.Fatalf("write leaf: %v", err)
	}
	watcher.dispatch(stubNotifyEvent{path: leaf})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		observedMu.Lock()
		bothObserved := len(observed["cb_outer"]) > 0 && len(observed["cb_inner"]) > 0
		observedMu.Unlock()
		if bothObserved {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	observedMu.Lock()
	defer observedMu.Unlock()
	t.Fatalf("dispatch did not broadcast to both codebases: %v", observed)
}
