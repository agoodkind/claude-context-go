package daemon

import (
	"context"
	"sync"
	"testing"

	"goodkind.io/claude-context-go/internal/model"
)

// recordingLifecycleHook captures the codebases the manager hands to the
// watcher so an adoption test can confirm the adopted codebase starts being
// watched.
type recordingLifecycleHook struct {
	mu    sync.Mutex
	added []string
}

func (hook *recordingLifecycleHook) AddCodebase(_ context.Context, codebase model.Codebase) {
	hook.mu.Lock()
	defer hook.mu.Unlock()
	hook.added = append(hook.added, codebase.ID)
}

func (hook *recordingLifecycleHook) RemoveCodebase(_ context.Context, _ string) {}

func (hook *recordingLifecycleHook) wasAdded(id string) bool {
	hook.mu.Lock()
	defer hook.mu.Unlock()
	for _, added := range hook.added {
		if added == id {
			return true
		}
	}
	return false
}

// TestGetIndexAdoptsUnregisteredCollection proves a path whose Milvus collection
// exists but which has no registry entry is adopted as a persisted, watched
// first-class codebase with a stable id, rather than synthesized per call.
func TestGetIndexAdoptsUnregisteredCollection(t *testing.T) {
	manager, _ := newTestManagerWithCap(t, 2)
	manager.semantic = &fakeSemantic{reindex: nil, copyChunks: nil}
	hook := &recordingLifecycleHook{mu: sync.Mutex{}, added: nil}
	manager.SetCodebaseLifecycleHook(hook)

	canonical := newCapTestRepo(t)

	first, _, found, _, err := manager.GetIndex(context.Background(), canonical)
	if err != nil {
		t.Fatalf("GetIndex returned error: %v", err)
	}
	if !found {
		t.Fatal("expected found=true for a path whose collection exists")
	}
	if first.ID == "" {
		t.Fatal("adopted codebase has an empty id")
	}
	if first.Status != model.CodebaseStatusIndexed {
		t.Fatalf("adopted status = %q, want %q", first.Status, model.CodebaseStatusIndexed)
	}

	second, _, _, _, err := manager.GetIndex(context.Background(), canonical)
	if err != nil {
		t.Fatalf("second GetIndex returned error: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("codebase id changed between calls: %q then %q (adoption must persist a stable id)", first.ID, second.ID)
	}

	indexes := manager.ListIndexes(context.Background())
	present := false
	for _, codebase := range indexes {
		if codebase.ID == first.ID {
			present = true
			break
		}
	}
	if !present {
		t.Fatal("adopted codebase is absent from ListIndexes")
	}
	if !hook.wasAdded(first.ID) {
		t.Fatal("adopted codebase was not handed to the watcher")
	}

	// The adoption refresh sync runs in a detached goroutine. Wait for it to
	// complete (LastSuccessfulRun is set only when the job finishes) so its
	// goroutine stops touching the repo before t.Cleanup removes the temp dirs.
	waitForCondition(t, func() bool {
		codebase, _, ok, _, getErr := manager.GetIndex(context.Background(), canonical)
		return getErr == nil && ok && codebase.LastSuccessfulRun != nil
	})
}
