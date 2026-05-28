package daemon

import (
	"sort"
	"testing"
	"time"
)

// TestEventQueueCoalescesBurst proves a burst of events for one codebase
// collapses to a single drain carrying the deduplicated path set.
func TestEventQueueCoalescesBurst(t *testing.T) {
	t.Parallel()

	drained := make(chan []string, 4)
	queue := NewEventQueue(30*time.Millisecond, func(_ string, relativePaths []string) {
		drained <- relativePaths
	})

	for range 10 {
		queue.Enqueue("cb1", "a.go")
	}
	queue.Enqueue("cb1", "b.go")

	select {
	case paths := <-drained:
		sort.Strings(paths)
		if len(paths) != 2 || paths[0] != "a.go" || paths[1] != "b.go" {
			t.Fatalf("coalesced paths = %v, want [a.go b.go]", paths)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("drain was not called within the timeout")
	}
}

// TestEventQueueSeparatesCodebases proves events for different codebases drain
// independently with their own path sets.
func TestEventQueueSeparatesCodebases(t *testing.T) {
	t.Parallel()

	type drain struct {
		codebaseID string
		paths      []string
	}
	drained := make(chan drain, 4)
	queue := NewEventQueue(30*time.Millisecond, func(codebaseID string, relativePaths []string) {
		drained <- drain{codebaseID: codebaseID, paths: relativePaths}
	})

	queue.Enqueue("cb1", "a.go")
	queue.Enqueue("cb2", "x.go")

	seen := map[string]int{}
	for range 2 {
		select {
		case event := <-drained:
			seen[event.codebaseID] = len(event.paths)
		case <-time.After(2 * time.Second):
			t.Fatal("expected two independent drains")
		}
	}
	if seen["cb1"] != 1 || seen["cb2"] != 1 {
		t.Fatalf("per-codebase drains = %v, want cb1:1 cb2:1", seen)
	}
}
