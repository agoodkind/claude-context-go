package daemon

import (
	"sync"
	"time"
)

// EventQueue coalesces filesystem change events into debounced per-codebase
// batches of changed relative paths. A burst of events for one path collapses
// to a single entry, and the recorded op does not matter because the converge
// task reads disk when it runs: a path present at that moment is upserted and
// a path absent is deleted. The queue therefore tracks only the set of paths
// that changed, and delete-supersedes-reindex falls out of reading disk late.
type EventQueue struct {
	debounce time.Duration
	drain    func(codebaseID string, relativePaths []string)

	mu      sync.Mutex
	pending map[string]map[string]struct{}
	timers  map[string]*time.Timer
}

// NewEventQueue returns a queue that calls drain with the coalesced path set
// for a codebase once debounce elapses with no further events for it.
func NewEventQueue(debounce time.Duration, drain func(codebaseID string, relativePaths []string)) *EventQueue {
	return &EventQueue{
		debounce: debounce,
		drain:    drain,
		mu:       sync.Mutex{},
		pending:  make(map[string]map[string]struct{}),
		timers:   make(map[string]*time.Timer),
	}
}

// Enqueue records that relativePath changed under codebaseID and (re)starts
// the codebase debounce timer.
func (queue *EventQueue) Enqueue(codebaseID string, relativePath string) {
	queue.mu.Lock()
	defer queue.mu.Unlock()

	paths, found := queue.pending[codebaseID]
	if !found {
		paths = make(map[string]struct{})
		queue.pending[codebaseID] = paths
	}
	paths[relativePath] = struct{}{}

	if timer, found := queue.timers[codebaseID]; found {
		timer.Stop()
	}
	queue.timers[codebaseID] = time.AfterFunc(queue.debounce, func() {
		queue.flush(codebaseID)
	})
}

func (queue *EventQueue) flush(codebaseID string) {
	queue.mu.Lock()
	paths := queue.pending[codebaseID]
	delete(queue.pending, codebaseID)
	delete(queue.timers, codebaseID)
	queue.mu.Unlock()

	if len(paths) == 0 {
		return
	}
	relativePaths := make([]string, 0, len(paths))
	for relativePath := range paths {
		relativePaths = append(relativePaths, relativePath)
	}
	queue.drain(codebaseID, relativePaths)
}
