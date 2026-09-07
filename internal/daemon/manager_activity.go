package daemon

import (
	"sort"
	"time"
)

// SetWatcherActivityReporter installs the reporter a status read uses to reach
// file-change work. The background syncer calls it at start. Setting nil clears
// it, which leaves a status read reporting registered jobs only.
func (manager *Manager) SetWatcherActivityReporter(reporter WatcherActivityReporter) {
	manager.watcherActivityMutex.Lock()
	defer manager.watcherActivityMutex.Unlock()
	manager.watcherActivity = reporter
}

// WatcherActivity reports the syncer's raw file-change admission and queued
// paths, or nothing when no syncer is installed. StatusSnapshot filters an
// admitted entry after the matching converge job is registered.
func (manager *Manager) WatcherActivity() []WatcherActivity {
	manager.watcherActivityMutex.Lock()
	reporter := manager.watcherActivity
	manager.watcherActivityMutex.Unlock()
	if reporter == nil {
		return nil
	}
	return reporter.WatcherActivity()
}

// IndexSlots reports how many concurrently running index jobs hold a slot and
// how many slots exist. A job that cannot take one stays queued, so the pair is
// what explains a queued job that waits on no dependency.
func (manager *Manager) IndexSlots() (int, int) {
	snapshot := manager.jobScheduler.Snapshot()
	inUse := 0
	for _, count := range snapshot.Running {
		inUse += count
	}
	return inUse, snapshot.Capacity
}

// StartedAt reports when this daemon process built its manager, so a caller
// derives uptime from the process rather than from its own clock.
func (manager *Manager) StartedAt() time.Time {
	return manager.startedAt
}

// PendingWork is one coalesced request waiting for the codebase's active job to
// finish. It holds no job id because the depth-1 slot only becomes a job when
// drainPendingJobLocked runs, so until then the job store cannot report it.
type PendingWork struct {
	CodebaseID    string
	CanonicalPath string
	Operation     string
}

// Operations a pending slot can hold, named for what will run when it drains.
const (
	pendingOperationSync               = "sync"
	pendingOperationConversationIngest = "conversation_ingest"
)

// PendingWork reports the coalesced requests waiting on a terminal transition.
// They are real queued work: a caller has asked for an index or an upsert and
// the daemon has accepted it, but it holds no job record until it drains, so
// without this a status read shows one row while two units are outstanding.
func (manager *Manager) PendingWork() []PendingWork {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return manager.pendingWorkLocked()
}

// pendingWorkLocked builds the pending list. It is separate from PendingWork so
// a caller reading a wider snapshot sees the slots and the job store at one
// instant, which is what keeps a draining slot from falling between the two.
// Caller holds manager.mu.
func (manager *Manager) pendingWorkLocked() []PendingWork {
	pending := make([]PendingWork, 0, len(manager.pendingCodeJobs)+len(manager.pendingConversationJobs))
	for codebaseID := range manager.pendingCodeJobs {
		pending = append(pending, PendingWork{
			CodebaseID:    codebaseID,
			CanonicalPath: manager.codebases[codebaseID].CanonicalPath,
			Operation:     pendingOperationSync,
		})
	}
	for codebaseID := range manager.pendingConversationJobs {
		pending = append(pending, PendingWork{
			CodebaseID:    codebaseID,
			CanonicalPath: manager.codebases[codebaseID].CanonicalPath,
			Operation:     pendingOperationConversationIngest,
		})
	}
	sort.Slice(pending, func(first int, second int) bool {
		if pending[first].CodebaseID != pending[second].CodebaseID {
			return pending[first].CodebaseID < pending[second].CodebaseID
		}
		return pending[first].Operation < pending[second].Operation
	})
	return pending
}
