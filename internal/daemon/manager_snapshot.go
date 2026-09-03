package daemon

import (
	"sort"
	"time"

	"goodkind.io/lm-semantic-search/internal/jobscheduler"
	"goodkind.io/lm-semantic-search/internal/model"
)

// StatusSnapshot is everything a status read reports about work and codebases,
// taken so no caller can observe two of these facts from different instants.
//
// A depth-1 pending slot becomes a job, and a running job changes a codebase's
// display status, so reading those through separate accessors lets a transition
// land between two reads: the work shows twice, or in the worse direction, it
// shows in neither and a caller sees nothing outstanding while the daemon is
// busy. One snapshot removes both outcomes rather than choosing between them.
type StatusSnapshot struct {
	// StartedAt is when this daemon process built its manager.
	StartedAt time.Time
	// Scheduler carries capacity, per-priority work counts, and the activity
	// sample cached by the scheduler's background sampler.
	Scheduler jobscheduler.Snapshot
	// Health is the shared-dependency record, resolved the same way every other
	// surface resolves it. It is read once and carried here, because reading it
	// is not pure: it clears a boot-time store banner once the client has
	// reconnected, so two reads in one reply would describe two instants.
	Health dependencyHealth
	// ActiveJobs holds the non-terminal jobs only. Job history is deliberately
	// absent: a status reply stays a few kilobytes rather than growing with it.
	ActiveJobs []model.Job
	// Pending holds the coalesced requests that hold no job record yet.
	Pending []PendingWork
	// Codebases carries each tracked codebase with its resolved display status,
	// so a status count and an activity row cannot disagree about one codebase.
	Codebases []CodebaseView
	// Watcher holds file-change work that has not become a registered converge
	// job. The syncer owns the admission state under its own lock, and the
	// snapshot filters entries that the job store can already report.
	//
	// They are not atomic with Scheduler either, because a running converge holds
	// a scheduler lease and no single lock covers both the syncer's set and the
	// scheduler. The read order makes the skew one-directional: a converge
	// that ends mid-read still shows its row while the slot count has already
	// dropped, so the reply over-reports work for one poll rather than showing an
	// occupied slot no row accounts for.
	Watcher []WatcherActivity
}

// StatusSnapshot reads every fact a status reply needs. The watcher activity is
// gathered first, under the syncer's own locks, so the manager lock is never
// held across a call into the syncer. Everything the manager owns is then read
// under one hold of that lock.
func (manager *Manager) StatusSnapshot() StatusSnapshot {
	watcher := manager.WatcherActivity()

	manager.mu.Lock()
	defer manager.mu.Unlock()

	// Health resolves first because resolving it can clear a stale store banner,
	// and the codebase views fold the resulting mode into every display status.
	// Reading it after them would report a recovered store beside codebases
	// still marked as waiting on it.
	health := manager.dependencyHealthLocked()

	// Sized to the concurrency cap rather than to the job store. The store holds
	// every job this daemon has ever run, tens of thousands of them, and all but
	// a handful are terminal; reserving one slot each would allocate megabytes on
	// every refresh to hold a few live jobs.
	schedulerSnapshot := manager.jobScheduler.Snapshot()
	activeJobs := make([]model.Job, 0, schedulerSnapshot.Capacity)
	for _, job := range manager.jobs {
		if isTerminalJobState(job.State) {
			continue
		}
		job = jobWithSchedulerReason(job, schedulerSnapshot.Reasons)
		activeJobs = append(activeJobs, job)
	}
	sort.Slice(activeJobs, func(first int, second int) bool {
		return activeJobs[first].StartedAt.After(activeJobs[second].StartedAt)
	})
	watcher = watcherActivityWithoutRegisteredConverges(watcher, activeJobs)

	return StatusSnapshot{
		StartedAt:  manager.startedAt,
		Scheduler:  schedulerSnapshot,
		Health:     health,
		ActiveJobs: activeJobs,
		Pending:    manager.pendingWorkLocked(),
		Codebases:  manager.codebaseViewsLocked(),
		Watcher:    watcher,
	}
}

func jobWithSchedulerReason(
	job model.Job,
	reasons map[string]model.SchedulingReason,
) model.Job {
	if job.State != model.JobStateQueued && job.State != model.JobStatePaused {
		job.SchedulingReason = model.SchedulingReasonUnspecified
		return job
	}
	reason, found := reasons[job.ID]
	if found {
		job.SchedulingReason = model.CanonicalSchedulingReason(string(reason))
		return job
	}
	job.SchedulingReason = model.CanonicalSchedulingReason(
		string(job.SchedulingReason),
	)
	return job
}

func watcherActivityWithoutRegisteredConverges(
	watcher []WatcherActivity,
	activeJobs []model.Job,
) []WatcherActivity {
	registered := make(map[string]struct{})
	for _, job := range activeJobs {
		if job.Operation == "converge" {
			registered[job.CodebaseID] = struct{}{}
		}
	}
	if len(registered) == 0 {
		return watcher
	}

	filtered := make([]WatcherActivity, 0, len(watcher))
	for _, activity := range watcher {
		if _, found := registered[activity.CodebaseID]; found {
			continue
		}
		filtered = append(filtered, activity)
	}
	return filtered
}

// CanonicalPaths indexes the snapshot's codebases by id, so a watcher or
// pending row can name its path from the same instant the rest of the reply
// describes.
func (snapshot StatusSnapshot) CanonicalPaths() map[string]string {
	paths := make(map[string]string, len(snapshot.Codebases))
	for _, view := range snapshot.Codebases {
		paths[view.Codebase.ID] = view.Codebase.CanonicalPath
	}
	return paths
}
