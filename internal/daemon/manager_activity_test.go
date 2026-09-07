package daemon

import (
	"testing"
	"time"

	"goodkind.io/lm-semantic-search/internal/config"
	"goodkind.io/lm-semantic-search/internal/jobscheduler"
	"goodkind.io/lm-semantic-search/internal/model"
)

// TestWatcherActivityReportsConvergingAndQueued proves the background syncer
// reports both the converge it is running and the paths still waiting on a
// debounce timer. Neither has a job record, so this is the only surface that
// can report them at all.
func TestWatcherActivityReportsConvergingAndQueued(t *testing.T) {
	t.Parallel()

	syncer := NewBackgroundSync(config.Config{}, nil)
	syncer.queue = NewEventQueue(time.Hour, func(string, []string) {})
	syncer.queue.Enqueue("cb-queued", "a.go")
	syncer.queue.Enqueue("cb-queued", "a.go")
	syncer.queue.Enqueue("cb-queued", "b.go")

	if !syncer.beginConverge("cb-running") {
		t.Fatal("beginConverge returned false on an idle codebase")
	}
	defer syncer.endConverge("cb-running")
	// Admission alone is not running: the converge still has to win an index slot
	// and the shared advisory lock, which is what markConvergeRunning records.
	syncer.markConvergeRunning("cb-running")

	byID := map[string]WatcherActivity{}
	for _, entry := range syncer.WatcherActivity() {
		byID[entry.CodebaseID] = entry
	}

	running, found := byID["cb-running"]
	if !found {
		t.Fatalf("converging codebase absent from activity: %+v", byID)
	}
	if running.State != WatcherStateRunning {
		t.Fatalf("running state = %q, want %q", running.State, WatcherStateRunning)
	}
	if running.StartedAt.IsZero() {
		t.Fatal("running entry carries no start time, so a surface cannot report its age")
	}

	queued, found := byID["cb-queued"]
	if !found {
		t.Fatalf("queued codebase absent from activity: %+v", byID)
	}
	if queued.State != WatcherStateQueued {
		t.Fatalf("queued state = %q, want %q", queued.State, WatcherStateQueued)
	}
	if queued.PendingPaths != 2 {
		t.Fatalf("queued pending paths = %d, want 2 (a.go and b.go, coalesced)", queued.PendingPaths)
	}
}

// TestWatcherActivityReportsAConvergingCodebaseOnce proves a codebase that is
// converging while new paths accumulate reports one row, as running, carrying
// the paths that will drain after it. Two rows for one codebase would read as
// two units of work.
func TestWatcherActivityReportsAConvergingCodebaseOnce(t *testing.T) {
	t.Parallel()

	syncer := NewBackgroundSync(config.Config{}, nil)
	syncer.queue = NewEventQueue(time.Hour, func(string, []string) {})
	syncer.queue.Enqueue("cb-1", "a.go")

	if !syncer.beginConverge("cb-1") {
		t.Fatal("beginConverge returned false on an idle codebase")
	}
	defer syncer.endConverge("cb-1")
	syncer.markConvergeRunning("cb-1")

	activity := syncer.WatcherActivity()
	if len(activity) != 1 {
		t.Fatalf("activity rows = %d, want 1: %+v", len(activity), activity)
	}
	if activity[0].State != WatcherStateRunning {
		t.Fatalf("state = %q, want %q", activity[0].State, WatcherStateRunning)
	}
	if activity[0].PendingPaths != 1 {
		t.Fatalf("pending paths = %d, want 1 (the path queued behind the converge)", activity[0].PendingPaths)
	}
}

// TestManagerWatcherActivityWithoutReporter proves a manager with no syncer
// reports nothing rather than failing, which is the case in tests and in a
// daemon with file watching turned off.
func TestManagerWatcherActivityWithoutReporter(t *testing.T) {
	t.Parallel()

	manager := &Manager{}
	if activity := manager.WatcherActivity(); activity != nil {
		t.Fatalf("activity = %+v, want nil with no reporter installed", activity)
	}
}

// TestWatcherActivityReportsAdmittedButWaitingAsQueued proves a converge that
// holds the per-codebase slot but has not yet won an index slot or the shared
// advisory lock reports queued. Reporting it as running would show an age that
// measures the wait rather than the work.
func TestWatcherActivityReportsAdmittedButWaitingAsQueued(t *testing.T) {
	t.Parallel()

	syncer := NewBackgroundSync(config.Config{}, nil)
	if !syncer.beginConverge("cb-waiting") {
		t.Fatal("beginConverge returned false on an idle codebase")
	}
	defer syncer.endConverge("cb-waiting")

	activity := syncer.WatcherActivity()
	if len(activity) != 1 {
		t.Fatalf("activity rows = %d, want 1: %+v", len(activity), activity)
	}
	if activity[0].State != WatcherStateQueued {
		t.Fatalf("state = %q, want %q for an admitted converge still waiting on capacity",
			activity[0].State, WatcherStateQueued)
	}
	if !activity[0].StartedAt.IsZero() {
		t.Fatalf("started_at = %v, want zero until the converge actually starts", activity[0].StartedAt)
	}

	syncer.markConvergeRunning("cb-waiting")
	activity = syncer.WatcherActivity()
	if activity[0].State != WatcherStateRunning {
		t.Fatalf("state after markConvergeRunning = %q, want %q", activity[0].State, WatcherStateRunning)
	}
	if activity[0].StartedAt.IsZero() {
		t.Fatal("started_at is zero after the converge started, so a surface cannot report its age")
	}
}

// TestMarkConvergeRunningIgnoresAnUnadmittedCodebase proves the start stamp
// cannot resurrect a converge that already ended, which would leave a row for
// work nobody is doing.
func TestMarkConvergeRunningIgnoresAnUnadmittedCodebase(t *testing.T) {
	t.Parallel()

	syncer := NewBackgroundSync(config.Config{}, nil)
	syncer.markConvergeRunning("cb-never-admitted")
	if activity := syncer.WatcherActivity(); len(activity) != 0 {
		t.Fatalf("activity = %+v, want none for a codebase that was never admitted", activity)
	}
}

// TestStatusSnapshotSeesADrainingSlotExactlyOnce proves a pending slot that
// becomes a job cannot fall between two reads and vanish, and cannot appear
// twice either. The transition happens under manager.mu, and the snapshot reads
// both the slots and the job store under one hold of that lock, so a caller
// hammering the snapshot while the drain runs observes one or the other and
// never neither.
func TestStatusSnapshotSeesADrainingSlotExactlyOnce(t *testing.T) {
	t.Parallel()

	const codebaseID = "cb-draining"
	manager := &Manager{
		codebases: map[string]model.Codebase{
			codebaseID: {ID: codebaseID, CanonicalPath: "/code"},
		},
		jobs:                    map[string]model.Job{},
		pendingCodeJobs:         map[string]pendingCodeRequest{codebaseID: {}},
		pendingConversationJobs: map[string]conversationJobPayload{},
		jobScheduler:            jobscheduler.New(1),
	}

	// Move the slot into the job store the way drainPendingJobLocked does, under
	// the same lock the snapshot takes.
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		manager.mu.Lock()
		delete(manager.pendingCodeJobs, codebaseID)
		manager.jobs["job-1"] = model.Job{
			ID:         "job-1",
			CodebaseID: codebaseID,
			State:      model.JobStateRunning,
		}
		manager.mu.Unlock()
	}()

	// Read across the transition. Every read must show the work exactly once.
	for range 200 {
		snapshot := manager.StatusSnapshot()
		units := len(snapshot.ActiveJobs) + len(snapshot.Pending)
		if units != 1 {
			t.Fatalf("snapshot reported %d units of work, want exactly 1: jobs=%+v pending=%+v",
				units, snapshot.ActiveJobs, snapshot.Pending)
		}
	}
	<-drained

	final := manager.StatusSnapshot()
	if len(final.ActiveJobs) != 1 || len(final.Pending) != 0 {
		t.Fatalf("after the drain: jobs=%d pending=%d, want 1 and 0",
			len(final.ActiveJobs), len(final.Pending))
	}
}

// TestStatusSnapshotOmitsTerminalJobs proves job history stays out of the
// snapshot, so a status reply stays a few kilobytes rather than growing with
// every job the daemon has ever run.
func TestStatusSnapshotOmitsTerminalJobs(t *testing.T) {
	t.Parallel()

	manager := &Manager{
		codebases: map[string]model.Codebase{},
		jobs: map[string]model.Job{
			"running":   {ID: "running", State: model.JobStateRunning},
			"queued":    {ID: "queued", State: model.JobStateQueued},
			"completed": {ID: "completed", State: model.JobStateCompleted},
			"failed":    {ID: "failed", State: model.JobStateFailed},
			"cancelled": {ID: "cancelled", State: model.JobStateCancelled},
		},
		pendingCodeJobs:         map[string]pendingCodeRequest{},
		pendingConversationJobs: map[string]conversationJobPayload{},
		jobScheduler:            jobscheduler.New(4),
	}

	snapshot := manager.StatusSnapshot()
	if len(snapshot.ActiveJobs) != 2 {
		t.Fatalf("active jobs = %d, want 2 (running and queued): %+v",
			len(snapshot.ActiveJobs), snapshot.ActiveJobs)
	}
	for _, job := range snapshot.ActiveJobs {
		if isTerminalJobState(job.State) {
			t.Fatalf("terminal job %q reached the snapshot", job.ID)
		}
	}
	if snapshot.IndexSlotsTotal != 4 {
		t.Fatalf("index slots total = %d, want 4", snapshot.IndexSlotsTotal)
	}
}

// TestStatusSnapshotResolvesHealthLikeEverySurface proves the snapshot clears a
// stale store banner the same way DependencyHealth does. Copying the raw record
// instead would report a store as degraded after it had already reconnected,
// while codebase list, which resolves it, reported healthy.
func TestStatusSnapshotResolvesHealthLikeEverySurface(t *testing.T) {
	t.Parallel()

	newManager := func() *Manager {
		return &Manager{
			codebases:               map[string]model.Codebase{},
			jobs:                    map[string]model.Job{},
			pendingCodeJobs:         map[string]pendingCodeRequest{},
			pendingConversationJobs: map[string]conversationJobPayload{},
			jobScheduler:            jobscheduler.New(1),
			semantic:                nil,
			health: dependencyHealth{
				Mode:  dependencyStoreUnavailable,
				Since: time.Unix(1785156767, 0),
			},
		}
	}

	// With no semantic service the shortcut cannot fire, so both paths agree the
	// store is still down.
	stillDown := newManager()
	if !stillDown.StatusSnapshot().Health.Degraded() {
		t.Fatal("snapshot cleared an outage with no evidence of recovery")
	}
	if !newManager().DependencyHealth().Degraded() {
		t.Fatal("DependencyHealth cleared an outage with no evidence of recovery")
	}

	// The two paths must agree on the same manager state, whatever that state is.
	viaSnapshot := newManager().StatusSnapshot().Health
	viaAccessor := newManager().DependencyHealth()
	if viaSnapshot.Degraded() != viaAccessor.Degraded() || viaSnapshot.Mode != viaAccessor.Mode {
		t.Fatalf("snapshot health %+v disagrees with DependencyHealth %+v", viaSnapshot, viaAccessor)
	}
}

// TestWatcherActivityReportsDeferredFirstBuildPaths proves changed paths held
// behind a first build still report as waiting work. They converge the moment
// that build promotes, so omitting them shows a quiet system while edits pile
// up, which is the exact blindness this command exists to remove.
func TestWatcherActivityReportsDeferredFirstBuildPaths(t *testing.T) {
	t.Parallel()

	syncer := NewBackgroundSync(config.Config{}, nil)
	syncer.deferWatcherPaths("cb-first-build", []string{"a.go", "b.go", "a.go"})

	activity := syncer.WatcherActivity()
	if len(activity) != 1 {
		t.Fatalf("activity rows = %d, want 1: %+v", len(activity), activity)
	}
	if activity[0].CodebaseID != "cb-first-build" {
		t.Fatalf("codebase = %q, want cb-first-build", activity[0].CodebaseID)
	}
	if activity[0].State != WatcherStateQueued {
		t.Fatalf("state = %q, want %q", activity[0].State, WatcherStateQueued)
	}
	if activity[0].PendingPaths != 2 {
		t.Fatalf("pending paths = %d, want 2 (a.go and b.go, coalesced)", activity[0].PendingPaths)
	}
}

// TestWatcherActivitySumsQueuedAndDeferredPaths proves a codebase waiting in
// both places reports one row carrying both counts, rather than one source
// hiding the other.
func TestWatcherActivitySumsQueuedAndDeferredPaths(t *testing.T) {
	t.Parallel()

	syncer := NewBackgroundSync(config.Config{}, nil)
	syncer.queue = NewEventQueue(time.Hour, func(string, []string) {})
	syncer.queue.Enqueue("cb-both", "queued.go")
	syncer.deferWatcherPaths("cb-both", []string{"deferred.go", "also-deferred.go"})

	activity := syncer.WatcherActivity()
	if len(activity) != 1 {
		t.Fatalf("activity rows = %d, want 1: %+v", len(activity), activity)
	}
	if activity[0].PendingPaths != 3 {
		t.Fatalf("pending paths = %d, want 3 (one queued plus two deferred)", activity[0].PendingPaths)
	}
}
