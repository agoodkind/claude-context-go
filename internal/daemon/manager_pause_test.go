package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"goodkind.io/lm-semantic-search/internal/indexer"
	"goodkind.io/lm-semantic-search/internal/jobscheduler"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/store"
)

const pauseTestTimeout = 5 * time.Second

type pauseAcquireResult struct {
	lease *jobscheduler.Lease
	err   error
}

func TestPriorityPauseFinishesCurrentFileBeforeRelease(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	job := seedPauseTestJob(t, manager, repoPath, model.CodebaseKindCode)
	manager.semantic = &fakeSemantic{
		reindex: func(context.Context, string, []model.StoredChunk, []string) error {
			return nil
		},
	}

	entered := make(chan struct{})
	releaseFile := make(chan struct{})
	manager.runner = fakeRunner{
		indexOne: func(context.Context, string, string, model.IndexConfig) (indexer.OneFileResult, error) {
			close(entered)
			<-releaseFile
			return indexer.OneFileResult{
				Chunks:   []model.StoredChunk{{Content: "package pause"}},
				FileHash: "hash",
			}, nil
		},
	}
	path := filepath.Join(repoPath, "pause.go")
	if err := os.WriteFile(path, []byte("package pause\n"), 0o644); err != nil {
		t.Fatalf("write pause.go: %v", err)
	}

	scheduler := jobscheduler.New(1)
	lowLease := acquirePauseTestLease(t, scheduler, job.ID, model.JobPriorityLow, 1)
	defer lowLease.Release()
	highCancel, highResult := startPauseTestAcquire(scheduler, "job-high", model.JobPriorityHigh, 2)
	defer highCancel()
	waitPauseTestRequested(t, lowLease)

	ctx := withJobSchedulerLease(context.Background(), job.ID, lowLease)
	convergeDone := make(chan error, 1)
	go func() {
		_, err := manager.ConvergePaths(ctx, job.CodebaseID, []string{"pause.go"}, nil)
		convergeDone <- err
	}()
	waitPauseTestSignal(t, entered)

	if snapshot := scheduler.Snapshot(); snapshot.Running[model.JobPriorityLow] != 1 {
		t.Fatal("scheduler released the low job before its file completed")
	}
	close(releaseFile)
	highLease := receivePauseTestLease(t, highResult)
	paused := waitPauseTestJobState(t, manager, job.ID, model.JobStatePaused)
	if paused.ID != job.ID {
		t.Fatalf("paused job id = %q, want %q", paused.ID, job.ID)
	}
	highLease.Release()
	if err := receivePauseTestError(t, convergeDone); err != nil {
		t.Fatalf("ConvergePaths: %v", err)
	}
	waitPauseTestJobState(t, manager, job.ID, model.JobStateRunning)
}

func TestPausedJobReleasesLeaseAndResumesSameJob(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	job := seedPauseTestJob(t, manager, repoPath, model.CodebaseKindCode)
	scheduler := jobscheduler.New(1)
	lease := acquirePauseTestLease(t, scheduler, job.ID, model.JobPriorityLow, 1)
	defer lease.Release()
	highCancel, highResult := startPauseTestAcquire(scheduler, "job-high", model.JobPriorityHigh, 2)
	defer highCancel()
	waitPauseTestRequested(t, lease)

	checkpointDone := make(chan error, 1)
	go func() {
		ctx := withJobSchedulerLease(context.Background(), job.ID, lease)
		checkpointDone <- manager.checkpointJob(ctx)
	}()
	highLease := receivePauseTestLease(t, highResult)
	waitPauseTestJobState(t, manager, job.ID, model.JobStatePaused)
	highLease.Release()
	if err := receivePauseTestError(t, checkpointDone); err != nil {
		t.Fatalf("checkpointJob: %v", err)
	}
	resumed := waitPauseTestJobState(t, manager, job.ID, model.JobStateRunning)
	if resumed.ID != job.ID {
		t.Fatalf("resumed job id = %q, want %q", resumed.ID, job.ID)
	}
}

func TestResumeJobRejectsTerminalState(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	job := seedPauseTestJob(t, manager, repoPath, model.CodebaseKindCode)
	manager.updateJobCancelled(context.Background(), job.ID)

	if err := manager.resumeJob(job.ID); err == nil {
		t.Fatal("resumeJob accepted a terminal job")
	}
	terminal := waitPauseTestJobState(t, manager, job.ID, model.JobStateCancelled)
	if terminal.State != model.JobStateCancelled {
		t.Fatalf("terminal state = %q, want cancelled", terminal.State)
	}
}

func TestWatchdogPriorityRaceDoesNotDoubleRelease(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	originalAppendTransition := manager.appendJobTransition
	transitionMutex := sync.Mutex{}
	transitionCounts := map[string]int{}
	manager.appendJobTransition = func(event model.JobEvent) error {
		if err := originalAppendTransition(event); err != nil {
			return err
		}
		transitionMutex.Lock()
		transitionCounts[event.Event]++
		transitionMutex.Unlock()
		return nil
	}
	job := seedPauseTestJob(t, manager, repoPath, model.CodebaseKindCode)
	job.EffectiveSchedulingPolicy.Priority = model.JobPriorityLow
	job.QueueSequence = 1
	manager.mu.Lock()
	manager.jobs[job.ID] = job
	manager.mu.Unlock()

	capacity, outcome, err := manager.acquireJobCapacity(context.Background(), job, true)
	if err != nil || outcome != syncLockAcquired {
		t.Fatalf("acquireJobCapacity outcome = %q, err = %v", outcome, err)
	}
	defer capacity.release(context.Background())
	ctx := withJobCapacity(context.Background(), capacity)
	ctx = withJobSchedulerLease(ctx, job.ID, capacity.lease)
	manager.jobCapacityTimings.ReleaseGrace = 10 * time.Millisecond

	highCancel, highResult := startPauseTestAcquire(
		manager.jobScheduler,
		"job-watchdog-high",
		model.JobPriorityHigh,
		2,
	)
	defer highCancel()
	waitPauseTestRequested(t, capacity.lease)

	operationStarted := make(chan struct{})
	releaseOperation := make(chan struct{})
	stalledDone := make(chan error, 1)
	go func() {
		stalledDone <- manager.runReleasingCapacityIfStalled(ctx, func() error {
			close(operationStarted)
			<-releaseOperation
			return nil
		})
	}()
	<-operationStarted

	checkpointDone := make(chan error, 1)
	go func() {
		checkpointDone <- manager.checkpointJob(ctx)
	}()
	highLease := receivePauseTestLease(t, highResult)
	close(releaseOperation)
	highLease.Release()
	if err := receivePauseTestError(t, checkpointDone); err != nil {
		t.Fatalf("checkpointJob: %v", err)
	}
	if err := receivePauseTestError(t, stalledDone); err != nil {
		t.Fatalf("runReleasingCapacityIfStalled: %v", err)
	}
	waitPauseTestJobState(t, manager, job.ID, model.JobStateRunning)

	snapshot := manager.jobScheduler.Snapshot()
	if snapshot.Running[model.JobPriorityLow] != 1 ||
		snapshot.Queued[model.JobPriorityLow] != 0 ||
		snapshot.Paused[model.JobPriorityLow] != 0 ||
		snapshot.Yields != 1 {
		t.Fatalf("scheduler snapshot after watchdog race = %+v", snapshot)
	}
	transitionMutex.Lock()
	pausedCount := transitionCounts["job_paused"]
	resumedCount := transitionCounts["job_resumed"]
	transitionMutex.Unlock()
	if pausedCount != 1 || resumedCount != 1 {
		t.Fatalf("pause/resume journal counts = %d/%d, want 1/1", pausedCount, resumedCount)
	}
}

func TestPauseJournalFailureTerminatesBeforeRelease(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	job := seedPauseTestJob(t, manager, repoPath, model.CodebaseKindCode)
	manager.appendJobTransition = func(model.JobEvent) error {
		return errors.New("pause barrier failed")
	}
	scheduler := jobscheduler.New(1)
	lease := acquirePauseTestLease(t, scheduler, job.ID, model.JobPriorityLow, 1)
	highCancel, highResult := startPauseTestAcquire(scheduler, "job-high", model.JobPriorityHigh, 2)
	defer highCancel()
	waitPauseTestRequested(t, lease)

	ctx := withJobSchedulerLease(context.Background(), job.ID, lease)
	if err := manager.checkpointJob(ctx); err == nil {
		t.Fatal("checkpointJob returned nil after pause journal failure")
	}
	failed := waitPauseTestJobState(t, manager, job.ID, model.JobStateFailed)
	if failed.Error == nil || failed.Error.Code != pauseJournalFailureCode {
		t.Fatalf("failure = %+v, want code %q", failed.Error, pauseJournalFailureCode)
	}
	highLease := receivePauseTestLease(t, highResult)
	highLease.Release()
	if snapshot := scheduler.Snapshot(); snapshot.Paused[model.JobPriorityLow] != 0 {
		t.Fatal("pause journal failure retained the scheduler lease")
	}
}

func TestResumeJournalFailureTerminatesAndReleasesLease(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	job := seedPauseTestJob(t, manager, repoPath, model.CodebaseKindCode)
	manager.appendJobTransition = func(event model.JobEvent) error {
		if event.Event == "job_resumed" {
			return errors.New("resume barrier failed")
		}
		return nil
	}
	scheduler := jobscheduler.New(1)
	lease := acquirePauseTestLease(t, scheduler, job.ID, model.JobPriorityLow, 1)
	highCancel, highResult := startPauseTestAcquire(scheduler, "job-high", model.JobPriorityHigh, 2)
	defer highCancel()
	waitPauseTestRequested(t, lease)

	checkpointDone := make(chan error, 1)
	go func() {
		ctx := withJobSchedulerLease(context.Background(), job.ID, lease)
		checkpointDone <- manager.checkpointJob(ctx)
	}()
	highLease := receivePauseTestLease(t, highResult)
	highLease.Release()
	if err := receivePauseTestError(t, checkpointDone); err == nil {
		t.Fatal("checkpointJob returned nil after resume journal failure")
	}
	failed := waitPauseTestJobState(t, manager, job.ID, model.JobStateFailed)
	if failed.Error == nil || failed.Error.Code != resumeJournalFailureCode {
		t.Fatalf("failure = %+v, want code %q", failed.Error, resumeJournalFailureCode)
	}
	snapshot := scheduler.Snapshot()
	if snapshot.Running[model.JobPriorityLow] != 0 || snapshot.Paused[model.JobPriorityLow] != 0 {
		t.Fatal("resume journal failure retained scheduler capacity")
	}
}

func TestWatcherPauseBarrierFailurePreservesCodebase(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	job := seedDetachedPauseTestJob(t, manager, repoPath)
	manager.appendJobTransition = func(event model.JobEvent) error {
		if event.Event == "job_paused" {
			return errors.New("watcher pause barrier failed")
		}
		return nil
	}
	lease := acquirePauseTestLease(
		t,
		manager.jobScheduler,
		job.ID,
		model.JobPriorityLow,
		1,
	)
	highCancel, highResult := startPauseTestAcquire(
		manager.jobScheduler,
		"watcher-pause-high",
		model.JobPriorityHigh,
		2,
	)
	defer highCancel()
	waitPauseTestRequested(t, lease)

	ctx := withJobSchedulerLease(context.Background(), job.ID, lease)
	if err := manager.checkpointJob(ctx); err == nil {
		t.Fatal("checkpointJob returned nil after watcher pause barrier failure")
	}
	highLease := receivePauseTestLease(t, highResult)
	highLease.Release()
	assertDetachedPauseFailure(t, manager, job, pauseJournalFailureCode)
}

func TestWatcherResumeBarrierFailurePreservesCodebase(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	job := seedDetachedPauseTestJob(t, manager, repoPath)
	manager.appendJobTransition = func(event model.JobEvent) error {
		if event.Event == "job_resumed" {
			return errors.New("watcher resume barrier failed")
		}
		return nil
	}
	lease := acquirePauseTestLease(
		t,
		manager.jobScheduler,
		job.ID,
		model.JobPriorityLow,
		1,
	)
	highCancel, highResult := startPauseTestAcquire(
		manager.jobScheduler,
		"watcher-resume-high",
		model.JobPriorityHigh,
		2,
	)
	defer highCancel()
	waitPauseTestRequested(t, lease)

	checkpointDone := make(chan error, 1)
	go func() {
		ctx := withJobSchedulerLease(context.Background(), job.ID, lease)
		checkpointDone <- manager.checkpointJob(ctx)
	}()
	highLease := receivePauseTestLease(t, highResult)
	highLease.Release()
	if err := receivePauseTestError(t, checkpointDone); err == nil {
		t.Fatal("checkpointJob returned nil after watcher resume barrier failure")
	}
	assertDetachedPauseFailure(t, manager, job, resumeJournalFailureCode)
}

func TestCancellationBetweenPauseSnapshotAndJournalStaysTerminal(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	job := seedPauseTestJob(t, manager, repoPath, model.CodebaseKindCode)
	journalEntered := make(chan struct{})
	releaseJournal := make(chan struct{})
	manager.appendJobTransition = func(event model.JobEvent) error {
		if event.Event == "job_paused" {
			close(journalEntered)
			<-releaseJournal
		}
		return nil
	}

	pauseDone := make(chan error, 1)
	go func() {
		pauseDone <- manager.pauseJob(job.ID, "priority")
	}()
	<-journalEntered
	cancelDone := make(chan struct{})
	go func() {
		manager.updateDetachedJobCancelled(context.Background(), job.ID)
		close(cancelDone)
	}()
	close(releaseJournal)
	if err := receivePauseTestError(t, pauseDone); err != nil {
		t.Fatalf("pauseJob: %v", err)
	}
	waitPauseTestSignal(t, cancelDone)
	waitPauseTestJobState(t, manager, job.ID, model.JobStateCancelled)
	if err := manager.pauseJob(job.ID, "stale"); err != nil {
		t.Fatalf("stale pauseJob: %v", err)
	}
	waitPauseTestJobState(t, manager, job.ID, model.JobStateCancelled)
}

func TestConversationTerminalCannotBeFollowedByPause(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	job := seedPauseTestJob(t, manager, repoPath, model.CodebaseKindDocument)
	manager.finishConversationDelete(context.Background(), job.ID)
	if err := manager.pauseJob(job.ID, "stale"); err != nil {
		t.Fatalf("pauseJob: %v", err)
	}
	waitPauseTestJobState(t, manager, job.ID, model.JobStateCompleted)
}

func TestTerminalEventSupersedesPausedJournalState(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	job := seedPauseTestJob(t, manager, repoPath, model.CodebaseKindCode)
	if err := manager.pauseJob(job.ID, "priority"); err != nil {
		t.Fatalf("pauseJob: %v", err)
	}
	if _, err := manager.CancelJob(context.Background(), job.ID); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	manager.closeJobJournal()

	latest, err := store.ReadJobEventsLatest(manager.config.JobsPath)
	if err != nil {
		t.Fatalf("ReadJobEventsLatest: %v", err)
	}
	event, found := latest[job.ID]
	if !found {
		t.Fatalf("latest journal event missing job %s", job.ID)
	}
	if event.Job.State != model.JobStateCancelled {
		t.Fatalf("latest journal state = %q, want %q", event.Job.State, model.JobStateCancelled)
	}
}

func TestQuarantineTerminalCannotBeFollowedByPause(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	job := seedPauseTestJob(t, manager, repoPath, model.CodebaseKindCode)
	manager.updateJobQuarantined(context.Background(), job.ID, quarantineSignal{
		trigger:      "test",
		missingCount: 9,
		totalCount:   10,
	})
	if err := manager.pauseJob(job.ID, "stale"); err != nil {
		t.Fatalf("pauseJob: %v", err)
	}
	waitPauseTestJobState(t, manager, job.ID, model.JobStateFailed)
}

func seedPauseTestJob(
	t *testing.T,
	manager *Manager,
	repoPath string,
	kind model.CodebaseKind,
) model.Job {
	t.Helper()
	codebase := seedConvergeCodebase(t, manager, repoPath)
	codebase.Kind = kind
	codebase.Status = model.CodebaseStatusIndexing
	job := newQueuedJob(
		codebase.ID,
		repoPath,
		repoPath,
		testClientInfo(),
		string(jobOperationSync),
		false,
		codebase.EffectiveConfig,
		emptyAdmissionBudget,
		time.Now(),
	)
	job.State = model.JobStateRunning
	codebase.ActiveJobID = job.ID
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.jobs[job.ID] = job
	manager.mu.Unlock()
	return job
}

func seedDetachedPauseTestJob(
	t *testing.T,
	manager *Manager,
	repoPath string,
) model.Job {
	t.Helper()
	codebase := seedConvergeCodebase(t, manager, repoPath)
	job := newQueuedJob(
		codebase.ID,
		repoPath,
		repoPath,
		model.ClientInfo{Name: "daemon-watcher", PID: 0},
		"converge",
		false,
		codebase.EffectiveConfig,
		emptyAdmissionBudget,
		time.Now(),
	)
	job.State = model.JobStateRunning
	manager.mu.Lock()
	manager.jobs[job.ID] = job
	manager.mu.Unlock()
	return job
}

func assertDetachedPauseFailure(
	t *testing.T,
	manager *Manager,
	job model.Job,
	expectedCode string,
) {
	t.Helper()
	failed := waitPauseTestJobState(t, manager, job.ID, model.JobStateFailed)
	if failed.Error == nil || failed.Error.Code != expectedCode {
		t.Fatalf("failed watcher error = %+v, want code %q", failed.Error, expectedCode)
	}
	manager.mu.Lock()
	codebase := manager.codebases[job.CodebaseID]
	manager.mu.Unlock()
	if codebase.Status != model.CodebaseStatusIndexed ||
		codebase.ActiveJobID != "" ||
		codebase.LastFailedRun != nil {
		t.Fatalf("codebase changed by detached watcher failure: %+v", codebase)
	}
}

func acquirePauseTestLease(
	t *testing.T,
	scheduler *jobscheduler.Scheduler,
	jobID string,
	priority model.JobPriority,
	sequence uint64,
) *jobscheduler.Lease {
	t.Helper()
	lease, err := scheduler.Acquire(
		context.Background(),
		pauseTestSchedulerEntry(jobID, priority, sequence),
	)
	if err != nil {
		t.Fatalf("Acquire %s: %v", jobID, err)
	}
	return lease
}

func startPauseTestAcquire(
	scheduler *jobscheduler.Scheduler,
	jobID string,
	priority model.JobPriority,
	sequence uint64,
) (context.CancelFunc, <-chan pauseAcquireResult) {
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan pauseAcquireResult, 1)
	go func() {
		lease, err := scheduler.Acquire(ctx, pauseTestSchedulerEntry(jobID, priority, sequence))
		result <- pauseAcquireResult{lease: lease, err: err}
	}()
	return cancel, result
}

func pauseTestSchedulerEntry(
	jobID string,
	priority model.JobPriority,
	sequence uint64,
) jobscheduler.Entry {
	policy := model.DefaultSchedulingPolicy()
	policy.Priority = priority
	return jobscheduler.Entry{JobID: jobID, Policy: policy, QueueSequence: sequence}
}

func waitPauseTestRequested(t *testing.T, lease *jobscheduler.Lease) {
	t.Helper()
	deadline := time.Now().Add(pauseTestTimeout)
	for {
		requested, _ := lease.Checkpoint()
		if requested {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for pause request")
		}
		time.Sleep(time.Millisecond)
	}
}

func receivePauseTestLease(
	t *testing.T,
	result <-chan pauseAcquireResult,
) *jobscheduler.Lease {
	t.Helper()
	select {
	case acquired := <-result:
		if acquired.err != nil {
			t.Fatalf("Acquire: %v", acquired.err)
		}
		return acquired.lease
	case <-time.After(pauseTestTimeout):
		t.Fatal("timed out waiting for scheduler lease")
		return nil
	}
}

func receivePauseTestError(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(pauseTestTimeout):
		t.Fatal("timed out waiting for result")
		return nil
	}
}

func waitPauseTestJobState(
	t *testing.T,
	manager *Manager,
	jobID string,
	want model.JobState,
) model.Job {
	t.Helper()
	deadline := time.Now().Add(pauseTestTimeout)
	for {
		job, found := manager.GetJob(jobID)
		if found && job.State == want {
			return job
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for job %s state %s", jobID, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitPauseTestSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(pauseTestTimeout):
		t.Fatal("timed out waiting for signal")
	}
}
