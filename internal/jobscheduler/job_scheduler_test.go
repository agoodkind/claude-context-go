package jobscheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"goodkind.io/lm-semantic-search/internal/model"
)

const schedulerTestTimeout = time.Second

type schedulerAcquireResult struct {
	lease *Lease
	err   error
}

func TestSchedulerFillsFourSlotsByWaitingPriority(t *testing.T) {
	scheduler := New(4)
	leases := []*Lease{
		acquireSchedulerLease(t, scheduler, "high-1", model.JobPriorityHigh, 1),
		acquireSchedulerLease(t, scheduler, "high-2", model.JobPriorityHigh, 2),
		acquireSchedulerLease(t, scheduler, "normal-1", model.JobPriorityNormal, 3),
		acquireSchedulerLease(t, scheduler, "normal-2", model.JobPriorityNormal, 4),
	}
	defer releaseSchedulerLeases(leases)

	lowOneCancel, lowOneResult := startSchedulerAcquire(
		scheduler,
		"low-1",
		model.JobPriorityLow,
		5,
	)
	defer lowOneCancel()
	lowTwoCancel, lowTwoResult := startSchedulerAcquire(
		scheduler,
		"low-2",
		model.JobPriorityLow,
		6,
	)
	defer lowTwoCancel()
	waitForSchedulerCounts(t, scheduler, model.JobPriorityLow, 0, 2, 0)

	snapshot := scheduler.Snapshot()
	assertSchedulerCount(t, snapshot.Running, model.JobPriorityHigh, 2)
	assertSchedulerCount(t, snapshot.Running, model.JobPriorityNormal, 2)
	assertSchedulerCount(t, snapshot.Running, model.JobPriorityLow, 0)
	assertSchedulerAcquirePending(t, lowOneResult)
	assertSchedulerAcquirePending(t, lowTwoResult)
}

func TestSchedulerGivesAllFourSlotsToHighPriority(t *testing.T) {
	scheduler := New(4)
	leases := []*Lease{
		acquireSchedulerLease(t, scheduler, "high-1", model.JobPriorityHigh, 1),
		acquireSchedulerLease(t, scheduler, "high-2", model.JobPriorityHigh, 2),
		acquireSchedulerLease(t, scheduler, "high-3", model.JobPriorityHigh, 3),
		acquireSchedulerLease(t, scheduler, "high-4", model.JobPriorityHigh, 4),
	}
	defer releaseSchedulerLeases(leases)

	normalOneCancel, normalOneResult := startSchedulerAcquire(
		scheduler,
		"normal-1",
		model.JobPriorityNormal,
		5,
	)
	defer normalOneCancel()
	normalTwoCancel, normalTwoResult := startSchedulerAcquire(
		scheduler,
		"normal-2",
		model.JobPriorityNormal,
		6,
	)
	defer normalTwoCancel()
	lowOneCancel, lowOneResult := startSchedulerAcquire(
		scheduler,
		"low-1",
		model.JobPriorityLow,
		7,
	)
	defer lowOneCancel()
	lowTwoCancel, lowTwoResult := startSchedulerAcquire(
		scheduler,
		"low-2",
		model.JobPriorityLow,
		8,
	)
	defer lowTwoCancel()
	waitForSchedulerCounts(t, scheduler, model.JobPriorityNormal, 0, 2, 0)
	waitForSchedulerCounts(t, scheduler, model.JobPriorityLow, 0, 2, 0)

	snapshot := scheduler.Snapshot()
	assertSchedulerCount(t, snapshot.Running, model.JobPriorityHigh, 4)
	assertSchedulerCount(t, snapshot.Running, model.JobPriorityNormal, 0)
	assertSchedulerCount(t, snapshot.Running, model.JobPriorityLow, 0)
	assertSchedulerAcquirePending(t, normalOneResult)
	assertSchedulerAcquirePending(t, normalTwoResult)
	assertSchedulerAcquirePending(t, lowOneResult)
	assertSchedulerAcquirePending(t, lowTwoResult)
}

func TestSchedulerGivesReleasedSlotToHighestWaitingPriority(t *testing.T) {
	scheduler := New(4)
	highLeases := []*Lease{
		acquireSchedulerLease(t, scheduler, "high-1", model.JobPriorityHigh, 1),
		acquireSchedulerLease(t, scheduler, "high-2", model.JobPriorityHigh, 2),
		acquireSchedulerLease(t, scheduler, "high-3", model.JobPriorityHigh, 3),
		acquireSchedulerLease(t, scheduler, "high-4", model.JobPriorityHigh, 4),
	}
	defer releaseSchedulerLeases(highLeases)

	normalOneCancel, normalOneResult := startSchedulerAcquire(
		scheduler,
		"normal-1",
		model.JobPriorityNormal,
		5,
	)
	defer normalOneCancel()
	normalTwoCancel, normalTwoResult := startSchedulerAcquire(
		scheduler,
		"normal-2",
		model.JobPriorityNormal,
		6,
	)
	defer normalTwoCancel()
	lowCancel, lowResult := startSchedulerAcquire(
		scheduler,
		"low-1",
		model.JobPriorityLow,
		7,
	)
	defer lowCancel()
	waitForSchedulerCounts(t, scheduler, model.JobPriorityNormal, 0, 2, 0)
	waitForSchedulerCounts(t, scheduler, model.JobPriorityLow, 0, 1, 0)

	highLeases[0].Release()
	normalOneLease := receiveSchedulerLease(t, normalOneResult)
	defer normalOneLease.Release()
	if normalOneLease.jobID != "normal-1" {
		t.Fatalf("released slot went to %q, want normal-1", normalOneLease.jobID)
	}
	assertSchedulerAcquirePending(t, normalTwoResult)
	assertSchedulerAcquirePending(t, lowResult)
}

func TestSchedulerPausesOnlyEnoughLowerJobs(t *testing.T) {
	scheduler := New(4)
	lowLeases := []*Lease{
		acquireSchedulerLease(t, scheduler, "low-1", model.JobPriorityLow, 1),
		acquireSchedulerLease(t, scheduler, "low-2", model.JobPriorityLow, 2),
		acquireSchedulerLease(t, scheduler, "low-3", model.JobPriorityLow, 3),
		acquireSchedulerLease(t, scheduler, "low-4", model.JobPriorityLow, 4),
	}
	defer releaseSchedulerLeases(lowLeases)

	highCancel, highResult := startSchedulerAcquire(
		scheduler,
		"high-1",
		model.JobPriorityHigh,
		5,
	)
	defer highCancel()
	waitForSchedulerPauseRequest(t, lowLeases[3], true)

	requested := 0
	for _, lease := range lowLeases {
		pauseRequested, reason := lease.Checkpoint()
		if !pauseRequested {
			continue
		}
		requested++
		if reason != ReasonHigherPriorityWork {
			t.Fatalf("pause reason = %q, want %q", reason, ReasonHigherPriorityWork)
		}
	}
	if requested != 1 {
		t.Fatalf("pause requests = %d, want 1", requested)
	}
	assertSchedulerAcquirePending(t, highResult)
}

func TestSchedulerPausedJobKeepsFIFOPosition(t *testing.T) {
	scheduler := New(1)
	oldLow := acquireSchedulerLease(t, scheduler, "low-old", model.JobPriorityLow, 1)
	defer oldLow.Release()

	newLowCancel, newLowResult := startSchedulerAcquire(
		scheduler,
		"low-new",
		model.JobPriorityLow,
		2,
	)
	defer newLowCancel()
	highCancel, highResult := startSchedulerAcquire(
		scheduler,
		"high",
		model.JobPriorityHigh,
		3,
	)
	defer highCancel()
	waitForSchedulerPauseRequest(t, oldLow, true)

	if !oldLow.Yield(ReasonHigherPriorityWork) {
		t.Fatal("old low lease did not yield")
	}
	highLease := receiveSchedulerLease(t, highResult)
	defer highLease.Release()

	reacquired := make(chan error, 1)
	go func() {
		reacquired <- oldLow.Reacquire(context.Background())
	}()
	waitForSchedulerCounts(t, scheduler, model.JobPriorityLow, 0, 2, 0)
	highLease.Release()
	waitForSchedulerEntryState(t, scheduler, "low-old", EntryRunning)
	if err := receiveSchedulerError(t, reacquired); err != nil {
		t.Fatalf("reacquire old low lease: %v", err)
	}
	assertSchedulerAcquirePending(t, newLowResult)
}

func TestSchedulerRetryRoundAdmitsHighestPriorityFirst(t *testing.T) {
	scheduler := New(1)
	lowLease := acquireSchedulerLease(t, scheduler, "low", model.JobPriorityLow, 1)
	defer lowLease.Release()

	lowRetry := make(chan error, 1)
	go func() {
		lowRetry <- lowLease.RetrySharedAfter(
			context.Background(),
			time.Hour,
			"waiting for sync lock",
		)
	}()
	waitForSchedulerCounts(t, scheduler, model.JobPriorityLow, 0, 0, 1)

	highLease := acquireSchedulerLease(t, scheduler, "high", model.JobPriorityHigh, 2)
	defer highLease.Release()
	highRetry := make(chan error, 1)
	go func() {
		highRetry <- highLease.RetrySharedAfter(
			context.Background(),
			time.Hour,
			"waiting for sync lock",
		)
	}()
	waitForSchedulerCounts(t, scheduler, model.JobPriorityHigh, 0, 0, 1)
	scheduler.mutex.Lock()
	scheduler.retryWaiting[lowLease.jobID] = time.Now()
	scheduler.mutex.Unlock()
	scheduler.openRetryRound(scheduler.retryGeneration)

	if err := receiveSchedulerError(t, highRetry); err != nil {
		t.Fatalf("high retry: %v", err)
	}
	select {
	case err := <-lowRetry:
		t.Fatalf("low retry completed before high released: %v", err)
	default:
	}
	highLease.Release()
	scheduler.mutex.Lock()
	scheduler.retryWaiting[lowLease.jobID] = time.Now()
	scheduler.scheduleNextRetryLocked(time.Now())
	scheduler.mutex.Unlock()
	if err := receiveSchedulerError(t, lowRetry); err != nil {
		t.Fatalf("low retry: %v", err)
	}
}

func TestSchedulerRetryAfterHonorsEachLeaseDeadline(t *testing.T) {
	scheduler := New(2)
	lowLease := acquireSchedulerLease(t, scheduler, "low", model.JobPriorityLow, 1)
	defer lowLease.Release()
	highLease := acquireSchedulerLease(t, scheduler, "high", model.JobPriorityHigh, 2)
	defer highLease.Release()

	lowResult := make(chan error, 1)
	go func() {
		lowResult <- lowLease.RetryAfter(
			context.Background(),
			20*time.Millisecond,
			"retry low",
		)
	}()
	waitForSchedulerCounts(t, scheduler, model.JobPriorityLow, 0, 0, 1)

	highResult := make(chan error, 1)
	go func() {
		highResult <- highLease.RetryAfter(
			context.Background(),
			180*time.Millisecond,
			"retry high",
		)
	}()
	waitForSchedulerCounts(t, scheduler, model.JobPriorityHigh, 0, 0, 1)

	if err := receiveSchedulerError(t, lowResult); err != nil {
		t.Fatalf("low retry: %v", err)
	}
	select {
	case err := <-highResult:
		t.Fatalf("high retry finished before its deadline: %v", err)
	default:
	}
	if err := receiveSchedulerError(t, highResult); err != nil {
		t.Fatalf("high retry: %v", err)
	}
}

func TestSchedulerPausesLowBeforeNormal(t *testing.T) {
	scheduler := New(4)
	normalLeases := []*Lease{
		acquireSchedulerLease(t, scheduler, "normal-1", model.JobPriorityNormal, 1),
		acquireSchedulerLease(t, scheduler, "normal-2", model.JobPriorityNormal, 2),
	}
	defer releaseSchedulerLeases(normalLeases)
	lowLeases := []*Lease{
		acquireSchedulerLease(t, scheduler, "low-1", model.JobPriorityLow, 3),
		acquireSchedulerLease(t, scheduler, "low-2", model.JobPriorityLow, 4),
	}
	defer releaseSchedulerLeases(lowLeases)

	highOneCancel, _ := startSchedulerAcquire(
		scheduler,
		"high-1",
		model.JobPriorityHigh,
		5,
	)
	defer highOneCancel()
	highTwoCancel, _ := startSchedulerAcquire(
		scheduler,
		"high-2",
		model.JobPriorityHigh,
		6,
	)
	defer highTwoCancel()
	waitForSchedulerPauseRequest(t, lowLeases[0], true)
	waitForSchedulerPauseRequest(t, lowLeases[1], true)

	for _, lease := range normalLeases {
		pauseRequested, _ := lease.Checkpoint()
		if pauseRequested {
			t.Fatalf("normal lease %q paused while low remained", lease.jobID)
		}
	}
}

func TestSchedulerClearsPauseRequestWhenSlotOpens(t *testing.T) {
	scheduler := New(2)
	oldLow := acquireSchedulerLease(t, scheduler, "low-old", model.JobPriorityLow, 1)
	defer oldLow.Release()
	newLow := acquireSchedulerLease(t, scheduler, "low-new", model.JobPriorityLow, 2)
	defer newLow.Release()

	highCancel, highResult := startSchedulerAcquire(
		scheduler,
		"high",
		model.JobPriorityHigh,
		3,
	)
	defer highCancel()
	waitForSchedulerPauseRequest(t, newLow, true)

	oldLow.Release()
	highLease := receiveSchedulerLease(t, highResult)
	defer highLease.Release()
	waitForSchedulerPauseRequest(t, newLow, false)
	if pauseRequested, reason := newLow.Checkpoint(); pauseRequested || reason != "" {
		t.Fatalf("obsolete pause request = %v reason %q, want false and empty", pauseRequested, reason)
	}
}

func TestSchedulerUpdatePolicyRecomputesVictimsAndPreservesFields(t *testing.T) {
	scheduler := New(1)
	low := acquireSchedulerLease(t, scheduler, "low", model.JobPriorityLow, 1)
	defer low.Release()

	normalCancel, _ := startSchedulerAcquire(
		scheduler,
		"normal",
		model.JobPriorityNormal,
		2,
	)
	defer normalCancel()
	waitForSchedulerPauseRequest(t, low, true)

	priority := model.JobPriorityHigh
	if err := scheduler.UpdatePolicy(
		"low",
		model.SchedulingPolicyPatch{Priority: &priority},
	); err != nil {
		t.Fatalf("UpdatePolicy: %v", err)
	}
	waitForSchedulerPauseRequest(t, low, false)

	entry := schedulerEntryForTest(t, scheduler, "low")
	if entry.Policy.Priority != model.JobPriorityHigh {
		t.Fatalf("updated priority = %q, want high", entry.Policy.Priority)
	}
	if entry.Policy.Quiet || entry.Policy.IdleAfterSeconds != model.DefaultIdleAfterSeconds {
		t.Fatalf("unrelated policy fields changed: %+v", entry.Policy)
	}
}

func TestSchedulerLeaseOperationsAreIdempotent(t *testing.T) {
	scheduler := New(1)
	lease := acquireSchedulerLease(t, scheduler, "low", model.JobPriorityLow, 1)

	if !lease.Yield("watchdog") {
		t.Fatal("first yield = false, want true")
	}
	if lease.Yield("watchdog") {
		t.Fatal("second yield = true, want false")
	}
	assertSchedulerCount(t, scheduler.Snapshot().Paused, model.JobPriorityLow, 1)

	cancelledContext, cancel := context.WithCancel(context.Background())
	cancel()
	if err := lease.Reacquire(cancelledContext); !errors.Is(err, context.Canceled) {
		t.Fatalf("Reacquire error = %v, want context canceled", err)
	}
	assertSchedulerCount(t, scheduler.Snapshot().Paused, model.JobPriorityLow, 1)

	lease.Release()
	lease.Release()
	snapshot := scheduler.Snapshot()
	assertSchedulerCount(t, snapshot.Running, model.JobPriorityLow, 0)
	assertSchedulerCount(t, snapshot.Queued, model.JobPriorityLow, 0)
	assertSchedulerCount(t, snapshot.Paused, model.JobPriorityLow, 0)
}

func acquireSchedulerLease(
	t *testing.T,
	scheduler *Scheduler,
	jobID string,
	priority model.JobPriority,
	queueSequence uint64,
) *Lease {
	t.Helper()
	lease, err := scheduler.Acquire(context.Background(), schedulerTestEntry(jobID, priority, queueSequence))
	if err != nil {
		t.Fatalf("Acquire %s: %v", jobID, err)
	}
	return lease
}

func startSchedulerAcquire(
	scheduler *Scheduler,
	jobID string,
	priority model.JobPriority,
	queueSequence uint64,
) (context.CancelFunc, <-chan schedulerAcquireResult) {
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan schedulerAcquireResult, 1)
	go func() {
		lease, err := scheduler.Acquire(ctx, schedulerTestEntry(jobID, priority, queueSequence))
		result <- schedulerAcquireResult{lease: lease, err: err}
	}()
	return cancel, result
}

func schedulerTestEntry(
	jobID string,
	priority model.JobPriority,
	queueSequence uint64,
) Entry {
	policy := model.DefaultSchedulingPolicy()
	policy.Priority = priority
	return Entry{
		JobID:         jobID,
		Policy:        policy,
		QueueSequence: queueSequence,
	}
}

func receiveSchedulerLease(
	t *testing.T,
	result <-chan schedulerAcquireResult,
) *Lease {
	t.Helper()
	select {
	case acquired := <-result:
		if acquired.err != nil {
			t.Fatalf("Acquire: %v", acquired.err)
		}
		return acquired.lease
	case <-time.After(schedulerTestTimeout):
		t.Fatal("timed out waiting for scheduler lease")
		return nil
	}
}

func receiveSchedulerError(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(schedulerTestTimeout):
		t.Fatal("timed out waiting for scheduler operation")
		return nil
	}
}

func assertSchedulerAcquirePending(
	t *testing.T,
	result <-chan schedulerAcquireResult,
) {
	t.Helper()
	select {
	case acquired := <-result:
		if acquired.err != nil {
			t.Fatalf("pending Acquire returned error: %v", acquired.err)
		}
		acquired.lease.Release()
		t.Fatal("Acquire completed while entry should remain queued")
	default:
	}
}

func waitForSchedulerCounts(
	t *testing.T,
	scheduler *Scheduler,
	priority model.JobPriority,
	running int,
	queued int,
	paused int,
) {
	t.Helper()
	waitForSchedulerCondition(t, func() bool {
		snapshot := scheduler.Snapshot()
		return snapshot.Running[priority] == running &&
			snapshot.Queued[priority] == queued &&
			snapshot.Paused[priority] == paused
	})
}

func waitForSchedulerPauseRequest(
	t *testing.T,
	lease *Lease,
	want bool,
) {
	t.Helper()
	waitForSchedulerCondition(t, func() bool {
		requested, _ := lease.Checkpoint()
		return requested == want
	})
}

func waitForSchedulerEntryState(
	t *testing.T,
	scheduler *Scheduler,
	jobID string,
	want EntryState,
) {
	t.Helper()
	waitForSchedulerCondition(t, func() bool {
		scheduler.mutex.Lock()
		defer scheduler.mutex.Unlock()
		entry, found := scheduler.entries[jobID]
		return found && entry.State == want
	})
}

func waitForSchedulerCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(schedulerTestTimeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for scheduler state")
		}
		time.Sleep(time.Millisecond)
	}
}

func schedulerEntryForTest(
	t *testing.T,
	scheduler *Scheduler,
	jobID string,
) Entry {
	t.Helper()
	scheduler.mutex.Lock()
	defer scheduler.mutex.Unlock()
	entry, found := scheduler.entries[jobID]
	if !found {
		t.Fatalf("scheduler entry %q is missing", jobID)
	}
	return *entry
}

func assertSchedulerCount(
	t *testing.T,
	counts map[model.JobPriority]int,
	priority model.JobPriority,
	want int,
) {
	t.Helper()
	if counts[priority] != want {
		t.Fatalf("%s count = %d, want %d", priority, counts[priority], want)
	}
}

func releaseSchedulerLeases(leases []*Lease) {
	for _, lease := range leases {
		lease.Release()
	}
}
