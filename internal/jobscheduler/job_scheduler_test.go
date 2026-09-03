package jobscheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/platformactivity"
)

const schedulerTestTimeout = time.Second

type schedulerAcquireResult struct {
	lease *Lease
	err   error
}

type activityTestSource struct {
	mutex     sync.Mutex
	snapshot  platformactivity.Snapshot
	samples   int
	closeCall int
}

func (source *activityTestSource) Sample(context.Context) platformactivity.Snapshot {
	source.mutex.Lock()
	defer source.mutex.Unlock()
	source.samples++
	return source.snapshot
}

func (source *activityTestSource) Close() {
	source.mutex.Lock()
	defer source.mutex.Unlock()
	source.closeCall++
}

func (source *activityTestSource) setSnapshot(snapshot platformactivity.Snapshot) {
	source.mutex.Lock()
	defer source.mutex.Unlock()
	source.snapshot = snapshot
}

func (source *activityTestSource) counts() (int, int) {
	source.mutex.Lock()
	defer source.mutex.Unlock()
	return source.samples, source.closeCall
}

type panicActivitySource struct {
	activityTestSource
	panicAt map[int]bool
}

func (source *panicActivitySource) Sample(
	context.Context,
) platformactivity.Snapshot {
	source.mutex.Lock()
	source.samples++
	sample := source.samples
	snapshot := source.snapshot
	shouldPanic := source.panicAt[sample]
	source.mutex.Unlock()
	if shouldPanic {
		panic("activity source sample panic")
	}
	return snapshot
}

func TestQuietAdmissionRequiresConfiguredInputIdle(t *testing.T) {
	source := &activityTestSource{snapshot: platformactivity.Snapshot{
		InputAvailable:   true,
		InputIdleFor:     5*time.Minute - time.Second,
		InputReason:      "input active",
		ThermalAvailable: false,
	}}
	scheduler := New(context.Background(), 1, source)
	defer scheduler.Close()

	cancel, result := startQuietSchedulerAcquire(
		scheduler,
		"quiet",
		model.JobPriorityNormal,
		1,
		300,
	)
	defer cancel()
	waitForSchedulerCounts(t, scheduler, model.JobPriorityNormal, 0, 1, 0)
	entry := schedulerEntryForTest(t, scheduler, "quiet")
	if entry.Reason != ReasonWaitingForInputIdle {
		t.Fatalf("quiet wait reason = %q, want user active", entry.Reason)
	}

	source.setSnapshot(platformactivity.Snapshot{
		InputAvailable:   true,
		InputIdleFor:     5 * time.Minute,
		ThermalAvailable: false,
	})
	scheduler.sampleActivity(context.Background())
	lease := receiveSchedulerLease(t, result)
	lease.Release()
}

func TestQuietAdmissionPausesForActivityAndRecoversAutomatically(t *testing.T) {
	source := &activityTestSource{snapshot: quietActivitySnapshot()}
	scheduler := New(context.Background(), 1, source)
	defer scheduler.Close()
	lease := acquireQuietSchedulerLease(
		t,
		scheduler,
		"quiet",
		model.JobPriorityNormal,
		1,
		300,
	)
	defer lease.Release()

	source.setSnapshot(platformactivity.Snapshot{
		InputAvailable:   true,
		InputIdleFor:     0,
		InputReason:      "input active",
		ThermalAvailable: false,
	})
	scheduler.sampleActivity(context.Background())
	waitForSchedulerPauseRequest(t, lease, true)
	requested, reason := lease.Checkpoint()
	if !requested || reason != ReasonWaitingForInputIdle {
		t.Fatalf("activity pause = %v reason %q, want true and user active", requested, reason)
	}
	if !lease.Yield(reason) {
		t.Fatal("quiet lease did not yield for activity")
	}
	reacquired := make(chan error, 1)
	go func() {
		reacquired <- lease.Reacquire(context.Background())
	}()
	waitForSchedulerCounts(t, scheduler, model.JobPriorityNormal, 0, 1, 0)

	source.setSnapshot(quietActivitySnapshot())
	pollStarted := time.Now()
	select {
	case err := <-reacquired:
		if err != nil {
			t.Fatalf("quiet Reacquire: %v", err)
		}
		if elapsed := time.Since(pollStarted); elapsed < time.Second {
			t.Fatalf("automatic activity poll resumed after %s, want two-second cadence", elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("quiet lease did not recover on the two-second activity poll")
	}
}

func TestActivityUnavailableKeepsQuietWorkQueuedWithStableReason(t *testing.T) {
	source := &activityTestSource{snapshot: platformactivity.Snapshot{
		InputAvailable:   false,
		InputReason:      string(ReasonActivityUnavailable),
		ThermalAvailable: false,
	}}
	scheduler := New(
		context.Background(),
		1,
		source,
	)
	defer scheduler.Close()
	cancel, result := startQuietSchedulerAcquire(
		scheduler,
		"quiet",
		model.JobPriorityNormal,
		1,
		300,
	)
	defer cancel()
	waitForSchedulerCounts(t, scheduler, model.JobPriorityNormal, 0, 1, 0)

	for range 2 {
		scheduler.sampleActivity(context.Background())
		entry := schedulerEntryForTest(t, scheduler, "quiet")
		if entry.Reason != ReasonActivityUnavailable {
			t.Fatalf("unavailable reason = %q, want activity unavailable", entry.Reason)
		}
	}
	assertSchedulerAcquirePending(t, result)
}

func TestThermalSafetyMissingDataDoesNotBlockQuietAdmission(t *testing.T) {
	source := &activityTestSource{snapshot: platformactivity.Snapshot{
		InputAvailable:   true,
		InputIdleFor:     5 * time.Minute,
		ThermalAvailable: false,
		ThermalUnsafe:    true,
		ThermalReason:    "missing thermal data",
	}}
	scheduler := New(context.Background(), 1, source)
	defer scheduler.Close()
	lease := acquireQuietSchedulerLease(
		t,
		scheduler,
		"quiet",
		model.JobPriorityNormal,
		1,
		300,
	)
	lease.Release()
}

func TestThermalSafetyPausesQuietWork(t *testing.T) {
	source := &activityTestSource{snapshot: quietActivitySnapshot()}
	scheduler := New(context.Background(), 1, source)
	defer scheduler.Close()
	lease := acquireQuietSchedulerLease(
		t,
		scheduler,
		"quiet",
		model.JobPriorityNormal,
		1,
		300,
	)
	defer lease.Release()

	source.setSnapshot(platformactivity.Snapshot{
		InputAvailable:   true,
		InputIdleFor:     5 * time.Minute,
		ThermalAvailable: true,
		ThermalUnsafe:    true,
		ThermalReason:    "thermal pressure",
	})
	scheduler.sampleActivity(context.Background())
	waitForSchedulerPauseRequest(t, lease, true)
	requested, reason := lease.Checkpoint()
	if !requested || reason != ReasonThermalUnsafe {
		t.Fatalf("thermal pause = %v reason %q, want true and thermal safety", requested, reason)
	}
}

func TestHighBypassesIdleButNotThermalSafety(t *testing.T) {
	source := &activityTestSource{snapshot: platformactivity.Snapshot{
		InputAvailable:   false,
		InputReason:      "input unavailable",
		ThermalAvailable: false,
	}}
	scheduler := New(context.Background(), 1, source)
	defer scheduler.Close()
	lease := acquireQuietSchedulerLease(
		t,
		scheduler,
		"high-quiet",
		model.JobPriorityHigh,
		1,
		300,
	)
	defer lease.Release()

	source.setSnapshot(platformactivity.Snapshot{
		InputAvailable:   false,
		InputReason:      "input unavailable",
		ThermalAvailable: true,
		ThermalUnsafe:    true,
		ThermalReason:    "thermal pressure",
	})
	scheduler.sampleActivity(context.Background())
	waitForSchedulerPauseRequest(t, lease, true)
	requested, reason := lease.Checkpoint()
	if !requested || reason != ReasonThermalUnsafe {
		t.Fatalf("high quiet pause = %v reason %q, want thermal enforcement", requested, reason)
	}
}

func TestActivitySnapshotReadsCacheAndCloseClosesSource(t *testing.T) {
	source := &activityTestSource{snapshot: quietActivitySnapshot()}
	scheduler := New(context.Background(), 1, source)

	first := scheduler.Snapshot().Activity
	second := scheduler.Snapshot().Activity
	if first != second || first != quietActivitySnapshot() {
		t.Fatalf("cached activity snapshots = %+v and %+v", first, second)
	}
	if samples, _ := source.counts(); samples != 1 {
		t.Fatalf("source samples after status reads = %d, want 1", samples)
	}
	scheduler.Close()
	if _, closeCalls := source.counts(); closeCalls != 1 {
		t.Fatalf("source close calls = %d, want 1", closeCalls)
	}
}

func TestActivitySamplingSkipsUnchangedSnapshot(t *testing.T) {
	source := &activityTestSource{snapshot: quietActivitySnapshot()}
	scheduler := New(context.Background(), 1, source)
	defer scheduler.Close()

	scheduler.mutex.Lock()
	changed := scheduler.changed
	scheduler.mutex.Unlock()
	scheduler.sampleActivity(context.Background())

	scheduler.mutex.Lock()
	defer scheduler.mutex.Unlock()
	if scheduler.changed != changed {
		t.Fatal("unchanged activity sample notified scheduler waiters")
	}
}

func TestActivitySamplerContainsInitialSourcePanic(t *testing.T) {
	source := &panicActivitySource{
		activityTestSource: activityTestSource{snapshot: quietActivitySnapshot()},
		panicAt:            map[int]bool{1: true},
	}
	scheduler := New(context.Background(), 1, source)
	defer scheduler.Close()

	if got := scheduler.Snapshot().Activity; got != unavailableActivitySnapshot() {
		t.Fatalf("initial panic activity = %+v, want unavailable", got)
	}
	scheduler.sampleActivity(context.Background())
	if got := scheduler.Snapshot().Activity; got != quietActivitySnapshot() {
		t.Fatalf("sample after initial panic = %+v, want %+v", got, quietActivitySnapshot())
	}
}

func TestActivitySamplerContinuesAfterPeriodicSourcePanic(t *testing.T) {
	updated := platformactivity.Snapshot{
		InputAvailable:   true,
		InputIdleFor:     6 * time.Minute,
		ThermalAvailable: true,
	}
	source := &panicActivitySource{
		activityTestSource: activityTestSource{snapshot: quietActivitySnapshot()},
		panicAt:            map[int]bool{2: true},
	}
	scheduler := New(context.Background(), 1, source)
	defer scheduler.Close()
	source.setSnapshot(updated)

	scheduler.sampleActivity(context.Background())
	if got := scheduler.Snapshot().Activity; got != unavailableActivitySnapshot() {
		t.Fatalf("panic activity = %+v, want unavailable", got)
	}
	scheduler.sampleActivity(context.Background())
	if got := scheduler.Snapshot().Activity; got != updated {
		t.Fatalf("sample after periodic panic = %+v, want %+v", got, updated)
	}
}

func TestSchedulerFillsFourSlotsByWaitingPriority(t *testing.T) {
	scheduler := New(context.Background(), 4, nil)
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
	scheduler := New(context.Background(), 4, nil)
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
	scheduler := New(context.Background(), 4, nil)
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
	scheduler := New(context.Background(), 4, nil)
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
	scheduler := New(context.Background(), 1, nil)
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
	scheduler := New(context.Background(), 1, nil)
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
	retryGeneration := scheduler.retryGeneration
	scheduler.mutex.Unlock()
	scheduler.openRetryRound(retryGeneration)

	if err := receiveSchedulerError(t, highRetry); err != nil {
		t.Fatalf("high retry: %v", err)
	}
	select {
	case err := <-lowRetry:
		t.Fatalf("low retry completed before high released: %v", err)
	default:
	}
	highLease.Release()
	if err := receiveSchedulerError(t, lowRetry); err != nil {
		t.Fatalf("low retry: %v", err)
	}
}

func TestSchedulerPausesLowBeforeNormal(t *testing.T) {
	scheduler := New(context.Background(), 4, nil)
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
	scheduler := New(context.Background(), 2, nil)
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
	scheduler := New(context.Background(), 1, nil)
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
	scheduler := New(context.Background(), 1, nil)
	lease := acquireSchedulerLease(t, scheduler, "low", model.JobPriorityLow, 1)

	if !lease.Yield(model.SchedulingReasonUnspecified) {
		t.Fatal("first yield = false, want true")
	}
	if lease.Yield(model.SchedulingReasonUnspecified) {
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

func TestStaleLeaseCannotMutateReusedJobID(t *testing.T) {
	scheduler := New(context.Background(), 1, nil)
	stale := acquireSchedulerLease(t, scheduler, "reused", model.JobPriorityLow, 1)
	claim, claimed := stale.ClaimYield(model.SchedulingReasonUnspecified)
	if !claimed {
		t.Fatal("ClaimYield = false, want true")
	}
	stale.Release()

	current := acquireSchedulerLease(t, scheduler, "reused", model.JobPriorityHigh, 2)
	defer current.Release()
	claim.Cancel()
	if claim.Yield() {
		t.Fatal("stale claim Yield = true, want false")
	}
	stale.Release()
	priority := model.JobPriorityLow
	if err := stale.UpdatePolicy(model.SchedulingPolicyPatch{Priority: &priority}); err == nil {
		t.Fatal("stale lease UpdatePolicy succeeded")
	}

	entry := schedulerEntryForTest(t, scheduler, "reused")
	if entry.State != EntryRunning {
		t.Fatalf("reused entry state = %d, want running", entry.State)
	}
	if entry.Policy.Priority != model.JobPriorityHigh {
		t.Fatalf("reused entry priority = %q, want high", entry.Policy.Priority)
	}
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

func quietActivitySnapshot() platformactivity.Snapshot {
	return platformactivity.Snapshot{
		InputAvailable:   true,
		InputIdleFor:     5 * time.Minute,
		ThermalAvailable: true,
		ThermalUnsafe:    false,
	}
}

func acquireQuietSchedulerLease(
	t *testing.T,
	scheduler *Scheduler,
	jobID string,
	priority model.JobPriority,
	queueSequence uint64,
	idleAfterSeconds int32,
) *Lease {
	t.Helper()
	lease, err := scheduler.Acquire(
		context.Background(),
		quietSchedulerTestEntry(
			jobID,
			priority,
			queueSequence,
			idleAfterSeconds,
		),
	)
	if err != nil {
		t.Fatalf("Acquire quiet %s: %v", jobID, err)
	}
	return lease
}

func startQuietSchedulerAcquire(
	scheduler *Scheduler,
	jobID string,
	priority model.JobPriority,
	queueSequence uint64,
	idleAfterSeconds int32,
) (context.CancelFunc, <-chan schedulerAcquireResult) {
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan schedulerAcquireResult, 1)
	go func() {
		lease, err := scheduler.Acquire(
			ctx,
			quietSchedulerTestEntry(
				jobID,
				priority,
				queueSequence,
				idleAfterSeconds,
			),
		)
		result <- schedulerAcquireResult{lease: lease, err: err}
	}()
	return cancel, result
}

func quietSchedulerTestEntry(
	jobID string,
	priority model.JobPriority,
	queueSequence uint64,
	idleAfterSeconds int32,
) Entry {
	policy := model.SchedulingPolicy{
		Priority:         priority,
		Quiet:            true,
		IdleAfterSeconds: idleAfterSeconds,
	}
	return Entry{JobID: jobID, Policy: policy, QueueSequence: queueSequence}
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
