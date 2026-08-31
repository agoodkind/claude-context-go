package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"goodkind.io/lm-semantic-search/internal/clock"
	"goodkind.io/lm-semantic-search/internal/jobscheduler"
	"goodkind.io/lm-semantic-search/internal/model"
)

const (
	// defaultJobCapacityReleaseGrace is how long a read may run before the job
	// durably pauses and yields its scheduler lease and shared sync lock.
	// Whether a read will stall cannot be known before it runs, so this decides
	// by watching: a healthy reuse read answers in milliseconds and keeps both
	// holds, while a read parked behind a collection load that Milvus never
	// finishes crosses the grace and frees them for the minutes it may still
	// take.
	defaultJobCapacityReleaseGrace = 4500 * time.Millisecond

	stalledReadSchedulingReason = "stalled read"
	syncLockSchedulingReason    = "waiting for sync lock"
)

type jobCapacityContextKey struct{}

// jobCapacityTimings bounds how long a read may run before its scheduler lease
// yields. Reacquisition is intentionally unbounded and remains cancellable.
type jobCapacityTimings struct {
	ReleaseGrace time.Duration
}

func defaultJobCapacityTimings() jobCapacityTimings {
	return jobCapacityTimings{
		ReleaseGrace: defaultJobCapacityReleaseGrace,
	}
}

// jobCapacityReacquireError reports that a yielded job could not regain its
// scheduler lease or sync lock before cancellation or a permanent lock error.
type jobCapacityReacquireError struct {
	Cause error
}

func (err *jobCapacityReacquireError) Error() string {
	return fmt.Sprintf("reacquire indexing capacity after read: %v", err.Cause)
}

func (err *jobCapacityReacquireError) Unwrap() error {
	return err.Cause
}

// jobCapacity is one scheduler lease's hold on admission capacity and the
// shared advisory sync lock. No other production path owns either resource.
type jobCapacity struct {
	manager         *Manager
	jobID           string
	lease           *jobscheduler.Lease
	mu              sync.Mutex
	released        bool
	holdSyncLock    bool
	syncLockHeld    bool
	waitsOnSyncLock bool
	// syncLockLease is the release handle for the reference this capacity holds.
	// The watchdog releases it from its own goroutine and a resume takes a fresh
	// one, so the handle is stored rather than re-derived: a lease releases at
	// most once, which is what keeps a watchdog release from dropping the
	// reference a later resume took.
	syncLockLease syncLockLease
}

func withJobCapacity(ctx context.Context, capacity *jobCapacity) context.Context {
	return context.WithValue(ctx, jobCapacityContextKey{}, capacity)
}

func jobCapacityFromContext(ctx context.Context) *jobCapacity {
	capacity, _ := ctx.Value(jobCapacityContextKey{}).(*jobCapacity)
	return capacity
}

func (manager *Manager) acquireJobCapacity(
	ctx context.Context,
	job model.Job,
	holdSyncLock bool,
) (*jobCapacity, syncLockOutcome, error) {
	normalizedPolicy, err := model.ApplySchedulingPolicyPatch(
		job.EffectiveSchedulingPolicy,
		model.SchedulingPolicyPatch{
			Priority:         nil,
			Quiet:            nil,
			IdleAfterSeconds: nil,
		},
	)
	if err != nil {
		wrappedErr := fmt.Errorf("normalize scheduler job policy: %w", err)
		slog.ErrorContext(ctx, "normalize scheduler job policy failed", "job_id", job.ID, "err", wrappedErr)
		return nil, syncLockFailed, wrappedErr
	}
	job.EffectiveSchedulingPolicy = normalizedPolicy
	lease, err := manager.jobScheduler.Acquire(ctx, jobscheduler.Entry{
		JobID:          job.ID,
		Policy:         job.EffectiveSchedulingPolicy,
		QueueSequence:  job.QueueSequence,
		State:          jobscheduler.EntryWaiting,
		Reason:         "",
		PauseRequested: false,
	})
	if err != nil {
		wrappedErr := fmt.Errorf("acquire scheduler capacity: %w", err)
		slog.WarnContext(ctx, "acquire scheduler capacity stopped", "job_id", job.ID, "err", wrappedErr)
		return nil, syncLockCancelled, wrappedErr
	}
	manager.policyMutationMutex.Lock()
	manager.mu.Lock()
	latestJob, found := manager.jobs[job.ID]
	manager.mu.Unlock()
	if found {
		latestPolicy, policyErr := model.ApplySchedulingPolicyPatch(
			latestJob.EffectiveSchedulingPolicy,
			model.SchedulingPolicyPatch{
				Priority:         nil,
				Quiet:            nil,
				IdleAfterSeconds: nil,
			},
		)
		if policyErr != nil {
			manager.policyMutationMutex.Unlock()
			lease.Release()
			wrappedErr := fmt.Errorf("normalize latest scheduler job policy: %w", policyErr)
			slog.ErrorContext(ctx, "normalize latest scheduler job policy failed", "job_id", job.ID, "err", wrappedErr)
			return nil, syncLockFailed, wrappedErr
		}
		priority := latestPolicy.Priority
		quiet := latestPolicy.Quiet
		idleAfterSeconds := latestPolicy.IdleAfterSeconds
		if policyErr := lease.UpdatePolicy(model.SchedulingPolicyPatch{
			Priority:         &priority,
			Quiet:            &quiet,
			IdleAfterSeconds: &idleAfterSeconds,
		}); policyErr != nil {
			manager.policyMutationMutex.Unlock()
			lease.Release()
			wrappedErr := fmt.Errorf("refresh scheduler job policy: %w", policyErr)
			slog.ErrorContext(ctx, "refresh scheduler job policy failed", "job_id", job.ID, "err", wrappedErr)
			return nil, syncLockFailed, wrappedErr
		}
	}
	manager.policyMutationMutex.Unlock()
	if !holdSyncLock && manager.semantic != nil && manager.semantic.Available() {
		holdSyncLock = true
	}
	capacity := &jobCapacity{
		manager:         manager,
		jobID:           job.ID,
		lease:           lease,
		mu:              sync.Mutex{},
		released:        false,
		holdSyncLock:    holdSyncLock,
		syncLockHeld:    false,
		waitsOnSyncLock: false,
		syncLockLease:   syncLockLease{lock: nil, once: nil},
	}
	if !holdSyncLock {
		return capacity, syncLockAcquired, nil
	}
	outcome, lockErr := capacity.acquireSyncLock(ctx)
	if outcome != syncLockAcquired {
		capacity.release(context.WithoutCancel(ctx))
		return nil, outcome, lockErr
	}
	return capacity, syncLockAcquired, nil
}

func (capacity *jobCapacity) acquireSyncLock(
	ctx context.Context,
) (syncLockOutcome, error) {
	for {
		lease, outcome, err := capacity.manager.syncLock.acquire(ctx)
		switch outcome {
		case syncLockAcquired:
			capacity.mu.Lock()
			if capacity.released {
				capacity.mu.Unlock()
				lease.release(context.WithoutCancel(ctx))
				cause := ctx.Err()
				if cause == nil {
					cause = errSyncLockWaitCancelled
				}
				wrappedErr := fmt.Errorf("acquire sync lock after capacity release: %w", cause)
				slog.WarnContext(ctx, "acquire sync lock stopped after capacity release", "job_id", capacity.jobID, "err", wrappedErr)
				return syncLockCancelled, wrappedErr
			}
			capacity.syncLockLease = lease
			capacity.syncLockHeld = true
			capacity.waitsOnSyncLock = false
			capacity.mu.Unlock()
			return syncLockAcquired, nil
		case syncLockFailed:
			return syncLockFailed, err
		case syncLockCancelled:
			return syncLockCancelled, err
		case syncLockBusy:
			capacity.markSyncLockWait(ctx)
		}

		if err := capacity.retryAfter(ctx, syncLockRetryInterval, syncLockSchedulingReason); err != nil {
			wrappedErr := fmt.Errorf("retry scheduler capacity after sync lock wait: %w", err)
			slog.WarnContext(ctx, "retry scheduler capacity after sync lock wait stopped", "job_id", capacity.jobID, "err", wrappedErr)
			return syncLockCancelled, wrappedErr
		}
	}
}

func (capacity *jobCapacity) markSyncLockWait(ctx context.Context) {
	capacity.mu.Lock()
	alreadyWaiting := capacity.waitsOnSyncLock
	capacity.waitsOnSyncLock = true
	capacity.mu.Unlock()
	if alreadyWaiting {
		return
	}
	capacity.manager.setJobSchedulingReason(ctx, capacity.jobID, syncLockSchedulingReason)
}

func (capacity *jobCapacity) release(ctx context.Context) {
	capacity.mu.Lock()
	defer capacity.mu.Unlock()
	if capacity.released {
		return
	}
	capacity.releaseSyncLockLocked(ctx)
	capacity.lease.Release()
	capacity.released = true
}

func (capacity *jobCapacity) yieldClaim(
	ctx context.Context,
	claim *jobscheduler.PauseClaim,
) bool {
	capacity.mu.Lock()
	defer capacity.mu.Unlock()
	if capacity.released {
		return false
	}
	capacity.releaseSyncLockLocked(ctx)
	return claim.Yield()
}

func (capacity *jobCapacity) retryAfter(
	ctx context.Context,
	delay time.Duration,
	reason string,
) error {
	capacity.mu.Lock()
	if capacity.released {
		capacity.mu.Unlock()
		return fmt.Errorf("retry released job capacity")
	}
	capacity.releaseSyncLockLocked(context.WithoutCancel(ctx))
	capacity.mu.Unlock()
	if err := capacity.lease.RetryAfter(ctx, delay, reason); err != nil {
		wrappedErr := fmt.Errorf("retry scheduler lease: %w", err)
		slog.WarnContext(ctx, "retry scheduler lease stopped", "job_id", capacity.jobID, "err", wrappedErr)
		return wrappedErr
	}
	return nil
}

func (capacity *jobCapacity) releaseSyncLockLocked(ctx context.Context) {
	if capacity.syncLockHeld {
		capacity.syncLockLease.release(ctx)
		capacity.syncLockLease = syncLockLease{lock: nil, once: nil}
		capacity.syncLockHeld = false
	}
}

func (capacity *jobCapacity) reacquire(
	ctx context.Context,
	holdSyncLock bool,
) (syncLockOutcome, error) {
	if err := capacity.lease.Reacquire(ctx); err != nil {
		wrappedErr := fmt.Errorf("reacquire scheduler capacity: %w", err)
		slog.WarnContext(ctx, "reacquire scheduler capacity stopped", "job_id", capacity.jobID, "err", wrappedErr)
		return syncLockCancelled, wrappedErr
	}
	if !holdSyncLock {
		return syncLockAcquired, nil
	}
	return capacity.acquireSyncLock(ctx)
}

// stallRelease is the watchdog that durably pauses and yields this job once a
// read has run longer than the release grace. Its result is read only after
// join returns, so the channel close orders the accesses.
type stallRelease struct {
	stopped  chan struct{}
	finished chan struct{}
	released bool
	err      error
}

// startStallRelease arms the watchdog. It touches the capacity only after the
// grace elapses, which is a window in which the job's own goroutine is inside
// the read and touches nothing.
func startStallRelease(
	ctx context.Context,
	capacity *jobCapacity,
	grace time.Duration,
) *stallRelease {
	startedAt := clock.Now()
	watchdog := &stallRelease{
		stopped:  make(chan struct{}),
		finished: make(chan struct{}),
		released: false,
		err:      nil,
	}
	go func() {
		defer close(watchdog.finished)
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.ErrorContext(ctx, "indexing capacity watchdog panic",
					"component", "daemon",
					"subcomponent", "capacity",
					"err", fmt.Errorf("panic: %v", recovered),
				)
			}
		}()
		graceTimer := time.NewTimer(grace)
		defer graceTimer.Stop()
		select {
		case <-watchdog.stopped:
			return
		case <-graceTimer.C:
		}
		claim, claimed := capacity.lease.ClaimYield(stalledReadSchedulingReason)
		if !claimed {
			return
		}
		if err := capacity.manager.pauseJob(capacity.jobID, stalledReadSchedulingReason); err != nil {
			claim.Cancel()
			capacity.manager.failScheduledJob(context.WithoutCancel(ctx), capacity.jobID, err)
			capacity.release(context.WithoutCancel(ctx))
			watchdog.err = err
			return
		}
		if !capacity.manager.jobIsPaused(capacity.jobID) {
			claim.Cancel()
			return
		}
		watchdog.released = capacity.yieldClaim(context.WithoutCancel(ctx), claim)
		if !watchdog.released {
			failure := fmt.Errorf("yield stalled scheduler claim")
			capacity.manager.failScheduledJob(context.WithoutCancel(ctx), capacity.jobID, failure)
			capacity.release(context.WithoutCancel(ctx))
			watchdog.err = failure
			return
		}
		slog.WarnContext(ctx, "released indexing capacity for a stalled read",
			"component", "daemon",
			"subcomponent", "capacity",
			"grace_ms", grace.Milliseconds(),
			"elapsed_ms", clock.Now().Sub(startedAt).Milliseconds(),
		)
	}()
	return watchdog
}

// join stops the watchdog and reports what it released, if anything. It returns
// only once the watchdog goroutine has finished, so the caller owns the
// capacity again from here on.
func (watchdog *stallRelease) join() (bool, error) {
	close(watchdog.stopped)
	<-watchdog.finished
	return watchdog.released, watchdog.err
}

// runReleasingCapacityIfStalled runs one store read, giving up this job's
// indexing slot and its reference on the shared sync lock only for as long as
// that read actually stalls.
//
// Whether a read will stall is not knowable before it runs, so the decision is
// made by watching it. The read starts while the job still holds everything,
// and a watchdog releases the holds only once the read has failed to finish
// within the release grace. A healthy reuse read therefore keeps its place and
// pays nothing, while a read parked behind a collection load Milvus never
// finishes stops starving the jobs queued behind it.
//
// Once the holds are gone the job joins the scheduler with its original policy
// and queue sequence. It stays visibly paused until both resources return.
func (manager *Manager) runReleasingCapacityIfStalled(
	ctx context.Context,
	operation func() error,
) error {
	capacity := jobCapacityFromContext(ctx)
	if capacity == nil {
		return operation()
	}

	watchdog := startStallRelease(ctx, capacity, manager.jobCapacityTimings.ReleaseGrace)
	operationErr := operation()
	released, pauseErr := watchdog.join()
	if pauseErr != nil {
		return &jobCapacityReacquireError{Cause: pauseErr}
	}
	if !released {
		return operationErr
	}

	outcome, reacquireErr := capacity.reacquire(ctx, capacity.holdSyncLock)
	if outcome != syncLockAcquired {
		capacity.release(context.WithoutCancel(ctx))
		if outcome == syncLockFailed {
			manager.updateJobFailed(context.WithoutCancel(ctx), capacity.jobID, reacquireErr)
		} else {
			manager.updateJobCancelled(context.WithoutCancel(ctx), capacity.jobID)
		}
		return &jobCapacityReacquireError{Cause: reacquireErr}
	}
	if err := manager.resumeJob(capacity.jobID); err != nil {
		capacity.release(context.WithoutCancel(ctx))
		manager.failScheduledJob(context.WithoutCancel(ctx), capacity.jobID, err)
		return &jobCapacityReacquireError{Cause: err}
	}
	return operationErr
}
