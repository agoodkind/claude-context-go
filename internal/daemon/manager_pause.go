package daemon

import (
	"context"
	"fmt"
	"log/slog"

	"goodkind.io/gklog/correlation"
	"goodkind.io/lm-semantic-search/internal/adapterr"
	"goodkind.io/lm-semantic-search/internal/clock"
	"goodkind.io/lm-semantic-search/internal/jobscheduler"
	"goodkind.io/lm-semantic-search/internal/metrics"
	"goodkind.io/lm-semantic-search/internal/model"
)

const (
	pauseJournalFailureCode  = "pause_journal_failed"
	resumeJournalFailureCode = "resume_journal_failed"
)

type jobSchedulerLeaseContextKey struct{}

type jobSchedulerLeaseContext struct {
	jobID string
	lease *jobscheduler.Lease
}

type jobTransitionMutation func(*model.Job) bool

func withJobSchedulerLease(
	ctx context.Context,
	jobID string,
	lease *jobscheduler.Lease,
) context.Context {
	value := jobSchedulerLeaseContext{jobID: jobID, lease: lease}
	return context.WithValue(ctx, jobSchedulerLeaseContextKey{}, value)
}

func (manager *Manager) pauseJob(
	jobID string,
	reason string,
) error {
	_, _, err := manager.serializeJobTransition(
		jobID,
		"job_paused",
		func(job *model.Job) bool {
			if job.State != model.JobStateRunning {
				return false
			}
			now := clock.Now()
			job.State = model.JobStatePaused
			job.SchedulingReason = reason
			job.UpdatedAt = now
			job.Progress.Phase = "paused"
			job.Progress.LastEventAt = now
			job.Progress.HeartbeatAt = now
			return true
		},
	)
	if err == nil {
		return nil
	}
	return journalTransitionFailure(pauseJournalFailureCode, err)
}

func (manager *Manager) resumeJob(jobID string) error {
	_, transitioned, err := manager.serializeJobTransition(
		jobID,
		"job_resumed",
		func(job *model.Job) bool {
			if job.State != model.JobStatePaused {
				return false
			}
			now := clock.Now()
			job.State = model.JobStateRunning
			job.SchedulingReason = ""
			job.UpdatedAt = now
			job.Progress.Phase = "Preparing and scanning files..."
			job.Progress.LastEventAt = now
			job.Progress.HeartbeatAt = now
			return true
		},
	)
	if err == nil {
		if transitioned {
			return nil
		}
		return fmt.Errorf("resume job %s: state changed", jobID)
	}
	return journalTransitionFailure(resumeJournalFailureCode, err)
}

func (manager *Manager) failScheduledJob(
	ctx context.Context,
	jobID string,
	failure error,
) {
	manager.mu.Lock()
	job, found := manager.jobs[jobID]
	manager.mu.Unlock()
	if found && job.Operation == "converge" {
		manager.updateDetachedJobFailed(ctx, jobID, failure)
		return
	}
	manager.updateJobFailed(ctx, jobID, failure)
}

func (manager *Manager) jobIsPaused(jobID string) bool {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	job, found := manager.jobs[jobID]
	return found && job.State == model.JobStatePaused
}

func (manager *Manager) setJobSchedulingReason(
	ctx context.Context,
	jobID string,
	reason string,
) {
	_, _, err := manager.serializeJobTransition(
		jobID,
		"job_waiting",
		func(job *model.Job) bool {
			if job.State != model.JobStateQueued {
				return false
			}
			now := clock.Now()
			job.SchedulingReason = reason
			job.UpdatedAt = now
			job.Progress.LastEventAt = now
			job.Progress.HeartbeatAt = now
			return true
		},
	)
	if err != nil {
		slog.ErrorContext(ctx, "append queued scheduling reason failed", "job_id", jobID, "err", err)
	}
}

func (manager *Manager) checkpointJob(ctx context.Context) error {
	value, found := ctx.Value(jobSchedulerLeaseContextKey{}).(jobSchedulerLeaseContext)
	if !found || value.lease == nil {
		return nil
	}
	claim, claimed := value.lease.ClaimPauseRequest()
	if !claimed {
		return nil
	}
	reason := claim.Reason()
	if err := manager.pauseJob(value.jobID, reason); err != nil {
		claim.Cancel()
		if capacity := jobCapacityFromContext(ctx); capacity != nil {
			capacity.release(context.WithoutCancel(ctx))
		} else {
			value.lease.Release()
		}
		manager.failScheduledJob(context.WithoutCancel(ctx), value.jobID, err)
		return err
	}
	if !manager.jobIsPaused(value.jobID) {
		claim.Cancel()
		if capacity := jobCapacityFromContext(ctx); capacity != nil {
			capacity.release(context.WithoutCancel(ctx))
		} else {
			value.lease.Release()
		}
		return fmt.Errorf("pause job %s: state changed", value.jobID)
	}
	capacity := jobCapacityFromContext(ctx)
	if capacity != nil {
		if !capacity.yieldClaim(context.WithoutCancel(ctx), claim) {
			failure := fmt.Errorf("yield scheduler pause claim")
			capacity.release(context.WithoutCancel(ctx))
			manager.failScheduledJob(context.WithoutCancel(ctx), value.jobID, failure)
			return failure
		}
		outcome, err := capacity.reacquire(ctx, capacity.holdSyncLock)
		if outcome != syncLockAcquired {
			capacity.release(context.WithoutCancel(ctx))
			manager.cancelScheduledJob(context.WithoutCancel(ctx), value.jobID)
			wrappedErr := fmt.Errorf("reacquire paused scheduler lease: %w", err)
			slog.ErrorContext(ctx, "reacquire paused scheduler lease failed", "job_id", value.jobID, "err", wrappedErr)
			return wrappedErr
		}
		if err := manager.resumeJob(value.jobID); err != nil {
			capacity.release(context.WithoutCancel(ctx))
			manager.failScheduledJob(context.WithoutCancel(ctx), value.jobID, err)
			return err
		}
		return nil
	}

	if !claim.Yield() {
		failure := fmt.Errorf("yield scheduler pause claim")
		value.lease.Release()
		manager.failScheduledJob(context.WithoutCancel(ctx), value.jobID, failure)
		return failure
	}
	if err := value.lease.Reacquire(ctx); err != nil {
		value.lease.Release()
		manager.cancelScheduledJob(context.WithoutCancel(ctx), value.jobID)
		wrappedErr := fmt.Errorf("reacquire paused scheduler lease: %w", err)
		slog.ErrorContext(ctx, "reacquire paused scheduler lease failed", "job_id", value.jobID, "err", wrappedErr)
		return wrappedErr
	}
	if err := manager.resumeJob(value.jobID); err != nil {
		value.lease.Release()
		manager.failScheduledJob(context.WithoutCancel(ctx), value.jobID, err)
		return err
	}
	return nil
}

func (manager *Manager) cancelScheduledJob(ctx context.Context, jobID string) {
	manager.mu.Lock()
	job, found := manager.jobs[jobID]
	manager.mu.Unlock()
	if found && job.Operation == "converge" {
		manager.updateDetachedJobCancelled(ctx, jobID)
		return
	}
	manager.updateJobCancelled(ctx, jobID)
}

func (manager *Manager) serializeJobTransition(
	jobID string,
	event string,
	mutate jobTransitionMutation,
) (model.Job, bool, error) {
	manager.transitionMutex.Lock()
	defer manager.transitionMutex.Unlock()
	return manager.serializeJobTransitionLocked(jobID, event, mutate)
}

func (manager *Manager) serializeJobTransitionLocked(
	jobID string,
	event string,
	mutate jobTransitionMutation,
) (model.Job, bool, error) {
	manager.mu.Lock()
	job, found := manager.jobs[jobID]
	if !found || !mutate(&job) {
		manager.mu.Unlock()
		return job, false, nil
	}
	jobEvent := model.JobEvent{
		Event:      event,
		OccurredAt: clock.Now(),
		Job:        job,
	}
	manager.mu.Unlock()

	transitionErr := manager.writeJobTransition(jobEvent)
	manager.mu.Lock()
	manager.jobs[jobID] = job
	manager.mu.Unlock()
	if transitionErr != nil {
		wrappedErr := fmt.Errorf("append %s transition: %w", event, transitionErr)
		slog.Error("append job transition failed", "job_id", jobID, "event", event, "err", wrappedErr)
		return job, true, wrappedErr
	}
	return job, true, nil
}

func (manager *Manager) failJobTransition(
	ctx context.Context,
	jobID string,
	runErr error,
) (model.Job, bool) {
	traceID := string(correlation.FromContext(ctx).TraceID)
	transient := adapterr.IsTransient(runErr)
	job, transitioned, journalErr := manager.serializeJobTransition(
		jobID,
		"job_failed",
		func(job *model.Job) bool {
			if isTerminalJobState(job.State) {
				return false
			}
			now := clock.Now()
			job.State = model.JobStateFailed
			job.UpdatedAt = now
			job.CompletedAt = &now
			job.Progress.Phase = "failed"
			job.Progress.LastEventAt = now
			job.Progress.HeartbeatAt = now
			job.Error = &model.JobError{
				Message:   adapterr.SafeMessage(runErr),
				Code:      adapterr.Code(runErr),
				Retryable: transient,
				TraceID:   traceID,
				JobID:     jobID,
			}
			return true
		},
	)
	if !transitioned {
		return job, false
	}
	metrics.JobFailed()
	manager.mu.Lock()
	delete(manager.conversationJobs, jobID)
	manager.forgetJobJournalLocked(jobID)
	manager.mu.Unlock()
	slog.ErrorContext(ctx, "job.failed", "component", "daemon", "subcomponent", "jobs", "job_id", jobID, "trace_id", traceID, "transient", transient, "err", runErr)
	if journalErr != nil {
		slog.ErrorContext(ctx, "append failed job event failed", "job_id", jobID, "err", journalErr)
	}
	return job, true
}

func (manager *Manager) writeJobTransition(event model.JobEvent) error {
	if manager.jobJournal != nil && manager.appendJobTransition != nil {
		return manager.appendJobTransition(event)
	}
	return appendJobJournalEvent(manager.config.JobsPath, manager.appendJobEvent, event)
}

func journalTransitionFailure(code string, cause error) error {
	return &adapterr.AdapterError{
		Class:         adapterr.ClassInternal,
		Message:       code,
		Code:          code,
		Hint:          "",
		Cause:         cause,
		SafeForClient: false,
	}
}
