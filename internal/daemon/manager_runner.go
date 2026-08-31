package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"goodkind.io/gklog/correlation"
	"goodkind.io/lm-semantic-search/internal/metrics"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/spans"
)

const (
	jobStartRetryAttempts = 3
	jobStartRetryDelay    = time.Second
)

func (manager *Manager) runJobAsync(ctx context.Context, jobID string) {
	detachedCorr := correlation.FromContext(ctx).Child()
	backgroundContext, cancel := context.WithCancel(
		correlation.WithContext(context.WithoutCancel(ctx), detachedCorr),
	)
	done := make(chan struct{})

	manager.mu.Lock()
	manager.cancels[jobID] = cancel
	manager.done[jobID] = done
	manager.mu.Unlock()

	go func() {
		defer func() {
			cancel()
			if recovered := recover(); recovered != nil {
				slog.ErrorContext(backgroundContext, "indexing goroutine panic", "err", fmt.Errorf("panic: %v", recovered), "job_id", jobID)
			}
			manager.mu.Lock()
			delete(manager.cancels, jobID)
			delete(manager.done, jobID)
			manager.mu.Unlock()
			close(done)
		}()
		for attempt := 1; attempt <= jobStartRetryAttempts; attempt++ {
			manager.mu.Lock()
			job, found := manager.jobs[jobID]
			manager.mu.Unlock()
			if !found {
				return
			}
			if isTerminalJobState(job.State) {
				return
			}
			holdSyncLock := manager.semantic != nil && manager.semantic.Available()
			capacity, outcome, capacityErr := manager.acquireJobCapacity(
				backgroundContext,
				job,
				holdSyncLock,
			)
			if outcome != syncLockAcquired {
				manager.finishCapacityAdmissionFailure(
					backgroundContext,
					jobID,
					outcome,
					capacityErr,
				)
				return
			}
			lockOutcome, lockErr := manager.refreshSyncLockAfterAdmission(
				backgroundContext,
				capacity,
				holdSyncLock,
			)
			if lockOutcome != syncLockAcquired {
				capacity.release(context.WithoutCancel(backgroundContext))
				manager.finishCapacityAdmissionFailure(
					backgroundContext,
					jobID,
					lockOutcome,
					lockErr,
				)
				return
			}
			runContext := withJobCapacity(backgroundContext, capacity)
			runContext = withJobSchedulerLease(runContext, jobID, capacity.lease)
			graphTask, retryStart := func() (*graphIndexTask, bool) {
				defer capacity.release(context.WithoutCancel(backgroundContext))
				return manager.runJob(runContext, jobID)
			}()
			if manager.retryJobStart(backgroundContext, jobID, retryStart, attempt) {
				continue
			}
			if manager.jobIsQueued(jobID) {
				manager.updateJobCancelled(backgroundContext, jobID)
				return
			}
			manager.runGraphIndexTask(backgroundContext, graphTask)
			return
		}
	}()
}

func (manager *Manager) jobIsQueued(jobID string) bool {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	job, found := manager.jobs[jobID]
	return found && job.State == model.JobStateQueued
}

func (manager *Manager) runJob(ctx context.Context, jobID string) (*graphIndexTask, bool) {
	ctx, done := spans.Open(ctx, "daemon.runJob")
	defer done(nil)

	metrics.JobStarted()
	defer metrics.JobFinished()

	manager.mu.Lock()
	job, found := manager.jobs[jobID]
	manager.mu.Unlock()
	if !found {
		return nil, false
	}

	if err := manager.updateJobRunning(job); err != nil {
		slog.ErrorContext(ctx, "start job persistence failed", "job_id", job.ID, "err", err)
		return nil, errors.Is(err, errRetryJobStart)
	}

	// Every operation reaches a terminal job state below. An incremental sync
	// or streaming reindex that finds no usable delta (no prior snapshot, or a
	// live collection that has gone missing) falls through to the from-scratch
	// staging build, which is also the path a true first index and a forced
	// rebuild take. A code job walks the filesystem through the code source; a
	// conversation ingest feeds the manifest and documents through its own source
	// in runConversationIngest, then shares the same delta-then-bootstrap routine.
	codeSource := newCodeItemSource(manager.runner, manager.indexability, job.CodebaseID, job.CanonicalPath, job.Config)
	if manager.semantic != nil && manager.semantic.Available() {
		codeSource = codeSource.withCollectionName(manager.semantic.CollectionName(job.CanonicalPath))
	}
	switch jobOperation(job.Operation) {
	case jobOperationSync:
		handled, graphTask := manager.runDeltaSync(ctx, job, codeSource)
		if handled {
			return graphTask, false
		}
		return manager.runBootstrap(ctx, job, codeSource), false
	case jobOperationStreamingReindex:
		handled, graphTask := manager.runDeltaSync(ctx, job, codeSource)
		if handled {
			return graphTask, false
		}
		return manager.runBootstrap(ctx, job, codeSource), false
	case jobOperationIndex:
		reason := bootstrapReasonFirstIndex
		if job.Forced {
			reason = bootstrapReasonForcedReindex
		}
		manager.routeToBootstrap(ctx, job.ID, reason)
		return manager.runBootstrap(ctx, job, codeSource), false
	case jobOperationConversationIngest:
		manager.runConversationIngest(ctx, job)
	}
	return nil, false
}

// JobSuccessorID returns the id of the immediate next terminal job for job's
// codebase, or empty when job is the latest terminal job. The single-job views
// use it since they do not hold the full job set the list view does.
func (manager *Manager) JobSuccessorID(job model.Job) string {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	codebaseJobs := make([]model.Job, 0)
	for _, candidate := range manager.jobs {
		if candidate.CodebaseID == job.CodebaseID {
			codebaseJobs = append(codebaseJobs, candidate)
		}
	}
	return buildJobSuccessors(codebaseJobs)[job.ID]
}
