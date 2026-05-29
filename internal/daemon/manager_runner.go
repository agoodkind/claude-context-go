package daemon

import (
	"context"
	"fmt"
	"log/slog"

	"goodkind.io/claude-context-go/internal/metrics"
	"goodkind.io/claude-context-go/internal/spans"
	"goodkind.io/gklog/correlation"
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
		// The slot is acquired inside the goroutine so callers never block on
		// the cap; the job stays JobStateQueued until runJob calls
		// updateJobRunning, so a queued-behind-the-cap job reports queued.
		select {
		case manager.indexSlots <- struct{}{}:
			defer func() { <-manager.indexSlots }()
			manager.runJob(backgroundContext, jobID)
		case <-backgroundContext.Done():
			manager.updateJobCancelled(backgroundContext, jobID)
			return
		}
	}()
}

func (manager *Manager) runJob(ctx context.Context, jobID string) {
	ctx, done := spans.Open(ctx, "daemon.runJob")
	defer done(nil)

	metrics.JobStarted()
	defer metrics.JobFinished()

	manager.mu.Lock()
	job, found := manager.jobs[jobID]
	manager.mu.Unlock()
	if !found {
		return
	}

	manager.updateJobRunning(job)

	// Every operation reaches a terminal job state below. An incremental sync
	// or streaming reindex that finds no usable delta (no prior snapshot, or a
	// live collection that has gone missing) falls through to the from-scratch
	// staging build, which is also the path a true first index and a forced
	// rebuild take.
	switch jobOperation(job.Operation) {
	case jobOperationSync:
		if manager.runDeltaSync(ctx, job) {
			return
		}
		manager.runBootstrap(ctx, job)
	case jobOperationStreamingReindex:
		if manager.runDeltaSync(ctx, job) {
			return
		}
		manager.runBootstrap(ctx, job)
	case jobOperationIndex:
		manager.runBootstrap(ctx, job)
	}
}
