package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"goodkind.io/claude-context-go/internal/indexer"
	"goodkind.io/claude-context-go/internal/semantic"
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
		manager.runJob(backgroundContext, jobID)
	}()
}

func (manager *Manager) runJob(ctx context.Context, jobID string) {
	ctx, done := spans.Open(ctx, "daemon.runJob")
	defer done(nil)

	manager.mu.Lock()
	job, found := manager.jobs[jobID]
	manager.mu.Unlock()
	if !found {
		return
	}

	manager.updateJobRunning(job)

	switch jobOperation(job.Operation) {
	case jobOperationSync:
		if manager.runDeltaSync(ctx, job) {
			return
		}
	case jobOperationStreamingReindex:
		manager.runDeltaSync(ctx, job)
		return
	case jobOperationIndex:
	}

	result, err := manager.runner.Index(ctx, job.CanonicalPath, job.Config, func(progress indexer.Progress) {
		manager.updateJobProgress(job.ID, progress)
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			manager.updateJobCancelled(ctx, job.ID)
			return
		}
		manager.updateJobFailed(ctx, job.ID, err)
		return
	}
	if manager.semantic != nil && manager.semantic.Available() {
		err = manager.semantic.Replace(ctx, job.CanonicalPath, result.Chunks, func(progress semantic.Progress) {
			manager.updateJobSemanticProgress(job.ID, progress)
		})
		if err != nil {
			manager.updateJobFailed(ctx, job.ID, err)
			return
		}
	}
	manager.updateJobCompleted(job.ID, result)
}
