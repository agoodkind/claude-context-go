package daemon

import (
	"context"
	"log/slog"

	"goodkind.io/claude-context-go/internal/clock"
	"goodkind.io/claude-context-go/internal/indexer"
	"goodkind.io/claude-context-go/internal/merkle"
	"goodkind.io/claude-context-go/internal/metrics"
	"goodkind.io/claude-context-go/internal/model"
	"goodkind.io/claude-context-go/internal/store"
	"goodkind.io/gklog/correlation"
)

func (manager *Manager) updateJobRunning(job model.Job) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	currentJob, found := manager.jobs[job.ID]
	if !found {
		return
	}
	now := clock.Now()
	currentJob.State = model.JobStateRunning
	currentJob.UpdatedAt = now
	currentJob.Progress.Phase = "Preparing and scanning files..."
	currentJob.Progress.LastEventAt = now
	currentJob.Progress.HeartbeatAt = now
	currentJob.Progress.OverallPercent = 0
	_ = manager.appendJobLocked("job_running", currentJob)
}

func (manager *Manager) updateJobProgress(jobID string, progress indexer.Progress) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	job, found := manager.jobs[jobID]
	if !found {
		return
	}
	if job.State != model.JobStateQueued && job.State != model.JobStateRunning && job.State != model.JobStateCancelling {
		return
	}

	now := clock.Now()
	job.State = model.JobStateRunning
	job.UpdatedAt = now
	job.Progress.Phase = progress.Phase
	job.Progress.OverallPercent = progress.OverallPercent
	job.Progress.FilesTotal = progress.FilesTotal
	job.Progress.FilesProcessed = progress.FilesProcessed
	job.Progress.ChunksGenerated = progress.ChunksGenerated
	job.Progress.LastEventAt = now
	job.Progress.HeartbeatAt = now
	manager.jobs[jobID] = job
}

func (manager *Manager) updateJobCompleted(jobID string, result indexer.Result) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	job, found := manager.jobs[jobID]
	if !found {
		return
	}

	now := clock.Now()
	metrics.JobCompleted()
	job.State = model.JobStateCompleted
	job.UpdatedAt = now
	job.CompletedAt = &now
	job.Progress.Phase = "completed"
	job.Progress.OverallPercent = 100
	job.Progress.FilesProcessed = result.IndexedFiles
	job.Progress.FilesTotal = result.IndexedFiles
	job.Progress.ChunksGenerated = result.TotalChunks
	job.Progress.LastEventAt = now
	job.Progress.HeartbeatAt = now
	if err := manager.appendJobLocked("job_completed", job); err != nil {
		slog.Error("append completed job event failed", "job_id", jobID, "err", err)
	}

	codebase, found := manager.codebases[job.CodebaseID]
	if !found {
		return
	}
	codebase.Status = model.CodebaseStatusIndexed
	codebase.ActiveJobID = ""
	codebase.LastSuccessfulRun = &model.IndexRunSummary{
		IndexedFiles: result.IndexedFiles,
		TotalChunks:  result.TotalChunks,
		Status:       "completed",
		CompletedAt:  now,
		SkippedFiles: result.SkippedFiles,
	}
	codebase.MerkleSnapshotPath = manager.merklePath(codebase.ID)
	codebase.UpdatedAt = now
	manager.codebases[codebase.ID] = codebase
	chunkPath := manager.chunkPath(codebase.ID)
	if err := store.WriteChunks(chunkPath, result.Chunks); err != nil {
		slog.Error("write chunk cache failed", "job_id", jobID, "err", err)
	}
	if len(result.FileHashes) != 0 {
		snapshot := merkle.Snapshot{ConfigDigest: codebase.EffectiveConfig.IgnoreDigest, Files: result.FileHashes, Inodes: nil}
		if err := merkle.WriteSnapshot(codebase.MerkleSnapshotPath, snapshot); err != nil {
			slog.Error("write Merkle snapshot failed", "job_id", jobID, "err", err)
		}
	}
	if err := manager.saveLocked(); err != nil {
		slog.Error("write registry after completed job failed", "job_id", jobID, "err", err)
	}
}

func (manager *Manager) updateJobFailed(ctx context.Context, jobID string, runErr error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	job, found := manager.jobs[jobID]
	if !found {
		return
	}

	traceID := string(correlation.FromContext(ctx).TraceID)
	now := clock.Now()
	metrics.JobFailed()
	job.State = model.JobStateFailed
	job.UpdatedAt = now
	job.CompletedAt = &now
	job.Progress.Phase = "failed"
	job.Progress.LastEventAt = now
	job.Progress.HeartbeatAt = now
	job.Error = &model.JobError{
		Message:   runErr.Error(),
		Retryable: false,
		TraceID:   traceID,
		JobID:     jobID,
	}
	slog.ErrorContext(ctx, "job.failed", "component", "daemon", "subcomponent", "jobs", "job_id", jobID, "trace_id", traceID, "err", runErr)
	if err := manager.appendJobLocked("job_failed", job); err != nil {
		slog.ErrorContext(ctx, "append failed job event failed", "job_id", jobID, "err", err)
	}

	codebase, found := manager.codebases[job.CodebaseID]
	if !found {
		return
	}
	codebase.Status = model.CodebaseStatusFailed
	codebase.ActiveJobID = ""
	codebase.LastFailedRun = &model.IndexRunFailure{
		Message:                 runErr.Error(),
		LastAttemptedPercentage: 0,
		FailedAt:                now,
		TraceID:                 traceID,
		JobID:                   jobID,
	}
	codebase.UpdatedAt = now
	manager.codebases[codebase.ID] = codebase
	if err := manager.saveLocked(); err != nil {
		slog.ErrorContext(ctx, "write registry after failed job failed", "job_id", jobID, "err", err)
	}
}

func (manager *Manager) updateJobCancelled(ctx context.Context, jobID string) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	job, found := manager.jobs[jobID]
	if !found {
		return
	}

	traceID := string(correlation.FromContext(ctx).TraceID)
	now := clock.Now()
	metrics.JobCancelled()
	job.State = model.JobStateCancelled
	job.UpdatedAt = now
	job.CompletedAt = &now
	job.Progress.Phase = "cancelled"
	job.Progress.LastEventAt = now
	job.Progress.HeartbeatAt = now
	if err := manager.appendJobLocked("job_cancelled", job); err != nil {
		slog.ErrorContext(ctx, "append cancelled job event failed", "job_id", jobID, "err", err)
	}

	codebase, found := manager.codebases[job.CodebaseID]
	if !found {
		return
	}
	codebase.Status = model.CodebaseStatusFailed
	codebase.ActiveJobID = ""
	codebase.LastFailedRun = &model.IndexRunFailure{
		Message:                 "job cancelled",
		LastAttemptedPercentage: 0,
		FailedAt:                now,
		TraceID:                 traceID,
		JobID:                   jobID,
	}
	codebase.UpdatedAt = now
	manager.codebases[codebase.ID] = codebase
	if err := manager.saveLocked(); err != nil {
		slog.ErrorContext(ctx, "write registry after cancelled job failed", "job_id", jobID, "err", err)
	}
}
