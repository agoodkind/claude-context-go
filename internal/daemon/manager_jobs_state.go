package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"

	"goodkind.io/lm-semantic-search/internal/adapterr"
	"goodkind.io/lm-semantic-search/internal/clock"
	"goodkind.io/lm-semantic-search/internal/indexer"
	"goodkind.io/lm-semantic-search/internal/merkle"
	"goodkind.io/lm-semantic-search/internal/metrics"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/store"
)

type bootstrapReason string

var errRetryJobStart = errors.New("retry job start persistence")

const (
	bootstrapReasonFirstIndex                 bootstrapReason = "first_index"
	bootstrapReasonForcedReindex              bootstrapReason = "forced_reindex"
	bootstrapReasonStagingResume              bootstrapReason = "staging_resume"
	bootstrapReasonEmptyDiffCollectionMissing bootstrapReason = "empty_diff_collection_missing"
	bootstrapReasonEmptyDiffCollectionEmpty   bootstrapReason = "empty_diff_collection_empty"
	bootstrapReasonDeltaCollectionMissing     bootstrapReason = "delta_collection_missing"
	bootstrapReasonDeltaCodebaseMissing       bootstrapReason = "delta_codebase_missing"
)

func (manager *Manager) updateJobRunning(job model.Job) error {
	manager.policyMutationMutex.Lock()
	defer manager.policyMutationMutex.Unlock()

	manager.transitionMutex.Lock()
	defer manager.transitionMutex.Unlock()
	manager.mu.Lock()

	currentJob, found := manager.jobs[job.ID]
	if !found {
		manager.mu.Unlock()
		return fmt.Errorf("start job: job missing")
	}
	now := clock.Now()
	// A first build was pending while its job sat queued; now that the job is
	// running, the codebase is actively indexing. Persist this transition before
	// journaling the running job so a crash leaves boot recovery a resumable
	// registry state. A rebuild was already indexing.
	codebase, found := manager.codebases[currentJob.CodebaseID]
	if !found {
		manager.mu.Unlock()
		return fmt.Errorf("start job: codebase missing")
	}
	if codebase.ActiveJobID != currentJob.ID {
		manager.mu.Unlock()
		return fmt.Errorf("start job: codebase ownership changed")
	}
	previousCodebase := codebase
	switch codebase.Status {
	case model.CodebaseStatusPending:
		codebase.Status = model.CodebaseStatusIndexing
		codebase.UpdatedAt = now
		manager.codebases[codebase.ID] = codebase
		if err := manager.saveLocked(); err != nil {
			manager.codebases[codebase.ID] = previousCodebase
			manager.mu.Unlock()
			wrapped := errors.Join(errRetryJobStart, fmt.Errorf("persist running codebase state: %w", err))
			slog.Error("persist running codebase state", "err", wrapped, "codebase_id", codebase.ID, "job_id", currentJob.ID)
			return wrapped
		}
	case model.CodebaseStatusIndexing:
		if err := manager.saveLocked(); err != nil {
			manager.mu.Unlock()
			wrapped := errors.Join(errRetryJobStart, fmt.Errorf("revalidate running codebase state: %w", err))
			slog.Error("revalidate running codebase state", "err", wrapped, "codebase_id", codebase.ID, "job_id", currentJob.ID)
			return wrapped
		}
	case model.CodebaseStatusNotIndexed,
		model.CodebaseStatusIndexed,
		model.CodebaseStatusFailed,
		model.CodebaseStatusStale,
		model.CodebaseStatusMissing,
		model.CodebaseStatusDiscovered,
		model.CodebaseStatusQuarantined:
		manager.mu.Unlock()
		return fmt.Errorf("start job: codebase status %q cannot run", codebase.Status)
	default:
		manager.mu.Unlock()
		return fmt.Errorf("start job: codebase status %q cannot run", codebase.Status)
	}
	manager.mu.Unlock()

	_, transitioned, transitionErr := manager.serializeJobTransitionLocked(
		job.ID,
		"job_running",
		func(current *model.Job) bool {
			if current.State != model.JobStateQueued {
				return false
			}
			current.State = model.JobStateRunning
			current.SchedulingReason = model.SchedulingReasonUnspecified
			current.UpdatedAt = now
			current.Progress.Phase = "Preparing and scanning files..."
			current.Progress.LastEventAt = now
			current.Progress.HeartbeatAt = now
			current.Progress.OverallPercent = 0
			return true
		},
	)
	if transitionErr != nil {
		manager.mu.Lock()
		manager.jobs[currentJob.ID] = currentJob
		manager.mu.Unlock()
		return errors.Join(errRetryJobStart, fmt.Errorf("append running job event: %w", transitionErr))
	}
	if !transitioned {
		return fmt.Errorf("start job: job state changed")
	}
	return nil
}

func (manager *Manager) updateJobProgress(jobID string, progress indexer.Progress, unit string) {
	manager.transitionMutex.Lock()
	defer manager.transitionMutex.Unlock()
	manager.mu.Lock()
	defer manager.mu.Unlock()

	job, found := manager.jobs[jobID]
	if !found {
		return
	}
	delete(manager.conversationJobs, jobID)
	if job.State != model.JobStateQueued && job.State != model.JobStateRunning && job.State != model.JobStateCancelling {
		return
	}

	now := clock.Now()
	if job.State == model.JobStateQueued {
		job.State = model.JobStateRunning
	}
	job.UpdatedAt = now
	job.Progress.Phase = progress.Phase
	job.Progress.OverallPercent = progress.OverallPercent
	if unit != "" {
		job.Progress.Unit = unit
		if unit == "document" {
			job.Progress.ScopeUnit = "conversation"
		}
	}
	job.Progress.FilesTotal = progress.FilesTotal
	job.Progress.FilesProcessed = progress.FilesProcessed
	job.Progress.FilesEmbedded = progress.FilesEmbedded
	job.Progress.FilesSkippedOversize = progress.FilesSkippedOversize
	job.Progress.FilesSkippedUnreadable = progress.FilesSkippedUnreadable
	job.Progress.FilesPending = progress.FilesPending
	job.Progress.ChunksProcessed = progress.ChunksProcessed
	job.Progress.ChunksReused = progress.ChunksReused
	job.Progress.ChunksEmbedded = progress.ChunksEmbedded
	job.Progress.ChunksGenerated = progress.ChunksGenerated
	job.Progress.ChunksDropped = progress.ChunksDropped
	job.Progress.ReuseVectorsLoaded = progress.ReuseVectorsLoaded
	job.Progress.LastEventAt = now
	job.Progress.HeartbeatAt = now
	manager.jobs[jobID] = job
	manager.updateCodebaseLiveTotalsLocked(job)
	manager.journalJobProgressLocked(job)

	// A progress update that embedded a file proves the embedding pipeline is
	// reachable right now, so clear a degraded banner as soon as embedding
	// resumes rather than waiting for the job to complete. Without this a long
	// recovering build would keep showing a stale "paused" banner while it is
	// visibly making progress. A reuse-only or no-op update embeds nothing, so it
	// leaves the banner untouched.
	if progress.FilesEmbedded > 0 {
		manager.noteDependencyHealthyLocked()
	}
}

func (manager *Manager) updateDetachedJobProgress(jobID string, progress indexer.Progress, unit string) {
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
	if job.State == model.JobStateQueued {
		job.State = model.JobStateRunning
	}
	job.UpdatedAt = now
	job.Progress.Phase = progress.Phase
	job.Progress.OverallPercent = progress.OverallPercent
	if unit != "" {
		job.Progress.Unit = unit
	}
	job.Progress.FilesTotal = progress.FilesTotal
	job.Progress.FilesProcessed = progress.FilesProcessed
	job.Progress.FilesEmbedded = progress.FilesEmbedded
	job.Progress.FilesSkippedOversize = progress.FilesSkippedOversize
	job.Progress.FilesSkippedUnreadable = progress.FilesSkippedUnreadable
	job.Progress.FilesPending = progress.FilesPending
	job.Progress.ChunksProcessed = progress.ChunksProcessed
	job.Progress.ChunksReused = progress.ChunksReused
	job.Progress.ChunksEmbedded = progress.ChunksEmbedded
	job.Progress.ChunksGenerated = progress.ChunksGenerated
	job.Progress.ChunksDropped = progress.ChunksDropped
	job.Progress.ReuseVectorsLoaded = progress.ReuseVectorsLoaded
	job.Progress.LastEventAt = now
	job.Progress.HeartbeatAt = now
	manager.jobs[jobID] = job
	manager.journalJobProgressLocked(job)
}

func (manager *Manager) updateDetachedJobHeartbeat(jobID string) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	job, found := manager.jobs[jobID]
	if !found {
		return
	}
	if job.State != model.JobStateQueued && job.State != model.JobStateRunning && job.State != model.JobStateCancelling {
		return
	}
	job.Progress.HeartbeatAt = clock.Now()
	manager.jobs[jobID] = job
}

// updateJobChunkProgress advances the chunk counters, the current item's embed
// batch denominator, and the heartbeat during a single item's embed loop. It is
// called once per embed batch, so a long item (a large conversation with many
// chunks) shows visible forward movement and a fresh heartbeat instead of
// sitting frozen until the item finishes. It deliberately leaves the file
// counters and the change breakdown alone, since reportDeltaProgress owns the
// per-file totals and setJobDeltaCounts owns the added/modified/removed counts.
func (manager *Manager) updateJobChunkProgress(jobID string, processed int32, reused int32, embedded int32, dropped int32, batchesTotal int32, batchesCompleted int32, rowsWritten int32) {
	manager.transitionMutex.Lock()
	defer manager.transitionMutex.Unlock()
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
	// Promote a queued job to running on its first progress, but never override a
	// cancelling job back to running: a batch that was already in flight when the
	// operator cancelled must not resurrect the run in the status.
	if job.State == model.JobStateQueued {
		job.State = model.JobStateRunning
	}
	job.UpdatedAt = now
	job.Progress.ChunksProcessed = processed
	job.Progress.ChunksReused = reused
	job.Progress.ChunksEmbedded = embedded
	job.Progress.ChunksGenerated = embedded
	job.Progress.ChunksDropped = dropped
	job.Progress.EmbeddingBatchesTotal = batchesTotal
	job.Progress.EmbeddingBatchesCompleted = batchesCompleted
	job.Progress.CollectionRowsWritten = rowsWritten
	job.Progress.LastEventAt = now
	job.Progress.HeartbeatAt = now
	manager.jobs[jobID] = job
	manager.journalJobProgressLocked(job)
}

func (manager *Manager) updateCodebaseLiveTotalsLocked(job model.Job) {
	codebase, found := manager.codebases[job.CodebaseID]
	if !found {
		return
	}
	changed := false
	lastRun := codebase.LastSuccessfulRun
	if lastRun != nil && codebase.LiveFileTotal == 0 {
		codebase.LiveFileTotal = lastRun.IndexedFiles
		changed = true
	}
	if lastRun != nil && codebase.LiveChunkTotal == 0 {
		codebase.LiveChunkTotal = lastRun.TotalChunks
		changed = true
	}
	liveFiles := job.Progress.FilesInCodebase
	if liveFiles == 0 {
		liveFiles = job.Progress.FilesTotal
	}
	if liveFiles > 0 && codebase.LiveFileTotal != liveFiles {
		codebase.LiveFileTotal = liveFiles
		changed = true
	}
	liveChunks := max(job.Progress.ChunksTotal, max(job.Progress.ChunksProcessed, job.Progress.ChunksReused+job.Progress.ChunksEmbedded))
	if liveChunks > codebase.LiveChunkTotal {
		codebase.LiveChunkTotal = liveChunks
		changed = true
	}
	if !changed {
		return
	}
	manager.codebases[job.CodebaseID] = codebase
}

// setJobDeltaCounts records how many files a delta sync will add, modify, and
// remove, plus the codebase file total, so the status and job views can report
// the magnitude of a reconcile (for example after a large merge). The counts
// are set once when the diff is known; updateJobProgress sets only the embed
// counters, so these survive the per-file progress updates.
func (manager *Manager) setJobDeltaCounts(jobID string, added int, modified int, removed int, filesInCodebase int) {
	manager.transitionMutex.Lock()
	defer manager.transitionMutex.Unlock()
	manager.mu.Lock()
	defer manager.mu.Unlock()

	job, found := manager.jobs[jobID]
	if !found {
		return
	}
	job.Progress.FilesAdded = safeInt32(added)
	job.Progress.FilesModified = safeInt32(modified)
	job.Progress.FilesRemoved = safeInt32(removed)
	job.Progress.FilesInCodebase = safeInt32(filesInCodebase)
	manager.jobs[jobID] = job
}

// setJobRunMode records the kind of pass a run is making, decided once when
// the plan is chosen, so surfaces can label denominators and name a resume.
func (manager *Manager) setJobRunMode(jobID string, runMode string) {
	manager.transitionMutex.Lock()
	defer manager.transitionMutex.Unlock()
	manager.mu.Lock()
	defer manager.mu.Unlock()

	job, found := manager.jobs[jobID]
	if !found {
		return
	}
	job.Progress.RunMode = runMode
	manager.jobs[jobID] = job
}

// routeToBootstrap records the machine-readable reason before a job enters the
// expensive bootstrap path.
func (manager *Manager) routeToBootstrap(ctx context.Context, jobID string, reason bootstrapReason) {
	caller := bootstrapRouteCaller()
	if !reason.known() {
		slog.WarnContext(ctx, "unknown bootstrap reason", "reason", reason, "caller", caller, "job_id", jobID)
	}

	manager.transitionMutex.Lock()
	defer manager.transitionMutex.Unlock()
	manager.mu.Lock()
	job, found := manager.jobs[jobID]
	if !found {
		manager.mu.Unlock()
		return
	}
	job.Progress.BootstrapReason = string(reason)
	codebaseID := job.CodebaseID
	manager.jobs[jobID] = job
	manager.mu.Unlock()

	slog.InfoContext(ctx, "bootstrap.route", "reason", reason, "caller", caller, "codebase_id", codebaseID, "job_id", jobID)
}

func (reason bootstrapReason) known() bool {
	switch reason {
	case bootstrapReasonFirstIndex,
		bootstrapReasonForcedReindex,
		bootstrapReasonStagingResume,
		bootstrapReasonEmptyDiffCollectionMissing,
		bootstrapReasonEmptyDiffCollectionEmpty,
		bootstrapReasonDeltaCollectionMissing,
		bootstrapReasonDeltaCodebaseMissing:
		return true
	default:
		return false
	}
}

func bootstrapRouteCaller() string {
	pc, _, _, ok := runtime.Caller(2)
	if !ok {
		return ""
	}
	function := runtime.FuncForPC(pc)
	if function == nil {
		return ""
	}
	return function.Name()
}

func (manager *Manager) updateJobCompleted(ctx context.Context, jobID string, result indexer.Result) {
	manager.policyMutationMutex.Lock()
	followup := manager.updateJobCompletedWithPolicy(ctx, jobID, result)
	manager.policyMutationMutex.Unlock()
	manager.runDrainedFollowup(ctx, followup)
}

func (manager *Manager) updateJobCompletedWithPolicy(ctx context.Context, jobID string, result indexer.Result) cancellationFollowup {
	job, jobEvent, transitioned := manager.prepareTerminalJobTransition(
		jobID,
		"job_completed",
		func(job *model.Job) bool {
			if job.State == model.JobStateCancelled {
				return false
			}
			if job.State == model.JobStateCancelling {
				return false
			}
			now := clock.Now()
			job.State = model.JobStateCompleted
			job.UpdatedAt = now
			job.CompletedAt = &now
			job.Progress.Phase = "completed"
			job.Progress.OverallPercent = 100
			job.Progress.FilesProcessed = result.IndexedFiles
			job.Progress.FilesTotal = result.IndexedFiles
			job.Progress.ChunksTotal = result.TotalChunks
			job.Progress.ChunksGenerated = job.Progress.ChunksEmbedded
			job.Progress.LastEventAt = now
			job.Progress.HeartbeatAt = now
			return true
		},
	)
	if !transitioned {
		manager.mu.Lock()
		current, found := manager.jobs[jobID]
		manager.mu.Unlock()
		if found && current.State == model.JobStateCancelling {
			return manager.updateJobCancelledWithPolicy(ctx, jobID)
		}
		return emptyCancellationFollowup()
	}
	metrics.JobCompleted()
	if job.Progress.FilesEmbedded > 0 {
		manager.mu.Lock()
		manager.noteDependencyHealthyLocked()
		manager.mu.Unlock()
	}
	now := *job.CompletedAt
	manager.mu.Lock()
	manager.forgetJobJournalLocked(jobID)
	codebase, found := manager.codebases[job.CodebaseID]
	if !found {
		manager.mu.Unlock()
		return emptyCancellationFollowup()
	}
	delete(manager.failedBuildRetries, codebase.ID)
	codebase.Status = model.CodebaseStatusIndexed
	// Clear ActiveJobID only when it still points at this job, so a raced or
	// duplicate terminal transition never clobbers a drained successor.
	if codebase.ActiveJobID == jobID {
		codebase.ActiveJobID = ""
	}
	codebase.Quarantine = nil
	codebase.LastSuccessfulRun = &model.IndexRunSummary{
		IndexedFiles: result.IndexedFiles,
		TotalChunks:  result.TotalChunks,
		TotalBytes:   result.TotalBytes,
		Status:       "completed",
		CompletedAt:  now,
		SkippedFiles: result.SkippedFiles,
	}
	codebase.LiveFileTotal = result.IndexedFiles
	codebase.LiveChunkTotal = result.TotalChunks
	codebase.MerkleSnapshotPath = manager.merklePath(codebase.ID)
	codebase.UpdatedAt = now
	manager.codebases[codebase.ID] = codebase
	manager.writeCompletedArtifacts(ctx, codebase, result, jobID)
	if err := manager.saveLocked(); err != nil {
		slog.ErrorContext(ctx, "write registry after completed job failed", "job_id", jobID, "err", err)
	}
	// drainPendingJobLocked no-ops unless ActiveJobID was cleared above, so a raced
	// transition that did not own the slot never drains a duplicate.
	drainedJobID, drained := manager.drainPendingJobLocked(ctx, codebase.ID)
	manager.mu.Unlock()
	if journalErr := manager.writeJobTransition(jobEvent); journalErr != nil {
		slog.ErrorContext(ctx, "append completed job event failed", "job_id", jobID, "err", journalErr)
	}
	manager.notifyIndexReady(ctx, codebase)
	return cancellationFollowup{
		codebaseID:   codebase.ID,
		drainedJobID: drainedJobID,
		drained:      drained,
	}
}

// writeCompletedArtifacts persists the chunk cache and Merkle snapshot for a
// completed job. Code codebases keep the whole-result chunk cache write; legacy
// registry entries have an empty Kind and are treated as code.
func (manager *Manager) writeCompletedArtifacts(ctx context.Context, codebase model.Codebase, result indexer.Result, jobID string) {
	if codebase.Kind != model.CodebaseKindDocument {
		chunkPath := manager.chunkPath(codebase.ID)
		if err := store.WriteChunks(chunkPath, result.Chunks); err != nil {
			slog.ErrorContext(ctx, "write chunk cache failed", "job_id", jobID, "err", err)
		}
	}
	if len(result.FileHashes) != 0 {
		snapshot := merkle.Snapshot{ConfigDigest: codebase.EffectiveConfig.IgnoreDigest, Files: result.FileHashes, Inodes: nil}
		if err := merkle.WriteSnapshot(codebase.MerkleSnapshotPath, snapshot); err != nil {
			slog.ErrorContext(ctx, "write Merkle snapshot failed", "job_id", jobID, "err", err)
		}
	}
}

func (manager *Manager) updateJobFailed(ctx context.Context, jobID string, runErr error) {
	manager.policyMutationMutex.Lock()

	job, jobEvent, transitioned := manager.prepareFailedJobTransition(ctx, jobID, runErr)
	if !transitioned {
		manager.policyMutationMutex.Unlock()
		return
	}
	metrics.JobFailed()

	manager.mu.Lock()
	delete(manager.conversationJobs, jobID)
	manager.forgetJobJournalLocked(jobID)
	infra := adapterr.IsInfraFailure(runErr)
	safeMessage := ""
	errorCode := ""
	traceID := ""
	now := *job.CompletedAt
	if job.Error != nil {
		safeMessage = job.Error.Message
		errorCode = job.Error.Code
		traceID = job.Error.TraceID
	}
	codebase, found := manager.codebases[job.CodebaseID]
	if !found {
		manager.mu.Unlock()
		if journalErr := manager.writeJobTransition(jobEvent); journalErr != nil {
			slog.ErrorContext(ctx, "append failed job event failed", "job_id", jobID, "err", journalErr)
		}
		manager.policyMutationMutex.Unlock()
		return
	}
	// Clear ActiveJobID only when it still points at this job, so a raced or
	// duplicate terminal transition never clobbers a drained successor.
	if codebase.ActiveJobID == jobID {
		codebase.ActiveJobID = ""
	}
	switch {
	case infra:
		// A shared-infrastructure failure is not the codebase's fault and never
		// terminal; keep the codebase at its resumable last-good state. The repair
		// pass re-attempts it once the dependency recovers, and the health banner
		// carries the cause.
		manager.noteDependencyFailureLocked(runErr)
	case codebase.Kind != model.CodebaseKindDocument && sourceDirMissing(codebase.CanonicalPath):
		// The source directory vanished mid-run. This is not a build failure, so
		// present it as missing and keep the index in case the directory returns.
		codebase.Status = model.CodebaseStatusMissing
		codebase.LastFailedRun = nil
	default:
		codebase.Status = model.CodebaseStatusFailed
		codebase.LastFailedRun = &model.IndexRunFailure{
			Message:                 safeMessage,
			Code:                    errorCode,
			LastAttemptedPercentage: 0,
			FailedAt:                now,
			TraceID:                 traceID,
			JobID:                   jobID,
		}
	}
	codebase.UpdatedAt = now
	manager.codebases[codebase.ID] = codebase
	if err := manager.saveLocked(); err != nil {
		slog.ErrorContext(ctx, "write registry after failed job failed", "job_id", jobID, "err", err)
	}
	// drainPendingJobLocked no-ops unless ActiveJobID was cleared above.
	drainedJobID, drained := manager.drainPendingJobLocked(ctx, codebase.ID)
	codebaseID := codebase.ID
	manager.mu.Unlock()
	if journalErr := manager.writeJobTransition(jobEvent); journalErr != nil {
		slog.ErrorContext(ctx, "append failed job event failed", "job_id", jobID, "err", journalErr)
	}
	manager.policyMutationMutex.Unlock()
	manager.notifyIndexStopped(ctx, codebaseID)
	if drained {
		manager.runDrainedJob(ctx, codebaseID, drainedJobID)
	}
}

func (manager *Manager) updateDetachedJobFailed(ctx context.Context, jobID string, runErr error) {
	job, transitioned := manager.failJobTransition(ctx, jobID, runErr)
	if transitioned {
		manager.finishDetachedCodebase(ctx, job)
	}
}

func (manager *Manager) updateDetachedJobCompleted(ctx context.Context, jobID string, result indexer.Result) {
	job, transitioned, journalErr := manager.serializeJobTransition(
		jobID,
		"job_completed",
		func(job *model.Job) bool {
			if isTerminalJobState(job.State) {
				return false
			}
			now := clock.Now()
			job.State = model.JobStateCompleted
			job.UpdatedAt = now
			job.CompletedAt = &now
			job.Progress.Phase = "completed"
			job.Progress.OverallPercent = 100
			// Detached watcher converges report paths examined through their
			// progress callback. Result.IndexedFiles counts only paths that
			// changed the index, so it can be zero after a complete missing-path
			// converge. Terminal completion must not move those counters backward.
			job.Progress.FilesProcessed = max(job.Progress.FilesProcessed, result.IndexedFiles)
			job.Progress.FilesTotal = max(job.Progress.FilesTotal, result.IndexedFiles)
			job.Progress.ChunksTotal = result.TotalChunks
			job.Progress.ChunksGenerated = job.Progress.ChunksEmbedded
			job.Progress.LastEventAt = now
			job.Progress.HeartbeatAt = now
			return true
		},
	)
	if !transitioned {
		return
	}
	metrics.JobCompleted()
	if journalErr != nil {
		slog.ErrorContext(ctx, "append completed job event failed", "job_id", jobID, "err", journalErr)
	}
	manager.mu.Lock()
	manager.forgetJobJournalLocked(jobID)
	manager.mu.Unlock()
	manager.finishDetachedCodebase(ctx, job)
}

func (manager *Manager) updateDetachedJobCancelled(ctx context.Context, jobID string) {
	job, transitioned, journalErr := manager.serializeJobTransition(
		jobID,
		"job_cancelled",
		func(job *model.Job) bool {
			if isTerminalJobState(job.State) {
				return false
			}
			now := clock.Now()
			job.State = model.JobStateCancelled
			job.UpdatedAt = now
			job.CompletedAt = &now
			job.Progress.Phase = "cancelled"
			job.Progress.LastEventAt = now
			job.Progress.HeartbeatAt = now
			return true
		},
	)
	if !transitioned {
		return
	}
	metrics.JobCancelled()
	if journalErr != nil {
		slog.ErrorContext(ctx, "append cancelled job event failed", "job_id", jobID, "err", journalErr)
	}
	manager.mu.Lock()
	manager.forgetJobJournalLocked(jobID)
	manager.mu.Unlock()
	manager.finishDetachedCodebase(ctx, job)
}

func (manager *Manager) finishDetachedCodebase(ctx context.Context, job model.Job) {
	manager.mu.Lock()
	codebase, found := manager.codebases[job.CodebaseID]
	if !found || codebase.ActiveJobID != job.ID {
		manager.mu.Unlock()
		return
	}
	codebase.ActiveJobID = ""
	switch job.State {
	case model.JobStateCompleted:
		codebase.Status = model.CodebaseStatusIndexed
	case model.JobStateFailed:
		codebase.Status = model.CodebaseStatusFailed
	case model.JobStateCancelled:
		codebase.Status = codebaseStatusAfterCancellation(codebase)
	case model.JobStateQueued,
		model.JobStateRunning,
		model.JobStatePaused,
		model.JobStateCancelling:
		manager.mu.Unlock()
		return
	default:
		manager.mu.Unlock()
		return
	}
	codebase.UpdatedAt = clock.Now()
	manager.codebases[codebase.ID] = codebase
	if err := manager.saveLocked(); err != nil {
		slog.ErrorContext(ctx, "write registry after detached job terminal transition failed", "job_id", job.ID, "err", err)
	}
	drainedJobID, drained := manager.drainPendingJobLocked(ctx, codebase.ID)
	manager.mu.Unlock()
	manager.notifyIndexStopped(ctx, codebase.ID)
	if drained {
		manager.runDrainedJob(ctx, codebase.ID, drainedJobID)
	}
}

func (manager *Manager) updateJobCancelled(ctx context.Context, jobID string) {
	manager.policyMutationMutex.Lock()
	followup := manager.updateJobCancelledWithPolicy(ctx, jobID)
	manager.policyMutationMutex.Unlock()
	manager.runCancellationFollowup(ctx, followup)
}

type cancellationFollowup struct {
	codebaseID    string
	drainedJobID  string
	drained       bool
	notifyStopped bool
}

func emptyCancellationFollowup() cancellationFollowup {
	return cancellationFollowup{
		codebaseID:    "",
		drainedJobID:  "",
		drained:       false,
		notifyStopped: false,
	}
}

func (manager *Manager) runDrainedFollowup(
	ctx context.Context,
	followup cancellationFollowup,
) {
	if !followup.drained {
		return
	}
	manager.runDrainedJob(ctx, followup.codebaseID, followup.drainedJobID)
}

func (manager *Manager) runCancellationFollowup(
	ctx context.Context,
	followup cancellationFollowup,
) {
	if followup.notifyStopped {
		manager.notifyIndexStopped(ctx, followup.codebaseID)
	}
	if !followup.drained {
		return
	}
	manager.runDrainedJob(ctx, followup.codebaseID, followup.drainedJobID)
}

func (manager *Manager) updateJobCancelledWithPolicy(
	ctx context.Context,
	jobID string,
) cancellationFollowup {
	job, transitioned, journalErr := manager.serializeJobTransition(
		jobID,
		"job_cancelled",
		func(job *model.Job) bool {
			if isTerminalJobState(job.State) {
				return false
			}
			now := clock.Now()
			job.State = model.JobStateCancelled
			job.UpdatedAt = now
			job.CompletedAt = &now
			job.Progress.Phase = "cancelled"
			job.Progress.LastEventAt = now
			job.Progress.HeartbeatAt = now
			return true
		},
	)
	if !transitioned {
		return emptyCancellationFollowup()
	}
	metrics.JobCancelled()
	if journalErr != nil {
		slog.ErrorContext(ctx, "append cancelled job event failed", "job_id", jobID, "err", journalErr)
	}
	now := *job.CompletedAt
	manager.mu.Lock()
	delete(manager.conversationJobs, jobID)
	manager.forgetJobJournalLocked(jobID)
	codebase, found := manager.codebases[job.CodebaseID]
	if !found {
		manager.mu.Unlock()
		return emptyCancellationFollowup()
	}
	if codebase.ActiveJobID != jobID {
		manager.mu.Unlock()
		return emptyCancellationFollowup()
	}
	// A cancellation is not a failure: leave the codebase at its last-good state
	// so a status check reflects the current usable state, not a stale failure.
	// Clear ActiveJobID only when it still points at this job, so a raced or
	// duplicate terminal transition (an explicit CancelJob plus this context-cancel
	// path) never clobbers a drained successor.
	codebase.ActiveJobID = ""
	codebase.Status = codebaseStatusAfterCancellation(codebase)
	codebase.UpdatedAt = now
	manager.codebases[codebase.ID] = codebase
	if err := manager.saveLocked(); err != nil {
		slog.ErrorContext(ctx, "write registry after cancelled job failed", "job_id", jobID, "err", err)
	}
	// drainPendingJobLocked no-ops unless ActiveJobID was cleared above.
	drainedJobID, drained := manager.drainPendingJobLocked(ctx, codebase.ID)
	codebaseID := codebase.ID
	manager.mu.Unlock()
	return cancellationFollowup{
		codebaseID:    codebaseID,
		drainedJobID:  drainedJobID,
		drained:       drained,
		notifyStopped: true,
	}
}

func codebaseStatusAfterCancellation(codebase model.Codebase) model.CodebaseStatus {
	if codebase.Status != model.CodebaseStatusPending && codebase.Status != model.CodebaseStatusIndexing {
		return codebase.Status
	}
	if codebase.LastSuccessfulRun != nil {
		return model.CodebaseStatusIndexed
	}
	return model.CodebaseStatusNotIndexed
}

func waitForJobDone(ctx context.Context, jobDone chan struct{}) error {
	if jobDone == nil {
		return nil
	}

	select {
	case <-jobDone:
		return nil
	case <-ctx.Done():
		slog.ErrorContext(ctx, "wait for active job cancellation failed", "err", ctx.Err())
		return fmt.Errorf("wait for active job cancellation: %w", ctx.Err())
	}
}
