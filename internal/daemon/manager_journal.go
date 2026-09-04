package daemon

import (
	"log/slog"
	"time"

	"goodkind.io/lm-semantic-search/internal/clock"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/store"
)

// jobProgressJournalInterval throttles how often a running job's progress is
// entered into the journal queue.
//
// A clean shutdown flushes the queue, so it loses nothing. A hard kill loses
// this interval of detail plus whatever the writer had not yet written, bounded
// by the queue capacity. Boot reconciliation then sanitizes any job left
// non-terminal, so the recovered record understates progress rather than
// misreporting it.
const jobProgressJournalInterval = 10 * time.Second

// journalJobProgressLocked appends a throttled job_progress event so a daemon
// crash mid-run preserves recent progress in the journal. Without it only the
// job_running event (zero progress) is journaled, so reconcileJournalOnStartLocked
// recovers an orphan job that understates how far it actually got. The first
// progress update for a job always journals; later ones wait out the interval.
// Caller holds manager.mu.
func (manager *Manager) journalJobProgressLocked(job model.Job) {
	if manager.lastJobJournalAt == nil {
		manager.lastJobJournalAt = map[string]time.Time{}
	}
	now := clock.Now()
	last := manager.lastJobJournalAt[job.ID]
	if !last.IsZero() && now.Sub(last) < jobProgressJournalInterval {
		return
	}
	// Advance the throttle once the event enters the ordered journal queue, which
	// is what success means now that the writer owns the disk. An error here is
	// the full-queue path, where the write ran synchronously and genuinely failed,
	// so leaving the timestamp makes the next update retry it.
	//
	// A write that fails later, after the queue accepted it, cannot reset this
	// timestamp. It needs no reset: the next progress update past the interval
	// journals again on its own, so a failed write costs at most one interval of
	// delay rather than the progress detail itself.
	if err := manager.appendJobLocked("job_progress", job); err != nil {
		slog.Warn("journal job progress failed; will retry on next update", "job_id", job.ID, "err", err)
		return
	}
	manager.lastJobJournalAt[job.ID] = now
}

// forgetJobJournalLocked drops a finished job's throttle entry so the map only
// tracks active jobs. Caller holds manager.mu.
func (manager *Manager) forgetJobJournalLocked(jobID string) {
	delete(manager.lastJobJournalAt, jobID)
}

// reconcileJournalOnStartLocked sanitizes the job journal after the previous
// daemon process exited. Any queued, running, paused, or cancelling job becomes
// cancelled in the journal because its goroutine is gone, while its last
// journaled progress is preserved so the orphan record reflects how far the
// interrupted run got rather than resetting to zero. A code codebase keeps
// Status=Indexing when it was mid-flight so ResumeOrphanedJobs can pick it back
// up on boot, since the registry already holds the canonical path and effective
// config that resume needs. A document (conversation) codebase whose active job
// was orphaned is instead reset to Status=Indexed, because conversation ingest
// is push-driven and is not resumed from the registry.
func (manager *Manager) reconcileJournalOnStartLocked() {
	now := clock.Now()
	registryChanged := false
	for id, job := range manager.jobs {
		switch job.State {
		case model.JobStateQueued, model.JobStateRunning, model.JobStatePaused, model.JobStateCancelling:
		case model.JobStateCompleted, model.JobStateFailed, model.JobStateCancelled:
			if manager.clearTerminalJobOwnershipLocked(id, job, now) {
				registryChanged = true
			}
			continue
		default:
			continue
		}
		if job.Operation == "converge" {
			manager.interruptedConvergeJobs[id] = job
		}
		job.State = model.JobStateCancelled
		job.UpdatedAt = now
		completedAt := now
		job.CompletedAt = &completedAt
		job.Progress.Phase = "cancelled"
		job.Progress.LastEventAt = now
		job.Progress.HeartbeatAt = now
		manager.jobs[id] = job
		if err := store.AppendJobEvent(manager.config.JobsPath, model.JobEvent{
			Event:      "job_orphan_recovered",
			OccurredAt: now,
			Job:        job,
		}); err != nil {
			slog.Error("append orphan recovery event failed", "job_id", id, "err", err)
		}
		codebase, found := manager.codebases[job.CodebaseID]
		if found && codebase.Kind == model.CodebaseKindDocument && codebase.ActiveJobID == id {
			codebase.Status = model.CodebaseStatusIndexed
			codebase.ActiveJobID = ""
			codebase.UpdatedAt = now
			manager.codebases[codebase.ID] = codebase
			registryChanged = true
		}
		slog.Warn("orphan job sanitized in journal after restart", "job_id", id, "codebase_id", job.CodebaseID, "files_processed", job.Progress.FilesProcessed, "chunks_embedded", job.Progress.ChunksEmbedded)
	}
	if registryChanged {
		if err := manager.saveLocked(); err != nil {
			slog.Error("write registry after journal recovery failed", "err", err)
		}
	}
}

func (manager *Manager) clearTerminalJobOwnershipLocked(
	jobID string,
	job model.Job,
	now time.Time,
) bool {
	codebase, found := manager.codebases[job.CodebaseID]
	if !found || codebase.ActiveJobID != jobID {
		return false
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
		return false
	}
	codebase.UpdatedAt = now
	manager.codebases[codebase.ID] = codebase
	return true
}
