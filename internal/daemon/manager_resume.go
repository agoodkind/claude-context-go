package daemon

import (
	"context"
	"log/slog"

	"goodkind.io/claude-context-go/internal/clock"
	"goodkind.io/claude-context-go/internal/merkle"
	"goodkind.io/claude-context-go/internal/metrics"
	"goodkind.io/claude-context-go/internal/model"
	"goodkind.io/gklog/correlation"
)

// ResumeOrphanedJobs re-queues indexing for every codebase whose previous job
// was still running when the daemon exited, but only when a merkle checkpoint
// records the work already done. The delta and streaming paths checkpoint
// per file, so their interrupted runs resume and skip embedded files. A full
// bootstrap index writes its snapshot only on completion, so an interrupted
// bootstrap has no checkpoint; resuming it would re-embed the whole codebase,
// which a restart must never trigger. Call this once after NewManager returns
// and before the daemon advertises ready.
func (manager *Manager) ResumeOrphanedJobs(ctx context.Context) {
	manager.mu.Lock()
	type resumePlan struct {
		canonicalPath string
		config        model.IndexConfig
		codebaseID    string
	}
	plans := make([]resumePlan, 0)
	for _, codebase := range manager.codebases {
		if codebase.Status != model.CodebaseStatusIndexing {
			continue
		}
		plans = append(plans, resumePlan{
			canonicalPath: codebase.CanonicalPath,
			config:        codebase.EffectiveConfig,
			codebaseID:    codebase.ID,
		})
	}
	manager.mu.Unlock()

	if !manager.config.ResumeIndexingOnBoot {
		for _, plan := range plans {
			manager.logResumeSkipped(ctx, plan.codebaseID, plan.canonicalPath)
		}
		return
	}

	resumable := make([]resumePlan, 0, len(plans))
	for _, plan := range plans {
		if manager.hasResumableCheckpoint(plan.codebaseID, plan.config.IgnoreDigest) {
			resumable = append(resumable, plan)
			continue
		}
		manager.logResumeUnresumable(ctx, plan.codebaseID, plan.canonicalPath)
		manager.markUnresumableFailed(ctx, plan.codebaseID)
	}
	if len(resumable) == 0 {
		return
	}

	paths := make([]string, 0, len(resumable))
	for _, plan := range resumable {
		paths = append(paths, plan.canonicalPath)
	}
	slog.InfoContext(ctx, "resuming orphaned indexing jobs", "count", len(resumable), "paths", paths)
	for _, plan := range resumable {
		client := model.ClientInfo{Name: "daemon-resume", PID: 0}
		_, _, _, _, err := manager.StartIndex(ctx, plan.canonicalPath, client, plan.config, false)
		if err != nil {
			slog.ErrorContext(ctx, "resume orphaned job failed", "codebase_id", plan.codebaseID, "path", plan.canonicalPath, "err", err)
			continue
		}
		metrics.JobResumed()
		manager.logResumeLaunched(ctx, plan.codebaseID, plan.canonicalPath)
	}
}

// hasResumableCheckpoint reports whether a codebase left mid-index persisted a
// merkle checkpoint valid for its current config. Resuming without one would
// re-embed every file from scratch, so the daemon treats a missing checkpoint
// as not resumable.
func (manager *Manager) hasResumableCheckpoint(codebaseID string, configDigest string) bool {
	snapshot := merkle.LoadSnapshotForConfig(manager.merklePath(codebaseID), configDigest, manager.legacyDigestForCodebase(codebaseID))
	return len(snapshot.Files) > 0
}

// markUnresumableFailed transitions an interrupted codebase that has no
// checkpoint to failed, so it surfaces as failed rather than staying stuck
// showing indexing across restarts. A per-file build checkpoints after each
// file, so a missing checkpoint means almost nothing was embedded; a re-run of
// index_codebase starts the build fresh.
func (manager *Manager) markUnresumableFailed(ctx context.Context, codebaseID string) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	codebase, found := manager.codebases[codebaseID]
	if !found {
		return
	}
	if codebase.Status != model.CodebaseStatusIndexing {
		return
	}

	now := clock.Now()
	codebase.Status = model.CodebaseStatusFailed
	codebase.ActiveJobID = ""
	codebase.LastFailedRun = &model.IndexRunFailure{
		Message:                 "interrupted index had no checkpoint to resume; re-run index_codebase to finish",
		LastAttemptedPercentage: 0,
		FailedAt:                now,
		TraceID:                 string(correlation.FromContext(ctx).TraceID),
		JobID:                   "",
	}
	codebase.UpdatedAt = now
	manager.codebases[codebaseID] = codebase
	if err := manager.saveLocked(); err != nil {
		slog.ErrorContext(ctx, "write registry after marking unresumable index failed", "codebase_id", codebaseID, "err", err)
	}
}

// logResumeSkipped records that boot resume is disabled for one tracked
// codebase. It exists as a method so the per-codebase line is not emitted
// lexically inside the ResumeOrphanedJobs loop.
func (manager *Manager) logResumeSkipped(ctx context.Context, codebaseID string, path string) {
	slog.InfoContext(ctx, "skipping orphaned indexing job resume", "codebase_id", codebaseID, "path", path, "reason", "resume_on_boot_disabled")
}

// logResumeUnresumable records that an interrupted index had no checkpoint to
// resume from, so the daemon leaves it tracked rather than re-embedding the
// whole codebase on boot. Re-run index_codebase to finish it.
func (manager *Manager) logResumeUnresumable(ctx context.Context, codebaseID string, path string) {
	slog.InfoContext(ctx, "skipping unresumable interrupted index; re-run index_codebase to finish", "codebase_id", codebaseID, "path", path, "reason", "no_checkpoint")
}

// logResumeLaunched records that boot resume re-queued one codebase. It
// exists as a method so the per-codebase line is not emitted lexically
// inside the ResumeOrphanedJobs loop.
func (manager *Manager) logResumeLaunched(ctx context.Context, codebaseID string, path string) {
	slog.InfoContext(ctx, "resumed orphaned indexing job", "codebase_id", codebaseID, "path", path)
}
