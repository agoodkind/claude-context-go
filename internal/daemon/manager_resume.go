package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"sort"

	"goodkind.io/lm-semantic-search/internal/clock"
	"goodkind.io/lm-semantic-search/internal/gitworktree"
	"goodkind.io/lm-semantic-search/internal/merkle"
	"goodkind.io/lm-semantic-search/internal/metrics"
	"goodkind.io/lm-semantic-search/internal/model"
)

type resumeCheckpointKind int

const (
	resumeCheckpointNone resumeCheckpointKind = iota
	resumeCheckpointLive
	resumeCheckpointStaging
)

type resumePlan struct {
	canonicalPath             string
	config                    model.IndexConfig
	codebaseID                string
	checkpoint                resumeCheckpointKind
	effectiveSchedulingPolicy model.SchedulingPolicy
	schedulingOverride        model.SchedulingPolicyPatch
	queueSequence             uint64
	converge                  bool
	interruptedJobID          string
	// codebase is the record the probe reads its checkpoint through. The plan
	// carries the whole record rather than a precomputed verdict so the probe
	// reaches loadLiveCheckpoint with everything the one expectation rule needs.
	codebase model.Codebase
}

func sortResumePlans(plans []resumePlan) {
	sort.Slice(plans, func(first int, second int) bool {
		if plans[first].queueSequence == 0 {
			return false
		}
		if plans[second].queueSequence == 0 {
			return true
		}
		if plans[first].queueSequence != plans[second].queueSequence {
			return plans[first].queueSequence < plans[second].queueSequence
		}
		return plans[first].codebaseID < plans[second].codebaseID
	})
}

// ResumeOrphanedJobs re-queues indexing for every codebase whose previous job
// was still running when the daemon exited, but only when a merkle checkpoint
// records the work already done. Delta, streaming, and bootstrap builds all
// checkpoint per file, so an interrupted run can skip files already embedded.
// A bootstrap resume also requires its staging collection to still exist; when
// the checkpoint is missing, the daemon leaves the codebase re-queueable so the
// background repair pass restarts the build, rather than parking it as failed.
// Call this once after NewManager returns and before the daemon advertises ready.
func (manager *Manager) ResumeOrphanedJobs(ctx context.Context) {
	manager.mu.Lock()
	plans := make([]resumePlan, 0)
	for _, codebase := range manager.codebases {
		if codebase.Kind == model.CodebaseKindDocument {
			// A conversation codebase recovers through its own ingest trigger;
			// its chat:// path is not a directory the index runner can walk.
			continue
		}
		if codebase.Status != model.CodebaseStatusIndexing {
			continue
		}
		effectivePolicy := codebase.SchedulingPolicy
		override := model.SchedulingPolicyPatch{Priority: nil, Quiet: nil, IdleAfterSeconds: nil}
		var queueSequence uint64
		if interrupted, found := manager.jobs[codebase.ActiveJobID]; found {
			effectivePolicy = interrupted.EffectiveSchedulingPolicy
			override = interrupted.SchedulingOverride
			queueSequence = interrupted.QueueSequence
		}
		plans = append(plans, resumePlan{
			canonicalPath:             codebase.CanonicalPath,
			config:                    codebase.EffectiveConfig,
			codebaseID:                codebase.ID,
			checkpoint:                resumeCheckpointNone,
			effectiveSchedulingPolicy: effectivePolicy,
			schedulingOverride:        override,
			queueSequence:             queueSequence,
			converge:                  false,
			interruptedJobID:          "",
			codebase:                  codebase,
		})
	}
	for _, interrupted := range manager.interruptedConvergeJobs {
		codebase, found := manager.codebases[interrupted.CodebaseID]
		if !found || codebase.Kind == model.CodebaseKindDocument {
			continue
		}
		plans = append(plans, resumePlan{
			canonicalPath:             codebase.CanonicalPath,
			config:                    codebase.EffectiveConfig,
			codebaseID:                codebase.ID,
			checkpoint:                resumeCheckpointLive,
			effectiveSchedulingPolicy: interrupted.EffectiveSchedulingPolicy,
			schedulingOverride:        interrupted.SchedulingOverride,
			queueSequence:             interrupted.QueueSequence,
			converge:                  true,
			interruptedJobID:          interrupted.ID,
			codebase:                  codebase,
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
		if plan.converge {
			resumable = append(resumable, plan)
			continue
		}
		plan.checkpoint = manager.resumableCheckpointKind(ctx, plan.codebase, plan.config.IgnoreDigest)
		if plan.checkpoint != resumeCheckpointNone {
			resumable = append(resumable, plan)
			continue
		}
		manager.logResumeUnresumable(ctx, plan.codebaseID, plan.canonicalPath)
		manager.parkUnresumableForRetry(ctx, plan.codebaseID)
	}
	if len(resumable) == 0 {
		return
	}
	sortResumePlans(resumable)

	paths := make([]string, 0, len(resumable))
	for _, plan := range resumable {
		paths = append(paths, plan.canonicalPath)
	}
	slog.InfoContext(ctx, "resuming orphaned indexing jobs", "count", len(resumable), "paths", paths)
	for _, plan := range resumable {
		client := model.ClientInfo{Name: "daemon-resume", PID: 0}
		var err error
		switch {
		case plan.checkpoint == resumeCheckpointStaging:
			err = manager.startStagingResume(ctx, plan, client)
		case plan.converge:
			err = manager.resumeConverge(ctx, plan, client)
		default:
			err = manager.startRecoveredIndex(ctx, plan, client)
		}
		if err != nil {
			slog.ErrorContext(ctx, "resume orphaned job failed", "codebase_id", plan.codebaseID, "path", plan.canonicalPath, "err", err)
			continue
		}
		manager.recordResumeLaunched(ctx, plan)
	}
}

func (manager *Manager) recordResumeLaunched(ctx context.Context, plan resumePlan) {
	metrics.JobResumed()
	manager.mu.Lock()
	if plan.interruptedJobID != "" {
		delete(manager.interruptedConvergeJobs, plan.interruptedJobID)
	}
	manager.mu.Unlock()
	manager.logResumeLaunched(ctx, plan.codebaseID, plan.canonicalPath)
}

func (manager *Manager) resumeConverge(ctx context.Context, plan resumePlan, client model.ClientInfo) error {
	manager.mu.Lock()
	codebase, found := manager.codebases[plan.codebaseID]
	if !found {
		manager.mu.Unlock()
		return fmt.Errorf("recovered converge codebase %s is missing", plan.codebaseID)
	}
	indexConfig := manager.enrichIndexConfig(codebase.EffectiveConfig)
	indexConfig.IgnoreDigest = digestIndexConfig(indexConfig)
	_, resolution, err := manager.activeJobLocked(codebase, indexConfig)
	if err != nil {
		manager.mu.Unlock()
		return err
	}
	if resolution != activeJobNone {
		manager.mu.Unlock()
		return nil
	}
	job, err := manager.enqueueCodeSyncJobLocked(codebase, pendingCodeRequest{
		requestedPath: plan.canonicalPath,
		canonicalPath: plan.canonicalPath,
		client:        client,
		indexConfig:   indexConfig,
		force:         false,
		policyPatch:   plan.schedulingOverride,
	})
	if err != nil {
		manager.mu.Unlock()
		return err
	}
	current := manager.jobs[job.ID]
	current.EffectiveSchedulingPolicy = plan.effectiveSchedulingPolicy
	current.SchedulingOverride = plan.schedulingOverride
	if plan.queueSequence != 0 {
		current.QueueSequence = plan.queueSequence
	}
	manager.jobs[job.ID] = current
	if err := manager.appendJobLocked("resume_converge_sync", current); err != nil {
		manager.mu.Unlock()
		manager.updateJobCancelled(context.WithoutCancel(ctx), job.ID)
		return err
	}
	manager.mu.Unlock()
	manager.runJobAsync(ctx, job.ID)
	return nil
}

// resumableCheckpointKind reports which merkle checkpoint a codebase left
// mid-index persisted for its current config: the live snapshot, the staging
// bootstrap snapshot, or none. Resuming without one would re-embed every file
// from scratch, so the daemon treats a missing checkpoint as not resumable.
//
// The staging probe is always optional. This function runs only for codebases
// stuck at "indexing", and a bootstrap writes to the staging path, so an
// interrupted first build is exactly the case that may still own a staging
// checkpoint. What makes the probe optional is that an absent staging file is
// indistinguishable from an interruption that landed before the first per-file
// checkpoint was written, and no registry field separates those two: a required
// read would fire on every early interruption and the operator could not act on
// it. The absence is still visible, as resumeCheckpointNone logs "skipping
// unresumable interrupted index" with the reason no_checkpoint.
//
// The live probe goes through loadLiveCheckpoint, which is where the one rule
// lives: where no live checkpoint was ever written an absent file is the
// expected shape and stays quiet, and where one should exist its absence is a
// real loss and is reported to the operator.
func (manager *Manager) resumableCheckpointKind(ctx context.Context, codebase model.Codebase, configDigest string) resumeCheckpointKind {
	legacyDigest := manager.legacyDigestForCodebase(codebase.ID)
	stagingSnapshot := merkle.LoadOptionalSnapshotForConfig(manager.stagingMerklePath(codebase.ID), configDigest, legacyDigest)
	if len(stagingSnapshot.Files) > 0 {
		return resumeCheckpointStaging
	}
	liveCheckpoint := manager.loadLiveCheckpoint(ctx, codebase, configDigest)
	if len(liveCheckpoint.snapshot.Files) > 0 {
		return resumeCheckpointLive
	}
	return resumeCheckpointNone
}

func (manager *Manager) startStagingResume(ctx context.Context, plan resumePlan, client model.ClientInfo) error {
	indexConfig := manager.enrichIndexConfig(plan.config)
	indexConfig.IgnoreDigest = digestIndexConfig(indexConfig)

	manager.mu.Lock()
	codebase, found := manager.codebases[plan.codebaseID]
	if !found {
		manager.mu.Unlock()
		return nil
	}
	_, resolution, err := manager.activeJobLocked(codebase, indexConfig)
	if err != nil {
		manager.mu.Unlock()
		return err
	}
	// A staging resume is a startup best-effort: if any job is already active for
	// this codebase (a matching-config dedup or a non-matching coalesce), skip the
	// resume rather than fight the in-flight run.
	if resolution != activeJobNone {
		manager.mu.Unlock()
		return nil
	}

	codebase.Status = model.CodebaseStatusPending
	if codebase.LastSuccessfulRun != nil {
		codebase.Status = model.CodebaseStatusIndexing
	}
	codebase.EffectiveConfig = indexConfig
	if manager.semantic != nil && manager.semantic.Available() {
		codebase.CollectionName = manager.semantic.CollectionName(plan.canonicalPath)
	}
	if info, ok := gitworktree.Resolve(plan.canonicalPath); ok && info.Linked {
		codebase.WorktreeCommonDir = info.CommonDir
	}
	codebase.UpdatedAt = clock.Now()

	job := newQueuedJob(codebase.ID, plan.canonicalPath, plan.canonicalPath, client, string(jobOperationIndex), false, indexConfig, emptyAdmissionBudget, clock.Now())
	queueSequence := plan.queueSequence
	if queueSequence == 0 {
		queueSequence = manager.nextQueueSequenceLocked()
	}
	applyJobSchedulingPolicy(&job, plan.effectiveSchedulingPolicy, plan.schedulingOverride, queueSequence)
	codebase.ActiveJobID = job.ID
	manager.codebases[codebase.ID] = codebase
	if err := manager.saveLocked(); err != nil {
		manager.mu.Unlock()
		return err
	}
	if err := manager.appendJobLocked("resume_staging_index", job); err != nil {
		manager.mu.Unlock()
		return err
	}
	manager.observer.Invalidate(codebase.ID)
	manager.mu.Unlock()

	manager.runJobAsync(ctx, job.ID)
	return nil
}

// parkUnresumableForRetry leaves an interrupted codebase that has no checkpoint
// at "indexing" with its active job cleared, so the background repair pass
// re-queues a fresh build rather than parking it as a failure. A per-file build
// checkpoints after each file, so a missing checkpoint means almost nothing was
// embedded and the re-queued build restarts cleanly. Clearing the index is the
// only way to stop the retry.
func (manager *Manager) parkUnresumableForRetry(ctx context.Context, codebaseID string) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	codebase, found := manager.codebases[codebaseID]
	if !found {
		return
	}
	if codebase.Status != model.CodebaseStatusIndexing {
		return
	}

	codebase.ActiveJobID = ""
	codebase.UpdatedAt = clock.Now()
	manager.codebases[codebaseID] = codebase
	if err := manager.saveLocked(); err != nil {
		slog.ErrorContext(ctx, "write registry after parking unresumable index failed", "codebase_id", codebaseID, "err", err)
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
