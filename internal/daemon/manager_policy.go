package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"goodkind.io/lm-semantic-search/internal/model"
)

func newQueuedJob(
	codebaseID string,
	requestedPath string,
	canonicalPath string,
	client model.ClientInfo,
	operation string,
	forced bool,
	indexConfig model.IndexConfig,
	budget model.AdmissionBudget,
	now time.Time,
) model.Job {
	return model.Job{
		ID:            newID("job"),
		CodebaseID:    codebaseID,
		RequestedPath: requestedPath,
		CanonicalPath: canonicalPath,
		Client:        client,
		Operation:     operation,
		State:         model.JobStateQueued,
		Forced:        forced,
		Progress: model.Progress{
			Phase:                     "queued",
			OverallPercent:            0,
			Unit:                      "",
			RunMode:                   "",
			BootstrapReason:           "",
			ScopeUnit:                 "",
			FilesTotal:                0,
			FilesProcessed:            0,
			FilesAdded:                0,
			FilesModified:             0,
			FilesRemoved:              0,
			FilesInCodebase:           0,
			FilesEmbedded:             0,
			FilesSkippedOversize:      0,
			FilesSkippedUnreadable:    0,
			FilesPending:              0,
			ChunksTotal:               0,
			ChunksProcessed:           0,
			ChunksReused:              0,
			ChunksEmbedded:            0,
			ChunksGenerated:           0,
			ChunksDropped:             0,
			ReuseVectorsLoaded:        0,
			EmbeddingBatchesTotal:     0,
			EmbeddingBatchesCompleted: 0,
			CollectionRowsWritten:     0,
			LastEventAt:               now,
			HeartbeatAt:               now,
		},
		Config:                    indexConfig,
		Budget:                    budget,
		EffectiveSchedulingPolicy: model.DefaultSchedulingPolicy(),
		SchedulingOverride: model.SchedulingPolicyPatch{
			Priority:         nil,
			Quiet:            nil,
			IdleAfterSeconds: nil,
		},
		QueueSequence:    0,
		SchedulingReason: "",
		StartedAt:        now,
		UpdatedAt:        now,
		CompletedAt:      nil,
		Error:            nil,
	}
}

type indexPolicyIntent struct {
	Patch      model.SchedulingPolicyPatch
	Initialize bool
}

func (manager *Manager) resolveIndexPolicyLocked(
	codebase model.Codebase,
	intent indexPolicyIntent,
) (model.Codebase, model.SchedulingPolicy, error) {
	effectivePolicy, err := model.ApplySchedulingPolicyPatch(
		codebase.SchedulingPolicy,
		intent.Patch,
	)
	if err != nil {
		slog.Warn("resolve scheduling policy failed", "codebase_id", codebase.ID, "err", err)
		return model.Codebase{}, model.SchedulingPolicy{}, fmt.Errorf("apply scheduling policy patch: %w", err)
	}
	if intent.Initialize && codebase.PolicyPendingInitialization {
		codebase.SchedulingPolicy = effectivePolicy
		codebase.PolicyPendingInitialization = false
	}
	return codebase, effectivePolicy, nil
}

func (manager *Manager) resolveAndPersistIndexPolicy(
	codebaseID string,
	intent indexPolicyIntent,
) (model.Codebase, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	codebase, found := manager.codebases[codebaseID]
	if !found {
		return model.Codebase{}, fmt.Errorf("codebase %s is missing", codebaseID)
	}
	resolved, _, err := manager.resolveIndexPolicyLocked(codebase, intent)
	if err != nil {
		return model.Codebase{}, err
	}
	if resolved.SchedulingPolicy == codebase.SchedulingPolicy &&
		resolved.PolicyPendingInitialization == codebase.PolicyPendingInitialization {
		return resolved, nil
	}
	manager.codebases[resolved.ID] = resolved
	if err := manager.saveLocked(); err != nil {
		manager.codebases[codebase.ID] = codebase
		return model.Codebase{}, err
	}
	return resolved, nil
}

func (manager *Manager) persistResolvedIndexPolicyLocked(original model.Codebase, resolved model.Codebase) error {
	if resolved.SchedulingPolicy == original.SchedulingPolicy && resolved.PolicyPendingInitialization == original.PolicyPendingInitialization {
		return nil
	}
	manager.codebases[resolved.ID] = resolved
	if err := manager.saveLocked(); err != nil {
		manager.codebases[original.ID] = original
		return err
	}
	return nil
}

func mergeSchedulingPolicyPatches(
	existing model.SchedulingPolicyPatch,
	incoming model.SchedulingPolicyPatch,
) model.SchedulingPolicyPatch {
	merged := existing
	if incoming.Priority != nil {
		merged.Priority = incoming.Priority
	}
	if incoming.Quiet != nil {
		merged.Quiet = incoming.Quiet
	}
	if incoming.IdleAfterSeconds != nil {
		merged.IdleAfterSeconds = incoming.IdleAfterSeconds
	}
	return merged
}

func (manager *Manager) nextQueueSequenceLocked() uint64 {
	var highest uint64
	for _, job := range manager.jobs {
		if job.QueueSequence > highest {
			highest = job.QueueSequence
		}
	}
	return highest + 1
}

func applyJobSchedulingPolicy(
	job *model.Job,
	effectivePolicy model.SchedulingPolicy,
	override model.SchedulingPolicyPatch,
	queueSequence uint64,
) {
	job.EffectiveSchedulingPolicy = effectivePolicy
	job.SchedulingOverride = override
	job.QueueSequence = queueSequence
}

func (manager *Manager) startRecoveredIndex(ctx context.Context, plan resumePlan, client model.ClientInfo) error {
	job, codebase, deduplicated, overlapsCodebaseID, err := manager.startIndexWithRecovery(ctx, plan.canonicalPath, client, plan.config, false, emptyAdmissionBudget, indexPolicyIntent{
		Patch:      plan.schedulingOverride,
		Initialize: false,
	}, &plan)
	if err == nil && job.ID == "" && !deduplicated {
		return fmt.Errorf("resume did not queue job for codebase %s (overlap %s)", codebase.ID, overlapsCodebaseID)
	}
	return err
}
