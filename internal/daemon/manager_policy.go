package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"time"

	"goodkind.io/lm-semantic-search/internal/clock"
	"goodkind.io/lm-semantic-search/internal/jobscheduler"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/store"
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

type policyUpdatePlan struct {
	transaction                model.PolicyUpdateTransaction
	updatedCodebase            model.Codebase
	updatedJobs                []model.Job
	originalPendingCodeRequest *pendingCodeRequest
	markerPath                 string
}

type policyUpdateJobs struct {
	oldActiveJob    *model.Job
	oldDetachedJobs []model.Job
	updatedJobs     []model.Job
}

type schedulerPolicyUpdate struct {
	jobID   string
	receipt *jobscheduler.PolicyUpdateReceipt
}

var (
	writePolicyUpdateTransaction  = store.WritePolicyUpdate
	removePolicyUpdateTransaction = store.RemovePolicyUpdate
)

// UpdateCodebasePolicy applies a field-level stored-policy patch and updates
// current schedulable work through one recoverable durable transaction.
func (manager *Manager) UpdateCodebasePolicy(
	ctx context.Context,
	requestedPath string,
	patch model.SchedulingPolicyPatch,
) (model.Codebase, error) {
	if patch.Priority == nil && patch.Quiet == nil && patch.IdleAfterSeconds == nil {
		return model.Codebase{}, errors.New("scheduling policy patch must set at least one field")
	}
	if _, err := model.ApplySchedulingPolicyPatch(
		model.DefaultSchedulingPolicy(),
		patch,
	); err != nil {
		wrappedErr := fmt.Errorf("validate scheduling policy patch: %w", err)
		slog.WarnContext(ctx, "validate scheduling policy patch failed", "err", wrappedErr)
		return model.Codebase{}, wrappedErr
	}
	canonicalPath, err := manager.resolveCanonicalPath(requestedPath)
	if err != nil {
		wrappedErr := fmt.Errorf(
			"canonicalize path %s: %w",
			requestedPath,
			err,
		)
		slog.WarnContext(ctx, "canonicalize policy path failed", "path", requestedPath, "err", wrappedErr)
		return model.Codebase{}, wrappedErr
	}

	manager.policyMutationMutex.Lock()
	defer manager.policyMutationMutex.Unlock()
	if manager.policyMutationBlocked {
		return model.Codebase{}, errors.New("scheduling policy mutations are blocked after a rollback failure")
	}
	manager.transitionMutex.Lock()
	defer manager.transitionMutex.Unlock()

	plan, markerWritten, err := manager.preparePolicyUpdate(
		ctx,
		canonicalPath,
		requestedPath,
		patch,
	)
	if err != nil {
		if !markerWritten {
			return model.Codebase{}, err
		}
		return model.Codebase{}, manager.failPolicyUpdate(
			ctx,
			err,
			plan,
			false,
			nil,
			false,
		)
	}

	if err := manager.persistPolicyUpdateJobs(ctx, plan.updatedJobs); err != nil {
		return model.Codebase{}, manager.failPolicyUpdate(
			ctx,
			err,
			plan,
			true,
			nil,
			false,
		)
	}

	manager.publishPolicyUpdate(&plan, patch)

	schedulerUpdates, schedulerErr := manager.updateSchedulerPolicies(
		ctx,
		plan.updatedJobs,
		patch,
	)
	if schedulerErr != nil {
		return model.Codebase{}, manager.failPolicyUpdate(
			ctx,
			schedulerErr,
			plan,
			true,
			schedulerUpdates,
			true,
		)
	}

	if err := removePolicyUpdateTransaction(plan.markerPath); err != nil {
		failure := fmt.Errorf("remove policy update transaction: %w", err)
		return model.Codebase{}, manager.failPolicyUpdate(
			ctx,
			failure,
			plan,
			len(plan.updatedJobs) > 0,
			schedulerUpdates,
			true,
		)
	}
	return plan.updatedCodebase, nil
}

func (manager *Manager) persistPolicyUpdateJobs(
	ctx context.Context,
	updatedJobs []model.Job,
) error {
	for _, updatedJob := range updatedJobs {
		jobEvent := model.JobEvent{
			Event:      "job_policy_updated",
			OccurredAt: clock.Now(),
			Job:        updatedJob,
		}
		if err := manager.writeJobTransition(jobEvent); err != nil {
			wrappedErr := fmt.Errorf("persist updated job policy: %w", err)
			slog.ErrorContext(ctx, "persist updated job policy failed", "job_id", updatedJob.ID, "err", wrappedErr)
			return wrappedErr
		}
	}
	return nil
}

func (manager *Manager) updateSchedulerPolicies(
	ctx context.Context,
	updatedJobs []model.Job,
	patch model.SchedulingPolicyPatch,
) ([]schedulerPolicyUpdate, error) {
	updates := make([]schedulerPolicyUpdate, 0, len(updatedJobs))
	for _, updatedJob := range updatedJobs {
		update, err := manager.updateSchedulerPolicy(ctx, updatedJob, patch)
		if err != nil {
			return updates, err
		}
		updates = append(updates, update)
	}
	return updates, nil
}

func (manager *Manager) preparePolicyUpdate(
	ctx context.Context,
	canonicalPath string,
	requestedPath string,
	patch model.SchedulingPolicyPatch,
) (policyUpdatePlan, bool, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	matches := manager.findCodebasesByCoverage(canonicalPath)
	if len(matches) == 0 {
		return policyUpdatePlan{}, false, errors.New("codebase not tracked: " + requestedPath)
	}
	originalCodebase := matches[0]
	updatedPolicy, err := model.ApplySchedulingPolicyPatch(
		originalCodebase.SchedulingPolicy,
		patch,
	)
	if err != nil {
		wrappedErr := fmt.Errorf("apply scheduling policy patch: %w", err)
		slog.WarnContext(ctx, "apply scheduling policy patch failed", "codebase_id", originalCodebase.ID, "err", wrappedErr)
		return policyUpdatePlan{}, false, wrappedErr
	}
	updatedCodebase := originalCodebase
	updatedCodebase.SchedulingPolicy = updatedPolicy
	updatedCodebase.PolicyPendingInitialization = false
	updatedCodebase.UpdatedAt = clock.Now()

	jobs, err := manager.preparePolicyUpdateJobsLocked(
		ctx,
		originalCodebase,
		patch,
		updatedCodebase.UpdatedAt,
	)
	if err != nil {
		return policyUpdatePlan{}, false, err
	}

	plan := policyUpdatePlan{
		transaction: model.PolicyUpdateTransaction{
			CodebaseID:      originalCodebase.ID,
			OldCodebase:     originalCodebase,
			OldActiveJob:    jobs.oldActiveJob,
			OldDetachedJobs: jobs.oldDetachedJobs,
		},
		updatedCodebase:            updatedCodebase,
		updatedJobs:                jobs.updatedJobs,
		originalPendingCodeRequest: nil,
		markerPath:                 store.PolicyUpdatePath(manager.config.RegistryPath),
	}
	if err := writePolicyUpdateTransaction(plan.markerPath, plan.transaction); err != nil {
		wrappedErr := fmt.Errorf("write policy update transaction: %w", err)
		slog.ErrorContext(ctx, "write policy update transaction failed", "codebase_id", originalCodebase.ID, "err", wrappedErr)
		markerMayExist := errors.Is(err, store.ErrPolicyUpdateMarkerMayExist)
		return plan, markerMayExist, wrappedErr
	}

	manager.codebases[updatedCodebase.ID] = updatedCodebase
	registryErr := manager.saveLocked()
	manager.codebases[originalCodebase.ID] = originalCodebase
	if registryErr != nil {
		wrappedErr := fmt.Errorf("persist updated codebase policy: %w", registryErr)
		slog.ErrorContext(ctx, "persist updated codebase policy failed", "codebase_id", originalCodebase.ID, "err", wrappedErr)
		return plan, true, wrappedErr
	}
	return plan, true, nil
}

// preparePolicyUpdateJobsLocked captures every current schedulable job for the
// codebase, including watcher jobs that do not own Codebase.ActiveJobID.
// Caller holds manager.mu.
func (manager *Manager) preparePolicyUpdateJobsLocked(
	ctx context.Context,
	codebase model.Codebase,
	patch model.SchedulingPolicyPatch,
	updatedAt time.Time,
) (policyUpdateJobs, error) {
	jobIDs := make([]string, 0)
	for jobID, job := range manager.jobs {
		if job.CodebaseID == codebase.ID && policyUpdateAppliesToJob(job) {
			jobIDs = append(jobIDs, jobID)
		}
	}
	sort.Strings(jobIDs)

	jobs := policyUpdateJobs{
		oldActiveJob:    nil,
		oldDetachedJobs: nil,
		updatedJobs:     nil,
	}
	for _, jobID := range jobIDs {
		oldJob := manager.jobs[jobID]
		updatedJob := oldJob
		var err error
		updatedJob.EffectiveSchedulingPolicy, err = model.ApplySchedulingPolicyPatch(
			oldJob.EffectiveSchedulingPolicy,
			patch,
		)
		if err != nil {
			wrappedErr := fmt.Errorf(
				"apply scheduling policy patch to job %s: %w",
				oldJob.ID,
				err,
			)
			slog.WarnContext(ctx, "apply scheduling policy patch to job failed", "codebase_id", codebase.ID, "job_id", oldJob.ID, "err", wrappedErr)
			return policyUpdateJobs{}, wrappedErr
		}
		updatedJob.SchedulingOverride = clearPatchedSchedulingOverrides(
			oldJob.SchedulingOverride,
			patch,
		)
		updatedJob.UpdatedAt = updatedAt
		jobs.updatedJobs = append(jobs.updatedJobs, updatedJob)
		if oldJob.ID == codebase.ActiveJobID {
			activeJob := oldJob
			jobs.oldActiveJob = &activeJob
			continue
		}
		jobs.oldDetachedJobs = append(jobs.oldDetachedJobs, oldJob)
	}
	return jobs, nil
}

func (manager *Manager) publishPolicyUpdate(
	plan *policyUpdatePlan,
	patch model.SchedulingPolicyPatch,
) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	manager.codebases[plan.updatedCodebase.ID] = plan.updatedCodebase
	for _, updatedJob := range plan.updatedJobs {
		manager.jobs[updatedJob.ID] = updatedJob
	}
	pending, found := manager.pendingCodeJobs[plan.updatedCodebase.ID]
	if !found {
		return
	}
	originalPending := pending
	plan.originalPendingCodeRequest = &originalPending
	pending.policyPatch = clearPatchedSchedulingOverrides(
		pending.policyPatch,
		patch,
	)
	manager.pendingCodeJobs[plan.updatedCodebase.ID] = pending
}

func policyUpdateAppliesToJob(job model.Job) bool {
	switch job.State {
	case model.JobStateQueued, model.JobStateRunning, model.JobStatePaused:
		return true
	case model.JobStateCancelling,
		model.JobStateCompleted,
		model.JobStateFailed,
		model.JobStateCancelled:
		return false
	default:
		return false
	}
}

func clearPatchedSchedulingOverrides(
	override model.SchedulingPolicyPatch,
	patch model.SchedulingPolicyPatch,
) model.SchedulingPolicyPatch {
	cleared := override
	if patch.Priority != nil {
		cleared.Priority = nil
	}
	if patch.Quiet != nil {
		cleared.Quiet = nil
	}
	if patch.IdleAfterSeconds != nil {
		cleared.IdleAfterSeconds = nil
	}
	return cleared
}

func (manager *Manager) failPolicyUpdate(
	ctx context.Context,
	failure error,
	plan policyUpdatePlan,
	restoreJobs bool,
	schedulerUpdates []schedulerPolicyUpdate,
	restorePending bool,
) error {
	slog.ErrorContext(
		ctx,
		"policy update failed",
		"codebase_id",
		plan.transaction.CodebaseID,
		"err",
		failure,
	)
	rollbackErr := manager.rollbackPolicyUpdate(
		ctx,
		plan,
		restoreJobs,
		schedulerUpdates,
		restorePending,
	)
	if rollbackErr == nil {
		return failure
	}
	manager.policyMutationBlocked = true
	return errors.Join(
		failure,
		fmt.Errorf("roll back policy update: %w", rollbackErr),
	)
}

func (manager *Manager) rollbackPolicyUpdate(
	ctx context.Context,
	plan policyUpdatePlan,
	restoreJobs bool,
	schedulerUpdates []schedulerPolicyUpdate,
	restorePending bool,
) error {
	transaction := plan.transaction
	oldJobs := policyUpdateTransactionJobs(transaction)
	rollbackErrors := make([]error, 0, len(oldJobs)+len(schedulerUpdates)+2)
	if err := ensurePolicyUpdateTransaction(plan.markerPath, transaction); err != nil {
		return err
	}
	manager.mu.Lock()
	manager.codebases[transaction.CodebaseID] = transaction.OldCodebase
	if restoreJobs {
		for _, oldJob := range oldJobs {
			manager.jobs[oldJob.ID] = oldJob
		}
	}
	if restorePending && plan.originalPendingCodeRequest != nil {
		manager.pendingCodeJobs[transaction.CodebaseID] = *plan.originalPendingCodeRequest
	}
	if err := manager.saveLocked(); err != nil {
		rollbackErrors = append(
			rollbackErrors,
			fmt.Errorf("restore codebase registry: %w", err),
		)
	}
	manager.mu.Unlock()

	if restoreJobs {
		for _, oldJob := range oldJobs {
			if err := manager.writeJobTransition(model.JobEvent{
				Event:      "job_policy_update_rolled_back",
				OccurredAt: clock.Now(),
				Job:        oldJob,
			}); err != nil {
				rollbackErrors = append(
					rollbackErrors,
					fmt.Errorf("restore job %s journal: %w", oldJob.ID, err),
				)
			}
		}
	}
	oldJobsByID := make(map[string]model.Job, len(oldJobs))
	for _, oldJob := range oldJobs {
		oldJobsByID[oldJob.ID] = oldJob
	}
	for _, schedulerUpdate := range schedulerUpdates {
		oldJob, found := oldJobsByID[schedulerUpdate.jobID]
		if !found {
			rollbackErrors = append(
				rollbackErrors,
				fmt.Errorf(
					"restore scheduler policy: old job %s is missing",
					schedulerUpdate.jobID,
				),
			)
			continue
		}
		var err error
		if schedulerUpdate.receipt != nil {
			err = manager.jobScheduler.RollbackPolicyUpdate(
				*schedulerUpdate.receipt,
				oldJob.EffectiveSchedulingPolicy,
			)
		} else {
			err = manager.jobScheduler.UpdatePolicy(
				oldJob.ID,
				fullSchedulingPolicyPatch(oldJob.EffectiveSchedulingPolicy),
			)
		}
		if err != nil {
			rollbackErrors = append(
				rollbackErrors,
				fmt.Errorf("restore scheduler policy for job %s: %w", oldJob.ID, err),
			)
		}
	}
	if len(rollbackErrors) > 0 {
		joined := errors.Join(rollbackErrors...)
		slog.ErrorContext(
			ctx,
			"roll back policy update failed",
			"codebase_id",
			transaction.CodebaseID,
			"err",
			joined,
		)
		return joined
	}
	markerPath := store.PolicyUpdatePath(manager.config.RegistryPath)
	if err := removePolicyUpdateTransaction(markerPath); err != nil {
		return fmt.Errorf("remove rolled-back policy update transaction: %w", err)
	}
	return nil
}

func ensurePolicyUpdateTransaction(
	markerPath string,
	transaction model.PolicyUpdateTransaction,
) error {
	_, markerErr := store.ReadPolicyUpdate(markerPath)
	if errors.Is(markerErr, os.ErrNotExist) {
		if err := writePolicyUpdateTransaction(markerPath, transaction); err != nil {
			return fmt.Errorf("restore policy update transaction marker: %w", err)
		}
		return nil
	}
	if markerErr != nil {
		wrappedErr := fmt.Errorf(
			"read policy update transaction marker before rollback: %w",
			markerErr,
		)
		slog.Error("read policy update transaction marker before rollback failed", "path", markerPath, "err", wrappedErr)
		return wrappedErr
	}
	return nil
}

func (manager *Manager) updateSchedulerPolicy(
	ctx context.Context,
	job model.Job,
	patch model.SchedulingPolicyPatch,
) (schedulerPolicyUpdate, error) {
	if job.State == model.JobStateQueued {
		receipt, err := manager.jobScheduler.StagePolicyUpdate(job.ID, patch)
		if err != nil {
			wrappedErr := fmt.Errorf(
				"stage scheduler policy for job %s: %w",
				job.ID,
				err,
			)
			slog.ErrorContext(ctx, "stage scheduler policy failed", "job_id", job.ID, "err", wrappedErr)
			return schedulerPolicyUpdate{}, wrappedErr
		}
		return schedulerPolicyUpdate{jobID: job.ID, receipt: &receipt}, nil
	}
	if err := manager.jobScheduler.UpdatePolicy(job.ID, patch); err != nil {
		wrappedErr := fmt.Errorf(
			"update scheduler policy for job %s: %w",
			job.ID,
			err,
		)
		slog.ErrorContext(ctx, "update scheduler policy failed", "job_id", job.ID, "err", wrappedErr)
		return schedulerPolicyUpdate{}, wrappedErr
	}
	return schedulerPolicyUpdate{jobID: job.ID, receipt: nil}, nil
}

func policyUpdateTransactionJobs(
	transaction model.PolicyUpdateTransaction,
) []model.Job {
	jobCount := len(transaction.OldDetachedJobs)
	if transaction.OldActiveJob != nil {
		jobCount++
	}
	jobs := make([]model.Job, 0, jobCount)
	if transaction.OldActiveJob != nil {
		jobs = append(jobs, *transaction.OldActiveJob)
	}
	jobs = append(jobs, transaction.OldDetachedJobs...)
	return jobs
}

func fullSchedulingPolicyPatch(
	policy model.SchedulingPolicy,
) model.SchedulingPolicyPatch {
	priority := policy.Priority
	quiet := policy.Quiet
	idleAfterSeconds := policy.IdleAfterSeconds
	return model.SchedulingPolicyPatch{
		Priority:         &priority,
		Quiet:            &quiet,
		IdleAfterSeconds: &idleAfterSeconds,
	}
}

func (manager *Manager) recoverPendingPolicyUpdate(ctx context.Context) error {
	markerPath := store.PolicyUpdatePath(manager.config.RegistryPath)
	transaction, err := store.ReadPolicyUpdate(markerPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		wrappedErr := fmt.Errorf("read pending policy update: %w", err)
		slog.ErrorContext(ctx, "read pending policy update failed", "path", markerPath, "err", wrappedErr)
		return wrappedErr
	}
	if transaction.CodebaseID == "" ||
		transaction.OldCodebase.ID != transaction.CodebaseID {
		identityErr := fmt.Errorf("pending policy update has inconsistent codebase identity")
		slog.ErrorContext(ctx, "validate pending policy update failed", "path", markerPath, "err", identityErr)
		return identityErr
	}

	registry, err := store.ReadRegistry(manager.config.RegistryPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		wrappedErr := fmt.Errorf("read registry for pending policy rollback: %w", err)
		slog.ErrorContext(ctx, "read registry for pending policy rollback failed", "path", manager.config.RegistryPath, "err", wrappedErr)
		return wrappedErr
	}
	replaced := false
	for index := range registry.Codebases {
		if registry.Codebases[index].ID != transaction.CodebaseID {
			continue
		}
		registry.Codebases[index] = transaction.OldCodebase
		replaced = true
		break
	}
	if !replaced {
		registry.Codebases = append(registry.Codebases, transaction.OldCodebase)
	}
	registry.UpdatedAt = clock.Now()
	if err := store.WriteRegistry(manager.config.RegistryPath, registry); err != nil {
		wrappedErr := fmt.Errorf("restore registry from pending policy update: %w", err)
		slog.ErrorContext(ctx, "restore registry from pending policy update failed", "codebase_id", transaction.CodebaseID, "err", wrappedErr)
		return wrappedErr
	}
	for _, oldJob := range policyUpdateTransactionJobs(transaction) {
		if oldJob.CodebaseID != transaction.CodebaseID {
			identityErr := fmt.Errorf(
				"pending policy update job %s has inconsistent codebase identity",
				oldJob.ID,
			)
			slog.ErrorContext(ctx, "validate pending policy update job failed", "path", markerPath, "err", identityErr)
			return identityErr
		}
		if err := store.AppendJobEventSync(manager.config.JobsPath, model.JobEvent{
			Event:      "job_policy_update_rolled_back",
			OccurredAt: clock.Now(),
			Job:        oldJob,
		}); err != nil {
			wrappedErr := fmt.Errorf("restore job from pending policy update: %w", err)
			slog.ErrorContext(ctx, "restore job from pending policy update failed", "codebase_id", transaction.CodebaseID, "job_id", oldJob.ID, "err", wrappedErr)
			return wrappedErr
		}
	}
	if err := removePolicyUpdateTransaction(markerPath); err != nil {
		wrappedErr := fmt.Errorf("remove recovered policy update transaction: %w", err)
		slog.ErrorContext(ctx, "remove recovered policy update transaction failed", "codebase_id", transaction.CodebaseID, "err", wrappedErr)
		return wrappedErr
	}
	slog.InfoContext(
		ctx,
		"recovered pending policy update",
		"codebase_id",
		transaction.CodebaseID,
	)
	return nil
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
