package daemon

import (
	"context"
	"fmt"
	"log/slog"

	"goodkind.io/gklog/correlation"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/spans"
)

func (manager *Manager) startIndexWithRecovery(
	ctx context.Context,
	requestedPath string,
	client model.ClientInfo,
	indexConfig model.IndexConfig,
	force bool,
	budget model.AdmissionBudget,
	policyIntent indexPolicyIntent,
	recoveredPlan *resumePlan,
) (model.Job, model.Codebase, bool, string, error) {
	var emptyJob model.Job
	var emptyCodebase model.Codebase
	canonicalPath, err := manager.startIndexCanonicalPath(ctx, requestedPath)
	if err != nil {
		return emptyJob, emptyCodebase, false, "", err
	}
	indexConfig = manager.enrichIndexConfig(indexConfig)
	indexConfig.IgnoreDigest = digestIndexConfig(indexConfig)
	manager.policyMutationMutex.Lock()
	policyLocked := true
	defer func() {
		if policyLocked {
			manager.policyMutationMutex.Unlock()
		}
	}()

	for {
		job, codebase, redirected, redirectErr := manager.redirectStartIndex(
			ctx,
			requestedPath,
			canonicalPath,
			client,
			policyIntent,
		)
		if redirected {
			if redirectErr != nil || job.ID == "" {
				return job, codebase, false, "", redirectErr
			}
			runContext := manager.indexJobContext(ctx, job)
			manager.policyMutationMutex.Unlock()
			policyLocked = false
			manager.runJobAsync(runContext, job.ID)
			return job, codebase, false, "", nil
		}
		if dedupedJob, dedupedCodebase, deduped := manager.dedupAgainstActiveJob(
			canonicalPath,
			indexConfig,
		); deduped {
			resolvedCodebase, resolveErr := manager.resolveDeduplicatedStartIndex(
				dedupedJob,
				dedupedCodebase,
				requestedPath,
				canonicalPath,
				client,
				indexConfig,
				force,
				policyIntent,
			)
			if resolveErr != nil {
				return emptyJob, emptyCodebase, false, "", resolveErr
			}
			return dedupedJob, resolvedCodebase, true, "", nil
		}
		if !force || !manager.hasActiveJobForExactPath(canonicalPath) {
			break
		}

		manager.policyMutationMutex.Unlock()
		policyLocked = false
		cancelErr := manager.cancelActiveJobForPath(ctx, canonicalPath)
		manager.policyMutationMutex.Lock()
		policyLocked = true
		if cancelErr != nil {
			return emptyJob, emptyCodebase, false, "", cancelErr
		}
	}

	evidence := manager.probeCollectionEvidence(ctx, canonicalPath, "StartIndex")
	job, codebase, deduped, overlapsCodebaseID, err := manager.commitStartIndexLocked(
		ctx,
		canonicalPath,
		requestedPath,
		client,
		indexConfig,
		force,
		evidence.presence,
		budget,
		policyIntent,
		recoveredPlan,
	)
	if err != nil || deduped {
		return job, codebase, deduped, overlapsCodebaseID, err
	}
	if job.ID == "" {
		return emptyJob, codebase, false, overlapsCodebaseID, nil
	}
	manager.notifyIndexCodebaseStart(ctx, codebase)
	manager.policyMutationMutex.Unlock()
	policyLocked = false
	manager.runJobAsync(manager.indexJobContext(ctx, job), job.ID)
	return job, codebase, false, overlapsCodebaseID, nil
}

func (manager *Manager) startIndexCanonicalPath(
	ctx context.Context,
	requestedPath string,
) (string, error) {
	canonicalPath, err := manager.resolveCanonicalPath(requestedPath)
	if err != nil {
		slog.ErrorContext(ctx, "canonicalize path failed", "path", requestedPath, "err", err)
		return "", fmt.Errorf("canonicalize path %s: %w", requestedPath, err)
	}
	if err := manager.guardStateRoot(canonicalPath); err != nil {
		return "", err
	}
	if err := manager.guardFilesystemRoot(canonicalPath); err != nil {
		return "", err
	}
	if err := manager.guardDirectory(canonicalPath); err != nil {
		return "", err
	}
	return canonicalPath, nil
}

func (manager *Manager) redirectStartIndex(
	ctx context.Context,
	requestedPath string,
	canonicalPath string,
	client model.ClientInfo,
	policyIntent indexPolicyIntent,
) (model.Job, model.Codebase, bool, error) {
	ancestor, found := manager.mergeUpTarget(canonicalPath)
	if !found || manager.isWorktreeBoundary(canonicalPath, ancestor) {
		var emptyJob model.Job
		var emptyCodebase model.Codebase
		return emptyJob, emptyCodebase, false, nil
	}
	job, codebase, err := manager.redirectIndexToAncestor(
		ctx,
		requestedPath,
		ancestor,
		client,
		policyIntent,
	)
	return job, codebase, true, err
}

func (manager *Manager) resolveDeduplicatedStartIndex(
	job model.Job,
	codebase model.Codebase,
	requestedPath string,
	canonicalPath string,
	client model.ClientInfo,
	indexConfig model.IndexConfig,
	force bool,
	policyIntent indexPolicyIntent,
) (model.Codebase, error) {
	var emptyCodebase model.Codebase
	resolvedCodebase, err := manager.resolveAndPersistIndexPolicy(
		codebase.ID,
		policyIntent,
	)
	if err != nil {
		return emptyCodebase, err
	}
	manager.queueDeduplicatedPolicyOverride(
		job,
		requestedPath,
		canonicalPath,
		client,
		indexConfig,
		force,
		policyIntent.Patch,
	)
	return resolvedCodebase, nil
}

func (manager *Manager) notifyIndexCodebaseStart(ctx context.Context, codebase model.Codebase) {
	notifyContext := correlation.WithContext(
		context.WithoutCancel(ctx),
		correlation.FromContext(ctx).Child(),
	)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.ErrorContext(notifyContext, "notify codebase added panic", "codebase_id", codebase.ID, "err", recovered)
			}
		}()
		manager.notifyCodebaseAdded(notifyContext, codebase)
	}()
}

func (manager *Manager) indexJobContext(ctx context.Context, job model.Job) context.Context {
	return spans.Attach(
		ctx,
		correlation.IdentityAttribute{Key: "job_id", Value: job.ID},
		correlation.IdentityAttribute{Key: "codebase_id", Value: job.CodebaseID},
	)
}

func (manager *Manager) hasActiveJobForExactPath(canonicalPath string) bool {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	codebase, found := manager.findCodebaseByExactRoot(canonicalPath)
	return found && manager.activeJobSnapshotLocked(codebase) != nil
}
