package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"goodkind.io/gklog/correlation"
	"goodkind.io/lm-semantic-search/internal/clock"
	"goodkind.io/lm-semantic-search/internal/gitworktree"
	"goodkind.io/lm-semantic-search/internal/model"
)

type missingCollectionRepair struct {
	codebaseID    string
	canonicalPath string
	config        model.IndexConfig
}

// RepairMissingCollections is the daemon-owned anti-entropy pass that
// reconciles the registry to collection reality in both directions. When the
// registry says a codebase is indexed but Milvus no longer has its collection,
// it keeps the codebase tracked, marks it stale, and re-queues a full bootstrap
// rebuild. When the registry says a codebase failed or is stale but its
// collection is present, it heals the codebase back to indexed and clears the
// stale failure, so the registry is the single source of truth every reader
// agrees with. Read paths stay side-effect free; this pass owns the mutation.
func (manager *Manager) RepairMissingCollections(ctx context.Context) {
	plans, cleanups, err := manager.planMissingCollectionRepairs(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "repair missing collections failed", "err", err)
		return
	}

	manager.cleanRemovedWorktrees(ctx, cleanups)

	queuedPaths := make([]string, 0, len(plans))
	for _, plan := range plans {
		_, _, _, _, err := manager.StartIndex(
			ctx,
			plan.canonicalPath,
			model.ClientInfo{Name: "daemon-repair", PID: 0},
			plan.config,
			false,
			emptyAdmissionBudget,
		)
		if err != nil {
			manager.noteAutomaticRepairStartFailure(ctx, plan.codebaseID, err)
			slog.WarnContext(
				ctx,
				"automatic rebuild enqueue failed for missing Milvus collection",
				"codebase_id",
				plan.codebaseID,
				"path",
				plan.canonicalPath,
				"err",
				err,
			)
			continue
		}
		queuedPaths = append(queuedPaths, plan.canonicalPath)
	}
	if len(queuedPaths) > 0 {
		slog.InfoContext(
			ctx,
			"queued automatic rebuilds for missing Milvus collections",
			"count",
			len(queuedPaths),
			"paths",
			queuedPaths,
		)
	}
}

// cleanRemovedWorktrees drops the disposable index (registration plus
// collection) for each removed git worktree. A removed worktree is a
// definitive, intentional deletion, so leaving a stale entry would diverge the
// registry from reality; ClearIndex works with the directory already gone. The
// success path logs one summary outside the loop rather than per iteration, so
// the cleaned paths land in a single state-transition record.
func (manager *Manager) cleanRemovedWorktrees(ctx context.Context, cleanups []string) {
	cleaned := make([]string, 0, len(cleanups))
	for _, canonicalPath := range cleanups {
		if _, clearErr := manager.ClearIndex(ctx, canonicalPath, model.ClientInfo{Name: "daemon-worktree-cleanup", PID: 0}); clearErr != nil {
			slog.WarnContext(ctx, "auto-clean of removed worktree failed", "path", canonicalPath, "err", clearErr)
			continue
		}
		cleaned = append(cleaned, canonicalPath)
	}
	if len(cleaned) > 0 {
		slog.InfoContext(
			ctx,
			"auto-cleaned removed git worktree index",
			"component",
			"daemon",
			"subcomponent",
			"repair",
			"count",
			len(cleaned),
			"paths",
			cleaned,
		)
	}
}

func (manager *Manager) planMissingCollectionRepairs(ctx context.Context) ([]missingCollectionRepair, []string, error) {
	if manager.semantic == nil || !manager.semantic.Available() {
		return nil, nil, nil
	}

	collections, err := manager.semantic.ListCollections(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("list semantic collections: %w", err)
	}

	collectionSet := make(map[string]struct{}, len(collections))
	for _, collectionName := range collections {
		collectionSet[collectionName] = struct{}{}
	}

	manager.policyMutationMutex.Lock()
	defer manager.policyMutationMutex.Unlock()
	manager.mu.Lock()
	defer manager.mu.Unlock()

	plans := make([]missingCollectionRepair, 0)
	cleanups := make([]string, 0)
	changed := false
	for codebaseID, codebase := range manager.codebases {
		outcome := manager.classifyCodebaseRepair(codebaseID, codebase, collectionSet)
		if outcome.persist {
			changed = true
		}
		if outcome.cleanup {
			cleanups = append(cleanups, codebase.CanonicalPath)
		}
		if outcome.plan != nil {
			plans = append(plans, *outcome.plan)
		}
	}

	if changed {
		if err := manager.saveLocked(); err != nil {
			slog.ErrorContext(ctx, "persist stale codebases before automatic rebuild failed", "err", err)
			return nil, nil, fmt.Errorf("persist stale codebases before automatic rebuild: %w", err)
		}
	}
	return plans, cleanups, nil
}

// repairOutcome is the per-codebase decision the planning loop applies: whether
// the registry record was mutated and needs persisting, whether the codebase is
// a removed worktree to auto-clean, and the rebuild plan to enqueue if any.
type repairOutcome struct {
	persist bool
	cleanup bool
	plan    *missingCollectionRepair
}

// classifyCodebaseRepair decides the repair action for one codebase under the
// manager lock. A missing source directory is reconciled before any collection
// logic: a removed git worktree (git deleted its admin entry) is flagged for
// auto-clean, and any other vanished directory is marked missing and kept since
// it may return.
func (manager *Manager) classifyCodebaseRepair(
	codebaseID string,
	codebase model.Codebase,
	collectionSet map[string]struct{},
) repairOutcome {
	if codebase.Kind == model.CodebaseKindDocument {
		return repairOutcome{persist: false, cleanup: false, plan: nil}
	}

	if _, statErr := os.Stat(codebase.CanonicalPath); errors.Is(statErr, os.ErrNotExist) {
		if codebase.WorktreeCommonDir != "" &&
			!sourceDirMissing(codebase.WorktreeCommonDir) &&
			!gitworktree.WorktreeTracked(codebase.WorktreeCommonDir, codebase.CanonicalPath) {
			return repairOutcome{persist: false, cleanup: true, plan: nil}
		}
		if codebase.Status != model.CodebaseStatusMissing || codebase.ActiveJobID != "" {
			codebase.Status = model.CodebaseStatusMissing
			codebase.ActiveJobID = ""
			codebase.UpdatedAt = clock.Now()
			manager.codebases[codebaseID] = codebase
			return repairOutcome{persist: true, cleanup: false, plan: nil}
		}
		return repairOutcome{persist: false, cleanup: false, plan: nil}
	}

	switch codebase.Status {
	case model.CodebaseStatusIndexed, model.CodebaseStatusStale, model.CodebaseStatusFailed,
		model.CodebaseStatusIndexing, model.CodebaseStatusNotIndexed, model.CodebaseStatusMissing:
	case model.CodebaseStatusPending:
		// A pending codebase normally has its first build live or coalesced, and
		// the repair pass leaves that alone. One with neither has lost its first
		// build (a queued job cancelled at a daemon shutdown parks it here), so
		// it falls through to the interrupted-build resume check below; no other
		// path ever re-queues it.
		if manager.activeJobSnapshotLocked(codebase) != nil {
			return repairOutcome{persist: false, cleanup: false, plan: nil}
		}
		if _, coalesced := manager.pendingCodeJobs[codebaseID]; coalesced {
			return repairOutcome{persist: false, cleanup: false, plan: nil}
		}
	case model.CodebaseStatusDiscovered:
		// A discovered worktree has no collection yet by design; its build is
		// driven by the deferred timer and the periodic sweep, so the repair
		// pass leaves it alone.
		return repairOutcome{persist: false, cleanup: false, plan: nil}
	case model.CodebaseStatusQuarantined:
		// Quarantined codebases are owned by the background-sync corroboration
		// loop, which decides whether to hold, clear, or resume destructive sync.
		return repairOutcome{persist: false, cleanup: false, plan: nil}
	default:
		return repairOutcome{persist: false, cleanup: false, plan: nil}
	}

	return manager.reconcileCodebaseCollection(codebaseID, codebase, collectionSet)
}

// reconcileCodebaseCollection compares a present-on-disk codebase against the
// live collection set and returns the repair action: heal a stale or failed
// codebase whose collection reappeared, re-queue an interrupted build, or mark a
// codebase with a missing collection stale and enqueue a full rebuild.
func (manager *Manager) reconcileCodebaseCollection(
	codebaseID string,
	codebase model.Codebase,
	collectionSet map[string]struct{},
) repairOutcome {
	persist := false
	expectedCollectionName := codebase.CollectionName
	if expectedCollectionName == "" {
		expectedCollectionName = manager.semantic.CollectionName(codebase.CanonicalPath)
		if expectedCollectionName != "" {
			codebase.CollectionName = expectedCollectionName
			manager.codebases[codebaseID] = codebase
			persist = true
		}
	}
	// Backfill the worktree common dir for a codebase indexed before that field
	// existed, while its directory is still present. Recording it now is what
	// lets the auto-clean recognize this worktree as removed once git deletes it
	// later; without the backfill an older worktree index would fall to missing
	// instead of being dropped.
	if codebase.WorktreeCommonDir == "" {
		if info, ok := gitworktree.Resolve(codebase.CanonicalPath); ok && info.Linked {
			codebase.WorktreeCommonDir = info.CommonDir
			manager.codebases[codebaseID] = codebase
			persist = true
		}
	}
	if expectedCollectionName == "" {
		return repairOutcome{persist: persist, cleanup: false, plan: nil}
	}
	presence := presenceFromCollectionSet(expectedCollectionName, collectionSet)
	hasActiveJob := manager.activeJobSnapshotLocked(codebase) != nil
	if shouldSkipForActiveFirstBuildStaging(codebase, hasActiveJob) {
		return repairOutcome{persist: persist, cleanup: false, plan: nil}
	}

	// Reconcile the other direction: a codebase marked failed or stale whose
	// collection is present now is usable, so heal it to indexed and clear the
	// stale failure rather than leaving the registry divergent from reality.
	if presence == collectionPresencePresent && !hasActiveJob &&
		(codebase.Status == model.CodebaseStatusFailed || codebase.Status == model.CodebaseStatusStale) {
		codebase.Status = model.CodebaseStatusIndexed
		codebase.LastFailedRun = nil
		codebase.UpdatedAt = clock.Now()
		manager.codebases[codebaseID] = codebase
		return repairOutcome{persist: true, cleanup: false, plan: nil}
	}

	plan := &missingCollectionRepair{
		codebaseID:    codebaseID,
		canonicalPath: codebase.CanonicalPath,
		config:        codebase.EffectiveConfig,
	}

	// An interrupted build (indexing or not_indexed with no live job) never
	// finished, so re-queue it to resume from its checkpoint or restart. This is
	// the auto-retry that makes a "preparing" presentation honest; only clearing
	// the index stops it.
	if shouldResumeInterruptedBuild(codebase, hasActiveJob) {
		return repairOutcome{persist: persist, cleanup: false, plan: plan}
	}

	if !shouldQueueMissingCollectionRepair(codebase, hasActiveJob, presence) {
		return repairOutcome{persist: persist, cleanup: false, plan: nil}
	}

	if codebase.Status != model.CodebaseStatusStale || codebase.ActiveJobID != "" {
		codebase.Status = model.CodebaseStatusStale
		codebase.ActiveJobID = ""
		codebase.UpdatedAt = clock.Now()
		manager.codebases[codebaseID] = codebase
		persist = true
	}
	return repairOutcome{persist: persist, cleanup: false, plan: plan}
}

func (manager *Manager) noteAutomaticRepairStartFailure(ctx context.Context, codebaseID string, startErr error) {
	manager.policyMutationMutex.Lock()
	defer manager.policyMutationMutex.Unlock()

	manager.mu.Lock()
	defer manager.mu.Unlock()

	codebase, found := manager.codebases[codebaseID]
	if !found {
		return
	}

	now := clock.Now()
	// Keep the raw enqueue error in the log; the persisted message stays a clean
	// summary so the status surface carries no implementation detail.
	slog.ErrorContext(ctx, "automatic rebuild could not start for missing collection", "codebase_id", codebaseID, "err", startErr)
	codebase.Status = model.CodebaseStatusStale
	codebase.ActiveJobID = ""
	codebase.LastFailedRun = &model.IndexRunFailure{
		Message:                 "semantic collection is missing and automatic rebuild could not start",
		Code:                    "",
		LastAttemptedPercentage: 0,
		FailedAt:                now,
		TraceID:                 string(correlation.FromContext(ctx).TraceID),
		JobID:                   "",
	}
	codebase.UpdatedAt = now
	manager.codebases[codebaseID] = codebase
	if err := manager.saveLocked(); err != nil {
		slog.ErrorContext(ctx, "write registry after automatic rebuild enqueue failure failed", "codebase_id", codebaseID, "err", err)
	}
}
