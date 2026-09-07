package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"os"
	"sort"

	"goodkind.io/lm-semantic-search/internal/indexer"
	"goodkind.io/lm-semantic-search/internal/merkle"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/semantic"
	"goodkind.io/lm-semantic-search/internal/spans"
	"goodkind.io/lm-semantic-search/internal/store"
)

// deltaPlan packages the file-set decision for one runDeltaSync invocation.
// fallback=true signals "no usable previous snapshot, route through full
// Replace instead". handled=true signals the helper already terminated the
// job (cancellation, snapshot-capture failure, or a no-op completion). The
// seedSnapshot is the previous on-disk checkpoint loaded under the
// requested ConfigDigest so the per-file loop can skip files already
// embedded by a prior crashed run.
type deltaPlan struct {
	diff            merkle.Diff
	currentSnapshot merkle.Snapshot
	seedSnapshot    merkle.Snapshot
	configDigest    string
	graphTask       *graphIndexTask
	// forced is the precomputed forced work set from source.forcedWorkSet: the
	// delivered ids that still have real missing work, already unioned into diff.
	// runDeltaSync turns it into the per-item forced set so applyDeltaChanges
	// skips the hash-equality short-circuit for exactly these ids.
	forced   []string
	fallback bool
	handled  bool
}

// deltaOutcome reports what happened inside a runDeltaSync step.
// fallback=true tells the caller to drop to full Replace. handled=true
// means the step terminated the job (failed, cancelled, or progressed
// normally and the caller should not run later steps). progressed=true means
// the step changed the working set (an item embedded or removed), which is
// what makes a checkpoint write worthwhile; a skipped item leaves it false so
// the per-item loop does not rewrite an unchanged snapshot.
type deltaOutcome struct {
	fallback   bool
	handled    bool
	progressed bool
}

type deltaState struct {
	plan         deltaPlan
	snapshotPath string
	working      map[string]string
	// source lists items and produces one item's chunks. It is the only
	// kind-specific part of the routine: a code source walks the filesystem, a
	// conversation source reads the manifest and documents handed over the wire.
	source           itemSource
	semantic         bool
	itemReuseEnabled bool
	// staging routes per-file embeds into the staging collection that a
	// from-scratch build promotes onto the live name at the end, instead of
	// the live collection an incremental sync writes to directly.
	staging bool
	// reuse maps a chunk's content hash to an already-embedded dense vector,
	// populated for a merge-down build from the collections of the indexed
	// child codebases the new parent absorbs. The embed step takes a reused
	// vector instead of calling the embedder, so the shared subtree is never
	// re-embedded. Nil for an ordinary build, which embeds every chunk.
	reuse map[string][]float32
	// chunkCounts accumulates chunk outcomes across every per-file reindex in this
	// run. It is a pointer because deltaState is copied by value through the
	// per-file loop, and the totals must survive each copy.
	chunkCounts *chunkCounters
	// seededReuse is the size of the reuse-vector pool loaded at bootstrap seed
	// time. The building view shows it immediately as the reuse total, so the
	// displayed reuse does not climb file-by-file from zero; the per-file accrual
	// can only rise to meet it as matching chunks are served from the pool.
	seededReuse int32
	admission   *admissionState
	// forced names items the source demands be re-examined even when their
	// fingerprint is unchanged, so applyDeltaChanges skips its hash-equality
	// short-circuit for them. See forcedItemsSet.
	forced map[string]struct{}
}

// chunkCounters accumulates the reuse-vs-embed split across one run's per-file
// reindex calls.
type chunkCounters struct {
	processed          int32
	reused             int32
	embedded           int32
	dropped            int32
	reuseVectorsLoaded int32
}

// applyReindexForState runs one per-file delta against the live collection, or
// against the staging collection when the job is a from-scratch build, and
// classifies any backend error into a deltaOutcome. Both targets go through
// the same delete-then-insert flow, so a re-embedded file is idempotent either
// way. A zero-value outcome means the embed succeeded and the caller should
// continue; a non-zero outcome means the step already resolved the job
// (failed, cancelled) or signalled a fallback to a full build.
func (manager *Manager) applyReindexForState(ctx context.Context, job model.Job, state deltaState, chunks []model.StoredChunk, removal semantic.Removal, phase string) deltaOutcome {
	if state.admission != nil {
		if err := state.admission.Admit(chunks); err != nil {
			return manager.handleAdmissionHalt(ctx, job, state, err)
		}
	}
	// Capture the last semantic progress for this one reindex call. The semantic
	// callback reports cumulative reused/embedded counts within the call, so the
	// final value is this file's split, which we fold into the run totals.
	//
	// The callback also fires once per embed batch, so it is the seam where a
	// long item becomes visible: publish the run's chunk counters, this item's
	// batch denominator, and a fresh heartbeat on every batch. accProcessed and
	// friends are the run total from items already finished; this item's in-flight
	// split is added on top, and the post-call fold below makes the total exact.
	accProcessed, accReused, accEmbedded, accDropped, _ := state.chunkSplit()
	var fileProcessed, fileReused, fileEmbedded, fileDropped int32
	progressFn := func(progress semantic.Progress) {
		fileProcessed = progress.ChunksProcessed
		fileReused = progress.ChunksReused
		fileEmbedded = progress.ChunksEmbedded
		fileDropped = progress.ChunksDropped
		manager.updateJobChunkProgress(
			job.ID,
			accProcessed+fileProcessed,
			accReused+fileReused,
			accEmbedded+fileEmbedded,
			accDropped+fileDropped,
			progress.EmbeddingBatchesTotal,
			progress.EmbeddingBatchesCompleted,
			progress.CollectionRowsWritten,
		)
	}
	columnSet := state.source.columnSet()
	var err error
	if state.staging {
		err = manager.semantic.StageReindex(ctx, job.CanonicalPath, chunks, removal, progressFn, state.reuse, columnSet)
	} else {
		err = manager.semantic.Reindex(ctx, job.CanonicalPath, chunks, removal, progressFn, state.reuse, columnSet)
	}
	if err != nil {
		return manager.classifyReindexErr(ctx, job, err, phase)
	}
	if state.chunkCounts != nil {
		if fileProcessed == 0 {
			fileProcessed = fileReused + fileEmbedded
		}
		state.chunkCounts.processed += fileProcessed
		state.chunkCounts.reused += fileReused
		state.chunkCounts.embedded += fileEmbedded
		state.chunkCounts.dropped += fileDropped
	}
	return deltaOutcome{fallback: false, handled: false, progressed: false}
}

func (manager *Manager) handleAdmissionHalt(ctx context.Context, job model.Job, state deltaState, err error) deltaOutcome {
	if state.staging {
		manager.cleanupHaltedStaging(ctx, job, state)
	}
	manager.updateJobFailed(ctx, job.ID, err)
	return deltaOutcome{fallback: false, handled: true, progressed: false}
}

// cleanupHaltedStaging drops the staging collection and removes the staging
// merkle checkpoint after an admission halt, so a halted bootstrap or forced
// rebuild leaves no partial staging state and never promotes over the live
// collection.
func (manager *Manager) cleanupHaltedStaging(ctx context.Context, job model.Job, state deltaState) {
	if manager.semantic != nil && manager.semantic.Available() {
		if dropErr := manager.semantic.DropStaging(ctx, job.CanonicalPath); dropErr != nil {
			slog.WarnContext(ctx, "drop staging after admission halt failed", "path", job.CanonicalPath, "err", dropErr)
		}
	}
	if state.snapshotPath != "" {
		if removeErr := store.RemoveFile(state.snapshotPath); removeErr != nil {
			slog.WarnContext(ctx, "remove staging checkpoint after admission halt failed", "path", state.snapshotPath, "err", removeErr)
		}
	}
}

// planSyncDiff loads the previous snapshot under the requested config
// digest, captures the current one, and returns the diff. An empty diff
// completes the job as a no-op only when the shared collection-state policy
// confirms the live collection is still present or otherwise not definitively
// missing. A missing snapshot produces an empty seed whose diff classifies
// every file as Added, which the per-file loop handles uniformly.
// failedSyncPlan is the terminal plan a planSyncDiff step returns after it has
// already set the job to a failed or cancelled state: an empty diff the caller
// treats as handled, carrying no forced work.
func failedSyncPlan(seed merkle.Snapshot, configDigest string) deltaPlan {
	return deltaPlan{
		diff:            merkle.Diff{Added: nil, Modified: nil, Removed: nil},
		currentSnapshot: merkle.Snapshot{ConfigDigest: "", Files: nil, Inodes: nil},
		seedSnapshot:    seed,
		configDigest:    configDigest,
		graphTask:       nil,
		forced:          nil,
		fallback:        false,
		handled:         true,
	}
}

func (manager *Manager) planSyncDiff(ctx context.Context, job model.Job, codebaseID string, source itemSource) deltaPlan {
	configDigest := job.Config.IgnoreDigest
	seed := manager.resolveSeed(ctx, job, codebaseID, false, false).seed
	captured, err := source.capture(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			manager.updateJobCancelled(ctx, job.ID)
		} else {
			manager.updateJobFailed(ctx, job.ID, fmt.Errorf("capture sync snapshot: %w", err))
		}
		return failedSyncPlan(seed, configDigest)
	}
	forced, forcedErr := source.forcedWorkSet(ctx)
	if forcedErr != nil {
		if errors.Is(forcedErr, context.Canceled) {
			manager.updateJobCancelled(ctx, job.ID)
		} else {
			manager.updateJobFailed(ctx, job.ID, fmt.Errorf("classify forced work set: %w", forcedErr))
		}
		return failedSyncPlan(seed, configDigest)
	}
	diff := merkle.DiffSnapshots(seed, captured)
	// forcedWorkSet has already pruned the delivered no-ops, so unioning its
	// result fixes the changed set (and therefore the progress denominator)
	// before the empty-diff fast path and the per-item loop. A fully-backfilled
	// reexamine yields an empty pruned set, so the empty-diff path completes it
	// instantly without regenerating anything.
	diff = unionForcedItems(diff, forced, captured)
	if diff.Empty() {
		evidence := manager.probeCollectionEvidence(ctx, job.CanonicalPath, "planSyncDiff")
		if decideEmptyDiffMode(evidence, len(seed.Files)) == emptyDiffModeFallbackBootstrap {
			manager.routeToBootstrap(ctx, job.ID, bootstrapReasonForEmptyDiffFallback(evidence))
			return deltaPlan{
				diff:            diff,
				currentSnapshot: captured,
				seedSnapshot:    seed,
				configDigest:    configDigest,
				graphTask:       nil,
				forced:          forced,
				fallback:        true,
				handled:         false,
			}
		}
		snapshotHash := snapshotHashForGraph(captured, configDigest)
		fileCount, chunkCount := manager.codebaseTotals(ctx, job.CanonicalPath, captured.Files, 0)
		var totalBytes int64
		manager.mu.Lock()
		if codebase, found := manager.codebases[codebaseID]; found && codebase.LastSuccessfulRun != nil {
			totalBytes = codebase.LastSuccessfulRun.TotalBytes
		}
		manager.mu.Unlock()
		emptyDiffResult := indexer.Result{
			IndexedFiles:      fileCount,
			TotalChunks:       chunkCount,
			TotalBytes:        totalBytes,
			Chunks:            nil,
			FileHashes:        captured.Files,
			SkippedFiles:      nil,
			SkippedOversize:   0,
			SkippedUnreadable: 0,
			SkippedPending:    0,
		}
		var graphTask *graphIndexTask
		if manager.shouldReconcileGraph(codebaseID, snapshotHash, evidence.presence) {
			graphTask = newGraphIndexTask(codebaseID, job.CanonicalPath, snapshotHash, func(completeCtx context.Context) {
				manager.updateJobCompleted(completeCtx, job.ID, emptyDiffResult)
			})
		}
		if graphTask == nil {
			manager.updateJobCompleted(ctx, job.ID, emptyDiffResult)
		}
		return deltaPlan{
			diff:            diff,
			currentSnapshot: captured,
			seedSnapshot:    seed,
			configDigest:    configDigest,
			graphTask:       graphTask,
			forced:          forced,
			fallback:        false,
			handled:         true,
		}
	}
	return deltaPlan{
		diff:            diff,
		currentSnapshot: captured,
		seedSnapshot:    seed,
		configDigest:    configDigest,
		graphTask:       nil,
		forced:          forced,
		fallback:        false,
		handled:         false,
	}
}

// runDeltaSync attempts the incremental sync path and returns true when it
// fully handled the job (success, failure, no-op, or cancellation). It
// returns false to fall back to the full Replace path when there is no
// previous snapshot or the semantic collection is gone.
//
// Both "sync" and "streaming_reindex" route here and share one plan: the
// merkle diff against the previous snapshot, processing only added and
// modified files. The streaming operation differs only in a post-pass prune
// that drops rows for files no longer present, which covers the splitter
// upgrade where the empty seed re-embeds everything.
func (manager *Manager) runDeltaSync(ctx context.Context, job model.Job, source itemSource) (bool, *graphIndexTask) {
	ctx, done := spans.Open(ctx, "daemon.runDeltaSync")
	defer done(nil)

	manager.mu.Lock()
	codebase, codebaseFound := manager.codebases[job.CodebaseID]
	manager.mu.Unlock()
	if !codebaseFound {
		manager.routeToBootstrap(ctx, job.ID, bootstrapReasonDeltaCodebaseMissing)
		return false, nil
	}

	streamingReindex := jobOperation(job.Operation) == jobOperationStreamingReindex
	plan := manager.planSyncDiff(ctx, job, codebase.ID, source)
	if plan.fallback {
		return false, nil
	}
	if plan.handled {
		return true, plan.graphTask
	}
	if manager.applyDeltaAbsencePolicy(ctx, job, codebase, source, &plan) {
		return true, nil
	}
	manager.setJobRunMode(job.ID, model.RunModeChanged)

	// Record the change breakdown so the status and job views can report the
	// magnitude of this reconcile while it runs. The per-file progress updates
	// touch only the embed counters, so these counts persist for the run.
	manager.setJobDeltaCounts(job.ID, len(plan.diff.Added), len(plan.diff.Modified), len(plan.diff.Removed), len(plan.currentSnapshot.Files))

	semanticReady := manager.semantic != nil && manager.semantic.Available()
	state := deltaState{
		plan:             plan,
		snapshotPath:     manager.merklePath(codebase.ID),
		working:          make(map[string]string, len(plan.seedSnapshot.Files)),
		source:           source,
		semantic:         semanticReady,
		itemReuseEnabled: manager.resolveItemReusePolicy(ctx, job, false, semanticReady),
		staging:          false,
		reuse:            nil,
		chunkCounts:      &chunkCounters{processed: 0, reused: 0, embedded: 0, dropped: 0, reuseVectorsLoaded: 0},
		seededReuse:      0,
		admission:        manager.admissionForJob(job),
		forced:           forcedItemsSet(plan.forced),
	}
	maps.Copy(state.working, plan.seedSnapshot.Files)

	if outcome := manager.applyDeltaRemovals(ctx, job, state); outcome.fallback {
		return false, nil
	} else if outcome.handled {
		return true, nil
	}

	if len(plan.diff.Added) > 0 && state.semantic {
		reuse, seeded, _, reuseErr := manager.resolveReuseSeed(ctx, job)
		if reuseErr != nil {
			manager.finishJobForReuseFailure(ctx, job.ID, reuseErr)
			return true, nil
		}
		state.reuse = reuse
		state.seededReuse = seeded
		state.chunkCounts.reuseVectorsLoaded = seeded
	}

	result, outcome := manager.applyDeltaChanges(ctx, job, state)
	if outcome.fallback {
		return false, nil
	}
	if outcome.handled {
		return true, nil
	}

	if streamingReindex && state.semantic {
		if outcome := manager.pruneAfterStreaming(ctx, job, plan.currentSnapshot); outcome.handled {
			return true, nil
		}
	}

	result.FileHashes = state.working
	fileCount, chunkCount := manager.codebaseTotals(ctx, job.CanonicalPath, state.working, result.TotalChunks)
	result.IndexedFiles = fileCount
	result.TotalChunks = chunkCount
	var normalizeErr error
	result, normalizeErr = manager.normalizeDeltaTotalBytes(ctx, codebase, state, result)
	if normalizeErr != nil {
		manager.updateJobFailed(ctx, job.ID, normalizeErr)
		return true, nil
	}
	var graphTask *graphIndexTask
	if source.producesGraph() {
		// The graph task is stamped with the hash of the snapshot this run
		// committed (the working set), not the captured one: skipped, pending,
		// or unreadable paths can leave the checkpoint behind the capture, and
		// reconciliation compares against the committed checkpoint.
		committedSnapshot := merkle.Snapshot{ConfigDigest: plan.configDigest, Files: state.working, Inodes: nil}
		graphTask = newGraphIndexTask(codebase.ID, job.CanonicalPath, snapshotHashForGraph(committedSnapshot, plan.configDigest), func(completeCtx context.Context) {
			manager.updateJobCompleted(completeCtx, job.ID, result)
		})
	}
	if graphTask == nil {
		manager.updateJobCompleted(ctx, job.ID, result)
	}
	return true, graphTask
}

// runBootstrap builds a codebase index from scratch into a staging collection
// and swaps it onto the live name only once every file is embedded, so a
// search never observes a half-built first index. It embeds file by file and
// checkpoints after each, so an interrupted build resumes by skipping the
// files already embedded rather than restarting from the first one. Every
// operation that lacks a usable incremental delta routes here: a true first
// index, a forced rebuild, and a sync or streaming reindex whose live
// collection has gone missing.
func (manager *Manager) runBootstrap(ctx context.Context, job model.Job, source itemSource) *graphIndexTask {
	ctx, done := spans.Open(ctx, "daemon.runBootstrap")
	defer done(nil)

	manager.mu.Lock()
	codebase, codebaseFound := manager.codebases[job.CodebaseID]
	manager.mu.Unlock()
	if !codebaseFound {
		return nil
	}
	semanticReady := manager.semantic != nil && manager.semantic.Available()
	if semanticReady {
		stagingPin, err := manager.semantic.PinStaging(ctx, job.CanonicalPath)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				manager.updateJobCancelled(ctx, job.ID)
			} else {
				manager.updateJobFailed(ctx, job.ID, fmt.Errorf("pin staging collection: %w", err))
			}
			return nil
		}
		defer stagingPin.Release()
	}

	plan := manager.planBootstrap(ctx, job, codebase.ID, source)
	if plan.handled {
		return nil
	}
	runMode := model.RunModeFirstBuild
	if len(plan.seedSnapshot.Files) > 0 {
		runMode = model.RunModeResuming
	}
	if job.Forced && codebase.LastSuccessfulRun != nil {
		runMode = model.RunModeForcedReindex
	}
	manager.setJobRunMode(job.ID, runMode)

	state := deltaState{
		plan:             plan,
		snapshotPath:     manager.stagingMerklePath(codebase.ID),
		working:          make(map[string]string, len(plan.currentSnapshot.Files)),
		source:           source,
		semantic:         semanticReady,
		itemReuseEnabled: manager.resolveItemReusePolicy(ctx, job, true, semanticReady),
		staging:          true,
		reuse:            nil,
		chunkCounts:      &chunkCounters{processed: 0, reused: 0, embedded: 0, dropped: 0, reuseVectorsLoaded: 0},
		seededReuse:      0,
		admission:        manager.admissionForJob(job),
		// A bootstrap re-embeds every item, so nothing needs per-item forcing.
		forced: nil,
	}
	maps.Copy(state.working, plan.seedSnapshot.Files)

	reuse, seeded, descendants, reuseErr := manager.resolveReuseSeed(ctx, job)
	if reuseErr != nil {
		manager.finishJobForReuseFailure(ctx, job.ID, reuseErr)
		return nil
	}
	state.reuse = reuse
	state.seededReuse = seeded
	state.chunkCounts.reuseVectorsLoaded = seeded

	result, outcome := manager.applyDeltaChanges(ctx, job, state)
	if outcome.handled {
		return nil
	}
	if outcome.fallback {
		// A from-scratch build has no further fallback; treat one as a failure
		// so the job reaches a terminal state instead of stalling in running.
		manager.updateJobFailed(ctx, job.ID, errors.New("bootstrap build could not complete against the semantic backend"))
		return nil
	}

	if promote := manager.promoteBootstrap(ctx, job, state); promote.handled {
		return nil
	}
	if promote := manager.promoteStagingMerkle(ctx, job, state); promote.handled {
		return nil
	}

	manager.absorbDescendants(ctx, descendants)
	var graphTask *graphIndexTask
	if source.producesGraph() {
		graphTask = newGraphIndexTask(job.CodebaseID, job.CanonicalPath, snapshotHashForGraph(plan.currentSnapshot, plan.configDigest), func(completeCtx context.Context) {
			manager.updateJobCompleted(completeCtx, job.ID, result)
		})
	}

	result.FileHashes = state.working
	fileCount, chunkCount := manager.codebaseTotals(ctx, job.CanonicalPath, state.working, result.TotalChunks)
	result.IndexedFiles = fileCount
	result.TotalChunks = chunkCount
	result.FileHashes = nil
	if graphTask == nil {
		manager.updateJobCompleted(ctx, job.ID, result)
	}
	return graphTask
}

// planBootstrap captures a fresh snapshot for a from-scratch build and decides
// whether a persisted checkpoint can seed a resume. Every discovered file is
// classified Added so the per-file loop embeds it unless the checkpoint
// already records its current hash. The checkpoint seeds a resume only when a
// staging collection still backs it; otherwise the build restarts from the
// first file and any stale staging is dropped first.
func (manager *Manager) planBootstrap(ctx context.Context, job model.Job, codebaseID string, source itemSource) deltaPlan {
	configDigest := job.Config.IgnoreDigest
	semanticReady := manager.semantic != nil && manager.semantic.Available()

	captured, err := source.capture(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			manager.updateJobCancelled(ctx, job.ID)
		} else {
			manager.updateJobFailed(ctx, job.ID, fmt.Errorf("capture bootstrap snapshot: %w", err))
		}
		return deltaPlan{
			diff:            merkle.Diff{Added: nil, Modified: nil, Removed: nil},
			currentSnapshot: merkle.Snapshot{ConfigDigest: "", Files: nil, Inodes: nil},
			seedSnapshot:    merkle.Snapshot{ConfigDigest: configDigest, Files: nil, Inodes: nil},
			configDigest:    configDigest,
			graphTask:       nil,
			forced:          nil,
			fallback:        false,
			handled:         true,
		}
	}
	seedDecision := manager.resolveSeed(ctx, job, codebaseID, true, semanticReady)

	addedFiles := make([]string, 0, len(captured.Files))
	for relativePath := range captured.Files {
		addedFiles = append(addedFiles, relativePath)
	}
	sort.Strings(addedFiles)
	return deltaPlan{
		diff:            merkle.Diff{Added: addedFiles, Modified: nil, Removed: nil},
		currentSnapshot: captured,
		seedSnapshot:    seedDecision.seed,
		configDigest:    configDigest,
		graphTask:       nil,
		forced:          nil,
		fallback:        false,
		handled:         false,
	}
}

func (manager *Manager) promoteStagingMerkle(ctx context.Context, job model.Job, state deltaState) deltaOutcome {
	if !state.staging {
		return deltaOutcome{fallback: false, handled: false, progressed: false}
	}
	if state.snapshotPath == "" {
		return deltaOutcome{fallback: false, handled: false, progressed: false}
	}
	livePath := manager.merklePath(job.CodebaseID)
	if err := os.Rename(state.snapshotPath, livePath); err != nil {
		if errors.Is(err, os.ErrNotExist) && len(state.working) == 0 {
			return deltaOutcome{fallback: false, handled: false, progressed: false}
		}
		manager.cleanupHaltedStaging(ctx, job, state)
		manager.updateJobFailed(ctx, job.ID, fmt.Errorf("promote staging Merkle snapshot: %w", err))
		return deltaOutcome{fallback: false, handled: true, progressed: false}
	}
	slog.InfoContext(ctx, "promoted staging Merkle snapshot to live", "component", "daemon", "subcomponent", "delta", "codebase_id", job.CodebaseID, "path", livePath)
	return deltaOutcome{fallback: false, handled: false, progressed: false}
}

// promoteBootstrap swaps the freshly built staging collection onto the live
// name. When no file produced chunks there is no staging collection to
// promote, which is a successful empty index rather than an error. A handled
// outcome means promoteBootstrap already set a terminal job state.
func (manager *Manager) promoteBootstrap(ctx context.Context, job model.Job, state deltaState) deltaOutcome {
	if !state.semantic {
		return deltaOutcome{fallback: false, handled: false, progressed: false}
	}
	hasStaging, err := manager.semantic.HasStaging(ctx, job.CanonicalPath)
	if err != nil {
		manager.updateJobFailed(ctx, job.ID, err)
		return deltaOutcome{fallback: false, handled: true, progressed: false}
	}
	if !hasStaging {
		return deltaOutcome{fallback: false, handled: false, progressed: false}
	}
	if err := manager.semantic.PromoteStaging(ctx, job.CanonicalPath); err != nil {
		manager.cleanupHaltedStaging(ctx, job, state)
		if errors.Is(err, context.Canceled) {
			manager.updateJobCancelled(ctx, job.ID)
		} else {
			manager.updateJobFailed(ctx, job.ID, fmt.Errorf("promote staging collection: %w", err))
		}
		return deltaOutcome{fallback: false, handled: true, progressed: false}
	}
	return deltaOutcome{fallback: false, handled: false, progressed: false}
}

func (manager *Manager) applyDeltaRemovals(ctx context.Context, job model.Job, state deltaState) deltaOutcome {
	removed := state.plan.diff.Removed
	if len(removed) == 0 || !state.semantic {
		return deltaOutcome{fallback: false, handled: false, progressed: false}
	}
	if outcome := manager.applyReindexForState(ctx, job, state, nil, state.source.removalFor(removed), "delta removal"); outcome.fallback || outcome.handled {
		return outcome
	}
	for _, path := range removed {
		delete(state.working, path)
	}
	manager.writeCheckpoint(ctx, state, "removals")
	if err := manager.checkpointJob(ctx); err != nil {
		return deltaOutcome{fallback: false, handled: true, progressed: false}
	}
	return deltaOutcome{fallback: false, handled: false, progressed: false}
}

func (manager *Manager) applyDeltaChanges(ctx context.Context, job model.Job, state deltaState) (indexer.Result, deltaOutcome) {
	changed := make([]string, 0, len(state.plan.diff.Added)+len(state.plan.diff.Modified))
	changed = append(changed, state.plan.diff.Added...)
	changed = append(changed, state.plan.diff.Modified...)

	totalChanged := len(changed)
	totalFiles := safeInt32(totalChanged)
	result := indexer.Result{
		IndexedFiles:      0,
		TotalChunks:       0,
		TotalBytes:        0,
		Chunks:            make([]model.StoredChunk, 0),
		FileHashes:        nil,
		SkippedFiles:      []string{},
		SkippedOversize:   0,
		SkippedUnreadable: 0,
		SkippedPending:    0,
	}
	for index, relativePath := range changed {
		if err := ctx.Err(); err != nil {
			manager.updateJobCancelled(ctx, job.ID)
			return result, deltaOutcome{fallback: false, handled: true, progressed: false}
		}
		_, forced := state.forced[relativePath]
		if seedHash, present := state.plan.seedSnapshot.Files[relativePath]; present && !forced && seedHash == state.plan.currentSnapshot.Files[relativePath] {
			state.working[relativePath] = seedHash
			processed, reused, embedded, dropped, loaded := state.chunkSplit()
			manager.reportDeltaProgress(job.ID, safeInt32(index+1), totalChanged, totalFiles, result, processed, reused, embedded, dropped, loaded, state.source.unit())
			if err := manager.checkpointJob(ctx); err != nil {
				return result, deltaOutcome{fallback: false, handled: true, progressed: false}
			}
			continue
		}
		outcome := manager.handleChangedFile(ctx, job, state, relativePath, &result)
		if outcome.fallback || outcome.handled {
			return result, outcome
		}
		// A skipped item changes nothing in the working set, so rewriting the
		// snapshot for it would be one full-file disk write per skipped item; a
		// job that skips a thousand undelivered conversations checkpoints only
		// after the items that actually embedded or removed.
		if outcome.progressed {
			manager.writeCheckpoint(ctx, state, relativePath)
		}
		processed, reused, embedded, dropped, loaded := state.chunkSplit()
		manager.reportDeltaProgress(job.ID, safeInt32(index+1), totalChanged, totalFiles, result, processed, reused, embedded, dropped, loaded, state.source.unit())
		if err := manager.checkpointJob(ctx); err != nil {
			return result, deltaOutcome{fallback: false, handled: true, progressed: false}
		}
	}
	return result, deltaOutcome{fallback: false, handled: false, progressed: false}
}

func (manager *Manager) handleChangedFile(ctx context.Context, job model.Job, state deltaState, relativePath string, result *indexer.Result) deltaOutcome {
	fileResult, err := state.source.indexOne(ctx, relativePath)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			manager.updateJobCancelled(ctx, job.ID)
		} else {
			manager.updateJobFailed(ctx, job.ID, err)
		}
		return deltaOutcome{fallback: false, handled: true, progressed: false}
	}
	if fileResult.Removed {
		return manager.handleRemovedFile(ctx, job, state, relativePath, fileResult)
	}
	if fileResult.Skipped {
		switch fileResult.SkipReason {
		case indexer.SkipOversize:
			result.SkippedFiles = append(result.SkippedFiles, relativePath)
			result.SkippedOversize++
		case indexer.SkipPending:
			// Transient, not a permanent skip, so it stays out of SkippedFiles (the
			// ready-view's non-UTF-8 summary) and counts only as pending.
			result.SkippedPending++
		case indexer.SkipUnreadable, indexer.SkipNone:
			result.SkippedFiles = append(result.SkippedFiles, relativePath)
			result.SkippedUnreadable++
		}
		return deltaOutcome{fallback: false, handled: false, progressed: false}
	}
	if state.semantic {
		removal := effectiveRemoval(state.source, fileResult, relativePath)
		if outcome := manager.applyChangedFileSemantic(ctx, job, state, relativePath, fileResult, removal); outcome.fallback || outcome.handled {
			return outcome
		}
	}
	state.working[relativePath] = fileResult.FileHash
	result.Chunks = append(result.Chunks, fileResult.Chunks...)
	result.TotalChunks += safeInt32(len(fileResult.Chunks))
	result.TotalBytes += storedChunkBytes(fileResult.Chunks)
	result.IndexedFiles++
	return deltaOutcome{fallback: false, handled: false, progressed: true}
}

func (manager *Manager) handleRemovedFile(ctx context.Context, job model.Job, state deltaState, relativePath string, fileResult indexer.OneFileResult) deltaOutcome {
	slog.InfoContext(ctx, "converge.remove", "component", "daemon", "subcomponent", "delta", "path", relativePath, "semantic", state.semantic)
	if state.semantic {
		removal := effectiveRemoval(state.source, fileResult, relativePath)
		// Empty removals are valid delete-nothing overrides, so they advance state
		// without a semantic write.
		if !removal.Empty() {
			if outcome := manager.applyReindexForState(ctx, job, state, nil, removal, "per-file removal"); outcome.fallback || outcome.handled {
				return outcome
			}
		}
	}
	delete(state.working, relativePath)
	return deltaOutcome{fallback: false, handled: false, progressed: true}
}

func (manager *Manager) applyChangedFileSemantic(ctx context.Context, job model.Job, state deltaState, relativePath string, fileResult indexer.OneFileResult, removal semantic.Removal) deltaOutcome {
	var reuseErr error
	state, reuseErr = manager.reuseStateForChangedFile(
		ctx,
		state,
		relativePath,
		fileResult,
		removal,
		job.Forced,
	)
	if reuseErr != nil {
		manager.finishJobForReuseFailure(ctx, job.ID, reuseErr)
		return deltaOutcome{fallback: false, handled: true, progressed: false}
	}
	if len(fileResult.Chunks) == 0 && removal.Empty() {
		return deltaOutcome{fallback: false, handled: false, progressed: false}
	}
	return manager.applyReindexForState(ctx, job, state, fileResult.Chunks, removal, "per-file reindex")
}

func effectiveRemoval(source itemSource, fileResult indexer.OneFileResult, relativePath string) semantic.Removal {
	if fileResult.RemovalOverride {
		return semantic.Removal{Paths: fileResult.RemovalPaths, Prefixes: fileResult.RemovalPrefixes}
	}
	return source.removalFor([]string{relativePath})
}

// itemReuse loads one item's own stored vectors before the reindex deletes
// them, so chunks whose content is unchanged take their vector from the store
// instead of the embedder. It returns the build-wide reuse map unchanged when
// the source has no per-item reuse, and on a failed load it logs and falls
// back to that map so the item embeds every chunk rather than failing.
//
// It has one terminal outcome of its own. A stalled read durably pauses and
// yields its scheduler lease. Cancellation or a permanent lock error while
// resuming returns jobCapacityReacquireError instead of continuing without the
// lease.
func (manager *Manager) itemReuse(
	ctx context.Context,
	state deltaState,
	relativePath string,
) (map[string][]float32, int32, error) {
	if !state.itemReuseEnabled {
		return state.reuse, 0, nil
	}
	source := state.source.reuseSource(relativePath)
	if source.Scope == itemReuseScopeNone {
		return state.reuse, 0, nil
	}
	var itemReuse map[string][]float32
	err := manager.runReleasingCapacityIfStalled(ctx, func() error {
		var loadErr error
		switch source.Scope {
		case itemReuseScopeNone:
			return nil
		case itemReuseScopePath:
			itemReuse, loadErr = manager.semantic.LoadReuseVectorsForPath(
				ctx,
				source.CollectionName,
				source.RelativePath,
			)
		case itemReuseScopePrefix:
			itemReuse, loadErr = manager.semantic.LoadReuseVectorsForPrefix(
				ctx,
				source.CollectionName,
				source.RelativePath,
			)
		default:
			return nil
		}
		if loadErr == nil {
			return nil
		}
		slog.WarnContext(ctx, "load item reuse vectors from semantic backend failed",
			"path", relativePath,
			"collection", source.CollectionName,
			"scope", source.Scope,
			"err", loadErr,
		)
		return fmt.Errorf("load item reuse vectors: %w", loadErr)
	})
	if err != nil {
		var reacquireErr *jobCapacityReacquireError
		if errors.As(err, &reacquireErr) {
			return state.reuse, 0, reacquireErr
		}
		slog.WarnContext(ctx, "load item reuse vectors failed; embedding every chunk", "path", relativePath, "collection", source.CollectionName, "scope", source.Scope, "err", err)
		return state.reuse, 0, nil
	}
	if len(itemReuse) == 0 {
		return state.reuse, 0, nil
	}
	return mergedReuse(state.reuse, itemReuse), safeInt32(len(itemReuse)), nil
}

func (manager *Manager) finishJobForReuseFailure(ctx context.Context, jobID string, err error) {
	if errors.Is(err, context.Canceled) {
		manager.updateJobCancelled(ctx, jobID)
		return
	}
	manager.updateJobFailed(ctx, jobID, err)
}

// mergedReuse overlays an item's own reuse vectors on any build-wide reuse map
// without mutating either input. With no build-wide map the item map is used
// as-is, which is the conversation delta case.
func mergedReuse(base map[string][]float32, item map[string][]float32) map[string][]float32 {
	if len(base) == 0 {
		return item
	}
	merged := make(map[string][]float32, len(base)+len(item))
	maps.Copy(merged, base)
	maps.Copy(merged, item)
	return merged
}

func (manager *Manager) classifyReindexErr(ctx context.Context, job model.Job, err error, phase string) deltaOutcome {
	switch {
	case errors.Is(err, semantic.ErrCollectionMissing):
		manager.routeToBootstrap(ctx, job.ID, bootstrapReasonDeltaCollectionMissing)
		slog.WarnContext(ctx, "semantic collection missing; falling back to full reindex", "job_id", job.ID, "phase", phase)
		return deltaOutcome{fallback: true, handled: false, progressed: false}
	case errors.Is(err, context.Canceled):
		manager.updateJobCancelled(ctx, job.ID)
		return deltaOutcome{fallback: false, handled: true, progressed: false}
	default:
		manager.updateJobFailed(ctx, job.ID, err)
		return deltaOutcome{fallback: false, handled: true, progressed: false}
	}
}

func bootstrapReasonForEmptyDiffFallback(evidence collectionEvidence) bootstrapReason {
	if evidence.presence == collectionPresencePresent && evidence.rowsKnown && evidence.rows == 0 {
		return bootstrapReasonEmptyDiffCollectionEmpty
	}
	return bootstrapReasonEmptyDiffCollectionMissing
}

func (manager *Manager) writeCheckpoint(ctx context.Context, state deltaState, label string) {
	snapshot := merkle.Snapshot{ConfigDigest: state.plan.configDigest, Files: state.working, Inodes: nil}
	if err := merkle.WriteSnapshot(state.snapshotPath, snapshot); err != nil {
		slog.ErrorContext(ctx, "checkpoint write failed", "path", state.snapshotPath, "label", label, "err", err)
		return
	}
}

// phaseReindexingChanged is the job phase while the per-file embed loop runs,
// shared by every progress update from that loop.
const phaseReindexingChanged = "Reindexing changed files..."

// chunkSplit returns the run's processed, reused, embedded, dropped, and
// reuse-vectors-loaded counts for progress reporting. reused is the chunks
// actually served from the reuse pool this run, so it never exceeds processed.
// The seeded pool size is reported separately as the reuse-vectors-loaded
// figure, which the building view can show immediately for context without
// inflating the reused count.
func (state deltaState) chunkSplit() (int32, int32, int32, int32, int32) {
	if state.chunkCounts == nil {
		return 0, 0, 0, 0, state.seededReuse
	}
	loaded := max(state.chunkCounts.reuseVectorsLoaded, state.seededReuse)
	return state.chunkCounts.processed, state.chunkCounts.reused, state.chunkCounts.embedded, state.chunkCounts.dropped, loaded
}

// reportDeltaProgress publishes one progress update from the per-file embed
// loop. processed is the number of files the loop has finished. An initial call
// with processed=0 establishes the embedding phase and the to-process total, so
// the status shows the indexing view before the first (possibly slow) embed
// rather than a stalled "Preparing". reused and embedded are the run's
// accumulated chunk split; ChunksGenerated carries the embedded-this-run total
// so a surface can show total = reused + embedded.
func (manager *Manager) reportDeltaProgress(jobID string, processed int32, totalChanged int, totalFiles int32, result indexer.Result, chunksProcessed int32, reused int32, embedded int32, dropped int32, reuseVectorsLoaded int32, unit string) {
	manager.updateJobProgress(jobID, indexer.Progress{
		Phase:                  phaseReindexingChanged,
		OverallPercent:         float64(processed) / float64(maxInt(totalChanged, 1)) * 100,
		FilesTotal:             totalFiles,
		FilesProcessed:         processed,
		FilesEmbedded:          result.IndexedFiles,
		FilesSkippedOversize:   result.SkippedOversize,
		FilesSkippedUnreadable: result.SkippedUnreadable,
		FilesPending:           result.SkippedPending,
		ChunksProcessed:        chunksProcessed,
		ChunksReused:           reused,
		ChunksEmbedded:         embedded,
		ChunksGenerated:        embedded,
		ChunksDropped:          dropped,
		ReuseVectorsLoaded:     reuseVectorsLoaded,
	}, unit)
}

func (manager *Manager) pruneAfterStreaming(ctx context.Context, job model.Job, currentSnapshot merkle.Snapshot) deltaOutcome {
	currentPaths := make([]string, 0, len(currentSnapshot.Files))
	for relativePath := range currentSnapshot.Files {
		currentPaths = append(currentPaths, relativePath)
	}
	sort.Strings(currentPaths)
	if err := manager.semantic.PruneToCurrent(ctx, job.CanonicalPath, currentPaths); err != nil {
		switch {
		case errors.Is(err, context.Canceled):
			manager.updateJobCancelled(ctx, job.ID)
			return deltaOutcome{fallback: false, handled: true, progressed: false}
		case errors.Is(err, semantic.ErrCollectionMissing):
			slog.WarnContext(ctx, "semantic collection missing during streaming prune", "job_id", job.ID)
		default:
			manager.updateJobFailed(ctx, job.ID, err)
			return deltaOutcome{fallback: false, handled: true, progressed: false}
		}
	}
	return deltaOutcome{fallback: false, handled: false, progressed: false}
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// safeInt32 clamps int to int32 for protobuf-bound progress fields.
func safeInt32(value int) int32 {
	if value > math.MaxInt32 {
		return math.MaxInt32
	}
	if value < math.MinInt32 {
		return math.MinInt32
	}
	return int32(value)
}
