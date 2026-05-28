package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"sort"

	"goodkind.io/claude-context-go/internal/indexer"
	"goodkind.io/claude-context-go/internal/merkle"
	"goodkind.io/claude-context-go/internal/model"
	"goodkind.io/claude-context-go/internal/semantic"
	"goodkind.io/claude-context-go/internal/spans"
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
	fallback        bool
	handled         bool
}

// deltaOutcome reports what happened inside a runDeltaSync step.
// fallback=true tells the caller to drop to full Replace. handled=true
// means the step terminated the job (failed, cancelled, or progressed
// normally and the caller should not run later steps).
type deltaOutcome struct {
	fallback bool
	handled  bool
}

type deltaState struct {
	plan         deltaPlan
	snapshotPath string
	working      map[string]string
	semantic     bool
}

// planStreamingReindex captures a fresh merkle snapshot and synthesizes a
// diff where every discovered file counts as "modified". The streaming
// path also loads the previous checkpoint so the per-file loop can skip
// any file whose content hash is already recorded under the same config.
func (manager *Manager) planStreamingReindex(ctx context.Context, job model.Job, codebaseID string) deltaPlan {
	configDigest := job.Config.IgnoreDigest
	legacyDigest := manager.legacyDigestForCodebase(codebaseID)
	seed := merkle.LoadSnapshotForConfig(manager.merklePath(codebaseID), configDigest, legacyDigest)
	captured, err := merkle.Capture(ctx, job.CanonicalPath, job.Config)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			manager.updateJobCancelled(ctx, job.ID)
		} else {
			manager.updateJobFailed(ctx, job.ID, fmt.Errorf("capture reindex snapshot: %w", err))
		}
		return deltaPlan{
			diff:            merkle.Diff{Added: nil, Modified: nil, Removed: nil},
			currentSnapshot: merkle.Snapshot{ConfigDigest: "", Files: nil},
			seedSnapshot:    seed,
			configDigest:    configDigest,
			fallback:        false,
			handled:         true,
		}
	}
	modifiedFiles := make([]string, 0, len(captured.Files))
	for relativePath := range captured.Files {
		modifiedFiles = append(modifiedFiles, relativePath)
	}
	sort.Strings(modifiedFiles)
	return deltaPlan{
		diff:            merkle.Diff{Added: nil, Modified: modifiedFiles, Removed: nil},
		currentSnapshot: captured,
		seedSnapshot:    seed,
		configDigest:    configDigest,
		fallback:        false,
		handled:         false,
	}
}

// planSyncDiff loads the previous snapshot under the requested config
// digest, captures the current one, and returns the diff. An empty diff
// completes the job as a no-op. A missing snapshot produces an empty seed
// whose diff classifies every file as Added, which the per-file loop
// handles uniformly.
func (manager *Manager) planSyncDiff(ctx context.Context, job model.Job, codebaseID string) deltaPlan {
	configDigest := job.Config.IgnoreDigest
	snapshotPath := manager.merklePath(codebaseID)
	legacyDigest := manager.legacyDigestForCodebase(codebaseID)
	seed := merkle.LoadSnapshotForConfig(snapshotPath, configDigest, legacyDigest)
	captured, err := merkle.Capture(ctx, job.CanonicalPath, job.Config)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			manager.updateJobCancelled(ctx, job.ID)
		} else {
			manager.updateJobFailed(ctx, job.ID, fmt.Errorf("capture sync snapshot: %w", err))
		}
		return deltaPlan{
			diff:            merkle.Diff{Added: nil, Modified: nil, Removed: nil},
			currentSnapshot: merkle.Snapshot{ConfigDigest: "", Files: nil},
			seedSnapshot:    seed,
			configDigest:    configDigest,
			fallback:        false,
			handled:         true,
		}
	}
	diff := merkle.DiffSnapshots(seed, captured)
	if diff.Empty() {
		fileCount, chunkCount := manager.codebaseTotals(ctx, job.CanonicalPath, captured.Files, 0)
		manager.updateJobCompleted(job.ID, indexer.Result{
			IndexedFiles: fileCount,
			TotalChunks:  chunkCount,
			Chunks:       nil,
			FileHashes:   captured.Files,
			SkippedFiles: nil,
		})
		return deltaPlan{
			diff:            diff,
			currentSnapshot: captured,
			seedSnapshot:    seed,
			configDigest:    configDigest,
			fallback:        false,
			handled:         true,
		}
	}
	return deltaPlan{
		diff:            diff,
		currentSnapshot: captured,
		seedSnapshot:    seed,
		configDigest:    configDigest,
		fallback:        false,
		handled:         false,
	}
}

// runDeltaSync attempts the incremental sync path and returns true when it
// fully handled the job (success, failure, no-op, or cancellation). It
// returns false to fall back to the full Replace path when there is no
// previous snapshot or the semantic collection is gone.
//
// Two operations route here. "sync" computes the merkle diff against the
// previous snapshot and processes only added and modified files.
// "streaming_reindex" treats every discovered file as modified and feeds
// the list through semantic.Reindex so the Milvus collection stays
// populated row-by-row while the splitter upgrade runs.
func (manager *Manager) runDeltaSync(ctx context.Context, job model.Job) bool {
	ctx, done := spans.Open(ctx, "daemon.runDeltaSync")
	defer done(nil)

	manager.mu.Lock()
	codebase, codebaseFound := manager.codebases[job.CodebaseID]
	manager.mu.Unlock()
	if !codebaseFound {
		return false
	}

	streamingReindex := jobOperation(job.Operation) == jobOperationStreamingReindex
	plan := manager.computeDeltaPlan(ctx, job, codebase.ID, streamingReindex)
	if plan.fallback {
		return false
	}
	if plan.handled {
		return true
	}

	state := deltaState{
		plan:         plan,
		snapshotPath: manager.merklePath(codebase.ID),
		working:      make(map[string]string, len(plan.seedSnapshot.Files)),
		semantic:     manager.semantic != nil && manager.semantic.Available(),
	}
	maps.Copy(state.working, plan.seedSnapshot.Files)

	if outcome := manager.applyDeltaRemovals(ctx, job, state); outcome.fallback {
		return false
	} else if outcome.handled {
		return true
	}

	result, outcome := manager.applyDeltaChanges(ctx, job, state)
	if outcome.fallback {
		return false
	}
	if outcome.handled {
		return true
	}

	if streamingReindex && state.semantic {
		if outcome := manager.pruneAfterStreaming(ctx, job, plan.currentSnapshot); outcome.handled {
			return true
		}
	}

	result.FileHashes = state.working
	fileCount, chunkCount := manager.codebaseTotals(ctx, job.CanonicalPath, state.working, result.TotalChunks)
	result.IndexedFiles = fileCount
	result.TotalChunks = chunkCount
	manager.updateJobCompleted(job.ID, result)
	return true
}

// codebaseTotals reports the file and chunk totals that represent the
// codebase as a whole at the moment a delta sync completes, so the
// registry's LastSuccessfulRun describes current state rather than the
// per-run delta. fileCount is the size of the working merkle set, which
// matches the codebase under the active config digest. chunkCount comes
// from semantic.Service.Count when the backend is available; on
// unavailability or any error it falls back to fallbackChunks, which the
// caller passes as either the loop's running TotalChunks (incremental
// path) or zero (empty-diff fast path).
func (manager *Manager) codebaseTotals(ctx context.Context, canonicalPath string, working map[string]string, fallbackChunks int32) (int32, int32) {
	fileCount := safeInt32(len(working))
	if manager.semantic == nil || !manager.semantic.Available() {
		return fileCount, fallbackChunks
	}
	count, err := manager.semantic.Count(ctx, canonicalPath)
	if err != nil {
		if !errors.Is(err, semantic.ErrUnavailable) {
			slog.WarnContext(ctx, "semantic count failed; using fallback chunk total", "path", canonicalPath, "err", err)
		}
		return fileCount, fallbackChunks
	}
	return fileCount, count
}

func (manager *Manager) computeDeltaPlan(ctx context.Context, job model.Job, codebaseID string, streamingReindex bool) deltaPlan {
	if streamingReindex {
		return manager.planStreamingReindex(ctx, job, codebaseID)
	}
	return manager.planSyncDiff(ctx, job, codebaseID)
}

func (manager *Manager) applyDeltaRemovals(ctx context.Context, job model.Job, state deltaState) deltaOutcome {
	removed := state.plan.diff.Removed
	if len(removed) == 0 || !state.semantic {
		return deltaOutcome{fallback: false, handled: false}
	}
	if err := manager.semantic.Reindex(ctx, job.CanonicalPath, nil, removed, nil); err != nil {
		return manager.classifyReindexErr(ctx, job, err, "delta removal")
	}
	for _, path := range removed {
		delete(state.working, path)
	}
	manager.writeCheckpoint(ctx, state, "removals")
	return deltaOutcome{fallback: false, handled: false}
}

func (manager *Manager) applyDeltaChanges(ctx context.Context, job model.Job, state deltaState) (indexer.Result, deltaOutcome) {
	changed := make([]string, 0, len(state.plan.diff.Added)+len(state.plan.diff.Modified))
	changed = append(changed, state.plan.diff.Added...)
	changed = append(changed, state.plan.diff.Modified...)

	totalChanged := len(changed)
	totalFiles := safeInt32(totalChanged)
	result := indexer.Result{
		IndexedFiles: 0,
		TotalChunks:  0,
		Chunks:       make([]model.StoredChunk, 0),
		FileHashes:   nil,
		SkippedFiles: []string{},
	}
	for index, relativePath := range changed {
		if err := ctx.Err(); err != nil {
			manager.updateJobCancelled(ctx, job.ID)
			return result, deltaOutcome{fallback: false, handled: true}
		}
		if seedHash, present := state.plan.seedSnapshot.Files[relativePath]; present && seedHash == state.plan.currentSnapshot.Files[relativePath] {
			state.working[relativePath] = seedHash
			manager.reportDeltaProgress(job.ID, index, totalChanged, totalFiles, result.TotalChunks)
			continue
		}
		outcome := manager.handleChangedFile(ctx, job, state, relativePath, &result)
		if outcome.fallback || outcome.handled {
			return result, outcome
		}
		manager.writeCheckpoint(ctx, state, relativePath)
		manager.reportDeltaProgress(job.ID, index, totalChanged, totalFiles, result.TotalChunks)
	}
	return result, deltaOutcome{fallback: false, handled: false}
}

func (manager *Manager) handleChangedFile(ctx context.Context, job model.Job, state deltaState, relativePath string, result *indexer.Result) deltaOutcome {
	fileResult, err := manager.runner.IndexOne(ctx, job.CanonicalPath, relativePath, job.Config)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			manager.updateJobCancelled(ctx, job.ID)
		} else {
			manager.updateJobFailed(ctx, job.ID, err)
		}
		return deltaOutcome{fallback: false, handled: true}
	}
	if fileResult.Removed {
		slog.InfoContext(ctx, "converge.remove", "component", "daemon", "subcomponent", "delta", "path", relativePath, "semantic", state.semantic)
		if state.semantic {
			if rmErr := manager.semantic.Reindex(ctx, job.CanonicalPath, nil, []string{relativePath}, nil); rmErr != nil {
				return manager.classifyReindexErr(ctx, job, rmErr, "per-file removal")
			}
		}
		delete(state.working, relativePath)
		return deltaOutcome{fallback: false, handled: false}
	}
	if fileResult.Skipped {
		result.SkippedFiles = append(result.SkippedFiles, relativePath)
		return deltaOutcome{fallback: false, handled: false}
	}
	if state.semantic {
		if err := manager.semantic.Reindex(ctx, job.CanonicalPath, fileResult.Chunks, []string{relativePath}, nil); err != nil {
			return manager.classifyReindexErr(ctx, job, err, "per-file reindex")
		}
	}
	state.working[relativePath] = fileResult.FileHash
	result.Chunks = append(result.Chunks, fileResult.Chunks...)
	result.TotalChunks += safeInt32(len(fileResult.Chunks))
	result.IndexedFiles++
	return deltaOutcome{fallback: false, handled: false}
}

func (manager *Manager) classifyReindexErr(ctx context.Context, job model.Job, err error, phase string) deltaOutcome {
	switch {
	case errors.Is(err, semantic.ErrCollectionMissing):
		slog.WarnContext(ctx, "semantic collection missing; falling back to full reindex", "job_id", job.ID, "phase", phase)
		return deltaOutcome{fallback: true, handled: false}
	case errors.Is(err, context.Canceled):
		manager.updateJobCancelled(ctx, job.ID)
		return deltaOutcome{fallback: false, handled: true}
	default:
		manager.updateJobFailed(ctx, job.ID, err)
		return deltaOutcome{fallback: false, handled: true}
	}
}

func (manager *Manager) writeCheckpoint(ctx context.Context, state deltaState, label string) {
	snapshot := merkle.Snapshot{ConfigDigest: state.plan.configDigest, Files: state.working}
	if err := merkle.WriteSnapshot(state.snapshotPath, snapshot); err != nil {
		slog.ErrorContext(ctx, "checkpoint write failed", "path", state.snapshotPath, "label", label, "err", err)
	}
}

func (manager *Manager) reportDeltaProgress(jobID string, index int, totalChanged int, totalFiles int32, totalChunks int32) {
	manager.updateJobProgress(jobID, indexer.Progress{
		Phase:           "Reindexing changed files...",
		OverallPercent:  10 + (float64(index+1)/float64(maxInt(totalChanged, 1)))*90,
		FilesTotal:      totalFiles,
		FilesProcessed:  safeInt32(index + 1),
		ChunksGenerated: totalChunks,
	})
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
			return deltaOutcome{fallback: false, handled: true}
		case errors.Is(err, semantic.ErrCollectionMissing):
			slog.WarnContext(ctx, "semantic collection missing during streaming prune", "job_id", job.ID)
		default:
			manager.updateJobFailed(ctx, job.ID, err)
			return deltaOutcome{fallback: false, handled: true}
		}
	}
	return deltaOutcome{fallback: false, handled: false}
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
