package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"goodkind.io/lm-semantic-search/internal/model"
)

// GetIndex resolves one tracked codebase whose canonical path covers
// requestedPath. The classification return describes how the daemon sees
// the queried path (covered, excluded, out-of-scope) for fine-grained
// callers; tracked callers can ignore it. GetIndex returns Indexed for any
// path whose Milvus collection exists, synthesizing a record when the
// registry has no entry for it.
func (manager *Manager) GetIndex(ctx context.Context, requestedPath string) (model.Codebase, *model.Job, bool, *model.PathClassification, error) {
	canonicalPath, err := manager.resolveCanonicalPath(requestedPath)
	if err != nil {
		slog.ErrorContext(ctx, "canonicalize path failed", "path", requestedPath, "err", err)
		return model.Codebase{}, nil, false, nil, fmt.Errorf("canonicalize path %s: %w", requestedPath, err)
	}

	// Worktree-bounded resolution: a git worktree is its own codebase. When the
	// queried path lives in a worktree of an indexed repo that is not yet tracked,
	// this registers it as a discovered codebase and defers its reuse-seeded build,
	// rather than serving a covering parent that holds a different branch's
	// content. The read itself starts no embed job; the discovered codebase has no
	// active job until the deferred build begins.
	if codebase, ok := manager.resolveWorktreeIndex(ctx, canonicalPath); ok {
		classification := manager.classifyTrackedPath(ctx, codebase, canonicalPath)
		return codebase, nil, true, classification, nil
	}

	manager.mu.Lock()
	matches := manager.findCodebasesByCoverage(canonicalPath)
	if len(matches) > 0 {
		codebase := matches[0]
		activeJob := manager.activeJobSnapshotLocked(codebase)
		manager.mu.Unlock()
		if activeJob != nil && manager.jobScheduler != nil {
			resolvedJob := jobWithSchedulerReason(
				*activeJob,
				manager.jobScheduler.Snapshot().Reasons,
			)
			activeJob = &resolvedJob
		}
		classification := manager.classifyTrackedPath(ctx, codebase, canonicalPath)
		return codebase, activeJob, true, classification, nil
	}
	manager.mu.Unlock()

	if manager.semantic != nil && manager.semantic.Available() {
		hasCollection, hasErr := manager.semantic.HasCollectionForPath(ctx, canonicalPath)
		if hasErr == nil && hasCollection {
			// A live collection with no registry entry is a codebase the TS
			// tool indexed. Adopt it as first-class so it gains a stable id, a
			// watcher, and background sync; fall back to an ephemeral record
			// only when the registry write fails.
			codebase, adopted := manager.adoptUnregisteredCodebase(ctx, canonicalPath)
			if !adopted {
				codebase = manager.synthesizeUnregisteredCodebase(canonicalPath)
			}
			classification := &model.PathClassification{
				Kind:                model.PathClassificationInScopeIndexed,
				ExcludedByPattern:   "",
				ExcludedByGitignore: "",
				CoveringCodebaseID:  codebase.ID,
			}
			return codebase, nil, true, classification, nil
		}
		if hasErr != nil {
			slog.WarnContext(ctx, "Milvus HasCollection failed during GetIndex", "path", canonicalPath, "err", hasErr)
		}
	}

	var emptyCodebase model.Codebase
	classification := &model.PathClassification{
		Kind:                model.PathClassificationOutOfScope,
		ExcludedByPattern:   "",
		ExcludedByGitignore: "",
		CoveringCodebaseID:  "",
	}
	return emptyCodebase, nil, false, classification, nil
}

// classifyTrackedPath maps a covered canonical path into a classification.
// When the path equals the codebase root, it is treated as in-scope and
// indexed. The indexability resolver then decides between excluded and
// unindexed for any subpath; an excluded subpath is reported with the
// matched pattern and the gitignore source so callers can name the rule
// that masked the file.
func (manager *Manager) classifyTrackedPath(ctx context.Context, codebase model.Codebase, canonicalPath string) *model.PathClassification {
	// A codebase that has not built its first index yet (pending, discovered, or
	// not-indexed) has no searchable collection, so even its root classifies as
	// in-scope unindexed. That keeps GetIndex from probing an absent collection and
	// reading the result as a store outage; the status reads pending or discovered.
	if codebase.Status == model.CodebaseStatusPending ||
		codebase.Status == model.CodebaseStatusDiscovered ||
		codebase.Status == model.CodebaseStatusNotIndexed {
		return &model.PathClassification{
			Kind:                model.PathClassificationInScopeUnindexed,
			ExcludedByPattern:   "",
			ExcludedByGitignore: "",
			CoveringCodebaseID:  codebase.ID,
		}
	}
	classification := &model.PathClassification{
		Kind:                model.PathClassificationInScopeIndexed,
		ExcludedByPattern:   "",
		ExcludedByGitignore: "",
		CoveringCodebaseID:  codebase.ID,
	}
	if canonicalPath == codebase.CanonicalPath {
		return classification
	}
	relative, err := filepath.Rel(codebase.CanonicalPath, canonicalPath)
	if err != nil || relative == "" || relative == "." {
		return classification
	}
	isDir := false
	if info, statErr := os.Stat(canonicalPath); statErr == nil {
		isDir = info.IsDir()
	}
	if detail, excluded := manager.indexability.IgnoreDetail(ctx, codebase.ID, codebase.CanonicalPath, filepath.ToSlash(relative), isDir); excluded {
		classification.Kind = model.PathClassificationInScopeExcluded
		classification.ExcludedByPattern = detail.Pattern
		classification.ExcludedByGitignore = detail.Source
		return classification
	}
	// The merkle files map mirrors the file set that has chunks in the
	// collection, so it is the authoritative per-file membership source. A path
	// that is in scope and not excluded is only actually searchable when it, or
	// for a directory a file beneath it, is recorded there. Without this check
	// every non-root path reports unindexed even when its file is in the index.
	if manager.pathHasIndexedFiles(ctx, codebase, filepath.ToSlash(relative)) {
		return classification
	}
	classification.Kind = model.PathClassificationInScopeUnindexed
	return classification
}

// pathHasIndexedFiles reports whether the codebase's merkle snapshot covers
// relative (a repo-relative, slash-separated path): the file itself, or for a
// directory any indexed file beneath it. Membership means the path is
// searchable through the index. The per-file decision lives on the snapshot
// ([merkle.Snapshot.CoversPath]) so every caller shares one source of truth.
func (manager *Manager) pathHasIndexedFiles(ctx context.Context, codebase model.Codebase, relative string) bool {
	// A codebase mid-first-build reaches here, because classifyTrackedPath
	// returns early only for pending, discovered, and not-indexed, and it is the
	// case that made this probe log an error during every first build. The read
	// goes through the one live-checkpoint reader so that stays quiet while a
	// checkpoint lost after a real run is still reported.
	checkpoint := manager.loadLiveCheckpoint(ctx, codebase, codebase.EffectiveConfig.IgnoreDigest)
	if !checkpoint.usable() {
		return false
	}
	return checkpoint.snapshot.CoversPath(relative)
}

// snapshotPathForCodebase returns the on-disk merkle snapshot path for a
// codebase, preferring the path stored on the record and falling back to the
// id-derived location. Centralizing this avoids the resolution being repeated
// at each merkle read site.
func (manager *Manager) snapshotPathForCodebase(codebase model.Codebase) string {
	if path := strings.TrimSpace(codebase.MerkleSnapshotPath); path != "" {
		return path
	}
	return manager.merklePath(codebase.ID)
}

// synthesizeUnregisteredCodebase builds an in-memory codebase record for a
// path whose Milvus collection exists without a registry entry.
func (manager *Manager) synthesizeUnregisteredCodebase(canonicalPath string) model.Codebase {
	collectionName := ""
	if manager.semantic != nil {
		collectionName = manager.semantic.CollectionName(canonicalPath)
	}
	codebase := newCodebaseRecord(canonicalPath)
	codebase.Status = model.CodebaseStatusIndexed
	codebase.CollectionName = collectionName
	codebase.EffectiveConfig.Hybrid = manager.config.HybridMode
	return codebase
}

// fillLiveChunkTotal sets an in-flight job's live whole-collection chunk count
// on its progress snapshot, so status shows the running total rather than only
// this run's additions. It covers both incremental syncs (writing the live
// collection directly) and a forced reindex over an existing index (the prior
// collection is still live until the staging build promotes). It is best-effort:
// on any failure, including a from-scratch build whose collection does not exist
// yet, the field stays zero and the renderer falls back to the reuse+embed total
// or the last recorded total. The activeJob must be a snapshot the caller owns,
// since this mutates it.
func (manager *Manager) fillLiveChunkTotal(ctx context.Context, codebase model.Codebase, activeJob *model.Job) {
	if activeJob == nil {
		return
	}
	if manager.semantic == nil || !manager.semantic.Available() {
		return
	}
	// The gate is the same rule the checkpoint read uses, on the collection's
	// axis: count rows only where a collection exists to count. Keying on the run
	// record alone let a run that indexed zero files through, and so did any
	// later job whose operation is not an index, and counting rows in a
	// collection that was never created logs a row-count failure for every status
	// read until the job ends. It reads the whole codebase because an adopted one
	// owns a collection while carrying no run record.
	if !ownsLiveCollection(codebase) {
		slog.DebugContext(
			ctx,
			"no live collection to count yet",
			"codebase_id", codebase.ID,
			"path", codebase.CanonicalPath,
			"job_id", activeJob.ID,
		)
		return
	}
	count, err := manager.semantic.Count(ctx, codebase.CanonicalPath)
	if err != nil {
		return
	}
	activeJob.Progress.ChunksTotal = count
}
