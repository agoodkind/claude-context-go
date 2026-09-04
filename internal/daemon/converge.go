package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"

	"goodkind.io/lm-semantic-search/internal/merkle"
	"goodkind.io/lm-semantic-search/internal/metrics"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/semantic"
	"goodkind.io/lm-semantic-search/internal/spans"
)

const (
	convergeProgressPathInterval int32 = 256
	convergeProgressTimeInterval       = time.Second
)

// ConvergeOutcome reports what one ConvergePaths call handled, so a caller can
// state the size of the work rather than guess it. PathsConverged counts the
// paths that changed the index, which is fewer than PathsGiven whenever a path
// was already current, was deleted before the call ran, or the call stopped
// early on cancellation.
//
// It carries no embedded-chunk count. ConvergePaths does not measure embedding,
// and a count that meant "paths seen" would be read as "work embedded" by the
// dependency-health gate in updateJobCompleted.
type ConvergeOutcome struct {
	PathsGiven     int32
	PathsProcessed int32
	PathsConverged int32
}

// ConvergeProgressFunc receives a bounded progress update for one converge.
type ConvergeProgressFunc func(ConvergeOutcome)

type classifiedConvergePath struct {
	RelativePath string
	Missing      bool
}

type convergeLstatFunc func(string) (os.FileInfo, error)

// ConvergePaths makes the index match disk for each relative path in a
// codebase. It reads each path at call time: a path present on disk is
// upserted, a path absent is retained, and a path whose content hash already
// matches the snapshot is skipped. Reading disk per path means a delete that
// lands before the task runs leaves the checkpoint and semantic rows unchanged.
//
// Callers must serialize ConvergePaths against full syncs of the same
// codebase; the background sync coordinator does this through its single
// in-flight guard.
func (manager *Manager) ConvergePaths(ctx context.Context, codebaseID string, relativePaths []string, progress ConvergeProgressFunc) (outcome ConvergeOutcome, err error) {
	return manager.convergePathsWithLstat(ctx, codebaseID, relativePaths, progress, os.Lstat)
}

func (manager *Manager) convergePathsWithLstat(ctx context.Context, codebaseID string, relativePaths []string, progress ConvergeProgressFunc, lstat convergeLstatFunc) (outcome ConvergeOutcome, err error) {
	return manager.convergePathsWithLstatAndNow(ctx, codebaseID, relativePaths, progress, lstat, time.Now)
}

func (manager *Manager) convergePathsWithLstatAndNow(ctx context.Context, codebaseID string, relativePaths []string, progress ConvergeProgressFunc, lstat convergeLstatFunc, now func() time.Time) (outcome ConvergeOutcome, err error) {
	ctx, done := spans.Open(ctx, "daemon.convergePaths")
	defer done(&err)

	manager.mu.Lock()
	codebase, found := manager.codebases[codebaseID]
	manager.mu.Unlock()
	if !found {
		return outcome, nil
	}
	if sourceDirMissing(codebase.CanonicalPath) {
		manager.markCodebaseMissing(ctx, codebaseID)
		slog.WarnContext(ctx, "converge.root_missing_hold", "component", "daemon", "subcomponent", "converge", "codebase_id", codebaseID, "root", codebase.CanonicalPath)
		return outcome, nil
	}
	if manager.semantic == nil || !manager.semantic.Available() {
		return outcome, nil
	}
	if codebase.Status == model.CodebaseStatusQuarantined {
		return outcome, nil
	}

	configDigest := codebase.EffectiveConfig.IgnoreDigest
	snapshotPath := manager.snapshotPathForCodebase(codebase)
	snapshot := manager.loadLiveCheckpoint(ctx, codebase, configDigest).snapshot
	if snapshot.Files == nil {
		snapshot.Files = make(map[string]string)
	}
	snapshot.ConfigDigest = configDigest
	admission := manager.admissionForCodebase(codebase)

	// Sort so present files converge before missing ones. A rename pairs a
	// delete on the source with a create on the destination; if the source
	// is processed first, the snapshot's inode entry for the source is
	// dropped before the destination can look it up, and the CopyChunks
	// fast path is lost. Processing present files first lets the
	// destination match the source's inode while it still lives in the
	// snapshot.
	outcome.PathsGiven = safeInt32(len(relativePaths))
	lastProgressAt := now()
	lastReportedPaths := int32(0)
	reportedProgress := false
	reportProgress := func(final bool, allowHeartbeat bool) bool {
		if ctx.Err() != nil && !final {
			return false
		}
		if progress == nil {
			return true
		}
		pathsSinceReport := outcome.PathsProcessed - lastReportedPaths
		if !final && pathsSinceReport < convergeProgressPathInterval && now().Sub(lastProgressAt) < convergeProgressTimeInterval {
			return true
		}
		if reportedProgress && pathsSinceReport == 0 && !allowHeartbeat {
			return true
		}
		progress(outcome)
		lastProgressAt = now()
		lastReportedPaths = outcome.PathsProcessed
		reportedProgress = true
		return true
	}
	defer reportProgress(true, false)
	classifiedPaths, classifyErr := classifyConvergePathsWithProgress(ctx, codebase.CanonicalPath, relativePaths, lstat, func(int32) error {
		if reportProgress(false, true) {
			return nil
		}
		return ctx.Err()
	})
	if classifyErr != nil {
		return outcome, classifyErr
	}

	changed, convergeErr := manager.convergeClassifiedPaths(
		ctx,
		codebase,
		classifiedPaths,
		&snapshot,
		admission,
		lstat,
		&outcome,
		reportProgress,
	)
	if changed {
		if writeErr := merkle.WriteSnapshot(snapshotPath, snapshot); writeErr != nil {
			slog.ErrorContext(ctx, "converge.snapshot_write_failed", "component", "daemon", "subcomponent", "converge", "path", snapshotPath, "err", writeErr)
			wrappedWriteErr := fmt.Errorf("write converge snapshot %s: %w", snapshotPath, writeErr)
			if convergeErr != nil {
				return outcome, errors.Join(convergeErr, wrappedWriteErr)
			}
			return outcome, wrappedWriteErr
		}
		if checkpointErr := manager.checkpointJob(ctx); checkpointErr != nil {
			return outcome, checkpointErr
		}
	}
	if convergeErr != nil {
		return outcome, convergeErr
	}
	return outcome, nil
}

func (manager *Manager) convergeClassifiedPaths(
	ctx context.Context,
	codebase model.Codebase,
	classifiedPaths []classifiedConvergePath,
	snapshot *merkle.Snapshot,
	admission *admissionState,
	lstat convergeLstatFunc,
	outcome *ConvergeOutcome,
	reportProgress func(bool, bool) bool,
) (bool, error) {
	changed := false
	for _, classifiedPath := range classifiedPaths {
		// A cancel stops the walk here rather than mid-path, so the snapshot
		// written below covers exactly the paths that reached the index. The
		// paths not reached become drift, which the periodic sync repairs.
		select {
		case <-ctx.Done():
			return changed, nil
		default:
		}
		if classifiedPath.Missing {
			outcome.PathsProcessed++
			if !reportProgress(false, false) {
				break
			}
			continue
		}
		converged, err := manager.convergeOnePath(
			ctx,
			codebase,
			classifiedPath.RelativePath,
			snapshot,
			admission,
			lstat,
		)
		if err != nil {
			return changed, err
		}
		if converged {
			changed = true
			outcome.PathsConverged++
		}
		outcome.PathsProcessed++
		if !reportProgress(false, true) {
			break
		}
	}
	return changed, nil
}

// convergeOnePath converges a single path against the snapshot and
// returns true when the snapshot was mutated. Non-absence filesystem and
// indexing errors stop the batch before it reports a successful completion.
//
// The decision routine:
//
//  1. The indexability resolver gates a present path first; a tracked
//     path that has become excluded, out of scope, or oversize is removed.
//  2. A file whose (device, inode) matches a different path already in
//     the snapshot with the same content hash is treated as a rename or
//     hardlink: CopyChunks rewrites the Milvus key, skipping a re-embed.
//  3. A file whose snapshot entry matches its current content hash is a
//     no-op (but the inode sidecar is refreshed when newer).
//  4. Any remaining mismatch is an upsert (Reindex with the new chunks).
//
// InodeTrackingDisabled on the codebase short-circuits steps 3 and the
// inode-stamp branch so unstable-inode filesystems still converge
// correctly using path + content-hash identity.
func (manager *Manager) convergeOnePath(ctx context.Context, codebase model.Codebase, relativePath string, snapshot *merkle.Snapshot, admission *admissionState, lstat convergeLstatFunc) (bool, error) {
	root := codebase.CanonicalPath
	cfg := codebase.EffectiveConfig

	// A path present on disk runs the indexability gate first: a tracked path
	// that has become excluded, out of scope, or oversize is removed. A stat
	// miss can occur when the file disappears after classification; IndexOne
	// reports that removal, and this method retains its semantic rows and
	// checkpoint entry.
	// os.Lstat does not follow symlinks, so a symlink is seen as non-regular and
	// rejected by the resolver's ReasonNotRegular gate rather than indexed as its
	// target.
	if info, statErr := lstat(filepath.Join(root, relativePath)); statErr == nil {
		if decision := manager.indexability.Decide(ctx, codebase.ID, root, relativePath, info); !decision.Indexed {
			return manager.convergeRemoveExcluded(ctx, root, relativePath, string(decision.Reason), snapshot), nil
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return false, fmt.Errorf("lstat converge path %q: %w", relativePath, statErr)
	}
	fileResult, indexErr := manager.runner.IndexOne(ctx, manager.indexability, codebase.ID, root, relativePath, cfg)
	if indexErr != nil {
		return false, fmt.Errorf("index converge path %q: %w", relativePath, indexErr)
	}
	if fileResult.Removed {
		return false, nil
	}
	if fileResult.Skipped {
		return false, nil
	}

	currentInode := stampInodeForPath(ctx, codebase, root, relativePath)
	previousHash, previouslyTracked := snapshot.Files[relativePath]
	if previouslyTracked && previousHash == fileResult.FileHash {
		// Same content; sidecar stamp only.
		if shouldUpdateInodeStamp(snapshot, relativePath, currentInode) {
			snapshot.RecordInode(relativePath, currentInode)
			return true, nil
		}
		return false, nil
	}

	if !previouslyTracked && manager.tryRenameCopy(ctx, root, relativePath, currentInode, fileResult.FileHash, snapshot) {
		return true, nil
	}

	if admissionErr := admission.Admit(fileResult.Chunks); admissionErr != nil {
		slog.WarnContext(ctx, "converge.admission_halt", "component", "daemon", "subcomponent", "converge", "path", relativePath, "err", admissionErr)
		return false, nil
	}
	if upErr := manager.semantic.Reindex(ctx, root, fileResult.Chunks, semantic.RemovePaths([]string{relativePath}), nil, nil, semantic.StoreColumnSetCode); upErr != nil {
		manager.logConvergeReindexErr(ctx, relativePath, "upsert", upErr)
		return false, nil
	}
	snapshot.Files[relativePath] = fileResult.FileHash
	snapshot.RecordInode(relativePath, currentInode)
	metrics.ConvergeUpsert()
	slog.InfoContext(ctx, "converge.upsert", "component", "daemon", "subcomponent", "converge", "path", relativePath, "chunks", len(fileResult.Chunks))
	return true, nil
}

// convergeRemoveExcluded removes a path the indexability resolver declined and
// updates the snapshot to match. It returns true when the snapshot was mutated.
// reason names the gate that declined the path and is logged for diagnosis.
func (manager *Manager) convergeRemoveExcluded(ctx context.Context, root string, relativePath string, reason string, snapshot *merkle.Snapshot) bool {
	if !snapshot.HasFile(relativePath) {
		return false
	}
	if rmErr := manager.semantic.Reindex(ctx, root, nil, semantic.RemovePaths([]string{relativePath}), nil, nil, semantic.StoreColumnSetCode); rmErr != nil {
		manager.logConvergeReindexErr(ctx, relativePath, "remove_excluded", rmErr)
		return false
	}
	delete(snapshot.Files, relativePath)
	snapshot.ForgetInode(relativePath)
	metrics.ConvergeRemove()
	slog.InfoContext(ctx, "converge.remove_excluded", "component", "daemon", "subcomponent", "converge", "path", relativePath, "reason", reason)
	return true
}

// tryRenameCopy attempts to lift the existing chunk rows of a renamed or
// hard-linked file into a new key via CopyChunks. Returns true when the
// copy succeeded and the snapshot was updated; the caller short-circuits
// the embed path in that case. A miss (no inode sibling, no hash match,
// CopyChunks unavailable, or a non-missing-collection error) falls
// through to the normal embed flow.
func (manager *Manager) tryRenameCopy(ctx context.Context, root string, relativePath string, currentInode merkle.InodeRef, freshHash string, snapshot *merkle.Snapshot) bool {
	siblings := snapshot.LookupByInode(currentInode)
	if len(siblings) == 0 {
		return false
	}
	source := pickRenameSource(siblings, snapshot.Files, freshHash)
	if source == "" {
		return false
	}
	copied, copyErr := manager.semantic.CopyChunks(ctx, root, source, relativePath)
	if copyErr != nil {
		if !errors.Is(copyErr, semantic.ErrCollectionMissing) {
			slog.WarnContext(ctx, "converge.copy_chunks_fallback", "component", "daemon", "subcomponent", "converge", "src", source, "dst", relativePath, "err", copyErr)
		}
		return false
	}
	if copied == 0 {
		return false
	}
	snapshot.Files[relativePath] = freshHash
	snapshot.RecordInode(relativePath, currentInode)
	metrics.ConvergeCopyChunks()
	slog.InfoContext(ctx, "converge.copy_chunks", "component", "daemon", "subcomponent", "converge", "src", source, "dst", relativePath, "rows", copied)
	return true
}

// stampInodeForPath returns the current (device, inode) for the converge
// path or a zero value when inode tracking is disabled or the stat fails.
// A zero return forces the caller into path-only behavior, which the
// decision table accepts as a degraded but correct mode.
func stampInodeForPath(ctx context.Context, codebase model.Codebase, root string, relativePath string) merkle.InodeRef {
	if codebase.InodeTrackingDisabled {
		return merkle.InodeRef{Device: "", Inode: 0}
	}
	full := filepath.Join(root, relativePath)
	identity, err := statInode(full)
	if err != nil {
		slog.DebugContext(ctx, "converge.inode_stat_failed", "component", "daemon", "subcomponent", "converge", "path", relativePath, "err", err)
		return merkle.InodeRef{Device: "", Inode: 0}
	}
	return merkle.InodeRef{Device: identity.device, Inode: identity.inode}
}

// shouldUpdateInodeStamp reports whether the snapshot's sidecar entry for
// relativePath is stale relative to the freshly stamped value.
func shouldUpdateInodeStamp(snapshot *merkle.Snapshot, relativePath string, current merkle.InodeRef) bool {
	if current.IsZero() {
		return false
	}
	existing, found := snapshot.Inodes[relativePath]
	if !found {
		return true
	}
	return existing != current
}

// pickRenameSource selects the snapshot path whose content hash matches
// the freshly-computed hash for the renamed file. A match means CopyChunks
// can lift the existing embeddings instead of paying a re-embed.
func pickRenameSource(candidates []string, fileHashes map[string]string, freshHash string) string {
	for _, candidate := range candidates {
		if fileHashes[candidate] == freshHash {
			return candidate
		}
	}
	return ""
}

func classifyConvergePathsWithProgress(ctx context.Context, root string, relativePaths []string, lstat convergeLstatFunc, progress func(int32) error) ([]classifiedConvergePath, error) {
	classifiedPaths := make([]classifiedConvergePath, 0, len(relativePaths))
	for _, relativePath := range relativePaths {
		if err := ctx.Err(); err != nil {
			cause := context.Cause(ctx)
			slog.WarnContext(ctx, "converge.classify_cancelled", "component", "daemon", "subcomponent", "converge", "err", cause)
			return nil, fmt.Errorf("classify converge paths: %w", cause)
		}
		_, err := lstat(filepath.Join(root, relativePath))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			slog.ErrorContext(ctx, "converge.classify_failed", "component", "daemon", "subcomponent", "converge", "path", relativePath, "err", err)
			return nil, fmt.Errorf("classify converge path %q: %w", relativePath, err)
		}
		classifiedPaths = append(classifiedPaths, classifiedConvergePath{
			RelativePath: relativePath,
			Missing:      errors.Is(err, os.ErrNotExist),
		})
		if progress != nil {
			if progressErr := progress(safeInt32(len(classifiedPaths))); progressErr != nil {
				return nil, progressErr
			}
		}
	}
	sort.SliceStable(classifiedPaths, func(first int, second int) bool {
		if classifiedPaths[first].Missing != classifiedPaths[second].Missing {
			return !classifiedPaths[first].Missing
		}
		return classifiedPaths[first].RelativePath < classifiedPaths[second].RelativePath
	})
	return classifiedPaths, nil
}

func fileExists(absolutePath string) bool {
	_, err := os.Lstat(absolutePath)
	return err == nil
}

func (manager *Manager) logConvergeReindexErr(ctx context.Context, relativePath string, op string, err error) {
	if errors.Is(err, semantic.ErrCollectionMissing) {
		slog.WarnContext(ctx, "converge.collection_missing", "component", "daemon", "subcomponent", "converge", "path", relativePath, "op", op)
		return
	}
	slog.ErrorContext(ctx, "converge.reindex_failed", "component", "daemon", "subcomponent", "converge", "path", relativePath, "op", op, "err", err)
}
