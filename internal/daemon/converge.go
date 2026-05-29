package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"

	"goodkind.io/claude-context-go/internal/discovery"
	"goodkind.io/claude-context-go/internal/merkle"
	"goodkind.io/claude-context-go/internal/model"
	"goodkind.io/claude-context-go/internal/semantic"
	"goodkind.io/claude-context-go/internal/spans"
)

// ConvergePaths makes the index match disk for each relative path in a
// codebase. It reads each path at call time: a path present on disk is
// upserted, a path absent is deleted, and a path whose content hash already
// matches the snapshot is skipped. Reading disk per path means a delete that
// lands before the task runs is handled as a removal rather than an error.
//
// Callers must serialize ConvergePaths against full syncs of the same
// codebase; the background sync coordinator does this through its single
// in-flight guard.
func (manager *Manager) ConvergePaths(ctx context.Context, codebaseID string, relativePaths []string) (err error) {
	ctx, done := spans.Open(ctx, "daemon.convergePaths")
	defer done(&err)

	manager.mu.Lock()
	codebase, found := manager.codebases[codebaseID]
	manager.mu.Unlock()
	if !found {
		return nil
	}
	if manager.semantic == nil || !manager.semantic.Available() {
		return nil
	}

	configDigest := codebase.EffectiveConfig.IgnoreDigest
	snapshotPath := manager.merklePath(codebaseID)
	snapshot := merkle.LoadSnapshotForConfig(snapshotPath, configDigest, manager.legacyDigestForCodebase(codebaseID))
	if snapshot.Files == nil {
		snapshot.Files = make(map[string]string)
	}
	snapshot.ConfigDigest = configDigest

	// Sort so present files converge before missing ones. A rename pairs a
	// delete on the source with a create on the destination; if the source
	// is processed first, the snapshot's inode entry for the source is
	// dropped before the destination can look it up, and the CopyChunks
	// fast path is lost. Processing present files first lets the
	// destination match the source's inode while it still lives in the
	// snapshot.
	orderedPaths := orderPathsByPresence(codebase.CanonicalPath, relativePaths)

	changed := false
	for _, relativePath := range orderedPaths {
		if converged := manager.convergeOnePath(ctx, codebase, relativePath, &snapshot); converged {
			changed = true
		}
	}

	if !changed {
		return nil
	}
	if writeErr := merkle.WriteSnapshot(snapshotPath, snapshot); writeErr != nil {
		slog.ErrorContext(ctx, "converge.snapshot_write_failed", "component", "daemon", "subcomponent", "converge", "path", snapshotPath, "err", writeErr)
		return fmt.Errorf("write converge snapshot %s: %w", snapshotPath, writeErr)
	}
	return nil
}

// convergeOnePath converges a single path against the snapshot and
// returns true when the snapshot was mutated. Errors are logged and
// swallowed so one bad path does not abort the batch.
//
// The decision routine:
//
//  1. PathIgnored rules trump everything; a tracked path that has
//     become excluded is removed.
//  2. A missing file is removed.
//  3. A file whose (device, inode) matches a different path already in
//     the snapshot with the same content hash is treated as a rename or
//     hardlink: CopyChunks rewrites the Milvus key, skipping a re-embed.
//  4. A file whose snapshot entry matches its current content hash is a
//     no-op (but the inode sidecar is refreshed when newer).
//  5. Any remaining mismatch is an upsert (Reindex with the new chunks).
//
// InodeTrackingDisabled on the codebase short-circuits steps 3 and the
// inode-stamp branch so unstable-inode filesystems still converge
// correctly using path + content-hash identity.
func (manager *Manager) convergeOnePath(ctx context.Context, codebase model.Codebase, relativePath string, snapshot *merkle.Snapshot) bool {
	root := codebase.CanonicalPath
	cfg := codebase.EffectiveConfig
	rules := codebase.ResolvedIgnoreRules

	if excluded, matchedPattern, gitignoreSource := discovery.PathIgnored(relativePath, rules); excluded {
		if _, tracked := snapshot.Files[relativePath]; !tracked {
			return false
		}
		if rmErr := manager.semantic.Reindex(ctx, root, nil, []string{relativePath}, nil); rmErr != nil {
			manager.logConvergeReindexErr(ctx, relativePath, "remove_excluded", rmErr)
			return false
		}
		delete(snapshot.Files, relativePath)
		snapshot.ForgetInode(relativePath)
		slog.InfoContext(ctx, "converge.remove_excluded", "component", "daemon", "subcomponent", "converge", "path", relativePath, "matched_pattern", matchedPattern, "gitignore", gitignoreSource)
		return true
	}
	fileResult, indexErr := manager.runner.IndexOne(ctx, root, relativePath, cfg)
	if indexErr != nil {
		slog.ErrorContext(ctx, "converge.index_failed", "component", "daemon", "subcomponent", "converge", "path", relativePath, "err", indexErr)
		return false
	}
	if fileResult.Removed {
		if rmErr := manager.semantic.Reindex(ctx, root, nil, []string{relativePath}, nil); rmErr != nil {
			manager.logConvergeReindexErr(ctx, relativePath, "remove", rmErr)
			return false
		}
		if _, present := snapshot.Files[relativePath]; !present {
			return false
		}
		delete(snapshot.Files, relativePath)
		snapshot.ForgetInode(relativePath)
		slog.InfoContext(ctx, "converge.remove", "component", "daemon", "subcomponent", "converge", "path", relativePath)
		return true
	}
	if fileResult.Skipped {
		return false
	}

	currentInode := stampInodeForPath(ctx, codebase, root, relativePath)
	previousHash, previouslyTracked := snapshot.Files[relativePath]
	if previouslyTracked && previousHash == fileResult.FileHash {
		// Same content; sidecar stamp only.
		if shouldUpdateInodeStamp(snapshot, relativePath, currentInode) {
			snapshot.RecordInode(relativePath, currentInode)
			return true
		}
		return false
	}

	if !previouslyTracked && manager.tryRenameCopy(ctx, root, relativePath, currentInode, fileResult.FileHash, snapshot) {
		return true
	}

	if upErr := manager.semantic.Reindex(ctx, root, fileResult.Chunks, []string{relativePath}, nil); upErr != nil {
		manager.logConvergeReindexErr(ctx, relativePath, "upsert", upErr)
		return false
	}
	snapshot.Files[relativePath] = fileResult.FileHash
	snapshot.RecordInode(relativePath, currentInode)
	slog.InfoContext(ctx, "converge.upsert", "component", "daemon", "subcomponent", "converge", "path", relativePath, "chunks", len(fileResult.Chunks))
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

// orderPathsByPresence sorts relativePaths so files that currently exist
// on disk come before files that have been removed. A stable secondary
// alphabetical order keeps the iteration deterministic when several
// files share the same presence bucket.
func orderPathsByPresence(root string, relativePaths []string) []string {
	ordered := append([]string{}, relativePaths...)
	sort.SliceStable(ordered, func(first int, second int) bool {
		firstExists := fileExists(filepath.Join(root, ordered[first]))
		secondExists := fileExists(filepath.Join(root, ordered[second]))
		if firstExists != secondExists {
			return firstExists
		}
		return ordered[first] < ordered[second]
	})
	return ordered
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
