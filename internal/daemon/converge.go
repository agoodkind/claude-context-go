package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

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

	changed := false
	for _, relativePath := range relativePaths {
		if converged := manager.convergeOnePath(ctx, codebase.CanonicalPath, relativePath, codebase.EffectiveConfig, snapshot.Files); converged {
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

// convergeOnePath converges a single path and reports whether it mutated the
// snapshot's file-hash map. Errors are logged and swallowed so one path does
// not abort the batch.
func (manager *Manager) convergeOnePath(ctx context.Context, root string, relativePath string, cfg model.IndexConfig, fileHashes map[string]string) bool {
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
		if _, present := fileHashes[relativePath]; !present {
			return false
		}
		delete(fileHashes, relativePath)
		slog.InfoContext(ctx, "converge.remove", "component", "daemon", "subcomponent", "converge", "path", relativePath)
		return true
	}
	if fileResult.Skipped {
		return false
	}
	if fileHashes[relativePath] == fileResult.FileHash {
		return false
	}
	if upErr := manager.semantic.Reindex(ctx, root, fileResult.Chunks, []string{relativePath}, nil); upErr != nil {
		manager.logConvergeReindexErr(ctx, relativePath, "upsert", upErr)
		return false
	}
	fileHashes[relativePath] = fileResult.FileHash
	slog.InfoContext(ctx, "converge.upsert", "component", "daemon", "subcomponent", "converge", "path", relativePath, "chunks", len(fileResult.Chunks))
	return true
}

func (manager *Manager) logConvergeReindexErr(ctx context.Context, relativePath string, op string, err error) {
	if errors.Is(err, semantic.ErrCollectionMissing) {
		slog.WarnContext(ctx, "converge.collection_missing", "component", "daemon", "subcomponent", "converge", "path", relativePath, "op", op)
		return
	}
	slog.ErrorContext(ctx, "converge.reindex_failed", "component", "daemon", "subcomponent", "converge", "path", relativePath, "op", op, "err", err)
}
