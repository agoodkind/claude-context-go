package daemon

import (
	"context"
	"fmt"
	"log/slog"

	"goodkind.io/claude-context-go/internal/clock"
	"goodkind.io/claude-context-go/internal/merkle"
	"goodkind.io/claude-context-go/internal/migrate"
	"goodkind.io/claude-context-go/internal/model"
	"goodkind.io/gklog/correlation"
)

// adoptUnregisteredCodebase turns a codebase that has a live Milvus collection
// but no registry entry into a first-class Go citizen: it persists a registry
// record, seeds the merkle baseline from the TS merkle so the first sync
// re-embeds only changed files, starts watching the tree, and enqueues one
// refresh sync. The collection itself is never touched, so the index stays
// portable. Returns the persisted record and true on success; false tells the
// caller to fall back to an ephemeral synthesized record.
//
// Adoption happens on use (the resolve path), so the minted id persists and is
// stable across later calls, unlike the per-call synthesized id it replaces.
func (manager *Manager) adoptUnregisteredCodebase(ctx context.Context, canonicalPath string) (model.Codebase, bool) {
	indexConfig := manager.enrichIndexConfig(model.IndexConfig{
		SplitterType: "", SplitterChunkSize: 0, SplitterOverlap: 0,
		Extensions: nil, IgnorePatterns: nil, IgnoreDigest: "",
		EmbeddingProvider: "", EmbeddingModel: "", EmbeddingDimension: 0,
		VectorBackend: "", Hybrid: false,
	})
	indexConfig.IgnoreDigest = digestIndexConfig(indexConfig)

	collectionName := ""
	if manager.semantic != nil {
		collectionName = manager.semantic.CollectionName(canonicalPath)
	}

	manager.mu.Lock()
	if existing, found := manager.findCodebaseByExactRoot(canonicalPath); found {
		manager.mu.Unlock()
		return existing, true
	}
	record := newCodebaseRecord(canonicalPath)
	record.Status = model.CodebaseStatusIndexed
	record.EffectiveConfig = indexConfig
	record.CollectionName = collectionName
	record.ResolvedIgnoreRules = resolveIgnoreRulesOrLog(ctx, canonicalPath, indexConfig.IgnorePatterns)
	record.InodeTrackingDisabled = detectInodeTrackingDisabled(ctx, canonicalPath)
	record.MerkleSnapshotPath = manager.merklePath(record.ID)
	record.UpdatedAt = clock.Now()
	manager.codebases[record.ID] = record
	if err := manager.saveLocked(); err != nil {
		delete(manager.codebases, record.ID)
		manager.mu.Unlock()
		slog.ErrorContext(ctx, "adopt: persist registry failed", "path", canonicalPath, "err", err)
		var empty model.Codebase
		return empty, false
	}
	manager.mu.Unlock()

	manager.seedAdoptedMerkle(ctx, record)
	manager.notifyCodebaseAdded(ctx, record)
	slog.InfoContext(ctx, "adopted unregistered codebase", "codebase_id", record.ID, "path", canonicalPath, "collection", collectionName)
	manager.enqueueAdoptionSync(ctx, canonicalPath)
	return record, true
}

// seedAdoptedMerkle writes a Go merkle baseline for an adopted codebase from
// the TS merkle, stamped with the codebase's config digest so the delta sync
// accepts it. It is best effort: a missing TS merkle or a write failure leaves
// no baseline, and the first sync then re-embeds every file.
func (manager *Manager) seedAdoptedMerkle(ctx context.Context, codebase model.Codebase) {
	snapshot, found, err := migrate.LoadTSMerkle(ctx, manager.config.ContextRoot, codebase.CanonicalPath)
	if err != nil {
		slog.WarnContext(ctx, "adopt: load TS merkle failed; first sync will re-embed all files", "path", codebase.CanonicalPath, "err", err)
		return
	}
	if !found || len(snapshot.Files) == 0 {
		return
	}
	seeded := merkle.Snapshot{ConfigDigest: codebase.EffectiveConfig.IgnoreDigest, Files: snapshot.Files, Inodes: nil}
	if err := merkle.WriteSnapshot(manager.merklePath(codebase.ID), seeded); err != nil {
		slog.WarnContext(ctx, "adopt: seed merkle write failed; first sync will re-embed all files", "path", codebase.CanonicalPath, "err", err)
		return
	}
	slog.InfoContext(ctx, "adopt: seeded merkle from TS baseline", "codebase_id", codebase.ID, "files", len(snapshot.Files))
}

// enqueueAdoptionSync starts one refresh sync for a freshly adopted codebase in
// a detached goroutine, so the resolve call that triggered adoption returns
// without waiting on the embed. The sync re-embeds only the files that changed
// since the seeded baseline.
func (manager *Manager) enqueueAdoptionSync(ctx context.Context, canonicalPath string) {
	detached := correlation.WithContext(context.WithoutCancel(ctx), correlation.FromContext(ctx).Child())
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.ErrorContext(detached, "adopt: refresh sync panic", "err", fmt.Errorf("panic: %v", recovered), "path", canonicalPath)
			}
		}()
		if _, _, _, err := manager.SyncIndex(detached, canonicalPath, model.ClientInfo{Name: "adopt-sync", PID: 0}); err != nil {
			slog.WarnContext(detached, "adopt: refresh sync enqueue failed", "path", canonicalPath, "err", err)
		}
	}()
}
