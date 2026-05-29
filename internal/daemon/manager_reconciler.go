package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"goodkind.io/claude-context-go/internal/clock"
	"goodkind.io/claude-context-go/internal/semantic"
)

// orphanFirstSeenMS records the first timestamp at which a daemon-owned
// Milvus collection was observed without a matching registry entry. The
// reconciler uses this so GC honors a grace period before dropping a
// collection: an in-flight rename, a crashed deploy, or a manual edit of
// the registry has time to be repaired before the collection is destroyed.
type orphanFirstSeenMS struct {
	mu    sync.Mutex
	table map[string]int64
}

func newOrphanFirstSeenTable() *orphanFirstSeenMS {
	return &orphanFirstSeenMS{
		mu:    sync.Mutex{},
		table: map[string]int64{},
	}
}

// recordOrUpdate records the first observation of collectionName at the
// supplied epoch-millis timestamp and returns the original record. A
// subsequent observation returns the original first-seen timestamp.
func (table *orphanFirstSeenMS) recordOrUpdate(collectionName string, nowMS int64) int64 {
	table.mu.Lock()
	defer table.mu.Unlock()
	existing, found := table.table[collectionName]
	if !found {
		table.table[collectionName] = nowMS
		return nowMS
	}
	return existing
}

// drop forgets the orphan record so a re-registered collection does not
// retain its old first-seen timestamp.
func (table *orphanFirstSeenMS) drop(collectionName string) {
	table.mu.Lock()
	defer table.mu.Unlock()
	delete(table.table, collectionName)
}

// StartReconcilerLoop launches the periodic reverse-pass reconciler. The
// forward pass still runs on every GetIndex and ListIndexes; this loop
// owns the slower reverse pass plus the orphan-collection GC.
func (manager *Manager) StartReconcilerLoop(ctx context.Context) {
	intervalMS := manager.config.OrphanReconcilerIntervalMS
	if intervalMS <= 0 {
		intervalMS = 3_600_000
	}
	interval := time.Duration(intervalMS) * time.Millisecond
	orphanTable := newOrphanFirstSeenTable()
	slog.InfoContext(ctx, "reverse reconciler started", "interval_ms", intervalMS, "orphan_gc_enabled", manager.config.MilvusOrphanGCEnabled, "orphan_grace_ms", manager.config.MilvusOrphanGraceMS)

	goSafe(ctx, "reconciler", func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				manager.runReverseReconcile(ctx, orphanTable)
			}
		}
	})
}

// runReverseReconcile lists Milvus collections that match the daemon's
// prefixes, identifies orphans (no matching registry entry), and drops
// them after the configured grace period.
func (manager *Manager) runReverseReconcile(ctx context.Context, orphanTable *orphanFirstSeenMS) {
	if manager.semantic == nil || !manager.semantic.Available() {
		slog.DebugContext(ctx, "reverse reconcile skipped: semantic unavailable")
		return
	}
	collections, err := manager.semantic.ListCollections(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "reverse reconcile list collections failed", "err", err)
		return
	}
	known := map[string]struct{}{}
	manager.mu.Lock()
	for _, codebase := range manager.codebases {
		if codebase.CollectionName != "" {
			known[codebase.CollectionName] = struct{}{}
		}
		// Also fold in legacy collection names so a rename in flight
		// during the previous deploy does not look like an orphan.
		for _, legacy := range codebase.LegacyCollectionNames {
			known[legacy] = struct{}{}
		}
	}
	gcEnabled := manager.config.MilvusOrphanGCEnabled
	graceMS := int64(manager.config.MilvusOrphanGraceMS)
	manager.mu.Unlock()

	nowMS := clock.Now().UnixMilli()
	orphans := make([]string, 0)
	for _, collectionName := range collections {
		if !semantic.IsDaemonOwnedCollection(collectionName) {
			continue
		}
		if _, found := known[collectionName]; found {
			orphanTable.drop(collectionName)
			continue
		}
		firstSeen := orphanTable.recordOrUpdate(collectionName, nowMS)
		if !gcEnabled {
			continue
		}
		if nowMS-firstSeen < graceMS {
			continue
		}
		orphans = append(orphans, collectionName)
	}
	daemonOwned := 0
	for _, collectionName := range collections {
		if semantic.IsDaemonOwnedCollection(collectionName) {
			daemonOwned++
		}
	}
	slog.InfoContext(ctx, "reverse reconcile pass", "collections_total", len(collections), "daemon_owned", daemonOwned, "registered", len(known), "candidates_for_gc", len(orphans))
	if len(orphans) == 0 {
		return
	}
	sort.Strings(orphans)
	dropped := make([]string, 0, len(orphans))
	failed := make([]string, 0)
	for _, orphan := range orphans {
		if err := manager.semantic.DropCollection(ctx, orphan); err != nil {
			slog.ErrorContext(ctx, "drop orphan collection failed", "collection", orphan, "err", err)
			failed = append(failed, orphan)
			continue
		}
		orphanTable.drop(orphan)
		dropped = append(dropped, orphan)
	}
	slog.InfoContext(ctx, "reverse reconcile dropped orphans", "dropped_count", len(dropped), "failed_count", len(failed), "dropped", dropped, "failed", failed)
}

// goSafe launches fn in a goroutine that recovers from panics so a
// reconciler crash does not bring the daemon down. The recovered panic is
// logged with the supplied label so it can be traced back to the
// background task that died.
func goSafe(ctx context.Context, label string, fn func()) {
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.ErrorContext(ctx, "background goroutine panic", "label", label, "panic", recovered, "err", fmt.Errorf("recovered panic in %s", label))
			}
		}()
		fn()
	}()
}
