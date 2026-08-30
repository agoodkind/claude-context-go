package daemon

import (
	"context"
	"log/slog"
	"time"

	"goodkind.io/gklog/correlation"
	"goodkind.io/lm-semantic-search/internal/clock"
	"goodkind.io/lm-semantic-search/internal/gitworktree"
	"goodkind.io/lm-semantic-search/internal/model"
)

// worktreeDeferredBuildClient labels the reuse-seeded index jobs the daemon
// starts for a worktree it discovered on a read, so the job's origin is legible
// in status and logs and is distinct from an operator-driven index_codebase.
var worktreeDeferredBuildClient = model.ClientInfo{Name: "worktree-deferred-build", PID: 0}

// defaultDeferredBuildDelay is how long after discovering a worktree on a read
// the daemon waits before starting its build. It is short enough to be far
// faster than the periodic sweep, yet keeps the build off the read path so the
// status or search call that discovered the worktree returns without embedding.
const defaultDeferredBuildDelay = 3 * time.Second

// resolveWorktreeIndex implements worktree-bounded resolution as a read that
// discovers but never embeds. When canonicalPath lives inside a worktree whose
// root is not yet tracked, and at least one sibling worktree of the same
// repository is already indexed, the daemon registers the worktree as a
// discovered codebase, starts watching it, and schedules a reuse-seeded build in
// the background; the read itself launches no embed job. The returned bool is
// false when canonicalPath is not such a worktree, leaving the caller's normal
// coverage resolution untouched.
func (manager *Manager) resolveWorktreeIndex(ctx context.Context, canonicalPath string) (model.Codebase, bool) {
	var empty model.Codebase
	info, isWorktree := gitworktree.Resolve(canonicalPath)
	if !isWorktree {
		return empty, false
	}

	manager.mu.Lock()
	if _, exists := manager.findCodebaseByExactRoot(info.WorktreeRoot); exists {
		// The worktree already has its own codebase; longest-prefix coverage
		// resolves to it without intervention.
		manager.mu.Unlock()
		return empty, false
	}
	hasIndexedSibling := manager.hasIndexedSiblingWorktreeLocked(info.WorktreeRoot, info.CommonDir)
	manager.mu.Unlock()
	if !hasIndexedSibling {
		return empty, false
	}

	record, ok := manager.discoverWorktree(ctx, info)
	if !ok {
		return empty, false
	}
	manager.scheduleDeferredBuild(ctx, record.CanonicalPath)
	return record, true
}

// discoverWorktree persists a first-class registry record for a worktree the
// daemon learned about on a read, in the discovered state, and starts watching
// it. It mirrors adoptUnregisteredCodebase but creates no job and starts no
// embed: the reuse-seeded build is deferred to scheduleDeferredBuild. It returns
// the persisted record and true, or false when the registry write fails so the
// caller falls back to normal resolution.
func (manager *Manager) discoverWorktree(ctx context.Context, info gitworktree.Info) (model.Codebase, bool) {
	indexConfig := manager.enrichIndexConfig(emptyAutoIndexConfig())
	indexConfig.IgnoreDigest = digestIndexConfig(indexConfig)

	collectionName := ""
	collectionAbsent := true
	if manager.semantic != nil {
		collectionName = manager.semantic.CollectionName(info.WorktreeRoot)
		if manager.semantic.Available() {
			hasCollection, collectionErr := manager.semantic.HasCollectionForPath(ctx, info.WorktreeRoot)
			if collectionErr != nil {
				slog.WarnContext(ctx, "discover worktree: check collection failed", "path", info.WorktreeRoot, "err", collectionErr)
			} else {
				collectionAbsent = !hasCollection
			}
		}
	}

	manager.mu.Lock()
	if existing, found := manager.findCodebaseByExactRoot(info.WorktreeRoot); found {
		manager.mu.Unlock()
		return existing, true
	}
	record := newCodebaseRecord(info.WorktreeRoot)
	record.Status = model.CodebaseStatusDiscovered
	record.PolicyPendingInitialization = collectionAbsent
	record.EffectiveConfig = indexConfig
	record.CollectionName = collectionName
	record.WorktreeCommonDir = info.CommonDir
	record.InodeTrackingDisabled = detectInodeTrackingDisabled(ctx, info.WorktreeRoot)
	record.MerkleSnapshotPath = manager.merklePath(record.ID)
	record.UpdatedAt = clock.Now()
	manager.codebases[record.ID] = record
	if err := manager.saveLocked(); err != nil {
		delete(manager.codebases, record.ID)
		manager.mu.Unlock()
		slog.ErrorContext(ctx, "discover worktree: persist registry failed", "path", info.WorktreeRoot, "err", err)
		var empty model.Codebase
		return empty, false
	}
	// A discovered worktree persists a fresh EffectiveConfig, so signal the
	// observer to invalidate rather than relying on the id being new; the next
	// decision rebuilds from the registry source of truth.
	manager.observer.Invalidate(record.ID)
	manager.mu.Unlock()

	notifyCtx := correlation.WithContext(context.WithoutCancel(ctx), correlation.FromContext(ctx).Child())
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.ErrorContext(notifyCtx, "notify codebase added panic", "codebase_id", record.ID, "err", recovered)
			}
		}()
		manager.notifyCodebaseAdded(notifyCtx, record)
	}()
	slog.InfoContext(ctx, "discovered worktree on read; build deferred", "codebase_id", record.ID, "path", info.WorktreeRoot, "common_dir", info.CommonDir)
	return record, true
}

// scheduleDeferredBuild starts the reuse-seeded build for a discovered worktree
// after a short delay, off the read path, in a detached timer. The delay makes
// the build far faster than the periodic sweep while keeping the read that
// discovered the worktree free of any embed. The build deduplicates against any
// job already in flight, so a repeat read or a watcher event cannot double-start.
func (manager *Manager) scheduleDeferredBuild(ctx context.Context, canonicalPath string) {
	detached := correlation.WithContext(context.WithoutCancel(ctx), correlation.FromContext(ctx).Child())
	delay := manager.deferredBuildDelay
	if delay <= 0 {
		delay = defaultDeferredBuildDelay
	}
	time.AfterFunc(delay, func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.ErrorContext(detached, "deferred worktree build panic", "path", canonicalPath, "err", recovered)
			}
		}()
		manager.startDeferredBuild(detached, canonicalPath)
	})
}

// startDeferredBuild starts the reuse-seeded bootstrap for a discovered worktree.
// It is the body scheduleDeferredBuild fires on its timer, split out so a test
// can drive it synchronously. Shared index admission deduplicates, so calling
// it for a worktree that already has an in-flight job is a no-op.
func (manager *Manager) startDeferredBuild(ctx context.Context, canonicalPath string) {
	if _, _, _, _, err := manager.startIndexWithIntent(ctx, canonicalPath, worktreeDeferredBuildClient, emptyAutoIndexConfig(), false, emptyAdmissionBudget, indexPolicyIntent{
		Patch:      model.SchedulingPolicyPatch{Priority: nil, Quiet: nil, IdleAfterSeconds: nil},
		Initialize: false,
	}); err != nil {
		slog.WarnContext(ctx, "deferred worktree build failed to start", "path", canonicalPath, "err", err)
	}
}

// worktreeReuseForecast reports how many indexed sibling worktree collections a
// worktree at codebase.CanonicalPath would reuse on its build, for the discovered
// status and the list. It is cheap: git topology plus an in-memory registry scan,
// no vector-store call, so a status read that shows the forecast stays cheap.
func (manager *Manager) worktreeReuseForecast(codebase model.Codebase) int32 {
	return safeInt32(len(manager.worktreeSiblingReuseCollections(codebase.CanonicalPath, codebase.EffectiveConfig)))
}

// hasIndexedSiblingWorktreeLocked reports whether any worktree of the same repo
// group (other than worktreeRoot) is already a tracked, indexed codebase, which
// is the condition that turns a worktree into "a worktree of an indexed repo"
// for the auto-create trigger. Caller must hold manager.mu.
func (manager *Manager) hasIndexedSiblingWorktreeLocked(worktreeRoot string, commonDir string) bool {
	if commonDir == "" {
		return false
	}
	siblingRoots := gitworktree.SiblingWorktreeRoots(commonDir)
	siblings := make(map[string]struct{}, len(siblingRoots))
	for _, root := range siblingRoots {
		if root != worktreeRoot {
			siblings[root] = struct{}{}
		}
	}
	if len(siblings) == 0 {
		return false
	}
	for _, codebase := range manager.codebases {
		if _, ok := siblings[codebase.CanonicalPath]; !ok {
			continue
		}
		if codebase.Status == model.CodebaseStatusIndexed || codebase.LastSuccessfulRun != nil {
			return true
		}
	}
	return false
}

// worktreeSiblingReuseCollections returns the collection names of indexed
// sibling worktrees of the same repo group as canonicalPath whose embedding
// model matches indexConfig. A worktree build preloads reuse vectors from these
// so files unchanged from a sibling reuse their embeddings and only the branch
// diff is embedded. It returns nil when canonicalPath is not a worktree or has
// no eligible sibling.
func (manager *Manager) worktreeSiblingReuseCollections(canonicalPath string, indexConfig model.IndexConfig) []string {
	info, ok := gitworktree.Resolve(canonicalPath)
	if !ok {
		return nil
	}
	siblingRoots := gitworktree.SiblingWorktreeRoots(info.CommonDir)
	siblings := make(map[string]struct{}, len(siblingRoots))
	for _, root := range siblingRoots {
		if root != info.WorktreeRoot {
			siblings[root] = struct{}{}
		}
	}
	if len(siblings) == 0 {
		return nil
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()
	collections := make([]string, 0)
	for _, codebase := range manager.codebases {
		if _, member := siblings[codebase.CanonicalPath]; !member {
			continue
		}
		if codebase.Kind == model.CodebaseKindDocument {
			continue
		}
		if codebase.CollectionName == "" {
			continue
		}
		// Reuse keys on durable facts, not the transient ActiveJobID: a sibling
		// that is currently indexed or has at least one past successful run has a
		// usable collection. An in-flight sync does not drop the live collection,
		// and reuse is content-hash keyed, so reading a mid-sync sibling is safe.
		// This mirrors the auto-create trigger's eligibility so the two agree.
		if codebase.Status != model.CodebaseStatusIndexed && codebase.LastSuccessfulRun == nil {
			continue
		}
		if !reuseModelMatches(codebase.EffectiveConfig, indexConfig) {
			continue
		}
		collections = append(collections, codebase.CollectionName)
	}
	return collections
}

// isWorktreeBoundary reports whether canonicalPath is the root of a git worktree
// whose repo group matches the covering ancestor, which makes the two distinct
// worktrees of the same repository that must stay separate codebases rather than
// merge. The merge-up redirect consults it so a worktree root is never folded
// into a sibling worktree's index.
func (manager *Manager) isWorktreeBoundary(canonicalPath string, ancestor model.Codebase) bool {
	info, ok := gitworktree.Resolve(canonicalPath)
	if !ok || info.WorktreeRoot != canonicalPath {
		return false
	}
	ancestorCommon, ok := gitworktree.CommonDirAt(ancestor.CanonicalPath)
	if !ok {
		return false
	}
	return ancestorCommon == info.CommonDir
}

// isSameRepoSiblingWorktree reports whether child is a worktree of the same repo
// group as parentPath but rooted at a different worktree. Such a child is a
// sibling worktree, not a nested part of the parent, so the parent's build must
// not reuse its vectors or absorb its registration.
func isSameRepoSiblingWorktree(parentPath string, childPath string) bool {
	parentCommon, ok := gitworktree.CommonDirAt(parentPath)
	if !ok {
		return false
	}
	childCommon, ok := gitworktree.CommonDirAt(childPath)
	if !ok {
		return false
	}
	return parentCommon == childCommon && parentPath != childPath
}

// emptyAutoIndexConfig is the zero index config handed to an auto-created
// worktree build; enrichIndexConfig fills in the daemon defaults.
func emptyAutoIndexConfig() model.IndexConfig {
	return model.IndexConfig{
		SplitterType: "", SplitterChunkSize: 0, SplitterOverlap: 0,
		IgnorePatterns: nil, IncludeSubmodules: nil, IgnoreDigest: "",
		EmbeddingProvider: "", EmbeddingModel: "", EmbeddingDimension: 0,
		VectorBackend: "", Hybrid: false,
	}
}
