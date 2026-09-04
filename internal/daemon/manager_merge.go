package daemon

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"goodkind.io/lm-semantic-search/internal/model"
)

// descendantReuseCandidates returns the settled, indexed child codebases
// strictly inside canonicalPath whose embedding model matches indexConfig, so
// their stored dense vectors are valid to reuse for a parent build under that
// model. A child mid-index or built with a different model is skipped. The
// caller must not hold the manager lock.
func (manager *Manager) descendantReuseCandidates(canonicalPath string, indexConfig model.IndexConfig) []model.Codebase {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	candidates := make([]model.Codebase, 0)
	for _, child := range manager.findDescendants(canonicalPath) {
		if child.CollectionName == "" || child.ActiveJobID != "" {
			continue
		}
		if child.Status != model.CodebaseStatusIndexed {
			continue
		}
		if !reuseModelMatches(child.EffectiveConfig, indexConfig) {
			continue
		}
		// A nested sibling worktree of the same repo is a separate codebase
		// holding a different branch; the parent's discovery already excludes its
		// files, so it must be neither reused nor absorbed.
		if isSameRepoSiblingWorktree(canonicalPath, child.CanonicalPath) {
			continue
		}
		candidates = append(candidates, child)
	}
	return candidates
}

// reuseModelMatches reports whether two index configs embed with the same
// model, provider, and dimension, which is the condition under which a stored
// vector from one is a valid embedding for the other.
func reuseModelMatches(left model.IndexConfig, right model.IndexConfig) bool {
	return left.EmbeddingProvider == right.EmbeddingProvider &&
		left.EmbeddingModel == right.EmbeddingModel &&
		left.EmbeddingDimension == right.EmbeddingDimension
}

// collectionNamesOf collects the non-empty collection names of codebases.
func collectionNamesOf(codebases []model.Codebase) []string {
	names := make([]string, 0, len(codebases))
	for _, codebase := range codebases {
		if codebase.CollectionName != "" {
			names = append(names, codebase.CollectionName)
		}
	}
	return names
}

// absorbDescendants folds child codebases into a freshly built parent index.
// It drops their registry entries and merkle snapshots and removes their
// watchers, but deliberately leaves their Milvus collections in place: the
// shared TS drop-in collection is never dropped. The parent now covers their
// paths, so a later status or search of a child path resolves to the parent. A
// child that gained an active job since it was selected is left untouched.
func (manager *Manager) absorbDescendants(ctx context.Context, descendants []model.Codebase) {
	if len(descendants) == 0 {
		return
	}
	manager.policyMutationMutex.Lock()
	manager.mu.Lock()
	removed := make([]string, 0, len(descendants))
	for _, child := range descendants {
		current, ok := manager.codebases[child.ID]
		if !ok || current.ActiveJobID != "" {
			continue
		}
		delete(manager.codebases, child.ID)
		removed = append(removed, child.ID)
		if rmErr := os.Remove(manager.merklePath(child.ID)); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
			slog.WarnContext(ctx, "remove absorbed child merkle failed", "child_codebase_id", child.ID, "err", rmErr)
		}
	}
	if len(removed) > 0 {
		if err := manager.saveLocked(); err != nil {
			slog.ErrorContext(ctx, "persist registry after absorb failed", "err", err)
		}
	}
	manager.mu.Unlock()
	manager.policyMutationMutex.Unlock()

	for _, id := range removed {
		if err := manager.clearGraphCache(ctx, id); err != nil {
			slog.WarnContext(ctx, "remove absorbed child graph failed", "child_codebase_id", id, "err", err)
		}
		manager.notifyCodebaseRemoved(ctx, id)
	}
	if len(removed) > 0 {
		slog.InfoContext(ctx, "merge.absorbed_children", "component", "daemon", "subcomponent", "merge", "count", len(removed), "child_codebase_ids", removed)
	}
}

// mergeUpTarget returns the nearest indexed (or actively indexing) codebase
// that strictly contains canonicalPath, when canonicalPath is not itself a
// tracked codebase root. The merge-up path uses it to redirect an index request
// for a nested directory into the larger covering index rather than building a
// redundant child. A path that is already its own tracked root is left to the
// normal index decision so a re-index of that exact root is never redirected.
func (manager *Manager) mergeUpTarget(canonicalPath string) (model.Codebase, bool) {
	var empty model.Codebase
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if _, exact := manager.findCodebaseByExactRoot(canonicalPath); exact {
		return empty, false
	}
	ancestor, found := manager.findStrictAncestor(canonicalPath)
	if !found {
		return empty, false
	}
	if ancestor.Status != model.CodebaseStatusIndexed && ancestor.Status != model.CodebaseStatusIndexing {
		return empty, false
	}
	return ancestor, true
}

// redirectIndexToAncestor resolves an index request for a nested path to its
// covering parent and syncs the parent so the requested subtree is current,
// instead of building a second overlapping collection. It returns the parent
// codebase and the sync job so the caller renders a redirect rather than a
// fresh index.
func (manager *Manager) redirectIndexToAncestor(
	ctx context.Context,
	requestedPath string,
	ancestor model.Codebase,
	client model.ClientInfo,
	intent indexPolicyIntent,
) (model.Job, model.Codebase, error) {
	job, codebase, _, err := manager.syncIndexWithPolicy(
		ctx,
		ancestor.CanonicalPath,
		client,
		intent.Patch,
	)
	if err != nil {
		var emptyJob model.Job
		var emptyCodebase model.Codebase
		return emptyJob, emptyCodebase, err
	}
	if intent.Initialize {
		initialized, initializeErr := manager.resolveAndPersistIndexPolicy(
			codebase.ID,
			intent,
		)
		if initializeErr != nil {
			var emptyJob model.Job
			var emptyCodebase model.Codebase
			return emptyJob, emptyCodebase, initializeErr
		}
		codebase = initialized
	}
	slog.InfoContext(ctx, "merge.redirect_to_ancestor", "component", "daemon", "subcomponent", "merge", "requested", requestedPath, "ancestor_codebase_id", ancestor.ID)
	return job, codebase, nil
}

// IndexedDescendants returns the indexed child codebases strictly inside the
// queried path. The status and start-index renderers use it to tell the
// operator that a parent already has indexed sub-folders whose embeddings a
// parent index would reuse, instead of reporting a bare "not indexed".
func (manager *Manager) IndexedDescendants(requestedPath string) []model.Codebase {
	canonicalPath, err := canonicalizePath(requestedPath)
	if err != nil {
		return nil
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	indexed := make([]model.Codebase, 0)
	for _, child := range manager.findDescendants(canonicalPath) {
		if child.Status == model.CodebaseStatusIndexed || child.LastSuccessfulRun != nil {
			indexed = append(indexed, child)
		}
	}
	return indexed
}
