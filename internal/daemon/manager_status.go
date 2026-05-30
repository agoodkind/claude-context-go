package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"

	"goodkind.io/claude-context-go/internal/discovery"
	"goodkind.io/claude-context-go/internal/model"
)

// GetIndex resolves one tracked codebase whose canonical path covers
// requestedPath. The classification return describes how the daemon sees
// the queried path (covered, excluded, out-of-scope) for fine-grained
// callers; tracked callers can ignore it. GetIndex returns Indexed for any
// path whose Milvus collection exists, synthesizing a record when the
// registry has no entry for it.
func (manager *Manager) GetIndex(ctx context.Context, requestedPath string) (model.Codebase, *model.Job, bool, *model.PathClassification, error) {
	manager.reconcileIndexedCodebases(ctx)

	canonicalPath, err := canonicalizePath(requestedPath)
	if err != nil {
		slog.ErrorContext(ctx, "canonicalize path failed", "path", requestedPath, "err", err)
		return model.Codebase{}, nil, false, nil, fmt.Errorf("canonicalize path %s: %w", requestedPath, err)
	}

	manager.mu.Lock()
	matches := manager.findCodebasesByCoverage(canonicalPath)
	if len(matches) > 0 {
		codebase := matches[0]
		activeJob := manager.activeJobSnapshotLocked(codebase)
		manager.mu.Unlock()
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
// indexed. The resolved ignore rules then decide between excluded and
// unindexed for any subpath; an excluded subpath is reported with the
// matched pattern and the gitignore source so callers can name the rule
// that masked the file.
func (manager *Manager) classifyTrackedPath(ctx context.Context, codebase model.Codebase, canonicalPath string) *model.PathClassification {
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
	rules := codebase.ResolvedIgnoreRules
	if rules.IsEmpty() {
		resolved, resolveErr := discovery.EffectiveIgnorePatterns(ctx, codebase.CanonicalPath, codebase.EffectiveConfig.IgnorePatterns)
		if resolveErr == nil {
			rules = resolved
			manager.cacheResolvedRules(codebase.ID, resolved)
		} else {
			slog.DebugContext(ctx, "classifyTrackedPath ignore-resolve failed", "codebase_id", codebase.ID, "err", resolveErr)
		}
	}
	if excluded, matchedPattern, gitignoreSource := discovery.PathIgnored(filepath.ToSlash(relative), rules); excluded {
		classification.Kind = model.PathClassificationInScopeExcluded
		classification.ExcludedByPattern = matchedPattern
		classification.ExcludedByGitignore = gitignoreSource
		return classification
	}
	classification.Kind = model.PathClassificationInScopeUnindexed
	return classification
}

// cacheResolvedRules folds a lazily-resolved IgnoreRules tree back into
// the codebase record so subsequent classification calls do not re-walk
// the codebase. The call is best-effort: a codebase that was deleted
// concurrently is left untouched.
func (manager *Manager) cacheResolvedRules(codebaseID string, rules discovery.IgnoreRules) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	codebase, found := manager.codebases[codebaseID]
	if !found {
		return
	}
	codebase.ResolvedIgnoreRules = rules
	manager.codebases[codebaseID] = codebase
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
