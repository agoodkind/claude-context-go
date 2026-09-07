package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/semantic"
	"goodkind.io/lm-semantic-search/internal/store"
)

// ClearIndex removes a tracked codebase from daemon state.
func (manager *Manager) ClearIndex(
	ctx context.Context,
	requestedPath string,
	client model.ClientInfo,
) (model.Codebase, error) {
	manager.policyMutationMutex.Lock()
	policyLocked := true
	unlockPolicy := func() {
		if !policyLocked {
			return
		}
		manager.policyMutationMutex.Unlock()
		policyLocked = false
	}
	defer func() {
		unlockPolicy()
	}()
	_ = client

	canonicalPath, err := manager.resolveCanonicalPath(requestedPath)
	if err != nil {
		slog.ErrorContext(ctx, "canonicalize path failed", "path", requestedPath, "err", err)
		return model.Codebase{}, fmt.Errorf("canonicalize path %s: %w", requestedPath, err)
	}

	var codebase model.Codebase
	for {
		manager.mu.Lock()
		matches := manager.findCodebasesByCoverage(canonicalPath)
		if len(matches) == 0 {
			manager.mu.Unlock()
			return model.Codebase{}, errors.New("codebase not tracked: " + requestedPath)
		}
		codebase = matches[0]
		manager.mu.Unlock()
		jobDone, cancel, cancelErr := manager.beginActiveJobCancellation(codebase)
		if cancelErr != nil {
			return model.Codebase{}, cancelErr
		}
		if !manager.activeJobMatches(codebase.ID, codebase.ActiveJobID) {
			continue
		}
		if cancel != nil {
			cancel()
		}
		if jobDone == nil {
			break
		}
		unlockPolicy()
		waitErr := waitForJobDone(ctx, jobDone)
		if waitErr != nil {
			return model.Codebase{}, waitErr
		}
		manager.policyMutationMutex.Lock()
		policyLocked = true
	}
	manager.mu.Lock()
	delete(manager.pendingConversationJobs, codebase.ID)
	delete(manager.pendingCodeJobs, codebase.ID)
	manager.mu.Unlock()
	if err := manager.removeCodebaseArtifacts(ctx, codebase); err != nil {
		return model.Codebase{}, err
	}

	manager.mu.Lock()
	clearedCodebase := codebase
	current, found := manager.codebases[codebase.ID]
	if !found {
		manager.mu.Unlock()
		unlockPolicy()
		manager.notifyCodebaseRemoved(ctx, codebase.ID)
		return clearedCodebase, nil
	}
	delete(manager.codebases, current.ID)
	if err := manager.saveLocked(); err != nil {
		manager.mu.Unlock()
		return model.Codebase{}, err
	}
	manager.mu.Unlock()
	unlockPolicy()
	manager.notifyCodebaseRemoved(ctx, current.ID)
	return current, nil
}

func (manager *Manager) removeCodebaseArtifacts(
	ctx context.Context,
	codebase model.Codebase,
) error {
	if err := store.RemoveFile(manager.chunkPath(codebase.ID)); err != nil {
		wrappedErr := fmt.Errorf("remove chunk cache for %s: %w", codebase.ID, err)
		slog.ErrorContext(ctx, "remove chunk cache failed", "codebase_id", codebase.ID, "err", wrappedErr)
		return wrappedErr
	}
	if err := store.RemoveFile(manager.merklePath(codebase.ID)); err != nil {
		wrappedErr := fmt.Errorf("remove Merkle snapshot for %s: %w", codebase.ID, err)
		slog.ErrorContext(ctx, "remove Merkle snapshot failed", "codebase_id", codebase.ID, "err", wrappedErr)
		return wrappedErr
	}
	if err := manager.clearGraphCache(ctx, codebase.ID); err != nil {
		wrappedErr := fmt.Errorf("remove graph cache for %s: %w", codebase.ID, err)
		slog.ErrorContext(ctx, "remove graph cache failed", "codebase_id", codebase.ID, "err", wrappedErr)
		return wrappedErr
	}
	if err := store.RemoveFile(manager.stagingMerklePath(codebase.ID)); err != nil {
		wrappedErr := fmt.Errorf("remove staging Merkle snapshot for %s: %w", codebase.ID, err)
		slog.ErrorContext(ctx, "remove staging Merkle snapshot failed", "codebase_id", codebase.ID, "err", wrappedErr)
		return wrappedErr
	}
	if manager.semantic == nil {
		return nil
	}
	if err := manager.semantic.Drop(ctx, codebase.CanonicalPath); err != nil &&
		!errors.Is(err, semantic.ErrUnavailable) {
		wrappedErr := fmt.Errorf("drop semantic index for %s: %w", codebase.CanonicalPath, err)
		slog.ErrorContext(ctx, "drop semantic index failed", "codebase_id", codebase.ID, "err", wrappedErr)
		return wrappedErr
	}
	if err := manager.semantic.DropStaging(ctx, codebase.CanonicalPath); err != nil &&
		!errors.Is(err, semantic.ErrUnavailable) {
		wrappedErr := fmt.Errorf("drop semantic staging for %s: %w", codebase.CanonicalPath, err)
		slog.ErrorContext(ctx, "drop semantic staging failed", "codebase_id", codebase.ID, "err", wrappedErr)
		return wrappedErr
	}
	return nil
}
