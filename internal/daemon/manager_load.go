package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/store"
)

func (manager *Manager) load(ctx context.Context) error {
	registry, err := store.ReadRegistry(manager.config.RegistryPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.ErrorContext(ctx, "read registry failed", "path", manager.config.RegistryPath, "err", err)
		return fmt.Errorf("read registry: %w", err)
	}
	for _, codebase := range registry.Codebases {
		if codebase.Kind == "" {
			codebase.Kind = model.CodebaseKindCode
		}
		normalizedPolicy, policyErr := model.ApplySchedulingPolicyPatch(codebase.SchedulingPolicy, model.SchedulingPolicyPatch{
			Priority:         nil,
			Quiet:            nil,
			IdleAfterSeconds: nil,
		})
		if policyErr != nil {
			return fmt.Errorf("normalize scheduling policy for codebase %s: %w", codebase.ID, policyErr)
		}
		codebase.SchedulingPolicy = normalizedPolicy
		manager.codebases[codebase.ID] = codebase
	}
	dropGhostURICodebases(manager.codebases)

	jobs, err := store.ReadJobEvents(manager.config.JobsPath)
	if err != nil {
		slog.ErrorContext(ctx, "read jobs failed", "path", manager.config.JobsPath, "err", err)
		return fmt.Errorf("read jobs: %w", err)
	}
	for id, job := range jobs {
		normalizedPolicy, policyErr := model.ApplySchedulingPolicyPatch(job.EffectiveSchedulingPolicy, model.SchedulingPolicyPatch{
			Priority:         nil,
			Quiet:            nil,
			IdleAfterSeconds: nil,
		})
		if policyErr != nil {
			return fmt.Errorf("normalize effective scheduling policy for job %s: %w", id, policyErr)
		}
		job.EffectiveSchedulingPolicy = normalizedPolicy
		manager.jobs[id] = job
	}
	manager.reconcileJournalOnStartLocked()
	return nil
}
