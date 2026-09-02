package daemon

import (
	"context"
	"fmt"
	"log/slog"
)

type semanticCloser interface {
	Close(ctx context.Context) error
}

// Close shuts down the manager's activity, graph, journal, and semantic resources.
func (manager *Manager) Close(ctx context.Context) error {
	if err := manager.cancelAndWaitForJobs(ctx); err != nil {
		return err
	}
	manager.jobScheduler.Close()
	manager.closeJobJournal()
	manager.CloseGraphEngines()
	closer, ok := manager.semantic.(semanticCloser)
	if !ok {
		return nil
	}
	if err := closer.Close(ctx); err != nil {
		slog.ErrorContext(ctx, "close semantic backend", "err", err)
		return fmt.Errorf("close semantic backend: %w", err)
	}
	return nil
}

func (manager *Manager) cancelAndWaitForJobs(ctx context.Context) error {
	for {
		manager.mu.Lock()
		cancels := make([]context.CancelFunc, 0, len(manager.cancels))
		for _, cancel := range manager.cancels {
			cancels = append(cancels, cancel)
		}
		doneChannels := make([]chan struct{}, 0, len(manager.done))
		for _, done := range manager.done {
			doneChannels = append(doneChannels, done)
		}
		manager.mu.Unlock()

		for _, cancel := range cancels {
			cancel()
		}
		if len(doneChannels) == 0 {
			return nil
		}
		for _, done := range doneChannels {
			if err := waitForJobDone(ctx, done); err != nil {
				slog.ErrorContext(ctx, "stop active jobs before manager close", "err", err)
				return fmt.Errorf("stop active jobs before manager close: wait for active job: %w", err)
			}
		}
	}
}
