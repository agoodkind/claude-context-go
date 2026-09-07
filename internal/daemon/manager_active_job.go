package daemon

import (
	"context"

	"goodkind.io/lm-semantic-search/internal/clock"
	"goodkind.io/lm-semantic-search/internal/model"
)

func (manager *Manager) activeJobSnapshotLocked(codebase model.Codebase) *model.Job {
	if codebase.ActiveJobID == "" {
		return nil
	}

	job, found := manager.jobs[codebase.ActiveJobID]
	if !found {
		return nil
	}
	switch job.State {
	case model.JobStateQueued, model.JobStateRunning, model.JobStatePaused, model.JobStateCancelling:
		jobCopy := job
		return &jobCopy
	case model.JobStateCompleted, model.JobStateFailed, model.JobStateCancelled:
		return nil
	default:
		return nil
	}
}

func (manager *Manager) cancelActiveJobForPath(ctx context.Context, canonicalPath string) error {
	for {
		manager.mu.Lock()
		codebase, found := manager.findCodebaseByExactRoot(canonicalPath)
		manager.mu.Unlock()
		if !found || codebase.ActiveJobID == "" {
			return nil
		}
		jobID := codebase.ActiveJobID
		jobDone, cancel, err := manager.beginActiveJobCancellation(codebase)
		if err != nil {
			return err
		}
		if !manager.activeJobMatches(codebase.ID, jobID) {
			continue
		}
		if cancel == nil {
			manager.updateJobCancelled(ctx, jobID)
			return nil
		}
		cancel()
		if err := waitForJobDone(ctx, jobDone); err != nil {
			return err
		}
		return nil
	}
}

func (manager *Manager) activeJobMatches(codebaseID string, jobID string) bool {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	codebase, found := manager.codebases[codebaseID]
	return found && codebase.ActiveJobID == jobID
}

func (manager *Manager) beginActiveJobCancellation(
	codebase model.Codebase,
) (chan struct{}, context.CancelFunc, error) {
	if codebase.ActiveJobID == "" {
		return nil, nil, nil
	}
	job, transitioned, transitionErr := manager.serializeJobTransition(
		codebase.ActiveJobID,
		"job_cancelling",
		func(job *model.Job) bool {
			if isTerminalJobState(job.State) || job.State == model.JobStateCancelling {
				return false
			}
			now := clock.Now()
			job.State = model.JobStateCancelling
			job.UpdatedAt = now
			job.Progress.Phase = "cancelling"
			job.Progress.LastEventAt = now
			job.Progress.HeartbeatAt = now
			return true
		},
	)
	if transitionErr != nil {
		return nil, nil, transitionErr
	}
	if !transitioned && job.State != model.JobStateCancelling {
		return nil, nil, nil
	}
	manager.mu.Lock()
	cancel := manager.cancels[job.ID]
	jobDone := manager.done[job.ID]
	manager.mu.Unlock()
	if cancel == nil {
		return jobDone, nil, nil
	}
	return jobDone, cancel, nil
}
