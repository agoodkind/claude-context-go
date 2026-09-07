package daemon

import (
	"context"
	"fmt"

	"goodkind.io/lm-semantic-search/internal/model"
)

// CancelJob marks a tracked job as cancelled.
func (manager *Manager) CancelJob(ctx context.Context, jobID string) (model.Job, error) {
	manager.mu.Lock()
	job, found := manager.jobs[jobID]
	if !found {
		manager.mu.Unlock()
		return model.Job{}, fmt.Errorf("job not found: %s", jobID)
	}
	if isTerminalJobState(job.State) {
		manager.mu.Unlock()
		manager.jobScheduler.DiscardStagedPolicyUpdate(jobID)
		return job, nil
	}
	cancel := manager.cancels[jobID]
	jobDone := manager.done[jobID]
	manager.mu.Unlock()

	if cancel != nil {
		cancel()
		if err := waitForJobDone(ctx, jobDone); err != nil {
			return model.Job{}, err
		}
	}

	manager.policyMutationMutex.Lock()
	manager.mu.Lock()
	job, found = manager.jobs[jobID]
	manager.mu.Unlock()
	if !found {
		manager.policyMutationMutex.Unlock()
		return model.Job{}, fmt.Errorf("job not found: %s", jobID)
	}
	if isTerminalJobState(job.State) {
		manager.jobScheduler.DiscardStagedPolicyUpdate(jobID)
		manager.policyMutationMutex.Unlock()
		return job, nil
	}

	followup := manager.updateJobCancelledWithPolicy(ctx, jobID)
	manager.jobScheduler.DiscardStagedPolicyUpdate(jobID)
	updated, found := manager.GetJob(jobID)
	manager.policyMutationMutex.Unlock()
	if !found {
		return model.Job{}, fmt.Errorf("job not found: %s", jobID)
	}
	manager.runCancellationFollowup(ctx, followup)
	return updated, nil
}
