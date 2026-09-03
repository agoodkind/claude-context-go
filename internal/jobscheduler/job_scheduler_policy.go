package jobscheduler

import (
	"fmt"
	"log/slog"

	"goodkind.io/lm-semantic-search/internal/model"
)

// PolicyUpdateReceipt records whether a queued policy update was staged before
// registration so rollback can restore the prior scheduler state atomically.
type PolicyUpdateReceipt struct {
	jobID             string
	staged            bool
	hadPreviousStaged bool
	previousStaged    model.SchedulingPolicyPatch
	entryGeneration   uint64
	policyGeneration  uint64
}

// StagePolicyUpdate applies a policy to a registered entry or stores it for an
// in-flight queued job whose Acquire call has not registered yet.
func (scheduler *Scheduler) StagePolicyUpdate(
	jobID string,
	patch model.SchedulingPolicyPatch,
) (PolicyUpdateReceipt, error) {
	var emptyReceipt PolicyUpdateReceipt
	if jobID == "" {
		return emptyReceipt, fmt.Errorf("scheduler job id is required")
	}
	if _, err := model.ApplySchedulingPolicyPatch(
		model.DefaultSchedulingPolicy(),
		patch,
	); err != nil {
		wrappedErr := fmt.Errorf("validate staged scheduler policy: %w", err)
		slog.Warn("validate staged scheduler policy failed", "job_id", jobID, "err", wrappedErr)
		return emptyReceipt, wrappedErr
	}

	scheduler.mutex.Lock()
	defer scheduler.mutex.Unlock()
	entry, found := scheduler.entries[jobID]
	if !found {
		current, hadPrevious := scheduler.registrationPolicies[jobID]
		scheduler.registrationPolicies[jobID] = mergeSchedulingPolicyPatch(
			current,
			patch,
		)
		scheduler.registrationPolicyGenerations[jobID]++
		return PolicyUpdateReceipt{
			jobID:             jobID,
			staged:            true,
			hadPreviousStaged: hadPrevious,
			previousStaged:    current,
			entryGeneration:   0,
			policyGeneration:  scheduler.registrationPolicyGenerations[jobID],
		}, nil
	}
	if err := scheduler.updatePolicyLocked(entry, patch); err != nil {
		return emptyReceipt, err
	}
	return PolicyUpdateReceipt{
		jobID:             jobID,
		staged:            false,
		hadPreviousStaged: false,
		previousStaged: model.SchedulingPolicyPatch{
			Priority:         nil,
			Quiet:            nil,
			IdleAfterSeconds: nil,
		},
		entryGeneration:  entry.generation,
		policyGeneration: entry.policyGeneration,
	}, nil
}

// RollbackPolicyUpdate restores a staged or registered queued-job policy.
func (scheduler *Scheduler) RollbackPolicyUpdate(
	receipt PolicyUpdateReceipt,
	previousPolicy model.SchedulingPolicy,
) error {
	if receipt.jobID == "" {
		return fmt.Errorf("scheduler policy update receipt is missing a job id")
	}
	if err := model.ValidateSchedulingPolicy(previousPolicy); err != nil {
		wrappedErr := fmt.Errorf("validate scheduler rollback policy: %w", err)
		slog.Warn("validate scheduler rollback policy failed", "job_id", receipt.jobID, "err", wrappedErr)
		return wrappedErr
	}

	scheduler.mutex.Lock()
	defer scheduler.mutex.Unlock()
	if entry, found := scheduler.entries[receipt.jobID]; found {
		if receipt.entryGeneration != entry.generation || receipt.policyGeneration != entry.policyGeneration {
			return nil
		}
		entry.Policy = previousPolicy
		entry.policyGeneration++
		scheduler.rebalanceLocked()
		scheduler.notifyLocked()
		return nil
	}
	if !receipt.staged {
		return fmt.Errorf("scheduler job %s is missing", receipt.jobID)
	}
	if receipt.hadPreviousStaged {
		if scheduler.registrationPolicyGenerations[receipt.jobID] != receipt.policyGeneration {
			return nil
		}
		scheduler.registrationPolicies[receipt.jobID] = receipt.previousStaged
		scheduler.registrationPolicyGenerations[receipt.jobID]++
		return nil
	}
	if scheduler.registrationPolicyGenerations[receipt.jobID] != receipt.policyGeneration {
		return nil
	}
	delete(scheduler.registrationPolicies, receipt.jobID)
	delete(scheduler.registrationPolicyGenerations, receipt.jobID)
	return nil
}

// DiscardStagedPolicyUpdate removes a policy for a job cancelled before
// scheduler registration.
func (scheduler *Scheduler) DiscardStagedPolicyUpdate(jobID string) {
	scheduler.mutex.Lock()
	defer scheduler.mutex.Unlock()
	delete(scheduler.registrationPolicies, jobID)
	delete(scheduler.registrationPolicyGenerations, jobID)
}

func mergeSchedulingPolicyPatch(
	existing model.SchedulingPolicyPatch,
	incoming model.SchedulingPolicyPatch,
) model.SchedulingPolicyPatch {
	merged := existing
	if incoming.Priority != nil {
		merged.Priority = incoming.Priority
	}
	if incoming.Quiet != nil {
		merged.Quiet = incoming.Quiet
	}
	if incoming.IdleAfterSeconds != nil {
		merged.IdleAfterSeconds = incoming.IdleAfterSeconds
	}
	return merged
}
