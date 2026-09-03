package model

import "fmt"

// DefaultIdleAfterSeconds is the idle threshold for a new policy.
const DefaultIdleAfterSeconds int32 = 300

// JobPriority orders queued index work.
type JobPriority string

const (
	// JobPriorityHigh runs before normal and low priority work.
	JobPriorityHigh JobPriority = "high"
	// JobPriorityNormal is the default queue priority.
	JobPriorityNormal JobPriority = "normal"
	// JobPriorityLow runs after high and normal priority work.
	JobPriorityLow JobPriority = "low"
)

// SchedulingPolicy stores a codebase's queue behavior.
type SchedulingPolicy struct {
	Priority         JobPriority `json:"priority,omitempty"`
	Quiet            bool        `json:"quiet,omitempty"`
	IdleAfterSeconds int32       `json:"idle_after_seconds,omitempty"`
}

// SchedulingPolicyPatch replaces only non-nil policy fields.
type SchedulingPolicyPatch struct {
	Priority         *JobPriority `json:"priority,omitempty"`
	Quiet            *bool        `json:"quiet,omitempty"`
	IdleAfterSeconds *int32       `json:"idle_after_seconds,omitempty"`
}

// PolicyUpdateTransaction retains the durable state needed to roll back one
// interrupted stored-policy mutation.
type PolicyUpdateTransaction struct {
	CodebaseID      string   `json:"codebase_id"`
	OldCodebase     Codebase `json:"old_codebase"`
	OldActiveJob    *Job     `json:"old_active_job,omitempty"`
	OldDetachedJobs []Job    `json:"old_detached_jobs,omitempty"`
}

// DefaultSchedulingPolicy returns the policy for new and legacy records.
func DefaultSchedulingPolicy() SchedulingPolicy {
	return SchedulingPolicy{
		Priority:         JobPriorityNormal,
		Quiet:            false,
		IdleAfterSeconds: DefaultIdleAfterSeconds,
	}
}

// ValidateSchedulingPolicy rejects unsupported priorities and idle thresholds.
func ValidateSchedulingPolicy(policy SchedulingPolicy) error {
	switch policy.Priority {
	case JobPriorityHigh, JobPriorityNormal, JobPriorityLow:
	default:
		return fmt.Errorf("priority must be high, normal, or low")
	}
	if policy.IdleAfterSeconds <= 0 {
		return fmt.Errorf("idle after seconds must be positive")
	}
	return nil
}

// ApplySchedulingPolicyPatch fills legacy zero values and applies the patch.
func ApplySchedulingPolicyPatch(stored SchedulingPolicy, patch SchedulingPolicyPatch) (SchedulingPolicy, error) {
	policy := stored
	defaults := DefaultSchedulingPolicy()
	if policy.Priority == "" {
		policy.Priority = defaults.Priority
	}
	if policy.IdleAfterSeconds == 0 {
		policy.IdleAfterSeconds = defaults.IdleAfterSeconds
	}
	if patch.Priority != nil {
		policy.Priority = *patch.Priority
	}
	if patch.Quiet != nil {
		policy.Quiet = *patch.Quiet
	}
	if patch.IdleAfterSeconds != nil {
		policy.IdleAfterSeconds = *patch.IdleAfterSeconds
	}
	if err := ValidateSchedulingPolicy(policy); err != nil {
		return SchedulingPolicy{}, err
	}
	return policy, nil
}
