package view

import (
	"strconv"
	"strings"
	"time"
)

// SchedulingView is one resolved scheduling policy and any active wait state.
type SchedulingView struct {
	Priority  string
	Quiet     bool
	IdleLabel string
	State     string
	Reason    SchedulingReason
}

// SchedulingReason is the presentation package's model-free reason vocabulary.
type SchedulingReason string

const (
	// SchedulingReasonUnspecified means no policy reason applies.
	SchedulingReasonUnspecified SchedulingReason = ""
	// SchedulingReasonHigherPriorityWork means higher-priority work owns capacity.
	SchedulingReasonHigherPriorityWork SchedulingReason = "higher-priority work"
	// SchedulingReasonUserActive means the input-idle threshold has not elapsed.
	SchedulingReasonUserActive SchedulingReason = "user active"
	// SchedulingReasonActivityUnavailable means activity cannot be observed.
	SchedulingReasonActivityUnavailable SchedulingReason = "activity unavailable"
	// SchedulingReasonThermalSafety means host thermal state blocks quiet work.
	SchedulingReasonThermalSafety SchedulingReason = "thermal safety"
)

// ResolveScheduling builds the shared scheduling presentation from stored or
// wire values.
func ResolveScheduling(
	priority string,
	quiet bool,
	idleAfterSeconds int32,
	state string,
	reason SchedulingReason,
) SchedulingView {
	resolvedState := strings.TrimSpace(state)
	resolvedReason := SchedulingReasonUnspecified
	if resolvedState == "queued" || resolvedState == "paused" {
		resolvedReason = canonicalSchedulingReason(reason)
	}
	return SchedulingView{
		Priority:  strings.ToLower(strings.TrimSpace(priority)),
		Quiet:     quiet,
		IdleLabel: schedulingIdleLabel(idleAfterSeconds),
		State:     resolvedState,
		Reason:    resolvedReason,
	}
}

// ZeroScheduling returns an empty scheduling view.
func ZeroScheduling() SchedulingView {
	return SchedulingView{
		Priority:  "",
		Quiet:     false,
		IdleLabel: "",
		State:     "",
		Reason:    SchedulingReasonUnspecified,
	}
}

func schedulingIdleLabel(idleAfterSeconds int32) string {
	if idleAfterSeconds <= 0 {
		return ""
	}
	if idleAfterSeconds%3600 == 0 {
		return strconv.FormatInt(int64(idleAfterSeconds/3600), 10) + "h"
	}
	if idleAfterSeconds%60 == 0 {
		return strconv.FormatInt(int64(idleAfterSeconds/60), 10) + "m"
	}
	return (time.Duration(idleAfterSeconds) * time.Second).String()
}

func canonicalSchedulingReason(reason SchedulingReason) SchedulingReason {
	switch reason {
	case SchedulingReasonHigherPriorityWork,
		SchedulingReasonUserActive,
		SchedulingReasonActivityUnavailable,
		SchedulingReasonThermalSafety:
		return reason
	case SchedulingReasonUnspecified:
		return SchedulingReasonUnspecified
	default:
		return SchedulingReasonUnspecified
	}
}
