package render

import (
	"strings"

	"goodkind.io/lm-semantic-search/internal/view"
)

// SchedulingPolicy formats the policy portion of a shared scheduling view.
func SchedulingPolicy(scheduling view.SchedulingView) string {
	if scheduling.Priority == "" {
		return ""
	}
	quiet := "off"
	if scheduling.Quiet {
		quiet = "on"
	}
	parts := []string{
		"priority=" + scheduling.Priority,
		"quiet=" + quiet,
	}
	if scheduling.IdleLabel != "" {
		parts = append(parts, "idle_after="+scheduling.IdleLabel)
	}
	return strings.Join(parts, " · ")
}

// SchedulingReason returns a waiting reason only for queued or paused work.
func SchedulingReason(scheduling view.SchedulingView) string {
	if scheduling.State != "queued" && scheduling.State != "paused" {
		return ""
	}
	return string(scheduling.Reason)
}

func insertSchedulingPolicy(
	body string,
	prefix string,
	scheduling view.SchedulingView,
) string {
	policy := SchedulingPolicy(scheduling)
	if policy == "" {
		return body
	}
	line := prefix + policy
	if body == "" {
		return line
	}
	heading, rest, found := strings.Cut(body, "\n")
	if !found {
		return body + "\n" + line
	}
	return heading + "\n" + line + "\n" + rest
}
