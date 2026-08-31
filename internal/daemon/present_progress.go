package daemon

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/pbconv"
	render "goodkind.io/lm-semantic-search/internal/render"
	"goodkind.io/lm-semantic-search/internal/view"
)

type jobPhase string

const (
	jobPhaseCancelling jobPhase = "cancelling"
	jobPhaseCancelled  jobPhase = "cancelled"
)

// formatCount renders an integer with thousands separators so large corpus
// numbers stay readable, for example "33,240".
func formatCount(value int32) string {
	digits := strconv.FormatInt(int64(value), 10)
	if len(digits) <= 3 {
		return digits
	}
	var out strings.Builder
	lead := len(digits) % 3
	if lead > 0 {
		out.WriteString(digits[:lead])
		if len(digits) > lead {
			out.WriteString(",")
		}
	}
	for i := lead; i < len(digits); i += 3 {
		out.WriteString(digits[i : i+3])
		if i+3 < len(digits) {
			out.WriteString(",")
		}
	}
	return out.String()
}

// formatBoundaryTime renders a timestamp for view labels in the host's zone.
func formatBoundaryTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	const layout = "1/2/2006, 3:04:05 PM MST"
	return render.InLocalZone(value).Format(layout)
}

// progressHeading names the pass for an active job, empty otherwise.
func progressHeading(job model.Job) string {
	if job.State != model.JobStateQueued && job.State != model.JobStateRunning && job.State != model.JobStatePaused && job.State != model.JobStateCancelling {
		return ""
	}
	switch job.Progress.RunMode {
	case model.RunModeResuming:
		return "Resuming after restart: checking changed work, embedding only what's new"
	case model.RunModeFirstBuild:
		return "Building initial index"
	case model.RunModeForcedReindex:
		return "Forced reindex"
	case model.RunModeChanged:
		return "Indexing new changes"
	default:
		return ""
	}
}

// resolveOutcomeBreakdown adapts a job's model.Progress into the shared resolver
// in internal/view, the single source of truth for the breakdown. The daemon
// only copies counters here; the bucket logic, row order, and scope label all
// live in view.ResolveBreakdown so every surface projects from the same value.
func resolveOutcomeBreakdown(progress model.Progress) view.OutcomeBreakdown {
	return view.ResolveBreakdown(pbconv.ProgressCounts(progress))
}

// resolveOverallPercent reduces a job's progress into the headline completion
// percent through the shared view resolver, so the compact job surface, the
// codebase status view, and the wire (pbconv.ToProgress) all report one value.
// It reads only existing chunk counters; the corpus-completion figure counts
// reused work as done so a restarted or reuse-heavy run does not read as
// near-zero behind a reset file cursor.
func resolveOverallPercent(progress model.Progress) float64 {
	return view.ResolveOverallPercent(pbconv.ProgressCounts(progress), progress.OverallPercent)
}

// resolveProgressSurface reduces a job's progress into the typed view. It is
// the only reader of Progress fields for the compact job surfaces.
func resolveProgressSurface(job model.Job) view.ProgressSurface {
	progress := job.Progress
	scopeUnit := progress.ScopeUnit
	if scopeUnit == "" {
		scopeUnit = progress.Unit
	}
	if scopeUnit == "" {
		scopeUnit = "file"
	}

	active := job.State == model.JobStateQueued || job.State == model.JobStateRunning || job.State == model.JobStatePaused || job.State == model.JobStateCancelling

	percentLabel := fmt.Sprintf("%.1f%%", resolveOverallPercent(progress))
	percentScopeLabel := percentLabel + " run progress"
	if active && !jobScopeKnown(progress) {
		if jobOperation(job.Operation) == jobOperationSync {
			percentLabel = "Changes detected, preparing to index"
		} else {
			percentLabel = "Preparing to index"
		}
		percentScopeLabel = percentLabel
	}

	scopeLine := ""
	if progress.FilesAdded > 0 || progress.FilesModified > 0 || progress.FilesRemoved > 0 {
		parts := []string{}
		if progress.FilesAdded > 0 {
			parts = append(parts, fmt.Sprintf("%s %s added", formatCount(progress.FilesAdded), scopeUnit+pluralSuffix(progress.FilesAdded)))
		}
		if progress.FilesModified > 0 {
			parts = append(parts, formatCount(progress.FilesModified)+" modified")
		}
		if progress.FilesRemoved > 0 {
			parts = append(parts, formatCount(progress.FilesRemoved)+" removed")
		}
		scopeLine = "Changed since last sync: " + strings.Join(parts, " · ")
	}

	batchLine := ""
	if active && progress.EmbeddingBatchesTotal > 0 {
		batchLine = "embedding batch " + formatCount(progress.EmbeddingBatchesCompleted) + " of " + formatCount(progress.EmbeddingBatchesTotal)
	}

	return view.ProgressSurface{
		Heading:           progressHeading(job),
		Breakdown:         resolveOutcomeBreakdown(progress),
		ScopeLine:         scopeLine,
		PercentLabel:      percentLabel,
		PercentScopeLabel: percentScopeLabel,
		BatchLine:         batchLine,
	}
}

// pluralSuffix returns "s" for counts other than one.
func pluralSuffix(count int32) string {
	if count == 1 {
		return ""
	}
	return "s"
}

// resolveTimingView formats a job's timing block.
func resolveTimingView(job model.Job) view.TimingView {
	timing := view.TimingView{
		StartedLabel:   formatBoundaryTime(job.StartedAt),
		UpdatedLabel:   formatBoundaryTime(job.UpdatedAt),
		CompletedLabel: "",
		DurationLabel:  "",
		DurationWord:   "Elapsed",
	}
	if job.CompletedAt != nil {
		timing.CompletedLabel = formatBoundaryTime(*job.CompletedAt)
		timing.DurationWord = "Duration"
	}
	end := job.UpdatedAt
	if job.CompletedAt != nil && !job.CompletedAt.IsZero() {
		end = *job.CompletedAt
	}
	if !job.StartedAt.IsZero() && !end.IsZero() && !end.Before(job.StartedAt) {
		duration := end.Sub(job.StartedAt).Round(time.Second)
		if duration <= 0 {
			timing.DurationLabel = "0s"
		} else {
			timing.DurationLabel = duration.String()
		}
	}
	return timing
}

// resolveJobEntry assembles the full job view for list and detail surfaces.
func resolveJobEntry(job model.Job, pipelineDegraded bool, supersededByJobID string) view.JobEntryView {
	return view.JobEntryView{
		ID:            job.ID,
		CanonicalPath: job.CanonicalPath,
		Operation:     job.Operation,
		PhaseLabel:    displayJobPhase(job.Progress.Phase),
		Surface:       resolveJobSurface(job, pipelineDegraded, supersededByJobID),
		Progress:      resolveProgressSurface(job),
		Timing:        resolveTimingView(job),
	}
}

func displayJobPhase(phase string) string {
	switch jobPhase(strings.TrimSpace(phase)) {
	case jobPhaseCancelling:
		return "canceling"
	case jobPhaseCancelled:
		return "canceled"
	default:
		return phase
	}
}

// resolveListSummary tallies the job list.
func resolveListSummary(jobs []model.Job, pipelineDegraded bool) view.ListSummary {
	successors := buildJobSuccessors(jobs)
	summary := view.ListSummary{
		Total:      len(jobs),
		Queued:     0,
		Running:    0,
		Canceling:  0,
		Completed:  0,
		Failed:     0,
		Superseded: 0,
		Canceled:   0,
	}
	for _, job := range jobs {
		switch job.State {
		case model.JobStateQueued:
			summary.Queued++
		case model.JobStateRunning:
			summary.Running++
		case model.JobStatePaused:
			summary.Queued++
		case model.JobStateCancelling:
			summary.Canceling++
		case model.JobStateCompleted:
			summary.Completed++
		case model.JobStateFailed:
			if resolveJobSurface(job, pipelineDegraded, successors[job.ID]).Superseded {
				summary.Superseded++
			} else {
				summary.Failed++
			}
		case model.JobStateCancelled:
			summary.Canceled++
		}
	}
	return summary
}
