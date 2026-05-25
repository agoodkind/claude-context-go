package daemon

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"goodkind.io/claude-context-go/internal/model"
)

const (
	indexingWarningTemplate = "⚠️  **Indexing in Progress**: This codebase is currently being indexed in the background. Search results may be incomplete or inaccurate until indexing completes. Progress: %.1f%%."
	noResultsIndexingTip    = "Note: This codebase is still being indexed. Try searching again after indexing completes, or the query may not match any indexed content."
	searchIndexingTip       = "💡 **Tip**: This codebase is still being indexed. More results may become available as indexing progresses."
)

type searchView struct {
	RequestedPath string
	Query         string
	Codebase      model.Codebase
	ActiveJob     *model.Job
	Results       []model.StoredChunk
}

func renderStartIndex(requestedPath string, codebase model.Codebase, job model.Job, deduplicated bool) string {
	if deduplicated {
		return fmt.Sprintf(
			"Background indexing is already running for codebase '%s' using %s splitter.\nCurrent job: %s\n\nIndexing is running in the background. You can search the codebase while indexing is in progress, but results may be incomplete until indexing completes.",
			codebase.CanonicalPath,
			strings.ToUpper(orDefault(job.Config.SplitterType, "ast")),
			job.ID,
		)
	}

	pathInfo := ""
	if requestedPath != "" && requestedPath != codebase.CanonicalPath {
		pathInfo = fmt.Sprintf("\nNote: Input path '%s' was resolved to canonical path '%s'", requestedPath, codebase.CanonicalPath)
	}

	return fmt.Sprintf(
		"Started background indexing for codebase '%s' using %s splitter.%s\n\nIndexing is running in the background. You can search the codebase while indexing is in progress, but results may be incomplete until indexing completes.",
		codebase.CanonicalPath,
		strings.ToUpper(orDefault(job.Config.SplitterType, "ast")),
		pathInfo,
	)
}

func renderClearIndex(codebase model.Codebase, remainingIndexed int, remainingIndexing int) string {
	message := fmt.Sprintf("Successfully cleared codebase '%s'", codebase.CanonicalPath)
	if remainingIndexed > 0 || remainingIndexing > 0 {
		message += fmt.Sprintf("\n%d other indexed codebase(s) and %d indexing codebase(s) remain", remainingIndexed, remainingIndexing)
	}
	return message
}

func renderCancelJob(job model.Job) string {
	if job.State == model.JobStateCancelled {
		return "Cancelled indexing job " + job.ID
	}
	return fmt.Sprintf("Indexing job %s is already %s", job.ID, job.State)
}

func renderSyncIndex(codebase model.Codebase, job model.Job, deduplicated bool) string {
	if deduplicated {
		return fmt.Sprintf("Sync request deduplicated onto active job %s for '%s'", job.ID, codebase.CanonicalPath)
	}
	return fmt.Sprintf("Started sync job %s for '%s'", job.ID, codebase.CanonicalPath)
}

func renderGetIndex(requestedPath string, tracked bool, codebase *model.Codebase, activeJob *model.Job) string {
	if !tracked || codebase == nil {
		return fmt.Sprintf("❌ Codebase '%s' is not indexed. Please use the index_codebase tool to index it first.", requestedPath)
	}

	switch codebase.Status {
	case model.CodebaseStatusNotIndexed:
		return fmt.Sprintf("❌ Codebase '%s' is not indexed. Please use the index_codebase tool to index it first.", requestedPath)
	case model.CodebaseStatusIndexed:
		if codebase.LastSuccessfulRun == nil {
			return fmt.Sprintf("✅ Codebase '%s' is fully indexed and ready for search.", codebase.CanonicalPath)
		}
		return fmt.Sprintf(
			"✅ Codebase '%s' is fully indexed and ready for search.\n📊 Statistics: %d files, %d chunks\n📅 Status: %s\n🕐 Last updated: %s",
			codebase.CanonicalPath,
			codebase.LastSuccessfulRun.IndexedFiles,
			codebase.LastSuccessfulRun.TotalChunks,
			orDefault(codebase.LastSuccessfulRun.Status, "completed"),
			formatLocalTime(codebase.LastSuccessfulRun.CompletedAt),
		)
	case model.CodebaseStatusIndexing:
		progress := 0.0
		lastUpdated := codebase.UpdatedAt
		if activeJob != nil {
			progress = activeJob.Progress.OverallPercent
			if !activeJob.Progress.LastEventAt.IsZero() {
				lastUpdated = activeJob.Progress.LastEventAt
			}
		}
		return fmt.Sprintf(
			"🔄 Codebase '%s' is currently being indexed. Progress: %.1f%%%s\n🕐 Last updated: %s",
			codebase.CanonicalPath,
			progress,
			progressPhaseSuffix(progress),
			formatLocalTime(lastUpdated),
		)
	case model.CodebaseStatusFailed, model.CodebaseStatusStale:
		if codebase.LastFailedRun == nil {
			return fmt.Sprintf("❌ Codebase '%s' indexing failed. You can retry indexing.", codebase.CanonicalPath)
		}
		return fmt.Sprintf(
			"❌ Codebase '%s' indexing failed.\n🚨 Error: %s\n📊 Failed at: %.1f%% progress\n🕐 Failed at: %s\n💡 You can retry indexing by running the index_codebase command again.",
			codebase.CanonicalPath,
			orDefault(codebase.LastFailedRun.Message, "unknown error"),
			float64(codebase.LastFailedRun.LastAttemptedPercentage),
			formatLocalTime(codebase.LastFailedRun.FailedAt),
		)
	default:
		return fmt.Sprintf("❌ Codebase '%s' is not indexed. Please use the index_codebase tool to index it first.", requestedPath)
	}
}

func renderListIndexes(codebases []model.Codebase) string {
	if len(codebases) == 0 {
		return "No tracked codebases."
	}

	lines := make([]string, 0, len(codebases)+1)
	lines = append(lines, fmt.Sprintf("Tracked codebases: %d", len(codebases)))
	for _, codebase := range codebases {
		lines = append(lines, fmt.Sprintf("- %s [%s]", codebase.CanonicalPath, codebase.Status))
	}
	return strings.Join(lines, "\n")
}

func renderGetJob(job *model.Job) string {
	if job == nil {
		return "Job not found."
	}
	return fmt.Sprintf(
		"Job %s\nCodebase: %s\nOperation: %s\nState: %s\nPhase: %s\nProgress: %.1f%%",
		job.ID,
		job.CanonicalPath,
		job.Operation,
		job.State,
		job.Progress.Phase,
		job.Progress.OverallPercent,
	)
}

func renderListJobs(jobs []model.Job) string {
	if len(jobs) == 0 {
		return "No tracked jobs."
	}

	lines := make([]string, 0, len(jobs)+1)
	lines = append(lines, fmt.Sprintf("Tracked jobs: %d", len(jobs)))
	for _, job := range jobs {
		lines = append(lines, fmt.Sprintf("- %s [%s %.1f%%] %s", job.ID, job.State, job.Progress.OverallPercent, job.CanonicalPath))
	}
	return strings.Join(lines, "\n")
}

func renderDoctor(diagnostics []string) string {
	if len(diagnostics) == 0 {
		return "No indexing issues detected."
	}

	lines := make([]string, 0, len(diagnostics)+1)
	lines = append(lines, "Indexing diagnostics:")
	for _, diagnostic := range diagnostics {
		lines = append(lines, "- "+diagnostic)
	}
	return strings.Join(lines, "\n")
}

func renderSearch(view searchView) string {
	warning := ""
	if view.ActiveJob != nil && view.Codebase.Status == model.CodebaseStatusIndexing {
		warning = fmt.Sprintf(indexingWarningTemplate, view.ActiveJob.Progress.OverallPercent)
	}

	if len(view.Results) == 0 {
		noResults := fmt.Sprintf("No results found for query: %q in codebase '%s'", view.Query, view.Codebase.CanonicalPath)
		if warning == "" {
			return noResults
		}
		return warning + "\n\n" + noResults + "\n\n" + noResultsIndexingTip
	}

	formatted := make([]string, 0, len(view.Results))
	for index, result := range view.Results {
		language := orDefault(result.Language, "text")
		formatted = append(formatted, fmt.Sprintf(
			"%d. Code snippet (%s) [%s]\n   Location: %s:%d-%d\n   Rank: %d\n   Context:\n```%s\n%s\n```",
			index+1,
			language,
			filepath.Base(view.Codebase.CanonicalPath),
			result.RelativePath,
			result.StartLine,
			result.EndLine,
			index+1,
			language,
			strings.TrimSpace(truncateContent(result.Content, 5000)),
		))
	}

	header := fmt.Sprintf("Found %d results for query: %q in codebase '%s'", len(view.Results), view.Query, view.Codebase.CanonicalPath)
	body := header + "\n\n" + strings.Join(formatted, "\n\n")
	if warning == "" {
		return body
	}
	return warning + "\n\n" + body + "\n\n" + searchIndexingTip
}

func progressPhaseSuffix(progress float64) string {
	if progress < 10 {
		return " (Preparing and scanning files...)"
	}
	if progress < 100 {
		return " (Processing files and generating embeddings...)"
	}
	return ""
}

func formatLocalTime(value time.Time) string {
	if value.IsZero() {
		return "unknown"
	}
	return value.Format("1/2/2006, 3:04:05 PM")
}

func truncateContent(content string, limit int) string {
	if len(content) <= limit {
		return content
	}
	if limit <= 3 {
		return content[:limit]
	}
	return content[:limit-3] + "..."
}

func orDefault(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
