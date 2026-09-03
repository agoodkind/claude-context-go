package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"goodkind.io/lm-semantic-search/internal/clock"
	"goodkind.io/lm-semantic-search/internal/gitworktree"
	"goodkind.io/lm-semantic-search/internal/model"
	render "goodkind.io/lm-semantic-search/internal/render"
	"goodkind.io/lm-semantic-search/internal/status"
	"goodkind.io/lm-semantic-search/internal/view"
)

// displayStatus is the user-facing status a codebase presents. It aliases the
// status package's Display so the daemon keeps its short names while the values
// and the resolution rules live in the single status source of truth.
type displayStatus = status.Display

const (
	displayPreparing   = status.DisplayPreparing
	displayIndexing    = status.DisplayIndexing
	displayIndexed     = status.DisplayIndexed
	displayQuarantined = status.DisplayQuarantined
	displayWaiting     = status.DisplayWaiting
	displayStale       = status.DisplayStale
	displayFailed      = status.DisplayFailed
	displayMissing     = status.DisplayMissing
	displayDiscovered  = status.DisplayDiscovered
	displayPending     = status.DisplayPending
	displayLoading     = status.DisplayLoading
	displayIdle        = status.DisplayIdle
)

// collectionNotApplicable is the daemon's short name for a surface that does not
// probe per-path collection readiness (the list view, the search status view).
// Such a surface reflects only the global dependency mode.
const collectionNotApplicable = status.CollectionNotApplicable

var relativeTimeNow = clock.Now

// computeDisplayStatus resolves the display status through the status package,
// the single source of truth for every surface (list, detail, MCP, CLI). It
// reduces the live job and the daemon's dependency health into the normalized
// status.Inputs and lets status.ResolveDisplay fold them onto the persisted
// status. pipelineDegraded carries only whether a shared dependency is degraded;
// ResolveDisplay reads it through Degraded(), so the specific mode does not
// matter here and the banner names the cause separately.
func computeDisplayStatus(codebase model.Codebase, activeJob *model.Job, dependency dependencyMode, readiness status.CollectionReadiness) displayStatus {
	return status.Resolve(status.Inputs{
		Status:                  codebase.Status,
		HasActiveJob:            activeJob != nil,
		JobQueued:               activeJob != nil && activeJob.State == model.JobStateQueued,
		JobPaused:               activeJob != nil && activeJob.State == model.JobStatePaused,
		JobScopeKnown:           activeJob != nil && jobScopeKnown(activeJob.Progress),
		BackgroundSyncReconcile: activeJob != nil && isBackgroundSyncReconcile(&codebase, activeJob),
		Dependency:              dependency,
		Collection:              readiness,
		Search:                  status.SearchNone,
		SearchableEligible:      false,
	}).Display
}

// computeSearchable resolves whether a path can be searched right now through the
// status package, the single source of truth, instead of combining the indexed
// classification with the dependency health inline at the RPC boundary. It is the
// searchable-bit mirror of computeDisplayStatus: searchableEligible is the
// per-path indexed precondition and pipelineDegraded carries whether the shared
// backend is degraded, and status.ResolveSearchable owns the fold so the wire
// `searchable` field and the displayed status cannot diverge.
func computeSearchable(searchableEligible bool, dependency dependencyMode, readiness status.CollectionReadiness) bool {
	return status.Resolve(status.Inputs{
		Status:                  "",
		HasActiveJob:            false,
		JobQueued:               false,
		JobPaused:               false,
		JobScopeKnown:           false,
		BackgroundSyncReconcile: false,
		Dependency:              dependency,
		Collection:              readiness,
		Search:                  status.SearchNone,
		SearchableEligible:      searchableEligible,
	}).Searchable
}

// resolveJobSurface reduces a raw job and the pipeline-degraded flag into the
// status package's resolved job view. It is the one seam between a model.Job and
// the SOT, the job-side mirror of computeDisplayStatus, so it lives here at the
// boundary rather than in the render layer the guard test keeps free of raw job
// reads. Every job surface formats from the JobSurface it returns instead of
// re-deriving a state label or error echo. A job stopping on a shared
// dependency is exactly a retryable error during a degraded pipeline, which
// ResolveJob folds by suppressing the per-job echo the banner already carries.
func resolveJobSurface(job model.Job, pipelineDegraded bool, supersededByJobID string) view.JobSurface {
	dependency := status.Healthy
	if pipelineDegraded {
		dependency = status.EmbedderBusy
	}
	retryable := false
	errorMessage := ""
	if job.Error != nil {
		retryable = job.Error.Retryable
		errorMessage = strings.TrimSpace(job.Error.Message)
	}
	resolved := status.ResolveJob(status.JobInputs{
		State:             job.State,
		Retryable:         retryable,
		ErrorMessage:      errorMessage,
		Dependency:        dependency,
		SupersededByJobID: supersededByJobID,
	})
	return view.JobSurface{
		StateLabel:        resolved.StateLabel,
		ErrorLine:         resolved.ErrorLine,
		Superseded:        resolved.Superseded,
		SupersededByJobID: resolved.SupersededByJobID,
		Scheduling: resolveSchedulingView(
			job.EffectiveSchedulingPolicy,
			job.State,
			job.SchedulingReason,
		),
	}
}

func resolveSchedulingView(
	policy model.SchedulingPolicy,
	state model.JobState,
	reason model.SchedulingReason,
) view.SchedulingView {
	return view.ResolveScheduling(
		string(policy.Priority),
		policy.Quiet,
		policy.IdleAfterSeconds,
		string(state),
		view.SchedulingReason(reason),
	)
}

// resolveCodebaseFailure reduces a codebase's raw failure record into the
// render-facing failure view, the codebase-side mirror of resolveJobSurface. It
// is the only reader of codebase.LastFailedRun outside the lifecycle logic, kept
// here at the boundary rather than in the render layer the guard test holds free
// of raw failure reads.
func resolveCodebaseFailure(codebase model.Codebase) view.FailureSurface {
	if codebase.LastFailedRun == nil {
		return emptyFailureSurface()
	}
	return view.FailureSurface{
		HasFailure:    true,
		Message:       codebase.LastFailedRun.Message,
		FailedAtLabel: formatBoundaryTime(codebase.LastFailedRun.FailedAt),
		JobID:         codebase.LastFailedRun.JobID,
		TraceID:       codebase.LastFailedRun.TraceID,
	}
}

func emptyFailureSurface() view.FailureSurface {
	return view.FailureSurface{
		HasFailure:    false,
		Message:       "",
		FailedAtLabel: "",
		JobID:         "",
		TraceID:       "",
	}
}

func resolveQuarantineSurface(codebase model.Codebase) view.QuarantineSurface {
	if codebase.Quarantine == nil {
		return view.QuarantineSurface{
			HasQuarantine:      false,
			Reason:             "",
			FirstObservedLabel: "",
			LastObservedLabel:  "",
			ObservationCount:   0,
			MissingCount:       0,
			TotalCount:         0,
			Trigger:            "",
		}
	}
	return view.QuarantineSurface{
		HasQuarantine:      true,
		Reason:             codebase.Quarantine.Reason,
		FirstObservedLabel: formatBoundaryTime(codebase.Quarantine.FirstObservedAt),
		LastObservedLabel:  formatBoundaryTime(codebase.Quarantine.LastObservedAt),
		ObservationCount:   codebase.Quarantine.ObservationCount,
		MissingCount:       codebase.Quarantine.LastMissingCount,
		TotalCount:         codebase.Quarantine.LastTotalCount,
		Trigger:            codebase.Quarantine.LastTrigger,
	}
}

// resolveStatusView builds the template view for an active or ready codebase.
// It is the relocated body of the render-side builder, so the templates keep
// their exact output. templateName selects among preparing, building,
// incremental, idle, ready, and waiting.
func resolveStatusView(codebase model.Codebase, activeJob *model.Job, display displayStatus, waitLabel string) (view.StatusView, string) {
	statusView := blankStatusView(filepath.Base(codebase.CanonicalPath), formatStampWithRelative(codebase.UpdatedAt))
	statusView.Path = codebase.CanonicalPath
	statusView.Scheduling = resolveSchedulingView(
		codebase.SchedulingPolicy,
		"",
		"",
	)
	switch display {
	case displayDiscovered:
		// A discovered worktree is registered and watched but not yet built. The
		// reuse forecast is attached by resolveGetIndexView, which holds the manager
		// needed to compute it cheaply.
		return statusView, "discovered.md.tmpl"
	case displayPending:
		// A requested index whose build has not started running yet (a queued job
		// or a discovered worktree awaiting its deferred build). It is not a store
		// outage; the build will move it to indexing.
		statusView.PrepareLabel = prepareLabel(activeJob)
		return statusView, "pending.md.tmpl"
	case displayLoading:
		// This codebase's own collection is not loaded into query nodes yet while
		// the store itself is reachable. A per-path, self-healing condition, never
		// the global store banner.
		return statusView, "loading.md.tmpl"
	case displayIdle:
		return statusView, "idle.md.tmpl"
	case displayWaiting:
		statusView.WaitLabel = schedulingWaitLabel(activeJob, dependency, waitingLabel(dependency))
		return statusView, "waiting.md.tmpl"
	case displayIndexed:
		return resolveIndexedStatusView(statusView, codebase, activeJob)
	case displayQuarantined:
		if run := codebase.LastSuccessfulRun; run != nil {
			statusView.HasStats = true
			statusView.Files = run.IndexedFiles
			statusView.Chunks = run.TotalChunks
			statusView.SkippedLine = skippedFilesLine(run.SkippedFiles)
			statusView.UpdatedAt = formatStampWithRelative(run.CompletedAt)
		}
		return statusView, "ready.md.tmpl"
	}
	statusView.PrepareLabel = prepareLabel(activeJob)
	embedding := false
	if activeJob != nil {
		progress := activeJob.Progress
		if !progress.LastEventAt.IsZero() {
			statusView.UpdatedAt = formatStampWithRelative(progress.LastEventAt)
		}
		changed := progress.FilesAdded + progress.FilesModified + progress.FilesRemoved
		statusView.FilesInCodebase = progress.FilesInCodebase
		statusView.FilesChanged = changed
		statusView.FilesUnchanged = max(progress.FilesInCodebase-changed, 0)
		statusView.Heading = headingFor(codebase, activeJob)
		// The collection still holds the prior run's chunks until this run
		// replaces them, so when the live count has not arrived yet the chunk
		// tree shows the standing total rather than a momentary zero. The floor
		// is folded into the progress the shared resolver reads, so the status
		// tree stays byte-identical to the compact job tree once live chunks
		// arrive.
		chunkProgress := progress
		chunkProgress.ChunksTotal = max(progress.ChunksTotal, progress.ChunksReused+progress.ChunksGenerated)
		if chunkProgress.ChunksTotal == 0 {
			chunkProgress.ChunksTotal = codebase.LiveChunkTotal
		}
		if chunkProgress.ChunksTotal == 0 && codebase.LastSuccessfulRun != nil {
			chunkProgress.ChunksTotal = codebase.LastSuccessfulRun.TotalChunks
		}
		statusView.Breakdown = resolveOutcomeBreakdown(chunkProgress)
		// The headline percent reads over the corpus-floored total so a
		// superseded-and-restarted run reflects how much of the corpus is
		// already embedded rather than this run's reset file cursor. The floor
		// is the codebase live total, matching the chunk tree above.
		statusView.Percent = int32(resolveOverallPercent(chunkProgress) + 0.5)
		embedding = progress.FilesTotal > 0 || progress.FilesInCodebase > 0
	}
	if !embedding {
		return statusView, "preparing.md.tmpl"
	}
	if activeJob != nil && jobOperation(activeJob.Operation) == jobOperationIndex {
		return statusView, "building.md.tmpl"
	}
	return statusView, "incremental.md.tmpl"
}

func resolveIndexedStatusView(
	statusView view.StatusView,
	codebase model.Codebase,
	activeJob *model.Job,
) (view.StatusView, string) {
	statusView.RawStatus = string(codebase.Status)
	if run := codebase.LastSuccessfulRun; run != nil {
		statusView.HasStats = true
		statusView.Files = run.IndexedFiles
		statusView.Chunks = run.TotalChunks
		statusView.LastSuccessfulRunCompletedAt = formatBoundaryStatusTime(run.CompletedAt)
		statusView.SkippedLine = skippedFilesLine(run.SkippedFiles)
		statusView.UpdatedAt = formatStampWithRelative(run.CompletedAt)
	}
	if activeJob != nil && isBackgroundSyncReconcile(&codebase, activeJob) {
		if !activeJob.Progress.LastEventAt.IsZero() {
			statusView.UpdatedAt = formatStampWithRelative(activeJob.Progress.LastEventAt)
		}
		statusView.SyncNote = backgroundSyncNote(activeJob)
	}
	return statusView, "ready.md.tmpl"
}

// blankStatusView returns a fully zeroed status view with only the name and
// timestamp set, so each caller fills the subset its template reads.
func blankStatusView(name string, updatedAt string) view.StatusView {
	return view.StatusView{
		Name:              name,
		Path:              "",
		HasStats:          false,
		Files:             0,
		Chunks:            0,
		SkippedLine:       "",
		PrepareLabel:      "",
		WaitLabel:         "",
		Percent:           0,
		Heading:           "",
		FilesInCodebase:   0,
		FilesChanged:      0,
		FilesUnchanged:    0,
		Breakdown:         view.ZeroBreakdown(),
		ReuseForecastLine: "",
		RawStatus:         "",
		CurrentIndex: view.CurrentIndexCounts{
			IndexedFiles: nil,
			TotalChunks:  nil,
		},
		UpdatedAt:                    updatedAt,
		Scheduling:                   view.ZeroScheduling(),
		LastSuccessfulRunCompletedAt: "",
		SyncNote:                     "",
		GraphUpdatedAt:               "",
		GraphBuilding:                false,
		GraphFailed:                  false,
		GraphReadyNoTime:             false,
		GraphNotBuilt:                false,
	}
}

func schedulingWaitLabel(activeJob *model.Job, dependency dependencyMode, fallback string) string {
	if activeJob == nil || activeJob.State != model.JobStatePaused || dependency != dependencyHealthy {
		return fallback
	}
	reason := resolveSchedulingView(
		activeJob.EffectiveSchedulingPolicy,
		activeJob.State,
		activeJob.SchedulingReason,
	).Reason
	if reason == "" {
		return "Paused"
	}
	return "Waiting: " + string(reason)
}

// formatBoundaryStatusTime renders a compact wall-clock time with zone for the
// status header, for example "4:52 PM PDT". The daemon stores UTC, so this
// converts to the host's zone through the shared lookup.
func formatBoundaryStatusTime(value time.Time) string {
	if value.IsZero() {
		return "unknown"
	}
	const layout = "3:04 PM MST"
	return render.InLocalZone(value).Format(layout)
}

func formatStatusTime(value time.Time) string {
	return formatBoundaryStatusTime(value)
}

func formatStampWithRelative(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	localValue := render.InLocalZone(value)
	localNow := render.InLocalZone(relativeTimeNow())
	layout := "3:04 PM MST"
	if localValue.Year() != localNow.Year() || localValue.YearDay() != localNow.YearDay() {
		layout = "Jan 2, 3:04 PM MST"
	}
	return localValue.Format(layout) + " (" + formatRelativeTime(value) + ")"
}

func formatRelativeTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	now := relativeTimeNow()
	if value.After(now) {
		return "just now"
	}
	elapsed := now.Sub(value)
	if elapsed < time.Minute {
		return "just now"
	}
	if elapsed < time.Hour {
		minutes := int(elapsed / time.Minute)
		return fmt.Sprintf("%dm ago", minutes)
	}
	if elapsed < 24*time.Hour {
		hours := int(elapsed / time.Hour)
		return fmt.Sprintf("%dh ago", hours)
	}
	days := int(elapsed / (24 * time.Hour))
	return fmt.Sprintf("%dd ago", days)
}

// waitingLabel names the dependency a waiting codebase is blocked on. The banner
// carries the exact cause and fix, so this stays a short, plain phrase.
func waitingLabel(mode dependencyMode) string {
	if mode == dependencyStoreUnavailable {
		return "Waiting for the vector store"
	}
	return "Waiting for the embedding server"
}

// isBackgroundSyncReconcile reports whether the active job is a background
// incremental sync over a codebase that already has a successful run. That
// delta writes to the live collection, so the index stays searchable while it
// runs and the ready view holds. A from-scratch "index" build (staging, not
// promoted) and a "streaming_reindex" rebuild keep their busy takeover.
func isBackgroundSyncReconcile(codebase *model.Codebase, job *model.Job) bool {
	return job != nil &&
		jobOperation(job.Operation) == jobOperationSync &&
		codebase.LastSuccessfulRun != nil
}

// backgroundSyncNote is the one-line note appended to the ready view while a
// background sync runs. Before the diff is captured the changed counts are
// zero, so it states only that a sync is underway.
func backgroundSyncNote(job *model.Job) string {
	progress := job.Progress
	changed := progress.FilesAdded + progress.FilesModified + progress.FilesRemoved
	if changed == 0 {
		// The watcher fired but the diff has not been computed yet, so the
		// changed-file count does not exist to show. Name the detection phase
		// honestly rather than calling it a sync of an unknown size.
		return "🔄 Checking for changes in the background"
	}
	percent := int32(progress.OverallPercent + 0.5)
	return fmt.Sprintf("🔄 Syncing %d changed %s in the background (%d%%)", changed, plural("file", int(changed)), percent)
}

// headingFor names what started an in-progress run so the building view leads
// with the trigger rather than the internal job path. A codebase with no
// completed run reads as a first build even when resuming a failed checkpoint;
// once a completed run exists, a forced or full reindex reads as a forced
// reindex, and anything else reads as indexing changed files.
func headingFor(codebase model.Codebase, job *model.Job) string {
	if codebase.LastSuccessfulRun == nil {
		return "Building initial index"
	}
	if job != nil && (jobOperation(job.Operation) == jobOperationIndex || job.Forced) {
		return "Forced reindex"
	}
	return "Indexing new changes"
}

// prepareLabel names the phase before embedding starts. A watcher-driven sync
// reaches this phase because a change was detected, so it says so; a full or
// forced reindex is just preparing.
func prepareLabel(job *model.Job) string {
	if job != nil && jobOperation(job.Operation) == jobOperationSync {
		return "Changes detected, preparing to index"
	}
	return "Preparing to index"
}

// skippedFilesLine formats the per-run skipped-file summary for the
// human-facing GetIndex view. The first few paths are listed inline so the
// operator can spot the culprits without grepping the daemon log.
func skippedFilesLine(skipped []string) string {
	if len(skipped) == 0 {
		return ""
	}
	const maxPreview = 3
	previewLimit := min(len(skipped), maxPreview)
	preview := strings.Join(skipped[:previewLimit], ", ")
	if len(skipped) > maxPreview {
		preview += fmt.Sprintf(", ... (+%d more)", len(skipped)-maxPreview)
	}
	return fmt.Sprintf("⏭️  Skipped: %d non-UTF-8 %s: %s", len(skipped), plural("file", len(skipped)), preview)
}

// jobScopeKnown reports whether the daemon has measured the work scope for a
// job. Before the scope is known a 0% reads as stalled rather than measured.
func jobScopeKnown(progress model.Progress) bool {
	return progress.FilesTotal > 0 || progress.FilesInCodebase > 0
}

// resolveGetIndexView assembles the full codebase status response view.
func (manager *Manager) resolveGetIndexView(
	ctx context.Context,
	requestedPath string,
	tracked bool,
	codebase *model.Codebase,
	activeJob *model.Job,
	health dependencyHealth,
	readiness status.CollectionReadiness,
	observedRows *int32,
	classification *model.PathClassification,
	descendants []model.Codebase,
) view.GetIndexView {
	getIndex := view.GetIndexView{
		Tracked:       tracked,
		RequestedPath: requestedPath,
		CanonicalPath: "",
		Display:       "",
		TemplateName:  "",
		Status:        blankStatusView("", ""),
		Failure:       emptyFailureSurface(),
		Quarantine: view.QuarantineSurface{
			HasQuarantine:      false,
			Reason:             "",
			FirstObservedLabel: "",
			LastObservedLabel:  "",
			ObservationCount:   0,
			MissingCount:       0,
			TotalCount:         0,
			Trigger:            "",
		},
		Narrative:          view.StatusNarrative{Lines: nil},
		WaitLabel:          "",
		ClassificationLine: classificationLine(classification),
		ResolutionLines:    pathResolutionLines(requestedPath),
		CoverageLine:       coveringResolutionLine(requestedPath, tracked, codebase),
		PathErrorLine:      "",
		DescendantsHint:    descendantsHint(requestedPath, descendants),
		SyncNote:           "",
	}
	if !tracked || codebase == nil {
		return getIndex
	}
	getIndex.CanonicalPath = codebase.CanonicalPath
	display := computeDisplayStatus(*codebase, activeJob, health.Mode, readiness)
	getIndex.Display = view.Display(display)
	getIndex.Failure = resolveCodebaseFailure(*codebase)
	getIndex.Quarantine = resolveQuarantineSurface(*codebase)
	statusView, templateName := resolveStatusView(*codebase, activeJob, display, waitingLabel(health.Mode))
	if display == displayIndexed {
		statusView.CurrentIndex = manager.currentIndexCounts(ctx, *codebase, observedRows)
	}
	if display == displayDiscovered {
		statusView.ReuseForecastLine = reuseForecastLine(manager.worktreeReuseForecast(*codebase))
	}
	resolveGraphStatusFields(&statusView, *codebase, manager.graphIndexing(codebase.ID))
	getIndex.Status = statusView
	getIndex.TemplateName = templateName
	getIndex.Narrative = resolveStatusNarrative(display, codebase.CanonicalPath, readiness, getIndex.Failure, getIndex.Quarantine, statusView)
	return getIndex
}

func mcpMissingPathError(clientName string, requestedPath string) string {
	if clientName != "mcp" {
		return ""
	}
	_, err := filepath.EvalSymlinks(requestedPath)
	if !errors.Is(err, os.ErrNotExist) {
		return ""
	}
	return err.Error()
}

func (manager *Manager) currentIndexCounts(
	ctx context.Context,
	codebase model.Codebase,
	observedRows *int32,
) view.CurrentIndexCounts {
	if ranWithoutCreatingACollection(codebase.LastSuccessfulRun) {
		indexedFiles := int32(0)
		totalChunks := int32(0)
		return view.CurrentIndexCounts{
			IndexedFiles: &indexedFiles,
			TotalChunks:  &totalChunks,
		}
	}

	counts := view.CurrentIndexCounts{
		IndexedFiles: nil,
		TotalChunks:  nil,
	}
	checkpoint := manager.loadLiveCheckpoint(ctx, codebase, codebase.EffectiveConfig.IgnoreDigest)
	if checkpoint.usable() {
		indexedFiles := safeInt32(len(checkpoint.snapshot.Files))
		counts.IndexedFiles = &indexedFiles
	}
	if ownsLiveCollection(codebase) && observedRows != nil {
		counts.TotalChunks = observedRows
	}
	return counts
}

func resolveGraphStatusFields(statusView *view.StatusView, codebase model.Codebase, graphBuilding bool) {
	if codebase.Kind == model.CodebaseKindDocument {
		return
	}
	if graphBuilding {
		statusView.GraphBuilding = true
		return
	}
	if codebase.GraphState == model.GraphStateStale {
		statusView.GraphFailed = true
		return
	}
	if !codebase.GraphUpdatedAt.IsZero() {
		statusView.GraphUpdatedAt = formatStampWithRelative(codebase.GraphUpdatedAt)
		return
	}
	if codebase.GraphState == model.GraphStateReady {
		statusView.GraphReadyNoTime = true
		return
	}
	statusView.GraphNotBuilt = true
}

// reuseForecastLine renders the discovered-worktree reuse forecast, or empty
// when the worktree has no eligible sibling to reuse from. The count is a sibling
// collection count, computed without a vector-store call.
func reuseForecastLine(siblingCount int32) string {
	if siblingCount <= 0 {
		return ""
	}
	return fmt.Sprintf("♻️ reuses embeddings from %d indexed sibling %s", siblingCount, plural("worktree", int(siblingCount)))
}

// descendantsHint replaces the bare not-indexed message for a path that already
// has indexed sub-folders. It names the sub-folders, totals their indexed files,
// and points at the one command that builds a merged parent index reusing their
// embeddings.
func descendantsHint(requestedPath string, descendants []model.Codebase) string {
	if len(descendants) == 0 {
		return ""
	}
	var totalFiles int32
	names := make([]string, 0, len(descendants))
	for _, child := range descendants {
		names = append(names, child.CanonicalPath)
		if child.LastSuccessfulRun != nil {
			totalFiles += child.LastSuccessfulRun.IndexedFiles
		}
	}
	fileCount := int(totalFiles)
	return fmt.Sprintf(
		"🛈 '%s' is not indexed on its own, but %d already-indexed %s live under %s: %s\n"+
			"Build one merged index that reuses those embeddings by running: index_codebase %s",
		requestedPath, fileCount, plural("file", fileCount), plural("sub-folder", len(names)), strings.Join(names, ", "), requestedPath,
	)
}

// coveringResolutionLine names the larger index a nested query resolved to,
// scoped to the sub-path, so the operator sees that a sub-folder query is served
// by the covering parent index rather than a separate one.
func coveringResolutionLine(requestedPath string, tracked bool, codebase *model.Codebase) string {
	if !tracked || codebase == nil {
		return ""
	}
	prefix := subtreePrefix(requestedPath, codebase.CanonicalPath)
	if prefix == "" {
		return ""
	}
	return fmt.Sprintf("🔁 Resolved to larger index '%s' (scoped to %s/).", codebase.CanonicalPath, prefix)
}

// classificationLine renders a one-line summary of the per-path classification
// verdict. Returns an empty string when the verdict adds no useful information
// beyond what the body already conveys.
func classificationLine(classification *model.PathClassification) string {
	if classification == nil {
		return ""
	}
	switch classification.Kind {
	case model.PathClassificationInScopeExcluded:
		parts := make([]string, 0, 2)
		if classification.ExcludedByGitignore != "" {
			parts = append(parts, "gitignore="+classification.ExcludedByGitignore)
		}
		if classification.ExcludedByPattern != "" {
			parts = append(parts, "pattern="+classification.ExcludedByPattern)
		}
		if len(parts) == 0 {
			return "🚫 Path is in scope of " + classification.CoveringCodebaseID + " but excluded by an ignore rule."
		}
		return "🚫 Path is in scope of " + classification.CoveringCodebaseID + " but excluded: " + strings.Join(parts, " ")
	case model.PathClassificationOutOfScope:
		return "🛈 Path is not under any tracked codebase."
	case model.PathClassificationInScopeUnindexed:
		return "🛈 Path is in scope of " + classification.CoveringCodebaseID + " but has no chunk row yet."
	case model.PathClassificationInScopeIndexed, model.PathClassificationUnspecified:
		return ""
	default:
		return ""
	}
}

// pathResolutionLines returns the identity lines for a queried path: the real
// path a symlink resolves to and the git worktree relation, in that order,
// omitting any that do not apply. Both status and search show these so a caller
// always sees that a path is a worktree of a parent repo, even when the index is
// idle.
func pathResolutionLines(requestedPath string) []string {
	lines := make([]string, 0, 2)
	if symlinkLine := renderSymlinkResolution(requestedPath); symlinkLine != "" {
		lines = append(lines, symlinkLine)
	}
	if worktreeLine := renderWorktreeRelation(requestedPath); worktreeLine != "" {
		lines = append(lines, worktreeLine)
	}
	return lines
}

// renderSymlinkResolution names the real path a symlinked query path resolves
// to, or returns an empty string when the query path traverses no symlink. A
// codebase's identity is the resolved real path, so when the caller passes a
// symlink this line states which real directory it points at.
func renderSymlinkResolution(requestedPath string) string {
	// Only an absolute argument is a path the note can describe. An id-shaped
	// or relative argument must not resolve against the daemon's own working
	// directory, which is never the caller's.
	if !filepath.IsAbs(strings.TrimSpace(requestedPath)) {
		return ""
	}
	absolutePath := filepath.Clean(requestedPath)
	resolved, err := filepath.EvalSymlinks(absolutePath)
	if err != nil || resolved == absolutePath {
		return ""
	}
	return "🔗 symlink resolved to: " + resolved
}

// renderWorktreeRelation names the main checkout and branch a linked worktree
// belongs to, so the operator sees that this index is one branch of a shared
// repository. It returns an empty string for the main worktree and for a non-git
// path.
func renderWorktreeRelation(requestedPath string) string {
	// Only an absolute argument is a path the note can describe. An id-shaped
	// or relative argument must not resolve against the daemon's own working
	// directory, which is never the caller's.
	if !filepath.IsAbs(strings.TrimSpace(requestedPath)) {
		return ""
	}
	absolutePath := filepath.Clean(requestedPath)
	info, ok := gitworktree.Resolve(absolutePath)
	if !ok || !info.Linked {
		return ""
	}
	mainCheckout := filepath.Dir(info.CommonDir)
	if info.Detached {
		return fmt.Sprintf("🌿 git worktree of %s (detached HEAD %s)", mainCheckout, info.Head)
	}
	return fmt.Sprintf("🌿 git worktree of %s (branch %s)", mainCheckout, info.Branch)
}

// resolveSearchResults reduces stored chunks to the view shape.
func resolveSearchResults(chunks []model.StoredChunk) []view.SearchResultView {
	results := make([]view.SearchResultView, 0, len(chunks))
	for _, chunk := range chunks {
		results = append(results, view.SearchResultView{
			RelativePath: chunk.RelativePath,
			StartLine:    chunk.StartLine,
			EndLine:      chunk.EndLine,
			Language:     chunk.Language,
			Score:        chunk.Score,
			Content:      chunk.Content,
		})
	}
	return results
}

// resolveConversationSearchResults reduces stored conversation chunks to the
// view shape.
func resolveConversationSearchResults(chunks []model.StoredChunk) []view.ConversationResultView {
	results := make([]view.ConversationResultView, 0, len(chunks))
	for _, chunk := range chunks {
		results = append(results, view.ConversationResultView{
			ConversationID: chunk.ConversationID,
			MessageIndex:   chunk.MessageIndex,
			Role:           chunk.Role,
			TimestampUnix:  chunk.TimestampUnix,
			Score:          chunk.Score,
			Content:        chunk.Content,
		})
	}
	return results
}

// resolveSearchStatusView builds the optional in-flight status portion for a
// search response.
func resolveSearchStatusView(codebase model.Codebase, activeJob *model.Job, health dependencyHealth, graphBuilding bool) (view.StatusView, string, bool) {
	if activeJob == nil {
		return blankStatusView("", ""), "", false
	}
	display := computeDisplayStatus(codebase, activeJob, health.Mode, status.CollectionNotApplicable)
	statusView, templateName := resolveStatusView(codebase, activeJob, display, waitingLabel(health.Mode))
	resolveGraphStatusFields(&statusView, codebase, graphBuilding)
	return statusView, templateName, isBackgroundSyncReconcile(&codebase, activeJob)
}

// resolveStartIndexView assembles the start acknowledgment including the merge
// note relocated from grpc_server.startIndexMergeNote.
func (server *GRPCServer) resolveStartIndexView(
	requestedPath string,
	codebase model.Codebase,
	job model.Job,
	deduplicated bool,
	overlapsCodebaseID string,
) view.StartIndexView {
	return view.StartIndexView{
		RequestedPath:      requestedPath,
		CanonicalPath:      codebase.CanonicalPath,
		CodebaseID:         codebase.ID,
		JobID:              job.ID,
		SplitterType:       job.Config.SplitterType,
		Deduplicated:       deduplicated,
		OverlapsCodebaseID: overlapsCodebaseID,
		MergeNote:          server.startIndexMergeNote(requestedPath, codebase),
	}
}

// resolveCancelJobAck builds the cancel acknowledgment while preserving the
// current cancelled-vs-terminal wording split.
func resolveCancelJobAck(job model.Job) view.MutationAckView {
	return view.MutationAckView{
		Kind:            view.AckCancel,
		Path:            "",
		JobID:           job.ID,
		StateLabel:      status.JobStateLabelFor(job.State),
		AlreadyTerminal: job.State != model.JobStateCancelled,
		Deduplicated:    false,
		CollectionID:    "",
		CollectionName:  "",
		CodebaseID:      job.CodebaseID,
		ConversationID:  "",
		DocumentCount:   0,
		NeededCount:     0,
		TotalCount:      0,
	}
}

// isTerminalJobState reports whether a job state is terminal (no further work).
// The successor chain links only terminal jobs, since an active job is not yet a
// recorded outcome in the ledger.
func isTerminalJobState(state model.JobState) bool {
	switch state {
	case model.JobStateCompleted, model.JobStateFailed, model.JobStateCancelled:
		return true
	case model.JobStateQueued, model.JobStateRunning, model.JobStatePaused, model.JobStateCancelling:
		return false
	default:
		return false
	}
}

// jobOrderTime returns the time a terminal job is ordered by: its completion
// time when set, else its start time.
func jobOrderTime(job model.Job) time.Time {
	if job.CompletedAt != nil {
		return *job.CompletedAt
	}
	return job.StartedAt
}

// buildJobSuccessors returns, for each terminal job id, the id of the immediate
// next terminal job for the same codebase, or no entry when it is the latest.
// Active jobs are excluded, so a failure whose only later attempt is still
// running has no successor until that attempt terminates. The chain is the basis
// for the superseded relationship: a failed job with a successor was overtaken.
func buildJobSuccessors(jobs []model.Job) map[string]string {
	byCodebase := make(map[string][]model.Job)
	for _, job := range jobs {
		if !isTerminalJobState(job.State) {
			continue
		}
		byCodebase[job.CodebaseID] = append(byCodebase[job.CodebaseID], job)
	}
	successors := make(map[string]string)
	for _, codebaseJobs := range byCodebase {
		sort.Slice(codebaseJobs, func(first int, second int) bool {
			timeFirst := jobOrderTime(codebaseJobs[first])
			timeSecond := jobOrderTime(codebaseJobs[second])
			if !timeFirst.Equal(timeSecond) {
				return timeFirst.Before(timeSecond)
			}
			return codebaseJobs[first].ID < codebaseJobs[second].ID
		})
		for index := 0; index+1 < len(codebaseJobs); index++ {
			successors[codebaseJobs[index].ID] = codebaseJobs[index+1].ID
		}
	}
	return successors
}
