package daemon

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/pbconv"
	render "goodkind.io/lm-semantic-search/internal/render"
	"goodkind.io/lm-semantic-search/internal/view"
	"google.golang.org/protobuf/encoding/protojson"
)

// treeLines keeps only the file-and-chunk tree lines (the breakdown block) from
// a rendered status or job output, raw and in order, so two surfaces can be
// compared byte-for-byte. A divergence in wording or indentation fails.
func treeLines(out string) []string {
	kept := []string{}
	for _, line := range strings.Split(out, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "📄") || strings.HasPrefix(trimmed, "🧩") ||
			strings.HasPrefix(trimmed, "├─") || strings.HasPrefix(trimmed, "└─") {
			kept = append(kept, line)
		}
	}
	return kept
}

// TestStatusTreeIdenticalAcrossSurfaces is the structural guard against
// diverging status output: the compact job surface and the codebase status
// surface must carry the same resolved tree and render it byte-identically, so
// a status can never read one way under `job get` and another under
// `get_indexing_status`.
func TestStatusTreeIdenticalAcrossSurfaces(t *testing.T) {
	t.Parallel()
	codebase := &model.Codebase{
		CanonicalPath:     "/Users/agoodkind/Sites/swift-makefile",
		LastSuccessfulRun: &model.IndexRunSummary{TotalChunks: 57240},
	}
	job := model.Job{
		ID:            "job_ident",
		CanonicalPath: codebase.CanonicalPath,
		State:         model.JobStateRunning,
		Operation:     "streaming_reindex",
		Progress: model.Progress{
			RunMode:        model.RunModeChanged,
			OverallPercent: 37, FilesTotal: 452, FilesProcessed: 290,
			FilesInCodebase: 4292, FilesAdded: 29, FilesModified: 423, FilesRemoved: 10,
			FilesEmbedded: 285, FilesSkippedOversize: 3, FilesSkippedUnreadable: 2,
			ChunksGenerated: 1043, ChunksTotal: 57240, LastEventAt: renderTestTime,
		},
	}
	compact := resolveProgressSurface(job).Breakdown
	status, _ := resolveStatusView(*codebase, &job, displayIndexing, "")
	if !reflect.DeepEqual(compact, status.Breakdown) {
		t.Fatalf("breakdown differs between surfaces:\ncompact=%+v\nstatus =%+v", compact, status.Breakdown)
	}
	jobTree := treeLines(render.GetJob(resolveJobEntry(job, false, ""), true))
	statusTree := treeLines(renderActiveStatusForTest(codebase, &job))
	if !reflect.DeepEqual(jobTree, statusTree) {
		t.Fatalf("rendered tree differs between surfaces:\njob:\n%s\nstatus:\n%s",
			strings.Join(jobTree, "\n"), strings.Join(statusTree, "\n"))
	}
	if len(jobTree) == 0 {
		t.Fatal("no tree lines extracted; the guard would pass vacuously")
	}
}

// TestBreakdownIdenticalAcrossAllSurfaces is the single-SOT guard. The daemon
// text, the proto breakdown rendered back (the TUI path), and the JSON-decoded
// breakdown rendered back must all equal the one render.BreakdownLines over the
// one view.ResolveBreakdown. Any surface that re-derives breaks this test.
func TestBreakdownIdenticalAcrossAllSurfaces(t *testing.T) {
	t.Parallel()
	progress := model.Progress{
		RunMode: model.RunModeChanged, Unit: "document", ScopeUnit: "conversation",
		FilesTotal: 72, FilesProcessed: 70, FilesAdded: 63, FilesModified: 9,
		FilesEmbedded: 9, FilesPending: 61,
		ChunksProcessed: 3516, ChunksGenerated: 1204, ChunksEmbedded: 1204,
		ChunksReused: 2312, ChunksTotal: 3516, ReuseVectorsLoaded: 2400,
		LastEventAt: renderTestTime,
	}

	// The single source of truth: one resolver, one renderer.
	want := render.BreakdownLines(view.ResolveBreakdown(pbconv.ProgressCounts(progress)))
	if len(want) == 0 {
		t.Fatal("breakdown produced no lines; the guard would pass vacuously")
	}

	job := model.Job{ID: "j", State: model.JobStateRunning, Operation: "conversation_ingest", Progress: progress}

	// Daemon compact text (job get / job list path).
	daemonText := treeLines(render.GetJob(resolveJobEntry(job, false, ""), true))
	if !reflect.DeepEqual(daemonText, want) {
		t.Fatalf("daemon text differs from SOT:\n%s\nwant\n%s", strings.Join(daemonText, "\n"), strings.Join(want, "\n"))
	}

	// Proto breakdown rendered back: the path the TUI uses from active_progress.
	pbJob := pbconv.ToJob(job)
	protoRender := render.BreakdownLines(pbconv.BreakdownFromProto(pbJob.GetProgress().GetBreakdown()))
	if !reflect.DeepEqual(protoRender, want) {
		t.Fatalf("proto-rendered breakdown differs from SOT:\n%s", strings.Join(protoRender, "\n"))
	}

	// JSON (--json) decoded and rendered back.
	data, err := protojson.Marshal(pbJob)
	if err != nil {
		t.Fatalf("protojson.Marshal returned error: %v", err)
	}
	var decoded pb.Job
	if err := protojson.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("protojson.Unmarshal returned error: %v", err)
	}
	jsonRender := render.BreakdownLines(pbconv.BreakdownFromProto(decoded.GetProgress().GetBreakdown()))
	if !reflect.DeepEqual(jsonRender, want) {
		t.Fatalf("JSON-decoded breakdown differs from SOT:\n%s", strings.Join(jsonRender, "\n"))
	}

	// The undelivered conversation is a first-class wire counter, not inferred.
	if got := decoded.GetProgress().GetBreakdown(); !breakdownHasPending(got) {
		t.Fatalf("JSON breakdown missing a pending row: %+v", got.GetFileRows())
	}
	if got := decoded.GetProgress().GetChunksProcessed(); got != 3516 {
		t.Fatalf("JSON chunks_processed = %d, want 3516", got)
	}
	if got := decoded.GetProgress().GetChunksEmbedded(); got != 1204 {
		t.Fatalf("JSON chunks_embedded = %d, want 1204", got)
	}
	if got := decoded.GetProgress().GetReuseVectorsLoaded(); got != 2400 {
		t.Fatalf("JSON reuse_vectors_loaded = %d, want 2400", got)
	}
}

func TestSchedulingIdenticalAcrossAllSurfaces(t *testing.T) {
	t.Parallel()

	policy := model.SchedulingPolicy{
		Priority:         model.JobPriorityLow,
		Quiet:            true,
		IdleAfterSeconds: 600,
	}
	want := "priority=low · quiet=on · idle_after=10m"
	scheduling := view.ResolveScheduling(
		string(policy.Priority),
		policy.Quiet,
		policy.IdleAfterSeconds,
		string(model.JobStatePaused),
		view.SchedulingReasonUserActive,
	)
	if got := render.SchedulingPolicy(scheduling); got != want {
		t.Fatalf("shared scheduling render = %q, want %q", got, want)
	}
	if scheduling.Reason != "user active" {
		t.Fatalf("shared scheduling reason = %q, want user active", scheduling.Reason)
	}

	job := model.Job{
		ID:                        "job-scheduling",
		CanonicalPath:             "/repo",
		Operation:                 "sync",
		State:                     model.JobStatePaused,
		EffectiveSchedulingPolicy: policy,
		SchedulingReason:          model.SchedulingReasonUserActive,
	}
	daemonText := render.GetJob(resolveJobEntry(job, false, ""), true)
	for _, expected := range []string{
		"State: paused",
		"Effective policy: " + want,
		"Waiting: user active",
	} {
		if !strings.Contains(daemonText, expected) {
			t.Fatalf("job display missing %q:\n%s", expected, daemonText)
		}
	}

	wire := pbconv.ToJob(job)
	if wire.GetSchedulingReason() != pb.SchedulingReason_SCHEDULING_REASON_USER_ACTIVE {
		t.Fatalf(
			"wire scheduling reason = %v, want user active",
			wire.GetSchedulingReason(),
		)
	}
	protoRender := render.SchedulingPolicy(pbconv.SchedulingFromProto(
		wire.GetEffectiveSchedulingPolicy(),
		wire.GetState(),
		wire.GetSchedulingReason(),
	))
	if protoRender != want {
		t.Fatalf("proto scheduling render = %q, want %q", protoRender, want)
	}

	data, err := protojson.Marshal(wire)
	if err != nil {
		t.Fatalf("protojson.Marshal returned error: %v", err)
	}
	if !strings.Contains(
		string(data),
		`"schedulingReason":"SCHEDULING_REASON_USER_ACTIVE"`,
	) {
		t.Fatalf("JSON job omitted the canonical reason enum: %s", data)
	}
	var decoded pb.Job
	if err := protojson.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("protojson.Unmarshal returned error: %v", err)
	}
	if decoded.GetEffectiveSchedulingPolicy() == nil {
		t.Fatalf("JSON job omitted structured effective policy: %s", data)
	}
	jsonRender := render.SchedulingPolicy(pbconv.SchedulingFromProto(
		decoded.GetEffectiveSchedulingPolicy(),
		decoded.GetState(),
		decoded.GetSchedulingReason(),
	))
	if jsonRender != want {
		t.Fatalf("JSON scheduling render = %q, want %q", jsonRender, want)
	}
}

func TestStoredSchedulingPolicyRendersInCodebaseStatusAndList(t *testing.T) {
	t.Parallel()

	codebase := &model.Codebase{
		ID:            "cb-scheduling",
		CanonicalPath: "/repo",
		Status:        model.CodebaseStatusIndexed,
		SchedulingPolicy: model.SchedulingPolicy{
			Priority:         model.JobPriorityHigh,
			Quiet:            false,
			IdleAfterSeconds: 300,
		},
	}
	want := "priority=high · quiet=off · idle_after=5m"
	statusText := renderReadyStatusForTest(codebase)
	if !strings.Contains(statusText, "Stored policy: "+want) {
		t.Fatalf("codebase status missing stored policy:\n%s", statusText)
	}

	scheduling := view.ResolveScheduling(
		string(codebase.SchedulingPolicy.Priority),
		codebase.SchedulingPolicy.Quiet,
		codebase.SchedulingPolicy.IdleAfterSeconds,
		"",
		view.SchedulingReasonUnspecified,
	)
	listText := render.ListIndexes([]view.CodebaseRowView{{
		ID:            codebase.ID,
		CanonicalPath: codebase.CanonicalPath,
		Display:       view.Display(displayIndexed),
		Scheduling:    scheduling,
	}})
	if !strings.Contains(listText, "stored policy: "+want) {
		t.Fatalf("codebase list missing stored policy:\n%s", listText)
	}

	data, err := protojson.Marshal(pbconv.ToCodebase(*codebase))
	if err != nil {
		t.Fatalf("protojson.Marshal returned error: %v", err)
	}
	var decoded pb.Codebase
	if err := protojson.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("protojson.Unmarshal returned error: %v", err)
	}
	if decoded.GetSchedulingPolicy() == nil {
		t.Fatalf("JSON codebase omitted structured stored policy: %s", data)
	}
	jsonRender := render.SchedulingPolicy(pbconv.SchedulingFromProto(
		decoded.GetSchedulingPolicy(),
		"",
		pb.SchedulingReason_SCHEDULING_REASON_UNSPECIFIED,
	))
	if jsonRender != want {
		t.Fatalf("JSON stored scheduling render = %q, want %q", jsonRender, want)
	}
}

func TestPausedCodebaseStatusShowsSchedulingReason(t *testing.T) {
	t.Parallel()

	codebase := model.Codebase{
		CanonicalPath:    "/repo",
		Status:           model.CodebaseStatusIndexing,
		SchedulingPolicy: model.DefaultSchedulingPolicy(),
	}
	job := model.Job{
		State:                     model.JobStatePaused,
		EffectiveSchedulingPolicy: model.DefaultSchedulingPolicy(),
		SchedulingReason:          model.SchedulingReasonThermalSafety,
	}
	display := computeDisplayStatus(
		codebase,
		&job,
		dependencyHealthy,
		collectionNotApplicable,
	)
	if display != displayWaiting {
		t.Fatalf("paused codebase display = %q, want %q", display, displayWaiting)
	}
	statusView, templateName := resolveStatusView(
		codebase,
		&job,
		display,
		dependencyHealthy,
	)
	text := render.GetIndex(view.GetIndexView{
		Tracked:      true,
		Display:      view.Display(display),
		TemplateName: templateName,
		Status:       statusView,
	})
	if !strings.Contains(text, "Waiting: thermal safety") {
		t.Fatalf("paused codebase status missing reason:\n%s", text)
	}
}

func TestRenderListJobsCountsPausedSeparately(t *testing.T) {
	t.Parallel()

	text := renderListJobsForTest([]model.Job{{
		ID:                        "job-paused",
		CanonicalPath:             "/repo",
		Operation:                 "sync",
		State:                     model.JobStatePaused,
		EffectiveSchedulingPolicy: model.DefaultSchedulingPolicy(),
		SchedulingReason:          model.SchedulingReasonHigherPriorityWork,
	}}, false)
	for _, want := range []string{
		"Active: 0 queued, 0 running, 1 paused, 0 canceling",
		"[paused",
		"Waiting: higher-priority work",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("paused job list missing %q:\n%s", want, text)
		}
	}
}

// breakdownHasPending reports whether the proto breakdown carries a pending row.
func breakdownHasPending(breakdown *pb.OutcomeBreakdown) bool {
	for _, row := range breakdown.GetFileRows() {
		if row.GetKind() == pb.OutcomeKind_OUTCOME_KIND_PENDING {
			return true
		}
	}
	return false
}

// TestRestartedRunReadsMostlyDoneNotNearZero is the core fix: a run whose file
// cursor reset near zero after being superseded and restarted, but whose reused
// work covers most of the corpus, reads as mostly done on every surface. The
// compact job view, the codebase status view, and the wire all report the same
// corpus-completion percent rather than the reset file-cursor percent.
func TestRestartedRunReadsMostlyDoneNotNearZero(t *testing.T) {
	t.Parallel()
	codebase := &model.Codebase{
		CanonicalPath:  "/Users/agoodkind/Sites/swift-makefile",
		LiveChunkTotal: 40000,
	}
	job := model.Job{
		ID:            "job_restart",
		CanonicalPath: codebase.CanonicalPath,
		State:         model.JobStateRunning,
		Operation:     "streaming_reindex",
		Progress: model.Progress{
			RunMode:        model.RunModeResuming,
			OverallPercent: 2.3,
			FilesTotal:     4292, FilesProcessed: 50, FilesInCodebase: 4292,
			FilesModified: 4292,
			ChunksReused:  37800, ChunksEmbedded: 200, ChunksTotal: 40000,
			LastEventAt: renderTestTime,
		},
	}

	// The compact job surface leads with the corpus-completion percent (38,000 of
	// 40,000 chunks present = 95.0%), not the reset 2.3% file cursor.
	surface := resolveProgressSurface(job)
	if surface.PercentLabel != "95.0%" {
		t.Fatalf("compact percent = %q, want 95.0%% (reused work counted as done)", surface.PercentLabel)
	}
	if surface.PercentScopeLabel != "95.0% run progress" {
		t.Fatalf("scoped percent = %q, want 95.0%% run progress", surface.PercentScopeLabel)
	}
	jobOut := render.GetJob(resolveJobEntry(job, false, ""), true)
	// An exact line, not a prefix: a prefix match is what let "Progress: 95.0%
	// run progress" ship, because the doubled label still satisfied it.
	if !hasExactLine(jobOut, "📊 Run progress: 95.0%") {
		t.Fatalf("job view did not read as mostly done:\n%s", jobOut)
	}
	if strings.Contains(jobOut, "2.3%") {
		t.Fatalf("job view still shows the reset file-cursor percent:\n%s", jobOut)
	}

	// The codebase status view reads the same corpus completion.
	statusOut := renderActiveStatusForTest(codebase, &job)
	if !strings.Contains(statusOut, "🔄 Indexing new changes: 95%") {
		t.Fatalf("status view did not read as mostly done:\n%s", statusOut)
	}

	// The wire carries the same resolved percent, so the TUI renders identically.
	wire := pbconv.ToJob(job).GetProgress().GetOverallPercent()
	if wire != 95.0 {
		t.Fatalf("wire OverallPercent = %.4f, want 95.0 (parity with the human surface)", wire)
	}

	// The corpus total stays visible in the chunk tree (codebase-level accumulation).
	if !strings.Contains(statusOut, "🧩 40,000 chunks processed") {
		t.Fatalf("status view lost the corpus chunk total:\n%s", statusOut)
	}
}

// TestReuseHeavyRunSurfacesReusedAsProgress proves a run that embeds little but
// reuses most of the corpus reads as mostly done, so the reused work is progress
// in the headline rather than being invisible behind a low embed percent.
func TestReuseHeavyRunSurfacesReusedAsProgress(t *testing.T) {
	t.Parallel()
	job := model.Job{
		ID:            "job_reuse",
		CanonicalPath: "/repo",
		State:         model.JobStateRunning,
		Operation:     "conversation_ingest",
		Progress: model.Progress{
			RunMode:        model.RunModeChanged,
			OverallPercent: 5.0,
			FilesTotal:     1011, FilesProcessed: 40,
			ChunksReused: 30000, ChunksEmbedded: 1000, ChunksTotal: 40000,
			LastEventAt: renderTestTime,
		},
	}
	surface := resolveProgressSurface(job)
	if surface.PercentLabel != "77.5%" {
		t.Fatalf("reuse-heavy percent = %q, want 77.5%% (31,000 of 40,000 present)", surface.PercentLabel)
	}
	if wire := pbconv.ToJob(job).GetProgress().GetOverallPercent(); wire != 77.5 {
		t.Fatalf("wire OverallPercent = %.4f, want 77.5 (parity)", wire)
	}
}

// TestStatusTreeMatchesSessionCases locks the two real cases from the design
// session to their exact trees: a code delta and a conversation ingest. The
// file rows sum to the processed count in both.
func TestStatusTreeMatchesSessionCases(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		job  model.Job
		want []string
	}{
		{
			name: "code delta 1 of 2",
			job: model.Job{
				State: model.JobStateRunning, Operation: "streaming_reindex",
				Progress: model.Progress{
					RunMode: model.RunModeChanged, Unit: "file",
					FilesTotal: 2, FilesProcessed: 1, FilesAdded: 2, FilesEmbedded: 1,
					ChunksGenerated: 778, ChunksTotal: 778, LastEventAt: renderTestTime,
				},
			},
			want: []string{
				"📄 1 of 2 changed files processed",
				"└─ ➕ 1 embedded",
				"🧩 778 chunks processed",
				"├─ ➕ 778 added",
				"└─ ♻️ 0 reused",
			},
		},
		{
			name: "conversation ingest 70 of 72",
			job: model.Job{
				State: model.JobStateRunning, Operation: "conversation_ingest",
				Progress: model.Progress{
					RunMode: model.RunModeChanged, Unit: "document", ScopeUnit: "conversation",
					FilesTotal: 72, FilesProcessed: 70, FilesAdded: 63, FilesModified: 9,
					FilesEmbedded: 9, FilesPending: 61,
					ChunksGenerated: 1204, ChunksReused: 2312, ChunksTotal: 3516, LastEventAt: renderTestTime,
				},
			},
			want: []string{
				"📄 70 of 72 changed documents processed",
				"├─ ➕ 9 embedded",
				"└─ ⏳ 61 pending, not sent yet",
				"🧩 3,516 chunks processed",
				"├─ ➕ 1,204 added",
				"└─ ♻️ 2,312 reused",
			},
		},
	}
	for _, testCase := range cases {
		breakdown := resolveProgressSurface(testCase.job).Breakdown
		if sum := sumRowCounts(breakdown.FileRows); sum != breakdown.Processed {
			t.Fatalf("%s: file rows sum to %d, want processed %d", testCase.name, sum, breakdown.Processed)
		}
		got := treeLines(render.GetJob(resolveJobEntry(testCase.job, false, ""), true))
		want := testCase.want
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s tree =\n%s\nwant\n%s", testCase.name, strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
	}
}

var renderTestTime = time.Unix(1700000000, 0)

func renderListJobsForTest(jobs []model.Job, degraded bool) string {
	successors := buildJobSuccessors(jobs)
	summary := resolveListSummary(jobs, degraded)
	active := make([]view.JobEntryView, 0, len(jobs))
	terminal := make([]view.JobEntryView, 0, len(jobs))
	for _, job := range jobs {
		entry := resolveJobEntry(job, degraded, successors[job.ID])
		if isTerminalJobState(job.State) {
			terminal = append(terminal, entry)
		} else {
			active = append(active, entry)
		}
	}
	return render.ListJobs(summary, active, terminal)
}

func renderStatusForTest(codebase *model.Codebase, activeJob *model.Job, display displayStatus) string {
	statusView, templateName := resolveStatusView(*codebase, activeJob, display, waitingLabel(dependencyHealthy))
	return render.GetIndex(view.GetIndexView{
		Tracked:      true,
		TemplateName: templateName,
		Status:       statusView,
	})
}

func renderActiveStatusForTest(codebase *model.Codebase, activeJob *model.Job) string {
	return renderStatusForTest(codebase, activeJob, displayIndexing)
}

func renderReadyStatusForTest(codebase *model.Codebase) string {
	return renderStatusForTest(codebase, nil, displayIndexed)
}

func renderGetIndexBodyForTest(requestedPath string, tracked bool, codebase *model.Codebase, activeJob *model.Job, health dependencyHealth) string {
	getIndex := view.GetIndexView{
		Tracked:            tracked,
		RequestedPath:      requestedPath,
		CanonicalPath:      "",
		Display:            "",
		TemplateName:       "",
		Status:             view.StatusView{},
		Failure:            view.FailureSurface{},
		Quarantine:         view.QuarantineSurface{},
		WaitLabel:          "",
		ClassificationLine: "",
		ResolutionLines:    nil,
		CoverageLine:       "",
		PathErrorLine:      "",
		DescendantsHint:    "",
		SyncNote:           "",
	}
	if tracked && codebase != nil {
		getIndex.CanonicalPath = codebase.CanonicalPath
		display := computeDisplayStatus(*codebase, activeJob, health.Mode, collectionNotApplicable)
		getIndex.Display = view.Display(display)
		getIndex.Failure = resolveCodebaseFailure(*codebase)
		getIndex.Quarantine = resolveQuarantineSurface(*codebase)
		statusView, templateName := resolveStatusView(*codebase, activeJob, display, waitingLabel(health.Mode))
		getIndex.Status = statusView
		getIndex.TemplateName = templateName
		getIndex.Narrative = resolveStatusNarrative(display, codebase.CanonicalPath, collectionNotApplicable, getIndex.Failure, getIndex.Quarantine, statusView)
	}
	return render.GetIndex(getIndex)
}

// TestRenderSymlinkResolution proves the status output names the real path a
// symlinked query path resolves to, and adds nothing for a non-symlink path.
func TestRenderSymlinkResolution(t *testing.T) {
	t.Parallel()
	// Resolve the temp dir first: on macOS t.TempDir lives under /var, itself a
	// symlink to /private/var, so the resolved form is the true non-symlink path.
	realRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve temp dir: %v", err)
	}
	link := filepath.Join(filepath.Dir(realRoot), "codebase-link")
	if err := os.Symlink(realRoot, link); err != nil {
		t.Fatalf("create symlink: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(link) })

	if got := renderSymlinkResolution(link); got != "🔗 symlink resolved to: "+realRoot {
		t.Fatalf("symlink path: got %q, want resolution to %q", got, realRoot)
	}
	if got := renderSymlinkResolution(realRoot); got != "" {
		t.Fatalf("non-symlink path should add no line, got %q", got)
	}
}

// TestRenderIndexedDetailReady proves the ready status leads with the repo
// title, states readiness, and shows the standing index totals.
func TestRenderIndexedDetailReady(t *testing.T) {
	t.Parallel()
	codebase := &model.Codebase{
		CanonicalPath: "/Users/agoodkind/Sites/swift-makefile",
		Status:        model.CodebaseStatusIndexed,
		UpdatedAt:     renderTestTime,
		LastSuccessfulRun: &model.IndexRunSummary{
			IndexedFiles: 58,
			TotalChunks:  600,
			Status:       "completed",
			CompletedAt:  renderTestTime,
		},
	}
	out := renderReadyStatusForTest(codebase)
	for _, want := range []string{
		"📁 swift-makefile",
		"✅ Ready to search",
		"codebase.status: indexed",
		"current_index.indexed_files: null",
		"current_index.total_chunks: null",
		"last_successful_run.indexed_files: 58",
		"last_successful_run.total_chunks: 600",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("ready status missing %q in:\n%s", want, out)
		}
	}
}

// TestRenderIndexingActivePreparingSync proves a watcher-driven sync that has
// not started embedding reads as "Changes detected, preparing to index".
func TestRenderIndexingActivePreparingSync(t *testing.T) {
	t.Parallel()
	codebase := &model.Codebase{CanonicalPath: "/Users/agoodkind/Sites/swift-makefile"}
	job := &model.Job{Operation: "sync", Progress: model.Progress{FilesTotal: 0, LastEventAt: renderTestTime}}
	out := renderActiveStatusForTest(codebase, job)
	if !strings.Contains(out, "📁 swift-makefile") {
		t.Fatalf("missing title in:\n%s", out)
	}
	if !strings.Contains(out, "⚙️ Changes detected, preparing to index") {
		t.Fatalf("expected changes-detected prepare line in:\n%s", out)
	}
	if strings.Contains(out, "🔄 Indexing") {
		t.Fatalf("did not expect indexing line during prepare in:\n%s", out)
	}
}

// TestRenderIndexingActivePreparingForced proves a full or forced reindex that
// has not started embedding reads as a plain "Preparing to index".
func TestRenderIndexingActivePreparingForced(t *testing.T) {
	t.Parallel()
	codebase := &model.Codebase{CanonicalPath: "/Users/agoodkind/Sites/swift-makefile"}
	job := &model.Job{Operation: "index", Progress: model.Progress{FilesTotal: 0}}
	out := renderActiveStatusForTest(codebase, job)
	if !strings.Contains(out, "⚙️ Preparing to index") {
		t.Fatalf("expected plain prepare line in:\n%s", out)
	}
	if strings.Contains(out, "Changes detected") {
		t.Fatalf("did not expect changes-detected for a forced reindex in:\n%s", out)
	}
}

// TestRenderIndexingActiveBuilding proves a cold from-scratch build reads as
// "Building initial index" with the percent, files embedded, and chunks so far.
func TestRenderIndexingActiveBuilding(t *testing.T) {
	t.Parallel()
	codebase := &model.Codebase{CanonicalPath: "/Users/agoodkind/Sites/swift-makefile"}
	job := &model.Job{
		Operation: "index",
		Progress:  model.Progress{RunMode: model.RunModeFirstBuild, OverallPercent: 42, FilesTotal: 58, FilesProcessed: 24, FilesEmbedded: 24, ChunksGenerated: 71, LastEventAt: renderTestTime},
	}
	out := renderActiveStatusForTest(codebase, job)
	for _, want := range []string{"📁 swift-makefile", "🔄 Building initial index: 42%", "📄 24 of 58 files (full build) processed", "└─ ➕ 24 embedded", "🧩 71 chunks processed", "└─ ➕ 71 added"} {
		if !strings.Contains(out, want) {
			t.Fatalf("building status missing %q in:\n%s", want, out)
		}
	}
	// A cold first build loads and serves no reuse vectors, so the chunk tree
	// omits the reused row; a seeded first build shows it (see the next test).
	if strings.Contains(out, "reused") {
		t.Fatalf("a cold first build should omit the reused row in:\n%s", out)
	}
}

// TestRenderIndexingActiveSeededFirstBuild proves a first build that loads
// sibling vectors does not read like a cold full build.
func TestRenderIndexingActiveSeededFirstBuild(t *testing.T) {
	t.Parallel()
	codebase := &model.Codebase{CanonicalPath: "/Users/agoodkind/Sites/swift-makefile"}
	job := &model.Job{
		Operation: "index",
		Progress: model.Progress{
			RunMode:            model.RunModeFirstBuild,
			OverallPercent:     42,
			FilesTotal:         58,
			FilesProcessed:     1,
			FilesEmbedded:      1,
			ChunksProcessed:    678,
			ChunksReused:       623,
			ChunksEmbedded:     55,
			ReuseVectorsLoaded: 2316,
			LastEventAt:        renderTestTime,
		},
	}
	out := renderActiveStatusForTest(codebase, job)
	for _, want := range []string{
		"📁 swift-makefile",
		"🔄 Building initial index: 42%",
		"📄 1 of 58 files (first build, reusing prior vectors) processed",
		"└─ ➕ 1 embedded",
		"🧩 678 chunks processed",
		"├─ ➕ 55 added",
		"└─ ♻️ 623 reused",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("seeded building status missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "(full build)") {
		t.Fatalf("seeded first build should not use the cold full-build label in:\n%s", out)
	}
}

// TestHeadingFor proves the in-progress heading derives from whether a completed
// run exists and from the trigger: a first build, a forced reindex over a good
// index, or a changed-files sync.
func TestHeadingFor(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		cb   model.Codebase
		job  model.Job
		want string
	}{
		{"initial", model.Codebase{}, model.Job{Operation: "index", Forced: true}, "Building initial index"},
		{"forced", model.Codebase{LastSuccessfulRun: &model.IndexRunSummary{}}, model.Job{Operation: "index", Forced: true}, "Forced reindex"},
		{"forced-sync", model.Codebase{LastSuccessfulRun: &model.IndexRunSummary{}}, model.Job{Operation: "sync", Forced: true}, "Forced reindex"},
		{"changed", model.Codebase{LastSuccessfulRun: &model.IndexRunSummary{}}, model.Job{Operation: "sync"}, "Indexing new changes"},
	}
	for _, testCase := range cases {
		job := testCase.job
		if got := headingFor(testCase.cb, &job); got != testCase.want {
			t.Errorf("%s: headingFor = %q, want %q", testCase.name, got, testCase.want)
		}
	}
}

// TestRenderIndexingActiveIncremental proves an incremental run reads as
// "Indexing new changes" with the percent, the scanned/unchanged/re-embedded
// breakdown, the live chunk total, and the chunks added this scan.
func TestRenderIndexingActiveIncremental(t *testing.T) {
	t.Parallel()
	codebase := &model.Codebase{
		CanonicalPath:     "/Users/agoodkind/Sites/swift-makefile",
		LastSuccessfulRun: &model.IndexRunSummary{TotalChunks: 600},
	}
	job := &model.Job{
		Operation: "streaming_reindex",
		Progress: model.Progress{
			RunMode:        model.RunModeChanged,
			OverallPercent: 37, FilesTotal: 452, FilesProcessed: 290,
			FilesInCodebase: 4292, FilesAdded: 29, FilesModified: 423, FilesRemoved: 10,
			FilesEmbedded: 285, FilesSkippedOversize: 3, FilesSkippedUnreadable: 2,
			ChunksGenerated: 1043, ChunksTotal: 57240, LastEventAt: renderTestTime,
		},
	}
	out := renderActiveStatusForTest(codebase, job)
	for _, want := range []string{
		"📁 swift-makefile",
		"🔄 Indexing new changes: 37%",
		"🔢 4292 files: 462 changed, 3830 unchanged",
		"📄 300 of 462 changed files processed",
		"➕ 285 embedded",
		"🗑️ 10 removed",
		"📏 3 skipped, too large",
		"⚠️ 2 error, unreadable",
		"🧩 57,240 chunks processed",
		"➕ 1,043 added",
		"♻️ 0 reused",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("incremental status missing %q in:\n%s", want, out)
		}
	}
	// The old derived "already indexed" column is gone, and reuse is visible.
	if strings.Contains(out, "already indexed") {
		t.Fatalf("status still emits 'already indexed' in:\n%s", out)
	}
}

// TestRenderIndexingActiveIncrementalZeroProcessed proves that once the diff is
// known the status shows the indexing view with zero processed, rather than
// "Preparing", so a slow first embed does not read as a stall.
func TestRenderIndexingActiveIncrementalZeroProcessed(t *testing.T) {
	t.Parallel()
	codebase := &model.Codebase{CanonicalPath: "/Users/agoodkind/Sites/swift-makefile"}
	// Diff just captured: FilesInCodebase and the changed counts are set, but the
	// embed loop has not reported a FilesTotal yet. The renderer flips to the
	// indexing view off the diff-known fact, not the first per-file update.
	job := &model.Job{
		Operation: "sync",
		Progress: model.Progress{
			RunMode:        model.RunModeChanged,
			OverallPercent: 0, FilesTotal: 0, FilesProcessed: 0,
			FilesInCodebase: 4292, FilesAdded: 0, FilesModified: 118, FilesRemoved: 0,
			FilesEmbedded: 0, ChunksGenerated: 0,
		},
	}
	out := renderActiveStatusForTest(codebase, job)
	for _, want := range []string{
		"🔄 Indexing new changes: 0%",
		"🔢 4292 files: 118 changed, 4174 unchanged",
		"📄 0 of 118 changed files processed",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("zero-processed incremental status missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "Preparing") {
		t.Fatalf("expected indexing view, not preparing, in:\n%s", out)
	}
}

// TestRenderIndexingActiveIncrementalFallsBackToLastTotal proves the chunk total
// falls back to the last recorded run total when the live count is unpopulated.
func TestRenderIndexingActiveIncrementalFallsBackToLastTotal(t *testing.T) {
	t.Parallel()
	codebase := &model.Codebase{
		CanonicalPath:     "/Users/agoodkind/Sites/swift-makefile",
		LastSuccessfulRun: &model.IndexRunSummary{TotalChunks: 600},
	}
	job := &model.Job{
		Operation: "sync",
		Progress:  model.Progress{OverallPercent: 10, FilesTotal: 58, FilesProcessed: 1, ChunksTotal: 0},
	}
	out := renderActiveStatusForTest(codebase, job)
	if !strings.Contains(out, "🧩 600 chunks processed") {
		t.Fatalf("expected fallback to last recorded total in:\n%s", out)
	}
}

// TestRenderGetIndexBodySyncKeepsReady proves a background sync over an
// already-indexed codebase holds the ready view with a background note rather
// than the busy "Indexing new changes" takeover, because the live collection
// stays searchable while the delta runs.
func TestRenderGetIndexBodySyncKeepsReady(t *testing.T) {
	t.Parallel()
	codebase := &model.Codebase{
		CanonicalPath:     "/Users/agoodkind/Sites/swift-makefile",
		Status:            model.CodebaseStatusIndexing,
		LastSuccessfulRun: &model.IndexRunSummary{IndexedFiles: 58, TotalChunks: 600, CompletedAt: renderTestTime},
	}
	job := &model.Job{
		Operation: "sync",
		Progress:  model.Progress{OverallPercent: 33, FilesInCodebase: 58, FilesModified: 3, FilesProcessed: 1, LastEventAt: renderTestTime},
	}
	out := renderGetIndexBodyForTest("/Users/agoodkind/Sites/swift-makefile", true, codebase, job, dependencyHealth{})
	for _, want := range []string{
		"✅ Ready to search",
		"codebase.status: indexing",
		"last_successful_run.indexed_files: 58",
		"last_successful_run.total_chunks: 600",
		"🔄 Syncing 3 changed files in the background (33%)",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("sync-reconcile status missing %q in:\n%s", want, out)
		}
	}
	for _, reject := range []string{"Indexing new changes", "Building initial index"} {
		if strings.Contains(out, reject) {
			t.Fatalf("sync-reconcile status should not show %q in:\n%s", reject, out)
		}
	}
}

// TestRenderGetIndexBodySyncPreDiffKeepsReady proves a sync that has not yet
// captured its diff still reads as ready, with a generic background note.
func TestRenderGetIndexBodySyncPreDiffKeepsReady(t *testing.T) {
	t.Parallel()
	codebase := &model.Codebase{
		CanonicalPath:     "/Users/agoodkind/Sites/swift-makefile",
		Status:            model.CodebaseStatusIndexing,
		LastSuccessfulRun: &model.IndexRunSummary{IndexedFiles: 58, TotalChunks: 600, CompletedAt: renderTestTime},
	}
	job := &model.Job{Operation: "sync", Progress: model.Progress{FilesInCodebase: 0, LastEventAt: renderTestTime}}
	out := renderGetIndexBodyForTest("/Users/agoodkind/Sites/swift-makefile", true, codebase, job, dependencyHealth{})
	for _, want := range []string{"✅ Ready to search", "🔄 Checking for changes in the background"} {
		if !strings.Contains(out, want) {
			t.Fatalf("pre-diff sync status missing %q in:\n%s", want, out)
		}
	}
}

func TestRenderGetIndexBodyQuarantinedPreservesSearchabilityMessage(t *testing.T) {
	t.Parallel()
	codebase := &model.Codebase{
		CanonicalPath:     "/Users/agoodkind/Sites/swift-makefile",
		Status:            model.CodebaseStatusQuarantined,
		LastSuccessfulRun: &model.IndexRunSummary{IndexedFiles: 58, TotalChunks: 600, CompletedAt: renderTestTime},
		Quarantine: &model.QuarantineState{
			Reason:           quarantineReasonFullScanLargeDelete,
			FirstObservedAt:  renderTestTime,
			LastObservedAt:   renderTestTime,
			ObservationCount: 1,
			LastTrigger:      quarantineTriggerFullScan,
			LastMissingCount: 400,
			LastTotalCount:   4292,
		},
	}
	out := renderGetIndexBodyForTest("/Users/agoodkind/Sites/swift-makefile", true, codebase, nil, dependencyHealth{})
	for _, want := range []string{
		"is quarantined after a suspicious large disappearance",
		"Search continues to serve the last known-good index",
		"Last known good index: 58 files, 600 chunks",
		"400 of 4,292 tracked files",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("quarantined status missing %q in:\n%s", want, out)
		}
	}
}

// TestRenderGetIndexBodyNonTemplateEmptyNarrativeFallsBack proves the render
// fallback: a non-template display that arrives without a narrative (a caller
// that skipped resolveStatusNarrative) surfaces the status word and path rather
// than a blank body.
func TestRenderGetIndexBodyNonTemplateEmptyNarrativeFallsBack(t *testing.T) {
	t.Parallel()
	out := render.GetIndex(view.GetIndexView{
		Tracked:       true,
		RequestedPath: "/repo",
		CanonicalPath: "/repo",
		Display:       view.Display(displayQuarantined),
	})
	if !strings.Contains(out, "Codebase '/repo' status: quarantined") {
		t.Fatalf("empty-narrative fallback missing status word in:\n%s", out)
	}
}

// TestRenderGetIndexBodyBuildingTakesOver proves a from-scratch build still
// owns the display, because its staging collection is not promoted yet.
func TestRenderGetIndexBodyBuildingTakesOver(t *testing.T) {
	t.Parallel()
	codebase := &model.Codebase{CanonicalPath: "/Users/agoodkind/Sites/swift-makefile", Status: model.CodebaseStatusIndexing}
	job := &model.Job{Operation: "index", Progress: model.Progress{OverallPercent: 42, FilesTotal: 58, FilesProcessed: 24, ChunksGenerated: 71}}
	out := renderGetIndexBodyForTest("/Users/agoodkind/Sites/swift-makefile", true, codebase, job, dependencyHealth{})
	if !strings.Contains(out, "🔄 Building initial index") {
		t.Fatalf("expected building takeover in:\n%s", out)
	}
	if strings.Contains(out, "✅ Ready to search") {
		t.Fatalf("a from-scratch build must not read as ready in:\n%s", out)
	}
}

// TestRenderGetIndexBodyStreamingReindexTakesOver proves a streaming_reindex
// keeps the busy takeover, scoping the ready-during-sync change to "sync" only.
func TestRenderGetIndexBodyStreamingReindexTakesOver(t *testing.T) {
	t.Parallel()
	codebase := &model.Codebase{
		CanonicalPath:     "/Users/agoodkind/Sites/swift-makefile",
		Status:            model.CodebaseStatusIndexing,
		LastSuccessfulRun: &model.IndexRunSummary{IndexedFiles: 58, TotalChunks: 600, CompletedAt: renderTestTime},
	}
	job := &model.Job{Operation: "streaming_reindex", Progress: model.Progress{OverallPercent: 37, FilesInCodebase: 58, FilesModified: 58, FilesProcessed: 20}}
	out := renderGetIndexBodyForTest("/Users/agoodkind/Sites/swift-makefile", true, codebase, job, dependencyHealth{})
	if !strings.Contains(out, "🔄 Indexing new changes") {
		t.Fatalf("expected streaming_reindex takeover in:\n%s", out)
	}
	if strings.Contains(out, "✅ Ready to search") {
		t.Fatalf("a streaming_reindex must not read as ready in:\n%s", out)
	}
}

// TestRenderProgressLines proves the job-view progress lines show typed
// denominators, chunks, and the typed change breakdown.
func TestRenderProgressLines(t *testing.T) {
	t.Parallel()
	empty := view.ProgressSurface{
		Heading: "",
		Breakdown: view.OutcomeBreakdown{
			ScopeLabel:  "",
			Processed:   0,
			ScopeTotal:  0,
			FileRows:    nil,
			ChunksTotal: 0,
			ChunkRows:   nil,
		},
		ScopeLine:    "",
		PercentLabel: "",
	}
	emptyOut := render.GetJob(view.JobEntryView{ID: "job_empty", Progress: empty}, true)
	if strings.Contains(emptyOut, "📄") || strings.Contains(emptyOut, "🧩") {
		t.Fatalf("expected no progress lines for an empty view, got:\n%s", emptyOut)
	}
	progress := view.ProgressSurface{
		Heading: "",
		Breakdown: view.OutcomeBreakdown{
			ScopeLabel:  "files (full build)",
			Processed:   7,
			ScopeTotal:  58,
			FileRows:    []view.OutcomeRow{view.NewOutcomeRow(view.KindEmbedded, 7)},
			ChunksTotal: 84,
			ChunkRows:   []view.OutcomeRow{view.NewOutcomeRow(view.KindAdded, 84)},
		},
		ScopeLine:    "Changed since last sync: 12 files added · 30 modified · 5 removed",
		PercentLabel: "12.1%",
	}
	got := render.GetJob(view.JobEntryView{ID: "job_progress", Progress: progress}, true)
	for _, want := range []string{
		"📄 7 of 58 files (full build) processed",
		"└─ ➕ 7 embedded",
		"🧩 84 chunks processed",
		"└─ ➕ 84 added",
		"Changed since last sync: 12 files added · 30 modified · 5 removed",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("progress lines missing %q in:\n%s", want, got)
		}
	}
}

func TestRenderGetJobNotFound(t *testing.T) {
	t.Parallel()
	if got := render.GetJob(view.JobEntryView{}, false); got != "Job not found." {
		t.Fatalf("renderGetJob not found = %q, want %q", got, "Job not found.")
	}
}

// TestRenderGetJobShowsMagnitude proves the job view carries the magnitude.
func TestRenderGetJobShowsMagnitude(t *testing.T) {
	t.Parallel()
	job := &model.Job{
		ID:            "job_x",
		CanonicalPath: "/repo",
		Operation:     "sync",
		State:         model.JobStateRunning,
		Progress:      model.Progress{FilesTotal: 58, FilesProcessed: 7, FilesEmbedded: 7, ChunksGenerated: 84},
	}
	out := render.GetJob(resolveJobEntry(*job, false, ""), true)
	if !strings.Contains(out, "📄 7 of 58 files processed") {
		t.Fatalf("expected magnitude in job view, got:\n%s", out)
	}
	if !strings.Contains(out, "🧩 84 chunks processed") {
		t.Fatalf("expected chunk line in job view, got:\n%s", out)
	}
}

func TestRenderGetJobUsesAmericanCanceledSpelling(t *testing.T) {
	t.Parallel()
	completedAt := renderTestTime.Add(90 * time.Second)
	job := &model.Job{
		ID:            "job_x",
		CanonicalPath: "/repo",
		Operation:     "sync",
		State:         model.JobStateCancelled,
		StartedAt:     renderTestTime,
		UpdatedAt:     completedAt,
		CompletedAt:   &completedAt,
		Progress:      model.Progress{Phase: "cancelled"},
	}
	out := render.GetJob(resolveJobEntry(*job, false, ""), true)
	if strings.Contains(out, "cancelled") {
		t.Fatalf("job view should use American spelling, got:\n%s", out)
	}
	for _, want := range []string{"State: canceled", "Phase: canceled", "Duration: 1m30s"} {
		if !strings.Contains(out, want) {
			t.Fatalf("job view missing %q in:\n%s", want, out)
		}
	}
}

func TestRenderListJobsSummarizesHistory(t *testing.T) {
	t.Parallel()
	activeUpdatedAt := renderTestTime.Add(2 * time.Minute)
	completedAt := renderTestTime.Add(45 * time.Minute)
	jobs := []model.Job{
		{
			ID:            "job_running",
			CanonicalPath: "/repo/running",
			Operation:     "index",
			State:         model.JobStateRunning,
			StartedAt:     renderTestTime,
			UpdatedAt:     activeUpdatedAt,
			Progress:      model.Progress{OverallPercent: 22.5, FilesTotal: 58, FilesProcessed: 7, ChunksGenerated: 84},
		},
		{
			ID:            "job_completed",
			CanonicalPath: "/repo/completed",
			Operation:     "sync",
			State:         model.JobStateCompleted,
			StartedAt:     renderTestTime,
			UpdatedAt:     completedAt,
			CompletedAt:   &completedAt,
			Progress:      model.Progress{OverallPercent: 100, FilesTotal: 58, FilesProcessed: 58, ChunksGenerated: 144},
		},
		{
			ID:            "job_cancelled",
			CanonicalPath: "/repo/cancelled",
			Operation:     "sync",
			State:         model.JobStateCancelled,
			StartedAt:     renderTestTime,
			UpdatedAt:     completedAt,
			CompletedAt:   &completedAt,
			Progress:      model.Progress{OverallPercent: 0, Phase: "cancelled"},
		},
	}
	out := renderListJobsForTest(jobs, false)
	for _, want := range []string{
		"Tracked jobs: 3 total",
		"Active: 0 queued, 1 running, 0 paused, 0 canceling",
		"Ended: 1 completed, 0 failed, 0 superseded, 1 canceled",
		"Active jobs:",
		"Ended jobs: 2",
		"Duration: 45m0s",
		"Elapsed: 2m0s",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("job list missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "[cancelled") {
		t.Fatalf("job list should use American spelling for states, got:\n%s", out)
	}
}

func TestRenderListJobsLabelsFinishedJobHistoryAsEnded(t *testing.T) {
	t.Parallel()

	completedAt := renderTestTime.Add(10 * time.Hour)
	jobs := []model.Job{{
		ID:            "job_ended",
		CanonicalPath: "/repo",
		Operation:     "conversation_ingest",
		State:         model.JobStateCancelled,
		StartedAt:     renderTestTime,
		UpdatedAt:     completedAt,
		CompletedAt:   &completedAt,
		Progress: model.Progress{
			OverallPercent: 26,
			FilesTotal:     92,
			FilesProcessed: 1,
			FilesPending:   1,
		},
	}}

	out := renderListJobsForTest(jobs, true)
	if !strings.Contains(out, "Ended jobs: 1") {
		t.Fatalf("job history does not say the displayed job ended:\n%s", out)
	}
	if !strings.Contains(out, "[canceled") {
		t.Fatalf("ended job lost its terminal state:\n%s", out)
	}
	// One screen, one word for one set. The tally above the history counts the
	// same jobs the history lists, so a second word for it would read as a
	// second set the operator has to reconcile.
	if strings.Contains(out, "Terminal") {
		t.Fatalf("the job list names the finished set with two different words:\n%s", out)
	}
}

// The chunk header states what the run did with the chunks, not how settled the
// number is or where the chunks ended up, so one line is true in every state a
// job can end in. "total" overclaims finality while a run is live, "counted so
// far" hedges a settled number after it ends, and "indexed" claims the chunks
// are in the collection, which a failed first build's are not: the halt drops
// the staging collection and leaves the counters on the job record. The same
// breakdown feeds every surface and the wire form the TUI rebuilds from carries
// no liveness flag, so one wording has to hold across all of them.
func TestRenderChunkCountHeaderHoldsForRunningAndEndedJobs(t *testing.T) {
	t.Parallel()

	progress := model.Progress{
		RunMode:         model.RunModeChanged,
		FilesTotal:      92,
		FilesProcessed:  1,
		FilesEmbedded:   1,
		ChunksGenerated: 1168,
	}
	haltedProgress := progress
	haltedProgress.RunMode = model.RunModeFirstBuild
	haltedProgress.Phase = "failed"
	completedAt := renderTestTime.Add(time.Hour)
	cases := []struct {
		name string
		job  model.Job
	}{
		{
			name: "running",
			job: model.Job{
				ID: "job_chunks_running", CanonicalPath: "/repo", Operation: "conversation_ingest",
				State: model.JobStateRunning, Progress: progress,
			},
		},
		{
			name: "ended",
			job: model.Job{
				ID: "job_chunks_ended", CanonicalPath: "/repo", Operation: "conversation_ingest",
				State: model.JobStateCompleted, StartedAt: renderTestTime,
				UpdatedAt: completedAt, CompletedAt: &completedAt, Progress: progress,
			},
		},
		{
			// A first build that failed had its staging collection dropped, so its
			// chunks are in no collection at all. The header must not say they are.
			name: "halted first build",
			job: model.Job{
				ID: "job_chunks_halted", CanonicalPath: "/repo", Operation: "index",
				State: model.JobStateFailed, StartedAt: renderTestTime,
				UpdatedAt: completedAt, CompletedAt: &completedAt, Progress: haltedProgress,
				Error: &model.JobError{Message: "index budget exceeded", Code: "index_budget_exceeded"},
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			out := render.GetJob(resolveJobEntry(testCase.job, false, ""), true)
			if !hasExactLine(out, "🧩 1,168 chunks processed") {
				t.Fatalf("chunk count header is not the one shared line:\n%s", out)
			}
			for _, wrong := range []string{"chunks total", "chunks counted so far", "chunks indexed"} {
				if strings.Contains(out, wrong) {
					t.Fatalf("chunk header claims something the surface cannot know (%q):\n%s", wrong, out)
				}
			}
		})
	}
}

// The detail block prints its own key, so the figure under it stays bare; the
// list bracket has no key, so it carries the scope inline. Both are asserted as
// exact lines, because the prefix match this test replaced is what let
// "Progress: 95.0% run progress" reach an operator.
func TestRenderJobSurfacesNameTheProgressScopeExactlyOnce(t *testing.T) {
	t.Parallel()

	completedAt := renderTestTime.Add(time.Hour)
	job := model.Job{
		ID:            "job_scope_once",
		CanonicalPath: "/repo",
		Operation:     "sync",
		State:         model.JobStateCompleted,
		StartedAt:     renderTestTime,
		UpdatedAt:     completedAt,
		CompletedAt:   &completedAt,
		Progress: model.Progress{
			RunMode: model.RunModeChanged, OverallPercent: 100,
			FilesTotal: 4, FilesProcessed: 4, FilesEmbedded: 4, ChunksGenerated: 12,
		},
	}

	detail := render.GetJob(resolveJobEntry(job, false, ""), true)
	if !hasExactLine(detail, "📊 Run progress: 100.0%") {
		t.Fatalf("job detail does not carry the bare figure under its own key:\n%s", detail)
	}
	if strings.Contains(detail, "run progress: 100.0% run progress") ||
		strings.Contains(detail, "Progress: 100.0% run progress") {
		t.Fatalf("job detail doubles the progress label:\n%s", detail)
	}

	list := renderListJobsForTest([]model.Job{job}, false)
	if !hasExactLine(list, "- job_scope_once [completed · 100.0% run progress] sync /repo") {
		t.Fatalf("job list bracket does not name the progress scope:\n%s", list)
	}
}

// hasExactLine reports whether out carries want as a whole line, ignoring the
// surrounding indentation a surface adds. A Contains check would also accept a
// line that merely starts with want, which is how a doubled label ships.
func hasExactLine(out string, want string) bool {
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) == want {
			return true
		}
	}
	return false
}

func TestRenderConversationJobLabelsEachProgressScope(t *testing.T) {
	t.Parallel()

	job := model.Job{
		ID:            "job_scopes",
		CanonicalPath: "chat:///clyde-conversations",
		Operation:     "conversation_ingest",
		State:         model.JobStateRunning,
		Progress: model.Progress{
			RunMode:        model.RunModeChanged,
			Unit:           "document",
			ScopeUnit:      "conversation",
			OverallPercent: 1.1,
			FilesTotal:     92,
			FilesProcessed: 1,
			FilesAdded:     76,
			FilesModified:  16,
			FilesEmbedded:  1,
		},
	}

	jobs := []model.Job{job}
	completedAt := renderTestTime.Add(time.Hour)
	for index := range 9 {
		jobs = append(jobs, model.Job{
			ID:            fmt.Sprintf("job_ended_%d", index),
			CanonicalPath: "/repo",
			Operation:     "sync",
			State:         model.JobStateCompleted,
			StartedAt:     renderTestTime,
			UpdatedAt:     completedAt,
			CompletedAt:   &completedAt,
		})
	}

	out := renderListJobsForTest(jobs, false)
	for _, want := range []string{
		"[running · 1.1% run progress]",
		"📄 1 of 92 changed documents processed",
		"Changed since last sync: 76 conversations added · 16 modified",
		"Tracked jobs: 10 total",
		"Active: 0 queued, 1 running, 0 paused, 0 canceling",
		"Recent ended jobs: showing 8 of 9",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("job list does not label scope %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "showing 8 of 10") {
		t.Fatalf("recent history denominator includes the active job:\n%s", out)
	}
}

// An earlier failure for a codebase that has a later terminal job is tallied as
// superseded, not failed, and the entry names the successor.
func TestRenderListJobsSeparatesSupersededFailures(t *testing.T) {
	t.Parallel()
	t0 := renderTestTime
	older := t0.Add(1 * time.Minute)
	newer := t0.Add(2 * time.Minute)
	jobs := []model.Job{
		{
			ID:            "job_old",
			CodebaseID:    "A",
			CanonicalPath: "/repo/a",
			Operation:     "sync",
			State:         model.JobStateFailed,
			StartedAt:     t0,
			CompletedAt:   &older,
			Error:         &model.JobError{Message: "embedding endpoint is unreachable", Retryable: true},
		},
		{
			ID:            "job_new",
			CodebaseID:    "A",
			CanonicalPath: "/repo/a",
			Operation:     "sync",
			State:         model.JobStateFailed,
			StartedAt:     t0,
			CompletedAt:   &newer,
			Error:         &model.JobError{Message: "internal error", Retryable: false},
		},
	}
	out := renderListJobsForTest(jobs, false)
	if want := "Ended: 0 completed, 1 failed, 1 superseded, 0 canceled"; !strings.Contains(out, want) {
		t.Fatalf("summary did not separate superseded from failed, want %q in:\n%s", want, out)
	}
	if want := "superseded by job_new"; !strings.Contains(out, want) {
		t.Fatalf("superseded entry did not name its successor, want %q in:\n%s", want, out)
	}
	if want := strings.Repeat("─", 40); !strings.Contains(out, want) {
		t.Fatalf("two jobs in a section must be separated by the divider in:\n%s", out)
	}
}

// TestRenderGetJobPreparingNotZeroPercent proves a running index job whose work
// scope is not measured yet (A1) shows the preparing label, not a 0.0%.
func TestRenderGetJobPreparingNotZeroPercent(t *testing.T) {
	t.Parallel()
	job := &model.Job{
		ID:            "job_prep",
		CanonicalPath: "/repo",
		Operation:     "index",
		State:         model.JobStateRunning,
		Progress:      model.Progress{FilesTotal: 0, FilesInCodebase: 0, OverallPercent: 0},
	}
	out := render.GetJob(resolveJobEntry(*job, false, ""), true)
	if !hasExactLine(out, "📊 Run progress: Preparing to index") {
		t.Fatalf("expected preparing label, got:\n%s", out)
	}
	if strings.Contains(out, "0.0%") {
		t.Fatalf("running job with unknown scope must not show 0.0%%, got:\n%s", out)
	}
}

// TestRenderListJobsPreparingNotZeroPercent proves the same for the list view (A1).
func TestRenderListJobsPreparingNotZeroPercent(t *testing.T) {
	t.Parallel()
	jobs := []model.Job{{
		ID:            "job_prep",
		CanonicalPath: "/repo",
		Operation:     "index",
		State:         model.JobStateRunning,
		Progress:      model.Progress{FilesTotal: 0, FilesInCodebase: 0},
	}}
	out := renderListJobsForTest(jobs, false)
	if !strings.Contains(out, "Preparing to index") {
		t.Fatalf("expected preparing label in list, got:\n%s", out)
	}
	if strings.Contains(out, "0.0%") {
		t.Fatalf("list entry with unknown scope must not show 0.0%%, got:\n%s", out)
	}
	if strings.Contains(out, strings.Repeat("─", 40)) {
		t.Fatalf("a single job must not be wrapped by a divider, got:\n%s", out)
	}
}

// TestRenderGetJobSyncPreparingWording proves a sync job with unknown scope uses
// the sync-specific preparing wording (A2).
func TestRenderGetJobSyncPreparingWording(t *testing.T) {
	t.Parallel()
	job := &model.Job{
		ID:            "job_sync",
		CanonicalPath: "/repo",
		Operation:     "sync",
		State:         model.JobStateRunning,
		Progress:      model.Progress{FilesTotal: 0, FilesInCodebase: 0},
	}
	out := render.GetJob(resolveJobEntry(*job, false, ""), true)
	if !strings.Contains(out, "Changes detected, preparing to index") {
		t.Fatalf("expected sync preparing wording, got:\n%s", out)
	}
}

// TestRenderGetJobKeepsRealZeroPercent proves a running job whose scope IS known
// still shows 0.0% (a genuine "0 of N"), not the preparing label (A3).
func TestRenderGetJobKeepsRealZeroPercent(t *testing.T) {
	t.Parallel()
	job := &model.Job{
		ID:            "job_zero",
		CanonicalPath: "/repo",
		Operation:     "index",
		State:         model.JobStateRunning,
		Progress:      model.Progress{FilesTotal: 58, OverallPercent: 0},
	}
	out := render.GetJob(resolveJobEntry(*job, false, ""), true)
	if !hasExactLine(out, "📊 Run progress: 0.0%") {
		t.Fatalf("known-scope zero should render 0.0%%, got:\n%s", out)
	}
	if strings.Contains(out, "Preparing to index") {
		t.Fatalf("known-scope job should not show preparing, got:\n%s", out)
	}
}

// TestRenderGetJobShowsMeasuredPercent proves a measured percent renders as-is (A4).
func TestRenderGetJobShowsMeasuredPercent(t *testing.T) {
	t.Parallel()
	job := &model.Job{
		ID:            "job_mid",
		CanonicalPath: "/repo",
		Operation:     "index",
		State:         model.JobStateRunning,
		Progress:      model.Progress{FilesTotal: 4292, FilesProcessed: 2139, OverallPercent: 49.8},
	}
	out := render.GetJob(resolveJobEntry(*job, false, ""), true)
	if !hasExactLine(out, "📊 Run progress: 49.8%") {
		t.Fatalf("expected 49.8%%, got:\n%s", out)
	}
}

// TestRenderGetJobFailedShowsPercentAndError proves a terminal failed job keeps
// its percent and error line, never the preparing label, even at 0% (A5).
func TestRenderGetJobFailedShowsPercentAndError(t *testing.T) {
	t.Parallel()
	job := &model.Job{
		ID:            "job_fail",
		CanonicalPath: "/repo",
		Operation:     "index",
		State:         model.JobStateFailed,
		Progress:      model.Progress{FilesTotal: 0, OverallPercent: 0},
		Error:         &model.JobError{Message: "embedder_unreachable: dial tcp [::1]:5400: connect: connection refused"},
	}
	out := render.GetJob(resolveJobEntry(*job, false, ""), true)
	if !hasExactLine(out, "📊 Run progress: 0.0%") {
		t.Fatalf("failed job should show its percent, got:\n%s", out)
	}
	if strings.Contains(out, "Preparing to index") {
		t.Fatalf("failed (terminal) job must not show preparing, got:\n%s", out)
	}
	if !strings.Contains(out, "Error: embedder_unreachable") {
		t.Fatalf("failed job should show error line, got:\n%s", out)
	}
}

// TestStatusTemplateNoBlankLines proves the embedded templates produce a tidy
// block with no blank lines and the expected line count, guarding against
// template whitespace regressions.
func TestStatusTemplateNoBlankLines(t *testing.T) {
	t.Parallel()
	codebase := &model.Codebase{
		CanonicalPath: "/Users/agoodkind/Sites/swift-makefile",
		Status:        model.CodebaseStatusIndexed,
		UpdatedAt:     renderTestTime,
		LastSuccessfulRun: &model.IndexRunSummary{
			IndexedFiles: 58,
			TotalChunks:  600,
			Status:       "completed",
			CompletedAt:  renderTestTime,
		},
	}
	cases := map[string]string{
		"ready":       renderReadyStatusForTest(codebase),
		"preparing":   renderActiveStatusForTest(codebase, &model.Job{Operation: "sync", Progress: model.Progress{FilesTotal: 0}}),
		"building":    renderActiveStatusForTest(codebase, &model.Job{Operation: "index", Progress: model.Progress{RunMode: model.RunModeForcedReindex, FilesTotal: 58, FilesProcessed: 7, FilesEmbedded: 7, ChunksGenerated: 84}}),
		"incremental": renderActiveStatusForTest(codebase, &model.Job{Operation: "sync", Progress: model.Progress{RunMode: model.RunModeChanged, FilesTotal: 58, FilesProcessed: 7, FilesInCodebase: 100, FilesAdded: 5, FilesModified: 50, FilesRemoved: 3, FilesEmbedded: 2, ChunksGenerated: 84, ChunksTotal: 620}}),
	}
	wantLines := map[string]int{"ready": 9, "preparing": 3, "building": 8, "incremental": 11}
	for name, out := range cases {
		for _, line := range strings.Split(out, "\n") {
			if strings.TrimSpace(line) == "" {
				t.Fatalf("%s has a blank line:\n%s", name, out)
			}
		}
		if got := len(strings.Split(out, "\n")); got != wantLines[name] {
			t.Fatalf("%s line count = %d, want %d:\n%s", name, got, wantLines[name], out)
		}
	}
}

func TestStatusTemplateRendersStructuredGraphLine(t *testing.T) {
	t.Parallel()
	semanticLine := "🕐 Semantic: updated 4:10 PM PDT (6m ago)"
	cases := []struct {
		name       string
		statusView view.StatusView
		want       string
	}{
		{
			name: "updated",
			statusView: view.StatusView{
				Name:           "repo",
				UpdatedAt:      "4:10 PM PDT (6m ago)",
				GraphUpdatedAt: "4:09 PM PDT (7m ago)",
			},
			want: "🕸️ Code graph: updated 4:09 PM PDT (7m ago)",
		},
		{
			name: "building",
			statusView: view.StatusView{
				Name:          "repo",
				UpdatedAt:     "4:10 PM PDT (6m ago)",
				GraphBuilding: true,
			},
			want: "🕸️ Code graph: building",
		},
		{
			name: "failed",
			statusView: view.StatusView{
				Name:        "repo",
				UpdatedAt:   "4:10 PM PDT (6m ago)",
				GraphFailed: true,
			},
			want: "🕸️ Code graph: update didn't finish, retries",
		},
		{
			name: "ready no time",
			statusView: view.StatusView{
				Name:             "repo",
				UpdatedAt:        "4:10 PM PDT (6m ago)",
				GraphReadyNoTime: true,
			},
			want: "🕸️ Code graph: ready",
		},
		{
			name: "not built",
			statusView: view.StatusView{
				Name:          "repo",
				UpdatedAt:     "4:10 PM PDT (6m ago)",
				GraphNotBuilt: true,
			},
			want: "🕸️ Code graph: builds shortly",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			out := render.GetIndex(view.GetIndexView{
				Tracked:      true,
				TemplateName: "ready.md.tmpl",
				Status:       testCase.statusView,
			})
			if !strings.Contains(out, testCase.want) {
				t.Fatalf("status output missing graph line %q:\n%s", testCase.want, out)
			}
			if !strings.Contains(out, semanticLine) {
				t.Fatalf("status output missing semantic line %q:\n%s", semanticLine, out)
			}
			if strings.Contains(out, "semantic snapshot") {
				t.Fatalf("status output still mentions semantic snapshot:\n%s", out)
			}
		})
	}
}

func TestStatusFooterRendersAcrossCodebaseTemplates(t *testing.T) {
	t.Parallel()
	templates := []string{
		"ready.md.tmpl",
		"building.md.tmpl",
		"incremental.md.tmpl",
		"discovered.md.tmpl",
		"pending.md.tmpl",
		"preparing.md.tmpl",
		"loading.md.tmpl",
		"waiting.md.tmpl",
	}
	statusView := view.StatusView{
		Name:              "repo",
		UpdatedAt:         "4:10 PM PDT (6m ago)",
		GraphUpdatedAt:    "4:09 PM PDT (7m ago)",
		PrepareLabel:      "Preparing to index",
		WaitLabel:         "Waiting for the embedding server",
		Heading:           "Building initial index",
		Percent:           42,
		FilesInCodebase:   314,
		FilesChanged:      12,
		FilesUnchanged:    302,
		ReuseForecastLine: "♻️ reuses embeddings from 1 indexed sibling worktree",
		Files:             314,
		Chunks:            1979,
		HasStats:          true,
	}
	for _, templateName := range templates {
		t.Run(templateName, func(t *testing.T) {
			out := render.GetIndex(view.GetIndexView{
				Tracked:      true,
				TemplateName: templateName,
				Status:       statusView,
			})
			if !strings.Contains(out, "🕐 Semantic: updated 4:10 PM PDT (6m ago)") {
				t.Fatalf("%s missing semantic footer:\n%s", templateName, out)
			}
			if !strings.Contains(out, "🕸️ Code graph: updated 4:09 PM PDT (7m ago)") {
				t.Fatalf("%s missing graph footer:\n%s", templateName, out)
			}
		})
	}
}

// TestNotReadyStatusOffersAShellSafeWaitCommand proves the two not-ready
// layouts hand the operator a wait command they can paste verbatim. The path
// carries a space and an apostrophe, so an unquoted or half-quoted path would
// run as several arguments and index the wrong directory.
func TestNotReadyStatusOffersAShellSafeWaitCommand(t *testing.T) {
	t.Parallel()
	const awkwardPath = "/Users/agoodkind/My Sites/alex's repo"
	const wantCommand = `lm-semantic-search codebase wait '/Users/agoodkind/My Sites/alex'"'"'s repo'`

	for _, templateName := range []string{"pending.md.tmpl", "loading.md.tmpl"} {
		t.Run(templateName, func(t *testing.T) {
			t.Parallel()
			out := render.GetIndex(view.GetIndexView{
				Tracked:      true,
				TemplateName: templateName,
				Status:       view.StatusView{Name: "repo", Path: awkwardPath},
			})
			if !strings.Contains(out, wantCommand) {
				t.Fatalf("%s missing shell-safe wait command %q:\n%s", templateName, wantCommand, out)
			}
			if !strings.Contains(out, "wait_for_indexing") {
				t.Fatalf("%s missing the MCP tool alternative:\n%s", templateName, out)
			}
		})
	}
}

// TestRenderClearIndexHasNoRemainLine proves the clear output is just the
// success line, with no trailing "other ... remain" count.
func TestRenderClearIndexHasNoRemainLine(t *testing.T) {
	t.Parallel()
	codebase := model.Codebase{CanonicalPath: "/Users/agoodkind/Sites/swift-makefile"}
	out := render.MutationAck(view.MutationAckView{Kind: view.AckClear, Path: codebase.CanonicalPath})
	want := "Successfully cleared codebase '/Users/agoodkind/Sites/swift-makefile'"
	if out != want {
		t.Fatalf("renderClearIndex = %q, want %q", out, want)
	}
	if strings.Contains(out, "remain") {
		t.Fatalf("renderClearIndex still contains a remain line:\n%s", out)
	}
}

// TestRenderListIndexesShowsIDAndPluralHeader proves each row carries the
// copy-pasteable id and the header pluralizes the codebase count correctly.
func TestRenderListIndexesShowsIDAndPluralHeader(t *testing.T) {
	t.Parallel()
	single := []view.CodebaseRowView{
		{ID: "cb_1_aaaa", CanonicalPath: "/tmp/alpha", Display: view.Display(displayIndexed)},
	}
	out := render.ListIndexes(single)
	if !strings.Contains(out, "Tracked 1 codebase:") {
		t.Fatalf("single header not singular:\n%s", out)
	}
	if !strings.Contains(out, "cb_1_aaaa") || !strings.Contains(out, "/tmp/alpha") || !strings.Contains(out, "[indexed]") {
		t.Fatalf("row missing id, path, or display status:\n%s", out)
	}

	many := []view.CodebaseRowView{
		{ID: "cb_1_aaaa", CanonicalPath: "/tmp/alpha", Display: view.Display(displayIndexed)},
		{ID: "cb_2_bbbb", CanonicalPath: "/tmp/beta", Display: view.Display(displayPreparing)},
	}
	outMany := render.ListIndexes(many)
	if !strings.Contains(outMany, "Tracked 2 codebases:") {
		t.Fatalf("plural header wrong:\n%s", outMany)
	}
	if !strings.Contains(outMany, "[preparing]") {
		t.Fatalf("list should show the display status, not raw status:\n%s", outMany)
	}
	if strings.Contains(outMany, "(s)") {
		t.Fatalf("list still uses a (s) suffix:\n%s", outMany)
	}
}

// TestWirePhasePercentTracksTheCurrentScope proves the wire carries a live
// phase percent derived from the run's own counters, distinct from the headline
// percent. A reuse-heavy run reads mostly done overall while its file cursor,
// the work it is actually doing now, sits low.
func TestWirePhasePercentTracksTheCurrentScope(t *testing.T) {
	t.Parallel()
	job := model.Job{
		ID:            "job_phase",
		CanonicalPath: "/repo",
		State:         model.JobStateRunning,
		Operation:     "conversation_ingest",
		Progress: model.Progress{
			RunMode:        model.RunModeChanged,
			OverallPercent: 5.0,
			FilesTotal:     1000, FilesProcessed: 250,
			ChunksReused: 30000, ChunksEmbedded: 1000, ChunksTotal: 40000,
			LastEventAt: renderTestTime,
		},
	}
	progress := pbconv.ToJob(job).GetProgress()
	if got := progress.GetPhasePercent(); got != 25.0 {
		t.Fatalf("wire PhasePercent = %.4f, want 25.0 (250 of 1000 files this phase)", got)
	}
	// The headline percent is a different question and keeps its own answer.
	if got := progress.GetOverallPercent(); got != 77.5 {
		t.Fatalf("wire OverallPercent = %.4f, want 77.5 (unchanged by the phase percent)", got)
	}
}

// TestWirePhasePercentFallsBackToEmbedBatches proves a run whose only measured
// scope is one item's embed loop still reports movement, rather than the flat
// zero the field carried before it was derived.
func TestWirePhasePercentFallsBackToEmbedBatches(t *testing.T) {
	t.Parallel()
	job := model.Job{
		ID:            "job_batches",
		CanonicalPath: "/repo",
		State:         model.JobStateRunning,
		Operation:     "conversation_ingest",
		Progress: model.Progress{
			EmbeddingBatchesTotal: 20, EmbeddingBatchesCompleted: 5,
			LastEventAt: renderTestTime,
		},
	}
	if got := pbconv.ToJob(job).GetProgress().GetPhasePercent(); got != 25.0 {
		t.Fatalf("wire PhasePercent = %.4f, want 25.0 (5 of 20 embed batches)", got)
	}
}
