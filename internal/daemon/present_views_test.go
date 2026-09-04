package daemon

import (
	"context"
	"strings"
	"testing"

	"goodkind.io/lm-semantic-search/internal/jobscheduler"
	"goodkind.io/lm-semantic-search/internal/model"
	render "goodkind.io/lm-semantic-search/internal/render"
	"goodkind.io/lm-semantic-search/internal/view"
)

func TestResolveStatusViewFallsBackToLiveChunkTotal(t *testing.T) {
	t.Parallel()
	codebase := model.Codebase{CanonicalPath: "/repo", LiveChunkTotal: 33240}
	job := model.Job{
		State:     model.JobStateRunning,
		Operation: "sync",
		Progress:  model.Progress{FilesInCodebase: 100, FilesModified: 2},
	}
	statusView, templateName := resolveStatusView(codebase, &job, displayIndexing, dependencyHealthy)
	if statusView.Breakdown.ChunksTotal != 33240 {
		t.Fatalf("ChunksTotal = %d, want the live total 33240", statusView.Breakdown.ChunksTotal)
	}
	if templateName != "incremental.md.tmpl" {
		t.Fatalf("template = %q, want incremental", templateName)
	}
}

func TestResolveStatusViewPreservesDependencyWaitLabelForPausedJob(t *testing.T) {
	t.Parallel()

	codebase := model.Codebase{CanonicalPath: "/repo"}
	job := model.Job{
		State:            model.JobStatePaused,
		SchedulingReason: model.SchedulingReasonThermalSafety,
	}
	statusView, _ := resolveStatusView(
		codebase,
		&job,
		displayWaiting,
		dependencyStoreUnavailable,
	)
	if statusView.WaitLabel != "Waiting for the vector store" {
		t.Fatalf("WaitLabel = %q, want dependency blocker", statusView.WaitLabel)
	}
}

func TestSchedulingReasonVocabulary(t *testing.T) {
	t.Parallel()

	policy := model.DefaultSchedulingPolicy()
	testCases := map[string]model.SchedulingReason{
		"higher-priority work":       model.SchedulingReasonHigherPriorityWork,
		"input active":               model.SchedulingReasonUserActive,
		"input activity unavailable": model.SchedulingReasonActivityUnavailable,
		"thermal state unsafe":       model.SchedulingReasonThermalSafety,
	}
	for rawReason, want := range testCases {
		scheduling := resolveSchedulingView(
			policy,
			model.JobStatePaused,
			model.CanonicalSchedulingReason(rawReason),
		)
		if scheduling.Reason != view.SchedulingReason(want) {
			t.Errorf("reason %q = %q, want %q", rawReason, scheduling.Reason, want)
		}
	}
	if got := model.CanonicalSchedulingReason("free-form reason"); got != model.SchedulingReasonUnspecified {
		t.Fatalf("unknown reason = %q, want unspecified", got)
	}
}

func TestSchedulingReasonReadsCachedSchedulerState(t *testing.T) {
	scheduler := jobscheduler.New(context.Background(), 1, nil)
	highPolicy := model.DefaultSchedulingPolicy()
	highPolicy.Priority = model.JobPriorityHigh
	blocker, err := scheduler.Acquire(context.Background(), jobscheduler.Entry{
		JobID:         "job-high",
		Policy:        highPolicy,
		QueueSequence: 1,
	})
	if err != nil {
		t.Fatalf("Acquire blocker: %v", err)
	}

	lowPolicy := model.DefaultSchedulingPolicy()
	lowPolicy.Priority = model.JobPriorityLow
	ctx, cancel := context.WithCancel(context.Background())
	waiterDone := make(chan error, 1)
	go func() {
		lease, acquireErr := scheduler.Acquire(ctx, jobscheduler.Entry{
			JobID:         "job-low",
			Policy:        lowPolicy,
			QueueSequence: 2,
		})
		if lease != nil {
			lease.Release()
		}
		waiterDone <- acquireErr
	}()
	t.Cleanup(func() {
		cancel()
		blocker.Release()
		<-waiterDone
		scheduler.Close()
	})
	waitForCondition(t, func() bool {
		return scheduler.Snapshot().Queued[model.JobPriorityLow] == 1
	})

	job := model.Job{
		ID:                        "job-low",
		State:                     model.JobStateQueued,
		EffectiveSchedulingPolicy: lowPolicy,
	}
	manager := &Manager{
		codebases:               map[string]model.Codebase{},
		jobs:                    map[string]model.Job{job.ID: job},
		pendingCodeJobs:         map[string]pendingCodeRequest{},
		pendingConversationJobs: map[string]conversationJobPayload{},
		jobScheduler:            scheduler,
	}
	resolved, found := manager.GetJob(job.ID)
	if !found {
		t.Fatal("GetJob did not find queued job")
	}
	if resolved.SchedulingReason != model.SchedulingReasonHigherPriorityWork {
		t.Fatalf(
			"queued reason = %q, want higher-priority work",
			resolved.SchedulingReason,
		)
	}
	statusSnapshot := manager.StatusSnapshot()
	if len(statusSnapshot.ActiveJobs) != 1 ||
		statusSnapshot.ActiveJobs[0].SchedulingReason !=
			model.SchedulingReasonHigherPriorityWork {
		t.Fatalf("status jobs = %+v, want canonical queued reason", statusSnapshot.ActiveJobs)
	}
}

// TestResolveStatusViewDiscoveredSelectsTemplate proves a discovered codebase
// resolves to the discovered template, and that rendering a discovered status
// view through render.GetIndex carries the reuse-forecast line and the
// not-yet-indexed body.
func TestResolveStatusViewDiscoveredSelectsTemplate(t *testing.T) {
	t.Parallel()
	codebase := model.Codebase{Status: model.CodebaseStatusDiscovered, CanonicalPath: "/x"}
	statusView, templateName := resolveStatusView(codebase, nil, displayDiscovered, dependencyHealthy)
	if templateName != "discovered.md.tmpl" {
		t.Fatalf("template = %q, want discovered.md.tmpl", templateName)
	}

	statusView.ReuseForecastLine = "♻️ reuses embeddings from 2 indexed sibling worktrees"
	out := render.GetIndex(view.GetIndexView{
		Tracked:       true,
		RequestedPath: "/x",
		CanonicalPath: "/x",
		Display:       view.Display(displayDiscovered),
		TemplateName:  templateName,
		Status:        statusView,
	})
	if !strings.Contains(out, "discovered, not yet indexed") {
		t.Fatalf("discovered render missing the not-yet-indexed body; got %q", out)
	}
	if !strings.Contains(out, statusView.ReuseForecastLine) {
		t.Fatalf("discovered render missing the reuse forecast line; got %q", out)
	}
}

func TestRenderMutationAckManifest(t *testing.T) {
	t.Parallel()
	out := render.MutationAck(view.MutationAckView{
		Kind:            view.AckManifest,
		Path:            "",
		JobID:           "",
		StateLabel:      "",
		AlreadyTerminal: false,
		Deduplicated:    false,
		CollectionID:    "clyde-conversations",
		CollectionName:  "",
		CodebaseID:      "",
		ConversationID:  "",
		DocumentCount:   0,
		NeededCount:     11,
		TotalCount:      1011,
	})
	if !strings.Contains(out, "needs 11 of 1011") {
		t.Fatalf("manifest ack = %q, want the needed-of-total counts", out)
	}
}
