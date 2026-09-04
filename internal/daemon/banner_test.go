package daemon

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
	"goodkind.io/lm-semantic-search/internal/adapterr"
	"goodkind.io/lm-semantic-search/internal/clock"
	"goodkind.io/lm-semantic-search/internal/config"
	"goodkind.io/lm-semantic-search/internal/model"
	render "goodkind.io/lm-semantic-search/internal/render"
	"goodkind.io/lm-semantic-search/internal/view"
)

// A healthy record renders no banner; each degraded mode renders its own variant
// with the matching environment reference.
func TestRenderHealthBannerVariants(t *testing.T) {
	t.Parallel()
	cfg := config.Config{OpenAIBaseURL: "http://localhost:5400/v1", MilvusAddress: "127.0.0.1:19530"}

	if got := render.HealthBanner(resolveBannerView(dependencyHealth{Mode: dependencyHealthy}, cfg)); got != "" {
		t.Fatalf("healthy banner = %q, want empty", got)
	}

	cases := []struct {
		mode        dependencyMode
		wantHeadGot string
		wantRef     string
	}{
		{dependencyEmbedderUnreachable, "unreachable", "OPENAI_BASE_URL=http://localhost:5400/v1"},
		{dependencyEmbedderRejected, "rejecting requests", "Check the model name, dimensions, and credentials"},
		{dependencyEmbedderPaused, "paused to preserve battery", "Leave low power mode or resume the embedding service"},
		{dependencyEmbedderBusy, "throttling requests", "OPENAI_BASE_URL=http://localhost:5400/v1"},
		{dependencyStoreUnavailable, "Vector store unavailable", "MILVUS_ADDRESS=127.0.0.1:19530"},
	}
	for _, testCase := range cases {
		out := render.HealthBanner(resolveBannerView(dependencyHealth{Mode: testCase.mode, StoreReachableAt: clock.Now(), EmbedderReachableAt: clock.Now()}, cfg))
		if !strings.HasPrefix(out, "🟥 ") {
			t.Fatalf("%s banner missing the 🟥 marker: %q", testCase.mode, out)
		}
		if !strings.Contains(out, testCase.wantHeadGot) {
			t.Fatalf("%s banner missing %q in: %q", testCase.mode, testCase.wantHeadGot, out)
		}
		if !strings.Contains(out, testCase.wantRef) {
			t.Fatalf("%s banner missing reference %q in: %q", testCase.mode, testCase.wantRef, out)
		}
		if strings.Count(out, "🟥") != 1 {
			t.Fatalf("%s banner should have exactly one marker: %q", testCase.mode, out)
		}
	}
}

func TestListJobsPauseFailureShowsPauseRecoveryGuidance(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	canonical, err := filepath.EvalSymlinks(repoPath)
	if err != nil {
		t.Fatalf("EvalSymlinks returned error: %v", err)
	}

	codebase := newCodebaseRecord(canonical)
	codebase.Status = model.CodebaseStatusIndexing
	job := model.Job{
		ID:            "job-paused",
		CodebaseID:    codebase.ID,
		CanonicalPath: canonical,
		Operation:     "index",
		State:         model.JobStateRunning,
	}
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.jobs[job.ID] = job
	manager.mu.Unlock()

	manager.updateJobFailed(
		context.Background(),
		job.ID,
		adapterr.NewEmbedderPaused(
			"embedding endpoint reported: service paused to preserve battery (low_power_mode)",
			"leave low power mode or resume the embedding service",
			errors.New("503 service_paused"),
		),
	)

	server := NewGRPCServer(manager, nil)
	response, err := server.ListJobs(context.Background(), &pb.ListJobsRequest{})
	if err != nil {
		t.Fatalf("ListJobs returned error: %v", err)
	}
	text := response.GetDisplayText()
	if !strings.Contains(text, "paused to preserve battery") {
		t.Fatalf("ListJobs omitted the battery pause explanation:\n%s", text)
	}
	if !strings.Contains(text, "Leave low power mode or resume the embedding service") {
		t.Fatalf("ListJobs omitted pause recovery guidance:\n%s", text)
	}
	for _, contradictory := range []string{"rejecting requests", "embedding config is fixed", "model name, dimensions, and credentials"} {
		if strings.Contains(text, contradictory) {
			t.Fatalf("ListJobs included contradictory configuration guidance %q:\n%s", contradictory, text)
		}
	}
}

// The waiting body names the embedder for the embedder modes and the vector store
// for the store mode, leaving the specific cause to the banner.
func TestRenderWaitingNamesDependency(t *testing.T) {
	t.Parallel()
	codebase := &model.Codebase{CanonicalPath: "/Users/agoodkind/Sites/swift-makefile"}

	embedderView, embedderTemplate := resolveStatusView(*codebase, nil, displayWaiting, dependencyEmbedderUnreachable)
	embedderOut := render.GetIndex(view.GetIndexView{Tracked: true, TemplateName: embedderTemplate, Status: embedderView})
	if !strings.Contains(embedderOut, "⏳ Waiting for the embedding server") {
		t.Fatalf("embedder waiting body wrong:\n%s", embedderOut)
	}
	storeView, storeTemplate := resolveStatusView(*codebase, nil, displayWaiting, dependencyStoreUnavailable)
	storeOut := render.GetIndex(view.GetIndexView{Tracked: true, TemplateName: storeTemplate, Status: storeView})
	if !strings.Contains(storeOut, "⏳ Waiting for the vector store") {
		t.Fatalf("store waiting body wrong:\n%s", storeOut)
	}
}

// The job view suppresses the Error line when the banner is showing and the job
// stopped on that retryable cause, and shows it otherwise.
func TestRenderGetJobNoEchoWhenDegraded(t *testing.T) {
	t.Parallel()
	job := &model.Job{
		ID:            "job_x",
		CanonicalPath: "/repo",
		Operation:     "index",
		State:         model.JobStateFailed,
		Error:         &model.JobError{Message: "embedding endpoint is unreachable; verify OPENAI_BASE_URL", Retryable: true},
	}

	degraded := render.GetJob(resolveJobEntry(*job, true, ""), true)
	if strings.Contains(degraded, "Error:") {
		t.Fatalf("degraded job view echoed the banner cause:\n%s", degraded)
	}
	if !strings.Contains(degraded, "State: failed, retryable") {
		t.Fatalf("degraded job view missing retryable state:\n%s", degraded)
	}

	healthy := render.GetJob(resolveJobEntry(*job, false, ""), true)
	if !strings.Contains(healthy, "Error:") {
		t.Fatalf("non-degraded job view should keep the error line:\n%s", healthy)
	}
}

// During a degraded pipeline, GetIndex shows exactly one banner, the waiting
// body, and one correlation header, with no blank lines from the envelope.
func TestGetIndexDegradedEnvelope(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	// The active health probe runs on this indexed path, so the fake must report
	// the same outage the test simulates or the probe would clear the banner.
	manager.semantic = &fakeSemantic{probeErr: adapterr.NewEmbedderUnreachable(nil)}
	canonical, err := filepath.EvalSymlinks(repoPath)
	if err != nil {
		t.Fatalf("EvalSymlinks returned error: %v", err)
	}

	codebase := newCodebaseRecord(canonical)
	codebase.Status = model.CodebaseStatusIndexing
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.health = dependencyHealth{Mode: dependencyEmbedderUnreachable, Since: clock.Now(), StoreReachableAt: clock.Now(), EmbedderReachableAt: clock.Now()}
	manager.mu.Unlock()

	server := NewGRPCServer(manager, nil)
	resp, err := server.GetIndex(context.Background(), &pb.GetIndexRequest{Path: repoPath})
	if err != nil {
		t.Fatalf("GetIndex returned error: %v", err)
	}
	text := resp.GetDisplayText()

	if strings.Count(text, "🟥") != 1 {
		t.Fatalf("want exactly one banner, got:\n%s", text)
	}
	if !strings.Contains(text, "Embedding server unreachable") {
		t.Fatalf("missing unreachable banner:\n%s", text)
	}
	if !strings.Contains(text, "⏳ Waiting for the embedding server") {
		t.Fatalf("codebase should read waiting, got:\n%s", text)
	}
	if strings.Count(text, "🔎 trace_id=") != 1 {
		t.Fatalf("want exactly one correlation header, got:\n%s", text)
	}
	if strings.Contains(text, "❌") {
		t.Fatalf("a degraded pipeline must not read as a codebase failure:\n%s", text)
	}
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "" {
			t.Fatalf("envelope produced a blank line:\n%s", text)
		}
	}
}

// Every text-bearing mutation RPC shows the degraded banner, not only the read
// surfaces: a StartIndex during an outage must carry the same warning.
func TestStartIndexShowsBannerWhenDegraded(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	manager.runner = fakeRunner{}
	// The backend must report the same outage the test simulates. StartIndex
	// launches the index job asynchronously, and a job that reaches both
	// dependencies clears the health record, so a backend that answers normally
	// makes this a race between that job and reading the display text. Reporting
	// the outage keeps the record degraded for the whole call.
	manager.semantic = &fakeSemantic{
		unavailable: true,
		probeErr:    adapterr.NewEmbedderUnreachable(nil),
	}
	manager.mu.Lock()
	manager.health = dependencyHealth{Mode: dependencyEmbedderUnreachable, Since: clock.Now(), StoreReachableAt: clock.Now(), EmbedderReachableAt: clock.Now()}
	manager.mu.Unlock()

	server := NewGRPCServer(manager, nil)
	resp, err := server.StartIndex(context.Background(), &pb.StartIndexRequest{Path: repoPath})
	if err != nil {
		t.Fatalf("StartIndex returned error: %v", err)
	}
	if !strings.Contains(resp.GetDisplayText(), "🟥") {
		t.Fatalf("StartIndex display text lacks the degraded banner:\n%s", resp.GetDisplayText())
	}
	// Let the async index job settle before the test returns, so its checkpoint
	// writes do not race t.TempDir cleanup.
	waitForCodebaseSettled(t, manager, repoPath)
}
