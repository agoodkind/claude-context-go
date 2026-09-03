package status

import (
	"strings"
	"testing"

	"goodkind.io/lm-semantic-search/internal/model"
)

// Every base display is reachable from a healthy snapshot, and the table never
// yields not_indexed for a tracked codebase.
func TestResolveDisplayBase(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   Inputs
		want Display
	}{
		{"active reconcile", Inputs{HasActiveJob: true, BackgroundSyncReconcile: true}, DisplayIndexed},
		{"active scoped", Inputs{HasActiveJob: true, JobScopeKnown: true}, DisplayIndexing},
		{"active preparing", Inputs{HasActiveJob: true}, DisplayPreparing},
		{"quarantined", Inputs{Status: model.CodebaseStatusQuarantined}, DisplayQuarantined},
		{"indexed", Inputs{Status: model.CodebaseStatusIndexed}, DisplayIndexed},
		{"stale", Inputs{Status: model.CodebaseStatusStale}, DisplayStale},
		{"failed", Inputs{Status: model.CodebaseStatusFailed}, DisplayFailed},
		{"missing", Inputs{Status: model.CodebaseStatusMissing}, DisplayMissing},
		{"interrupted indexing", Inputs{Status: model.CodebaseStatusIndexing}, DisplayPreparing},
		{"not indexed", Inputs{Status: model.CodebaseStatusNotIndexed}, DisplayPreparing},
	}
	for _, testCase := range cases {
		got := ResolveDisplay(testCase.in)
		if got != testCase.want {
			t.Errorf("%s: ResolveDisplay = %q, want %q", testCase.name, got, testCase.want)
		}
		if string(got) == string(model.CodebaseStatusNotIndexed) {
			t.Errorf("%s: a tracked codebase must never present as not_indexed", testCase.name)
		}
	}
}

// A just-requested codebase reads as pending: a queued live job, the persisted
// pending status, and a discovered worktree all present as pending or discovered,
// never as waiting or store-down. An indexed codebase whose own collection is not
// loaded (store healthy) reads as loading, also not store-down.
func TestResolvePendingAndLoading(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   Inputs
		want Display
	}{
		{"queued job is pending", Inputs{HasActiveJob: true, JobQueued: true}, DisplayPending},
		{"pending status is pending", Inputs{Status: model.CodebaseStatusPending}, DisplayPending},
		{"running scoped is indexing", Inputs{HasActiveJob: true, JobScopeKnown: true}, DisplayIndexing},
		{"indexed collection loading reads loading", Inputs{Status: model.CodebaseStatusIndexed, Collection: CollectionLoading}, DisplayLoading},
		{"indexed collection absent reads missing", Inputs{Status: model.CodebaseStatusIndexed, Collection: CollectionAbsent}, DisplayMissing},
		{"indexed collection ready stays indexed", Inputs{Status: model.CodebaseStatusIndexed, Collection: CollectionReady}, DisplayIndexed},
	}
	for _, testCase := range cases {
		got := ResolveDisplay(testCase.in)
		if got != testCase.want {
			t.Errorf("%s: ResolveDisplay = %q, want %q", testCase.name, got, testCase.want)
		}
		if surface := Resolve(testCase.in); surface.BannerPresent {
			t.Errorf("%s: pending/loading must not raise the global banner", testCase.name)
		}
	}
}

func TestPausedCodebaseResolvesWaiting(t *testing.T) {
	t.Parallel()

	surface := Resolve(Inputs{
		Status:        model.CodebaseStatusIndexing,
		HasActiveJob:  true,
		JobPaused:     true,
		JobScopeKnown: true,
	})
	if surface.Display != DisplayWaiting {
		t.Fatalf("paused display = %q, want %q", surface.Display, DisplayWaiting)
	}
}

func TestResolveIdleCollectionAcceptsSearch(t *testing.T) {
	t.Parallel()

	surface := Resolve(Inputs{
		Status:             model.CodebaseStatusIndexed,
		Collection:         CollectionIdle,
		SearchableEligible: true,
	})
	if surface.Display != DisplayIdle {
		t.Fatalf("Display = %q, want %q", surface.Display, DisplayIdle)
	}
	if !surface.Searchable {
		t.Fatal("Searchable = false, want true for an idle collection")
	}
}

func TestResolveCollectionResidencyMatrix(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		readiness   CollectionReadiness
		wantDisplay Display
		searchable  bool
	}{
		{"absent", CollectionAbsent, DisplayMissing, false},
		{"idle", CollectionIdle, DisplayIdle, true},
		{"loading", CollectionLoading, DisplayLoading, true},
		{"ready", CollectionReady, DisplayIndexed, true},
		{"unknown", CollectionUnknown, DisplayIndexed, false},
	}
	for _, testCase := range testCases {
		surface := Resolve(Inputs{
			Status:             model.CodebaseStatusIndexed,
			Collection:         testCase.readiness,
			SearchableEligible: true,
		})
		if surface.Display != testCase.wantDisplay {
			t.Errorf("%s: Display = %q, want %q", testCase.name, surface.Display, testCase.wantDisplay)
		}
		if surface.Searchable != testCase.searchable {
			t.Errorf("%s: Searchable = %v, want %v", testCase.name, surface.Searchable, testCase.searchable)
		}
	}
}

func TestResolveQuarantined(t *testing.T) {
	t.Parallel()
	surface := Resolve(Inputs{Status: model.CodebaseStatusQuarantined})
	if surface.Display != DisplayQuarantined {
		t.Fatalf("Display = %q, want %q", surface.Display, DisplayQuarantined)
	}
	if surface.Glyph != "⚠" {
		t.Fatalf("Glyph = %q, want ⚠", surface.Glyph)
	}
	if surface.Label != "quarantined" {
		t.Fatalf("Label = %q, want quarantined", surface.Label)
	}
}

// A discovered codebase with no active job resolves to the discovered display
// and carries the discovered glyph and label, so the registered-but-unbuilt
// worktree reads distinctly from indexing and from not_indexed.
func TestResolveDiscovered(t *testing.T) {
	t.Parallel()
	surface := Resolve(Inputs{Status: model.CodebaseStatusDiscovered})
	if surface.Display != DisplayDiscovered {
		t.Fatalf("Display = %q, want %q", surface.Display, DisplayDiscovered)
	}
	if surface.Glyph != "⊙" {
		t.Fatalf("Glyph = %q, want ⊙", surface.Glyph)
	}
	if surface.Label != "discovered" {
		t.Fatalf("Label = %q, want discovered", surface.Label)
	}
	if GlyphFor(DisplayDiscovered) != "⊙" {
		t.Fatalf("GlyphFor(discovered) = %q, want ⊙", GlyphFor(DisplayDiscovered))
	}
	if LabelFor(DisplayDiscovered) != "discovered" {
		t.Fatalf("LabelFor(discovered) = %q, want discovered", LabelFor(DisplayDiscovered))
	}
}

// The degraded fold turns a not-embedding codebase into waiting (preparing and
// indexed), while a live scoped job stays indexing and a local terminal state is
// untouched. This is the rule that keeps a surface from reading "ready" while a
// search would fail.
func TestResolveDisplayDegradedFold(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   Inputs
		want Display
	}{
		{"interrupted folds to waiting", Inputs{Status: model.CodebaseStatusIndexing, Dependency: EmbedderBusy}, DisplayWaiting},
		{"not indexed folds to waiting", Inputs{Status: model.CodebaseStatusNotIndexed, Dependency: EmbedderUnreachable}, DisplayWaiting},
		{"indexed folds to waiting", Inputs{Status: model.CodebaseStatusIndexed, Dependency: EmbedderBusy}, DisplayWaiting},
		{"reconcile folds to waiting", Inputs{HasActiveJob: true, BackgroundSyncReconcile: true, Dependency: StoreUnavailable}, DisplayWaiting},
		{"live scoped stays indexing", Inputs{HasActiveJob: true, JobScopeKnown: true, Dependency: EmbedderBusy}, DisplayIndexing},
		{"stale stays stale", Inputs{Status: model.CodebaseStatusStale, Dependency: EmbedderBusy}, DisplayStale},
		{"failed stays failed", Inputs{Status: model.CodebaseStatusFailed, Dependency: EmbedderBusy}, DisplayFailed},
		{"missing stays missing", Inputs{Status: model.CodebaseStatusMissing, Dependency: EmbedderBusy}, DisplayMissing},
	}
	for _, testCase := range cases {
		got := ResolveDisplay(testCase.in)
		if got != testCase.want {
			t.Errorf("%s: ResolveDisplay(degraded) = %q, want %q", testCase.name, got, testCase.want)
		}
	}
}

// Every display has a non-empty glyph and label, and the lookups fall back
// safely for an unknown value.
func TestGlyphAndLabelCoverEveryDisplay(t *testing.T) {
	t.Parallel()
	for _, display := range []Display{
		DisplayPreparing, DisplayIndexing, DisplayIndexed, DisplayWaiting,
		DisplayStale, DisplayFailed, DisplayMissing,
	} {
		if GlyphFor(display) == "" {
			t.Errorf("%s: empty glyph", display)
		}
		if LabelFor(display) == "" {
			t.Errorf("%s: empty label", display)
		}
	}
	if GlyphFor(Display("bogus")) != "•" {
		t.Error("unknown display should fall back to the bullet glyph")
	}
	if LabelFor(Display("bogus")) != "bogus" {
		t.Error("unknown display should fall back to its raw token")
	}
}

// Healthy has no banner; every degraded mode resolves a non-empty headline, and
// an unrecognized degraded mode falls back to the generic headline.
func TestBannerHeadlineCoversEveryMode(t *testing.T) {
	t.Parallel()
	if BannerHeadlineFor(Healthy) != "" {
		t.Error("healthy mode must have no banner headline")
	}
	for _, mode := range []DependencyMode{EmbedderUnreachable, EmbedderRejected, EmbedderPaused, EmbedderBusy, StoreUnavailable} {
		if !mode.Degraded() {
			t.Errorf("%s should report degraded", mode)
		}
		if BannerHeadlineFor(mode) == "" {
			t.Errorf("%s: empty banner headline", mode)
		}
	}
	if BannerHeadlineFor(DependencyMode("future_mode")) != genericDegradedHeadline {
		t.Error("unknown degraded mode should fall back to the generic headline")
	}
}

func TestEmbedderBusyBannerReportsThrottlingWithoutClaimingPause(t *testing.T) {
	t.Parallel()

	headline := BannerHeadlineFor(EmbedderBusy)
	if !strings.Contains(headline, "throttling requests") {
		t.Fatalf("busy headline does not report throttling: %q", headline)
	}
	// Saying what throttling is not leaves room for a replacement that stops
	// short of the word "paused" and still tells the operator work has halted,
	// so the headline has to say outright that it continues.
	if !strings.Contains(headline, "Active indexing may continue between rejected batches") {
		t.Fatalf("busy headline does not report that indexing continues: %q", headline)
	}
	if strings.Contains(headline, "paused") {
		t.Fatalf("busy headline claims indexing stopped: %q", headline)
	}
}

// Resolve sets BannerPresent from the mode and fills glyph and label from the
// resolved display, for both a healthy and a degraded snapshot.
func TestResolveSurfaceBanner(t *testing.T) {
	t.Parallel()
	healthy := Resolve(Inputs{Status: model.CodebaseStatusIndexed, Dependency: Healthy})
	if healthy.BannerPresent || healthy.BannerHeadline != "" {
		t.Errorf("healthy surface must carry no banner: %+v", healthy)
	}
	if healthy.Display != DisplayIndexed || healthy.Glyph != GlyphFor(DisplayIndexed) || healthy.Label != LabelFor(DisplayIndexed) {
		t.Errorf("healthy indexed surface mismatched: %+v", healthy)
	}
	degraded := Resolve(Inputs{Status: model.CodebaseStatusIndexed, Dependency: EmbedderBusy})
	if !degraded.BannerPresent || degraded.BannerHeadline == "" {
		t.Errorf("degraded surface must carry a banner: %+v", degraded)
	}
	if degraded.Display != DisplayWaiting {
		t.Errorf("degraded indexed surface should fold to waiting: %+v", degraded)
	}
}

// Each search outcome maps to its state note: only the repairing outcome carries
// text; the rest are silent.
func TestStateNoteForEveryOutcome(t *testing.T) {
	t.Parallel()
	withNote := map[SearchOutcome]bool{
		SearchNone:      false,
		SearchOK:        false,
		SearchEmpty:     false,
		SearchRepairing: true,
		SearchNotReady:  false,
		SearchMissing:   false,
		SearchFallback:  false,
	}
	for outcome, wantNote := range withNote {
		note := StateNoteFor(outcome)
		if wantNote && note == "" {
			t.Errorf("%s: expected a state note", outcome)
		}
		if !wantNote && note != "" {
			t.Errorf("%s: expected no state note, got %q", outcome, note)
		}
	}
}

// Every job state maps to a non-empty word, cancelling and cancelled use the
// American spellings, and an unknown state falls back to its raw token.
func TestJobStateLabelForCoversEveryState(t *testing.T) {
	t.Parallel()
	for _, state := range []model.JobState{
		model.JobStateQueued, model.JobStateRunning, model.JobStatePaused, model.JobStateCancelling,
		model.JobStateCompleted, model.JobStateFailed, model.JobStateCancelled,
	} {
		if JobStateLabelFor(state) == "" {
			t.Errorf("%s: empty job state label", state)
		}
	}
	if got := JobStateLabelFor(model.JobStateCancelling); got != "canceling" {
		t.Errorf("cancelling label = %q, want canceling", got)
	}
	if got := JobStateLabelFor(model.JobStateCancelled); got != "canceled" {
		t.Errorf("cancelled label = %q, want canceled", got)
	}
	if got := JobStateLabelFor(model.JobState("bogus")); got != "bogus" {
		t.Errorf("unknown state should fall back to its raw token, got %q", got)
	}
}

// ResolveJob adds the retryable suffix to a self-healing failure, and suppresses
// the error echo only when a retryable failure coincides with a degraded
// dependency, since the banner then already carries the cause.
func TestResolveJob(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name           string
		in             JobInputs
		wantLabel      string
		wantError      string
		wantSuperseded bool
	}{
		{
			"running healthy",
			JobInputs{State: model.JobStateRunning},
			"running", "", false,
		},
		{
			"retryable failure, healthy, shows error",
			JobInputs{State: model.JobStateFailed, Retryable: true, ErrorMessage: "embedding endpoint is unreachable"},
			"failed, retryable", "embedding endpoint is unreachable", false,
		},
		{
			"retryable failure, degraded, suppresses echo",
			JobInputs{State: model.JobStateFailed, Retryable: true, ErrorMessage: "embedding endpoint is unreachable", Dependency: EmbedderUnreachable},
			"failed, retryable", "", false,
		},
		{
			"superseded retryable failure",
			JobInputs{State: model.JobStateFailed, Retryable: true, ErrorMessage: "boom", SupersededByJobID: "job_B"},
			"failed, retryable, superseded by job_B", "boom", true,
		},
		{
			"superseded hard failure",
			JobInputs{State: model.JobStateFailed, ErrorMessage: "internal error", SupersededByJobID: "job_B"},
			"failed, superseded by job_B", "internal error", true,
		},
		{
			"completed job with a successor is not superseded",
			JobInputs{State: model.JobStateCompleted, SupersededByJobID: "job_B"},
			"completed", "", false,
		},
	}
	for _, testCase := range cases {
		got := ResolveJob(testCase.in)
		if got.StateLabel != testCase.wantLabel {
			t.Errorf("%s: StateLabel = %q, want %q", testCase.name, got.StateLabel, testCase.wantLabel)
		}
		if got.ErrorLine != testCase.wantError {
			t.Errorf("%s: ErrorLine = %q, want %q", testCase.name, got.ErrorLine, testCase.wantError)
		}
		if got.Superseded != testCase.wantSuperseded {
			t.Errorf("%s: Superseded = %v, want %v", testCase.name, got.Superseded, testCase.wantSuperseded)
		}
	}
}
