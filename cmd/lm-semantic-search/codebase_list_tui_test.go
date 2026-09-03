package main

import (
	"strings"
	"testing"
	"unicode/utf8"

	tea "github.com/charmbracelet/bubbletea"

	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
)

func TestListModelViewShowsRecords(t *testing.T) {
	codebases := []*pb.Codebase{
		{Id: "cb_1_aaaa", CanonicalPath: "/tmp/alpha", Status: "indexed", DisplayStatus: "indexed", GlyphToken: "✓", StatusLabel: "indexed", LastSuccessfulRun: &pb.IndexRunSummary{IndexedFiles: 12}},
		{Id: "cb_2_bbbb", CanonicalPath: "/tmp/beta", Status: "stale", DisplayStatus: "stale", GlyphToken: "!", StatusLabel: "stale"},
	}
	model := newListModel(cliOptions{}, codebases, nil)

	view := model.View()
	for _, want := range []string{"cb_1_aaaa", "cb_2_bbbb", "alpha", "beta", "indexed", "stale", "12", "NAME", "STATUS", "PATH"} {
		if !strings.Contains(view, want) {
			t.Errorf("View() missing %q\n%s", want, view)
		}
	}
}

func TestListModelViewShowsStoredSchedulingPolicy(t *testing.T) {
	codebases := []*pb.Codebase{{
		Id:            "cb_1_aaaa",
		CanonicalPath: "/tmp/alpha",
		DisplayStatus: "indexed",
		GlyphToken:    "✓",
		StatusLabel:   "indexed",
		SchedulingPolicy: &pb.SchedulingPolicy{
			Priority:         pb.SchedulingPriority_SCHEDULING_PRIORITY_LOW,
			Quiet:            true,
			IdleAfterSeconds: 600,
		},
	}}
	model := newListModel(cliOptions{}, codebases, nil)
	model.width = 160

	output := model.View()
	for _, want := range []string{
		"Stored policy:",
		"priority=low · quiet=on · idle_after=10m",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("View() missing scheduling value %q\n%s", want, output)
		}
	}
}

// TestListRowRendersDaemonTokens proves a row renders the daemon-provided glyph
// and label tokens rather than a client-side map, so the CLI shares the daemon's
// status vocabulary including "waiting".
func TestListRowRendersDaemonTokens(t *testing.T) {
	codebases := []*pb.Codebase{
		{Id: "cb_1_aaaa", CanonicalPath: "/tmp/alpha", Status: "indexing", DisplayStatus: "waiting", GlyphToken: "⋯", StatusLabel: "waiting"},
	}
	model := newListModel(cliOptions{}, codebases, nil)

	view := model.View()
	for _, want := range []string{"⋯", "waiting"} {
		if !strings.Contains(view, want) {
			t.Errorf("View() missing daemon token %q\n%s", want, view)
		}
	}
}

// TestListRowDoesNotFallbackToRawStatusWithoutTokens proves a row from an older
// daemon that omits resolved tokens does not reintroduce the raw status word.
func TestListRowDoesNotFallbackToRawStatusWithoutTokens(t *testing.T) {
	codebases := []*pb.Codebase{
		{Id: "cb_1_aaaa", CanonicalPath: "/tmp/alpha", Status: "indexed"},
	}
	model := newListModel(cliOptions{}, codebases, nil)

	view := model.View()
	if strings.Contains(view, "indexed") {
		t.Errorf("View() rendered fallback raw status:\n%s", view)
	}
}

// TestListViewShowsDegradedBanner proves the interactive list renders the
// degraded-dependency banner above the table when the daemon reports an outage,
// and shows none when healthy, so the TUI carries the same warning as the piped
// surfaces.
func TestListViewShowsDegradedBanner(t *testing.T) {
	codebases := []*pb.Codebase{
		{Id: "cb_1_aaaa", CanonicalPath: "/tmp/alpha", Status: "indexed", DisplayStatus: "waiting", GlyphToken: "⋯", StatusLabel: "waiting"},
	}
	degraded := newListModel(cliOptions{}, codebases, &pb.DependencyHealth{Degraded: true, Mode: "embedder_unreachable"})
	view := degraded.View()
	if !strings.Contains(view, "🟥") {
		t.Errorf("degraded list view missing banner marker:\n%s", view)
	}
	if !strings.Contains(view, "unreachable") {
		t.Errorf("degraded list view missing banner headline:\n%s", view)
	}

	healthy := newListModel(cliOptions{}, codebases, &pb.DependencyHealth{Degraded: false})
	if strings.Contains(healthy.View(), "🟥") {
		t.Errorf("healthy list view should carry no banner:\n%s", healthy.View())
	}
}

func TestFitHeadKeepsTail(t *testing.T) {
	got := fitHead("/Users/agoodkind/Sites/lmd", 8)
	if !strings.HasSuffix(got, "lmd") {
		t.Errorf("fitHead kept the wrong end: %q", got)
	}
	if !strings.HasPrefix(got, "…") {
		t.Errorf("fitHead missing leading ellipsis: %q", got)
	}
	if utf8.RuneCountInString(got) != 8 {
		t.Errorf("fitHead width = %d runes, want 8: %q", utf8.RuneCountInString(got), got)
	}
}

func TestListModelNavigationMovesSelection(t *testing.T) {
	codebases := []*pb.Codebase{
		{Id: "cb_1_aaaa", CanonicalPath: "/tmp/alpha", Status: "indexed"},
		{Id: "cb_2_bbbb", CanonicalPath: "/tmp/beta", Status: "stale"},
	}
	model := newListModel(cliOptions{}, codebases, nil)
	if model.cursor != 0 {
		t.Fatalf("initial cursor = %d, want 0", model.cursor)
	}

	downModel, _ := model.Update(tea.KeyMsg{Type: tea.KeyDown})
	moved, ok := downModel.(listModel)
	if !ok {
		t.Fatalf("Update did not return a listModel")
	}
	if moved.cursor != 1 {
		t.Errorf("cursor after down = %d, want 1", moved.cursor)
	}

	upModel, _ := moved.Update(tea.KeyMsg{Type: tea.KeyUp})
	back, ok := upModel.(listModel)
	if !ok {
		t.Fatalf("Update did not return a listModel")
	}
	if back.cursor != 0 {
		t.Errorf("cursor after up = %d, want 0", back.cursor)
	}
}

func TestListModelRefreshPreservesSelectionByID(t *testing.T) {
	codebases := []*pb.Codebase{
		{Id: "cb_1_aaaa", CanonicalPath: "/tmp/alpha", Status: "indexed"},
		{Id: "cb_2_bbbb", CanonicalPath: "/tmp/beta", Status: "stale"},
	}
	model := newListModel(cliOptions{}, codebases, nil)
	model.cursor = 1

	// A refresh reorders the records; the cursor should follow cb_2_bbbb.
	reordered := []*pb.Codebase{
		{Id: "cb_3_cccc", CanonicalPath: "/tmp/gamma", Status: "indexed"},
		{Id: "cb_2_bbbb", CanonicalPath: "/tmp/beta", Status: "indexed"},
		{Id: "cb_1_aaaa", CanonicalPath: "/tmp/alpha", Status: "indexed"},
	}
	refreshed := model.applyRefresh(refreshedMsg{codebases: reordered, health: nil, err: nil})
	if refreshed.codebases[refreshed.cursor].GetId() != "cb_2_bbbb" {
		t.Errorf("cursor moved off cb_2_bbbb after refresh: index=%d id=%s", refreshed.cursor, refreshed.codebases[refreshed.cursor].GetId())
	}
}

func TestListModelQuitKeySignalsQuit(t *testing.T) {
	codebases := []*pb.Codebase{
		{Id: "cb_1_aaaa", CanonicalPath: "/tmp/alpha", Status: "indexed"},
	}
	model := newListModel(cliOptions{}, codebases, nil)
	updated, cmd := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'q'}})
	quit, ok := updated.(listModel)
	if !ok {
		t.Fatalf("Update did not return a listModel")
	}
	if !quit.quitting {
		t.Error("q did not set quitting")
	}
	if cmd == nil {
		t.Error("q did not return a quit command")
	}
}
