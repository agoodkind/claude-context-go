package daemon

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"goodkind.io/lm-semantic-search/internal/indexer"
	"goodkind.io/lm-semantic-search/internal/model"
)

// indexedSiblingScene builds a main repo with one already-indexed sibling
// codebase registered directly (no live build), plus an untracked linked
// worktree the daemon can discover on a read. It pins deferredBuildDelay to an
// hour so the deferred build timer never fires mid-test, and returns the
// manager and the canonical worktree root the discovery read should resolve to.
func indexedSiblingScene(t *testing.T) (*Manager, string) {
	t.Helper()

	manager, _, _ := newTestManager(t)
	manager.runner = fakeRunner{}
	manager.deferredBuildDelay = time.Hour
	manager.semantic = &fakeSemantic{
		hasCollectionForPath: func(context.Context, string) (bool, error) { return false, nil },
	}

	base := t.TempDir()
	mainRoot := filepath.Join(base, "repo")
	makeMainRepo(t, mainRoot)
	worktreeDir := filepath.Join(base, "feature")
	makeLinkedWorktree(t, mainRoot, "feature", worktreeDir, "feature")

	// Register the main worktree as an already-indexed sibling so the discovery
	// trigger's "has an indexed sibling" condition holds without driving a build.
	registerSiblingCodebase(t, manager, mainRoot, func(c *model.Codebase) {
		c.Status = model.CodebaseStatusIndexed
		c.LastSuccessfulRun = &model.IndexRunSummary{IndexedFiles: 1, TotalChunks: 1, Status: "completed"}
	})

	return manager, evalSym(t, worktreeDir)
}

// jobsForCodebase counts the jobs the manager tracks for one codebase id.
func jobsForCodebase(manager *Manager, codebaseID string) int {
	return len(manager.ListJobs(codebaseID))
}

// TestGetIndexDiscoversWorktreeWithoutJob proves a status read of an untracked
// worktree of an indexed sibling registers it as a discovered codebase with no
// active job and starts no job synchronously, because the build is deferred.
func TestGetIndexDiscoversWorktreeWithoutJob(t *testing.T) {
	manager, worktreeRoot := indexedSiblingScene(t)

	codebase, activeJob, found, _, err := manager.GetIndex(context.Background(), worktreeRoot)
	if err != nil {
		t.Fatalf("GetIndex(worktree) returned error: %v", err)
	}
	if !found {
		t.Fatal("GetIndex(worktree) did not resolve to a codebase")
	}
	if codebase.Status != model.CodebaseStatusDiscovered {
		t.Fatalf("discovered codebase Status = %q, want %q", codebase.Status, model.CodebaseStatusDiscovered)
	}
	if !codebase.PolicyPendingInitialization {
		t.Fatal("discovered codebase PolicyPendingInitialization = false, want true before its first build")
	}
	if codebase.CanonicalPath != worktreeRoot {
		t.Fatalf("discovered codebase CanonicalPath = %q, want %q", codebase.CanonicalPath, worktreeRoot)
	}
	if codebase.ActiveJobID != "" {
		t.Fatalf("discovered codebase ActiveJobID = %q, want empty", codebase.ActiveJobID)
	}
	if activeJob != nil {
		t.Fatalf("GetIndex returned active job %s, want nil for a discovered codebase", activeJob.ID)
	}
	if got := jobsForCodebase(manager, codebase.ID); got != 0 {
		t.Fatalf("discovered codebase has %d jobs, want 0 (the read must not start an embed)", got)
	}
}

func TestGetIndexDiscoversWorktreeWithExistingCollectionInitialized(t *testing.T) {
	manager, worktreeRoot := indexedSiblingScene(t)
	manager.semantic = &fakeSemantic{
		hasCollectionForPath: func(context.Context, string) (bool, error) { return true, nil },
	}

	codebase, _, found, _, err := manager.GetIndex(context.Background(), worktreeRoot)
	if err != nil || !found {
		t.Fatalf("GetIndex(worktree) returned err=%v found=%v", err, found)
	}
	if codebase.PolicyPendingInitialization {
		t.Fatal("discovered worktree with an existing collection PolicyPendingInitialization = true, want false")
	}
}

func TestGetIndexDiscoversWorktreeWithoutCollectionEvidencePendingInitialization(t *testing.T) {
	manager, worktreeRoot := indexedSiblingScene(t)
	manager.semantic = nil

	codebase, _, found, _, err := manager.GetIndex(context.Background(), worktreeRoot)
	if err != nil || !found {
		t.Fatalf("GetIndex(worktree) returned err=%v found=%v", err, found)
	}
	if !codebase.PolicyPendingInitialization {
		t.Fatal("worktree without collection evidence PolicyPendingInitialization = false, want true")
	}
}

// TestSearchCodeDiscoversWorktreeReturnsForecastNote proves a search of the same
// untracked worktree returns no results, a non-empty discovery state note, and
// starts no job. It must not fall back to the indexed sibling's results.
func TestSearchCodeDiscoversWorktreeReturnsForecastNote(t *testing.T) {
	manager, worktreeRoot := indexedSiblingScene(t)

	outcome, err := manager.SearchCode(context.Background(), worktreeRoot, "anything", 5, nil)
	if err != nil {
		t.Fatalf("SearchCode(worktree) returned error: %v", err)
	}
	if len(outcome.Results) != 0 {
		t.Fatalf("SearchCode returned %d results, want 0 for a just-discovered worktree", len(outcome.Results))
	}
	if outcome.StateNote == "" {
		t.Fatal("SearchCode returned an empty StateNote, want a discovery note")
	}
	if outcome.Codebase.Status != model.CodebaseStatusDiscovered {
		t.Fatalf("SearchCode outcome codebase Status = %q, want %q", outcome.Codebase.Status, model.CodebaseStatusDiscovered)
	}
	if got := jobsForCodebase(manager, outcome.Codebase.ID); got != 0 {
		t.Fatalf("discovered codebase has %d jobs after search, want 0", got)
	}
}

// TestStartDeferredBuildStartsOneBootstrap proves driving the deferred build
// directly starts exactly one job for the discovered worktree and flips it into
// the indexing state. The build path goes through StartIndex, which dedups.
func TestStartDeferredBuildStartsOneBootstrap(t *testing.T) {
	manager, worktreeRoot := indexedSiblingScene(t)

	// Hold the per-file embed open so the codebase stays in the indexing state
	// long enough to observe, then release it for a clean teardown.
	release := make(chan struct{})
	manager.runner = fakeRunner{
		indexOne: func(ctx context.Context, root string, relativePath string, indexConfig model.IndexConfig) (indexer.OneFileResult, error) {
			<-release
			content := "package feature\n"
			return indexer.OneFileResult{
				Chunks: []model.StoredChunk{{
					Content:       content,
					RelativePath:  relativePath,
					StartLine:     1,
					EndLine:       1,
					Language:      "go",
					FileExtension: ".go",
				}},
				FileHash: hashText(content),
				Skipped:  false,
				Removed:  false,
			}, nil
		},
	}

	// First discover the worktree so a discovered codebase record exists.
	discovered, _, found, _, err := manager.GetIndex(context.Background(), worktreeRoot)
	if err != nil || !found {
		t.Fatalf("GetIndex(worktree) returned err=%v found=%v", err, found)
	}
	if got := jobsForCodebase(manager, discovered.ID); got != 0 {
		t.Fatalf("pre-build: discovered codebase has %d jobs, want 0", got)
	}

	manager.startDeferredBuild(context.Background(), worktreeRoot)

	waitForCodebaseStatus(t, manager, worktreeRoot, model.CodebaseStatusIndexing)
	codebase, _, found, _, err := manager.GetIndex(context.Background(), worktreeRoot)
	if err != nil || !found {
		t.Fatalf("GetIndex(worktree) after build start returned err=%v found=%v", err, found)
	}
	if !codebase.PolicyPendingInitialization {
		t.Fatal("building worktree PolicyPendingInitialization = false, want true until an explicit index initializes policy")
	}
	if got := jobsForCodebase(manager, discovered.ID); got != 1 {
		t.Fatalf("deferred build started %d jobs, want exactly 1", got)
	}

	close(release)
	waitForCodebaseStatus(t, manager, worktreeRoot, model.CodebaseStatusIndexed)
}

// TestAdoptUnregisteredCodebaseMarksIndexedAndDefersRefresh proves adoption marks
// the codebase indexed and starts no sync job synchronously: the refresh sync is
// deferred on the same timer the discovered-worktree build uses, pinned to an
// hour here so it cannot fire during the assertions.
func TestAdoptUnregisteredCodebaseMarksIndexedAndDefersRefresh(t *testing.T) {
	manager, _ := newTestManagerWithCap(t, 2)
	manager.deferredBuildDelay = time.Hour
	manager.semantic = &fakeSemantic{
		collectionName:       func(path string) string { return "cc_" + filepath.Base(path) },
		hasCollectionForPath: func(context.Context, string) (bool, error) { return true, nil },
	}

	canonical := newCapTestRepo(t)

	codebase, adopted := manager.adoptUnregisteredCodebase(context.Background(), canonical)
	if !adopted {
		t.Fatal("adoptUnregisteredCodebase returned adopted=false")
	}
	if codebase.Status != model.CodebaseStatusIndexed {
		t.Fatalf("adopted codebase Status = %q, want %q", codebase.Status, model.CodebaseStatusIndexed)
	}
	if got := jobsForCodebase(manager, codebase.ID); got != 0 {
		t.Fatalf("adoption started %d jobs synchronously, want 0 (the refresh sync is deferred)", got)
	}
}
