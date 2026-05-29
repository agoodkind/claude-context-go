package daemon

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"goodkind.io/claude-context-go/internal/clock"
	"goodkind.io/claude-context-go/internal/indexer"
	"goodkind.io/claude-context-go/internal/merkle"
	"goodkind.io/claude-context-go/internal/model"
)

// recordingRunner returns a fakeRunner whose IndexOne appends each embedded
// relative path to embedded and returns one chunk per file, so a test can
// assert exactly which files a from-scratch build re-embedded.
func recordingRunner(mu *sync.Mutex, embedded *[]string) fakeRunner {
	return fakeRunner{
		index:      nil,
		indexFiles: nil,
		indexOne: func(_ context.Context, _ string, relativePath string, _ model.IndexConfig) (indexer.OneFileResult, error) {
			mu.Lock()
			*embedded = append(*embedded, relativePath)
			mu.Unlock()
			content := "package main\n// " + relativePath + "\n"
			return indexer.OneFileResult{
				Chunks:   []model.StoredChunk{{Content: content, RelativePath: relativePath, StartLine: 1, EndLine: 1, Language: "go", FileExtension: ".go"}},
				FileHash: hashText(content),
				Skipped:  false,
				Removed:  false,
			}, nil
		},
	}
}

// newMultiFileRepo writes one valid Go file per name under a fresh repo
// directory and returns its canonicalized path.
func newMultiFileRepo(t *testing.T, names ...string) string {
	t.Helper()

	repoPath := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(repoPath, 0o755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(repoPath, name), []byte("package main\n// "+name+"\n"), 0o644); err != nil {
			t.Fatalf("WriteFile returned error: %v", err)
		}
	}
	canonical, err := filepath.EvalSymlinks(repoPath)
	if err != nil {
		t.Fatalf("EvalSymlinks returned error: %v", err)
	}
	return canonical
}

// seedBootstrapCodebase registers a from-scratch build target and returns the
// job runBootstrap consumes. The codebase starts in the indexing state so the
// run mirrors a resume after an interrupted first index.
func seedBootstrapCodebase(t *testing.T, manager *Manager, canonical string, cfg model.IndexConfig) (string, model.Job) {
	t.Helper()

	codebaseID := "cb-bootstrap-" + filepath.Base(filepath.Dir(canonical))
	manager.mu.Lock()
	manager.codebases[codebaseID] = model.Codebase{
		ID:              codebaseID,
		CanonicalPath:   canonical,
		Status:          model.CodebaseStatusIndexing,
		EffectiveConfig: cfg,
	}
	manager.mu.Unlock()

	job := newQueuedJob(codebaseID, canonical, canonical, testClientInfo(), string(jobOperationIndex), cfg, clock.Now())
	manager.mu.Lock()
	manager.jobs[job.ID] = job
	manager.mu.Unlock()
	return codebaseID, job
}

func TestRunBootstrapResumesSkippingEmbeddedFiles(t *testing.T) {
	manager, _ := newTestManagerWithCap(t, 2)
	var mu sync.Mutex
	embedded := make([]string, 0)
	manager.runner = recordingRunner(&mu, &embedded)

	canonical := newMultiFileRepo(t, "a.go", "b.go", "c.go")
	cfg := defaultIndexConfig()
	cfg.IgnoreDigest = "sha256:bootstrap-resume"

	captured, err := merkle.Capture(context.Background(), canonical, cfg)
	if err != nil {
		t.Fatalf("Capture returned error: %v", err)
	}

	codebaseID, job := seedBootstrapCodebase(t, manager, canonical, cfg)

	// A checkpoint recording only a.go as embedded; the resume must skip it
	// and embed only b.go and c.go.
	checkpoint := merkle.Snapshot{
		ConfigDigest: cfg.IgnoreDigest,
		Files:        map[string]string{"a.go": captured.Files["a.go"]},
		Inodes:       nil,
	}
	if err := merkle.WriteSnapshot(manager.merklePath(codebaseID), checkpoint); err != nil {
		t.Fatalf("WriteSnapshot returned error: %v", err)
	}

	manager.runBootstrap(context.Background(), job)

	mu.Lock()
	slices.Sort(embedded)
	got := slices.Clone(embedded)
	mu.Unlock()
	if want := []string{"b.go", "c.go"}; !slices.Equal(got, want) {
		t.Fatalf("embedded files = %v, want %v (a.go must be skipped via checkpoint)", got, want)
	}

	finalSnapshot, err := merkle.ReadSnapshot(manager.merklePath(codebaseID))
	if err != nil {
		t.Fatalf("ReadSnapshot returned error: %v", err)
	}
	if len(finalSnapshot.Files) != 3 {
		t.Fatalf("final snapshot files = %d, want 3 (a.go, b.go, c.go)", len(finalSnapshot.Files))
	}
	waitForCodebaseStatus(t, manager, canonical, model.CodebaseStatusIndexed)
}

func TestRunBootstrapEmbedsEveryFileWithoutCheckpoint(t *testing.T) {
	manager, _ := newTestManagerWithCap(t, 2)
	var mu sync.Mutex
	embedded := make([]string, 0)
	manager.runner = recordingRunner(&mu, &embedded)

	canonical := newMultiFileRepo(t, "a.go", "b.go", "c.go")
	cfg := defaultIndexConfig()
	cfg.IgnoreDigest = "sha256:bootstrap-fresh"

	_, job := seedBootstrapCodebase(t, manager, canonical, cfg)

	manager.runBootstrap(context.Background(), job)

	mu.Lock()
	slices.Sort(embedded)
	got := slices.Clone(embedded)
	mu.Unlock()
	if want := []string{"a.go", "b.go", "c.go"}; !slices.Equal(got, want) {
		t.Fatalf("embedded files = %v, want %v (a full build embeds every file)", got, want)
	}
	waitForCodebaseStatus(t, manager, canonical, model.CodebaseStatusIndexed)
}

func TestResumeOrphanedJobsMarksNoCheckpointInterruptedFailed(t *testing.T) {
	manager, _ := newTestManagerWithCap(t, 2)
	manager.config.ResumeIndexingOnBoot = true

	canonical := newMultiFileRepo(t, "a.go")
	codebaseID := "cb-no-checkpoint"
	manager.mu.Lock()
	manager.codebases[codebaseID] = model.Codebase{
		ID:              codebaseID,
		CanonicalPath:   canonical,
		Status:          model.CodebaseStatusIndexing,
		EffectiveConfig: defaultIndexConfig(),
	}
	manager.mu.Unlock()

	manager.ResumeOrphanedJobs(context.Background())

	if jobs := manager.ListJobs(""); len(jobs) != 0 {
		t.Fatalf("no-checkpoint resume launched %d jobs, want 0", len(jobs))
	}

	manager.mu.Lock()
	codebase := manager.codebases[codebaseID]
	manager.mu.Unlock()
	if codebase.Status != model.CodebaseStatusFailed {
		t.Fatalf("codebase status = %q, want %q so it is not stuck showing indexing", codebase.Status, model.CodebaseStatusFailed)
	}
	if codebase.LastFailedRun == nil {
		t.Fatal("LastFailedRun not recorded for an unresumable interrupted index")
	}
}
