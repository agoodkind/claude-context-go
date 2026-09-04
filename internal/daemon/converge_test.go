package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"goodkind.io/lm-semantic-search/internal/adapterr"
	"goodkind.io/lm-semantic-search/internal/indexer"
	"goodkind.io/lm-semantic-search/internal/merkle"
	"goodkind.io/lm-semantic-search/internal/model"
)

// seedConvergeCodebase registers an indexed codebase rooted at repoPath, so a
// converge test starts from a tracked codebase without repeating the registry
// setup.
func seedConvergeCodebase(t *testing.T, manager *Manager, repoPath string) model.Codebase {
	t.Helper()

	indexConfig := defaultIndexConfig()
	indexConfig.IgnoreDigest = digestIndexConfig(indexConfig)
	codebase := newCodebaseRecord(repoPath)
	codebase.Status = model.CodebaseStatusIndexed
	codebase.EffectiveConfig = indexConfig
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.mu.Unlock()
	return codebase
}

func writeConvergePresentFileInfo(t *testing.T, repoPath string, relativePath string) os.FileInfo {
	t.Helper()

	absolutePath := filepath.Join(repoPath, relativePath)
	if err := os.WriteFile(absolutePath, []byte("package present\n"), 0o600); err != nil {
		t.Fatalf("write %s: %v", relativePath, err)
	}
	info, err := os.Lstat(absolutePath)
	if err != nil {
		t.Fatalf("Lstat %s: %v", relativePath, err)
	}
	return info
}

// TestConvergePathsReportsWhatItConverged proves the caller learns how many of
// the paths it handed over actually reached the index. A job built around this
// call reports that count as its scope, so a wrong count is a wrong status.
func TestConvergePathsReportsWhatItConverged(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	codebase := seedConvergeCodebase(t, manager, repoPath)
	manager.semantic = &fakeSemantic{
		reindex: func(context.Context, string, []model.StoredChunk, []string) error { return nil },
	}

	present := filepath.Join(repoPath, "present.go")
	if err := os.WriteFile(present, []byte("package main\n\nfunc Present() {}\n"), 0o644); err != nil {
		t.Fatalf("write the present file: %v", err)
	}

	outcome, err := manager.ConvergePaths(context.Background(), codebase.ID, []string{"present.go", "absent.go"}, nil)
	if err != nil {
		t.Fatalf("ConvergePaths returned error: %v", err)
	}
	if outcome.PathsGiven != 2 {
		t.Fatalf("PathsGiven = %d, want 2", outcome.PathsGiven)
	}
	if outcome.PathsConverged != 1 {
		t.Fatalf("PathsConverged = %d, want 1; only present.go exists on disk", outcome.PathsConverged)
	}
}

func TestConvergePathsRetains18227MissingPaths(t *testing.T) {
	t.Parallel()

	const missingPathCount = 18_227
	manager, _, repoPath := newTestManager(t)
	codebase := seedConvergeCodebase(t, manager, repoPath)
	writeSyntheticSnapshot(t, manager, codebase, 1)
	checkpointPath := manager.snapshotPathForCodebase(codebase)
	checkpointBytes, err := os.ReadFile(checkpointPath)
	if err != nil {
		t.Fatalf("ReadFile checkpoint: %v", err)
	}
	checkpointInfo, err := os.Stat(checkpointPath)
	if err != nil {
		t.Fatalf("Stat checkpoint: %v", err)
	}

	var indexOneCalls atomic.Int32
	manager.runner = fakeRunner{
		indexOne: func(context.Context, string, string, model.IndexConfig) (indexer.OneFileResult, error) {
			indexOneCalls.Add(1)
			return indexer.OneFileResult{Removed: true}, nil
		},
	}
	var reindexCalls atomic.Int32
	manager.semantic = &fakeSemantic{
		reindex: func(context.Context, string, []model.StoredChunk, []string) error {
			reindexCalls.Add(1)
			return nil
		},
	}

	relativePaths := make([]string, 0, missingPathCount)
	for i := 0; i < missingPathCount; i++ {
		relativePaths = append(relativePaths, fmt.Sprintf("missing/%05d.go", i))
	}

	outcome, err := manager.ConvergePaths(context.Background(), codebase.ID, relativePaths, nil)
	if err != nil {
		t.Fatalf("ConvergePaths returned error: %v", err)
	}
	if outcome.PathsGiven != missingPathCount {
		t.Fatalf("PathsGiven = %d, want %d", outcome.PathsGiven, missingPathCount)
	}
	if outcome.PathsProcessed != missingPathCount {
		t.Fatalf("PathsProcessed = %d, want %d", outcome.PathsProcessed, missingPathCount)
	}
	if outcome.PathsConverged != 0 {
		t.Fatalf("PathsConverged = %d, want 0", outcome.PathsConverged)
	}
	if got := indexOneCalls.Load(); got != 0 {
		t.Fatalf("IndexOne calls = %d, want 0 for missing watcher paths", got)
	}
	if got := reindexCalls.Load(); got != 0 {
		t.Fatalf("Reindex calls = %d, want 0 for missing watcher paths", got)
	}

	afterBytes, err := os.ReadFile(checkpointPath)
	if err != nil {
		t.Fatalf("ReadFile checkpoint after converge: %v", err)
	}
	if !bytes.Equal(afterBytes, checkpointBytes) {
		t.Fatal("checkpoint content changed after missing watcher paths")
	}
	afterInfo, err := os.Stat(checkpointPath)
	if err != nil {
		t.Fatalf("Stat checkpoint after converge: %v", err)
	}
	if !os.SameFile(checkpointInfo, afterInfo) {
		t.Fatal("missing watcher paths replaced the checkpoint atomically")
	}
}

func TestConvergePathsRateLimitsProgress(t *testing.T) {
	t.Parallel()

	const missingPathCount = 18_227
	manager, _, repoPath := newTestManager(t)
	codebase := seedConvergeCodebase(t, manager, repoPath)
	manager.semantic = &fakeSemantic{}

	relativePaths := make([]string, 0, missingPathCount)
	for i := 0; i < missingPathCount; i++ {
		relativePaths = append(relativePaths, fmt.Sprintf("missing/%05d.go", i))
	}
	updates := make([]ConvergeOutcome, 0)
	outcome, err := manager.convergePathsWithLstatAndNow(
		context.Background(),
		codebase.ID,
		relativePaths,
		func(progress ConvergeOutcome) {
			updates = append(updates, progress)
		},
		os.Lstat,
		func() time.Time { return time.Unix(1, 0) },
	)
	if err != nil {
		t.Fatalf("ConvergePaths returned error: %v", err)
	}
	if outcome.PathsProcessed != missingPathCount {
		t.Fatalf("PathsProcessed = %d, want %d", outcome.PathsProcessed, missingPathCount)
	}
	if len(updates) == 0 {
		t.Fatal("ConvergePaths did not report progress")
	}
	if len(updates) > 73 {
		t.Fatalf("progress updates = %d, want at most 73", len(updates))
	}
	processed := int32(0)
	for _, update := range updates {
		if update.PathsGiven != missingPathCount {
			t.Fatalf("PathsGiven = %d, want %d", update.PathsGiven, missingPathCount)
		}
		if update.PathsProcessed <= processed {
			t.Fatalf("progress did not increase: %d then %d", processed, update.PathsProcessed)
		}
		processed = update.PathsProcessed
	}
	if processed != missingPathCount {
		t.Fatalf("final progress = %d, want %d", processed, missingPathCount)
	}
}

func TestConvergePathsReportsHeartbeatDuringSlowPresentPath(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	codebase := seedConvergeCodebase(t, manager, repoPath)

	const pathCount = 256
	paths := make([]string, 0, pathCount)
	for index := 0; index < pathCount; index++ {
		relativePath := fmt.Sprintf("present/%03d.go", index)
		if err := os.MkdirAll(filepath.Dir(filepath.Join(repoPath, relativePath)), 0o700); err != nil {
			t.Fatalf("create source directory: %v", err)
		}
		if err := os.WriteFile(filepath.Join(repoPath, relativePath), []byte("package present\n"), 0o600); err != nil {
			t.Fatalf("write %s: %v", relativePath, err)
		}
		paths = append(paths, relativePath)
	}

	currentTime := time.Unix(1, 0)
	var reindexCalls atomic.Int32
	manager.semantic = &fakeSemantic{reindex: func(context.Context, string, []model.StoredChunk, []string) error {
		if reindexCalls.Add(1) == 1 {
			currentTime = currentTime.Add(1100 * time.Millisecond)
		}
		return nil
	}}
	updates := make([]ConvergeOutcome, 0)
	outcome, err := manager.convergePathsWithLstatAndNow(
		context.Background(),
		codebase.ID,
		paths,
		func(progress ConvergeOutcome) { updates = append(updates, progress) },
		os.Lstat,
		func() time.Time { return currentTime },
	)
	if err != nil {
		t.Fatalf("convergePathsWithLstatAndNow returned error: %v", err)
	}
	if outcome.PathsProcessed != pathCount {
		t.Fatalf("PathsProcessed = %d, want %d", outcome.PathsProcessed, pathCount)
	}
	if reindexCalls.Load() != pathCount {
		t.Fatalf("Reindex calls = %d, want %d", reindexCalls.Load(), pathCount)
	}
	if len(updates) != 2 {
		t.Fatalf("progress updates = %d, want one completed path and final progress", len(updates))
	}
	if updates[0].PathsProcessed != 1 {
		t.Fatalf("first progress = %d, want 1 after the first path finishes", updates[0].PathsProcessed)
	}
	if updates[1].PathsProcessed != pathCount {
		t.Fatalf("final progress = %d, want %d", updates[1].PathsProcessed, pathCount)
	}
}

func TestConvergePathsRetainsPathRemovedAfterClassification(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	codebase := seedConvergeCodebase(t, manager, repoPath)
	snapshot := merkle.Snapshot{
		ConfigDigest: codebase.EffectiveConfig.IgnoreDigest,
		Files:        map[string]string{"disappeared.go": "hash-disappeared"},
	}
	checkpointPath := manager.snapshotPathForCodebase(codebase)
	if err := merkle.WriteSnapshot(checkpointPath, snapshot); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	checkpointBytes, err := os.ReadFile(checkpointPath)
	if err != nil {
		t.Fatalf("ReadFile checkpoint: %v", err)
	}

	var indexOneCalls atomic.Int32
	manager.runner = fakeRunner{
		indexOne: func(context.Context, string, string, model.IndexConfig) (indexer.OneFileResult, error) {
			indexOneCalls.Add(1)
			return indexer.OneFileResult{Removed: true}, nil
		},
	}
	var reindexCalls atomic.Int32
	manager.semantic = &fakeSemantic{
		reindex: func(context.Context, string, []model.StoredChunk, []string) error {
			reindexCalls.Add(1)
			return nil
		},
	}

	lstatCalls := 0
	presentInfo := writeConvergePresentFileInfo(t, repoPath, "disappeared.go")
	outcome, err := manager.convergePathsWithLstat(context.Background(), codebase.ID, []string{"disappeared.go"}, nil, func(string) (os.FileInfo, error) {
		lstatCalls++
		if lstatCalls == 1 {
			return presentInfo, nil
		}
		return nil, os.ErrNotExist
	})
	if err != nil {
		t.Fatalf("convergePathsWithLstat: %v", err)
	}
	if outcome.PathsProcessed != 1 || outcome.PathsConverged != 0 {
		t.Fatalf("outcome = %+v, want one retained path", outcome)
	}
	if got := indexOneCalls.Load(); got != 1 {
		t.Fatalf("IndexOne calls = %d, want 1 after present classification", got)
	}
	if got := reindexCalls.Load(); got != 0 {
		t.Fatalf("Reindex calls = %d, want 0 after late disappearance", got)
	}
	afterBytes, err := os.ReadFile(checkpointPath)
	if err != nil {
		t.Fatalf("ReadFile checkpoint after converge: %v", err)
	}
	if !bytes.Equal(afterBytes, checkpointBytes) {
		t.Fatal("checkpoint changed after a late disappearance")
	}
}

func TestClassifyConvergePathsRejectsNonAbsenceError(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	codebase := seedConvergeCodebase(t, manager, repoPath)
	writeSyntheticSnapshot(t, manager, codebase, 1)
	checkpointPath := manager.snapshotPathForCodebase(codebase)
	checkpointBytes, err := os.ReadFile(checkpointPath)
	if err != nil {
		t.Fatalf("ReadFile checkpoint: %v", err)
	}

	var indexOneCalls atomic.Int32
	manager.runner = fakeRunner{
		indexOne: func(context.Context, string, string, model.IndexConfig) (indexer.OneFileResult, error) {
			indexOneCalls.Add(1)
			return indexer.OneFileResult{}, nil
		},
	}
	var reindexCalls atomic.Int32
	manager.semantic = &fakeSemantic{
		reindex: func(context.Context, string, []model.StoredChunk, []string) error {
			reindexCalls.Add(1)
			return nil
		},
	}

	_, err = manager.convergePathsWithLstat(context.Background(), codebase.ID, []string{"unreadable.go"}, nil, func(string) (os.FileInfo, error) {
		return nil, os.ErrPermission
	})
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("error = %v, want permission error", err)
	}
	if got := indexOneCalls.Load(); got != 0 {
		t.Fatalf("IndexOne calls = %d, want 0 before classification failure", got)
	}
	if got := reindexCalls.Load(); got != 0 {
		t.Fatalf("Reindex calls = %d, want 0 before classification failure", got)
	}
	afterBytes, readErr := os.ReadFile(checkpointPath)
	if readErr != nil {
		t.Fatalf("ReadFile checkpoint after classification failure: %v", readErr)
	}
	if !bytes.Equal(afterBytes, checkpointBytes) {
		t.Fatal("checkpoint changed after classification failure")
	}
}

func TestConvergePathsReportsSlowClassificationProgress(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	codebase := seedConvergeCodebase(t, manager, repoPath)
	manager.semantic = &fakeSemantic{}

	currentTime := time.Unix(1, 0)
	statCalls := 0
	updates := make([]ConvergeOutcome, 0)
	outcome, err := manager.convergePathsWithLstatAndNow(
		context.Background(),
		codebase.ID,
		[]string{"first.go", "second.go", "third.go"},
		func(progress ConvergeOutcome) {
			updates = append(updates, progress)
		},
		func(string) (os.FileInfo, error) {
			statCalls++
			if statCalls <= 2 {
				currentTime = currentTime.Add(convergeProgressTimeInterval)
			}
			return nil, os.ErrNotExist
		},
		func() time.Time {
			return currentTime
		},
	)
	if err != nil {
		t.Fatalf("convergePathsWithLstatAndNow returned error: %v", err)
	}
	if outcome.PathsProcessed != 3 {
		t.Fatalf("PathsProcessed = %d, want 3", outcome.PathsProcessed)
	}
	if len(updates) != 3 {
		t.Fatalf("progress updates = %d, want 3", len(updates))
	}
	if updates[0].PathsProcessed != 0 {
		t.Fatalf("slow classification progress = %d, want 0 before a path finishes", updates[0].PathsProcessed)
	}
	if updates[1].PathsProcessed != 0 {
		t.Fatalf("classification heartbeat progress = %d, want 0 before a path finishes", updates[1].PathsProcessed)
	}
	if updates[2].PathsProcessed != 3 {
		t.Fatalf("final classification progress = %d, want 3", updates[2].PathsProcessed)
	}
}

func TestConvergePathsReportsFinalProgressAfterClassificationError(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	codebase := seedConvergeCodebase(t, manager, repoPath)
	writeSyntheticSnapshot(t, manager, codebase, 1)
	checkpointPath := manager.snapshotPathForCodebase(codebase)
	checkpointBytes, err := os.ReadFile(checkpointPath)
	if err != nil {
		t.Fatalf("ReadFile checkpoint: %v", err)
	}
	checkpointInfo, err := os.Stat(checkpointPath)
	if err != nil {
		t.Fatalf("Stat checkpoint: %v", err)
	}

	var reindexCalls atomic.Int32
	manager.semantic = &fakeSemantic{
		reindex: func(context.Context, string, []model.StoredChunk, []string) error {
			reindexCalls.Add(1)
			return nil
		},
	}
	updates := make([]ConvergeOutcome, 0)
	statCalls := 0
	outcome, err := manager.convergePathsWithLstat(context.Background(), codebase.ID, []string{"first.go", "second.go", "denied.go"}, func(progress ConvergeOutcome) {
		updates = append(updates, progress)
	}, func(string) (os.FileInfo, error) {
		statCalls++
		if statCalls == 3 {
			return nil, os.ErrPermission
		}
		return nil, os.ErrNotExist
	})
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("error = %v, want permission error", err)
	}
	if outcome.PathsProcessed != 0 {
		t.Fatalf("PathsProcessed = %d, want 0 before convergence begins", outcome.PathsProcessed)
	}
	if len(updates) != 1 || updates[0].PathsProcessed != 0 {
		t.Fatalf("final progress = %+v, want one update for no handled paths", updates)
	}
	if got := reindexCalls.Load(); got != 0 {
		t.Fatalf("Reindex calls = %d, want 0 after classification failure", got)
	}
	afterBytes, readErr := os.ReadFile(checkpointPath)
	if readErr != nil {
		t.Fatalf("ReadFile checkpoint after classification failure: %v", readErr)
	}
	if !bytes.Equal(afterBytes, checkpointBytes) {
		t.Fatal("checkpoint changed after classification failure")
	}
	afterInfo, statErr := os.Stat(checkpointPath)
	if statErr != nil {
		t.Fatalf("Stat checkpoint after classification failure: %v", statErr)
	}
	if !os.SameFile(checkpointInfo, afterInfo) {
		t.Fatal("classification failure replaced the checkpoint atomically")
	}
}

func TestConvergePathsReportsFinalProgressAfterClassificationCancellation(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	codebase := seedConvergeCodebase(t, manager, repoPath)
	writeSyntheticSnapshot(t, manager, codebase, 1)
	checkpointPath := manager.snapshotPathForCodebase(codebase)
	checkpointBytes, err := os.ReadFile(checkpointPath)
	if err != nil {
		t.Fatalf("ReadFile checkpoint: %v", err)
	}
	checkpointInfo, err := os.Stat(checkpointPath)
	if err != nil {
		t.Fatalf("Stat checkpoint: %v", err)
	}

	var reindexCalls atomic.Int32
	manager.semantic = &fakeSemantic{
		reindex: func(context.Context, string, []model.StoredChunk, []string) error {
			reindexCalls.Add(1)
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates := make([]ConvergeOutcome, 0)
	statCalls := 0
	outcome, err := manager.convergePathsWithLstat(ctx, codebase.ID, []string{"first.go", "second.go", "third.go"}, func(progress ConvergeOutcome) {
		updates = append(updates, progress)
	}, func(string) (os.FileInfo, error) {
		statCalls++
		if statCalls == 2 {
			cancel()
		}
		return nil, os.ErrNotExist
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context cancellation", err)
	}
	if outcome.PathsProcessed != 0 {
		t.Fatalf("PathsProcessed = %d, want 0 before convergence begins", outcome.PathsProcessed)
	}
	if len(updates) != 1 || updates[0].PathsProcessed != 0 {
		t.Fatalf("final progress = %+v, want one update for no handled paths", updates)
	}
	if got := reindexCalls.Load(); got != 0 {
		t.Fatalf("Reindex calls = %d, want 0 after classification cancellation", got)
	}
	afterBytes, readErr := os.ReadFile(checkpointPath)
	if readErr != nil {
		t.Fatalf("ReadFile checkpoint after classification cancellation: %v", readErr)
	}
	if !bytes.Equal(afterBytes, checkpointBytes) {
		t.Fatal("checkpoint changed after classification cancellation")
	}
	afterInfo, statErr := os.Stat(checkpointPath)
	if statErr != nil {
		t.Fatalf("Stat checkpoint after classification cancellation: %v", statErr)
	}
	if !os.SameFile(checkpointInfo, afterInfo) {
		t.Fatal("classification cancellation replaced the checkpoint atomically")
	}
}

func TestConvergePathsStopsAfterHandledPathCancellation(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	codebase := seedConvergeCodebase(t, manager, repoPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var reindexCalls atomic.Int32
	manager.semantic = &fakeSemantic{
		reindex: func(context.Context, string, []model.StoredChunk, []string) error {
			if reindexCalls.Add(1) == 1 {
				cancel()
			}
			return nil
		},
	}
	for _, name := range []string{"first.go", "second.go"} {
		if err := os.WriteFile(filepath.Join(repoPath, name), []byte("package cancel\n"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	outcome, err := manager.convergePathsWithLstat(
		ctx,
		codebase.ID,
		[]string{"first.go", "second.go"},
		nil,
		os.Lstat,
	)
	if err != nil {
		t.Fatalf("converge after handled cancellation returned error: %v", err)
	}
	if outcome.PathsProcessed != 1 {
		t.Fatalf("PathsProcessed = %d, want one handled path", outcome.PathsProcessed)
	}
	if reindexCalls.Load() != 1 {
		t.Fatalf("Reindex calls = %d, want one handled path", reindexCalls.Load())
	}
}

func TestConvergePathsPropagatesPostclassificationLstatError(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	codebase := seedConvergeCodebase(t, manager, repoPath)
	writeSyntheticSnapshot(t, manager, codebase, 1)
	checkpointPath := manager.snapshotPathForCodebase(codebase)
	checkpointBytes, err := os.ReadFile(checkpointPath)
	if err != nil {
		t.Fatalf("ReadFile checkpoint: %v", err)
	}

	var indexOneCalls atomic.Int32
	manager.runner = fakeRunner{
		indexOne: func(context.Context, string, string, model.IndexConfig) (indexer.OneFileResult, error) {
			indexOneCalls.Add(1)
			return indexer.OneFileResult{}, nil
		},
	}
	var reindexCalls atomic.Int32
	manager.semantic = &fakeSemantic{
		reindex: func(context.Context, string, []model.StoredChunk, []string) error {
			reindexCalls.Add(1)
			return nil
		},
	}

	lstatCalls := 0
	presentInfo := writeConvergePresentFileInfo(t, repoPath, "f000.go")
	_, err = manager.convergePathsWithLstat(context.Background(), codebase.ID, []string{"f000.go"}, nil, func(string) (os.FileInfo, error) {
		lstatCalls++
		if lstatCalls == 1 {
			return presentInfo, nil
		}
		return nil, os.ErrPermission
	})
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("error = %v, want permission error", err)
	}
	if lstatCalls != 2 {
		t.Fatalf("Lstat calls = %d, want 2 for post-classification failure", lstatCalls)
	}
	if got := indexOneCalls.Load(); got != 0 {
		t.Fatalf("IndexOne calls = %d, want 0 after post-classification stat failure", got)
	}
	if got := reindexCalls.Load(); got != 0 {
		t.Fatalf("Reindex calls = %d, want 0 after post-classification stat failure", got)
	}
	afterBytes, readErr := os.ReadFile(checkpointPath)
	if readErr != nil {
		t.Fatalf("ReadFile checkpoint after post-classification failure: %v", readErr)
	}
	if !bytes.Equal(afterBytes, checkpointBytes) {
		t.Fatal("checkpoint changed after post-classification failure")
	}
}

func TestConvergePathsPersistsSuccessfulPathBeforeLaterError(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	codebase := seedConvergeCodebase(t, manager, repoPath)
	writeConvergePresentFileInfo(t, repoPath, "first.go")
	writeConvergePresentFileInfo(t, repoPath, "second.go")

	manager.runner = fakeRunner{
		indexOne: func(_ context.Context, _ string, relativePath string, _ model.IndexConfig) (indexer.OneFileResult, error) {
			if relativePath == "second.go" {
				return indexer.OneFileResult{}, os.ErrPermission
			}
			return indexer.OneFileResult{
				Chunks: []model.StoredChunk{{
					Content:      "package first\n",
					RelativePath: relativePath,
					StartLine:    1,
					EndLine:      1,
				}},
				FileHash: "hash-first",
			}, nil
		},
	}
	var reindexCalls atomic.Int32
	manager.semantic = &fakeSemantic{
		reindex: func(context.Context, string, []model.StoredChunk, []string) error {
			reindexCalls.Add(1)
			return nil
		},
	}

	outcome, err := manager.ConvergePaths(
		context.Background(),
		codebase.ID,
		[]string{"first.go", "second.go"},
		nil,
	)
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("error = %v, want permission error", err)
	}
	if outcome.PathsConverged != 1 {
		t.Fatalf("PathsConverged = %d, want 1", outcome.PathsConverged)
	}
	if got := reindexCalls.Load(); got != 1 {
		t.Fatalf("Reindex calls = %d, want 1", got)
	}

	checkpoint, readErr := merkle.ReadSnapshot(manager.snapshotPathForCodebase(codebase))
	if readErr != nil {
		t.Fatalf("ReadSnapshot after later error: %v", readErr)
	}
	if checkpoint.Files["first.go"] != "hash-first" {
		t.Fatalf("first.go hash = %q, want %q", checkpoint.Files["first.go"], "hash-first")
	}
	if checkpoint.HasFile("second.go") {
		t.Fatal("checkpoint recorded second.go after its error")
	}
}

func TestConvergePathsPropagatesIndexOneError(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	codebase := seedConvergeCodebase(t, manager, repoPath)
	manager.semantic = &fakeSemantic{
		reindex: func(context.Context, string, []model.StoredChunk, []string) error {
			return nil
		},
	}
	manager.runner = fakeRunner{
		indexOne: func(context.Context, string, string, model.IndexConfig) (indexer.OneFileResult, error) {
			return indexer.OneFileResult{}, os.ErrPermission
		},
	}

	lstatCalls := 0
	presentInfo := writeConvergePresentFileInfo(t, repoPath, "unreadable.go")
	_, err := manager.convergePathsWithLstat(context.Background(), codebase.ID, []string{"unreadable.go"}, nil, func(string) (os.FileInfo, error) {
		lstatCalls++
		if lstatCalls == 1 {
			return presentInfo, nil
		}
		return nil, os.ErrNotExist
	})
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("error = %v, want permission error", err)
	}
	if lstatCalls != 2 {
		t.Fatalf("Lstat calls = %d, want 2 for post-classification index failure", lstatCalls)
	}
}

func TestClassifyConvergePathsStopsOnCancellation(t *testing.T) {
	t.Parallel()

	const pathCount = 18_227
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	paths := make([]string, pathCount)
	for i := range paths {
		paths[i] = fmt.Sprintf("missing/%05d.go", i)
	}
	statCalls := 0
	_, err := classifyConvergePathsWithProgress(ctx, t.TempDir(), paths, func(string) (os.FileInfo, error) {
		statCalls++
		if statCalls == 100 {
			cancel()
		}
		return nil, os.ErrNotExist
	}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context cancellation", err)
	}
	if statCalls >= pathCount {
		t.Fatalf("Lstat calls = %d, want cancellation before %d paths", statCalls, pathCount)
	}
}

// TestConvergePathsStopsBetweenPathsOnCancel proves a cancelled converge stops
// rather than finishing its list. The paths it did not reach keep their previous
// index entries, which the periodic sync repairs on its next pass.
func TestConvergePathsStopsBetweenPathsOnCancel(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	codebase := seedConvergeCodebase(t, manager, repoPath)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel as soon as the first path reaches the store, so the second path is
	// never read.
	manager.semantic = &fakeSemantic{
		reindex: func(context.Context, string, []model.StoredChunk, []string) error {
			cancel()
			return nil
		},
	}

	names := []string{"first.go", "second.go"}
	for _, name := range names {
		body := "package main\n\nfunc " + name[:len(name)-3] + "() {}\n"
		if err := os.WriteFile(filepath.Join(repoPath, name), []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	outcome, err := manager.ConvergePaths(ctx, codebase.ID, names, nil)
	if err != nil {
		t.Fatalf("ConvergePaths returned error: %v", err)
	}
	if outcome.PathsGiven != 2 {
		t.Fatalf("PathsGiven = %d, want 2", outcome.PathsGiven)
	}
	if outcome.PathsConverged >= 2 {
		t.Fatalf("PathsConverged = %d, want fewer than 2; the cancel did not stop the loop", outcome.PathsConverged)
	}
}

func TestConvergePathsStopsBetweenPathsOnCancelWithoutProgress(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	codebase := seedConvergeCodebase(t, manager, repoPath)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var reindexCalls atomic.Int32
	manager.semantic = &fakeSemantic{
		reindex: func(context.Context, string, []model.StoredChunk, []string) error {
			reindexCalls.Add(1)
			cancel()
			return nil
		},
	}
	for _, name := range []string{"first.go", "second.go"} {
		if err := os.WriteFile(filepath.Join(repoPath, name), []byte("package cancel\n"), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	_, err := manager.ConvergePaths(ctx, codebase.ID, []string{"first.go", "second.go"}, nil)
	if err != nil {
		t.Fatalf("ConvergePaths returned error: %v", err)
	}
	if reindexCalls.Load() != 1 {
		t.Fatalf("Reindex calls = %d, want 1 after cancellation", reindexCalls.Load())
	}
}

func TestConvergeViaWatcherRegistersRunningJob(t *testing.T) {
	t.Parallel()

	manager, cfg, repoPath := newTestManager(t)
	codebase := seedConvergeCodebase(t, manager, repoPath)
	entered := make(chan struct{})
	release := make(chan struct{})
	manager.semantic = &fakeSemantic{
		reindex: func(context.Context, string, []model.StoredChunk, []string) error {
			close(entered)
			<-release
			return nil
		},
	}

	if err := os.WriteFile(filepath.Join(repoPath, "watched.go"), []byte("package watched\n"), 0o644); err != nil {
		t.Fatalf("write watched.go: %v", err)
	}

	syncer := NewBackgroundSync(cfg, manager)
	manager.SetWatcherActivityReporter(syncer)
	done := make(chan struct{})
	go func() {
		defer close(done)
		syncer.convergeViaWatcher(context.Background(), codebase.ID, []string{"watched.go"})
	}()
	t.Cleanup(func() {
		closeOnce(release)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("watcher converge did not stop during cleanup")
		}
	})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher converge did not start")
	}

	jobs := manager.ListJobs(codebase.ID)
	if len(jobs) != 1 {
		t.Fatalf("ListJobs returned %d jobs, want 1 running converge", len(jobs))
	}
	job := jobs[0]
	if job.Operation != "converge" {
		t.Fatalf("Operation = %q, want %q", job.Operation, "converge")
	}
	if job.State != model.JobStateRunning {
		t.Fatalf("State = %q, want %q", job.State, model.JobStateRunning)
	}
	if job.Progress.FilesTotal != 1 {
		t.Fatalf("FilesTotal = %d, want 1", job.Progress.FilesTotal)
	}
	if job.Progress.Unit != "path" {
		t.Fatalf("Unit = %q, want %q", job.Progress.Unit, "path")
	}
	if job.Client != (model.ClientInfo{Name: "daemon-watcher", PID: 0}) {
		t.Fatalf("Client = %+v, want daemon-watcher with PID 0", job.Client)
	}
	resolved, found := manager.GetJob(job.ID)
	if !found {
		t.Fatalf("GetJob(%q) did not resolve the running converge", job.ID)
	}
	if resolved.ID != job.ID {
		t.Fatalf("GetJob returned ID %q, want %q", resolved.ID, job.ID)
	}

	snapshot := manager.StatusSnapshot()
	if units := len(snapshot.ActiveJobs) + len(snapshot.Watcher); units != 1 {
		t.Fatalf(
			"StatusSnapshot reported %d units for one running converge: jobs=%d watcher=%d",
			units,
			len(snapshot.ActiveJobs),
			len(snapshot.Watcher),
		)
	}
}

func TestCancelJobStopsWatcherConverge(t *testing.T) {
	t.Parallel()

	manager, cfg, repoPath := newTestManager(t)
	codebase := seedConvergeCodebase(t, manager, repoPath)
	entered := make(chan struct{})
	stopped := make(chan struct{})
	manager.semantic = &fakeSemantic{
		reindex: func(ctx context.Context, _ string, _ []model.StoredChunk, _ []string) error {
			close(entered)
			<-ctx.Done()
			close(stopped)
			return nil
		},
	}

	if err := os.WriteFile(filepath.Join(repoPath, "cancel.go"), []byte("package cancel\n"), 0o644); err != nil {
		t.Fatalf("write cancel.go: %v", err)
	}

	parentCtx, stopParent := context.WithCancel(context.Background())
	syncer := NewBackgroundSync(cfg, manager)
	done := make(chan struct{})
	go func() {
		defer close(done)
		syncer.convergeViaWatcher(parentCtx, codebase.ID, []string{"cancel.go"})
	}()
	t.Cleanup(func() {
		stopParent()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("watcher converge did not stop during cleanup")
		}
	})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher converge did not start")
	}

	jobs := manager.ListJobs(codebase.ID)
	if len(jobs) != 1 {
		t.Fatalf("ListJobs returned %d jobs, want 1 cancellable converge", len(jobs))
	}
	if _, err := manager.CancelJob(context.Background(), jobs[0].ID); err != nil {
		t.Fatalf("CancelJob returned error: %v", err)
	}

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("CancelJob did not stop the running converge")
	}
	<-done

	cancelled, found := manager.GetJob(jobs[0].ID)
	if !found {
		t.Fatalf("GetJob(%q) did not resolve the cancelled converge", jobs[0].ID)
	}
	if cancelled.State != model.JobStateCancelled {
		t.Fatalf("State = %q, want %q", cancelled.State, model.JobStateCancelled)
	}
}

func TestWatcherCancellationReportsHandledPathPrefix(t *testing.T) {
	t.Parallel()

	manager, cfg, repoPath := newTestManager(t)
	codebase := seedConvergeCodebase(t, manager, repoPath)
	entered := make(chan struct{})
	stopped := make(chan struct{})
	manager.semantic = &fakeSemantic{
		reindex: func(ctx context.Context, _ string, _ []model.StoredChunk, _ []string) error {
			close(entered)
			<-ctx.Done()
			close(stopped)
			return nil
		},
	}
	for _, name := range []string{"first.go", "second.go"} {
		if err := os.WriteFile(filepath.Join(repoPath, name), []byte("package cancel\n"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	parentCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	syncer := NewBackgroundSync(cfg, manager)
	done := make(chan struct{})
	go func() {
		defer close(done)
		syncer.convergeViaWatcher(parentCtx, codebase.ID, []string{"first.go", "second.go"})
	}()
	<-entered
	cancel()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher converge did not observe cancellation")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher converge did not finish after cancellation")
	}

	jobs := manager.ListJobs(codebase.ID)
	if len(jobs) != 1 {
		t.Fatalf("ListJobs returned %d jobs, want 1 cancelled converge", len(jobs))
	}
	job := jobs[0]
	if job.State != model.JobStateCancelled {
		t.Fatalf("State = %q, want %q", job.State, model.JobStateCancelled)
	}
	if job.Progress.FilesProcessed != 1 {
		t.Fatalf("FilesProcessed = %d, want 1 handled path", job.Progress.FilesProcessed)
	}
}

func TestCompletedWatcherConvergeKeepsDegradedDependency(t *testing.T) {
	t.Parallel()

	manager, cfg, repoPath := newTestManager(t)
	codebase := seedConvergeCodebase(t, manager, repoPath)
	codebase.LiveFileTotal = 91
	codebase.LiveChunkTotal = 92
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.mu.Unlock()
	manager.semantic = &fakeSemantic{
		reindex: func(context.Context, string, []model.StoredChunk, []string) error {
			return nil
		},
	}
	manager.mu.Lock()
	manager.noteDependencyFailureLocked(adapterr.NewEmbedderUnreachable(nil))
	manager.mu.Unlock()

	if err := os.WriteFile(filepath.Join(repoPath, "complete.go"), []byte("package complete\n"), 0o644); err != nil {
		t.Fatalf("write complete.go: %v", err)
	}

	syncer := NewBackgroundSync(cfg, manager)
	syncer.convergeViaWatcher(context.Background(), codebase.ID, []string{"complete.go"})

	jobs := manager.ListJobs(codebase.ID)
	if len(jobs) != 1 {
		t.Fatalf("ListJobs returned %d jobs, want 1 completed converge", len(jobs))
	}
	job := jobs[0]
	if job.State != model.JobStateCompleted {
		t.Fatalf("State = %q, want %q", job.State, model.JobStateCompleted)
	}
	if job.Progress.FilesEmbedded != 0 {
		t.Fatalf("FilesEmbedded = %d, want 0", job.Progress.FilesEmbedded)
	}
	if !manager.DependencyHealth().Degraded() {
		t.Fatal("completed converge cleared the degraded dependency banner")
	}
	manager.mu.Lock()
	updatedCodebase := manager.codebases[codebase.ID]
	manager.mu.Unlock()
	if updatedCodebase.LiveFileTotal != codebase.LiveFileTotal || updatedCodebase.LiveChunkTotal != codebase.LiveChunkTotal {
		t.Fatalf("live totals = %d files, %d chunks; want %d files, %d chunks", updatedCodebase.LiveFileTotal, updatedCodebase.LiveChunkTotal, codebase.LiveFileTotal, codebase.LiveChunkTotal)
	}
}

func TestConvergeViaWatcherHeartbeatsSlowPresentPathWithoutDuplicateProgressJournal(t *testing.T) {
	manager, cfg, repoPath := newTestManager(t)
	codebase := seedConvergeCodebase(t, manager, repoPath)

	const pathCount = 256
	paths := make([]string, 0, pathCount)
	for index := 0; index < pathCount; index++ {
		relativePath := fmt.Sprintf("present/%03d.go", index)
		absolutePath := filepath.Join(repoPath, relativePath)
		if err := os.MkdirAll(filepath.Dir(absolutePath), 0o700); err != nil {
			t.Fatalf("create source directory: %v", err)
		}
		if err := os.WriteFile(absolutePath, []byte("package present\n"), 0o600); err != nil {
			t.Fatalf("write %s: %v", relativePath, err)
		}
		paths = append(paths, relativePath)
	}
	manager.runner = fakeRunner{
		indexOne: func(_ context.Context, _ string, relativePath string, _ model.IndexConfig) (indexer.OneFileResult, error) {
			return indexer.OneFileResult{
				Chunks: []model.StoredChunk{{
					Content:      "package present\n",
					RelativePath: relativePath,
					StartLine:    1,
					EndLine:      1,
				}},
				FileHash: "hash-" + relativePath,
			}, nil
		},
	}

	var reindexCalls atomic.Int32
	var progressUpdatedAt time.Time
	var progressHeartbeatAt time.Time
	manager.semantic = &fakeSemantic{
		reindex: func(context.Context, string, []model.StoredChunk, []string) error {
			call := reindexCalls.Add(1)
			jobs := manager.ListJobs(codebase.ID)
			if len(jobs) != 1 {
				t.Fatalf("ListJobs returned %d jobs during converge, want 1", len(jobs))
			}
			job := jobs[0]
			switch call {
			case 1:
				if job.Progress.FilesProcessed != 0 {
					t.Fatalf("progress before the first path finishes = %d, want 0", job.Progress.FilesProcessed)
				}
				progressUpdatedAt = job.UpdatedAt
				progressHeartbeatAt = job.Progress.HeartbeatAt
				time.Sleep(1100 * time.Millisecond)
			case 2:
				if job.Progress.FilesProcessed != 1 {
					t.Fatalf("progress after the first path finishes = %d, want 1", job.Progress.FilesProcessed)
				}
				if !job.UpdatedAt.After(progressUpdatedAt) {
					t.Fatalf("UpdatedAt = %s, want after %s", job.UpdatedAt, progressUpdatedAt)
				}
				if !job.Progress.HeartbeatAt.After(progressHeartbeatAt) {
					t.Fatalf("HeartbeatAt = %s, want after %s", job.Progress.HeartbeatAt, progressHeartbeatAt)
				}
			}
			return nil
		},
	}

	syncer := NewBackgroundSync(cfg, manager)
	syncer.convergeViaWatcher(context.Background(), codebase.ID, paths)
	if got := reindexCalls.Load(); got != pathCount {
		t.Fatalf("Reindex calls = %d, want %d", got, pathCount)
	}
	jobs := manager.ListJobs(codebase.ID)
	if len(jobs) != 1 || jobs[0].State != model.JobStateCompleted {
		t.Fatalf("completed watcher jobs = %+v, want one completed job", jobs)
	}

	manager.closeJobJournal()
	journal, err := os.ReadFile(cfg.JobsPath)
	if err != nil {
		t.Fatalf("ReadFile jobs journal: %v", err)
	}
	if got := bytes.Count(journal, []byte(`"event":"job_progress"`)); got != 1 {
		t.Fatalf("job_progress events = %d, want 1 despite the heartbeat", got)
	}
}

func TestConvergeViaWatcherCompletesRetainedMissingProgress(t *testing.T) {
	t.Parallel()

	const missingPathCount = 18_227
	manager, cfg, repoPath := newTestManager(t)
	codebase := seedConvergeCodebase(t, manager, repoPath)
	codebase.LiveFileTotal = 91
	codebase.LiveChunkTotal = 92
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.mu.Unlock()
	writeSyntheticSnapshot(t, manager, codebase, 1)
	checkpointPath := manager.snapshotPathForCodebase(codebase)
	checkpointBytes, err := os.ReadFile(checkpointPath)
	if err != nil {
		t.Fatalf("ReadFile checkpoint: %v", err)
	}
	checkpointInfo, err := os.Stat(checkpointPath)
	if err != nil {
		t.Fatalf("Stat checkpoint: %v", err)
	}

	var indexOneCalls atomic.Int32
	manager.runner = fakeRunner{
		indexOne: func(context.Context, string, string, model.IndexConfig) (indexer.OneFileResult, error) {
			indexOneCalls.Add(1)
			return indexer.OneFileResult{}, nil
		},
	}
	var reindexCalls atomic.Int32
	manager.semantic = &fakeSemantic{
		reindex: func(context.Context, string, []model.StoredChunk, []string) error {
			reindexCalls.Add(1)
			return nil
		},
	}

	relativePaths := make([]string, 0, missingPathCount)
	for i := 0; i < missingPathCount; i++ {
		relativePaths = append(relativePaths, fmt.Sprintf("missing/%05d.go", i))
	}
	syncer := NewBackgroundSync(cfg, manager)
	syncer.convergeViaWatcher(context.Background(), codebase.ID, relativePaths)
	manager.closeJobJournal()
	journal, err := os.ReadFile(cfg.JobsPath)
	if err != nil {
		t.Fatalf("ReadFile jobs journal: %v", err)
	}
	if !bytes.Contains(journal, []byte(`"event":"job_progress"`)) {
		t.Fatal("watcher converge did not journal detached progress")
	}

	jobs := manager.ListJobs(codebase.ID)
	if len(jobs) != 1 {
		t.Fatalf("ListJobs returned %d jobs, want 1 completed converge", len(jobs))
	}
	job := jobs[0]
	if job.State != model.JobStateCompleted {
		t.Fatalf("State = %q, want %q", job.State, model.JobStateCompleted)
	}
	if job.Progress.Unit != "path" {
		t.Fatalf("Unit = %q, want %q", job.Progress.Unit, "path")
	}
	if job.Progress.FilesTotal != missingPathCount || job.Progress.FilesProcessed != missingPathCount {
		t.Fatalf("progress = %d of %d, want %d of %d", job.Progress.FilesProcessed, job.Progress.FilesTotal, missingPathCount, missingPathCount)
	}
	if job.Progress.FilesEmbedded != 0 {
		t.Fatalf("FilesEmbedded = %d, want 0", job.Progress.FilesEmbedded)
	}
	if !job.Progress.HeartbeatAt.After(job.StartedAt) {
		t.Fatalf("HeartbeatAt = %s, want after StartedAt %s", job.Progress.HeartbeatAt, job.StartedAt)
	}
	if got := indexOneCalls.Load(); got != 0 {
		t.Fatalf("IndexOne calls = %d, want 0 for missing watcher paths", got)
	}
	if got := reindexCalls.Load(); got != 0 {
		t.Fatalf("Reindex calls = %d, want 0 for missing watcher paths", got)
	}
	manager.mu.Lock()
	updatedCodebase := manager.codebases[codebase.ID]
	manager.mu.Unlock()
	if updatedCodebase.LiveFileTotal != codebase.LiveFileTotal || updatedCodebase.LiveChunkTotal != codebase.LiveChunkTotal {
		t.Fatalf("live totals = %d files, %d chunks; want %d files, %d chunks", updatedCodebase.LiveFileTotal, updatedCodebase.LiveChunkTotal, codebase.LiveFileTotal, codebase.LiveChunkTotal)
	}
	afterBytes, err := os.ReadFile(checkpointPath)
	if err != nil {
		t.Fatalf("ReadFile checkpoint after converge: %v", err)
	}
	if !bytes.Equal(afterBytes, checkpointBytes) {
		t.Fatal("checkpoint content changed after missing watcher paths")
	}
	afterInfo, err := os.Stat(checkpointPath)
	if err != nil {
		t.Fatalf("Stat checkpoint after converge: %v", err)
	}
	if !os.SameFile(checkpointInfo, afterInfo) {
		t.Fatal("missing watcher paths replaced the checkpoint atomically")
	}
}

func TestConvergeViaWatcherTerminalStatePreservesNewerFirstBuild(t *testing.T) {
	testCases := []struct {
		name         string
		cancel       bool
		failSnapshot bool
		wantState    model.JobState
	}{
		{name: "completed", wantState: model.JobStateCompleted},
		{name: "failed", failSnapshot: true, wantState: model.JobStateFailed},
		{name: "cancelled", cancel: true, wantState: model.JobStateCancelled},
		{name: "cancelled after snapshot error", cancel: true, failSnapshot: true, wantState: model.JobStateCancelled},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			manager, cfg, repoPath := newTestManager(t)
			codebase := seedConvergeCodebase(t, manager, repoPath)
			if testCase.failSnapshot {
				codebase.MerkleSnapshotPath = t.TempDir()
				manager.mu.Lock()
				manager.codebases[codebase.ID] = codebase
				manager.mu.Unlock()
			}
			if err := os.WriteFile(filepath.Join(repoPath, "raced.go"), []byte("package raced\n"), 0o644); err != nil {
				t.Fatalf("write raced.go: %v", err)
			}
			entered := make(chan struct{})
			release := make(chan struct{})
			manager.semantic = &fakeSemantic{reindex: func(context.Context, string, []model.StoredChunk, []string) error {
				close(entered)
				<-release
				return nil
			}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			syncer := NewBackgroundSync(cfg, manager)
			done := make(chan struct{})
			go func() {
				defer close(done)
				syncer.convergeViaWatcher(ctx, codebase.ID, []string{"raced.go"})
			}()
			<-entered

			firstJob := newQueuedJob(codebase.ID, repoPath, repoPath, testClientInfo(), string(jobOperationIndex), false, defaultIndexConfig(), emptyAdmissionBudget, codebase.UpdatedAt)
			current := codebase
			current.Status = model.CodebaseStatusPending
			current.ActiveJobID = firstJob.ID
			manager.mu.Lock()
			manager.codebases[current.ID] = current
			manager.jobs[firstJob.ID] = firstJob
			if err := manager.saveLocked(); err != nil {
				manager.mu.Unlock()
				t.Fatalf("saveLocked returned error: %v", err)
			}
			manager.mu.Unlock()
			if testCase.cancel {
				cancel()
			}
			close(release)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("watcher converge did not finish")
			}

			jobs := manager.ListJobs(codebase.ID)
			var converge model.Job
			for _, job := range jobs {
				if job.Operation == "converge" {
					converge = job
				}
			}
			manager.mu.Lock()
			gotCodebase := manager.codebases[current.ID]
			manager.config.RegistryPath = t.TempDir()
			manager.mu.Unlock()
			if converge.State != testCase.wantState {
				t.Fatalf("converge state = %q, want %q", converge.State, testCase.wantState)
			}
			if gotCodebase.Status != current.Status || gotCodebase.ActiveJobID != current.ActiveJobID || gotCodebase.LastSuccessfulRun != current.LastSuccessfulRun || !gotCodebase.UpdatedAt.Equal(current.UpdatedAt) {
				t.Fatalf("codebase after watcher converge = %+v, want unchanged %+v", gotCodebase, current)
			}
			backendCalled := false
			manager.runner = fakeRunner{index: func(context.Context, string, model.IndexConfig, func(indexer.Progress)) (indexer.Result, error) {
				backendCalled = true
				return indexer.Result{}, nil
			}}
			manager.runJob(context.Background(), firstJob.ID)
			if backendCalled {
				t.Fatal("first build reached backend after registry persistence failed")
			}
		})
	}
}
