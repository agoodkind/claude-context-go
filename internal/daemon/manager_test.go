package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "goodkind.io/claude-context-go/gen/go/claudecontext/v1"
	"goodkind.io/claude-context-go/internal/config"
	"goodkind.io/claude-context-go/internal/indexer"
	"goodkind.io/claude-context-go/internal/model"
	"goodkind.io/claude-context-go/internal/store"
)

type fakeRunner struct {
	index      func(context.Context, string, model.IndexConfig, func(indexer.Progress)) (indexer.Result, error)
	indexFiles func(context.Context, string, []string, model.IndexConfig, func(indexer.Progress)) (indexer.Result, error)
}

func (runner fakeRunner) Index(ctx context.Context, root string, indexConfig model.IndexConfig, progress func(indexer.Progress)) (indexer.Result, error) {
	return runner.index(ctx, root, indexConfig, progress)
}

func (runner fakeRunner) IndexFiles(ctx context.Context, root string, relativePaths []string, indexConfig model.IndexConfig, progress func(indexer.Progress)) (indexer.Result, error) {
	if runner.indexFiles != nil {
		return runner.indexFiles(ctx, root, relativePaths, indexConfig, progress)
	}
	return runner.index(ctx, root, indexConfig, progress)
}

func TestGetIndexNotTrackedReturnsFriendlyStatus(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	server := NewGRPCServer(manager, nil)

	response, err := server.GetIndex(context.Background(), &pb.GetIndexRequest{Path: repoPath})
	if err != nil {
		t.Fatalf("GetIndex returned error: %v", err)
	}
	if response.GetTracked() {
		t.Fatal("GetIndex unexpectedly reported a tracked codebase")
	}
	if !strings.Contains(response.GetDisplayText(), "is not indexed") {
		t.Fatalf("GetIndex returned unexpected text: %q", response.GetDisplayText())
	}
}

func TestStartIndexStoresFullyQualifiedEmbeddingConfig(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	manager.runner = fakeRunner{
		index: func(ctx context.Context, root string, indexConfig model.IndexConfig, progress func(indexer.Progress)) (indexer.Result, error) {
			return indexer.Result{IndexedFiles: 1, TotalChunks: 1}, nil
		},
	}

	_, codebase, _, err := manager.StartIndex(
		context.Background(),
		repoPath,
		testClientInfo(),
		defaultIndexConfig(),
		false,
	)
	if err != nil {
		t.Fatalf("StartIndex returned error: %v", err)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	if codebase.EffectiveConfig.EmbeddingProvider != "OpenAI" {
		t.Fatalf("EmbeddingProvider=%q", codebase.EffectiveConfig.EmbeddingProvider)
	}
	if codebase.EffectiveConfig.EmbeddingModel != "nvidia/NV-EmbedCode-7b-v1" {
		t.Fatalf("EmbeddingModel=%q", codebase.EffectiveConfig.EmbeddingModel)
	}
	if codebase.EffectiveConfig.VectorBackend != "milvus" {
		t.Fatalf("VectorBackend=%q", codebase.EffectiveConfig.VectorBackend)
	}
	if !codebase.EffectiveConfig.Hybrid {
		t.Fatal("Hybrid was false")
	}
}

func TestClearIndexRemovesRegistryAndChunkCache(t *testing.T) {
	t.Parallel()

	manager, cfg, repoPath := newTestManager(t)
	manager.runner = fakeRunner{
		index: func(ctx context.Context, root string, indexConfig model.IndexConfig, progress func(indexer.Progress)) (indexer.Result, error) {
			return indexer.Result{
				IndexedFiles: 1,
				TotalChunks:  1,
				Chunks: []model.StoredChunk{{
					Content:       "func SearchableThing() {}\n",
					RelativePath:  "main.go",
					StartLine:     1,
					EndLine:       1,
					Language:      "go",
					FileExtension: ".go",
				}},
			}, nil
		},
	}

	_, codebase, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false)
	if err != nil {
		t.Fatalf("StartIndex returned error: %v", err)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	chunkPath := filepath.Join(cfg.ChunksDir, codebase.ID+".json")
	if _, err := os.Stat(chunkPath); err != nil {
		t.Fatalf("chunk file missing before clear: %v", err)
	}

	if _, err := manager.ClearIndex(context.Background(), repoPath, testClientInfo()); err != nil {
		t.Fatalf("ClearIndex returned error: %v", err)
	}

	if _, err := os.Stat(chunkPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("chunk file still present after clear: %v", err)
	}

	_, _, found, err := manager.GetIndex(context.Background(), repoPath)
	if err != nil {
		t.Fatalf("GetIndex returned error: %v", err)
	}
	if found {
		t.Fatal("GetIndex still found a codebase after clear")
	}

	registry, err := store.ReadRegistry(cfg.RegistryPath)
	if err != nil {
		t.Fatalf("ReadRegistry returned error: %v", err)
	}
	if len(registry.Codebases) != 0 {
		t.Fatalf("registry still contains %d codebases", len(registry.Codebases))
	}
}

func TestClearIndexCancelsActiveJob(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	started := make(chan struct{})
	manager.runner = fakeRunner{
		index: func(ctx context.Context, root string, indexConfig model.IndexConfig, progress func(indexer.Progress)) (indexer.Result, error) {
			close(started)
			progress(indexer.Progress{
				Phase:          "Processing files and generating embeddings...",
				OverallPercent: 33.3,
				FilesTotal:     3,
				FilesProcessed: 1,
			})
			<-ctx.Done()
			return indexer.Result{}, ctx.Err()
		},
	}

	if _, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false); err != nil {
		t.Fatalf("StartIndex returned error: %v", err)
	}
	<-started

	if _, err := manager.ClearIndex(context.Background(), repoPath, testClientInfo()); err != nil {
		t.Fatalf("ClearIndex returned error: %v", err)
	}

	_, _, found, err := manager.GetIndex(context.Background(), repoPath)
	if err != nil {
		t.Fatalf("GetIndex returned error: %v", err)
	}
	if found {
		t.Fatal("GetIndex still found a codebase after active clear")
	}
}

func TestForceReindexStartsFreshJobAndSearchShowsIndexingWarning(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	initialResult := indexer.Result{
		IndexedFiles: 1,
		TotalChunks:  1,
		Chunks: []model.StoredChunk{{
			Content:       "func SearchableThing() {}\n",
			RelativePath:  "main.go",
			StartLine:     1,
			EndLine:       1,
			Language:      "go",
			FileExtension: ".go",
		}},
	}
	manager.runner = fakeRunner{
		index: func(ctx context.Context, root string, indexConfig model.IndexConfig, progress func(indexer.Progress)) (indexer.Result, error) {
			return initialResult, nil
		},
	}

	initialJob, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false)
	if err != nil {
		t.Fatalf("initial StartIndex returned error: %v", err)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	release := make(chan struct{})
	manager.runner = fakeRunner{
		index: func(ctx context.Context, root string, indexConfig model.IndexConfig, progress func(indexer.Progress)) (indexer.Result, error) {
			progress(indexer.Progress{
				Phase:           "Processing files and generating embeddings...",
				OverallPercent:  42.5,
				FilesTotal:      10,
				FilesProcessed:  4,
				ChunksGenerated: 1,
			})
			<-release
			return indexer.Result{
				IndexedFiles: 2,
				TotalChunks:  1,
				Chunks: []model.StoredChunk{{
					Content:       "func SearchableThing() {}\n// replacement\n",
					RelativePath:  "main.go",
					StartLine:     1,
					EndLine:       2,
					Language:      "go",
					FileExtension: ".go",
				}},
			}, nil
		},
	}

	reindexJob, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), true)
	if err != nil {
		t.Fatalf("force StartIndex returned error: %v", err)
	}
	if reindexJob.ID == initialJob.ID {
		t.Fatal("force reindex reused the previous job id")
	}
	waitForProgress(t, manager, repoPath, 42.5)

	server := NewGRPCServer(manager, nil)
	statusResponse, err := server.GetIndex(context.Background(), &pb.GetIndexRequest{Path: repoPath})
	if err != nil {
		t.Fatalf("GetIndex returned error: %v", err)
	}
	if !strings.Contains(statusResponse.GetDisplayText(), "42.5%") {
		t.Fatalf("GetIndex returned unexpected status text: %q", statusResponse.GetDisplayText())
	}

	searchResponse, err := server.SearchCode(context.Background(), &pb.SearchCodeRequest{
		Path:  repoPath,
		Query: "SearchableThing",
		Limit: 5,
	})
	if err != nil {
		t.Fatalf("SearchCode returned error: %v", err)
	}
	if len(searchResponse.GetResults()) == 0 {
		t.Fatal("SearchCode returned no results during force reindex")
	}
	if !strings.HasPrefix(searchResponse.GetDisplayText(), "⚠️  **Indexing in Progress**") {
		t.Fatalf("SearchCode returned unexpected warning text: %q", searchResponse.GetDisplayText())
	}
	if !strings.Contains(searchResponse.GetDisplayText(), "42.5%") {
		t.Fatalf("SearchCode returned unexpected progress text: %q", searchResponse.GetDisplayText())
	}

	close(release)
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)
}

func TestStartIndexForceDeduplicatesAgainstInFlightJob(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	initialResult := indexer.Result{
		IndexedFiles: 1,
		TotalChunks:  1,
		Chunks: []model.StoredChunk{{
			Content:       "package main\n",
			RelativePath:  "main.go",
			StartLine:     1,
			EndLine:       1,
			Language:      "go",
			FileExtension: ".go",
		}},
	}
	release := make(chan struct{})
	var indexCalls int32
	manager.runner = fakeRunner{
		index: func(ctx context.Context, root string, indexConfig model.IndexConfig, progress func(indexer.Progress)) (indexer.Result, error) {
			atomic.AddInt32(&indexCalls, 1)
			<-release
			return initialResult, nil
		},
	}

	firstJob, _, deduplicated, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), true)
	if err != nil {
		t.Fatalf("first force StartIndex returned error: %v", err)
	}
	if deduplicated {
		t.Fatal("first request should not be a dedup hit")
	}

	const concurrentForceCallers = 20
	type startResult struct {
		job          model.Job
		deduplicated bool
		err          error
	}
	results := make([]startResult, concurrentForceCallers)
	var wg sync.WaitGroup
	wg.Add(concurrentForceCallers)
	for i := 0; i < concurrentForceCallers; i++ {
		go func(slot int) {
			defer wg.Done()
			job, _, dedup, callErr := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), true)
			results[slot] = startResult{job: job, deduplicated: dedup, err: callErr}
		}(i)
	}
	wg.Wait()

	for slot, outcome := range results {
		if outcome.err != nil {
			t.Fatalf("concurrent force StartIndex %d returned error: %v", slot, outcome.err)
		}
		if !outcome.deduplicated {
			t.Fatalf("concurrent force StartIndex %d should have been deduplicated; got new job %s", slot, outcome.job.ID)
		}
		if outcome.job.ID != firstJob.ID {
			t.Fatalf("concurrent force StartIndex %d returned job %s want %s", slot, outcome.job.ID, firstJob.ID)
		}
	}

	close(release)
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	calls := atomic.LoadInt32(&indexCalls)
	if calls != 1 {
		t.Fatalf("indexer ran %d times; want exactly 1 (idempotent across %d force callers)", calls, concurrentForceCallers+1)
	}
}

func TestSyncIndexStartsFreshJobForChangedCodebase(t *testing.T) {
	t.Parallel()

	manager, cfg, repoPath := newTestManager(t)
	mainPath := filepath.Join(repoPath, "main.go")
	manager.runner = fakeRunner{
		index: func(ctx context.Context, root string, indexConfig model.IndexConfig, progress func(indexer.Progress)) (indexer.Result, error) {
			initialContent := "package main\nfunc SearchableThing() {}\n"
			return indexer.Result{
				IndexedFiles: 1,
				TotalChunks:  1,
				Chunks: []model.StoredChunk{{
					Content:       initialContent,
					RelativePath:  "main.go",
					StartLine:     1,
					EndLine:       2,
					Language:      "go",
					FileExtension: ".go",
				}},
				FileHashes: map[string]string{"main.go": hashText(initialContent)},
			}, nil
		},
	}

	_, codebase, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false)
	if err != nil {
		t.Fatalf("StartIndex returned error: %v", err)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	if err := os.WriteFile(mainPath, []byte("package main\nfunc SearchableThing() { println(\"changed\") }\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	release := make(chan struct{})
	manager.runner = fakeRunner{
		index: func(ctx context.Context, root string, indexConfig model.IndexConfig, progress func(indexer.Progress)) (indexer.Result, error) {
			changedContent := "package main\nfunc SearchableThing() { println(\"changed\") }\n"
			progress(indexer.Progress{
				Phase:           "Processing files and generating embeddings...",
				OverallPercent:  55,
				FilesTotal:      1,
				FilesProcessed:  1,
				ChunksGenerated: 1,
			})
			<-release
			return indexer.Result{
				IndexedFiles: 1,
				TotalChunks:  1,
				Chunks: []model.StoredChunk{{
					Content:       changedContent,
					RelativePath:  "main.go",
					StartLine:     1,
					EndLine:       2,
					Language:      "go",
					FileExtension: ".go",
				}},
				FileHashes: map[string]string{"main.go": hashText(changedContent)},
			}, nil
		},
	}

	syncer := NewBackgroundSync(cfg, manager)
	syncer.runSyncAll(context.Background(), "test")
	waitForProgress(t, manager, repoPath, 55)

	codebase, activeJob, found, err := manager.GetIndex(context.Background(), repoPath)
	if err != nil {
		t.Fatalf("GetIndex returned error: %v", err)
	}
	if !found || activeJob == nil {
		t.Fatal("expected active sync job")
	}
	if activeJob.Operation != "sync" {
		t.Fatalf("Operation=%q", activeJob.Operation)
	}
	if codebase.MerkleSnapshotPath == "" {
		t.Fatal("MerkleSnapshotPath was empty")
	}

	close(release)
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)
}

func TestBackgroundSyncSkipsUnchangedCodebase(t *testing.T) {
	t.Parallel()

	manager, cfg, repoPath := newTestManager(t)
	manager.runner = fakeRunner{
		index: func(ctx context.Context, root string, indexConfig model.IndexConfig, progress func(indexer.Progress)) (indexer.Result, error) {
			content := "package main\n"
			return indexer.Result{
				IndexedFiles: 1,
				TotalChunks:  1,
				Chunks: []model.StoredChunk{{
					Content:       content,
					RelativePath:  "main.go",
					StartLine:     1,
					EndLine:       1,
					Language:      "go",
					FileExtension: ".go",
				}},
				FileHashes: map[string]string{"main.go": hashText(content)},
			}, nil
		},
	}

	if _, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false); err != nil {
		t.Fatalf("StartIndex returned error: %v", err)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	manager.runner = fakeRunner{
		index: func(ctx context.Context, root string, indexConfig model.IndexConfig, progress func(indexer.Progress)) (indexer.Result, error) {
			t.Fatal("background sync should not have started a job")
			return indexer.Result{}, nil
		},
	}

	syncer := NewBackgroundSync(cfg, manager)
	syncer.runSyncAll(context.Background(), "test")

	jobs := manager.ListJobs("")
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
}

func TestGetIndexMatchesTrackedParentForSubdirectory(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	manager.runner = fakeRunner{
		index: func(ctx context.Context, root string, indexConfig model.IndexConfig, progress func(indexer.Progress)) (indexer.Result, error) {
			return indexer.Result{
				IndexedFiles: 1,
				TotalChunks:  1,
				Chunks: []model.StoredChunk{{
					Content:       "package main\n",
					RelativePath:  "main.go",
					StartLine:     1,
					EndLine:       1,
					Language:      "go",
					FileExtension: ".go",
				}},
			}, nil
		},
	}

	if _, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false); err != nil {
		t.Fatalf("StartIndex returned error: %v", err)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	childDirectory := filepath.Join(repoPath, "internal", "mcpserver")
	if err := os.MkdirAll(childDirectory, 0o755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}

	codebase, _, found, err := manager.GetIndex(context.Background(), childDirectory)
	if err != nil {
		t.Fatalf("GetIndex returned error: %v", err)
	}
	if !found {
		t.Fatal("GetIndex did not match the tracked parent codebase")
	}
	expectedCanonicalPath, err := filepath.EvalSymlinks(repoPath)
	if err != nil {
		t.Fatalf("EvalSymlinks returned error: %v", err)
	}
	if codebase.CanonicalPath != expectedCanonicalPath {
		t.Fatalf("GetIndex returned canonicalPath=%q", codebase.CanonicalPath)
	}
}

func newTestManager(t *testing.T) (*Manager, config.Config, string) {
	t.Helper()

	stateRoot := t.TempDir()
	repoPath := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(repoPath, 0o755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoPath, "main.go"), []byte("package main\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	cfg := config.Config{
		StateRoot:         stateRoot,
		SocketPath:        filepath.Join(stateRoot, "sockets", "claude-contextd.sock"),
		RegistryPath:      filepath.Join(stateRoot, "registry.json"),
		JobsPath:          filepath.Join(stateRoot, "jobs.jsonl"),
		EventsPath:        filepath.Join(stateRoot, "events.jsonl"),
		LogsDir:           filepath.Join(stateRoot, "logs"),
		LogPath:           filepath.Join(stateRoot, "logs", "claude-contextd.log"),
		MerkleDir:         filepath.Join(stateRoot, "merkle"),
		LocksDir:          filepath.Join(stateRoot, "locks"),
		SocketsDir:        filepath.Join(stateRoot, "sockets"),
		ChunksDir:         filepath.Join(stateRoot, "chunks"),
		ContextRoot:       filepath.Join(stateRoot, "context"),
		EmbeddingProvider: "OpenAI",
		EmbeddingModel:    "nvidia/NV-EmbedCode-7b-v1",
		HybridMode:        true,
		SyncIntervalMS:    300000,
		SyncLockStaleMS:   600000,
	}
	for _, path := range []string{cfg.StateRoot, cfg.LogsDir, cfg.MerkleDir, cfg.LocksDir, cfg.SocketsDir, cfg.ChunksDir, cfg.ContextRoot} {
		if err := store.EnsureDir(path); err != nil {
			t.Fatalf("EnsureDir returned error: %v", err)
		}
	}
	if err := store.WriteRegistry(cfg.RegistryPath, model.RegistryFile{}); err != nil {
		t.Fatalf("WriteRegistry returned error: %v", err)
	}

	manager, err := NewManager(cfg)
	if err != nil {
		t.Fatalf("NewManager returned error: %v", err)
	}
	return manager, cfg, repoPath
}

func defaultIndexConfig() model.IndexConfig {
	return model.IndexConfig{
		SplitterType:      "ast",
		SplitterChunkSize: 2500,
		SplitterOverlap:   300,
		VectorBackend:     "milvus",
		Hybrid:            true,
	}
}

func testClientInfo() model.ClientInfo {
	return model.ClientInfo{Name: "test"}
}

func hashText(text string) string {
	hashBytes := sha256.Sum256([]byte(text))
	return hex.EncodeToString(hashBytes[:])
}

func waitForCodebaseStatus(t *testing.T, manager *Manager, repoPath string, wantStatus model.CodebaseStatus) {
	t.Helper()

	waitForCondition(t, func() bool {
		codebase, _, found, err := manager.GetIndex(context.Background(), repoPath)
		if err != nil || !found {
			return false
		}
		return codebase.Status == wantStatus
	})
}

func waitForProgress(t *testing.T, manager *Manager, repoPath string, minimum float64) {
	t.Helper()

	waitForCondition(t, func() bool {
		_, activeJob, found, err := manager.GetIndex(context.Background(), repoPath)
		if err != nil || !found || activeJob == nil {
			return false
		}
		return activeJob.Progress.OverallPercent >= minimum
	})
}

func waitForCondition(t *testing.T, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition did not become true before timeout")
}
