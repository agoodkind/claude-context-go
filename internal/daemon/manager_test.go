package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "goodkind.io/lm-semantic-search/gen/go/lmsemanticsearch/v1"
	"goodkind.io/lm-semantic-search/internal/config"
	"goodkind.io/lm-semantic-search/internal/indexability"
	"goodkind.io/lm-semantic-search/internal/indexer"
	"goodkind.io/lm-semantic-search/internal/merkle"
	"goodkind.io/lm-semantic-search/internal/model"
	render "goodkind.io/lm-semantic-search/internal/render"
	"goodkind.io/lm-semantic-search/internal/semantic"
	"goodkind.io/lm-semantic-search/internal/store"
	"goodkind.io/lm-semantic-search/internal/view"
)

type fakeRunner struct {
	index      func(context.Context, string, model.IndexConfig, func(indexer.Progress)) (indexer.Result, error)
	indexFiles func(context.Context, string, []string, model.IndexConfig, func(indexer.Progress)) (indexer.Result, error)
	indexOne   func(context.Context, string, string, model.IndexConfig) (indexer.OneFileResult, error)
}

func (runner fakeRunner) Index(ctx context.Context, _ *indexability.Resolver, _ string, root string, indexConfig model.IndexConfig, progress func(indexer.Progress)) (indexer.Result, error) {
	return runner.index(ctx, root, indexConfig, progress)
}

func (runner fakeRunner) IndexFiles(ctx context.Context, _ *indexability.Resolver, _ string, root string, relativePaths []string, indexConfig model.IndexConfig, progress func(indexer.Progress)) (indexer.Result, error) {
	if runner.indexFiles != nil {
		return runner.indexFiles(ctx, root, relativePaths, indexConfig, progress)
	}
	return runner.index(ctx, root, indexConfig, progress)
}

// fakeRunnerRealIndexer backs fakeRunner.IndexOne when a test does not supply
// its own indexOne. The daemon drives every index through the per-file path,
// so the default double performs real per-file indexing of the on-disk files a
// test writes: real chunks and a content hash that matches the merkle
// snapshot, which keeps a follow-up sync from re-detecting unchanged files.
var fakeRunnerRealIndexer = indexer.NewRunner()

func (runner fakeRunner) IndexOne(ctx context.Context, resolver *indexability.Resolver, codebaseID string, root string, relativePath string, indexConfig model.IndexConfig) (indexer.OneFileResult, error) {
	if runner.indexOne != nil {
		return runner.indexOne(ctx, root, relativePath, indexConfig)
	}
	return fakeRunnerRealIndexer.IndexOne(ctx, resolver, codebaseID, root, relativePath, indexConfig)
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
	if strings.Contains(response.GetDisplayText(), "no such file or directory") {
		t.Fatalf("GetIndex added a path error for an existing path: %q", response.GetDisplayText())
	}
}

func TestGetIndexMissingPathRawErrorIsMCPOnly(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	server := NewGRPCServer(manager, nil)
	canonicalRepoPath, err := filepath.EvalSymlinks(repoPath)
	if err != nil {
		t.Fatalf("EvalSymlinks returned error for repository path: %v", err)
	}
	missingPath := filepath.Join(canonicalRepoPath, "missing")
	_, pathErr := filepath.EvalSymlinks(missingPath)
	if !errors.Is(pathErr, os.ErrNotExist) {
		t.Fatalf("EvalSymlinks returned error %v, want missing-path error", pathErr)
	}
	wantMCPStatus := "❌ Codebase '" + missingPath +
		"' is not indexed. Please use the index_codebase tool to index it first.\n" +
		pathErr.Error() + "\n" +
		"🛈 Path is not under any tracked codebase."

	mcpResponse, err := server.GetIndex(context.Background(), &pb.GetIndexRequest{
		Path:   missingPath,
		Client: &pb.ClientInfo{Name: "mcp"},
	})
	if err != nil {
		t.Fatalf("MCP GetIndex returned error: %v", err)
	}
	if !strings.Contains(mcpResponse.GetDisplayText(), wantMCPStatus) {
		t.Fatalf("MCP GetIndex returned text %q, want contiguous status %q", mcpResponse.GetDisplayText(), wantMCPStatus)
	}

	otherResponse, err := server.GetIndex(context.Background(), &pb.GetIndexRequest{
		Path:   missingPath,
		Client: &pb.ClientInfo{Name: "test"},
	})
	if err != nil {
		t.Fatalf("non-MCP GetIndex returned error: %v", err)
	}
	if strings.Contains(otherResponse.GetDisplayText(), "lstat "+missingPath) {
		t.Fatalf("non-MCP GetIndex returned the raw path error: %q", otherResponse.GetDisplayText())
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

	_, codebase, _, _, err := manager.StartIndex(
		context.Background(),
		repoPath,
		testClientInfo(),
		defaultIndexConfig(),
		false,
		emptyAdmissionBudget,
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
	semanticDouble := &fakeSemantic{
		collectionName:       func(string) string { return "clear_collection" },
		listCollections:      func(context.Context) ([]string, error) { return []string{}, nil },
		hasCollectionForPath: func(context.Context, string) (bool, error) { return false, nil },
	}
	manager.semantic = semanticDouble
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

	_, codebase, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false, emptyAdmissionBudget)
	if err != nil {
		t.Fatalf("StartIndex returned error: %v", err)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	chunkPath := filepath.Join(cfg.ChunksDir, codebase.ID+".json")
	merklePath := filepath.Join(cfg.MerkleDir, codebase.ID+".json")
	stagingMerklePath := manager.stagingMerklePath(codebase.ID)
	if _, err := os.Stat(chunkPath); err != nil {
		t.Fatalf("chunk file missing before clear: %v", err)
	}
	if _, err := os.Stat(merklePath); err != nil {
		t.Fatalf("merkle file missing before clear: %v", err)
	}
	stagingCheckpoint := merkle.Snapshot{
		ConfigDigest: codebase.EffectiveConfig.IgnoreDigest,
		Files:        map[string]string{"main.go": "stale-checkpoint"},
	}
	if err := merkle.WriteSnapshot(stagingMerklePath, stagingCheckpoint); err != nil {
		t.Fatalf("WriteSnapshot returned error: %v", err)
	}
	if _, err := os.Stat(stagingMerklePath); err != nil {
		t.Fatalf("staging merkle file missing before clear: %v", err)
	}
	stagingDropsBeforeClear := len(semanticDouble.droppedStaging)

	if _, err := manager.ClearIndex(context.Background(), repoPath, testClientInfo()); err != nil {
		t.Fatalf("ClearIndex returned error: %v", err)
	}

	if _, err := os.Stat(chunkPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("chunk file still present after clear: %v", err)
	}
	if _, err := os.Stat(merklePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("merkle file still present after clear: %v", err)
	}
	if _, err := os.Stat(stagingMerklePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging merkle file still present after clear: %v", err)
	}
	if len(semanticDouble.dropped) != 1 || semanticDouble.dropped[0] != codebase.CanonicalPath {
		t.Fatalf("semantic drop calls = %v, want [%s]", semanticDouble.dropped, codebase.CanonicalPath)
	}
	if len(semanticDouble.droppedStaging) != stagingDropsBeforeClear+1 || semanticDouble.droppedStaging[len(semanticDouble.droppedStaging)-1] != codebase.CanonicalPath {
		t.Fatalf("semantic staging drop calls = %v, want one additional clear-time drop for %s", semanticDouble.droppedStaging, codebase.CanonicalPath)
	}

	_, _, found, _, err := manager.GetIndex(context.Background(), repoPath)
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
	var startOnce sync.Once
	manager.runner = fakeRunner{
		indexOne: func(ctx context.Context, root string, relativePath string, indexConfig model.IndexConfig) (indexer.OneFileResult, error) {
			startOnce.Do(func() { close(started) })
			<-ctx.Done()
			return indexer.OneFileResult{Chunks: nil, FileHash: "", Skipped: false, Removed: false}, ctx.Err()
		},
	}

	if _, _, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false, emptyAdmissionBudget); err != nil {
		t.Fatalf("StartIndex returned error: %v", err)
	}
	<-started

	if _, err := manager.ClearIndex(context.Background(), repoPath, testClientInfo()); err != nil {
		t.Fatalf("ClearIndex returned error: %v", err)
	}

	_, _, found, _, err := manager.GetIndex(context.Background(), repoPath)
	if err != nil {
		t.Fatalf("GetIndex returned error: %v", err)
	}
	if found {
		t.Fatal("GetIndex still found a codebase after active clear")
	}
}

func TestCancelActiveJobWithoutHandleFinalizesJob(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	canonicalPath, err := canonicalizePath(repoPath)
	if err != nil {
		t.Fatalf("canonicalizePath: %v", err)
	}
	codebase := newCodebaseRecord(canonicalPath)
	job := model.Job{
		ID:            "job_handleless",
		CodebaseID:    codebase.ID,
		CanonicalPath: canonicalPath,
		State:         model.JobStateRunning,
	}
	codebase.ActiveJobID = job.ID
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.jobs[job.ID] = job
	manager.mu.Unlock()

	if err := manager.cancelActiveJobForPath(context.Background(), canonicalPath); err != nil {
		t.Fatalf("cancelActiveJobForPath: %v", err)
	}

	cancelled, found := manager.GetJob(job.ID)
	if !found {
		t.Fatal("cancelled handleless job is missing")
	}
	if cancelled.State != model.JobStateCancelled {
		t.Fatalf("job state = %q, want cancelled", cancelled.State)
	}
	manager.mu.Lock()
	updatedCodebase := manager.codebases[codebase.ID]
	manager.mu.Unlock()
	if updatedCodebase.ActiveJobID != "" {
		t.Fatalf("ActiveJobID = %q, want empty", updatedCodebase.ActiveJobID)
	}
}

func TestForceStartIndexUsesCollectionEvidenceAfterCancellation(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	configuration := manager.enrichIndexConfig(defaultIndexConfig())
	configuration.IgnoreDigest = digestIndexConfig(configuration)
	collectionMissing := atomic.Bool{}
	observedMissingCollection := atomic.Bool{}
	manager.semantic = &fakeSemantic{
		collectionName: func(string) string { return "force-reprobe-collection" },
		inspectCollection: func(context.Context, string) (semantic.CollectionFacts, error) {
			if collectionMissing.Load() {
				observedMissingCollection.Store(true)
				return semantic.CollectionFacts{Exists: false, Rows: 0, RowsKnown: false}, nil
			}
			return semantic.CollectionFacts{Exists: true, Rows: 1, RowsKnown: true}, nil
		},
	}
	manager.runner = fakeRunner{
		indexOne: func(_ context.Context, _ string, relativePath string, _ model.IndexConfig) (indexer.OneFileResult, error) {
			return indexer.OneFileResult{
				Chunks: []model.StoredChunk{{
					Content:       "package main\n",
					RelativePath:  relativePath,
					StartLine:     1,
					EndLine:       1,
					Language:      "go",
					FileExtension: ".go",
				}},
				FileHash: hashText("package main\n"),
			}, nil
		},
	}
	canonicalPath, err := manager.resolveCanonicalPath(repoPath)
	if err != nil {
		t.Fatalf("resolveCanonicalPath: %v", err)
	}
	codebase := newCodebaseRecord(canonicalPath)
	codebase.Status = model.CodebaseStatusIndexed
	codebase.EffectiveConfig = configuration
	codebase.CollectionName = "force-reprobe-collection"
	codebase.LastSuccessfulRun = &model.IndexRunSummary{IndexedFiles: 1, TotalChunks: 1, Status: "completed"}
	active := model.Job{ID: "job-force-reprobe-active", CodebaseID: codebase.ID, CanonicalPath: canonicalPath, State: model.JobStateRunning, Config: configuration}
	codebase.ActiveJobID = active.ID
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.jobs[active.ID] = active
	manager.mu.Unlock()
	manager.appendJobTransition = func(event model.JobEvent) error {
		if event.Event == "job_cancelled" {
			collectionMissing.Store(true)
		}
		return nil
	}

	job, _, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), true, emptyAdmissionBudget)
	if err != nil {
		t.Fatalf("StartIndex(force=true): %v", err)
	}
	if job.ID == "" {
		t.Fatal("StartIndex(force=true) returned no job")
	}
	if !observedMissingCollection.Load() {
		t.Fatal("StartIndex did not re-probe collection state after force cancellation")
	}
}

// TestStartIndexRecordsForceFlagOnJob proves the job records whether the caller
// passed force=true, so a trigger-aware heading can distinguish a forced reindex
// from a first build or a changed-files sync.
func TestStartIndexRecordsForceFlagOnJob(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	manager.runner = fakeRunner{
		indexOne: func(_ context.Context, _ string, relativePath string, _ model.IndexConfig) (indexer.OneFileResult, error) {
			content := "package main\n"
			return indexer.OneFileResult{
				Chunks:   []model.StoredChunk{{Content: content, RelativePath: relativePath, StartLine: 1, EndLine: 1, Language: "go", FileExtension: ".go"}},
				FileHash: hashText(content),
				Skipped:  false,
				Removed:  false,
			}, nil
		},
	}

	plainJob, _, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false, emptyAdmissionBudget)
	if err != nil {
		t.Fatalf("StartIndex(force=false) returned error: %v", err)
	}
	if plainJob.Forced {
		t.Fatalf("StartIndex(force=false) job.Forced = true, want false")
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	forcedJob, _, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), true, emptyAdmissionBudget)
	if err != nil {
		t.Fatalf("StartIndex(force=true) returned error: %v", err)
	}
	if !forcedJob.Forced {
		t.Fatalf("StartIndex(force=true) job.Forced = false, want true")
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)
}

// TestStartIndexPersistsSkippedFiles proves the daemon writes the indexer's
// skipped-file list into the codebase registry's LastSuccessfulRun. The
// operator surface (GetIndex display, Doctor output) reads from there.
func TestStartIndexPersistsSkippedFiles(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	// Two discoverable .go files the per-file indexer reports as skipped (the
	// production trigger is non-UTF-8 content). The daemon must persist that
	// skipped list into LastSuccessfulRun and Doctor must surface the count.
	for _, name := range []string{"binary.go", "weird.go"} {
		if err := os.WriteFile(filepath.Join(repoPath, name), []byte("package main\n"), 0o644); err != nil {
			t.Fatalf("WriteFile returned error: %v", err)
		}
	}
	manager.runner = fakeRunner{
		indexOne: func(ctx context.Context, root string, relativePath string, indexConfig model.IndexConfig) (indexer.OneFileResult, error) {
			if relativePath == "binary.go" || relativePath == "weird.go" {
				return indexer.OneFileResult{Chunks: nil, FileHash: "", Skipped: true, Removed: false}, nil
			}
			content := "package main\n"
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

	if _, _, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false, emptyAdmissionBudget); err != nil {
		t.Fatalf("StartIndex returned error: %v", err)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	codebase, _, found, _, err := manager.GetIndex(context.Background(), repoPath)
	if err != nil {
		t.Fatalf("GetIndex returned error: %v", err)
	}
	if !found {
		t.Fatal("GetIndex did not find the codebase")
	}
	if codebase.LastSuccessfulRun == nil {
		t.Fatal("LastSuccessfulRun nil after StartIndex completion")
	}
	wantSkipped := []string{"binary.go", "weird.go"}
	if !slices.Equal(codebase.LastSuccessfulRun.SkippedFiles, wantSkipped) {
		t.Fatalf("LastSuccessfulRun.SkippedFiles = %v, want %v", codebase.LastSuccessfulRun.SkippedFiles, wantSkipped)
	}

	diagnostics := manager.Doctor(context.Background())
	wantSuffix := "2 non-UTF-8 files skipped during last indexing run"
	foundDiagnostic := false
	for _, diagnostic := range diagnostics {
		if strings.Contains(diagnostic, wantSuffix) {
			foundDiagnostic = true
			break
		}
	}
	if !foundDiagnostic {
		t.Fatalf("Doctor diagnostics did not surface skipped-file summary; got %v", diagnostics)
	}
}

func TestStartIndexForceDeduplicatesAgainstInFlightJob(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	release := make(chan struct{})
	var indexCalls int32
	manager.runner = fakeRunner{
		indexOne: func(ctx context.Context, root string, relativePath string, indexConfig model.IndexConfig) (indexer.OneFileResult, error) {
			atomic.AddInt32(&indexCalls, 1)
			<-release
			return indexer.OneFileResult{
				Chunks: []model.StoredChunk{{
					Content:       "package main\n",
					RelativePath:  relativePath,
					StartLine:     1,
					EndLine:       1,
					Language:      "go",
					FileExtension: ".go",
				}},
				FileHash: hashText("package main\n"),
				Skipped:  false,
				Removed:  false,
			}, nil
		},
	}

	firstJob, _, deduplicated, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), true, emptyAdmissionBudget)
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
			job, _, dedup, _, callErr := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), true, emptyAdmissionBudget)
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

// TestStartIndexStreamingReindexUpgradesSplitterInPlace exercises the agent
// driven granularity upgrade path. After an initial langchain index, calling
// StartIndex again with the ast splitter (force=false, config differs) must
// queue a new job that streams replacements through semantic.Reindex rather
// than dropping the collection. The new EffectiveConfig reflects the
// requested splitter at job completion.
func TestStartIndexStreamingReindexUpgradesSplitterInPlace(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	indexCalls := atomic.Int32{}
	indexFilesCalls := atomic.Int32{}
	var observedFiles []string
	var fileMu sync.Mutex
	manager.runner = fakeRunner{
		index: func(ctx context.Context, root string, indexConfig model.IndexConfig, progress func(indexer.Progress)) (indexer.Result, error) {
			indexCalls.Add(1)
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
		indexOne: func(ctx context.Context, root string, relativePath string, indexConfig model.IndexConfig) (indexer.OneFileResult, error) {
			indexFilesCalls.Add(1)
			fileMu.Lock()
			observedFiles = append(observedFiles, relativePath)
			fileMu.Unlock()
			content := "package main\nfunc main() {}\n"
			return indexer.OneFileResult{
				Chunks: []model.StoredChunk{
					{Content: content, RelativePath: relativePath, StartLine: 1, EndLine: 1, Language: "go", FileExtension: ".go"},
					{Content: content, RelativePath: relativePath, StartLine: 2, EndLine: 2, Language: "go", FileExtension: ".go"},
				},
				FileHash: hashText(content),
				Skipped:  false,
			}, nil
		},
	}

	initialConfig := defaultIndexConfig()
	initialConfig.SplitterType = "langchain"
	if _, _, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), initialConfig, false, emptyAdmissionBudget); err != nil {
		t.Fatalf("initial StartIndex returned error: %v", err)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	upgradedConfig := defaultIndexConfig()
	upgradedConfig.SplitterType = "ast"
	upgradeJob, _, deduplicated, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), upgradedConfig, false, emptyAdmissionBudget)
	if err != nil {
		t.Fatalf("upgrade StartIndex returned error: %v", err)
	}
	if deduplicated {
		t.Fatal("upgrade call was deduplicated; agent-driven splitter upgrade should queue a new job")
	}
	if jobOperation(upgradeJob.Operation) != jobOperationStreamingReindex {
		t.Fatalf("upgrade job Operation = %q, want %q", upgradeJob.Operation, jobOperationStreamingReindex)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	if indexFilesCalls.Load() == 0 {
		t.Fatal("IndexOne was never called; streaming reindex should walk the codebase file by file")
	}
	fileMu.Lock()
	finalObservedFiles := append([]string{}, observedFiles...)
	fileMu.Unlock()
	if !slices.Contains(finalObservedFiles, "main.go") {
		t.Fatalf("streaming reindex did not pass main.go as modified; saw %v", finalObservedFiles)
	}

	codebase, _, found, _, err := manager.GetIndex(context.Background(), repoPath)
	if err != nil {
		t.Fatalf("GetIndex returned error: %v", err)
	}
	if !found {
		t.Fatal("GetIndex did not find the upgraded codebase")
	}
	if codebase.EffectiveConfig.SplitterType != "ast" {
		t.Fatalf("EffectiveConfig.SplitterType = %q after streaming reindex, want %q", codebase.EffectiveConfig.SplitterType, "ast")
	}
}

// TestRunDeltaSyncCheckpointsPerFile proves the merkle snapshot grows file
// by file as the streaming reindex makes progress. A daemon crash between
// any two file embeds leaves a partial snapshot on disk that the next run
// reads as the resume seed.
func TestRunDeltaSyncCheckpointsPerFile(t *testing.T) {
	t.Parallel()

	manager, cfg, repoPath := newTestManager(t)
	extras := []string{"a.go", "b.go", "c.go", "d.go"}
	for _, name := range extras {
		path := filepath.Join(repoPath, name)
		if err := os.WriteFile(path, []byte("package main\n// "+name+"\n"), 0o644); err != nil {
			t.Fatalf("WriteFile returned error: %v", err)
		}
	}

	// The initial index uses the real per-file indexer, establishing a
	// checkpoint whose hashes match disk.
	if _, _, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false, emptyAdmissionBudget); err != nil {
		t.Fatalf("initial StartIndex returned error: %v", err)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	// Change every file so the streaming reindex re-embeds all five and
	// checkpoints after each.
	for _, name := range []string{"main.go", "a.go", "b.go", "c.go", "d.go"} {
		if err := os.WriteFile(filepath.Join(repoPath, name), []byte("package main\n// "+name+" edited\n"), 0o644); err != nil {
			t.Fatalf("WriteFile returned error: %v", err)
		}
	}

	embedCalls := atomic.Int32{}
	manager.runner = fakeRunner{
		indexOne: func(ctx context.Context, root string, relativePath string, indexConfig model.IndexConfig) (indexer.OneFileResult, error) {
			embedCalls.Add(1)
			content := "package main\n// " + relativePath + " edited\n"
			return indexer.OneFileResult{
				Chunks: []model.StoredChunk{{
					Content:       content,
					RelativePath:  relativePath,
					StartLine:     1,
					EndLine:       2,
					Language:      "go",
					FileExtension: ".go",
				}},
				FileHash: hashText(content),
				Skipped:  false,
				Removed:  false,
			}, nil
		},
	}

	// Force a streaming reindex by passing force=true (matching config).
	if _, _, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), true, emptyAdmissionBudget); err != nil {
		t.Fatalf("streaming StartIndex returned error: %v", err)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	codebase, _, found, _, err := manager.GetIndex(context.Background(), repoPath)
	if err != nil || !found {
		t.Fatalf("GetIndex returned err=%v found=%v", err, found)
	}
	snapshotPath := filepath.Join(cfg.MerkleDir, codebase.ID+".json")
	snapshot, err := merkle.ReadSnapshot(snapshotPath)
	if err != nil {
		t.Fatalf("ReadSnapshot returned error: %v", err)
	}
	if snapshot.ConfigDigest != codebase.EffectiveConfig.IgnoreDigest {
		t.Fatalf("snapshot ConfigDigest=%q want=%q", snapshot.ConfigDigest, codebase.EffectiveConfig.IgnoreDigest)
	}
	for _, name := range []string{"main.go", "a.go", "b.go", "c.go", "d.go"} {
		if _, present := snapshot.Files[name]; !present {
			t.Fatalf("snapshot missing %s; have %v", name, snapshot.Files)
		}
	}
	if got := embedCalls.Load(); got != 5 {
		t.Fatalf("indexOne calls = %d, want 5", got)
	}
}

// TestRunDeltaSyncReportsCodebaseTotalsAfterSync proves the registry's
// LastSuccessfulRun describes the codebase as a whole after an incremental
// sync rather than the delta of files touched in that one run. The test
// indexes five files, modifies one, runs a sync via the background-sync
// harness, and asserts IndexedFiles == 5.
func TestRunDeltaSyncReportsCodebaseTotalsAfterSync(t *testing.T) {
	t.Parallel()

	manager, cfg, repoPath := newTestManager(t)
	extras := []string{"a.go", "b.go", "c.go", "d.go"}
	for _, name := range extras {
		path := filepath.Join(repoPath, name)
		if err := os.WriteFile(path, []byte("package main\n// "+name+"\n"), 0o644); err != nil {
			t.Fatalf("WriteFile returned error: %v", err)
		}
	}

	initialHashes := map[string]string{
		"main.go": hashText("package main\n"),
		"a.go":    hashText("package main\n// a.go\n"),
		"b.go":    hashText("package main\n// b.go\n"),
		"c.go":    hashText("package main\n// c.go\n"),
		"d.go":    hashText("package main\n// d.go\n"),
	}
	manager.runner = fakeRunner{
		index: func(ctx context.Context, root string, indexConfig model.IndexConfig, progress func(indexer.Progress)) (indexer.Result, error) {
			return indexer.Result{
				IndexedFiles: 5,
				TotalChunks:  5,
				Chunks: []model.StoredChunk{{
					Content:       "package main\n",
					RelativePath:  "main.go",
					StartLine:     1,
					EndLine:       1,
					Language:      "go",
					FileExtension: ".go",
				}},
				FileHashes: initialHashes,
			}, nil
		},
		indexOne: func(ctx context.Context, root string, relativePath string, indexConfig model.IndexConfig) (indexer.OneFileResult, error) {
			content := "package main\n// " + relativePath + " edited\n"
			return indexer.OneFileResult{
				Chunks: []model.StoredChunk{{
					Content:       content,
					RelativePath:  relativePath,
					StartLine:     1,
					EndLine:       2,
					Language:      "go",
					FileExtension: ".go",
				}},
				FileHash: hashText(content),
				Skipped:  false,
			}, nil
		},
	}

	if _, _, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false, emptyAdmissionBudget); err != nil {
		t.Fatalf("initial StartIndex returned error: %v", err)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	mainPath := filepath.Join(repoPath, "main.go")
	if err := os.WriteFile(mainPath, []byte("package main\nfunc Edited() {}\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	syncer := NewBackgroundSync(cfg, manager)
	syncer.runSyncAll(context.Background(), "test")
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	codebase, _, found, _, err := manager.GetIndex(context.Background(), repoPath)
	if err != nil || !found {
		t.Fatalf("GetIndex returned err=%v found=%v", err, found)
	}
	if codebase.LastSuccessfulRun == nil {
		t.Fatal("LastSuccessfulRun is nil after sync")
	}
	if codebase.LastSuccessfulRun.IndexedFiles != 5 {
		t.Fatalf("LastSuccessfulRun.IndexedFiles = %d, want 5 (codebase total, not delta)", codebase.LastSuccessfulRun.IndexedFiles)
	}
}

func TestRunDeltaSyncPreservesWholeCodebaseTotalBytesAfterSmallEdit(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	extras := []string{"a.go", "b.go", "c.go", "d.go"}
	for _, name := range extras {
		path := filepath.Join(repoPath, name)
		content := "package main\n// " + strings.Repeat(name+"\n", 4)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("WriteFile returned error: %v", err)
		}
	}

	if _, _, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false, emptyAdmissionBudget); err != nil {
		t.Fatalf("initial StartIndex returned error: %v", err)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	initialCodebase, _, found, _, err := manager.GetIndex(context.Background(), repoPath)
	if err != nil || !found {
		t.Fatalf("initial GetIndex returned err=%v found=%v", err, found)
	}
	if initialCodebase.LastSuccessfulRun == nil {
		t.Fatal("initial LastSuccessfulRun is nil")
	}
	initialTotalBytes := initialCodebase.LastSuccessfulRun.TotalBytes
	if initialTotalBytes == 0 {
		t.Fatal("initial LastSuccessfulRun.TotalBytes is 0; test requires a non-zero byte total")
	}
	initialChunks, err := store.ReadChunks(manager.chunkPath(initialCodebase.ID))
	if err != nil {
		t.Fatalf("ReadChunks returned error: %v", err)
	}
	initialMainBytes := storedChunkBytesForPath(initialChunks, "main.go")
	if initialMainBytes == 0 {
		t.Fatal("initial chunk cache has no bytes for main.go")
	}

	changedContent := "package main\nfunc Edited() {}\n"
	manager.runner = fakeRunner{
		indexOne: func(ctx context.Context, root string, relativePath string, indexConfig model.IndexConfig) (indexer.OneFileResult, error) {
			if relativePath != "main.go" {
				t.Errorf("IndexOne called for %s, want only main.go", relativePath)
			}
			return indexer.OneFileResult{
				Chunks: []model.StoredChunk{{
					Content:       changedContent,
					RelativePath:  relativePath,
					StartLine:     1,
					EndLine:       2,
					Language:      "go",
					FileExtension: ".go",
				}},
				FileHash: hashText(changedContent),
				Skipped:  false,
				Removed:  false,
			}, nil
		},
	}
	if err := os.WriteFile(filepath.Join(repoPath, "main.go"), []byte(changedContent), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	if _, _, _, err := manager.SyncIndex(context.Background(), repoPath, model.ClientInfo{Name: "test-sync"}); err != nil {
		t.Fatalf("SyncIndex returned error: %v", err)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	codebase, _, found, _, err := manager.GetIndex(context.Background(), repoPath)
	if err != nil || !found {
		t.Fatalf("GetIndex returned err=%v found=%v", err, found)
	}
	if codebase.LastSuccessfulRun == nil {
		t.Fatal("LastSuccessfulRun is nil after sync")
	}
	expectedTotalBytes := initialTotalBytes - initialMainBytes + int64(len(changedContent))
	if codebase.LastSuccessfulRun.TotalBytes != expectedTotalBytes {
		t.Fatalf("LastSuccessfulRun.TotalBytes = %d, want %d (whole codebase total after one-file edit)", codebase.LastSuccessfulRun.TotalBytes, expectedTotalBytes)
	}
	if codebase.LastSuccessfulRun.TotalBytes < initialTotalBytes {
		t.Fatalf("LastSuccessfulRun.TotalBytes shrank from %d to %d after a small edit", initialTotalBytes, codebase.LastSuccessfulRun.TotalBytes)
	}
}

func storedChunkBytesForPath(chunks []model.StoredChunk, relativePath string) int64 {
	var total int64
	for _, chunk := range chunks {
		if chunk.RelativePath != relativePath {
			continue
		}
		total += int64(len(chunk.Content))
	}
	return total
}

// TestRunDeltaSyncConvergesDeletedFileToRemoval proves that a file present at
// capture time but reported absent when its converge task runs is treated as
// a removal: the job completes, and the path is dropped from the snapshot so
// the next run does not carry it forward. This is the mid-run delete case
// that previously failed the whole job on an os.Stat error.
func TestRunDeltaSyncConvergesDeletedFileToRemoval(t *testing.T) {
	t.Parallel()

	manager, cfg, repoPath := newTestManager(t)
	extras := []string{"a.go", "b.go", "c.go", "d.go"}
	for _, name := range extras {
		path := filepath.Join(repoPath, name)
		if err := os.WriteFile(path, []byte("package main\n// "+name+"\n"), 0o644); err != nil {
			t.Fatalf("WriteFile returned error: %v", err)
		}
	}

	// The initial index uses the real per-file indexer so the checkpoint records
	// all five files with hashes that match disk.
	if _, _, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false, emptyAdmissionBudget); err != nil {
		t.Fatalf("initial StartIndex returned error: %v", err)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	// Change a.go on disk so the next sync classifies it as modified and runs
	// its converge task. The leaf below then reports the changed file absent,
	// exercising the removal path; the four unchanged files are skipped.
	if err := os.WriteFile(filepath.Join(repoPath, "a.go"), []byte("package main\n// a.go changed\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	manager.runner = fakeRunner{
		indexOne: func(ctx context.Context, root string, relativePath string, indexConfig model.IndexConfig) (indexer.OneFileResult, error) {
			return indexer.OneFileResult{Chunks: nil, FileHash: "", Skipped: false, Removed: true}, nil
		},
	}

	syncer := NewBackgroundSync(cfg, manager)
	syncer.runSyncAll(context.Background(), "test")
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	codebase, _, found, _, err := manager.GetIndex(context.Background(), repoPath)
	if err != nil || !found {
		t.Fatalf("GetIndex returned err=%v found=%v", err, found)
	}
	if codebase.LastFailedRun != nil {
		t.Fatalf("LastFailedRun = %+v, want nil; an absent file must not fail the job", codebase.LastFailedRun)
	}
	snapshotPath := filepath.Join(cfg.MerkleDir, codebase.ID+".json")
	snapshot, err := merkle.ReadSnapshot(snapshotPath)
	if err != nil {
		t.Fatalf("ReadSnapshot returned error: %v", err)
	}
	if _, present := snapshot.Files["a.go"]; present {
		t.Fatalf("snapshot still lists a.go after removal; have %v", snapshot.Files)
	}
	for _, name := range []string{"main.go", "b.go", "c.go", "d.go"} {
		if _, present := snapshot.Files[name]; !present {
			t.Fatalf("snapshot dropped %s; a removal must not affect other files; have %v", name, snapshot.Files)
		}
	}
}

// TestRenderHistoricalFailureIncludesCorrelationIds proves a failed-run
// status line carries the trace_id and job_id so the operator can resolve it
// against the daemon's structured logs.
func TestRenderHistoricalFailureIncludesCorrelationIds(t *testing.T) {
	t.Parallel()

	codebase := model.Codebase{
		CanonicalPath: "/repo",
		LastFailedRun: &model.IndexRunFailure{
			Message:                 "boom",
			LastAttemptedPercentage: 42,
			FailedAt:                time.Now(),
			TraceID:                 "trace-abc",
			JobID:                   "job-xyz",
		},
	}
	failure := resolveCodebaseFailure(codebase)
	out := render.GetIndex(view.GetIndexView{
		Tracked:       true,
		RequestedPath: codebase.CanonicalPath,
		CanonicalPath: codebase.CanonicalPath,
		Display:       view.Display(displayFailed),
		Failure:       failure,
		Narrative:     resolveStatusNarrative(displayFailed, codebase.CanonicalPath, collectionNotApplicable, failure, view.QuarantineSurface{}, view.StatusView{}),
	})
	if !strings.Contains(out, "trace_id=trace-abc") {
		t.Fatalf("render output missing trace_id; got %q", out)
	}
	// The diagnostics line leads with the failed job and folds the trace into
	// parentheses, so it reads as the past failure's reference rather than a
	// second request-trace line.
	if !strings.Contains(out, "Failed job job-xyz") {
		t.Fatalf("render output missing failed-job reference; got %q", out)
	}
}

func TestRenderStaleStatusIncludesRepairReason(t *testing.T) {
	t.Parallel()

	codebase := model.Codebase{
		CanonicalPath: "/repo",
		LastFailedRun: &model.IndexRunFailure{
			Message:                 "Milvus collection missing; automatic rebuild could not start: boom",
			LastAttemptedPercentage: 0,
			FailedAt:                time.Now(),
			TraceID:                 "trace-abc",
			JobID:                   "job-xyz",
		},
	}
	failure := resolveCodebaseFailure(codebase)
	out := render.GetIndex(view.GetIndexView{
		Tracked:       true,
		RequestedPath: codebase.CanonicalPath,
		CanonicalPath: codebase.CanonicalPath,
		Display:       view.Display(displayStale),
		Failure:       failure,
		Narrative:     resolveStatusNarrative(displayStale, codebase.CanonicalPath, collectionNotApplicable, failure, view.QuarantineSurface{}, view.StatusView{}),
	})
	if !strings.Contains(out, "is stale") {
		t.Fatalf("render output missing stale marker; got %q", out)
	}
	if !strings.Contains(out, "automatic rebuild could not start") {
		t.Fatalf("render output missing repair detail; got %q", out)
	}
	if !strings.Contains(out, "trace_id=trace-abc") {
		t.Fatalf("render output missing trace_id; got %q", out)
	}
}

// TestRunDeltaSyncEmptyDiffPreservesCodebaseTotals proves the empty-diff
// fast path no longer re-zeros LastSuccessfulRun every time a background
// sync runs against an unchanged codebase. The test indexes five files,
// runs a no-op sync, and asserts the stored file and byte totals survive.
func TestRunDeltaSyncEmptyDiffPreservesCodebaseTotals(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	extras := []string{"a.go", "b.go", "c.go", "d.go"}
	for _, name := range extras {
		path := filepath.Join(repoPath, name)
		if err := os.WriteFile(path, []byte("package main\n// "+name+"\n"), 0o644); err != nil {
			t.Fatalf("WriteFile returned error: %v", err)
		}
	}

	// The initial index uses the real per-file indexer so the checkpoint hashes
	// match disk; a follow-up sync with no changes then yields an empty diff.
	if _, _, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false, emptyAdmissionBudget); err != nil {
		t.Fatalf("initial StartIndex returned error: %v", err)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)
	initialCodebase, _, found, _, err := manager.GetIndex(context.Background(), repoPath)
	if err != nil || !found {
		t.Fatalf("initial GetIndex returned err=%v found=%v", err, found)
	}
	if initialCodebase.LastSuccessfulRun == nil {
		t.Fatal("initial LastSuccessfulRun is nil")
	}
	initialTotalBytes := initialCodebase.LastSuccessfulRun.TotalBytes
	if initialTotalBytes == 0 {
		t.Fatal("initial LastSuccessfulRun.TotalBytes is 0; test requires a non-zero byte total")
	}

	// Any per-file embed on the unchanged codebase is a bug: the empty-diff fast
	// path must skip every file.
	manager.runner = fakeRunner{
		indexOne: func(ctx context.Context, root string, relativePath string, indexConfig model.IndexConfig) (indexer.OneFileResult, error) {
			t.Errorf("indexOne should not run on empty-diff sync; got %s", relativePath)
			return indexer.OneFileResult{Chunks: nil, FileHash: "", Skipped: true, Removed: false}, nil
		},
	}

	// Call SyncIndex directly so the planSyncDiff empty-diff fast path
	// runs even though the background-sync pre-check would have skipped
	// this unchanged codebase.
	if _, _, _, err := manager.SyncIndex(context.Background(), repoPath, model.ClientInfo{Name: "test-sync"}); err != nil {
		t.Fatalf("SyncIndex returned error: %v", err)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	codebase, _, found, _, err := manager.GetIndex(context.Background(), repoPath)
	if err != nil || !found {
		t.Fatalf("GetIndex returned err=%v found=%v", err, found)
	}
	if codebase.LastSuccessfulRun == nil {
		t.Fatal("LastSuccessfulRun is nil after no-op sync")
	}
	if codebase.LastSuccessfulRun.IndexedFiles != 5 {
		t.Fatalf("LastSuccessfulRun.IndexedFiles = %d, want 5 (empty-diff should preserve totals)", codebase.LastSuccessfulRun.IndexedFiles)
	}
	if codebase.LastSuccessfulRun.TotalBytes != initialTotalBytes {
		t.Fatalf("LastSuccessfulRun.TotalBytes = %d, want %d (empty-diff should preserve totals)", codebase.LastSuccessfulRun.TotalBytes, initialTotalBytes)
	}
}

// TestRunDeltaSyncInvalidatesSnapshotOnConfigChange proves a stale snapshot
// (different ConfigDigest) is treated as empty so every file gets
// re-embedded under the new config.
func TestRunDeltaSyncInvalidatesSnapshotOnConfigChange(t *testing.T) {
	t.Parallel()

	manager, cfg, repoPath := newTestManager(t)
	indexedCount := atomic.Int32{}
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
				FileHashes: map[string]string{"main.go": hashText("main.go")},
			}, nil
		},
		indexOne: func(ctx context.Context, root string, relativePath string, indexConfig model.IndexConfig) (indexer.OneFileResult, error) {
			indexedCount.Add(1)
			content := "package main\nfunc Upgraded() {}\n"
			return indexer.OneFileResult{
				Chunks: []model.StoredChunk{{
					Content:       content,
					RelativePath:  relativePath,
					StartLine:     1,
					EndLine:       2,
					Language:      "go",
					FileExtension: ".go",
				}},
				FileHash: hashText(content),
				Skipped:  false,
			}, nil
		},
	}

	initialConfig := defaultIndexConfig()
	initialConfig.SplitterType = "langchain"
	if _, _, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), initialConfig, false, emptyAdmissionBudget); err != nil {
		t.Fatalf("initial StartIndex returned error: %v", err)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	codebase, _, found, _, err := manager.GetIndex(context.Background(), repoPath)
	if err != nil || !found {
		t.Fatalf("GetIndex returned err=%v found=%v", err, found)
	}
	snapshotPath := filepath.Join(cfg.MerkleDir, codebase.ID+".json")
	firstSnapshot, err := merkle.ReadSnapshot(snapshotPath)
	if err != nil {
		t.Fatalf("ReadSnapshot returned error: %v", err)
	}
	firstDigest := firstSnapshot.ConfigDigest

	upgradedConfig := defaultIndexConfig()
	upgradedConfig.SplitterType = "ast"
	if _, _, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), upgradedConfig, false, emptyAdmissionBudget); err != nil {
		t.Fatalf("upgrade StartIndex returned error: %v", err)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	secondSnapshot, err := merkle.ReadSnapshot(snapshotPath)
	if err != nil {
		t.Fatalf("ReadSnapshot returned error: %v", err)
	}
	if secondSnapshot.ConfigDigest == firstDigest {
		t.Fatalf("ConfigDigest did not change after splitter switch: %q", secondSnapshot.ConfigDigest)
	}
	if got := indexedCount.Load(); got == 0 {
		t.Fatal("indexOne was never called; expected the digest mismatch to force re-embed")
	}
}

// TestStartIndexRejectsMatchingConfigOnIndexedCodebase confirms that a
// re-call with the same splitter and no force still returns the friendly
// "already indexed" error so accidental no-op calls remain cheap.
// TestStartIndexReRegistrationIsIdempotent confirms that re-calling
// StartIndex for an already-indexed codebase with the same config is a
// success path that returns the existing codebase record without enqueuing
// a new job. The previous behavior treated this as an error; the new model
// makes registration idempotent so callers can safely re-register without
// branching on a sentinel error string.
func TestStartIndexReRegistrationIsIdempotent(t *testing.T) {
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

	_, firstCodebase, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false, emptyAdmissionBudget)
	if err != nil {
		t.Fatalf("initial StartIndex returned error: %v", err)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	job, secondCodebase, deduplicated, overlapsCodebaseID, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false, emptyAdmissionBudget)
	if err != nil {
		t.Fatalf("re-register returned error, want idempotent success: %v", err)
	}
	if deduplicated {
		t.Fatal("re-register marked deduplicated, but no in-flight job exists")
	}
	if job.ID != "" {
		t.Fatalf("re-register returned a job (%s), but no new job should run", job.ID)
	}
	if secondCodebase.ID != firstCodebase.ID {
		t.Fatalf("re-register returned codebase %s, want existing %s", secondCodebase.ID, firstCodebase.ID)
	}
	if overlapsCodebaseID != "" {
		t.Fatalf("re-register reported overlap %s, but the same canonical path has no overlap", overlapsCodebaseID)
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

	_, codebase, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false, emptyAdmissionBudget)
	if err != nil {
		t.Fatalf("StartIndex returned error: %v", err)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	if err := os.WriteFile(mainPath, []byte("package main\nfunc SearchableThing() { println(\"changed\") }\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	release := make(chan struct{})
	manager.runner = fakeRunner{
		indexOne: func(ctx context.Context, root string, relativePath string, indexConfig model.IndexConfig) (indexer.OneFileResult, error) {
			<-release
			changedContent := "package main\nfunc SearchableThing() { println(\"changed\") }\n"
			return indexer.OneFileResult{
				Chunks: []model.StoredChunk{{
					Content:       changedContent,
					RelativePath:  relativePath,
					StartLine:     1,
					EndLine:       2,
					Language:      "go",
					FileExtension: ".go",
				}},
				FileHash: hashText(changedContent),
				Skipped:  false,
			}, nil
		},
	}

	syncer := NewBackgroundSync(cfg, manager)
	syncer.runSyncAll(context.Background(), "test")
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexing)

	codebase, activeJob, found, _, err := manager.GetIndex(context.Background(), repoPath)
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

	if _, _, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false, emptyAdmissionBudget); err != nil {
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

	if _, _, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), defaultIndexConfig(), false, emptyAdmissionBudget); err != nil {
		t.Fatalf("StartIndex returned error: %v", err)
	}
	waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)

	childDirectory := filepath.Join(repoPath, "internal", "mcpserver")
	if err := os.MkdirAll(childDirectory, 0o755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}

	codebase, _, found, _, err := manager.GetIndex(context.Background(), childDirectory)
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

	cfg, repoPath := newTestManagerConfig(t)
	manager, err := NewManager(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewManager returned error: %v", err)
	}
	// Stop queued, paused, and running work before the temp directories disappear.
	// Close cached graph engines afterward so no SQLite handle races RemoveAll.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := manager.cancelAndWaitForJobs(ctx); err != nil {
			t.Errorf("cancelAndWaitForJobs: %v", err)
		}
		manager.CloseGraphEngines()
	})
	return manager, cfg, repoPath
}

// newTestManagerConfig builds the on-disk layout a test manager needs and
// returns its config plus the repository path, without constructing the
// manager. A test that must seed daemon state before startup reads it uses this
// and calls NewManager itself.
func newTestManagerConfig(t *testing.T) (config.Config, string) {
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
		SocketPath:        filepath.Join(stateRoot, "sockets", "lm-semantic-search-daemon.sock"),
		RegistryPath:      filepath.Join(stateRoot, "registry.json"),
		JobsPath:          filepath.Join(stateRoot, "jobs.jsonl"),
		EventsPath:        filepath.Join(stateRoot, "events.jsonl"),
		LogsDir:           filepath.Join(stateRoot, "logs"),
		LogPath:           filepath.Join(stateRoot, "logs", "lm-semantic-search-daemon.log"),
		MerkleDir:         filepath.Join(stateRoot, "merkle"),
		LocksDir:          filepath.Join(stateRoot, "locks"),
		SocketsDir:        filepath.Join(stateRoot, "sockets"),
		ChunksDir:         filepath.Join(stateRoot, "chunks"),
		GraphDir:          filepath.Join(stateRoot, "graph"),
		ContextRoot:       filepath.Join(stateRoot, "context"),
		EmbeddingProvider: "OpenAI",
		EmbeddingModel:    "nvidia/NV-EmbedCode-7b-v1",
		HybridMode:        true,
		SyncIntervalMS:    300000,
	}
	for _, path := range []string{cfg.StateRoot, cfg.LogsDir, cfg.MerkleDir, cfg.LocksDir, cfg.SocketsDir, cfg.ChunksDir, cfg.GraphDir, cfg.ContextRoot} {
		if err := store.EnsureDir(path); err != nil {
			t.Fatalf("EnsureDir returned error: %v", err)
		}
	}
	if err := store.WriteRegistry(cfg.RegistryPath, model.RegistryFile{}); err != nil {
		t.Fatalf("WriteRegistry returned error: %v", err)
	}

	return cfg, repoPath
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
		codebase, _, found, _, err := manager.GetIndex(context.Background(), repoPath)
		if err != nil || !found {
			return false
		}
		return codebase.Status == wantStatus
	})
}

func waitForProgress(t *testing.T, manager *Manager, repoPath string, minimum float64) {
	t.Helper()

	waitForCondition(t, func() bool {
		_, activeJob, found, _, err := manager.GetIndex(context.Background(), repoPath)
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
