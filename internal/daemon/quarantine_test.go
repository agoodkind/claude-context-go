package daemon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"goodkind.io/lm-semantic-search/internal/indexer"
	"goodkind.io/lm-semantic-search/internal/merkle"
	"goodkind.io/lm-semantic-search/internal/model"
)

func writeSyntheticSnapshot(t *testing.T, manager *Manager, codebase model.Codebase, fileCount int) {
	t.Helper()
	files := make(map[string]string, fileCount)
	for i := 0; i < fileCount; i++ {
		files[fmt.Sprintf("f%03d.go", i)] = fmt.Sprintf("hash-%03d", i)
	}
	snapshot := merkle.Snapshot{
		ConfigDigest: codebase.EffectiveConfig.IgnoreDigest,
		Files:        files,
		Inodes:       nil,
	}
	if err := merkle.WriteSnapshot(manager.snapshotPathForCodebase(codebase), snapshot); err != nil {
		t.Fatalf("WriteSnapshot returned error: %v", err)
	}
}

func TestConvergePathsRootMissingMarksMissingAndSkipsDelete(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	indexConfig := defaultIndexConfig()
	indexConfig.IgnoreDigest = digestIndexConfig(indexConfig)
	codebase := newCodebaseRecord(repoPath)
	codebase.Status = model.CodebaseStatusIndexed
	codebase.EffectiveConfig = indexConfig
	codebase.LastSuccessfulRun = &model.IndexRunSummary{IndexedFiles: 1, TotalChunks: 1}
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.mu.Unlock()
	writeSyntheticSnapshot(t, manager, codebase, 1)

	var reindexCalls atomic.Int32
	manager.semantic = &fakeSemantic{
		reindex: func(_ context.Context, _ string, _ []model.StoredChunk, _ []string) error {
			reindexCalls.Add(1)
			return nil
		},
	}

	if err := os.RemoveAll(repoPath); err != nil {
		t.Fatalf("RemoveAll returned error: %v", err)
	}
	if _, err := manager.ConvergePaths(context.Background(), codebase.ID, []string{"f000.go"}, nil); err != nil {
		t.Fatalf("ConvergePaths returned error: %v", err)
	}

	manager.mu.Lock()
	readCodebase, found := manager.codebases[codebase.ID]
	manager.mu.Unlock()
	if !found {
		t.Fatalf("tracked codebase %s disappeared", codebase.ID)
	}
	if readCodebase.Status != model.CodebaseStatusMissing {
		t.Fatalf("status = %q, want missing", readCodebase.Status)
	}
	if got := reindexCalls.Load(); got != 0 {
		t.Fatalf("reindex calls = %d, want 0 when the root disappears", got)
	}

	snapshot, err := merkle.ReadSnapshot(manager.snapshotPathForCodebase(codebase))
	if err != nil {
		t.Fatalf("ReadSnapshot returned error: %v", err)
	}
	if len(snapshot.Files) != 1 {
		t.Fatalf("snapshot files = %d, want 1 unchanged entry after missing-root hold", len(snapshot.Files))
	}
}

func TestConvergePathsLargeWatcher(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	indexConfig := defaultIndexConfig()
	indexConfig.IgnoreDigest = digestIndexConfig(indexConfig)
	codebase := newCodebaseRecord(repoPath)
	codebase.Status = model.CodebaseStatusIndexed
	codebase.EffectiveConfig = indexConfig
	codebase.LastSuccessfulRun = &model.IndexRunSummary{IndexedFiles: 120, TotalChunks: 120}
	codebase.LiveFileTotal = 120
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.mu.Unlock()
	writeSyntheticSnapshot(t, manager, codebase, 120)
	checkpointPath := manager.snapshotPathForCodebase(codebase)
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
		reindex: func(_ context.Context, _ string, _ []model.StoredChunk, _ []string) error {
			reindexCalls.Add(1)
			return nil
		},
	}

	relativePaths := make([]string, 0, 110)
	for i := 0; i < 110; i++ {
		relativePaths = append(relativePaths, fmt.Sprintf("f%03d.go", i))
	}
	outcome, err := manager.ConvergePaths(context.Background(), codebase.ID, relativePaths, nil)
	if err != nil {
		t.Fatalf("ConvergePaths returned error: %v", err)
	}
	if outcome.PathsGiven != 110 {
		t.Fatalf("PathsGiven = %d, want 110", outcome.PathsGiven)
	}
	if outcome.PathsProcessed != 110 {
		t.Fatalf("PathsProcessed = %d, want 110", outcome.PathsProcessed)
	}
	if outcome.PathsConverged != 0 {
		t.Fatalf("PathsConverged = %d, want 0", outcome.PathsConverged)
	}

	manager.mu.Lock()
	readCodebase, found := manager.codebases[codebase.ID]
	manager.mu.Unlock()
	if !found {
		t.Fatalf("tracked codebase %s disappeared", codebase.ID)
	}
	if readCodebase.Status != model.CodebaseStatusIndexed {
		t.Fatalf("status = %q, want indexed", readCodebase.Status)
	}
	if readCodebase.Quarantine != nil {
		t.Fatalf("Quarantine = %+v, want nil", readCodebase.Quarantine)
	}
	if got := indexOneCalls.Load(); got != 0 {
		t.Fatalf("IndexOne calls = %d, want 0 for missing watcher paths", got)
	}
	if got := reindexCalls.Load(); got != 0 {
		t.Fatalf("reindex calls = %d, want 0 for missing watcher paths", got)
	}

	afterBytes, err := os.ReadFile(checkpointPath)
	if err != nil {
		t.Fatalf("ReadFile checkpoint after converge: %v", err)
	}
	if string(afterBytes) != string(checkpointBytes) {
		t.Fatal("checkpoint content changed after missing watcher paths")
	}
}

func TestSyncIndexLargeRemovalQuarantinesBeforeDelete(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	indexConfig := defaultIndexConfig()

	for i := 0; i < 119; i++ {
		path := filepath.Join(repoPath, fmt.Sprintf("f%03d.go", i))
		if err := os.WriteFile(path, []byte("package main\n"), 0o644); err != nil {
			t.Fatalf("WriteFile(%s) returned error: %v", path, err)
		}
	}

	var reindexCalls atomic.Int32
	manager.semantic = &fakeSemantic{
		reindex: func(_ context.Context, _ string, _ []model.StoredChunk, _ []string) error {
			reindexCalls.Add(1)
			return nil
		},
	}

	if _, _, _, _, err := manager.StartIndex(context.Background(), repoPath, testClientInfo(), indexConfig, false, emptyAdmissionBudget); err != nil {
		t.Fatalf("StartIndex returned error: %v", err)
	}
	indexed := waitForCodebaseSettled(t, manager, repoPath)
	reindexCalls.Store(0)

	for i := 0; i < 110; i++ {
		if err := os.Remove(filepath.Join(repoPath, fmt.Sprintf("f%03d.go", i))); err != nil {
			t.Fatalf("Remove returned error: %v", err)
		}
	}

	job, _, _, err := manager.SyncIndex(context.Background(), repoPath, testClientInfo())
	if err != nil {
		t.Fatalf("SyncIndex returned error: %v", err)
	}
	waitForCondition(t, func() bool {
		codebase, _, found, _, getErr := manager.GetIndex(context.Background(), repoPath)
		if getErr != nil || !found {
			return false
		}
		return codebase.Status == model.CodebaseStatusQuarantined
	})

	quarantinedCodebase, _, found, _, err := manager.GetIndex(context.Background(), repoPath)
	if err != nil || !found {
		t.Fatalf("GetIndex returned err=%v found=%v", err, found)
	}
	if quarantinedCodebase.Quarantine == nil {
		t.Fatal("Quarantine = nil, want recorded quarantine state")
	}
	if quarantinedCodebase.Quarantine.LastMissingCount != 110 || quarantinedCodebase.Quarantine.LastTotalCount != 120 {
		t.Fatalf("quarantine counts = %+v, want 110 of 120", quarantinedCodebase.Quarantine)
	}
	recordedJob, found := manager.GetJob(job.ID)
	if !found {
		t.Fatalf("GetJob(%s) did not find the quarantined sync job", job.ID)
	}
	if recordedJob.State != model.JobStateFailed {
		t.Fatalf("job state = %q, want failed terminal quarantine job", recordedJob.State)
	}
	if recordedJob.Error == nil || recordedJob.Error.Message == "" {
		t.Fatal("quarantine job missing error message")
	}
	if got := reindexCalls.Load(); got != 0 {
		t.Fatalf("reindex calls = %d, want 0 before quarantine release", got)
	}

	snapshot, err := merkle.ReadSnapshot(manager.snapshotPathForCodebase(indexed))
	if err != nil {
		t.Fatalf("ReadSnapshot returned error: %v", err)
	}
	if len(snapshot.Files) != 120 {
		t.Fatalf("snapshot files = %d, want 120 unchanged entries after quarantine", len(snapshot.Files))
	}
}

func TestHandleQuarantinedCodebaseClearsOnEmptyDiff(t *testing.T) {
	t.Parallel()

	manager, cfg, repoPath := newTestManager(t)
	indexConfig := defaultIndexConfig()
	indexConfig.IgnoreDigest = digestIndexConfig(indexConfig)
	codebase := newCodebaseRecord(repoPath)
	codebase.Status = model.CodebaseStatusQuarantined
	codebase.EffectiveConfig = indexConfig
	codebase.LastSuccessfulRun = &model.IndexRunSummary{IndexedFiles: 1, TotalChunks: 1}
	codebase.Quarantine = &model.QuarantineState{
		Reason:           quarantineReasonFullScanLargeDelete,
		ObservationCount: 1,
		LastTrigger:      quarantineTriggerFullScan,
		LastMissingCount: 110,
		LastTotalCount:   120,
	}
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.mu.Unlock()

	content := "package main\n"
	if err := os.WriteFile(filepath.Join(repoPath, "main.go"), []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	snapshot := merkle.Snapshot{
		ConfigDigest: indexConfig.IgnoreDigest,
		Files:        map[string]string{"main.go": hashText(content)},
		Inodes:       nil,
	}
	if err := merkle.WriteSnapshot(manager.snapshotPathForCodebase(codebase), snapshot); err != nil {
		t.Fatalf("WriteSnapshot returned error: %v", err)
	}

	syncer := NewBackgroundSync(cfg, manager)
	syncer.handleQuarantinedCodebase(context.Background(), codebase)

	manager.mu.Lock()
	readCodebase, found := manager.codebases[codebase.ID]
	manager.mu.Unlock()
	if !found {
		t.Fatalf("tracked codebase %s disappeared", codebase.ID)
	}
	if readCodebase.Status != model.CodebaseStatusIndexed {
		t.Fatalf("status = %q, want indexed after empty diff", readCodebase.Status)
	}
	if readCodebase.Quarantine != nil {
		t.Fatalf("Quarantine = %+v, want cleared after empty diff", readCodebase.Quarantine)
	}
}

func TestAssessDeltaDeleteWaveFlagsLargeRemoval(t *testing.T) {
	t.Parallel()

	codebase := model.Codebase{
		Kind:              model.CodebaseKindCode,
		Status:            model.CodebaseStatusIndexed,
		LastSuccessfulRun: &model.IndexRunSummary{IndexedFiles: 120, TotalChunks: 120},
		LiveFileTotal:     120,
	}
	snapshot := merkle.Snapshot{Files: make(map[string]string, 120)}
	for i := 0; i < 120; i++ {
		snapshot.Files[fmt.Sprintf("f%03d.go", i)] = fmt.Sprintf("hash-%03d", i)
	}
	diff := merkle.Diff{Added: nil, Modified: nil, Removed: make([]string, 110)}
	for i := 0; i < 110; i++ {
		diff.Removed[i] = fmt.Sprintf("f%03d.go", i)
	}
	signal, suspicious := assessDeltaDeleteWave(codebase, diff, snapshot, t.TempDir())
	if !suspicious {
		t.Fatal("assessDeltaDeleteWave returned suspicious=false, want true for 110 of 120 removed")
	}
	if signal.missingCount != 110 || signal.totalCount != 120 {
		t.Fatalf("signal = %+v, want 110 of 120", signal)
	}
}

func TestAssessDeltaDeleteWaveSkipsAfterFullScanConfirmation(t *testing.T) {
	t.Parallel()

	codebase := model.Codebase{
		Kind:              model.CodebaseKindCode,
		Status:            model.CodebaseStatusQuarantined,
		LastSuccessfulRun: &model.IndexRunSummary{IndexedFiles: 120, TotalChunks: 120},
		LiveFileTotal:     120,
		Quarantine: &model.QuarantineState{
			ObservationCount: quarantineConfirmationObservations,
			LastTrigger:      quarantineTriggerFullScan,
		},
	}
	snapshot := merkle.Snapshot{Files: make(map[string]string, 120)}
	for i := 0; i < 120; i++ {
		snapshot.Files[fmt.Sprintf("f%03d.go", i)] = fmt.Sprintf("hash-%03d", i)
	}
	diff := merkle.Diff{Added: nil, Modified: nil, Removed: make([]string, 110)}
	for i := 0; i < 110; i++ {
		diff.Removed[i] = fmt.Sprintf("f%03d.go", i)
	}
	if _, suspicious := assessDeltaDeleteWave(codebase, diff, snapshot, t.TempDir()); suspicious {
		t.Fatal("assessDeltaDeleteWave returned suspicious=true after confirmation, want false so destructive sync can proceed")
	}
}

func TestAssessDeltaDeleteWaveIgnoresPhysicallyPresentRemovedPaths(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	codebase := model.Codebase{
		Kind:              model.CodebaseKindCode,
		Status:            model.CodebaseStatusIndexed,
		LastSuccessfulRun: &model.IndexRunSummary{IndexedFiles: 120, TotalChunks: 120},
		LiveFileTotal:     120,
	}
	snapshot := merkle.Snapshot{Files: make(map[string]string, 120)}
	diff := merkle.Diff{Added: nil, Modified: nil, Removed: make([]string, 110)}
	for i := 0; i < 120; i++ {
		relativePath := fmt.Sprintf("vendor/lib/f%03d.go", i)
		snapshot.Files[relativePath] = fmt.Sprintf("hash-%03d", i)
		if i < 110 {
			diff.Removed[i] = relativePath
		}
	}
	for i := 0; i < 110; i++ {
		path := filepath.Join(root, diff.Removed[i])
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("MkdirAll returned error: %v", err)
		}
		if err := os.WriteFile(path, []byte("package lib\n"), 0o644); err != nil {
			t.Fatalf("WriteFile returned error: %v", err)
		}
	}

	signal, suspicious := assessDeltaDeleteWave(codebase, diff, snapshot, root)
	if suspicious {
		t.Fatalf("assessDeltaDeleteWave quarantined physically present newly-excluded files: signal = %+v", signal)
	}
}

func TestQuarantineTerminalDoesNotClearSuccessor(t *testing.T) {
	t.Parallel()

	manager, _, repoPath := newTestManager(t)
	codebase := newCodebaseRecord(repoPath)
	codebase.Status = model.CodebaseStatusIndexing
	codebase.ActiveJobID = "successor"
	stale := model.Job{ID: "stale", CodebaseID: codebase.ID, State: model.JobStateRunning}
	successor := model.Job{ID: "successor", CodebaseID: codebase.ID, State: model.JobStateRunning}
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.jobs[stale.ID] = stale
	manager.jobs[successor.ID] = successor
	manager.mu.Unlock()

	manager.updateJobQuarantined(context.Background(), stale.ID, quarantineSignal{
		trigger:      "test",
		missingCount: suspiciousRemovalAbsoluteThreshold,
		totalCount:   suspiciousRemovalAbsoluteThreshold,
	})

	failed, found := manager.GetJob(stale.ID)
	if !found || failed.State != model.JobStateFailed {
		t.Fatalf("stale job = %+v, want failed", failed)
	}
	manager.mu.Lock()
	current := manager.codebases[codebase.ID]
	manager.mu.Unlock()
	if current.ActiveJobID != successor.ID {
		t.Fatalf("active job id = %q, want successor %q", current.ActiveJobID, successor.ID)
	}
	if current.Status != model.CodebaseStatusIndexing || current.Quarantine != nil {
		t.Fatalf("successor codebase changed by stale quarantine: %+v", current)
	}
}

// TestHandleQuarantinedCodebaseHoldsDuringVCSOperation confirms the
// confirmation gate never clears a quarantine or advances toward destructive
// sync while a git operation is in progress, even when the on-disk tree looks
// clean.
func TestHandleQuarantinedCodebaseHoldsDuringVCSOperation(t *testing.T) {
	t.Parallel()

	manager, cfg, repoPath := newTestManager(t)
	indexConfig := defaultIndexConfig()
	indexConfig.IgnoreDigest = digestIndexConfig(indexConfig)
	codebase := newCodebaseRecord(repoPath)
	codebase.Status = model.CodebaseStatusQuarantined
	codebase.EffectiveConfig = indexConfig
	codebase.LastSuccessfulRun = &model.IndexRunSummary{IndexedFiles: 1, TotalChunks: 1}
	codebase.Quarantine = &model.QuarantineState{
		Reason:           quarantineReasonFullScanLargeDelete,
		ObservationCount: 1,
		LastTrigger:      quarantineTriggerFullScan,
		LastMissingCount: 110,
		LastTotalCount:   120,
	}
	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	manager.mu.Unlock()

	content := "package main\n"
	if err := os.WriteFile(filepath.Join(repoPath, "main.go"), []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	snapshot := merkle.Snapshot{
		ConfigDigest: indexConfig.IgnoreDigest,
		Files:        map[string]string{"main.go": hashText(content)},
		Inodes:       nil,
	}
	if err := merkle.WriteSnapshot(manager.snapshotPathForCodebase(codebase), snapshot); err != nil {
		t.Fatalf("WriteSnapshot returned error: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(repoPath, ".git", "rebase-merge"), 0o755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}

	syncer := NewBackgroundSync(cfg, manager)
	syncer.handleQuarantinedCodebase(context.Background(), codebase)

	manager.mu.Lock()
	readCodebase, found := manager.codebases[codebase.ID]
	manager.mu.Unlock()
	if !found {
		t.Fatalf("tracked codebase %s disappeared", codebase.ID)
	}
	if readCodebase.Status != model.CodebaseStatusQuarantined {
		t.Fatalf("status = %q, want quarantine held during a git operation", readCodebase.Status)
	}
	if readCodebase.Quarantine == nil {
		t.Fatal("Quarantine = nil, want quarantine retained during a git operation")
	}
}
