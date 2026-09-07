package daemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"goodkind.io/lm-semantic-search/internal/clock"
	"goodkind.io/lm-semantic-search/internal/indexer"
	"goodkind.io/lm-semantic-search/internal/model"
	"goodkind.io/lm-semantic-search/internal/semantic"
)

func TestConvergeViaWatcherDefersFirstBuildWithoutCollectionMissingLog(t *testing.T) {
	manager, cfg, repoPath := newTestManager(t)
	codebase := registerFirstBuildCodebase(t, manager, repoPath, "cb-first-build-watch", "job-first-build-watch", true)

	var reindexCalls atomic.Int32
	manager.semantic = &fakeSemantic{
		reindex: func(context.Context, string, []model.StoredChunk, []string) error {
			reindexCalls.Add(1)
			return semantic.ErrCollectionMissing
		},
	}

	var logBuffer bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuffer, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	syncer := NewBackgroundSync(cfg, manager)
	syncer.convergeViaWatcher(context.Background(), codebase.ID, []string{"main.go"})

	if got := reindexCalls.Load(); got != 0 {
		t.Fatalf("first-build watcher converge reindexed %d time(s), want 0", got)
	}
	if strings.Contains(logBuffer.String(), "collection_missing") {
		t.Fatalf("first-build watcher converge logged collection_missing: %s", logBuffer.String())
	}
	if got := len(syncer.deferredWatcherPaths[codebase.ID]); got != 1 {
		t.Fatalf("deferred watcher path count = %d, want 1", got)
	}
}

func TestIndexReadyFlushesDeferredWatcherPathsOnce(t *testing.T) {
	manager, cfg, repoPath := newTestManager(t)
	codebase := registerFirstBuildCodebase(t, manager, repoPath, "cb-first-build-ready", "job-first-build-ready", true)

	var reindexCalls atomic.Int32
	manager.runner = fakeRunner{
		indexOne: func(_ context.Context, _ string, relativePath string, _ model.IndexConfig) (indexer.OneFileResult, error) {
			content := "package main\nfunc ReadyFlush() {}\n"
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
	manager.semantic = &fakeSemantic{
		reindex: func(context.Context, string, []model.StoredChunk, []string) error {
			reindexCalls.Add(1)
			return nil
		},
	}

	syncer := NewBackgroundSync(cfg, manager)
	syncer.convergeViaWatcher(context.Background(), codebase.ID, []string{"main.go"})
	completeFirstBuildCodebase(manager, codebase)

	completed := codebase
	completed.Status = model.CodebaseStatusIndexed
	completed.ActiveJobID = ""
	completed.LastSuccessfulRun = &model.IndexRunSummary{IndexedFiles: 1, TotalChunks: 1}
	syncer.IndexReady(context.Background(), completed)
	syncer.IndexReady(context.Background(), completed)

	waitForCondition(t, func() bool { return reindexCalls.Load() == 1 })
	if got := len(syncer.deferredWatcherPaths[codebase.ID]); got != 0 {
		t.Fatalf("deferred watcher paths left after IndexReady = %d, want 0", got)
	}
}

func TestIndexStoppedDropsDeferredWatcherPathsOnFailureAndCancel(t *testing.T) {
	t.Run("failed job drops deferred paths", func(t *testing.T) {
		manager, cfg, repoPath := newTestManager(t)
		codebase := registerFirstBuildCodebase(t, manager, repoPath, "cb-first-build-fail", "job-first-build-fail", true)
		var reindexCalls atomic.Int32
		manager.semantic = &fakeSemantic{
			reindex: func(context.Context, string, []model.StoredChunk, []string) error {
				reindexCalls.Add(1)
				return nil
			},
		}
		manager.runner = fakeRunner{
			indexOne: func(_ context.Context, _ string, relativePath string, _ model.IndexConfig) (indexer.OneFileResult, error) {
				content := "package main\n"
				return indexer.OneFileResult{Chunks: []model.StoredChunk{{Content: content, RelativePath: relativePath, StartLine: 1, EndLine: 1}}, FileHash: hashText(content)}, nil
			},
		}

		syncer := NewBackgroundSync(cfg, manager)
		manager.SetCodebaseLifecycleHook(syncer)
		syncer.convergeViaWatcher(context.Background(), codebase.ID, []string{"main.go"})

		manager.updateJobFailed(context.Background(), "job-first-build-fail", errors.New("boom"))
		completeFirstBuildCodebase(manager, codebase)
		syncer.IndexReady(context.Background(), codebase)

		if got := reindexCalls.Load(); got != 0 {
			t.Fatalf("failed first build replayed %d deferred converge(s), want 0", got)
		}
		if got := len(syncer.deferredWatcherPaths[codebase.ID]); got != 0 {
			t.Fatalf("deferred paths after failure = %d, want 0", got)
		}
	})

	t.Run("cancelled job drops deferred paths", func(t *testing.T) {
		manager, cfg, repoPath := newTestManager(t)
		codebase := registerFirstBuildCodebase(t, manager, repoPath, "cb-first-build-cancel", "job-first-build-cancel", true)
		var reindexCalls atomic.Int32
		manager.semantic = &fakeSemantic{
			reindex: func(context.Context, string, []model.StoredChunk, []string) error {
				reindexCalls.Add(1)
				return nil
			},
		}
		manager.runner = fakeRunner{
			indexOne: func(_ context.Context, _ string, relativePath string, _ model.IndexConfig) (indexer.OneFileResult, error) {
				content := "package main\n"
				return indexer.OneFileResult{Chunks: []model.StoredChunk{{Content: content, RelativePath: relativePath, StartLine: 1, EndLine: 1}}, FileHash: hashText(content)}, nil
			},
		}

		syncer := NewBackgroundSync(cfg, manager)
		manager.SetCodebaseLifecycleHook(syncer)
		syncer.convergeViaWatcher(context.Background(), codebase.ID, []string{"main.go"})

		if _, err := manager.CancelJob(context.Background(), "job-first-build-cancel"); err != nil {
			t.Fatalf("CancelJob returned error: %v", err)
		}
		completeFirstBuildCodebase(manager, codebase)
		syncer.IndexReady(context.Background(), codebase)

		if got := reindexCalls.Load(); got != 0 {
			t.Fatalf("cancelled first build replayed %d deferred converge(s), want 0", got)
		}
		if got := len(syncer.deferredWatcherPaths[codebase.ID]); got != 0 {
			t.Fatalf("deferred paths after cancel = %d, want 0", got)
		}
	})
}

func TestRunSyncAllSkipsActiveFirstBuildButResumesInterruptedFirstBuild(t *testing.T) {
	t.Run("active first build is skipped", func(t *testing.T) {
		manager, cfg, repoPath := newTestManager(t)
		registerFirstBuildCodebase(t, manager, repoPath, "cb-active-first-build", "job-active-first-build", true)
		manager.semantic = &fakeSemantic{
			collectionName:       func(string) string { return "missing_first_build_collection" },
			listCollections:      func(context.Context) ([]string, error) { return []string{}, nil },
			hasCollectionForPath: func(context.Context, string) (bool, error) { return false, nil },
		}

		syncer := NewBackgroundSync(cfg, manager)
		syncer.runSyncAll(context.Background(), "test")

		if jobs := manager.ListJobs(""); len(jobs) != 1 {
			t.Fatalf("active first-build sweep left %d jobs, want the original active job only", len(jobs))
		}
	})

	t.Run("interrupted first build resumes", func(t *testing.T) {
		manager, cfg, repoPath := newTestManager(t)
		registerFirstBuildCodebase(t, manager, repoPath, "cb-interrupted-first-build", "", false)
		started := make(chan struct{}, 1)
		release := make(chan struct{})
		manager.runner = fakeRunner{
			indexOne: func(ctx context.Context, root string, relativePath string, indexConfig model.IndexConfig) (indexer.OneFileResult, error) {
				select {
				case started <- struct{}{}:
				default:
				}
				<-release
				content := "package main\n"
				return indexer.OneFileResult{Chunks: []model.StoredChunk{{Content: content, RelativePath: relativePath, StartLine: 1, EndLine: 1}}, FileHash: hashText(content)}, nil
			},
		}
		manager.semantic = &fakeSemantic{
			collectionName:       func(string) string { return "missing_interrupted_collection" },
			listCollections:      func(context.Context) ([]string, error) { return []string{}, nil },
			hasCollectionForPath: func(context.Context, string) (bool, error) { return false, nil },
		}

		syncer := NewBackgroundSync(cfg, manager)
		syncer.runSyncAll(context.Background(), "test")

		waitForCondition(t, func() bool { return len(manager.ListJobs("")) == 1 })
		close(release)
		<-started
		waitForCodebaseStatus(t, manager, repoPath, model.CodebaseStatusIndexed)
	})
}

func TestRepairMissingCollectionsIgnoresActiveFirstBuildStaging(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	codebase := registerFirstBuildCodebase(t, manager, repoPath, "cb-repair-first-build", "job-repair-first-build", true)
	manager.semantic = &fakeSemantic{
		collectionName:       func(string) string { return "missing_first_build_collection" },
		listCollections:      func(context.Context) ([]string, error) { return []string{}, nil },
		hasCollectionForPath: func(context.Context, string) (bool, error) { return false, nil },
	}

	manager.RepairMissingCollections(context.Background())

	readCodebase, _, found, _, err := manager.GetIndex(context.Background(), repoPath)
	if err != nil || !found {
		t.Fatalf("GetIndex returned err=%v found=%v", err, found)
	}
	if readCodebase.Status != model.CodebaseStatusIndexing {
		t.Fatalf("repair changed active first-build status to %q, want %q", readCodebase.Status, model.CodebaseStatusIndexing)
	}
	if readCodebase.LastSuccessfulRun != nil {
		t.Fatal("repair populated LastSuccessfulRun for active first build")
	}
	if jobs := manager.ListJobs(""); len(jobs) != 1 || jobs[0].ID != codebase.ActiveJobID {
		t.Fatalf("repair jobs = %v, want only active first-build job %s", jobs, codebase.ActiveJobID)
	}
}

func TestStaleConvergeTerminalStatePreservesNewerFirstBuild(t *testing.T) {
	testCases := []struct {
		name      string
		wantState model.JobState
		finish    func(context.Context, *Manager, string)
	}{
		{name: "completed", wantState: model.JobStateCompleted, finish: func(ctx context.Context, manager *Manager, jobID string) {
			manager.updateDetachedJobCompleted(ctx, jobID, indexer.Result{IndexedFiles: 1, TotalChunks: 1})
		}},
		{name: "failed", wantState: model.JobStateFailed, finish: func(ctx context.Context, manager *Manager, jobID string) {
			manager.updateDetachedJobFailed(ctx, jobID, errors.New("converge failed"))
		}},
		{name: "cancelled", wantState: model.JobStateCancelled, finish: func(ctx context.Context, manager *Manager, jobID string) {
			manager.updateDetachedJobCancelled(ctx, jobID)
		}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			manager, _, repoPath := newTestManager(t)
			staleCodebase := model.Codebase{
				ID:              "cb-converge-stale",
				CanonicalPath:   repoPath,
				Status:          model.CodebaseStatusIndexed,
				EffectiveConfig: defaultIndexConfig(),
			}
			manager.mu.Lock()
			manager.codebases[staleCodebase.ID] = staleCodebase
			manager.mu.Unlock()
			syncer := NewBackgroundSync(manager.config, manager)
			registration, _, err := syncer.registerConvergeJob(context.Background(), staleCodebase, []string{"main.go"})
			if err != nil {
				t.Fatalf("registerConvergeJob returned error: %v", err)
			}
			defer registration.release()

			firstJob := newQueuedJob(staleCodebase.ID, repoPath, repoPath, testClientInfo(), string(jobOperationIndex), false, defaultIndexConfig(), emptyAdmissionBudget, clock.Now())
			current := staleCodebase
			current.Status = model.CodebaseStatusPending
			current.ActiveJobID = firstJob.ID
			manager.mu.Lock()
			manager.codebases[current.ID] = current
			manager.jobs[firstJob.ID] = firstJob
			if err := manager.saveLocked(); err != nil {
				manager.mu.Unlock()
				t.Fatalf("saveLocked returned error: %v", err)
			}
			manager.config.RegistryPath = t.TempDir()
			manager.mu.Unlock()

			testCase.finish(context.Background(), manager, registration.job.ID)
			manager.mu.Lock()
			gotCodebase := manager.codebases[current.ID]
			gotConverge := manager.jobs[registration.job.ID]
			manager.mu.Unlock()
			if gotCodebase.Status != current.Status || gotCodebase.ActiveJobID != current.ActiveJobID || gotCodebase.LastFailedRun != current.LastFailedRun || gotCodebase.LastSuccessfulRun != current.LastSuccessfulRun || !gotCodebase.UpdatedAt.Equal(current.UpdatedAt) {
				t.Fatalf("codebase after converge terminal state = %+v, want unchanged %+v", gotCodebase, current)
			}
			if gotConverge.State != testCase.wantState {
				t.Fatalf("converge state = %q, want %q", gotConverge.State, testCase.wantState)
			}

			backendCalled := false
			manager.runner = fakeRunner{index: func(context.Context, string, model.IndexConfig, func(indexer.Progress)) (indexer.Result, error) {
				backendCalled = true
				return indexer.Result{}, nil
			}}
			manager.runJob(context.Background(), firstJob.ID)
			if backendCalled {
				t.Fatal("first build reached backend after its registry persistence failed")
			}
		})
	}
}

func TestRegisterConvergeJobRejectsChangedOwnership(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	staleCodebase := model.Codebase{
		ID:              "cb-converge-changed-owner",
		CanonicalPath:   repoPath,
		Status:          model.CodebaseStatusIndexed,
		EffectiveConfig: defaultIndexConfig(),
	}
	current := staleCodebase
	current.Status = model.CodebaseStatusPending
	current.ActiveJobID = "newer-first-build"
	manager.mu.Lock()
	manager.codebases[current.ID] = current
	manager.mu.Unlock()

	syncer := NewBackgroundSync(manager.config, manager)
	syncer.queue = NewEventQueue(time.Hour, func(string, []string) {})
	if _, _, err := syncer.registerConvergeJob(context.Background(), staleCodebase, []string{"main.go"}); err == nil {
		t.Fatal("registerConvergeJob returned nil after ownership changed")
	}
	manager.mu.Lock()
	gotCodebase := manager.codebases[current.ID]
	jobCount := len(manager.jobs)
	manager.mu.Unlock()
	if gotCodebase.Status != current.Status || gotCodebase.ActiveJobID != current.ActiveJobID {
		t.Fatalf("codebase after rejected converge = %+v, want unchanged %+v", gotCodebase, current)
	}
	if jobCount != 0 {
		t.Fatalf("job count after rejected converge = %d, want 0", jobCount)
	}
	if got := syncer.queue.PendingCounts()[current.ID]; got != 1 {
		t.Fatalf("requeued path count = %d, want 1", got)
	}
}

func TestRegisterConvergeJobRequeuesPathsWhenJournalQueuedFails(t *testing.T) {
	manager, _, repoPath := newTestManager(t)
	codebase := seedConvergeCodebase(t, manager, repoPath)
	manager.jobJournal.close()
	manager.jobJournal = nil
	manager.appendJobEvent = func(_ string, event model.JobEvent) error {
		if event.Event == "converge_queued" {
			return errors.New("journal unavailable")
		}
		return nil
	}

	syncer := NewBackgroundSync(manager.config, manager)
	syncer.queue = NewEventQueue(time.Hour, func(string, []string) {})
	if _, _, err := syncer.registerConvergeJob(context.Background(), codebase, []string{"main.go"}); err == nil {
		t.Fatal("registerConvergeJob returned nil after queued journal failure")
	}
	manager.mu.Lock()
	jobCount := len(manager.jobs)
	cancelCount := len(manager.cancels)
	manager.mu.Unlock()
	if jobCount != 0 || cancelCount != 0 {
		t.Fatalf("failed registration left jobs=%d cancels=%d, want 0 and 0", jobCount, cancelCount)
	}
	if got := syncer.queue.PendingCounts()[codebase.ID]; got != 1 {
		t.Fatalf("requeued path count = %d, want 1", got)
	}
}

func TestDetachedJobTerminalTransitionsPreserveFirstTerminalState(t *testing.T) {
	testCases := []struct {
		name   string
		finish func(context.Context, *Manager, string)
	}{
		{name: "completed", finish: func(ctx context.Context, manager *Manager, jobID string) {
			manager.updateDetachedJobCompleted(ctx, jobID, indexer.Result{IndexedFiles: 1, TotalChunks: 1})
		}},
		{name: "failed", finish: func(ctx context.Context, manager *Manager, jobID string) {
			manager.updateDetachedJobFailed(ctx, jobID, errors.New("converge failed"))
		}},
		{name: "cancelled", finish: func(ctx context.Context, manager *Manager, jobID string) {
			manager.updateDetachedJobCancelled(ctx, jobID)
		}},
	}
	terminalStates := []model.JobState{
		model.JobStateCompleted,
		model.JobStateFailed,
		model.JobStateCancelled,
	}
	for _, initialState := range terminalStates {
		for _, testCase := range testCases {
			t.Run(string(initialState)+"/"+testCase.name, func(t *testing.T) {
				manager, _, repoPath := newTestManager(t)
				completedAt := clock.Now().Add(-time.Minute)
				job := newQueuedJob("cb-terminal", repoPath, repoPath, testClientInfo(), "converge", false, defaultIndexConfig(), emptyAdmissionBudget, completedAt)
				job.State = initialState
				job.CompletedAt = &completedAt
				job.Progress.Phase = string(initialState)
				manager.mu.Lock()
				manager.jobs[job.ID] = job
				manager.mu.Unlock()

				testCase.finish(context.Background(), manager, job.ID)

				got, found := manager.GetJob(job.ID)
				if !found {
					t.Fatalf("GetJob(%q) did not find terminal job", job.ID)
				}
				if got.State != initialState || got.Progress.Phase != string(initialState) || got.CompletedAt == nil || !got.CompletedAt.Equal(completedAt) {
					t.Fatalf("job after %s transition = %+v, want original %s terminal state", testCase.name, got, initialState)
				}
			})
		}
	}
}

func TestUpdateJobFailedPreservesExistingTerminalState(t *testing.T) {
	terminalStates := []model.JobState{
		model.JobStateCompleted,
		model.JobStateFailed,
		model.JobStateCancelled,
	}
	for _, initialState := range terminalStates {
		t.Run(string(initialState), func(t *testing.T) {
			manager, _, repoPath := newTestManager(t)
			completedAt := clock.Now().Add(-time.Minute)
			job := newQueuedJob("cb-terminal", repoPath, repoPath, testClientInfo(), string(jobOperationIndex), false, defaultIndexConfig(), emptyAdmissionBudget, completedAt)
			job.State = initialState
			job.CompletedAt = &completedAt
			job.Progress.Phase = string(initialState)
			manager.mu.Lock()
			manager.jobs[job.ID] = job
			manager.mu.Unlock()

			manager.updateJobFailed(context.Background(), job.ID, errors.New("late failure"))

			got, found := manager.GetJob(job.ID)
			if !found {
				t.Fatalf("GetJob(%q) did not find terminal job", job.ID)
			}
			if got.State != initialState || got.Progress.Phase != string(initialState) || got.CompletedAt == nil || !got.CompletedAt.Equal(completedAt) {
				t.Fatalf("job after late failure = %+v, want original %s terminal state", got, initialState)
			}
		})
	}
}

func registerFirstBuildCodebase(t *testing.T, manager *Manager, repoPath string, codebaseID string, jobID string, active bool) model.Codebase {
	t.Helper()

	canonicalPath, err := filepath.EvalSymlinks(repoPath)
	if err != nil {
		t.Fatalf("EvalSymlinks returned error: %v", err)
	}
	indexConfig := defaultIndexConfig()
	indexConfig.IgnoreDigest = digestIndexConfig(indexConfig)
	codebase := newCodebaseRecord(canonicalPath)
	codebase.ID = codebaseID
	codebase.Status = model.CodebaseStatusIndexing
	codebase.EffectiveConfig = indexConfig
	codebase.CollectionName = "missing_first_build_collection"
	if active {
		codebase.ActiveJobID = jobID
	}

	manager.mu.Lock()
	manager.codebases[codebase.ID] = codebase
	if active {
		job := newQueuedJob(codebase.ID, repoPath, canonicalPath, testClientInfo(), string(jobOperationIndex), false, indexConfig, emptyAdmissionBudget, codebase.UpdatedAt)
		job.ID = jobID
		job.State = model.JobStateRunning
		manager.jobs[job.ID] = job
	}
	if saveErr := manager.saveLocked(); saveErr != nil {
		manager.mu.Unlock()
		t.Fatalf("saveLocked returned error: %v", saveErr)
	}
	manager.mu.Unlock()
	return codebase
}

func completeFirstBuildCodebase(manager *Manager, codebase model.Codebase) {
	completed := codebase
	completed.Status = model.CodebaseStatusIndexed
	completed.ActiveJobID = ""
	completed.LastSuccessfulRun = &model.IndexRunSummary{IndexedFiles: 1, TotalChunks: 1}
	manager.mu.Lock()
	manager.codebases[completed.ID] = completed
	manager.mu.Unlock()
}
